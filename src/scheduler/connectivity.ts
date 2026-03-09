import type { SupportedSite } from '../sites';

export type SiteConnectivityStatus = 'healthy' | 'degraded' | 'blocked' | 'probing';

export type NYCAcrisFailureClass =
  | 'policy_block'
  | 'timeout_or_navigation'
  | 'token_or_session_state'
  | 'selector_or_empty_results'
  | 'viewer_roundtrip'
  | 'sheet_export';

export interface SiteConnectivityState {
  site: SupportedSite;
  status: SiteConnectivityStatus;
  opened_at?: string;
  last_success_at?: string;
  last_failure_at?: string;
  policy_block_count: number;
  timeout_count: number;
  empty_result_count: number;
  window_started_at?: string;
  next_probe_at?: string;
  consecutive_probe_successes: number;
  last_failure_reason?: string;
  last_alerted_at?: string;
  last_recovery_alert_at?: string;
}

export interface ConnectivityConfig {
  policyFailureThreshold: number;
  transientFailureThreshold: number;
  failureWindowMinutes: number;
  circuitCooldownMinutes: number;
  probeIntervalMinutes: number;
  probeSuccessesRequired: number;
}

const DEFAULT_CONFIG: ConnectivityConfig = {
  policyFailureThreshold: 2,
  transientFailureThreshold: 3,
  failureWindowMinutes: 15,
  circuitCooldownMinutes: 60,
  probeIntervalMinutes: 15,
  probeSuccessesRequired: 2,
};

export function getConnectivityConfig(): ConnectivityConfig {
  return {
    policyFailureThreshold: Number(process.env.NYC_ACRIS_POLICY_FAILURE_THRESHOLD ?? DEFAULT_CONFIG.policyFailureThreshold),
    transientFailureThreshold: Number(process.env.NYC_ACRIS_TRANSIENT_FAILURE_THRESHOLD ?? DEFAULT_CONFIG.transientFailureThreshold),
    failureWindowMinutes: Number(process.env.NYC_ACRIS_FAILURE_WINDOW_MINUTES ?? DEFAULT_CONFIG.failureWindowMinutes),
    circuitCooldownMinutes: Number(process.env.NYC_ACRIS_CIRCUIT_COOLDOWN_MINUTES ?? DEFAULT_CONFIG.circuitCooldownMinutes),
    probeIntervalMinutes: Number(process.env.NYC_ACRIS_PROBE_INTERVAL_MINUTES ?? DEFAULT_CONFIG.probeIntervalMinutes),
    probeSuccessesRequired: Number(process.env.NYC_ACRIS_PROBE_SUCCESSES_REQUIRED ?? DEFAULT_CONFIG.probeSuccessesRequired),
  };
}

export function createDefaultConnectivityState(site: SupportedSite): SiteConnectivityState {
  return {
    site,
    status: 'healthy',
    policy_block_count: 0,
    timeout_count: 0,
    empty_result_count: 0,
    consecutive_probe_successes: 0,
  };
}

function toIso(input: Date): string {
  return input.toISOString();
}

function addMinutes(isoOrDate: string | Date, minutes: number): string {
  const input = typeof isoOrDate === 'string' ? new Date(isoOrDate) : isoOrDate;
  return new Date(input.getTime() + minutes * 60 * 1000).toISOString();
}

function isExpired(windowStartedAt: string | undefined, now: Date, windowMinutes: number): boolean {
  if (!windowStartedAt) return true;
  return now.getTime() - new Date(windowStartedAt).getTime() > windowMinutes * 60 * 1000;
}

function resetRollingCounters(state: SiteConnectivityState, now: Date): SiteConnectivityState {
  return {
    ...state,
    policy_block_count: 0,
    timeout_count: 0,
    empty_result_count: 0,
    window_started_at: toIso(now),
  };
}

export function classifyNYCAcrisFailure(message: string): NYCAcrisFailureClass {
  const normalized = message.toLowerCase();

  if (
    /brob|robots\.txt|classified as government|usage policy|access denied|full access|proxy_error/.test(normalized)
  ) {
    return 'policy_block';
  }

  if (/sheet_upload_mismatch|sheet export|googleapis|sheets/i.test(message)) {
    return 'sheet_export';
  }

  if (/missing anti-forgery token|requestverificationtoken|session_budget_exceeded|live session state/.test(normalized)) {
    return 'token_or_session_state';
  }

  if (/documentimageview|mainframe|viewer|return to acris result page|go_image/.test(normalized)) {
    return 'viewer_roundtrip';
  }

  if (/no acris rows found|no rows found on result page|no new rows found|selector/i.test(normalized)) {
    return 'selector_or_empty_results';
  }

  return 'timeout_or_navigation';
}

export function recordConnectivityFailure(
  currentState: SiteConnectivityState | null | undefined,
  reason: string,
  failureClass: NYCAcrisFailureClass,
  now = new Date(),
): { state: SiteConnectivityState; becameBlocked: boolean; becameDegraded: boolean } {
  const config = getConnectivityConfig();
  let state = currentState ?? createDefaultConnectivityState('nyc_acris');

  if (isExpired(state.window_started_at, now, config.failureWindowMinutes)) {
    state = resetRollingCounters(state, now);
  }

  state = {
    ...state,
    last_failure_at: toIso(now),
    last_failure_reason: reason,
    consecutive_probe_successes: 0,
  };

  if (failureClass === 'policy_block') {
    state.policy_block_count += 1;
  } else if (
    failureClass === 'timeout_or_navigation' ||
    failureClass === 'viewer_roundtrip' ||
    failureClass === 'token_or_session_state'
  ) {
    state.timeout_count += 1;
  } else if (failureClass === 'selector_or_empty_results') {
    state.empty_result_count += 1;
  }

  const shouldBlock =
    state.policy_block_count >= config.policyFailureThreshold ||
    state.timeout_count >= config.transientFailureThreshold;
  const shouldDegrade = state.empty_result_count >= 2;
  const becameBlocked = state.status !== 'blocked' && shouldBlock;
  const becameDegraded = state.status === 'healthy' && shouldDegrade && !shouldBlock;

  if (shouldBlock) {
    state.status = 'blocked';
    state.opened_at = state.opened_at ?? toIso(now);
    state.next_probe_at = addMinutes(now, config.probeIntervalMinutes);
  } else if (shouldDegrade) {
    state.status = 'degraded';
    state.next_probe_at = addMinutes(now, config.probeIntervalMinutes);
  }

  return { state, becameBlocked, becameDegraded };
}

export function recordConnectivitySuccess(
  currentState: SiteConnectivityState | null | undefined,
  mode: 'probe' | 'run',
  now = new Date(),
): { state: SiteConnectivityState; recovered: boolean } {
  const config = getConnectivityConfig();
  let state = currentState ?? createDefaultConnectivityState('nyc_acris');
  const wasBlockedOrProbing = state.status === 'blocked' || state.status === 'probing';

  if (mode === 'probe' && wasBlockedOrProbing) {
    const consecutiveProbeSuccesses = state.consecutive_probe_successes + 1;
    if (consecutiveProbeSuccesses >= config.probeSuccessesRequired) {
      return {
        recovered: true,
        state: {
          ...createDefaultConnectivityState(state.site),
          last_success_at: toIso(now),
          last_recovery_alert_at: state.last_recovery_alert_at,
          last_alerted_at: state.last_alerted_at,
        },
      };
    }

    return {
      recovered: false,
      state: {
        ...state,
        status: 'probing',
        last_success_at: toIso(now),
        consecutive_probe_successes: consecutiveProbeSuccesses,
        next_probe_at: addMinutes(now, config.probeIntervalMinutes),
      },
    };
  }

  return {
    recovered: wasBlockedOrProbing || state.status === 'degraded',
    state: {
      ...createDefaultConnectivityState(state.site),
      last_success_at: toIso(now),
      last_alerted_at: state.last_alerted_at,
      last_recovery_alert_at: state.last_recovery_alert_at,
    },
  };
}

export function markConnectivityAlerted(state: SiteConnectivityState, now = new Date()): SiteConnectivityState {
  return {
    ...state,
    last_alerted_at: toIso(now),
  };
}

export function markConnectivityRecoveryAlerted(state: SiteConnectivityState, now = new Date()): SiteConnectivityState {
  return {
    ...state,
    last_recovery_alert_at: toIso(now),
  };
}

export function shouldRunConnectivityProbe(state: SiteConnectivityState | null | undefined, now = new Date()): boolean {
  if (!state) return false;
  if (state.site !== 'nyc_acris') return false;
  if (state.status !== 'blocked' && state.status !== 'probing' && state.status !== 'degraded') return false;
  if (!state.next_probe_at) return false;
  return new Date(state.next_probe_at).getTime() <= now.getTime();
}

export function shouldSendProlongedBlockedAlert(state: SiteConnectivityState | null | undefined, now = new Date()): boolean {
  if (!state || state.status !== 'blocked' || !state.opened_at) return false;
  const openedAt = new Date(state.opened_at).getTime();
  const overdueAt = openedAt + 4 * 60 * 60 * 1000;
  if (now.getTime() < overdueAt) return false;
  if (!state.last_alerted_at) return true;
  return new Date(state.last_alerted_at).getTime() < overdueAt;
}

export function getNextAllowedRunAt(state: SiteConnectivityState | null | undefined): string | undefined {
  if (!state) return undefined;
  if (state.status === 'blocked' || state.status === 'probing') return state.next_probe_at;
  return undefined;
}
