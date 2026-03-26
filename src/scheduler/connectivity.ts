import type { SupportedSite } from '../sites';
import type { MaricopaConnectivityFailureClass } from '../scraper/maricopa_recorder';

export type SiteConnectivityStatus = 'healthy' | 'degraded' | 'blocked' | 'probing';

export type NYCAcrisFailureClass =
  | 'policy_block'
  | 'transport_or_bootstrap'
  | 'timeout_or_navigation'
  | 'token_or_session_state'
  | 'selector_or_empty_results'
  | 'viewer_roundtrip'
  | 'range_result_integrity'
  | 'sheet_export';

export type SiteConnectivityFailureClass = NYCAcrisFailureClass | MaricopaConnectivityFailureClass;

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

function siteEnvPrefix(site: SupportedSite): string {
  return site.toUpperCase().replace(/[^A-Z0-9]/g, '_');
}

function getConnectivityEnv(site: SupportedSite, suffix: string): string | undefined {
  const primary = process.env[`${siteEnvPrefix(site)}_${suffix}`];
  if (primary) return primary;

  if (site === 'nyc_acris') {
    return process.env[`NYC_ACRIS_${suffix}`];
  }

  return undefined;
}

export function getConnectivityConfig(site: SupportedSite): ConnectivityConfig {
  return {
    policyFailureThreshold: Number(getConnectivityEnv(site, 'POLICY_FAILURE_THRESHOLD') ?? DEFAULT_CONFIG.policyFailureThreshold),
    transientFailureThreshold: Number(getConnectivityEnv(site, 'TRANSIENT_FAILURE_THRESHOLD') ?? DEFAULT_CONFIG.transientFailureThreshold),
    failureWindowMinutes: Number(getConnectivityEnv(site, 'FAILURE_WINDOW_MINUTES') ?? DEFAULT_CONFIG.failureWindowMinutes),
    circuitCooldownMinutes: Number(getConnectivityEnv(site, 'CIRCUIT_COOLDOWN_MINUTES') ?? DEFAULT_CONFIG.circuitCooldownMinutes),
    probeIntervalMinutes: Number(getConnectivityEnv(site, 'PROBE_INTERVAL_MINUTES') ?? DEFAULT_CONFIG.probeIntervalMinutes),
    probeSuccessesRequired: Number(getConnectivityEnv(site, 'PROBE_SUCCESSES_REQUIRED') ?? DEFAULT_CONFIG.probeSuccessesRequired),
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

  if (/outside requested range|upstream_range=|range integrity/i.test(normalized)) {
    return 'range_result_integrity';
  }

  if (
    /about:blank|chrome-error:\/\/chromewebdata|unexpected_url|net::err|err_|navigation timeout|page not ready/.test(normalized)
  ) {
    return 'transport_or_bootstrap';
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

export function classifyMaricopaFailure(message: string): MaricopaConnectivityFailureClass {
  const normalized = message.toLowerCase();

  if (/sheet_upload_mismatch|sheet export|googleapis|sheets|quota exceeded|read requests per minute/i.test(message)) {
    return 'sheet_export';
  }

  if (/session is stale|session is missing|refresh:maricopa-session|session_missing_or_stale/.test(normalized)) {
    return 'session_missing_or_stale';
  }

  if (/artifact candidates are missing|artifact candidates are stale|discover:maricopa-live|artifact_candidates_missing|artifact_candidates_stale/.test(normalized)) {
    return 'artifact_candidates_missing';
  }

  if (/missing ocr binaries|ocr runtime|ocr_runtime_unavailable/.test(normalized)) {
    return 'ocr_runtime_unavailable';
  }

  if (/challenge|interstitial|cloudflare|blocked html|security check|captcha/.test(normalized)) {
    return 'challenge_or_interstitial';
  }

  return 'artifact_fetch_failed';
}

function isImmediateBlockFailure(site: SupportedSite, failureClass: SiteConnectivityFailureClass): boolean {
  if (site !== 'maricopa_recorder') return false;
  return (
    failureClass === 'session_missing_or_stale' ||
    failureClass === 'artifact_candidates_missing' ||
    failureClass === 'challenge_or_interstitial' ||
    failureClass === 'ocr_runtime_unavailable'
  );
}

export function recordConnectivityFailure(
  currentState: SiteConnectivityState | null | undefined,
  reason: string,
  failureClass: SiteConnectivityFailureClass,
  now = new Date(),
): { state: SiteConnectivityState; becameBlocked: boolean; becameDegraded: boolean } {
  let state = currentState ?? createDefaultConnectivityState('nyc_acris');
  const config = getConnectivityConfig(state.site);

  if (isExpired(state.window_started_at, now, config.failureWindowMinutes)) {
    state = resetRollingCounters(state, now);
  }

  state = {
    ...state,
    last_failure_at: toIso(now),
    last_failure_reason: reason,
    consecutive_probe_successes: 0,
  };

  if (isImmediateBlockFailure(state.site, failureClass)) {
    state.policy_block_count = Math.max(state.policy_block_count, config.policyFailureThreshold);
  } else if (failureClass === 'policy_block') {
    state.policy_block_count += 1;
  } else if (
    failureClass === 'transport_or_bootstrap' ||
    failureClass === 'timeout_or_navigation' ||
    failureClass === 'viewer_roundtrip' ||
    failureClass === 'token_or_session_state' ||
    failureClass === 'range_result_integrity' ||
    failureClass === 'artifact_fetch_failed'
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
  let state = currentState ?? createDefaultConnectivityState('nyc_acris');
  const config = getConnectivityConfig(state.site);
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
