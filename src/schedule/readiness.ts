import { checkOCRRuntime } from '../scraper/ocr-runtime';
import { getMaricopaPersistedStateReadiness } from '../scraper/maricopa_artifacts';
import { supportedSites, type SupportedSite } from '../sites';
import { ScheduledRunStore, getSchedulerStoreReadiness } from '../scheduler/store';
import { createDefaultConnectivityState, getNextAllowedRunAt, type SiteConnectivityStatus } from '../scheduler/connectivity';
import { checkSpreadsheetAccess, getMergedSheetTargetConfig } from '../sheets/push';

export interface ReadinessCheck {
  name: string;
  ok: boolean;
  detail?: string;
}

export interface ScheduleReadinessReport {
  status: 'ready' | 'not_ready';
  checks: ReadinessCheck[];
  merged_output: {
    source_spreadsheet_id_suffix: string;
    target_spreadsheet_id_suffix: string;
    target_reachable: boolean;
    fallback_active: boolean;
    detail?: string;
  };
  site_connectivity: Record<SupportedSite, {
    status: SiteConnectivityStatus;
    next_probe_at?: string;
    next_allowed_run_at?: string;
    last_failure_reason?: string;
    last_success_at?: string;
  }>;
  maricopa: {
    artifact_retrieval_enabled: boolean;
    session_present: boolean;
    session_fresh: boolean;
    session_captured_at?: string;
    artifact_candidates_present: boolean;
    artifact_candidate_count: number;
    refresh_required: boolean;
    refresh_reason?: string;
    detail: string;
    last_success_at?: string;
    next_allowed_run_at?: string;
    last_failure_reason?: string;
  };
}

function checkRequiredEnv(): ReadinessCheck {
  const missing = ['SHEET_ID', 'SHEETS_KEY', 'SCHEDULE_RUN_TOKEN'].filter((envVar) => !process.env[envVar]);
  const hasBrowserTransport = Boolean(
    process.env.BRIGHTDATA_BROWSER_WS ||
    process.env.BRIGHTDATA_PROXY_SERVER ||
    process.env.SBR_CDP_URL
  );

  if (missing.length > 0 || !hasBrowserTransport) {
    const details = [...missing];
    if (!hasBrowserTransport) {
      details.push('one of BRIGHTDATA_BROWSER_WS, BRIGHTDATA_PROXY_SERVER, SBR_CDP_URL');
    }
    return {
      name: 'required_env_present',
      ok: false,
      detail: `Missing env vars: ${details.join(', ')}`,
    };
  }

  return {
    name: 'required_env_present',
    ok: true,
  };
}

function checkSiteScheduleConfig(): ReadinessCheck {
  const requiredSiteScheduleVars = [
    'SCHEDULE_CA_SOS_WEEKLY_DAYS',
    'SCHEDULE_CA_SOS_RUN_HOUR',
    'SCHEDULE_CA_SOS_RUN_MINUTE',
    'SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS',
    'SCHEDULE_MARICOPA_RECORDER_RUN_HOUR',
    'SCHEDULE_MARICOPA_RECORDER_RUN_MINUTE',
    'SCHEDULE_NYC_ACRIS_TIMEZONE',
    'SCHEDULE_NYC_ACRIS_WEEKLY_DAYS',
    'SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR',
    'SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE',
    'SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR',
    'SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE',
    'SCHEDULE_NYC_ACRIS_MAX_RECORDS',
  ];
  const missing = requiredSiteScheduleVars.filter((envVar) => !process.env[envVar]);

  if (missing.length > 0) {
    return {
      name: 'site_schedule_configured',
      ok: false,
      detail: `Missing site schedule env vars: ${missing.join(', ')}`,
    };
  }

  return {
    name: 'site_schedule_configured',
    ok: true,
  };
}

function checkDownstreamCredentialsLoaded(): ReadinessCheck {
  const sheetsKeyRaw = process.env.SHEETS_KEY;
  const sheetId = process.env.SHEET_ID;

  if (!sheetsKeyRaw || !sheetId) {
    return {
      name: 'downstream_credentials_loaded',
      ok: false,
      detail: 'SHEETS_KEY and SHEET_ID must both be set.',
    };
  }

  try {
    const parsed = JSON.parse(sheetsKeyRaw.replace(/^'+|'+$/g, '')) as Record<string, unknown>;
    const hasClientEmail = typeof parsed.client_email === 'string' && parsed.client_email.length > 0;
    const hasPrivateKey = typeof parsed.private_key === 'string' && parsed.private_key.length > 0;

    if (!hasClientEmail || !hasPrivateKey) {
      return {
        name: 'downstream_credentials_loaded',
        ok: false,
        detail: 'SHEETS_KEY JSON is missing client_email or private_key.',
      };
    }

    return {
      name: 'downstream_credentials_loaded',
      ok: true,
    };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      name: 'downstream_credentials_loaded',
      ok: false,
      detail: `SHEETS_KEY is not valid JSON: ${message}`,
    };
  }
}

async function checkSourceSheetReachable(): Promise<ReadinessCheck> {
  const sheetId = process.env.SHEET_ID;

  if (!sheetId || !process.env.SHEETS_KEY) {
    return {
      name: 'source_sheet_reachable',
      ok: false,
      detail: 'SHEETS_KEY and SHEET_ID must both be set.',
    };
  }

  const access = await checkSpreadsheetAccess(sheetId);
  return {
    name: 'source_sheet_reachable',
    ok: access.ok,
    detail: access.detail,
  };
}

function checkOCRReady(): ReadinessCheck {
  const result = checkOCRRuntime();
  if (!result.ok) {
    return {
      name: 'ocr_runtime_ready',
      ok: false,
      detail: result.detail ?? `Missing OCR binaries: ${result.missing.join(', ')}`,
    };
  }

  return {
    name: 'ocr_runtime_ready',
    ok: true,
  };
}

export async function getScheduleReadinessReport(): Promise<ScheduleReadinessReport> {
  const storeReadiness = await getSchedulerStoreReadiness();
  const dbCheck: ReadinessCheck = storeReadiness.ok
    ? { name: 'db_reachable', ok: true, detail: `scheduler_store=${storeReadiness.backend}` }
    : { name: 'db_reachable', ok: false, detail: storeReadiness.detail };
  const credentialsCheck = checkDownstreamCredentialsLoaded();
  const sourceSheetCheck = credentialsCheck.ok
    ? await checkSourceSheetReachable()
    : {
      name: 'source_sheet_reachable',
      ok: false,
      detail: 'Skipped because downstream credentials are not loaded.',
    };
  const mergedConfig = getMergedSheetTargetConfig();
  const targetAccess = credentialsCheck.ok
    ? await checkSpreadsheetAccess(mergedConfig.target_spreadsheet_id)
    : { ok: false, detail: 'Skipped because downstream credentials are not loaded.' };
  const checks = [checkRequiredEnv(), checkSiteScheduleConfig(), dbCheck, credentialsCheck, sourceSheetCheck, checkOCRReady()];
  const siteConnectivityEntries = storeReadiness.ok
    ? await (async () => {
      const store = new ScheduledRunStore();
      try {
        return await Promise.all(
          supportedSites.map(async (site) => {
            const state = await store.getConnectivityState(site) ?? createDefaultConnectivityState(site);
            return [site, {
              status: state.status,
              next_probe_at: state.next_probe_at,
              next_allowed_run_at: getNextAllowedRunAt(state),
              last_failure_reason: state.last_failure_reason,
              last_success_at: state.last_success_at,
            }] as const;
          })
        );
      } finally {
        await store.close().catch(() => null);
      }
    })()
    : supportedSites.map((site) => {
      const state = createDefaultConnectivityState(site);
      return [site, {
        status: state.status,
        next_probe_at: state.next_probe_at,
        next_allowed_run_at: getNextAllowedRunAt(state),
        last_failure_reason: state.last_failure_reason,
        last_success_at: state.last_success_at,
      }] as const;
    });
  const site_connectivity = Object.fromEntries(siteConnectivityEntries) as ScheduleReadinessReport['site_connectivity'];
  const maricopaPersistedState = await getMaricopaPersistedStateReadiness();
  const maricopaConnectivity = site_connectivity.maricopa_recorder;
  if (maricopaPersistedState.artifactRetrievalEnabled && maricopaPersistedState.refreshRequired) {
    checks.push({
      name: 'maricopa_persisted_state_ready',
      ok: false,
      detail: maricopaPersistedState.detail,
    });
  } else {
    checks.push({
      name: 'maricopa_persisted_state_ready',
      ok: true,
      detail: maricopaPersistedState.detail,
    });
  }

  return {
    status: checks.every((check) => check.ok) ? 'ready' : 'not_ready',
    checks,
    merged_output: {
      source_spreadsheet_id_suffix: mergedConfig.source_spreadsheet_id.slice(-6),
      target_spreadsheet_id_suffix: mergedConfig.target_spreadsheet_id.slice(-6),
      target_reachable: targetAccess.ok,
      fallback_active: !targetAccess.ok && mergedConfig.target_spreadsheet_id !== mergedConfig.source_spreadsheet_id,
      detail: targetAccess.detail,
    },
    site_connectivity,
    maricopa: {
      artifact_retrieval_enabled: maricopaPersistedState.artifactRetrievalEnabled,
      session_present: maricopaPersistedState.sessionPresent,
      session_fresh: maricopaPersistedState.sessionFresh,
      session_captured_at: maricopaPersistedState.sessionCapturedAt,
      artifact_candidates_present: maricopaPersistedState.artifactCandidatesPresent,
      artifact_candidate_count: maricopaPersistedState.artifactCandidateCount,
      refresh_required: maricopaPersistedState.refreshRequired,
      refresh_reason: maricopaPersistedState.refreshReason,
      detail: maricopaPersistedState.detail,
      last_success_at: maricopaConnectivity.last_success_at,
      next_allowed_run_at: maricopaConnectivity.next_allowed_run_at,
      last_failure_reason: maricopaConnectivity.last_failure_reason,
    },
  };
}
