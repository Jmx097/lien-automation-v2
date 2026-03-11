import { checkOCRRuntime } from '../scraper/ocr-runtime';
import { supportedSites, type SupportedSite } from '../sites';
import { ScheduledRunStore, getSchedulerStoreReadiness } from '../scheduler/store';
import { createDefaultConnectivityState, getNextAllowedRunAt, type SiteConnectivityStatus } from '../scheduler/connectivity';

export interface ReadinessCheck {
  name: string;
  ok: boolean;
  detail?: string;
}

export interface ScheduleReadinessReport {
  status: 'ready' | 'not_ready';
  checks: ReadinessCheck[];
  site_connectivity: Record<SupportedSite, {
    status: SiteConnectivityStatus;
    next_probe_at?: string;
    next_allowed_run_at?: string;
    last_failure_reason?: string;
    last_success_at?: string;
  }>;
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
    'SCHEDULE_NYC_ACRIS_TIMEZONE',
    'SCHEDULE_NYC_ACRIS_WEEKLY_DAYS',
    'SCHEDULE_NYC_ACRIS_RUN_HOUR',
    'SCHEDULE_NYC_ACRIS_RUN_MINUTE',
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
  const checks = [checkRequiredEnv(), checkSiteScheduleConfig(), dbCheck, checkDownstreamCredentialsLoaded(), checkOCRReady()];
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

  return {
    status: checks.every((check) => check.ok) ? 'ready' : 'not_ready',
    checks,
    site_connectivity,
  };
}

