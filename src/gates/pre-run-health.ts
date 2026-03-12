import { execSync } from 'child_process';
import { checkOCRRuntime } from '../scraper/ocr-runtime';
import { supportedSites, type SupportedSite } from '../sites';

export interface PreRunHealthResult {
  success: boolean;
  errors: string[];
  warnings: string[];
}

type FetchLike = typeof fetch;

export interface PreRunHealthDependencies {
  execSyncImpl?: typeof execSync;
  fetchImpl?: FetchLike;
  env?: NodeJS.ProcessEnv;
  readinessUrl?: string;
  versionUrl?: string;
  sites?: SupportedSite[];
}

type ReadinessPayload = {
  status?: string;
  checks?: Array<{ name?: string; ok?: boolean; detail?: string }>;
  merged_output?: {
    target_reachable?: boolean;
    fallback_active?: boolean;
    detail?: string;
  };
};

const DEFAULT_READINESS_URL = 'http://127.0.0.1:8080/schedule/health';
const DEFAULT_VERSION_URL = 'http://127.0.0.1:8080/version';

export async function preRunHealthCheck(dependencies: PreRunHealthDependencies = {}): Promise<PreRunHealthResult> {
  const errors: string[] = [];
  const warnings: string[] = [];
  const execSyncImpl = dependencies.execSyncImpl ?? execSync;
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const env = dependencies.env ?? process.env;
  const sites = dependencies.sites ?? [...supportedSites];

  checkRuntimeEnv(errors, env, sites);
  checkPlaywrightAvailability(errors, execSyncImpl);
  checkOCRReadiness(errors, warnings, env, sites);
  await checkReadinessEndpoint(errors, warnings, fetchImpl, dependencies.readinessUrl ?? DEFAULT_READINESS_URL);
  await checkVersionEndpoint(errors, warnings, fetchImpl, dependencies.versionUrl ?? DEFAULT_VERSION_URL);

  return {
    success: errors.length === 0,
    errors,
    warnings,
  };
}

function checkRuntimeEnv(errors: string[], env: NodeJS.ProcessEnv, sites: SupportedSite[]): void {
  const required = ['SHEET_ID', 'SHEETS_KEY', 'SCHEDULE_RUN_TOKEN'];
  for (const envVar of required) {
    if (!env[envVar]?.trim()) errors.push(`Required environment variable ${envVar} is not set`);
  }

  const hasBrowserTransport = Boolean(
    env.BRIGHTDATA_BROWSER_WS?.trim() ||
    env.BRIGHTDATA_PROXY_SERVER?.trim() ||
    env.SBR_CDP_URL?.trim()
  );
  if (!hasBrowserTransport) {
    errors.push('One browser transport must be configured: BRIGHTDATA_BROWSER_WS, BRIGHTDATA_PROXY_SERVER, or SBR_CDP_URL');
  }

  const requiredSiteVars: Record<SupportedSite, string[]> = {
    ca_sos: ['SCHEDULE_CA_SOS_WEEKLY_DAYS', 'SCHEDULE_CA_SOS_RUN_HOUR', 'SCHEDULE_CA_SOS_RUN_MINUTE'],
    nyc_acris: [
      'SCHEDULE_NYC_ACRIS_WEEKLY_DAYS',
      'SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR',
      'SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE',
      'SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR',
      'SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE',
    ],
  };

  for (const site of sites) {
    for (const envVar of requiredSiteVars[site]) {
      if (!env[envVar]?.trim()) errors.push(`Required environment variable ${envVar} is not set for ${site}`);
    }
  }

  if (env.SHEETS_KEY?.trim()) {
    try {
      const parsed = JSON.parse(env.SHEETS_KEY.replace(/^'+|'+$/g, '')) as Record<string, unknown>;
      if (!parsed.client_email || !parsed.private_key) {
        errors.push('SHEETS_KEY JSON must include client_email and private_key');
      }
    } catch (error: unknown) {
      errors.push(`SHEETS_KEY is not valid JSON: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}

function checkPlaywrightAvailability(errors: string[], execSyncImpl: typeof execSync): void {
  try {
    execSyncImpl('npx playwright --version', { stdio: 'ignore' });
  } catch {
    errors.push('Playwright is not installed or not functioning properly');
  }
}

function checkOCRReadiness(errors: string[], warnings: string[], env: NodeJS.ProcessEnv, sites: SupportedSite[]): void {
  if (!sites.includes('ca_sos')) return;
  const requireOCR = env.REQUIRE_OCR_TOOLS !== '0';
  const result = checkOCRRuntime({ env });
  if (result.ok) return;

  const message = result.detail ?? `Missing OCR binaries: ${result.missing.join(', ')}`;
  if (requireOCR) errors.push(message);
  else warnings.push(message);
}

async function checkReadinessEndpoint(
  errors: string[],
  warnings: string[],
  fetchImpl: FetchLike,
  readinessUrl: string,
): Promise<void> {
  try {
    const response = await fetchImpl(readinessUrl);
    const raw = await response.text();
    if (!response.ok) {
      errors.push(`Readiness endpoint failed with status ${response.status}`);
      return;
    }

    const payload = JSON.parse(raw) as ReadinessPayload;
    if (payload.status !== 'ready') {
      errors.push(`Readiness endpoint is not ready: ${payload.status ?? 'unknown'}`);
    }

    for (const check of payload.checks ?? []) {
      if (check.ok === false) {
        errors.push(`Readiness check ${check.name ?? 'unknown'} failed${check.detail ? `: ${check.detail}` : ''}`);
      }
    }

    if (payload.merged_output?.target_reachable === false) {
      errors.push(`Merged output target is not reachable${payload.merged_output.detail ? `: ${payload.merged_output.detail}` : ''}`);
    }

    if (payload.merged_output?.fallback_active) {
      warnings.push('Merged output fallback is active; Master is being published to the source workbook');
    }
  } catch (error: unknown) {
    errors.push(`Readiness endpoint check failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function checkVersionEndpoint(
  errors: string[],
  warnings: string[],
  fetchImpl: FetchLike,
  versionUrl: string,
): Promise<void> {
  try {
    const response = await fetchImpl(versionUrl);
    const raw = await response.text();
    if (!response.ok) {
      errors.push(`Version endpoint failed with status ${response.status}`);
      return;
    }

    const payload = JSON.parse(raw) as { git_sha?: string; node_version?: string };
    if (!payload.git_sha || payload.git_sha === 'unknown') {
      warnings.push('Version endpoint did not report a concrete git_sha');
    }
    if (!payload.node_version) {
      warnings.push('Version endpoint did not report node_version');
    }
  } catch (error: unknown) {
    errors.push(`Version endpoint check failed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

export default preRunHealthCheck;
