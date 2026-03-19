import { chromium, type Browser, type BrowserContext, type BrowserContextOptions, type LaunchOptions } from 'playwright';
import crypto from 'crypto';
import type { SupportedSite } from '../sites';
import { log } from '../utils/logger';
import { redactSecret, sanitizeErrorMessage } from '../utils/redaction';

export type BrowserTransportMode =
  | 'brightdata-browser-api'
  | 'brightdata-proxy'
  | 'legacy-sbr-cdp'
  | 'local';

export interface BrowserContextHandle {
  browser: Browser;
  context: BrowserContext;
  mode: BrowserTransportMode;
  close: () => Promise<void>;
  diagnostics: TransportDiagnostic[];
}

export interface CreateBrowserContextOptions {
  contextOptions?: BrowserContextOptions;
  headless?: boolean;
  site?: SupportedSite;
  purpose?: BrowserTransportPurpose;
  transportModeOverride?: BrowserTransportMode;
}

export type BrowserTransportPurpose = 'execution' | 'diagnostic';

export interface ResolveTransportModeOptions {
  site?: SupportedSite;
  purpose?: BrowserTransportPurpose;
  transportModeOverride?: BrowserTransportMode;
}

export interface TransportDiagnostic {
  stage: 'connect_over_cdp' | 'create_browser_context' | 'close_browser_context';
  status: 'started' | 'succeeded' | 'failed';
  transportMode: BrowserTransportMode;
  attempt: number;
  at: string;
  timeoutMs?: number;
  durationMs?: number;
  detail?: string;
}

const DEFAULT_BROWSER_API_CONNECT_TIMEOUT_MS = Number(process.env.BRIGHTDATA_BROWSER_WS_CONNECT_TIMEOUT_MS ?? '90000');
const DEFAULT_LEGACY_CDP_CONNECT_TIMEOUT_MS = Number(process.env.SBR_CDP_CONNECT_TIMEOUT_MS ?? '45000');
const DEFAULT_BROWSER_API_CONNECT_RETRIES = Number(process.env.BRIGHTDATA_BROWSER_WS_CONNECT_RETRIES ?? '2');
const DEFAULT_LEGACY_CDP_CONNECT_RETRIES = Number(process.env.SBR_CDP_CONNECT_RETRIES ?? '1');
const DEFAULT_CDP_RETRY_BASE_DELAY_MS = Number(process.env.BROWSER_TRANSPORT_RETRY_BASE_DELAY_MS ?? '1000');
const DEFAULT_CDP_RETRY_MAX_DELAY_MS = Number(process.env.BROWSER_TRANSPORT_RETRY_MAX_DELAY_MS ?? '10000');
const DEFAULT_BROWSER_CONTEXT_CREATE_TIMEOUT_MS = Number(process.env.BROWSER_CONTEXT_CREATE_TIMEOUT_MS ?? '30000');

function withSession(url: string): string {
  const sessionId = `session_${Date.now()}_${crypto.randomBytes(8).toString('hex')}`;
  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}session=${sessionId}`;
}

function getProxyOptions(): LaunchOptions['proxy'] | undefined {
  const server = process.env.BRIGHTDATA_PROXY_SERVER?.trim();
  if (!server) return undefined;

  return {
    server,
    username: process.env.BRIGHTDATA_PROXY_USERNAME?.trim(),
    password: process.env.BRIGHTDATA_PROXY_PASSWORD?.trim(),
  };
}

const SITE_TRANSPORT_MODE_ENV_KEYS: Partial<Record<SupportedSite, string[]>> = {
  nyc_acris: ['NYC_ACRIS_TRANSPORT_MODE'],
};

const VALID_TRANSPORT_MODES = new Set<BrowserTransportMode>([
  'brightdata-browser-api',
  'brightdata-proxy',
  'legacy-sbr-cdp',
  'local',
]);

function getEnvTransportModeOverride(site?: SupportedSite): BrowserTransportMode | undefined {
  if (!site) return undefined;

  const sitePrefix = site.toUpperCase().replace(/[^A-Z0-9]/g, '_');
  const envKeys = [`${sitePrefix}_TRANSPORT_MODE`, ...(SITE_TRANSPORT_MODE_ENV_KEYS[site] ?? [])];

  for (const envKey of envKeys) {
    const rawValue = process.env[envKey]?.trim();
    if (!rawValue) continue;
    if (VALID_TRANSPORT_MODES.has(rawValue as BrowserTransportMode)) {
      return rawValue as BrowserTransportMode;
    }

    log({
      stage: 'browser_transport_invalid_site_override',
      site,
      env_key: envKey,
      requested_mode: rawValue,
    });
  }

  return undefined;
}

function resolveGlobalTransportMode(): BrowserTransportMode {
  if (process.env.BRIGHTDATA_BROWSER_WS?.trim()) return 'brightdata-browser-api';
  if (process.env.BRIGHTDATA_PROXY_SERVER?.trim()) return 'brightdata-proxy';
  if (process.env.SBR_CDP_URL?.trim()) return 'legacy-sbr-cdp';
  return 'local';
}

export function resolveTransportMode(options: ResolveTransportModeOptions = {}): BrowserTransportMode {
  const override = options.transportModeOverride;
  if (override) return override;

  const configuredOverride = getEnvTransportModeOverride(options.site);
  if (configuredOverride) return configuredOverride;

  if (options.site === 'nyc_acris' && (options.purpose ?? 'execution') === 'execution' && process.env.SBR_CDP_URL?.trim()) {
    return 'legacy-sbr-cdp';
  }

  return resolveGlobalTransportMode();
}

function getCDPConnectTimeoutMs(mode: Extract<BrowserTransportMode, 'brightdata-browser-api' | 'legacy-sbr-cdp'>): number {
  return mode === 'brightdata-browser-api'
    ? DEFAULT_BROWSER_API_CONNECT_TIMEOUT_MS
    : DEFAULT_LEGACY_CDP_CONNECT_TIMEOUT_MS;
}

function getCDPConnectRetries(mode: Extract<BrowserTransportMode, 'brightdata-browser-api' | 'legacy-sbr-cdp'>): number {
  return mode === 'brightdata-browser-api'
    ? DEFAULT_BROWSER_API_CONNECT_RETRIES
    : DEFAULT_LEGACY_CDP_CONNECT_RETRIES;
}

function shouldRetryCDPConnect(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? '');
  return /connectOverCDP: Timeout|ECONNRESET|ETIMEDOUT|socket hang up|websocket|net::ERR|Target page, context or browser has been closed|502|503|504/i.test(message);
}

function sanitizeEndpoint(endpointURL: string): string {
  try {
    const parsed = new URL(endpointURL);
    const username = parsed.username ? redactSecret(parsed.username) : '';
    const password = parsed.password ? redactSecret(parsed.password) : '';
    parsed.username = username;
    parsed.password = password;
    return parsed.toString();
  } catch {
    return sanitizeErrorMessage(endpointURL);
  }
}

function pushTransportDiagnostic(
  diagnostics: TransportDiagnostic[],
  diagnostic: TransportDiagnostic,
): void {
  diagnostics.push(diagnostic);
  if (diagnostics.length > 20) {
    diagnostics.splice(0, diagnostics.length - 20);
  }
}

async function runTransportStage<T>(
  diagnostics: TransportDiagnostic[],
  options: {
    stage: TransportDiagnostic['stage'];
    transportMode: BrowserTransportMode;
    attempt?: number;
    timeoutMs?: number;
    detail?: string;
  },
  run: () => Promise<T>,
): Promise<T> {
  const attempt = options.attempt ?? 1;
  const startedAt = new Date().toISOString();
  const startedAtMs = Date.now();
  const started: TransportDiagnostic = {
    stage: options.stage,
    status: 'started',
    transportMode: options.transportMode,
    attempt,
    at: startedAt,
    timeoutMs: options.timeoutMs,
    detail: options.detail,
  };
  pushTransportDiagnostic(diagnostics, started);
  log({
    stage: `browser_transport_${options.stage}_started`,
    transport_mode: options.transportMode,
    attempt,
    timeout_ms: options.timeoutMs,
    detail: options.detail,
  });

  let timer: NodeJS.Timeout | undefined;
  try {
    const result = await Promise.race([
      run(),
      ...(options.timeoutMs
        ? [new Promise<T>((_, reject) => {
            timer = setTimeout(() => {
              reject(new Error(`browser transport ${options.stage} timed out after ${options.timeoutMs}ms`));
            }, options.timeoutMs);
          })]
        : []),
    ]);
    const succeeded: TransportDiagnostic = {
      stage: options.stage,
      status: 'succeeded',
      transportMode: options.transportMode,
      attempt,
      at: new Date().toISOString(),
      timeoutMs: options.timeoutMs,
      durationMs: Date.now() - startedAtMs,
      detail: options.detail,
    };
    pushTransportDiagnostic(diagnostics, succeeded);
    log({
      stage: `browser_transport_${options.stage}_succeeded`,
      transport_mode: options.transportMode,
      attempt,
      timeout_ms: options.timeoutMs,
      duration_ms: succeeded.durationMs,
      detail: options.detail,
    });
    return result;
  } catch (error: unknown) {
    const detail = sanitizeErrorMessage(error);
    const failed: TransportDiagnostic = {
      stage: options.stage,
      status: 'failed',
      transportMode: options.transportMode,
      attempt,
      at: new Date().toISOString(),
      timeoutMs: options.timeoutMs,
      durationMs: Date.now() - startedAtMs,
      detail,
    };
    pushTransportDiagnostic(diagnostics, failed);
    log({
      stage: `browser_transport_${options.stage}_failed`,
      transport_mode: options.transportMode,
      attempt,
      timeout_ms: options.timeoutMs,
      duration_ms: failed.durationMs,
      error: detail,
      detail: options.detail,
    });
    throw error;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectOverCDPWithRetry(
  endpointURL: string,
  mode: Extract<BrowserTransportMode, 'brightdata-browser-api' | 'legacy-sbr-cdp'>,
  diagnostics: TransportDiagnostic[],
): Promise<Browser> {
  const timeout = getCDPConnectTimeoutMs(mode);
  const retries = getCDPConnectRetries(mode);
  let lastError: unknown;
  const endpointDetail = sanitizeEndpoint(endpointURL);

  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      return await runTransportStage(
        diagnostics,
        {
          stage: 'connect_over_cdp',
          transportMode: mode,
          attempt,
          timeoutMs: timeout,
          detail: endpointDetail,
        },
        () => chromium.connectOverCDP(endpointURL, { timeout }),
      );
    } catch (error: unknown) {
      lastError = error;
      if (attempt > retries || !shouldRetryCDPConnect(error)) {
        throw error;
      }

      const delayMs = Math.min(DEFAULT_CDP_RETRY_BASE_DELAY_MS * (2 ** (attempt - 1)), DEFAULT_CDP_RETRY_MAX_DELAY_MS);
      log({
        stage: 'browser_transport_connect_retry',
        transport_mode: mode,
        attempt,
        max_attempts: retries + 1,
        delay_ms: delayMs,
        timeout_ms: timeout,
        error: error instanceof Error ? error.message : String(error),
      });
      await sleep(delayMs);
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError ?? 'CDP connect failed'));
}

export async function createIsolatedBrowserContext(options: CreateBrowserContextOptions = {}): Promise<BrowserContextHandle> {
  const mode = resolveTransportMode(options);
  const diagnostics: TransportDiagnostic[] = [];
  const contextOptions: BrowserContextOptions = {
    acceptDownloads: true,
    ...options.contextOptions,
  };

  if (mode === 'brightdata-browser-api') {
    const browser = await connectOverCDPWithRetry(withSession(process.env.BRIGHTDATA_BROWSER_WS!.trim()), mode, diagnostics);
    let context: BrowserContext;
    try {
      context = await runTransportStage(
        diagnostics,
        {
          stage: 'create_browser_context',
          transportMode: mode,
          timeoutMs: DEFAULT_BROWSER_CONTEXT_CREATE_TIMEOUT_MS,
        },
        () => browser.newContext(contextOptions),
      );
    } catch (error: unknown) {
      await browser.close().catch(() => {});
      throw error;
    }
    return {
      browser,
      context,
      mode,
      diagnostics,
      close: async () => {
        await runTransportStage(
          diagnostics,
          {
            stage: 'close_browser_context',
            transportMode: mode,
          },
          async () => {
            await context.close().catch(() => {});
            await browser.close().catch(() => {});
          },
        ).catch(() => {});
      },
    };
  }

  if (mode === 'legacy-sbr-cdp') {
    const browser = await connectOverCDPWithRetry(withSession(process.env.SBR_CDP_URL!.trim()), mode, diagnostics);
    let context: BrowserContext;
    try {
      context = await runTransportStage(
        diagnostics,
        {
          stage: 'create_browser_context',
          transportMode: mode,
          timeoutMs: DEFAULT_BROWSER_CONTEXT_CREATE_TIMEOUT_MS,
        },
        () => browser.newContext(contextOptions),
      );
    } catch (error: unknown) {
      await browser.close().catch(() => {});
      throw error;
    }
    return {
      browser,
      context,
      mode,
      diagnostics,
      close: async () => {
        await runTransportStage(
          diagnostics,
          {
            stage: 'close_browser_context',
            transportMode: mode,
          },
          async () => {
            await context.close().catch(() => {});
            await browser.close().catch(() => {});
          },
        ).catch(() => {});
      },
    };
  }

  const browser = await chromium.launch({
    headless: options.headless ?? process.env.HEADLESS?.toLowerCase() !== 'false',
    proxy: getProxyOptions(),
  });
  const context = await browser.newContext(contextOptions);

  return {
    browser,
    context,
    mode,
    diagnostics,
    close: async () => {
      await runTransportStage(
        diagnostics,
        {
          stage: 'close_browser_context',
          transportMode: mode,
        },
        async () => {
          await context.close().catch(() => {});
          await browser.close().catch(() => {});
        },
      ).catch(() => {});
    },
  };
}
