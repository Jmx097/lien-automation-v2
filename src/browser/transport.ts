import { chromium, type Browser, type BrowserContext, type BrowserContextOptions, type LaunchOptions } from 'playwright';
import crypto from 'crypto';
import { log } from '../utils/logger';

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
}

export interface CreateBrowserContextOptions {
  contextOptions?: BrowserContextOptions;
  headless?: boolean;
}

const DEFAULT_BROWSER_API_CONNECT_TIMEOUT_MS = Number(process.env.BRIGHTDATA_BROWSER_WS_CONNECT_TIMEOUT_MS ?? '90000');
const DEFAULT_LEGACY_CDP_CONNECT_TIMEOUT_MS = Number(process.env.SBR_CDP_CONNECT_TIMEOUT_MS ?? '45000');
const DEFAULT_BROWSER_API_CONNECT_RETRIES = Number(process.env.BRIGHTDATA_BROWSER_WS_CONNECT_RETRIES ?? '2');
const DEFAULT_LEGACY_CDP_CONNECT_RETRIES = Number(process.env.SBR_CDP_CONNECT_RETRIES ?? '1');
const DEFAULT_CDP_RETRY_BASE_DELAY_MS = Number(process.env.BROWSER_TRANSPORT_RETRY_BASE_DELAY_MS ?? '1000');
const DEFAULT_CDP_RETRY_MAX_DELAY_MS = Number(process.env.BROWSER_TRANSPORT_RETRY_MAX_DELAY_MS ?? '10000');

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

export function resolveTransportMode(): BrowserTransportMode {
  if (process.env.BRIGHTDATA_BROWSER_WS?.trim()) return 'brightdata-browser-api';
  if (process.env.BRIGHTDATA_PROXY_SERVER?.trim()) return 'brightdata-proxy';
  if (process.env.SBR_CDP_URL?.trim()) return 'legacy-sbr-cdp';
  return 'local';
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

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectOverCDPWithRetry(
  endpointURL: string,
  mode: Extract<BrowserTransportMode, 'brightdata-browser-api' | 'legacy-sbr-cdp'>,
): Promise<Browser> {
  const timeout = getCDPConnectTimeoutMs(mode);
  const retries = getCDPConnectRetries(mode);
  let lastError: unknown;

  for (let attempt = 1; attempt <= retries + 1; attempt++) {
    try {
      return await chromium.connectOverCDP(endpointURL, { timeout });
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
  const mode = resolveTransportMode();
  const contextOptions: BrowserContextOptions = {
    acceptDownloads: true,
    ...options.contextOptions,
  };

  if (mode === 'brightdata-browser-api') {
    const browser = await connectOverCDPWithRetry(withSession(process.env.BRIGHTDATA_BROWSER_WS!.trim()), mode);
    const context = await browser.newContext(contextOptions);
    return {
      browser,
      context,
      mode,
      close: async () => {
        await context.close().catch(() => {});
        await browser.close().catch(() => {});
      },
    };
  }

  if (mode === 'legacy-sbr-cdp') {
    const browser = await connectOverCDPWithRetry(withSession(process.env.SBR_CDP_URL!.trim()), mode);
    const context = await browser.newContext(contextOptions);
    return {
      browser,
      context,
      mode,
      close: async () => {
        await context.close().catch(() => {});
        await browser.close().catch(() => {});
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
    close: async () => {
      await context.close().catch(() => {});
      await browser.close().catch(() => {});
    },
  };
}
