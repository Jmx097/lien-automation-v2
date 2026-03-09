import { chromium, type Browser, type BrowserContext, type BrowserContextOptions, type LaunchOptions } from 'playwright';
import crypto from 'crypto';

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

export async function createIsolatedBrowserContext(options: CreateBrowserContextOptions = {}): Promise<BrowserContextHandle> {
  const mode = resolveTransportMode();
  const contextOptions: BrowserContextOptions = {
    acceptDownloads: true,
    ...options.contextOptions,
  };

  if (mode === 'brightdata-browser-api') {
    const browser = await chromium.connectOverCDP(withSession(process.env.BRIGHTDATA_BROWSER_WS!.trim()));
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
    const browser = await chromium.connectOverCDP(withSession(process.env.SBR_CDP_URL!.trim()));
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
