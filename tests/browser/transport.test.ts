import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';

describe('resolveTransportMode', () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    process.env = { ...originalEnv };
    delete process.env.BRIGHTDATA_BROWSER_WS;
    delete process.env.BRIGHTDATA_PROXY_SERVER;
    delete process.env.SBR_CDP_URL;
  });

  afterEach(() => {
    process.env = { ...originalEnv };
  });

  it('prefers Bright Data Browser API over proxy and legacy SBR', async () => {
    process.env.BRIGHTDATA_BROWSER_WS = 'wss://example-browser';
    process.env.BRIGHTDATA_PROXY_SERVER = 'http://proxy.example';
    process.env.SBR_CDP_URL = 'wss://legacy.example';

    const { resolveTransportMode } = await import('../../src/browser/transport');
    expect(resolveTransportMode()).toBe('brightdata-browser-api');
  });

  it('uses proxy mode when browser api is absent', async () => {
    process.env.BRIGHTDATA_PROXY_SERVER = 'http://proxy.example';
    process.env.SBR_CDP_URL = 'wss://legacy.example';

    const { resolveTransportMode } = await import('../../src/browser/transport');
    expect(resolveTransportMode()).toBe('brightdata-proxy');
  });

  it('falls back to legacy sbr cdp before local mode', async () => {
    process.env.SBR_CDP_URL = 'wss://legacy.example';

    const { resolveTransportMode } = await import('../../src/browser/transport');
    expect(resolveTransportMode()).toBe('legacy-sbr-cdp');
  });
});

describe('createIsolatedBrowserContext', () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    vi.useFakeTimers();
    process.env = { ...originalEnv };
    delete process.env.BRIGHTDATA_PROXY_SERVER;
    delete process.env.SBR_CDP_URL;
    process.env.BRIGHTDATA_BROWSER_WS = 'wss://browser.example';
    process.env.BRIGHTDATA_BROWSER_WS_CONNECT_TIMEOUT_MS = '1234';
    process.env.BRIGHTDATA_BROWSER_WS_CONNECT_RETRIES = '1';
    process.env.BROWSER_TRANSPORT_RETRY_BASE_DELAY_MS = '10';
    process.env.BROWSER_TRANSPORT_RETRY_MAX_DELAY_MS = '10';
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
  });

  it('retries transient Browser API CDP connect timeouts and succeeds', async () => {
    const newContext = vi.fn().mockResolvedValue({ close: vi.fn().mockResolvedValue(undefined) });
    const close = vi.fn().mockResolvedValue(undefined);
    const connectOverCDP = vi
      .fn()
      .mockRejectedValueOnce(new Error('browserType.connectOverCDP: Timeout 30000ms exceeded'))
      .mockResolvedValueOnce({ newContext, close });

    vi.doMock('playwright', () => ({
      chromium: {
        connectOverCDP,
        launch: vi.fn(),
      },
    }));

    const { createIsolatedBrowserContext } = await import('../../src/browser/transport');
    const pending = createIsolatedBrowserContext();
    await vi.runAllTimersAsync();
    const handle = await pending;

    expect(connectOverCDP).toHaveBeenCalledTimes(2);
    expect(connectOverCDP.mock.calls[0][1]).toMatchObject({ timeout: 1234 });
    expect(handle.mode).toBe('brightdata-browser-api');
    expect(newContext).toHaveBeenCalledWith(expect.objectContaining({ acceptDownloads: true }));
  });

  it('does not retry non-transient Browser API CDP failures', async () => {
    const connectOverCDP = vi.fn().mockRejectedValue(new Error('Invalid Browser API credentials'));

    vi.doMock('playwright', () => ({
      chromium: {
        connectOverCDP,
        launch: vi.fn(),
      },
    }));

    const { createIsolatedBrowserContext } = await import('../../src/browser/transport');

    await expect(createIsolatedBrowserContext()).rejects.toThrow('Invalid Browser API credentials');
    expect(connectOverCDP).toHaveBeenCalledTimes(1);
  });
});
