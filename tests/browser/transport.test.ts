import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';

describe('resolveTransportMode', () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    process.env = { ...originalEnv };
    delete process.env.BRIGHTDATA_BROWSER_WS;
    delete process.env.BRIGHTDATA_PROXY_SERVER;
    delete process.env.SBR_CDP_URL;
    delete process.env.NYC_ACRIS_TRANSPORT_MODE;
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

  it('pins NYC execution to legacy sbr cdp when available', async () => {
    process.env.BRIGHTDATA_BROWSER_WS = 'wss://example-browser';
    process.env.SBR_CDP_URL = 'wss://legacy.example';

    const { resolveTransportMode } = await import('../../src/browser/transport');
    expect(resolveTransportMode({ site: 'nyc_acris', purpose: 'execution' })).toBe('legacy-sbr-cdp');
  });

  it('keeps diagnostic NYC resolution on global precedence for Browser API comparisons', async () => {
    process.env.BRIGHTDATA_BROWSER_WS = 'wss://example-browser';
    process.env.SBR_CDP_URL = 'wss://legacy.example';

    const { resolveTransportMode } = await import('../../src/browser/transport');
    expect(resolveTransportMode({ site: 'nyc_acris', purpose: 'diagnostic' })).toBe('brightdata-browser-api');
  });

  it('honors a valid NYC transport override env var', async () => {
    process.env.BRIGHTDATA_BROWSER_WS = 'wss://example-browser';
    process.env.NYC_ACRIS_TRANSPORT_MODE = 'legacy-sbr-cdp';

    const { resolveTransportMode } = await import('../../src/browser/transport');
    expect(resolveTransportMode({ site: 'nyc_acris', purpose: 'execution' })).toBe('legacy-sbr-cdp');
  });

  it('falls back safely when the NYC transport override env var is invalid', async () => {
    process.env.BRIGHTDATA_BROWSER_WS = 'wss://example-browser';
    process.env.SBR_CDP_URL = 'wss://legacy.example';
    process.env.NYC_ACRIS_TRANSPORT_MODE = 'totally-invalid';

    const { resolveTransportMode } = await import('../../src/browser/transport');
    expect(resolveTransportMode({ site: 'nyc_acris', purpose: 'diagnostic' })).toBe('brightdata-browser-api');
  });
});

describe('createIsolatedBrowserContext', () => {
  const originalEnv = { ...process.env };
  const mockLog = vi.fn();

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    vi.useFakeTimers();
    process.env = { ...originalEnv };
    delete process.env.BRIGHTDATA_PROXY_SERVER;
    delete process.env.SBR_CDP_URL;
    delete process.env.NYC_ACRIS_TRANSPORT_MODE;
    process.env.BRIGHTDATA_BROWSER_WS = 'wss://browser.example';
    process.env.BRIGHTDATA_BROWSER_WS_CONNECT_TIMEOUT_MS = '1234';
    process.env.BRIGHTDATA_BROWSER_WS_CONNECT_RETRIES = '1';
    process.env.BROWSER_TRANSPORT_RETRY_BASE_DELAY_MS = '10';
    process.env.BROWSER_TRANSPORT_RETRY_MAX_DELAY_MS = '10';
    process.env.BROWSER_CONTEXT_CREATE_TIMEOUT_MS = '25';
    vi.doMock('../../src/utils/logger', () => ({
      log: mockLog,
    }));
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
    expect(handle.diagnostics).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ stage: 'connect_over_cdp', status: 'started', attempt: 1, timeoutMs: 1234 }),
        expect.objectContaining({ stage: 'connect_over_cdp', status: 'failed', attempt: 1, timeoutMs: 1234 }),
        expect.objectContaining({ stage: 'connect_over_cdp', status: 'started', attempt: 2, timeoutMs: 1234 }),
        expect.objectContaining({ stage: 'connect_over_cdp', status: 'succeeded', attempt: 2, timeoutMs: 1234 }),
        expect.objectContaining({ stage: 'create_browser_context', status: 'succeeded', attempt: 1, timeoutMs: 25 }),
      ]),
    );
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'browser_transport_connect_over_cdp_failed',
      transport_mode: 'brightdata-browser-api',
      attempt: 1,
      timeout_ms: 1234,
    }));
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'browser_transport_create_browser_context_succeeded',
      transport_mode: 'brightdata-browser-api',
      timeout_ms: 25,
    }));
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
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'browser_transport_connect_over_cdp_failed',
      transport_mode: 'brightdata-browser-api',
      attempt: 1,
      error: 'Invalid Browser API credentials',
    }));
  });

  it('bounds browser context creation hangs and records the failing stage', async () => {
    const close = vi.fn().mockResolvedValue(undefined);
    const connectOverCDP = vi.fn().mockResolvedValue({ newContext: () => new Promise(() => {}), close });

    vi.doMock('playwright', () => ({
      chromium: {
        connectOverCDP,
        launch: vi.fn(),
      },
    }));

    const { createIsolatedBrowserContext } = await import('../../src/browser/transport');
    const pending = createIsolatedBrowserContext();
    const expectation = expect(pending).rejects.toThrow('browser transport create_browser_context timed out after 25ms');
    await vi.advanceTimersByTimeAsync(30);

    await expectation;
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'browser_transport_create_browser_context_failed',
      transport_mode: 'brightdata-browser-api',
      timeout_ms: 25,
      error: 'browser transport create_browser_context timed out after 25ms',
    }));
  });

  it('records successful transport diagnostics for connect and context creation', async () => {
    const context = { close: vi.fn().mockResolvedValue(undefined) };
    const browser = { newContext: vi.fn().mockResolvedValue(context), close: vi.fn().mockResolvedValue(undefined) };
    const connectOverCDP = vi.fn().mockResolvedValue(browser);

    vi.doMock('playwright', () => ({
      chromium: {
        connectOverCDP,
        launch: vi.fn(),
      },
    }));

    const { createIsolatedBrowserContext } = await import('../../src/browser/transport');
    const handle = await createIsolatedBrowserContext();

    expect(handle.diagnostics).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ stage: 'connect_over_cdp', status: 'succeeded', attempt: 1, timeoutMs: 1234 }),
        expect.objectContaining({ stage: 'create_browser_context', status: 'succeeded', attempt: 1, timeoutMs: 25 }),
      ]),
    );
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'browser_transport_connect_over_cdp_succeeded',
      transport_mode: 'brightdata-browser-api',
      attempt: 1,
      timeout_ms: 1234,
    }));
  });

  it('uses NYC execution policy in createIsolatedBrowserContext when both Browser API and legacy are configured', async () => {
    delete process.env.BRIGHTDATA_BROWSER_WS_CONNECT_TIMEOUT_MS;
    delete process.env.BRIGHTDATA_BROWSER_WS_CONNECT_RETRIES;
    process.env.SBR_CDP_URL = 'wss://legacy.example';

    const context = { close: vi.fn().mockResolvedValue(undefined) };
    const browser = { newContext: vi.fn().mockResolvedValue(context), close: vi.fn().mockResolvedValue(undefined) };
    const connectOverCDP = vi.fn().mockResolvedValue(browser);

    vi.doMock('playwright', () => ({
      chromium: {
        connectOverCDP,
        launch: vi.fn(),
      },
    }));

    const { createIsolatedBrowserContext } = await import('../../src/browser/transport');
    const handle = await createIsolatedBrowserContext({ site: 'nyc_acris', purpose: 'execution' });

    expect(handle.mode).toBe('legacy-sbr-cdp');
    expect(connectOverCDP).toHaveBeenCalledWith(expect.stringContaining('wss://legacy.example'), expect.objectContaining({ timeout: 45000 }));
  });
});
