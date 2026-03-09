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

