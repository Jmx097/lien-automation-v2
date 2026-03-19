import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mockProbeNYCAcrisConnectivity = vi.fn();
const mockResolveTransportMode = vi.fn();

vi.mock('../../src/scraper/nyc_acris', () => ({
  probeNYCAcrisConnectivity: mockProbeNYCAcrisConnectivity,
}));

vi.mock('../../src/browser/transport', () => ({
  resolveTransportMode: mockResolveTransportMode,
}));

vi.mock('dotenv', () => ({
  default: {
    config: vi.fn(),
  },
}));

describe('probe-nyc-bootstrap script', () => {
  const originalConsoleLog = console.log;
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    mockResolveTransportMode.mockReturnValue('brightdata-browser-api');
  });

  afterEach(() => {
    console.log = originalConsoleLog;
    process.env = { ...originalEnv };
  });

  it('prints a compact bootstrap probe payload with transport and trace fields', async () => {
    mockProbeNYCAcrisConnectivity.mockResolvedValueOnce({
      ok: false,
      detail: 'dead_bootstrap_page about:blank before first navigation',
      transportMode: 'brightdata-browser-api',
      failureClass: 'transport_or_bootstrap',
      diagnostic: {
        step: 'load_document_type_page_direct',
        attempt: 1,
        kind: 'document_type',
        expectedPath: '/DS/DocumentSearch/DocumentType',
        finalUrl: 'about:blank',
        title: '',
        readyState: 'unavailable',
        htmlLength: 39,
        bodyTextLength: 0,
        hasToken: false,
        hasShellMarker: false,
        hasResultMarker: false,
        hasViewerIframe: false,
        ok: false,
        reason: 'unexpected_url',
      },
      recoveryAction: 'retry_fresh_context',
      bootstrapStrategy: 'direct_document_type',
      steps: [{ step: 'probe_bootstrap_search_session', ok: false }],
      bootstrapTrace: ['bootstrap_page_created url=about:blank'],
      failures: ['dead_bootstrap_page about:blank before first navigation'],
      warnings: ['bootstrap_recovery strategy=direct_document_type recovery=retry_fresh_context'],
    });

    const logSpy = vi.fn();
    console.log = logSpy;

    const { main } = await import('../../scripts/probe-nyc-bootstrap');
    await main();

    expect(logSpy).toHaveBeenCalledTimes(1);
    expect(JSON.parse(logSpy.mock.calls[0][0])).toEqual({
      requestedTransportMode: 'brightdata-browser-api',
      transportMode: 'brightdata-browser-api',
      ok: false,
      detail: 'dead_bootstrap_page about:blank before first navigation',
      failureClass: 'transport_or_bootstrap',
      recoveryAction: 'retry_fresh_context',
      bootstrapStrategy: 'direct_document_type',
      diagnostic: expect.objectContaining({
        finalUrl: 'about:blank',
        reason: 'unexpected_url',
      }),
      steps: [{ step: 'probe_bootstrap_search_session', ok: false }],
      bootstrapTrace: ['bootstrap_page_created url=about:blank'],
      failures: ['dead_bootstrap_page about:blank before first navigation'],
      warnings: ['bootstrap_recovery strategy=direct_document_type recovery=retry_fresh_context'],
    });
    expect(mockResolveTransportMode).toHaveBeenCalledWith({
      site: 'nyc_acris',
      purpose: 'diagnostic',
      transportModeOverride: undefined,
    });
    expect(mockProbeNYCAcrisConnectivity).toHaveBeenCalledWith({
      transportPolicyPurpose: 'diagnostic',
      transportModeOverride: undefined,
    });
  });

  it('honors an explicit probe transport override without changing production defaults', async () => {
    process.env.NYC_ACRIS_PROBE_TRANSPORT_MODE = 'legacy-sbr-cdp';
    mockResolveTransportMode.mockReturnValue('legacy-sbr-cdp');
    mockProbeNYCAcrisConnectivity.mockResolvedValueOnce({
      ok: true,
      transportMode: 'legacy-sbr-cdp',
      recoveryAction: 'none',
      bootstrapStrategy: 'direct_document_type',
    });

    const logSpy = vi.fn();
    console.log = logSpy;

    const { main } = await import('../../scripts/probe-nyc-bootstrap');
    await main();

    expect(mockResolveTransportMode).toHaveBeenCalledWith({
      site: 'nyc_acris',
      purpose: 'diagnostic',
      transportModeOverride: 'legacy-sbr-cdp',
    });
    expect(mockProbeNYCAcrisConnectivity).toHaveBeenCalledWith({
      transportPolicyPurpose: 'diagnostic',
      transportModeOverride: 'legacy-sbr-cdp',
    });
    expect(JSON.parse(logSpy.mock.calls[0][0])).toEqual(
      expect.objectContaining({
        requestedTransportMode: 'legacy-sbr-cdp',
        transportMode: 'legacy-sbr-cdp',
        ok: true,
      }),
    );
  });
});
