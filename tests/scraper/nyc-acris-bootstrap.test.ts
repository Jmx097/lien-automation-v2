import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mockCreateIsolatedBrowserContext = vi.fn();
const originalEnv = { ...process.env };

vi.mock('../../src/browser/transport', () => ({
  createIsolatedBrowserContext: mockCreateIsolatedBrowserContext,
}));

type Scenario = {
  finalUrl: string;
  html: string;
  title?: string;
  readyState?: string;
};

type PageFactoryStep =
  | { type: 'page'; scenarios: Scenario[] }
  | { type: 'throw'; message: string };

class FakePage {
  private gotoIndex = 0;
  private current: Scenario;
  private readonly handlers = new Map<string, Array<(...args: any[]) => void>>();

  constructor(private readonly scenarios: Scenario[]) {
    this.current = scenarios[0] ?? {
      finalUrl: 'about:blank',
      html: '',
      title: '',
      readyState: 'unavailable',
    };
  }

  setDefaultTimeout() {}

  on(event: string, handler: (...args: any[]) => void) {
    const handlers = this.handlers.get(event) ?? [];
    handlers.push(handler);
    this.handlers.set(event, handlers);
  }

  mainFrame() {
    return this;
  }

  async goto() {
    const scenario = this.scenarios[Math.min(this.gotoIndex, this.scenarios.length - 1)];
    this.current = scenario;
    this.gotoIndex += 1;
    for (const handler of this.handlers.get('framenavigated') ?? []) {
      handler(this);
    }
  }

  async content() {
    return this.current.html;
  }

  async title() {
    return this.current.title ?? '';
  }

  async evaluate() {
    return this.current.readyState ?? 'complete';
  }

  url() {
    return this.current.finalUrl;
  }

  async close() {}
}

function buildHandle(pageSteps: Array<Scenario[] | PageFactoryStep>, mode = 'legacy-sbr-cdp') {
  let pageIndex = 0;
  return {
    mode,
    diagnostics: [
      {
        stage: 'create_browser_context',
        status: 'succeeded',
        transportMode: mode,
        attempt: 1,
        at: new Date().toISOString(),
        timeoutMs: 30000,
        durationMs: 10,
      },
    ],
    context: {
      on: vi.fn(),
      route: vi.fn().mockResolvedValue(undefined),
      newPage: vi.fn(async () => {
        const step = pageSteps[Math.min(pageIndex, pageSteps.length - 1)];
        pageIndex += 1;
        if ((step as PageFactoryStep)?.type === 'throw') {
          throw new Error((step as Extract<PageFactoryStep, { type: 'throw' }>).message);
        }
        const scenarios = Array.isArray(step) ? step : (step as Extract<PageFactoryStep, { type: 'page' }>).scenarios;
        return new FakePage(scenarios);
      }),
    },
    close: vi.fn().mockResolvedValue(undefined),
  };
}

describe('NYC ACRIS bootstrap recovery', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    vi.useFakeTimers();
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
  });

  it('recovers from an about:blank bootstrap by recreating the page', async () => {
    mockCreateIsolatedBrowserContext
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ], 'brightdata-browser-api'),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            {
              finalUrl: 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentType',
              html: '<html><body><form><input name="__RequestVerificationToken" value="abc123" /></form></body></html>',
              title: 'Search By Document Type',
              readyState: 'complete',
            },
          ],
        ], 'brightdata-browser-api'),
      );

    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity();
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.ok).toBe(true);
    expect(result.recoveryAction).toBe('retry_fresh_context');
    expect(result.bootstrapStrategy).toBe('direct_document_type');
    expect(result.diagnostic?.finalUrl).toContain('/DS/DocumentSearch/DocumentType');
    expect(result.steps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ step: 'bootstrap_create_page', ok: true }),
        expect.objectContaining({ step: 'bootstrap_load_document_type_direct', ok: true }),
      ]),
    );
  });

  it('falls back to direct document type bootstrap after repeated blank index startup', async () => {
    mockCreateIsolatedBrowserContext
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ]),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ]),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            {
              finalUrl: 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentType',
              html: '<html><body><form><input name="__RequestVerificationToken" value="abc123" /></form></body></html>',
              title: 'Search By Document Type',
              readyState: 'complete',
            },
          ],
        ]),
      );

    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity();
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.ok).toBe(false);
    expect(result.recoveryAction).toBe('retry_fresh_context');
    expect(result.bootstrapStrategy).toBe('direct_document_type');
    expect(result.detail).toContain('dead_bootstrap_page about:blank before first navigation');
  });

  it('falls back to a fresh context when retry_new_page hits a closed-context error', async () => {
    mockCreateIsolatedBrowserContext
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
          { type: 'throw', message: 'browserContext.newPage: Target page, context or browser has been closed' },
        ]),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            {
              finalUrl: 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentType',
              html: '<html><body><form><input name="__RequestVerificationToken" value="abc123" /></form></body></html>',
              title: 'Search By Document Type',
              readyState: 'complete',
            },
          ],
        ]),
      );

    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity();
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.ok).toBe(true);
    expect(result.recoveryAction).toBe('none');
    expect(result.bootstrapStrategy).toBe('direct_document_type');
    expect(result.diagnostic?.finalUrl).toContain('/DS/DocumentSearch/DocumentType');
  });

  it('uses fresh-context recovery for repeated blank direct bootstrap sequences', async () => {
    mockCreateIsolatedBrowserContext
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ]),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ]),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ]),
      );

    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity();
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.recoveryAction).toBe('retry_fresh_context');
    expect(result.bootstrapStrategy).toBe('direct_document_type');
  });

  it('fails fast with a dead bootstrap page error after fresh-context retry also stays blank', async () => {
    mockCreateIsolatedBrowserContext
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ], 'brightdata-browser-api'),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ], 'brightdata-browser-api'),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ], 'brightdata-browser-api'),
      );

    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity();
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.ok).toBe(false);
    expect(result.detail).toContain('dead_bootstrap_page about:blank before first navigation');
    expect(result.recoveryAction).toBe('retry_fresh_context');
  });

  it('treats first-attempt blank bootstrap timeouts with no navigation diagnostic as dead bootstrap failures', async () => {
    mockCreateIsolatedBrowserContext
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ], 'brightdata-browser-api'),
      )
      .mockResolvedValueOnce(
        buildHandle([
          [
            {
              finalUrl: 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentType',
              html: '<html><body><form><input name="__RequestVerificationToken" value="abc123" /></form></body></html>',
              title: 'Search By Document Type',
              readyState: 'complete',
            },
          ],
        ], 'brightdata-browser-api'),
      );

    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity();
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.ok).toBe(false);
    expect(result.detail).toContain('dead_bootstrap_page about:blank before first navigation');
    expect(result.recoveryAction).toBe('retry_fresh_context');
    expect(result.bootstrapStrategy).toBe('direct_document_type');
  });

  it('fails probe bootstrap with a bounded stage timeout and step diagnostics', async () => {
    process.env.NYC_ACRIS_PROBE_BOOTSTRAP_TIMEOUT_MS = '5000';
    process.env.NYC_ACRIS_BOOTSTRAP_NEW_PAGE_TIMEOUT_MS = '2000';
    mockCreateIsolatedBrowserContext.mockResolvedValueOnce({
      mode: 'brightdata-browser-api',
      diagnostics: [
        {
          stage: 'create_browser_context',
          status: 'succeeded',
          transportMode: 'brightdata-browser-api',
          attempt: 1,
          at: new Date().toISOString(),
          timeoutMs: 30000,
          durationMs: 10,
        },
      ],
      context: {
        on: vi.fn(),
        route: vi.fn().mockResolvedValue(undefined),
        newPage: vi.fn(async () => new Promise(() => {})),
      },
      close: vi.fn().mockResolvedValue(undefined),
    });

    const stageEvents: Array<{ step: string; status: string }> = [];
    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity({
      onStageEvent: (event) => {
        stageEvents.push({ step: event.step, status: event.status });
      },
    });
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.ok).toBe(false);
    expect(result.detail).toContain('timed out after');
    expect(result.detail).toContain('latest_transport=');
    expect(result.steps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          step: 'probe_bootstrap_search_session',
          ok: false,
          timeoutMs: 5000,
        }),
      ]),
    );
    expect(result.steps).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ step: 'probe_bootstrap_search_session' }),
      ]),
    );
    expect(stageEvents[0]).toEqual({ step: 'probe_bootstrap_search_session', status: 'started' });
    expect(stageEvents).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ status: 'failed' }),
      ]),
    );
  });
});
