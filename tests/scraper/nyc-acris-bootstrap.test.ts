import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mockCreateIsolatedBrowserContext = vi.fn();

vi.mock('../../src/browser/transport', () => ({
  createIsolatedBrowserContext: mockCreateIsolatedBrowserContext,
}));

type Scenario = {
  finalUrl: string;
  html: string;
  title?: string;
  readyState?: string;
};

class FakePage {
  private gotoIndex = 0;
  private current: Scenario;

  constructor(private readonly scenarios: Scenario[]) {
    this.current = scenarios[0] ?? {
      finalUrl: 'about:blank',
      html: '',
      title: '',
      readyState: 'unavailable',
    };
  }

  setDefaultTimeout() {}

  async goto() {
    const scenario = this.scenarios[Math.min(this.gotoIndex, this.scenarios.length - 1)];
    this.current = scenario;
    this.gotoIndex += 1;
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

function buildHandle(pageScenarios: Scenario[][], mode = 'legacy-sbr-cdp') {
  let pageIndex = 0;
  return {
    mode,
    context: {
      on: vi.fn(),
      route: vi.fn().mockResolvedValue(undefined),
      newPage: vi.fn(async () => {
        const scenarios = pageScenarios[Math.min(pageIndex, pageScenarios.length - 1)];
        pageIndex += 1;
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
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('recovers from an about:blank bootstrap by recreating the page', async () => {
    mockCreateIsolatedBrowserContext.mockResolvedValueOnce(
      buildHandle([
        [
          { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
        ],
        [
          {
            finalUrl: 'https://a836-acris.nyc.gov/DS/DocumentSearch/Index',
            html: '<html><head><script src="/DS/Scripts/Global.js"></script></head><body></body></html>',
            title: 'ACRIS Document Search',
            readyState: 'complete',
          },
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
    expect(result.recoveryAction).toBe('retry_new_page');
    expect(result.diagnostic?.finalUrl).toContain('/DS/DocumentSearch/DocumentType');
  });

  it('classifies exhausted blank bootstrap recovery as transport_or_bootstrap', async () => {
    mockCreateIsolatedBrowserContext
      .mockResolvedValueOnce(
        buildHandle([
          [
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
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
            { finalUrl: 'about:blank', html: '', readyState: 'unavailable' },
          ],
        ]),
      );

    const { probeNYCAcrisConnectivity } = await import('../../src/scraper/nyc_acris');
    const pending = probeNYCAcrisConnectivity();
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result.ok).toBe(false);
    expect(result.failureClass).toBe('transport_or_bootstrap');
    expect(result.recoveryAction).toBe('retry_fresh_context');
    expect(result.diagnostic?.finalUrl).toBe('about:blank');
  });
});
