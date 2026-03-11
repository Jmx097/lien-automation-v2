import { beforeEach, describe, expect, it, vi } from 'vitest';
import fs from 'fs';
import path from 'path';

const mockScraper = vi.fn();
const mockProbeCASOSResultCount = vi.fn();
const mockPushToSheetsForTab = vi.fn();
const mockLog = vi.fn();

const runs = new Map<string, any>();
let controlState: any = null;
const connectivityState = new Map<string, any>();

vi.mock('../../src/scraper/index', () => ({
  scrapers: {
    ca_sos: mockScraper,
    nyc_acris: mockScraper,
  },
}));

vi.mock('../../src/scraper/ca_sos_enhanced', () => ({
  probeCASOSResultCount: mockProbeCASOSResultCount,
}));

vi.mock('../../src/sheets/push', () => ({
  formatRunTabName: vi.fn(() => 'tab-name'),
  pushToSheetsForTab: mockPushToSheetsForTab,
}));

vi.mock('../../src/utils/logger', () => ({
  log: mockLog,
}));

vi.mock('../../src/scheduler/store', () => {
  class ScheduledRunStore {
    insertRun(run: any) {
      runs.set(run.idempotency_key, { ...run });
    }

    updateRun(run: any) {
      runs.set(run.idempotency_key, { ...run });
    }

    getByIdempotencyKey(idempotencyKey: string) {
      return runs.get(idempotencyKey) ?? null;
    }

    getMostRecentRun() {
      return null;
    }

    getSuccessfulRunByIdempotencyKey(idempotencyKey: string) {
      const run = runs.get(idempotencyKey);
      if (run?.status === 'success') return run;
      return null;
    }

    getRunHistory(limit = 50) {
      return Array.from(runs.values()).slice(0, limit);
    }

    getRecentSuccessfulRuns(site: string, limit = 4) {
      return Array.from(runs.values()).filter((r: any) => r.status === 'success' && r.site === site).slice(0, limit);
    }

    upsertControlState(site: string, effectiveMaxRecords: number) {
      controlState = { site, effective_max_records: effectiveMaxRecords };
    }

    getControlState(site: string) {
      return controlState?.site === site ? controlState : null;
    }

    upsertConnectivityState(state: any) {
      connectivityState.set(state.site, { ...state });
    }

    getConnectivityState(site: string) {
      return connectivityState.get(site) ?? null;
    }

    listConnectivityStates() {
      return Array.from(connectivityState.values());
    }

    insertMissedAlert() {}

    getMissedAlertByKey() {
      return null;
    }
  }

  return {
    ScheduledRunStore,
  };
});

describe('runScheduledScrape', () => {
  beforeEach(() => {
    vi.resetModules();
    runs.clear();
    controlState = null;
    connectivityState.clear();
    vi.clearAllMocks();
    mockProbeCASOSResultCount.mockReset();
    fs.rmSync(path.join(process.cwd(), 'out', 'acris', 'scheduled-cache'), { recursive: true, force: true });
    process.env.SCHEDULE_RUN_MAX_ATTEMPTS = '3';
    process.env.SCHEDULE_RUN_BASE_DELAY_MS = '0';
    process.env.SCHEDULE_RUN_MAX_DELAY_MS = '0';
  });

  it('uploads scraped records to sheets and persists quality metrics', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(2);
    mockScraper.mockResolvedValueOnce([
      { filing_number: '1', amount: '100', amount_reason: 'ok' },
      { filing_number: '2', amount: '200', amount_reason: 'ok' },
    ]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 2, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: '2026-03-03:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(1);
    expect(result.status).toBe('success');
    expect(result.rows_uploaded).toBe(2);
    expect(result.records_scraped).toBe(2);
    expect(result.amount_found_count).toBe(2);
    expect(result.amount_coverage_pct).toBeGreaterThan(90);
  });

  it('fails the run when sheet upload count mismatches', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 0, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: '2026-03-03:afternoon',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.error).toContain('sheet_upload_mismatch');
  });

  it('uses the probed CA result count as the scheduled max_records', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(31);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-03:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockProbeCASOSResultCount).toHaveBeenCalledTimes(1);
    expect(mockScraper).toHaveBeenCalledWith(expect.objectContaining({ max_records: 31 }));
  });

  it('short-circuits CA scheduled runs when the probe finds zero results', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(0);

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-10:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.records_scraped).toBe(0);
    expect(mockScraper).not.toHaveBeenCalled();
    expect(mockPushToSheetsForTab).not.toHaveBeenCalled();
  });

  it('falls back to the seeded cap when the CA probe fails', async () => {
    controlState = { site: 'ca_sos', effective_max_records: 55 };
    mockProbeCASOSResultCount.mockRejectedValueOnce(new Error('probe failed'));
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-11:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockScraper).toHaveBeenCalledWith(expect.objectContaining({ max_records: 55 }));
  });

  it('defers blocked nyc scheduled runs before scraping', async () => {
    connectivityState.set('nyc_acris', {
      site: 'nyc_acris',
      status: 'blocked',
      policy_block_count: 2,
      timeout_count: 0,
      empty_result_count: 0,
      consecutive_probe_successes: 0,
      next_probe_at: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
      last_failure_reason: 'Requested URL is restricted in accordance with robots.txt (brob)',
    });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-03:afternoon',
      slot: 'afternoon',
      triggerSource: 'external',
    });

    expect(result.status).toBe('deferred');
    expect(mockScraper).not.toHaveBeenCalled();
    expect(result.failure_class).toBe('policy_block');
  });

  it('reuses cached nyc rows when the previous failure was sheet export', async () => {
    const cacheDir = path.join(process.cwd(), 'out', 'acris', 'scheduled-cache');
    fs.mkdirSync(cacheDir, { recursive: true });
    fs.writeFileSync(
      path.join(cacheDir, 'nyc_acris_cached-sheet-retry_afternoon.json'),
      JSON.stringify([{ filing_number: '1', amount: '100', amount_reason: 'ok' }], null, 2),
      'utf8'
    );

    runs.set('nyc_acris:cached-sheet-retry:afternoon', {
      id: 'prior-run',
      site: 'nyc_acris',
      idempotency_key: 'nyc_acris:cached-sheet-retry:afternoon',
      status: 'error',
      failure_class: 'sheet_export',
    });
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:cached-sheet-retry:afternoon',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(mockScraper).not.toHaveBeenCalled();
    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(1);
  });

  it('retries transient scraper failures and succeeds within the run budget', async () => {
    mockScraper
      .mockRejectedValueOnce(new Error('viewer did not return to acris result page'))
      .mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:retry-success',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.retried).toBe(1);
    expect(result.attempt_count).toBe(2);
    expect(mockScraper).toHaveBeenCalledTimes(2);
  });

  it('retries transient sheet export failures by reusing cached nyc rows', async () => {
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab
      .mockRejectedValueOnce(new Error('googleapis sheets 503'))
      .mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:sheet-retry',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.retried).toBe(1);
    expect(result.attempt_count).toBe(2);
    expect(mockScraper).toHaveBeenCalledTimes(1);
    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(2);
  });

  it('does not retry non-retryable selector failures', async () => {
    mockScraper.mockRejectedValueOnce(new Error('No ACRIS rows found during live selector validation'));

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:selector-fail',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.failure_class).toBe('selector_or_empty_results');
    expect(result.retry_exhausted).toBe(0);
    expect(mockScraper).toHaveBeenCalledTimes(1);
  });

  it('marks retry exhaustion after repeated transient failures', async () => {
    process.env.SCHEDULE_RUN_MAX_ATTEMPTS = '2';
    mockScraper
      .mockRejectedValueOnce(new Error('viewer did not return to acris result page'))
      .mockRejectedValueOnce(new Error('viewer did not return to acris result page'));

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:retry-exhausted',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.failure_class).toBe('viewer_roundtrip');
    expect(result.retry_exhausted).toBe(1);
    expect(result.attempt_count).toBe(2);
    expect(mockScraper).toHaveBeenCalledTimes(2);
  });
});
