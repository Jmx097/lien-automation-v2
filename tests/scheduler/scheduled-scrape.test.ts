import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockScraper = vi.fn();
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
    runs.clear();
    controlState = null;
    connectivityState.clear();
    vi.clearAllMocks();
  });

  it('uploads scraped records to sheets and persists quality metrics', async () => {
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
});
