import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockScraper = vi.fn();
const mockPushToSheetsForTab = vi.fn();
const mockLog = vi.fn();

const runs = new Map<string, any>();
let controlState: any = null;

vi.mock('../../src/scraper/index', () => ({
  scrapers: {
    ca_sos: mockScraper,
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

    getRecentSuccessfulRuns(limit = 4) {
      return Array.from(runs.values()).filter((r: any) => r.status === 'success').slice(0, limit);
    }

    upsertControlState(effectiveMaxRecords: number) {
      controlState = { id: 1, effective_max_records: effectiveMaxRecords };
    }

    getControlState() {
      return controlState;
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
      idempotencyKey: '2026-03-03:afternoon',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.error).toContain('sheet_upload_mismatch');
  });
});
