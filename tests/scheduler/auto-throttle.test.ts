import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockScraper = vi.fn();
const mockPushToSheetsForTab = vi.fn();

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
  log: vi.fn(),
}));

vi.mock('../../src/scheduler/store', () => {
  class ScheduledRunStore {
    insertRun(run: any) { runs.set(run.idempotency_key, { ...run }); }
    updateRun(run: any) { runs.set(run.idempotency_key, { ...run }); }
    getByIdempotencyKey(k: string) { return runs.get(k) ?? null; }
    getMostRecentRun() { return null; }
    getSuccessfulRunByIdempotencyKey() { return null; }
    getRunHistory(limit = 50) { return Array.from(runs.values()).slice(0, limit); }
    getRecentSuccessfulRuns(limit = 4) {
      return Array.from(runs.values()).filter((r: any) => r.status === 'success').slice(0, limit);
    }
    upsertControlState(effectiveMaxRecords: number) {
      controlState = { id: 1, effective_max_records: effectiveMaxRecords };
    }
    getControlState() { return controlState; }
    insertMissedAlert() {}
    getMissedAlertByKey() { return null; }
  }

  return { ScheduledRunStore };
});

describe('scheduler auto-throttle', () => {
  beforeEach(() => {
    runs.clear();
    controlState = { id: 1, effective_max_records: 100 };
    vi.clearAllMocks();
    process.env.AMOUNT_MIN_COVERAGE_PCT = '95';
    process.env.SCHEDULE_AUTO_THROTTLE = '1';
  });

  it('reduces effective cap when amount coverage falls below threshold', async () => {
    mockScraper.mockResolvedValueOnce([
      { amount: '100', amount_reason: 'ok' },
      { amount: undefined, amount_reason: 'amount_not_found' },
    ]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 2, tab_title: 'tab-name' });

    const { runScheduledScrape, getScheduleState } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      idempotencyKey: '2026-03-04:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.amount_coverage_pct).toBeLessThan(95);

    const state = getScheduleState();
    expect(state.effective_max_records).toBeLessThan(100);
  });
});
