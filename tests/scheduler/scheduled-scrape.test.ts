import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockScraper = vi.fn();
const mockPushToSheets = vi.fn();
const mockLog = vi.fn();

const runs = new Map<string, any>();

vi.mock('../../src/scraper/index', () => ({
  scrapers: {
    ca_sos: mockScraper,
  },
}));

vi.mock('../../src/sheets/push', () => ({
  pushToSheets: mockPushToSheets,
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

    getSuccessfulRunByIdempotencyKey(idempotencyKey: string) {
      const run = runs.get(idempotencyKey);
      if (run?.status === 'success') return run;
      return null;
    }

    getRunHistory(limit = 50) {
      return Array.from(runs.values()).slice(0, limit);
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
    vi.clearAllMocks();
  });

  it('uploads scraped records to sheets and records rows_uploaded', async () => {
    mockScraper.mockResolvedValueOnce([{ filing_number: '1' }, { filing_number: '2' }]);
    mockPushToSheets.mockResolvedValueOnce({ uploaded: 2 });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      idempotencyKey: '2026-03-03:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockPushToSheets).toHaveBeenCalledTimes(1);
    expect(mockPushToSheets).toHaveBeenCalledWith([{ filing_number: '1' }, { filing_number: '2' }]);
    expect(result.status).toBe('success');
    expect(result.rows_uploaded).toBe(2);
    expect(result.records_scraped).toBe(2);
  });

  it('fails the run and persists upload error details when sheet upload fails', async () => {
    mockScraper.mockResolvedValueOnce([{ filing_number: '1' }]);
    mockPushToSheets.mockRejectedValueOnce(new Error('Sheets unavailable'));

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      idempotencyKey: '2026-03-03:afternoon',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(mockPushToSheets).toHaveBeenCalledTimes(1);
    expect(result.status).toBe('error');
    expect(result.error).toContain('Sheets unavailable');
    expect(result.rows_uploaded).toBe(0);

    const persisted = runs.get('2026-03-03:afternoon');
    expect(persisted.status).toBe('error');
    expect(persisted.error).toContain('Sheets unavailable');
  });
});
