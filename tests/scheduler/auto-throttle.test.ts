import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockScraper = vi.fn();
const mockProbeCASOSResultCount = vi.fn();
const mockPushToSheetsForTab = vi.fn();
const mockSyncMasterSheetTab = vi.fn();

const runs = new Map<string, any>();
let controlState: any = null;
const connectivityState = new Map<string, any>();

vi.mock('../../src/scraper/index', () => ({
  scrapers: {
    ca_sos: mockScraper,
    maricopa_recorder: mockScraper,
    nyc_acris: mockScraper,
  },
}));

vi.mock('../../src/scraper/ca_sos_enhanced', () => ({
  probeCASOSResultCount: mockProbeCASOSResultCount,
}));

vi.mock('../../src/sheets/push', () => ({
  formatRunTabName: vi.fn(() => 'tab-name'),
  pushToSheetsForTab: mockPushToSheetsForTab,
  syncMasterSheetTab: mockSyncMasterSheetTab,
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
    getRecentSuccessfulRuns(site: string, limit = 4) {
      return Array.from(runs.values()).filter((r: any) => r.status === 'success' && r.site === site).slice(0, limit);
    }
    upsertControlState(site: string, effectiveMaxRecords: number) {
      controlState = { site, effective_max_records: effectiveMaxRecords };
    }
    getControlState(site: string) { return controlState?.site === site ? controlState : null; }
    upsertConnectivityState(state: any) { connectivityState.set(state.site, { ...state }); }
    getConnectivityState(site: string) { return connectivityState.get(site) ?? null; }
    listConnectivityStates() { return Array.from(connectivityState.values()); }
    insertMissedAlert() {}
    getMissedAlertByKey() { return null; }
    insertQualityAnomalyAlert() {}
    getLatestQualityAnomalyAlert() { return null; }
  }

  return { ScheduledRunStore };
});

describe('scheduler auto-throttle', () => {
  beforeEach(() => {
    vi.resetModules();
    runs.clear();
    controlState = { site: 'ca_sos', effective_max_records: 100 };
    connectivityState.clear();
    vi.clearAllMocks();
    mockProbeCASOSResultCount.mockReset();
    mockSyncMasterSheetTab.mockReset();
    mockSyncMasterSheetTab.mockResolvedValue({
      tab_title: 'Master',
      row_count: 2,
      source_tabs: 1,
      target_spreadsheet_id: 'source-sheet',
      fallback_used: false,
    });
    process.env.AMOUNT_MIN_COVERAGE_PCT = '95';
    process.env.SCHEDULE_AUTO_THROTTLE = '1';
    process.env.SCHEDULE_DEADLINE_HOUR = '23';
    process.env.SCHEDULE_DEADLINE_MINUTE = '59';
  });

  it('does not auto-throttle ca_sos after dynamic probing', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(40);
    mockScraper.mockResolvedValueOnce([
      { amount: '100', amount_reason: 'ok' },
      { amount: undefined, amount_reason: 'amount_not_found' },
    ]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 2, tab_title: 'tab-name' });

    const { runScheduledScrape, getScheduleState } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: '2026-03-04:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.amount_coverage_pct).toBeLessThan(95);
    expect(result.effective_max_records).toBe(40);

    const state = await getScheduleState();
    expect(state.ca_sos.effective_max_records).toBe(100);
    expect(state.ca_sos.auto_throttle).toBe(false);
  });

  it('continues auto-throttling nyc_acris when amount coverage falls below threshold', async () => {
    controlState = { site: 'nyc_acris', effective_max_records: 100 };
    mockScraper.mockResolvedValueOnce([
      { amount: '100', amount_reason: 'ok' },
      { amount: undefined, amount_reason: 'amount_not_found' },
    ]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 2, tab_title: 'tab-name' });

    const { runScheduledScrape, getScheduleState } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: '2026-03-04:afternoon',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.amount_coverage_pct).toBeLessThan(95);

    const state = await getScheduleState();
    expect(state.nyc_acris.effective_max_records).toBeLessThan(100);
    expect(state.nyc_acris.auto_throttle).toBe(true);
  });
});
