import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockScraper = vi.fn();
const mockProbeCASOSResultCount = vi.fn();
const mockPushToSheetsForTab = vi.fn();
const runs = new Map<string, any>();
const controlState = new Map<string, any>();
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
  log: vi.fn(),
}));

vi.mock('../../src/scheduler/store', () => {
  class ScheduledRunStore {
    insertRun(run: any) { runs.set(run.idempotency_key, { ...run }); }
    updateRun(run: any) { runs.set(run.idempotency_key, { ...run }); }
    getByIdempotencyKey(key: string) { return runs.get(key) ?? null; }
    getMostRecentRun(site?: string) {
      const items = Array.from(runs.values()).filter((run: any) => !site || run.site === site);
      return items[items.length - 1] ?? null;
    }
    getSuccessfulRunByIdempotencyKey(key: string) {
      const run = runs.get(key);
      return run?.status === 'success' ? run : null;
    }
    getRunHistory(limit = 50) { return Array.from(runs.values()).slice(0, limit); }
    getRecentSuccessfulRuns(site: string, limit = 4) {
      return Array.from(runs.values()).filter((run: any) => run.site === site && run.status === 'success').slice(0, limit);
    }
    upsertControlState(site: string, effectiveMaxRecords: number) {
      controlState.set(site, { site, effective_max_records: effectiveMaxRecords });
    }
    getControlState(site: string) { return controlState.get(site) ?? null; }
    upsertConnectivityState(state: any) { connectivityState.set(state.site, { ...state }); }
    getConnectivityState(site: string) { return connectivityState.get(site) ?? null; }
    listConnectivityStates() { return Array.from(connectivityState.values()); }
    insertMissedAlert() {}
    getMissedAlertByKey() { return null; }
  }

  return { ScheduledRunStore };
});

describe('site-aware scheduler', () => {
  beforeEach(() => {
    vi.resetModules();
    runs.clear();
    controlState.clear();
    connectivityState.clear();
    mockScraper.mockReset();
    mockProbeCASOSResultCount.mockReset();
    mockPushToSheetsForTab.mockReset();
    delete process.env.SCHEDULE_NYC_ACRIS_RUN_HOUR;
    delete process.env.SCHEDULE_NYC_ACRIS_RUN_MINUTE;
    delete process.env.SCHEDULE_NYC_ACRIS_WEEKLY_DAYS;
  });

  it('returns independent next-run config for CA and NYC', async () => {
    const { getNextRuns } = await import('../../src/scheduler');
    const nextRuns = getNextRuns();

    expect(nextRuns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ site: 'ca_sos', days: 'TU,WE', run_time: '06:00', trigger_time: '06:00', finish_by_time: '09:00', deadline_time: '09:00' }),
        expect.objectContaining({ site: 'nyc_acris', days: 'TU,WE,TH,FR', run_time: '14:00', trigger_time: '14:00', finish_by_time: '18:00', deadline_time: '18:00' }),
      ])
    );
  });

  it('namespaces idempotency and control state by site', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValue([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValue({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape, getScheduleState } = await import('../../src/scheduler');
    await runScheduledScrape({ site: 'ca_sos', idempotencyKey: 'ca_sos:2026-03-03:morning', slot: 'morning', triggerSource: 'manual' });
    await runScheduledScrape({ site: 'nyc_acris', idempotencyKey: 'nyc_acris:2026-03-03:afternoon', slot: 'afternoon', triggerSource: 'manual' });

    const state = await getScheduleState();
    expect(state.ca_sos).toBeDefined();
    expect(state.nyc_acris).toBeDefined();
    expect(Array.from(runs.keys())).toEqual(
      expect.arrayContaining(['ca_sos:2026-03-03:morning', 'nyc_acris:2026-03-03:afternoon'])
    );
  });
});
