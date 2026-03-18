import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockScraper = vi.fn();
const mockProbeCASOSResultCount = vi.fn();
const mockPushToSheetsForTab = vi.fn();
const mockSyncMasterSheetTab = vi.fn();
const runs = new Map<string, any>();
const controlState = new Map<string, any>();
const connectivityState = new Map<string, any>();
const anomalyAlerts = new Map<string, any>();

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
    insertQualityAnomalyAlert(alert: any) { anomalyAlerts.set(`${alert.idempotency_key}:quality_anomaly`, { ...alert }); }
    getLatestQualityAnomalyAlert(site: string) {
      const alerts = Array.from(anomalyAlerts.values()).filter((alert: any) => alert.site === site);
      return alerts[alerts.length - 1] ?? null;
    }
  }

  return { ScheduledRunStore };
});

describe('site-aware scheduler', () => {
  beforeEach(() => {
    vi.resetModules();
    runs.clear();
    controlState.clear();
    connectivityState.clear();
    anomalyAlerts.clear();
    mockScraper.mockReset();
    mockProbeCASOSResultCount.mockReset();
    mockPushToSheetsForTab.mockReset();
    mockSyncMasterSheetTab.mockReset();
    delete process.env.SCHEDULE_NYC_ACRIS_WEEKLY_DAYS;
    delete process.env.SCHEDULE_TARGET_TIMEZONE;
    delete process.env.SCHEDULE_WEEKLY_DAYS;
    delete process.env.SCHEDULE_RUN_HOUR;
    delete process.env.SCHEDULE_RUN_MINUTE;
    delete process.env.SCHEDULE_DEADLINE_HOUR;
    delete process.env.SCHEDULE_DEADLINE_MINUTE;
  });

  it('returns independent next-run config for CA and NYC', async () => {
    const { getNextRuns } = await import('../../src/scheduler');
    const nextRuns = getNextRuns();

    expect(nextRuns).toHaveLength(9);
    expect(nextRuns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ site: 'ca_sos', schedule: 'daily_morning', days: 'MO,TU,WE,TH,FR', run_time: '07:00', trigger_time: '07:00', finish_by_time: '10:00', deadline_time: '10:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'ca_sos', schedule: 'daily_evening', days: 'MO,TU,WE,TH,FR', run_time: '19:00', trigger_time: '19:00', finish_by_time: '22:00', deadline_time: '22:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'maricopa_recorder', schedule: 'daily_afternoon', days: 'MO,TU,WE,TH,FR', run_time: '14:00', trigger_time: '14:00', finish_by_time: '14:00', deadline_time: '14:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'nyc_acris', schedule: 'daily_evening', days: 'MO,TU,WE,TH,FR', run_time: '22:00', trigger_time: '22:00', finish_by_time: '22:00', deadline_time: '22:00', timezone: 'America/Denver' }),
      ])
    );
  });

  it('uses explicit site schedule vars from .env when loaded through dotenv', async () => {
    const envPath = path.resolve(process.cwd(), '.env');
    const parsed = dotenv.parse(fs.readFileSync(envPath, 'utf8'));
    const scheduleKeys = Object.keys(parsed).filter((key) => key.startsWith('SCHEDULE_'));
    const previous = new Map(scheduleKeys.map((key) => [key, process.env[key]]));

    for (const key of scheduleKeys) {
      process.env[key] = parsed[key];
    }

    try {
      const { getNextRuns } = await import('../../src/scheduler');
      const nextRuns = getNextRuns();

      expect(nextRuns).toHaveLength(9);
      expect(nextRuns).toEqual(expect.arrayContaining([
        expect.objectContaining({ site: 'ca_sos', schedule: 'daily_morning', days: 'MO,TU,WE,TH,FR', run_time: '07:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'ca_sos', schedule: 'daily_afternoon', days: 'MO,TU,WE,TH,FR', run_time: '11:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'ca_sos', schedule: 'daily_evening', days: 'MO,TU,WE,TH,FR', run_time: '19:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'maricopa_recorder', schedule: 'daily_morning', days: 'MO,TU,WE,TH,FR', run_time: '10:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'maricopa_recorder', schedule: 'daily_afternoon', days: 'MO,TU,WE,TH,FR', run_time: '14:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'maricopa_recorder', schedule: 'daily_evening', days: 'MO,TU,WE,TH,FR', run_time: '22:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'nyc_acris', schedule: 'daily_morning', days: 'MO,TU,WE,TH,FR', run_time: '10:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'nyc_acris', schedule: 'daily_afternoon', days: 'MO,TU,WE,TH,FR', run_time: '14:00', timezone: 'America/Denver' }),
        expect.objectContaining({ site: 'nyc_acris', schedule: 'daily_evening', days: 'MO,TU,WE,TH,FR', run_time: '22:00', timezone: 'America/Denver' }),
      ]));
    } finally {
      for (const [key, value] of previous.entries()) {
        if (value == null) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });

  it('prefers explicit site schedule vars over legacy globals', async () => {
    process.env.SCHEDULE_TARGET_TIMEZONE = 'America/New_York';
    process.env.SCHEDULE_WEEKLY_DAYS = 'TU,WE';
    process.env.SCHEDULE_RUN_HOUR = '9';
    process.env.SCHEDULE_RUN_MINUTE = '30';
    process.env.SCHEDULE_DEADLINE_HOUR = '13';
    process.env.SCHEDULE_DEADLINE_MINUTE = '15';
    process.env.SCHEDULE_CA_SOS_TIMEZONE = 'America/Denver';
    process.env.SCHEDULE_CA_SOS_WEEKLY_DAYS = 'MO,TU,WE,TH,FR';
    process.env.SCHEDULE_CA_SOS_MORNING_RUN_HOUR = '10';
    process.env.SCHEDULE_CA_SOS_MORNING_RUN_MINUTE = '0';
    process.env.SCHEDULE_CA_SOS_AFTERNOON_RUN_HOUR = '14';
    process.env.SCHEDULE_CA_SOS_AFTERNOON_RUN_MINUTE = '0';
    process.env.SCHEDULE_CA_SOS_EVENING_RUN_HOUR = '22';
    process.env.SCHEDULE_CA_SOS_EVENING_RUN_MINUTE = '0';
    process.env.SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES = '180';

    const { getNextRuns } = await import('../../src/scheduler');
    const caRuns = getNextRuns().filter((run) => run.site === 'ca_sos');

    expect(caRuns).toEqual([
      expect.objectContaining({ schedule: 'daily_morning', days: 'MO,TU,WE,TH,FR', run_time: '07:00', timezone: 'America/Denver' }),
      expect.objectContaining({ schedule: 'daily_afternoon', days: 'MO,TU,WE,TH,FR', run_time: '11:00', timezone: 'America/Denver' }),
      expect.objectContaining({ schedule: 'daily_evening', days: 'MO,TU,WE,TH,FR', run_time: '19:00', timezone: 'America/Denver' }),
    ]);
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
    expect(state.maricopa_recorder).toBeDefined();
    expect(state.nyc_acris).toBeDefined();
    expect(state.ca_sos.latest_anomaly).toBeUndefined();
    expect(state.ca_sos.recent_run_count).toBe(1);
    expect(state.nyc_acris.recent_run_count).toBe(1);
    expect(state.ca_sos.latest_run_started_at).toBeTruthy();
    expect(Array.from(runs.keys())).toEqual(
      expect.arrayContaining(['ca_sos:2026-03-03:morning', 'nyc_acris:2026-03-03:afternoon'])
    );
  });

  it('includes latest anomaly state when present', async () => {
    anomalyAlerts.set('nyc_acris:2026-03-03:afternoon:quality_anomaly', {
      site: 'nyc_acris',
      idempotency_key: 'nyc_acris:2026-03-03:afternoon',
      run_id: 'sched_nyc_1',
      slot: 'afternoon',
      metrics_triggered: ['records_scraped'],
      summary: 'Quality anomaly for nyc_acris: records_scraped',
      detected_at: '2026-03-03T18:00:00.000Z',
    });

    const { getScheduleState } = await import('../../src/scheduler');
    const state = await getScheduleState();

    expect(state.nyc_acris.latest_anomaly).toEqual(expect.objectContaining({
      run_id: 'sched_nyc_1',
      metrics_triggered: ['records_scraped'],
    }));
  });
});
