import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ScheduledRunRecord, ScheduledRunStore } from '../../src/scheduler/store';
import type { SiteConnectivityState } from '../../src/scheduler/connectivity';

type StoredRunRow = ScheduledRunRecord & { created_at: string; updated_at: string };

const pgState = {
  runs: new Map<string, StoredRunRow>(),
  control: new Map<string, { site: string; effective_max_records: number; updated_at: string }>(),
  connectivity: new Map<string, SiteConnectivityState>(),
  artifacts: new Map<string, { site: string; artifact_key: string; payload_json: string; updated_at: string }>(),
  alerts: new Map<string, {
    site: string;
    idempotency_key: string;
    slot: 'morning' | 'afternoon';
    expected_by: string;
    alert_type: 'missed_run' | 'quality_anomaly';
    run_id?: string;
    metrics_triggered?: string;
    summary?: string;
    baseline_records_scraped?: number;
    baseline_amount_coverage_pct?: number;
    baseline_ocr_success_pct?: number;
    baseline_row_fail_pct?: number;
    records_scraped?: number;
    amount_coverage_pct?: number;
    ocr_success_pct?: number;
    row_fail_pct?: number;
    detected_at?: string;
    created_at?: string;
  }>(),
  failSelect1: false,
};

function resetPgState(): void {
  pgState.runs.clear();
  pgState.control.clear();
  pgState.connectivity.clear();
  pgState.artifacts.clear();
  pgState.alerts.clear();
  pgState.failSelect1 = false;
}

function nowIso(): string {
  return new Date().toISOString();
}

function sortRuns(rows: StoredRunRow[]): StoredRunRow[] {
  return [...rows].sort((a, b) => new Date(b.started_at).getTime() - new Date(a.started_at).getTime());
}

vi.mock('pg', () => {
  class Pool {
    async connect() {
      return {
        query: this.query.bind(this),
        release() {},
      };
    }

    async end() {}

    async query(sql: string, params: unknown[] = []) {
      const normalized = sql.replace(/\s+/g, ' ').trim();

      if (
        /^BEGIN$|^COMMIT$|^ROLLBACK$/.test(normalized) ||
        normalized.startsWith('CREATE TABLE') ||
        normalized.startsWith('CREATE INDEX') ||
        normalized.startsWith('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS') ||
        normalized.startsWith('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS') ||
        normalized.startsWith('ALTER TABLE scheduler_alerts DROP CONSTRAINT IF EXISTS') ||
        normalized.startsWith('ALTER TABLE scheduler_alerts ADD CONSTRAINT')
      ) {
        return { rows: [] };
      }

      if (normalized === 'SELECT 1') {
        if (pgState.failSelect1) throw new Error('connect ECONNREFUSED');
        return { rows: [{ '?column?': 1 }] };
      }

      if (normalized.startsWith('INSERT INTO scheduled_runs')) {
        const row: StoredRunRow = {
          id: String(params[0]),
          site: String(params[1]) as ScheduledRunRecord['site'],
          idempotency_key: String(params[2]),
          slot_time: String(params[3]),
          trigger_source: String(params[4]) as ScheduledRunRecord['trigger_source'],
          started_at: String(params[5]),
          finished_at: params[6] == null ? undefined : String(params[6]),
          status: String(params[7]) as ScheduledRunRecord['status'],
          records_scraped: Number(params[8]),
          records_skipped: Number(params[9]),
          rows_uploaded: Number(params[10]),
          amount_found_count: Number(params[11]),
          amount_missing_count: Number(params[12]),
          amount_coverage_pct: Number(params[13]),
          ocr_success_pct: Number(params[14]),
          row_fail_pct: Number(params[15]),
          deadline_hit: Number(params[16]),
          effective_max_records: Number(params[17]),
          partial: Number(params[18]),
          error: params[19] == null ? undefined : String(params[19]),
          failure_class: params[20] == null ? undefined : String(params[20]),
          attempt_count: Number(params[21]),
          max_attempts: Number(params[22]),
          retried: Number(params[23]),
          retry_exhausted: Number(params[24]),
          created_at: nowIso(),
          updated_at: nowIso(),
        };
        pgState.runs.set(row.id, row);
        return { rows: [] };
      }

      if (normalized.startsWith('UPDATE scheduled_runs')) {
        const existing = pgState.runs.get(String(params[30]));
        if (existing) {
          pgState.runs.set(existing.id, {
            ...existing,
            site: String(params[0]) as ScheduledRunRecord['site'],
            finished_at: params[1] == null ? undefined : String(params[1]),
            status: String(params[2]) as ScheduledRunRecord['status'],
            records_scraped: Number(params[3]),
            records_skipped: Number(params[4]),
            rows_uploaded: Number(params[5]),
            amount_found_count: Number(params[6]),
            amount_missing_count: Number(params[7]),
            amount_coverage_pct: Number(params[8]),
            ocr_success_pct: Number(params[9]),
            row_fail_pct: Number(params[10]),
            deadline_hit: Number(params[11]),
            effective_max_records: Number(params[12]),
            partial: Number(params[13]),
            error: params[14] == null ? undefined : String(params[14]),
            failure_class: params[15] == null ? undefined : String(params[15]),
            attempt_count: Number(params[16]),
            max_attempts: Number(params[17]),
            retried: Number(params[18]),
            retry_exhausted: Number(params[19]),
            updated_at: nowIso(),
          });
        }
        return { rows: [] };
      }

      if (normalized.includes('FROM scheduled_runs WHERE idempotency_key = $1 ORDER BY created_at DESC LIMIT 1')) {
        const row = sortRuns(Array.from(pgState.runs.values()).filter((run) => run.idempotency_key === params[0]))[0];
        return { rows: row ? [row] : [] };
      }

      if (normalized.includes("FROM scheduled_runs WHERE idempotency_key = $1 AND status = 'success'")) {
        const row = sortRuns(Array.from(pgState.runs.values()).filter((run) => run.idempotency_key === params[0] && run.status === 'success'))[0];
        return { rows: row ? [row] : [] };
      }

      if (normalized.includes('FROM scheduled_runs WHERE site = $1 ORDER BY started_at DESC LIMIT 1')) {
        const row = sortRuns(Array.from(pgState.runs.values()).filter((run) => run.site === params[0]))[0];
        return { rows: row ? [row] : [] };
      }

      if (normalized.includes('FROM scheduled_runs ORDER BY started_at DESC LIMIT 1')) {
        const row = sortRuns(Array.from(pgState.runs.values()))[0];
        return { rows: row ? [row] : [] };
      }

      if (normalized.includes('FROM scheduled_runs WHERE site = $1 ORDER BY started_at DESC LIMIT $2')) {
        const rows = sortRuns(Array.from(pgState.runs.values()).filter((run) => run.site === params[0])).slice(0, Number(params[1]));
        return { rows };
      }

      if (normalized.includes("FROM scheduled_runs WHERE site = $1 AND status = 'success' ORDER BY started_at DESC LIMIT $2")) {
        const rows = sortRuns(Array.from(pgState.runs.values()).filter((run) => run.site === params[0] && run.status === 'success')).slice(0, Number(params[1]));
        return { rows };
      }

      if (normalized.includes('FROM scheduled_runs ORDER BY started_at DESC LIMIT $1')) {
        return { rows: sortRuns(Array.from(pgState.runs.values())).slice(0, Number(params[0])) };
      }

      if (normalized.startsWith('INSERT INTO scheduler_site_control_state')) {
        pgState.control.set(String(params[0]), {
          site: String(params[0]),
          effective_max_records: Number(params[1]),
          updated_at: nowIso(),
        });
        return { rows: [] };
      }

      if (normalized.includes('FROM scheduler_site_control_state WHERE site = $1')) {
        const row = pgState.control.get(String(params[0]));
        return { rows: row ? [row] : [] };
      }

      if (normalized.startsWith('INSERT INTO scheduler_site_connectivity_state')) {
        const state: SiteConnectivityState = {
          site: String(params[0]) as SiteConnectivityState['site'],
          status: String(params[1]) as SiteConnectivityState['status'],
          opened_at: params[2] == null ? undefined : String(params[2]),
          last_success_at: params[3] == null ? undefined : String(params[3]),
          last_failure_at: params[4] == null ? undefined : String(params[4]),
          policy_block_count: Number(params[5]),
          timeout_count: Number(params[6]),
          empty_result_count: Number(params[7]),
          window_started_at: params[8] == null ? undefined : String(params[8]),
          next_probe_at: params[9] == null ? undefined : String(params[9]),
          consecutive_probe_successes: Number(params[10]),
          last_failure_reason: params[11] == null ? undefined : String(params[11]),
          last_alerted_at: params[12] == null ? undefined : String(params[12]),
          last_recovery_alert_at: params[13] == null ? undefined : String(params[13]),
        };
        pgState.connectivity.set(state.site, state);
        return { rows: [] };
      }

      if (normalized.includes('FROM scheduler_site_connectivity_state WHERE site = $1')) {
        const row = pgState.connectivity.get(String(params[0]));
        return { rows: row ? [row] : [] };
      }

      if (normalized.includes('FROM scheduler_site_connectivity_state ORDER BY site')) {
        return { rows: Array.from(pgState.connectivity.values()).sort((a, b) => a.site.localeCompare(b.site)) };
      }

      if (normalized.startsWith('INSERT INTO scheduler_site_artifacts')) {
        const row = {
          site: String(params[0]),
          artifact_key: String(params[1]),
          payload_json: String(params[2]),
          updated_at: nowIso(),
        };
        pgState.artifacts.set(`${row.site}:${row.artifact_key}`, row);
        return { rows: [] };
      }

      if (normalized.includes('FROM scheduler_site_artifacts') && normalized.includes('artifact_key = $2')) {
        const row = pgState.artifacts.get(`${String(params[0])}:${String(params[1])}`);
        return { rows: row ? [row] : [] };
      }

      if (normalized.startsWith('INSERT INTO scheduler_alerts')) {
        const key = `${String(params[1])}:${normalized.includes("'quality_anomaly'") ? 'quality_anomaly' : 'missed_run'}`;
        if (!pgState.alerts.has(key)) {
          pgState.alerts.set(key, {
            site: String(params[0]) as ScheduledRunRecord['site'],
            idempotency_key: String(params[1]),
            slot: String(params[2]) as 'morning' | 'afternoon',
            expected_by: String(params[3]),
            alert_type: normalized.includes("'quality_anomaly'") ? 'quality_anomaly' : 'missed_run',
            run_id: params[4] == null ? undefined : String(params[4]),
            metrics_triggered: params[5] == null ? undefined : String(params[5]),
            summary: params[6] == null ? undefined : String(params[6]),
            baseline_records_scraped: params[7] == null ? undefined : Number(params[7]),
            baseline_amount_coverage_pct: params[8] == null ? undefined : Number(params[8]),
            baseline_ocr_success_pct: params[9] == null ? undefined : Number(params[9]),
            baseline_row_fail_pct: params[10] == null ? undefined : Number(params[10]),
            records_scraped: params[11] == null ? undefined : Number(params[11]),
            amount_coverage_pct: params[12] == null ? undefined : Number(params[12]),
            ocr_success_pct: params[13] == null ? undefined : Number(params[13]),
            row_fail_pct: params[14] == null ? undefined : Number(params[14]),
            detected_at: params[15] == null ? undefined : String(params[15]),
            created_at: nowIso(),
          });
        }
        return { rows: [] };
      }

      if (normalized.includes("FROM scheduler_alerts WHERE idempotency_key = $1 AND alert_type = 'missed_run'")) {
        const row = pgState.alerts.get(`${String(params[0])}:missed_run`);
        return { rows: row ? [row] : [] };
      }

      if (normalized.includes("FROM scheduler_alerts") && normalized.includes("alert_type = 'quality_anomaly'")) {
        const rows = Array.from(pgState.alerts.values())
          .filter((row) => row.site === String(params[0]) && row.alert_type === 'quality_anomaly')
          .sort((a, b) => String(b.detected_at ?? b.created_at ?? '').localeCompare(String(a.detected_at ?? a.created_at ?? '')));
        return { rows: rows[0] ? [rows[0]] : [] };
      }

      if (normalized.includes('FROM scheduler_alerts WHERE idempotency_key = $1')) {
        const row = pgState.alerts.get(`${String(params[0])}:missed_run`);
        return { rows: row ? [row] : [] };
      }

      throw new Error(`Unhandled query in pg mock: ${normalized}`);
    }
  }

  return { Pool };
});

function buildRun(id: string, site: ScheduledRunRecord['site'], startedAt: string, status: ScheduledRunRecord['status'] = 'running'): ScheduledRunRecord {
  return {
    id,
    site,
    idempotency_key: `${site}:${id}`,
    slot_time: `${site}:${id}`,
    trigger_source: 'manual',
    started_at: startedAt,
    status,
    records_scraped: 1,
    records_skipped: 0,
    rows_uploaded: status === 'success' ? 1 : 0,
    amount_found_count: status === 'success' ? 1 : 0,
    amount_missing_count: 0,
    amount_coverage_pct: status === 'success' ? 100 : 0,
    ocr_success_pct: 100,
    row_fail_pct: 0,
    deadline_hit: 0,
    effective_max_records: 5,
    partial: 0,
  };
}

async function exerciseStore(store: ScheduledRunStore): Promise<void> {
  const first = buildRun('run-1', 'nyc_acris', '2026-03-10T12:00:00.000Z');
  const second = buildRun('run-2', 'nyc_acris', '2026-03-11T12:00:00.000Z', 'success');
  const caRun = buildRun('run-3', 'ca_sos', '2026-03-09T12:00:00.000Z', 'success');

  await store.insertRun(first);
  await store.insertRun(second);
  await store.insertRun(caRun);

  expect((await store.getByIdempotencyKey(second.idempotency_key))?.id).toBe(second.id);
  expect((await store.getMostRecentRun('nyc_acris'))?.id).toBe(second.id);
  expect((await store.getSuccessfulRunByIdempotencyKey(second.idempotency_key))?.status).toBe('success');
  expect((await store.getRunHistory(2)).map((run) => run.id)).toEqual(['run-2', 'run-1']);
  expect((await store.getRecentSuccessfulRuns('nyc_acris', 5)).map((run) => run.id)).toEqual(['run-2']);

  second.rows_uploaded = 2;
  second.records_scraped = 2;
  second.finished_at = '2026-03-11T12:30:00.000Z';
  await store.updateRun(second);
  expect((await store.getByIdempotencyKey(second.idempotency_key))?.rows_uploaded).toBe(2);

  await store.upsertControlState('nyc_acris', 7);
  expect((await store.getControlState('nyc_acris'))?.effective_max_records).toBe(7);

  const connectivity: SiteConnectivityState = {
    site: 'nyc_acris',
    status: 'blocked',
    policy_block_count: 2,
    timeout_count: 0,
    empty_result_count: 0,
    consecutive_probe_successes: 0,
    next_probe_at: '2026-03-11T13:00:00.000Z',
    last_failure_reason: 'policy block',
  };
  await store.upsertConnectivityState(connectivity);
  expect((await store.getConnectivityState('nyc_acris'))?.status).toBe('blocked');
  expect((await store.listConnectivityStates()).map((state) => state.site)).toEqual(['nyc_acris']);

  await store.upsertSiteStateArtifact({
    site: 'maricopa_recorder',
    artifact_key: 'session_state',
    payload_json: '{"captured_at":"2026-03-11T12:00:00.000Z"}',
    updated_at: '2026-03-11T12:00:00.000Z',
  });
  expect((await store.getSiteStateArtifact('maricopa_recorder', 'session_state'))?.payload_json)
    .toContain('captured_at');

  await store.insertMissedAlert({
    site: 'nyc_acris',
    idempotency_key: 'nyc_acris:2026-03-11:afternoon',
    slot: 'afternoon',
    expected_by: '2026-03-11T18:45:00.000Z',
  });
  expect((await store.getMissedAlertByKey('nyc_acris:2026-03-11:afternoon'))?.slot).toBe('afternoon');

  await store.insertQualityAnomalyAlert({
    site: 'nyc_acris',
    idempotency_key: second.idempotency_key,
    run_id: second.id,
    slot: 'afternoon',
    metrics_triggered: ['records_scraped'],
    summary: 'Quality anomaly for nyc_acris: records_scraped',
    baseline_records_scraped: 5,
    baseline_amount_coverage_pct: 99,
    baseline_ocr_success_pct: 98,
    baseline_row_fail_pct: 1,
    records_scraped: 2,
    amount_coverage_pct: 50,
    ocr_success_pct: 50,
    row_fail_pct: 60,
    detected_at: '2026-03-11T12:31:00.000Z',
  });
  await store.insertQualityAnomalyAlert({
    site: 'nyc_acris',
    idempotency_key: second.idempotency_key,
    run_id: second.id,
    slot: 'afternoon',
    metrics_triggered: ['records_scraped'],
    summary: 'Quality anomaly for nyc_acris: records_scraped',
    baseline_records_scraped: 5,
    baseline_amount_coverage_pct: 99,
    baseline_ocr_success_pct: 98,
    baseline_row_fail_pct: 1,
    records_scraped: 2,
    amount_coverage_pct: 50,
    ocr_success_pct: 50,
    row_fail_pct: 60,
    detected_at: '2026-03-11T12:31:00.000Z',
  });
  expect((await store.getLatestQualityAnomalyAlert('nyc_acris'))?.run_id).toBe(second.id);

  await store.close();
}

describe('scheduler store contract', () => {
  let sqliteDbPath: string;

  beforeEach(() => {
    vi.resetModules();
    resetPgState();
    sqliteDbPath = path.join(os.tmpdir(), `lien-scheduler-store-${Date.now()}.db`);
    delete process.env.DATABASE_URL;
    process.env.SQLITE_DB_PATH = sqliteDbPath;
  });

  afterEach(() => {
    delete process.env.DATABASE_URL;
    delete process.env.SQLITE_DB_PATH;
    if (sqliteDbPath) fs.rmSync(sqliteDbPath, { force: true });
  });

  it('supports the scheduler contract with sqlite', async () => {
    const { ScheduledRunStore } = await import('../../src/scheduler/store');
    await exerciseStore(new ScheduledRunStore());
  });

  it('supports the scheduler contract with postgres selected by DATABASE_URL', async () => {
    process.env.DATABASE_URL = 'postgres://postgres:postgres@127.0.0.1:5432/lien';
    const { ScheduledRunStore } = await import('../../src/scheduler/store');
    await exerciseStore(new ScheduledRunStore());
  });
});
