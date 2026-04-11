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
  queries: [] as string[],
  alerts: new Map<string, {
    site: string;
    idempotency_key: string;
    slot: 'morning' | 'afternoon' | 'evening';
    expected_by: string;
    alert_type: 'missed_run' | 'quality_anomaly' | 'sla_breach' | 'cadence_breach' | 'operational_warning';
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
  pgState.queries = [];
  pgState.failSelect1 = false;
}

function nowIso(): string {
  return new Date().toISOString();
}

function sortRuns(rows: StoredRunRow[]): StoredRunRow[] {
  return [...rows].sort((a, b) => new Date(b.started_at).getTime() - new Date(a.started_at).getTime());
}

function parseColumnList(sql: string, prefix: string, suffix: string): string[] {
  const start = sql.indexOf(prefix);
  const end = sql.indexOf(suffix, start + prefix.length);
  if (start === -1 || end === -1) return [];
  return sql
    .slice(start + prefix.length, end)
    .split(',')
    .map((column) => column.trim())
    .filter(Boolean);
}

function parseAssignmentColumns(sql: string): string[] {
  const start = sql.indexOf('SET ');
  const end = sql.indexOf(', updated_at = NOW()');
  if (start === -1 || end === -1) return [];
  return sql
    .slice(start + 4, end)
    .split(',')
    .map((assignment) => assignment.trim())
    .filter(Boolean)
    .map((assignment) => assignment.split('=')[0]?.trim() ?? '')
    .filter(Boolean);
}

function buildRunFromColumns(columns: string[], params: unknown[]): StoredRunRow {
  const row = Object.fromEntries(columns.map((column, index) => [column, params[index]])) as Record<string, unknown>;
  return {
    id: String(row.id),
    site: String(row.site) as ScheduledRunRecord['site'],
    idempotency_key: String(row.idempotency_key),
    slot_time: String(row.slot_time),
    trigger_source: String(row.trigger_source) as ScheduledRunRecord['trigger_source'],
    started_at: String(row.started_at),
    finished_at: row.finished_at == null ? undefined : String(row.finished_at),
    status: String(row.status) as ScheduledRunRecord['status'],
    records_scraped: Number(row.records_scraped ?? 0),
    records_skipped: Number(row.records_skipped ?? 0),
    rows_uploaded: Number(row.rows_uploaded ?? 0),
    amount_found_count: Number(row.amount_found_count ?? 0),
    amount_missing_count: Number(row.amount_missing_count ?? 0),
    amount_coverage_pct: Number(row.amount_coverage_pct ?? 0),
    ocr_success_pct: Number(row.ocr_success_pct ?? 0),
    row_fail_pct: Number(row.row_fail_pct ?? 0),
    deadline_hit: Number(row.deadline_hit ?? 0),
    effective_max_records: Number(row.effective_max_records ?? 0),
    partial: Number(row.partial ?? 0),
    error: row.error == null ? undefined : String(row.error),
    failure_class: row.failure_class == null ? undefined : String(row.failure_class),
    attempt_count: Number(row.attempt_count ?? 1),
    max_attempts: Number(row.max_attempts ?? 1),
    retried: Number(row.retried ?? 0),
    retry_exhausted: Number(row.retry_exhausted ?? 0),
    source_tab_title: row.source_tab_title == null ? undefined : String(row.source_tab_title),
    master_tab_title: row.master_tab_title == null ? undefined : String(row.master_tab_title),
    review_tab_title: row.review_tab_title == null ? undefined : String(row.review_tab_title),
    quarantined_row_count: Number(row.quarantined_row_count ?? 0),
    current_run_quarantined_row_count: Number(row.current_run_quarantined_row_count ?? 0),
    current_run_conflict_row_count: Number(row.current_run_conflict_row_count ?? 0),
    retained_prior_review_row_count: Number(row.retained_prior_review_row_count ?? 0),
    review_reason_counts_json: row.review_reason_counts_json == null ? undefined : String(row.review_reason_counts_json),
    requested_date_start: row.requested_date_start == null ? undefined : String(row.requested_date_start),
    requested_date_end: row.requested_date_end == null ? undefined : String(row.requested_date_end),
    discovered_count: Number(row.discovered_count ?? 0),
    returned_count: Number(row.returned_count ?? 0),
    filtered_out_count: Number(row.filtered_out_count ?? 0),
    returned_min_filing_date: row.returned_min_filing_date == null ? undefined : String(row.returned_min_filing_date),
    returned_max_filing_date: row.returned_max_filing_date == null ? undefined : String(row.returned_max_filing_date),
    upstream_min_filing_date: row.upstream_min_filing_date == null ? undefined : String(row.upstream_min_filing_date),
    upstream_max_filing_date: row.upstream_max_filing_date == null ? undefined : String(row.upstream_max_filing_date),
    partial_reason: row.partial_reason == null ? undefined : String(row.partial_reason),
    artifact_retrieval_enabled: Number(row.artifact_retrieval_enabled ?? 0),
    enriched_record_count: Number(row.enriched_record_count ?? 0),
    partial_record_count: Number(row.partial_record_count ?? 0),
    new_master_row_count: Number(row.new_master_row_count ?? 0),
    purged_review_row_count: Number(row.purged_review_row_count ?? 0),
    lead_alert_attempted: Number(row.lead_alert_attempted ?? 0),
    lead_alert_delivered: Number(row.lead_alert_delivered ?? 0),
    master_fallback_used: Number(row.master_fallback_used ?? 0),
    anomaly_detected: Number(row.anomaly_detected ?? 0),
    sla_score_pct: Number(row.sla_score_pct ?? 0),
    sla_pass: Number(row.sla_pass ?? 0),
    sla_policy_version: row.sla_policy_version == null ? undefined : String(row.sla_policy_version),
    sla_components_json: row.sla_components_json == null ? undefined : String(row.sla_components_json),
    created_at: nowIso(),
    updated_at: nowIso(),
  };
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
      pgState.queries.push(normalized);

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
        const columns = parseColumnList(normalized, 'INSERT INTO scheduled_runs (', ') VALUES');
        const row = buildRunFromColumns(columns, params);
        pgState.runs.set(row.id, row);
        return { rows: [] };
      }

      if (normalized.startsWith('UPDATE scheduled_runs')) {
        const columns = parseAssignmentColumns(normalized);
        const existing = pgState.runs.get(String(params[params.length - 1]));
        if (existing) {
          const updates = Object.fromEntries(columns.map((column, index) => [column, params[index]]));
          pgState.runs.set(existing.id, {
            ...existing,
            ...updates,
            site: String(updates.site ?? existing.site) as ScheduledRunRecord['site'],
            finished_at: updates.finished_at == null ? undefined : String(updates.finished_at),
            status: String(updates.status ?? existing.status) as ScheduledRunRecord['status'],
            records_scraped: Number(updates.records_scraped ?? existing.records_scraped),
            records_skipped: Number(updates.records_skipped ?? existing.records_skipped),
            rows_uploaded: Number(updates.rows_uploaded ?? existing.rows_uploaded),
            amount_found_count: Number(updates.amount_found_count ?? existing.amount_found_count),
            amount_missing_count: Number(updates.amount_missing_count ?? existing.amount_missing_count),
            amount_coverage_pct: Number(updates.amount_coverage_pct ?? existing.amount_coverage_pct),
            ocr_success_pct: Number(updates.ocr_success_pct ?? existing.ocr_success_pct),
            row_fail_pct: Number(updates.row_fail_pct ?? existing.row_fail_pct),
            deadline_hit: Number(updates.deadline_hit ?? existing.deadline_hit),
            effective_max_records: Number(updates.effective_max_records ?? existing.effective_max_records),
            partial: Number(updates.partial ?? existing.partial),
            error: updates.error == null ? undefined : String(updates.error),
            failure_class: updates.failure_class == null ? undefined : String(updates.failure_class),
            attempt_count: Number(updates.attempt_count ?? existing.attempt_count ?? 1),
            max_attempts: Number(updates.max_attempts ?? existing.max_attempts ?? 1),
            retried: Number(updates.retried ?? existing.retried ?? 0),
            retry_exhausted: Number(updates.retry_exhausted ?? existing.retry_exhausted ?? 0),
            requested_date_start: updates.requested_date_start == null ? undefined : String(updates.requested_date_start),
            requested_date_end: updates.requested_date_end == null ? undefined : String(updates.requested_date_end),
            discovered_count: Number(updates.discovered_count ?? existing.discovered_count ?? 0),
            returned_count: Number(updates.returned_count ?? existing.returned_count ?? 0),
            filtered_out_count: Number(updates.filtered_out_count ?? existing.filtered_out_count ?? 0),
            partial_reason: updates.partial_reason == null ? undefined : String(updates.partial_reason),
            sla_score_pct: Number(updates.sla_score_pct ?? existing.sla_score_pct ?? 0),
            sla_pass: Number(updates.sla_pass ?? existing.sla_pass ?? 0),
            sla_policy_version: updates.sla_policy_version == null
              ? existing.sla_policy_version
              : String(updates.sla_policy_version),
            sla_components_json: updates.sla_components_json == null
              ? existing.sla_components_json
              : String(updates.sla_components_json),
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
        const alertType = (
          normalized.includes("'missed_run'") ? 'missed_run'
            : normalized.includes("'quality_anomaly'") ? 'quality_anomaly'
              : String(params[4])
        ) as 'missed_run' | 'quality_anomaly' | 'sla_breach' | 'cadence_breach' | 'operational_warning';
        const key = `${String(params[1])}:${alertType}`;
        if (!pgState.alerts.has(key)) {
          const usesLiteralAlertType = normalized.includes("'missed_run'") || normalized.includes("'quality_anomaly'");
          pgState.alerts.set(key, {
            site: String(params[0]) as ScheduledRunRecord['site'],
            idempotency_key: String(params[1]),
            slot: String(params[2]) as 'morning' | 'afternoon' | 'evening',
            expected_by: String(params[3]),
            alert_type: alertType,
            run_id: usesLiteralAlertType ? undefined : (params[5] == null ? undefined : String(params[5])),
            metrics_triggered: usesLiteralAlertType ? undefined : (params[6] == null ? undefined : String(params[6])),
            summary: usesLiteralAlertType ? undefined : (params[7] == null ? undefined : String(params[7])),
            baseline_records_scraped: usesLiteralAlertType ? undefined : (params[8] == null ? undefined : Number(params[8])),
            baseline_amount_coverage_pct: usesLiteralAlertType ? undefined : (params[9] == null ? undefined : Number(params[9])),
            baseline_ocr_success_pct: usesLiteralAlertType ? undefined : (params[10] == null ? undefined : Number(params[10])),
            baseline_row_fail_pct: usesLiteralAlertType ? undefined : (params[11] == null ? undefined : Number(params[11])),
            records_scraped: usesLiteralAlertType ? undefined : (params[12] == null ? undefined : Number(params[12])),
            amount_coverage_pct: usesLiteralAlertType ? undefined : (params[13] == null ? undefined : Number(params[13])),
            ocr_success_pct: usesLiteralAlertType ? undefined : (params[14] == null ? undefined : Number(params[14])),
            row_fail_pct: usesLiteralAlertType ? undefined : (params[15] == null ? undefined : Number(params[15])),
            detected_at: usesLiteralAlertType ? undefined : (params[16] == null ? undefined : String(params[16])),
            created_at: nowIso(),
          });
        }
        return { rows: [] };
      }

      if (normalized.includes("FROM scheduler_alerts") && normalized.includes("alert_type = 'quality_anomaly'")) {
        const rows = Array.from(pgState.alerts.values())
          .filter((row) => row.site === String(params[0]) && row.alert_type === 'quality_anomaly')
          .sort((a, b) => String(b.detected_at ?? b.created_at ?? '').localeCompare(String(a.detected_at ?? a.created_at ?? '')));
        return { rows: rows[0] ? [rows[0]] : [] };
      }

      if (normalized.includes('FROM scheduler_alerts WHERE idempotency_key = $1')) {
        const row = pgState.alerts.get(`${String(params[0])}:${String(params[1] ?? 'missed_run')}`);
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
    effective_max_records: 75,
    partial: 0,
    sla_score_pct: status === 'success' ? 100 : 0,
    sla_pass: status === 'success' ? 1 : 0,
    sla_policy_version: 'tri_site_composite_v1',
    sla_components_json: JSON.stringify({
      delivery_pct: status === 'success' ? 100 : 0,
      integrity_pct: status === 'success' ? 100 : 0,
      completeness_pct: status === 'success' ? 100 : 0,
      extraction_pct: status === 'success' ? 100 : 0,
    }),
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
  second.sla_score_pct = 96;
  second.sla_pass = 1;
  await store.updateRun(second);
  expect((await store.getByIdempotencyKey(second.idempotency_key))?.rows_uploaded).toBe(2);
  expect((await store.getByIdempotencyKey(second.idempotency_key))?.sla_score_pct).toBe(96);

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
    idempotency_key: 'nyc_acris:2026-03-11:evening',
    slot: 'evening',
    expected_by: '2026-03-11T22:45:00.000Z',
  });
  expect((await store.getMissedAlertByKey('nyc_acris:2026-03-11:evening'))?.slot).toBe('evening');

  await store.insertSchedulerAlert({
    site: 'nyc_acris',
    idempotency_key: 'nyc_acris:2026-03-11:afternoon',
    slot: 'afternoon',
    alert_type: 'sla_breach',
    expected_by: '2026-03-11T14:00:00.000Z',
    run_id: second.id,
    metrics_triggered: ['completeness_pct'],
    summary: 'SLA breach for nyc_acris afternoon',
    detected_at: '2026-03-11T12:32:00.000Z',
  });
  expect((await store.getAlertByKey('nyc_acris:2026-03-11:afternoon', 'sla_breach'))?.alert_type).toBe('sla_breach');

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
    expect(pgState.queries).toContain('ALTER TABLE scheduler_alerts DROP CONSTRAINT IF EXISTS scheduler_alerts_slot_check');
    expect(pgState.queries).toContain(
      "ALTER TABLE scheduler_alerts ADD CONSTRAINT scheduler_alerts_slot_check CHECK(slot IN ('morning', 'afternoon', 'evening'))",
    );
  });
});
