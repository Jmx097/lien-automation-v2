import Database from 'better-sqlite3';
import { ensureDatabaseReady, resolveDbPath } from '../db/init';
import type { SiteConnectivityState } from './connectivity';
import type { SupportedSite } from '../sites';

export interface ScheduledRunRecord {
  id: string;
  site: SupportedSite;
  idempotency_key: string;
  slot_time: string;
  trigger_source: 'external' | 'manual';
  started_at: string;
  finished_at?: string;
  status: 'running' | 'success' | 'error' | 'deferred';
  records_scraped: number;
  records_skipped: number;
  rows_uploaded: number;
  amount_found_count: number;
  amount_missing_count: number;
  amount_coverage_pct: number;
  ocr_success_pct: number;
  row_fail_pct: number;
  deadline_hit: number;
  effective_max_records: number;
  partial: number;
  error?: string;
  failure_class?: string;
}

interface MissedAlertRecord {
  site: SupportedSite;
  idempotency_key: string;
  slot: 'morning' | 'afternoon';
  expected_by: string;
}

export interface ScheduleControlState {
  site: SupportedSite;
  effective_max_records: number;
  updated_at: string;
}

export class ScheduledRunStore {
  private db: Database.Database;

  constructor() {
    ensureDatabaseReady();
    this.db = new Database(resolveDbPath());
    this.ensureSchema();
  }

  private ensureSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS scheduled_runs (
        id TEXT PRIMARY KEY,
        site TEXT NOT NULL DEFAULT 'ca_sos',
        idempotency_key TEXT NOT NULL UNIQUE,
        slot_time TEXT NOT NULL,
        trigger_source TEXT NOT NULL CHECK(trigger_source IN ('external', 'manual')),
        started_at TEXT NOT NULL,
        finished_at TEXT,
        status TEXT NOT NULL CHECK(status IN ('running', 'success', 'error', 'deferred')),
        records_scraped INTEGER NOT NULL DEFAULT 0,
        records_skipped INTEGER NOT NULL DEFAULT 0,
        rows_uploaded INTEGER NOT NULL DEFAULT 0,
        amount_found_count INTEGER NOT NULL DEFAULT 0,
        amount_missing_count INTEGER NOT NULL DEFAULT 0,
        amount_coverage_pct REAL NOT NULL DEFAULT 0,
        ocr_success_pct REAL NOT NULL DEFAULT 0,
        row_fail_pct REAL NOT NULL DEFAULT 0,
        deadline_hit INTEGER NOT NULL DEFAULT 0,
        effective_max_records INTEGER NOT NULL DEFAULT 0,
        partial INTEGER NOT NULL DEFAULT 0,
        error TEXT,
        failure_class TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_scheduled_runs_started_at ON scheduled_runs(started_at);
      CREATE INDEX IF NOT EXISTS idx_scheduled_runs_status ON scheduled_runs(status);
      CREATE INDEX IF NOT EXISTS idx_scheduled_runs_site_started_at ON scheduled_runs(site, started_at);

      CREATE TABLE IF NOT EXISTS scheduler_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT NOT NULL DEFAULT 'ca_sos',
        idempotency_key TEXT NOT NULL,
        slot TEXT NOT NULL CHECK(slot IN ('morning', 'afternoon')),
        alert_type TEXT NOT NULL CHECK(alert_type IN ('missed_run')),
        expected_by TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(idempotency_key, alert_type)
      );

      CREATE TABLE IF NOT EXISTS scheduler_site_control_state (
        site TEXT PRIMARY KEY,
        effective_max_records INTEGER NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS scheduler_site_connectivity_state (
        site TEXT PRIMARY KEY,
        status TEXT NOT NULL CHECK(status IN ('healthy', 'degraded', 'blocked', 'probing')),
        opened_at TEXT,
        last_success_at TEXT,
        last_failure_at TEXT,
        policy_block_count INTEGER NOT NULL DEFAULT 0,
        timeout_count INTEGER NOT NULL DEFAULT 0,
        empty_result_count INTEGER NOT NULL DEFAULT 0,
        window_started_at TEXT,
        next_probe_at TEXT,
        consecutive_probe_successes INTEGER NOT NULL DEFAULT 0,
        last_failure_reason TEXT,
        last_alerted_at TEXT,
        last_recovery_alert_at TEXT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    const scheduledRunColumns = this.db.prepare("PRAGMA table_info('scheduled_runs')").all() as Array<{ name: string }>;
    if (!scheduledRunColumns.some((column) => column.name === 'site')) {
      this.db.prepare("ALTER TABLE scheduled_runs ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
    }

    const alertColumns = this.db.prepare("PRAGMA table_info('scheduler_alerts')").all() as Array<{ name: string }>;
    if (!alertColumns.some((column) => column.name === 'site')) {
      this.db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
    }

    if (!scheduledRunColumns.some((column) => column.name === 'failure_class')) {
      this.db.prepare("ALTER TABLE scheduled_runs ADD COLUMN failure_class TEXT").run();
    }
  }

  insertRun(run: ScheduledRunRecord): void {
    this.db.prepare(
      `INSERT INTO scheduled_runs (
        id, site, idempotency_key, slot_time, trigger_source, started_at, finished_at, status,
        records_scraped, records_skipped, rows_uploaded,
        amount_found_count, amount_missing_count, amount_coverage_pct, ocr_success_pct, row_fail_pct,
        deadline_hit, effective_max_records, partial, error, failure_class
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      run.id,
      run.site,
      run.idempotency_key,
      run.slot_time,
      run.trigger_source,
      run.started_at,
      run.finished_at ?? null,
      run.status,
      run.records_scraped,
      run.records_skipped,
      run.rows_uploaded,
      run.amount_found_count,
      run.amount_missing_count,
      run.amount_coverage_pct,
      run.ocr_success_pct,
      run.row_fail_pct,
      run.deadline_hit,
      run.effective_max_records,
      run.partial,
      run.error ?? null,
      run.failure_class ?? null
    );
  }

  updateRun(run: ScheduledRunRecord): void {
    this.db.prepare(
      `UPDATE scheduled_runs
       SET site = ?, finished_at = ?, status = ?, records_scraped = ?, records_skipped = ?, rows_uploaded = ?,
           amount_found_count = ?, amount_missing_count = ?, amount_coverage_pct = ?, ocr_success_pct = ?, row_fail_pct = ?,
           deadline_hit = ?, effective_max_records = ?, partial = ?, error = ?, failure_class = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`
    ).run(
      run.site,
      run.finished_at ?? null,
      run.status,
      run.records_scraped,
      run.records_skipped,
      run.rows_uploaded,
      run.amount_found_count,
      run.amount_missing_count,
      run.amount_coverage_pct,
      run.ocr_success_pct,
      run.row_fail_pct,
      run.deadline_hit,
      run.effective_max_records,
      run.partial,
      run.error ?? null,
      run.failure_class ?? null,
      run.id
    );
  }

  getByIdempotencyKey(idempotencyKey: string): ScheduledRunRecord | null {
    const row = this.db
      .prepare('SELECT * FROM scheduled_runs WHERE idempotency_key = ? ORDER BY created_at DESC LIMIT 1')
      .get(idempotencyKey) as ScheduledRunRecord | undefined;
    return row ?? null;
  }

  getMostRecentRun(site?: SupportedSite): ScheduledRunRecord | null {
    const statement = site
      ? this.db.prepare('SELECT * FROM scheduled_runs WHERE site = ? ORDER BY started_at DESC LIMIT 1')
      : this.db.prepare('SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT 1');
    const row = (site ? statement.get(site) : statement.get()) as ScheduledRunRecord | undefined;
    return row ?? null;
  }

  getSuccessfulRunByIdempotencyKey(idempotencyKey: string): ScheduledRunRecord | null {
    const row = this.db
      .prepare("SELECT * FROM scheduled_runs WHERE idempotency_key = ? AND status = 'success' ORDER BY created_at DESC LIMIT 1")
      .get(idempotencyKey) as ScheduledRunRecord | undefined;
    return row ?? null;
  }

  getRunHistory(limit = 50, site?: SupportedSite): ScheduledRunRecord[] {
    if (site) {
      return this.db
        .prepare('SELECT * FROM scheduled_runs WHERE site = ? ORDER BY started_at DESC LIMIT ?')
        .all(site, limit) as ScheduledRunRecord[];
    }

    return this.db.prepare('SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT ?').all(limit) as ScheduledRunRecord[];
  }

  getRecentSuccessfulRuns(site: SupportedSite, limit = 4): ScheduledRunRecord[] {
    return this.db
      .prepare("SELECT * FROM scheduled_runs WHERE site = ? AND status = 'success' ORDER BY started_at DESC LIMIT ?")
      .all(site, limit) as ScheduledRunRecord[];
  }

  upsertControlState(site: SupportedSite, effectiveMaxRecords: number): void {
    this.db.prepare(
      `INSERT INTO scheduler_site_control_state (site, effective_max_records, updated_at)
       VALUES (?, ?, CURRENT_TIMESTAMP)
       ON CONFLICT(site) DO UPDATE SET effective_max_records = excluded.effective_max_records, updated_at = CURRENT_TIMESTAMP`
    ).run(site, effectiveMaxRecords);
  }

  getControlState(site: SupportedSite): ScheduleControlState | null {
    const row = this.db
      .prepare('SELECT * FROM scheduler_site_control_state WHERE site = ?')
      .get(site) as ScheduleControlState | undefined;
    return row ?? null;
  }

  upsertConnectivityState(state: SiteConnectivityState): void {
    this.db.prepare(
      `INSERT INTO scheduler_site_connectivity_state (
        site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
        empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
        last_failure_reason, last_alerted_at, last_recovery_alert_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(site) DO UPDATE SET
        status = excluded.status,
        opened_at = excluded.opened_at,
        last_success_at = excluded.last_success_at,
        last_failure_at = excluded.last_failure_at,
        policy_block_count = excluded.policy_block_count,
        timeout_count = excluded.timeout_count,
        empty_result_count = excluded.empty_result_count,
        window_started_at = excluded.window_started_at,
        next_probe_at = excluded.next_probe_at,
        consecutive_probe_successes = excluded.consecutive_probe_successes,
        last_failure_reason = excluded.last_failure_reason,
        last_alerted_at = excluded.last_alerted_at,
        last_recovery_alert_at = excluded.last_recovery_alert_at,
        updated_at = CURRENT_TIMESTAMP`
    ).run(
      state.site,
      state.status,
      state.opened_at ?? null,
      state.last_success_at ?? null,
      state.last_failure_at ?? null,
      state.policy_block_count,
      state.timeout_count,
      state.empty_result_count,
      state.window_started_at ?? null,
      state.next_probe_at ?? null,
      state.consecutive_probe_successes,
      state.last_failure_reason ?? null,
      state.last_alerted_at ?? null,
      state.last_recovery_alert_at ?? null,
    );
  }

  getConnectivityState(site: SupportedSite): SiteConnectivityState | null {
    const row = this.db.prepare(
      `SELECT site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
              empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
              last_failure_reason, last_alerted_at, last_recovery_alert_at
       FROM scheduler_site_connectivity_state
       WHERE site = ?`
    ).get(site) as SiteConnectivityState | undefined;
    return row ?? null;
  }

  listConnectivityStates(): SiteConnectivityState[] {
    return this.db.prepare(
      `SELECT site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
              empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
              last_failure_reason, last_alerted_at, last_recovery_alert_at
       FROM scheduler_site_connectivity_state
       ORDER BY site`
    ).all() as SiteConnectivityState[];
  }

  insertMissedAlert(alert: MissedAlertRecord): void {
    this.db.prepare(
      `INSERT OR IGNORE INTO scheduler_alerts (site, idempotency_key, slot, expected_by, alert_type)
       VALUES (?, ?, ?, ?, 'missed_run')`
    ).run(alert.site, alert.idempotency_key, alert.slot, alert.expected_by);
  }

  getMissedAlertByKey(idempotencyKey: string): MissedAlertRecord | null {
    const row = this.db
      .prepare("SELECT site, idempotency_key, slot, expected_by FROM scheduler_alerts WHERE idempotency_key = ? AND alert_type = 'missed_run' LIMIT 1")
      .get(idempotencyKey) as MissedAlertRecord | undefined;
    return row ?? null;
  }
}
