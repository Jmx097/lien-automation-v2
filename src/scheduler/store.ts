import Database from 'better-sqlite3';
import { ensureDatabaseReady, resolveDbPath } from '../db/init';

export interface ScheduledRunRecord {
  id: string;
  idempotency_key: string;
  slot_time: string;
  trigger_source: 'external' | 'manual';
  started_at: string;
  finished_at?: string;
  status: 'running' | 'success' | 'error';
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
}

interface MissedAlertRecord {
  idempotency_key: string;
  slot: 'morning' | 'afternoon';
  expected_by: string;
}

export interface ScheduleControlState {
  id: number;
  effective_max_records: number;
  updated_at: string;
}

export class ScheduledRunStore {
  private db: Database.Database;

  constructor() {
    ensureDatabaseReady();
    const dbPath = resolveDbPath();
    this.db = new Database(dbPath);
    this.ensureSchema();
  }

  private ensureSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS scheduled_runs (
        id TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE,
        slot_time TEXT NOT NULL,
        trigger_source TEXT NOT NULL CHECK(trigger_source IN ('external', 'manual')),
        started_at TEXT NOT NULL,
        finished_at TEXT,
        status TEXT NOT NULL CHECK(status IN ('running', 'success', 'error')),
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
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_scheduled_runs_started_at ON scheduled_runs(started_at);
      CREATE INDEX IF NOT EXISTS idx_scheduled_runs_status ON scheduled_runs(status);

      CREATE TABLE IF NOT EXISTS scheduler_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        idempotency_key TEXT NOT NULL,
        slot TEXT NOT NULL CHECK(slot IN ('morning', 'afternoon')),
        alert_type TEXT NOT NULL CHECK(alert_type IN ('missed_run')),
        expected_by TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(idempotency_key, alert_type)
      );
    `);
    const columns = this.db.prepare("PRAGMA table_info('scheduled_runs')").all() as Array<{ name: string }>;
    const has = (name: string) => columns.some((column) => column.name === name);

    const maybeAdd = (name: string, sqlType: string, defaultValue: string) => {
      if (!has(name)) {
        this.db.prepare(`ALTER TABLE scheduled_runs ADD COLUMN ${name} ${sqlType} NOT NULL DEFAULT ${defaultValue}`).run();
      }
    };

    maybeAdd('rows_uploaded', 'INTEGER', '0');
    maybeAdd('amount_found_count', 'INTEGER', '0');
    maybeAdd('amount_missing_count', 'INTEGER', '0');
    maybeAdd('amount_coverage_pct', 'REAL', '0');
    maybeAdd('ocr_success_pct', 'REAL', '0');
    maybeAdd('row_fail_pct', 'REAL', '0');
    maybeAdd('deadline_hit', 'INTEGER', '0');
    maybeAdd('effective_max_records', 'INTEGER', '0');
    maybeAdd('partial', 'INTEGER', '0');

    if (!has('slot_time')) {
      this.db.prepare("ALTER TABLE scheduled_runs ADD COLUMN slot_time TEXT NOT NULL DEFAULT ''").run();
      this.db.prepare("UPDATE scheduled_runs SET slot_time = idempotency_key WHERE slot_time = '' OR slot_time IS NULL").run();
    }

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS scheduler_control_state (
        id INTEGER PRIMARY KEY CHECK(id = 1),
        effective_max_records INTEGER NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);
  }

  insertRun(run: ScheduledRunRecord): void {
    this.db
      .prepare(
        `INSERT INTO scheduled_runs (
          id, idempotency_key, slot_time, trigger_source, started_at, finished_at, status,
          records_scraped, records_skipped, rows_uploaded,
          amount_found_count, amount_missing_count, amount_coverage_pct, ocr_success_pct, row_fail_pct,
          deadline_hit, effective_max_records, partial,
          error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        run.id,
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
        run.error ?? null
      );
  }

  updateRun(run: ScheduledRunRecord): void {
    this.db
      .prepare(
        `UPDATE scheduled_runs
         SET finished_at = ?, status = ?, records_scraped = ?, records_skipped = ?, rows_uploaded = ?,
             amount_found_count = ?, amount_missing_count = ?, amount_coverage_pct = ?, ocr_success_pct = ?, row_fail_pct = ?,
             deadline_hit = ?, effective_max_records = ?, partial = ?, error = ?, updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`
      )
      .run(
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
        run.id
      );
  }

  getByIdempotencyKey(idempotencyKey: string): ScheduledRunRecord | null {
    const row = this.db
      .prepare('SELECT * FROM scheduled_runs WHERE idempotency_key = ? ORDER BY created_at DESC LIMIT 1')
      .get(idempotencyKey) as ScheduledRunRecord | undefined;
    return row ?? null;
  }

  getMostRecentRun(): ScheduledRunRecord | null {
    const row = this.db.prepare('SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT 1').get() as ScheduledRunRecord | undefined;
    return row ?? null;
  }

  getSuccessfulRunByIdempotencyKey(idempotencyKey: string): ScheduledRunRecord | null {
    const row = this.db
      .prepare("SELECT * FROM scheduled_runs WHERE idempotency_key = ? AND status = 'success' ORDER BY created_at DESC LIMIT 1")
      .get(idempotencyKey) as ScheduledRunRecord | undefined;
    return row ?? null;
  }

  getRunHistory(limit = 50): ScheduledRunRecord[] {
    return this.db.prepare('SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT ?').all(limit) as ScheduledRunRecord[];
  }

  getRecentSuccessfulRuns(limit = 4): ScheduledRunRecord[] {
    return this.db
      .prepare("SELECT * FROM scheduled_runs WHERE status = 'success' ORDER BY started_at DESC LIMIT ?")
      .all(limit) as ScheduledRunRecord[];
  }

  upsertControlState(effectiveMaxRecords: number): void {
    this.db
      .prepare(
        `INSERT INTO scheduler_control_state (id, effective_max_records, updated_at)
         VALUES (1, ?, CURRENT_TIMESTAMP)
         ON CONFLICT(id) DO UPDATE SET effective_max_records = excluded.effective_max_records, updated_at = CURRENT_TIMESTAMP`
      )
      .run(effectiveMaxRecords);
  }

  getControlState(): ScheduleControlState | null {
    const row = this.db.prepare('SELECT * FROM scheduler_control_state WHERE id = 1').get() as ScheduleControlState | undefined;
    return row ?? null;
  }

  insertMissedAlert(alert: MissedAlertRecord): void {
    this.db
      .prepare(
        `INSERT OR IGNORE INTO scheduler_alerts (idempotency_key, slot, expected_by, alert_type)
         VALUES (?, ?, ?, 'missed_run')`
      )
      .run(alert.idempotency_key, alert.slot, alert.expected_by);
  }

  getMissedAlertByKey(idempotencyKey: string): MissedAlertRecord | null {
    const row = this.db
      .prepare("SELECT idempotency_key, slot, expected_by FROM scheduler_alerts WHERE idempotency_key = ? AND alert_type = 'missed_run' LIMIT 1")
      .get(idempotencyKey) as MissedAlertRecord | undefined;
    return row ?? null;
  }
}


