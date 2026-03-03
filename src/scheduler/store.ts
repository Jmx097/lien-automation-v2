import Database from 'better-sqlite3';
import path from 'path';

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
  error?: string;
}

interface MissedAlertRecord {
  idempotency_key: string;
  slot: 'morning' | 'afternoon';
  expected_by: string;
}

export class ScheduledRunStore {
  private db: Database.Database;

  constructor() {
    const dbPath = path.join(process.cwd(), 'data/db/lien-queue.db');
    this.db = new Database(dbPath);
    this.ensureSchema();
  }

  private ensureSchema(): void {
    const columns = this.db.prepare("PRAGMA table_info('scheduled_runs')").all() as Array<{ name: string }>;
    const hasRowsUploaded = columns.some((column) => column.name === 'rows_uploaded');
    const hasSlotTime = columns.some((column) => column.name === 'slot_time');
    const hasLegacySlot = columns.some((column) => column.name === 'slot');

    if (!hasRowsUploaded) {
      this.db.prepare('ALTER TABLE scheduled_runs ADD COLUMN rows_uploaded INTEGER NOT NULL DEFAULT 0').run();
    }

    if (!hasSlotTime) {
      this.db.prepare("ALTER TABLE scheduled_runs ADD COLUMN slot_time TEXT NOT NULL DEFAULT ''").run();
      if (hasLegacySlot) {
        this.db.prepare("UPDATE scheduled_runs SET slot_time = idempotency_key WHERE slot_time = '' OR slot_time IS NULL").run();
      }
    }
  }

  insertRun(run: ScheduledRunRecord): void {
    this.db
      .prepare(
        `INSERT INTO scheduled_runs (
          id, idempotency_key, slot_time, trigger_source, started_at, finished_at, status,
          records_scraped, records_skipped, rows_uploaded, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
        run.error ?? null
      );
  }

  updateRun(run: ScheduledRunRecord): void {
    this.db
      .prepare(
        `UPDATE scheduled_runs
         SET finished_at = ?, status = ?, records_scraped = ?, records_skipped = ?, rows_uploaded = ?, error = ?, updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`
      )
      .run(run.finished_at ?? null, run.status, run.records_scraped, run.records_skipped, run.rows_uploaded, run.error ?? null, run.id);
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
    const rows = this.db.prepare('SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT ?').all(limit) as ScheduledRunRecord[];
    return rows;
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
