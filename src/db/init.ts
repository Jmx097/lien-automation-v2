import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const DEFAULT_DB_PATH = '/tmp/lien.db';

export function resolveDbPath(): string {
  const configured = process.env.SQLITE_DB_PATH?.trim();
  const raw = configured && configured.length > 0 ? configured : DEFAULT_DB_PATH;
  const normalized = path.isAbsolute(raw) ? raw : path.resolve(process.cwd(), raw);

  fs.mkdirSync(path.dirname(normalized), { recursive: true });
  return normalized;
}

export function ensureDatabaseReady(): string {
  const dbPath = resolveDbPath();
  const db = new Database(dbPath);

  db.exec(`
    CREATE TABLE IF NOT EXISTS queue_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fingerprint TEXT UNIQUE NOT NULL,
      site TEXT NOT NULL,
      filingNumber TEXT NOT NULL,
      filingDate TEXT NOT NULL,
      status TEXT DEFAULT 'queued' CHECK(status IN ('queued', 'processing', 'done', 'failed')),
      locked_until DATETIME,
      attempts INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_status ON queue_jobs(status);
    CREATE INDEX IF NOT EXISTS idx_locked ON queue_jobs(locked_until);

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

    CREATE TABLE IF NOT EXISTS scheduler_control_state (
      id INTEGER PRIMARY KEY CHECK(id = 1),
      effective_max_records INTEGER NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  db.close();
  return dbPath;
}
