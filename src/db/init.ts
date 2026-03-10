import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const DEFAULT_DB_PATH = '/tmp/lien.db';

function recreateScheduledRunsTable(db: Database.Database): void {
  const columns = db.prepare("PRAGMA table_info('scheduled_runs')").all() as Array<{ name: string }>;
  const hasColumn = (name: string) => columns.some((column) => column.name === name);

  db.exec(`
    ALTER TABLE scheduled_runs RENAME TO scheduled_runs_legacy;

    CREATE TABLE scheduled_runs (
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
  `);

  db.prepare(
    `INSERT INTO scheduled_runs (
      id, site, idempotency_key, slot_time, trigger_source, started_at, finished_at, status,
      records_scraped, records_skipped, rows_uploaded, amount_found_count, amount_missing_count,
      amount_coverage_pct, ocr_success_pct, row_fail_pct, deadline_hit, effective_max_records,
      partial, error, failure_class, created_at, updated_at
    )
    SELECT
      id,
      ${hasColumn('site') ? 'site' : "'ca_sos'"},
      idempotency_key,
      ${hasColumn('slot_time') ? 'slot_time' : "idempotency_key"},
      trigger_source,
      started_at,
      finished_at,
      status,
      ${hasColumn('records_scraped') ? 'records_scraped' : '0'},
      ${hasColumn('records_skipped') ? 'records_skipped' : '0'},
      ${hasColumn('rows_uploaded') ? 'rows_uploaded' : '0'},
      ${hasColumn('amount_found_count') ? 'amount_found_count' : '0'},
      ${hasColumn('amount_missing_count') ? 'amount_missing_count' : '0'},
      ${hasColumn('amount_coverage_pct') ? 'amount_coverage_pct' : '0'},
      ${hasColumn('ocr_success_pct') ? 'ocr_success_pct' : '0'},
      ${hasColumn('row_fail_pct') ? 'row_fail_pct' : '0'},
      ${hasColumn('deadline_hit') ? 'deadline_hit' : '0'},
      ${hasColumn('effective_max_records') ? 'effective_max_records' : '0'},
      ${hasColumn('partial') ? 'partial' : '0'},
      error,
      ${hasColumn('failure_class') ? 'failure_class' : 'NULL'},
      ${hasColumn('created_at') ? 'created_at' : 'CURRENT_TIMESTAMP'},
      ${hasColumn('updated_at') ? 'updated_at' : 'CURRENT_TIMESTAMP'}
    FROM scheduled_runs_legacy`
  ).run();

  db.exec(`
    DROP TABLE scheduled_runs_legacy;
    CREATE INDEX IF NOT EXISTS idx_scheduled_runs_started_at ON scheduled_runs(started_at);
    CREATE INDEX IF NOT EXISTS idx_scheduled_runs_status ON scheduled_runs(status);
    CREATE INDEX IF NOT EXISTS idx_scheduled_runs_site_started_at ON scheduled_runs(site, started_at);
  `);
}

function migrateScheduledRunsIfNeeded(db: Database.Database): void {
  const table = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'scheduled_runs'").get() as
    | { sql: string | null }
    | undefined;

  if (!table?.sql) return;
  if (table.sql.includes("'deferred'") && table.sql.includes('failure_class')) return;

  recreateScheduledRunsTable(db);
}

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

    CREATE TABLE IF NOT EXISTS scheduler_control_state (
      id INTEGER PRIMARY KEY CHECK(id = 1),
      effective_max_records INTEGER NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
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

  const scheduledRunColumns = db.prepare("PRAGMA table_info('scheduled_runs')").all() as Array<{ name: string }>;
  if (!scheduledRunColumns.some((column) => column.name === 'site')) {
    db.prepare("ALTER TABLE scheduled_runs ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
  }

  const schedulerAlertColumns = db.prepare("PRAGMA table_info('scheduler_alerts')").all() as Array<{ name: string }>;
  if (!schedulerAlertColumns.some((column) => column.name === 'site')) {
    db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
  }

  if (!scheduledRunColumns.some((column) => column.name === 'failure_class')) {
    db.prepare("ALTER TABLE scheduled_runs ADD COLUMN failure_class TEXT").run();
  }

  migrateScheduledRunsIfNeeded(db);

  db.close();
  return dbPath;
}
