const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const dbPath = process.env.SQLITE_DB_PATH && process.env.SQLITE_DB_PATH.trim() ? process.env.SQLITE_DB_PATH : '/tmp/lien.db';
const resolvedDbPath = path.isAbsolute(dbPath) ? dbPath : path.resolve(process.cwd(), dbPath);
fs.mkdirSync(path.dirname(resolvedDbPath), { recursive: true });
const db = new Database(resolvedDbPath, { verbose: console.log });

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

const scheduledRunColumns = db.prepare("PRAGMA table_info('scheduled_runs')").all();
if (!scheduledRunColumns.some((column) => column.name === 'site')) {
  db.prepare("ALTER TABLE scheduled_runs ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
}

const schedulerAlertColumns = db.prepare("PRAGMA table_info('scheduler_alerts')").all();
if (!schedulerAlertColumns.some((column) => column.name === 'site')) {
  db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
}

if (!scheduledRunColumns.some((column) => column.name === 'failure_class')) {
  db.prepare("ALTER TABLE scheduled_runs ADD COLUMN failure_class TEXT").run();
}

db.close();
console.log('DB inited at', resolvedDbPath);

