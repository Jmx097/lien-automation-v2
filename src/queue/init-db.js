const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(process.cwd(), 'data/db/lien-queue.db');
const db = new Database(dbPath, { verbose: console.log });

db.exec(`
  CREATE TABLE IF NOT EXISTS queue_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT UNIQUE NOT NULL,
    site TEXT NOT NULL,
    filingNumber TEXT NOT NULL,
    filingDate TEXT NOT NULL,
    status TEXT DEFAULT 'queued' CHECK(status IN ('queued', 'processing', 'done', 'failed')),
    status_new TEXT DEFAULT 'pending' CHECK(status_new IN ('pending', 'in_progress', 'done', 'failed')),
    chunk_id TEXT,
    error_code TEXT,
    retry_count INTEGER DEFAULT 0,
    last_attempt_at DATETIME,
    completed_at DATETIME,
    locked_until DATETIME,
    attempts INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_status ON queue_jobs(status);
  CREATE INDEX IF NOT EXISTS idx_locked ON queue_jobs(locked_until);
  CREATE INDEX IF NOT EXISTS idx_queue_chunk_id ON queue_jobs(chunk_id);
  CREATE INDEX IF NOT EXISTS idx_queue_status_new ON queue_jobs(status_new);
  CREATE INDEX IF NOT EXISTS idx_queue_last_attempt ON queue_jobs(last_attempt_at);

  CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY,
    last_processed_id INTEGER,
    last_processed_date TEXT,
    chunk_size INTEGER DEFAULT 25,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS run_summaries (
    id INTEGER PRIMARY KEY,
    chunk_id TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    expected_count INTEGER,
    processed_count INTEGER,
    failed_count INTEGER,
    timeout_count INTEGER,
    summary_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
`);

db.close();
console.log('DB inited at', dbPath);
