// scripts/update-queue-schema.js
// Script to update the queue database schema for Phase 2 chunking support

const Database = require('better-sqlite3');
const path = require('path');

// Connect to the database
const dbPath = path.join(process.cwd(), 'data/db/lien-queue.db');
const db = new Database(dbPath, { verbose: console.log });

console.log('Updating queue database schema for chunking support...');

try {
  // Add new columns to queue_jobs table
  console.log('Adding chunking columns to queue_jobs table...');
  
  // Check if columns exist before adding them
  const columns = db.prepare("PRAGMA table_info(queue_jobs)").all();
  const columnNames = columns.map(col => col.name);
  
  if (!columnNames.includes('chunk_id')) {
    db.exec(`ALTER TABLE queue_jobs ADD COLUMN chunk_id TEXT`);
    console.log('Added chunk_id column');
  }
  
  if (!columnNames.includes('status_new')) {
    // We need to handle the status column carefully since it has a CHECK constraint
    // We'll add a temporary column, migrate data, drop the old column, and rename
    db.exec(`
      ALTER TABLE queue_jobs ADD COLUMN status_new TEXT DEFAULT 'pending' 
      CHECK(status_new IN ('pending', 'in_progress', 'done', 'failed'))
    `);
    
    // Migrate existing data
    db.exec(`
      UPDATE queue_jobs 
      SET status_new = CASE 
        WHEN status = 'queued' THEN 'pending'
        WHEN status = 'processing' THEN 'in_progress'
        WHEN status = 'done' THEN 'done'
        WHEN status = 'failed' THEN 'failed'
        ELSE 'pending'
      END
    `);
    
    console.log('Added status_new column and migrated data');
  }
  
  if (!columnNames.includes('error_code')) {
    db.exec(`ALTER TABLE queue_jobs ADD COLUMN error_code TEXT`);
    console.log('Added error_code column');
  }
  
  if (!columnNames.includes('retry_count')) {
    db.exec(`ALTER TABLE queue_jobs ADD COLUMN retry_count INTEGER DEFAULT 0`);
    console.log('Added retry_count column');
  }
  
  if (!columnNames.includes('last_attempt_at')) {
    db.exec(`ALTER TABLE queue_jobs ADD COLUMN last_attempt_at DATETIME`);
    console.log('Added last_attempt_at column');
  }
  
  if (!columnNames.includes('completed_at')) {
    db.exec(`ALTER TABLE queue_jobs ADD COLUMN completed_at DATETIME`);
    console.log('Added completed_at column');
  }
  
  // Create checkpoints table if it doesn't exist
  console.log('Creating checkpoints table...');
  db.exec(`
    CREATE TABLE IF NOT EXISTS checkpoints (
      id INTEGER PRIMARY KEY,
      last_processed_id INTEGER,
      last_processed_date TEXT,
      chunk_size INTEGER DEFAULT 25,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  console.log('Checkpoints table created/verified');
  
  // Create run_summaries table if it doesn't exist
  console.log('Creating run_summaries table...');
  db.exec(`
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
    )
  `);
  console.log('Run summaries table created/verified');
  
  // Create indexes for better performance
  console.log('Creating indexes...');
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_queue_chunk_id ON queue_jobs(chunk_id);
    CREATE INDEX IF NOT EXISTS idx_queue_status_new ON queue_jobs(status_new);
    CREATE INDEX IF NOT EXISTS idx_queue_last_attempt ON queue_jobs(last_attempt_at);
  `);
  console.log('Indexes created/verified');
  
  console.log('Database schema update completed successfully!');
  
} catch (error) {
  console.error('Error updating database schema:', error);
  process.exit(1);
} finally {
  db.close();
  console.log('Database connection closed.');
}