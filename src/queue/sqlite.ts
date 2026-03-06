import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { QueueStore, QueueJob } from './QueueStore';
import { LienRecord } from '../types';

interface QueueJobRow {
  id: number;
  fingerprint: string;
  site: string;
  filingNumber: string;
  filingDate: string;
  status: 'queued' | 'processing' | 'done' | 'failed';
  locked_until: string | null;
  attempts: number;
  created_at: string;
  updated_at: string;
}

interface CountRow {
  count: number;
}

type ExistsRow = { 1: number } | undefined;

export class SQLiteQueueStore implements QueueStore {
  private db: Database.Database;

  constructor() {
    const dbPath = path.join(process.cwd(), 'data/db/lien-queue.db');
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new Database(dbPath);
  }

  async insertMany(records: LienRecord[]): Promise<void> {
    const tx = this.db.transaction(() => {
      const insert = this.db.prepare(`
        INSERT OR IGNORE INTO queue_jobs (fingerprint, site, filingNumber, filingDate)
        VALUES (?, ?, ?, ?)
      `);
      for (const record of records) {
        const fingerprint = crypto
          .createHash('sha256')
          .update(`${record.source}-${record.file_number}-${record.filing_date}`)
          .digest('hex');
        insert.run(fingerprint, record.source, record.file_number, record.filing_date);
      }
    });
    tx();
  }

  async claimBatch(limit: number): Promise<QueueJob[]> {
    const now = new Date().toISOString();

    const select = this.db.prepare(`
      SELECT * FROM queue_jobs 
      WHERE status = 'queued' OR (status = 'failed' AND locked_until < ?)
      ORDER BY created_at ASC
      LIMIT ?
    `);

    const jobs = select.all(now, limit) as QueueJobRow[];

    if (jobs.length === 0) return [];

    const update = this.db.prepare(`
      UPDATE queue_jobs 
      SET status = 'processing', locked_until = ?, attempts = attempts + 1, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `);

    for (const job of jobs) {
      update.run(now, job.id);
    }

    return jobs.map(row => ({
      id: row.id,
      fingerprint: row.fingerprint,
      site: row.site,
      filingNumber: row.filingNumber,
      filingDate: row.filingDate,
      status: 'processing',
      locked_until: now,
      attempts: row.attempts + 1,
      created_at: row.created_at,
      updated_at: row.updated_at,
    }));
  }

  async markDone(ids: number[]): Promise<void> {
    const placeholders = ids.map(() => '?').join(',');
    const stmt = this.db.prepare(`
      UPDATE queue_jobs 
      SET status = 'done', locked_until = NULL, updated_at = CURRENT_TIMESTAMP
      WHERE id IN (${placeholders})
    `);
    stmt.run(...ids);
  }

  async markFailed(ids: number[], backoffMs: number): Promise<void> {
    const backoffUntil = new Date(Date.now() + backoffMs).toISOString();
    const placeholders = ids.map(() => '?').join(',');
    const stmt = this.db.prepare(`
      UPDATE queue_jobs 
      SET status = 'failed', locked_until = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id IN (${placeholders})
    `);
    stmt.run(backoffUntil, ...ids);
  }

  async getPendingCount(): Promise<number> {
    const row = this.db
      .prepare("SELECT COUNT(*) as count FROM queue_jobs WHERE status IN ('queued', 'processing')")
      .get() as CountRow;
    return row.count;
  }

  hasFingerprint(fingerprint: string): boolean {
    const row = this.db.prepare("SELECT 1 FROM queue_jobs WHERE fingerprint = ?").get(fingerprint) as ExistsRow;
    return !!row;
  }
}
