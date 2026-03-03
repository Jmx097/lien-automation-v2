import Database from 'better-sqlite3';
import path from 'path';
import crypto from 'crypto';
import {
  QueueStore,
  QueueJob,
  QueueCheckpoint,
  RunSummaryInput,
} from './QueueStore';
import { LienRecord } from '../types';

export class SQLiteQueueStore implements QueueStore {
  private db: Database.Database;
  private hasStatusNew: boolean;

  constructor() {
    const dbPath = path.join(process.cwd(), 'data/db/lien-queue.db');
    this.db = new Database(dbPath);
    const columns = this.db.prepare("PRAGMA table_info(queue_jobs)").all() as {
      name: string;
    }[];
    this.hasStatusNew = columns.some((col) => col.name === 'status_new');
  }

  async insertMany(
    records: LienRecord[],
    options?: { chunkId?: string }
  ): Promise<void> {
    const chunkId = options?.chunkId ?? null;

    const tx = this.db.transaction(() => {
      const insert = this.db.prepare(
        this.hasStatusNew
          ? `
        INSERT OR IGNORE INTO queue_jobs (
          fingerprint,
          site,
          filingNumber,
          filingDate,
          chunk_id,
          status_new,
          retry_count
        )
        VALUES (?, ?, ?, ?, ?, 'pending', 0)
      `
          : `
        INSERT OR IGNORE INTO queue_jobs (
          fingerprint,
          site,
          filingNumber,
          filingDate
        )
        VALUES (?, ?, ?, ?)
      `
      );
      for (const record of records) {
        const fingerprint = crypto
          .createHash('sha256')
          .update(`${record.source}-${record.file_number}-${record.filing_date}`)
          .digest('hex');
        if (this.hasStatusNew) {
          insert.run(
            fingerprint,
            record.source,
            record.file_number,
            record.filing_date,
            chunkId
          );
        } else {
          insert.run(
            fingerprint,
            record.source,
            record.file_number,
            record.filing_date
          );
        }
      }
    });
    tx();
  }

  async claimBatch(limit: number): Promise<QueueJob[]> {
    const now = new Date().toISOString();

    const select = this.db.prepare(
      this.hasStatusNew
        ? `
      SELECT * FROM queue_jobs
      WHERE status_new = 'pending'
         OR (status_new = 'failed' AND locked_until < ?)
      ORDER BY created_at ASC
      LIMIT ?
    `
        : `
      SELECT * FROM queue_jobs 
      WHERE status = 'queued' OR (status = 'failed' AND locked_until < ?)
      ORDER BY created_at ASC
      LIMIT ?
    `
    );

    const jobs: any[] = select.all(now, limit);

    if (jobs.length === 0) return [];

    const update = this.db.prepare(
      this.hasStatusNew
        ? `
      UPDATE queue_jobs 
      SET status_new = 'in_progress',
          locked_until = ?,
          attempts = attempts + 1,
          retry_count = COALESCE(retry_count, 0),
          last_attempt_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `
        : `
      UPDATE queue_jobs 
      SET status = 'processing',
          locked_until = ?,
          attempts = attempts + 1,
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `
    );

    for (const job of jobs) {
      update.run(now, job.id);
    }

    return jobs.map(row => ({
      id: row.id,
      fingerprint: row.fingerprint,
      site: row.site,
      filingNumber: row.filingNumber,
      filingDate: row.filingDate,
      chunk_id: row.chunk_id ?? null,
      status: this.hasStatusNew
        ? (row.status_new as QueueJob['status'])
        : (row.status === 'queued'
            ? 'pending'
            : row.status === 'processing'
              ? 'in_progress'
              : row.status),
      error_code: row.error_code ?? null,
      retry_count: row.retry_count ?? 0,
      last_attempt_at: row.last_attempt_at ?? null,
      completed_at: row.completed_at ?? null,
      locked_until: now,
      attempts: row.attempts + 1,
      created_at: row.created_at,
      updated_at: row.updated_at
    }));
  }

  async markDone(ids: number[]): Promise<void> {
    const placeholders = ids.map(() => '?').join(',');
    const stmt = this.db.prepare(
      this.hasStatusNew
        ? `
      UPDATE queue_jobs 
      SET status_new = 'done',
          locked_until = NULL,
          completed_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      WHERE id IN (${placeholders})
    `
        : `
      UPDATE queue_jobs 
      SET status = 'done',
          locked_until = NULL,
          updated_at = CURRENT_TIMESTAMP
      WHERE id IN (${placeholders})
    `
    );
    stmt.run(...ids);
  }

  async markFailed(ids: number[], backoffMs: number, errorCode?: string): Promise<void> {
    const backoffUntil = new Date(Date.now() + backoffMs).toISOString();
    const placeholders = ids.map(() => '?').join(',');
    const stmt = this.db.prepare(
      this.hasStatusNew
        ? `
      UPDATE queue_jobs 
      SET status_new = 'failed',
          locked_until = ?,
          retry_count = COALESCE(retry_count, 0) + 1,
          last_attempt_at = CURRENT_TIMESTAMP,
          error_code = COALESCE(?, error_code),
          updated_at = CURRENT_TIMESTAMP
      WHERE id IN (${placeholders})
    `
        : `
      UPDATE queue_jobs 
      SET status = 'failed',
          locked_until = ?,
          updated_at = CURRENT_TIMESTAMP
      WHERE id IN (${placeholders})
    `
    );
    if (this.hasStatusNew) {
      stmt.run(backoffUntil, errorCode ?? null, ...ids);
    } else {
      stmt.run(backoffUntil, ...ids);
    }
  }

  async getPendingCount(): Promise<number> {
    const row: any = this.db
      .prepare(
        this.hasStatusNew
          ? "SELECT COUNT(*) as count FROM queue_jobs WHERE status_new IN ('pending', 'in_progress')"
          : "SELECT COUNT(*) as count FROM queue_jobs WHERE status IN ('queued', 'processing')"
      )
      .get();
    return row.count as number;
  }

  async saveCheckpoint(input: {
    lastProcessedId: number | null;
    lastProcessedDate: string | null;
    chunkSize: number;
  }): Promise<void> {
    const stmt = this.db.prepare(`
      INSERT INTO checkpoints (last_processed_id, last_processed_date, chunk_size, updated_at)
      VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    `);
    stmt.run(input.lastProcessedId, input.lastProcessedDate, input.chunkSize);
  }

  async getLatestCheckpoint(): Promise<QueueCheckpoint | null> {
    const row = this.db
      .prepare(
        `
      SELECT id, last_processed_id, last_processed_date, chunk_size, updated_at
      FROM checkpoints
      ORDER BY id DESC
      LIMIT 1
    `
      )
      .get() as QueueCheckpoint | undefined;
    return row ?? null;
  }

  async insertRunSummary(input: RunSummaryInput): Promise<void> {
    const stmt = this.db.prepare(`
      INSERT INTO run_summaries (
        chunk_id,
        start_time,
        end_time,
        expected_count,
        processed_count,
        failed_count,
        timeout_count,
        summary_json
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      input.chunk_id,
      input.start_time,
      input.end_time,
      input.expected_count,
      input.processed_count,
      input.failed_count,
      input.timeout_count,
      input.summary_json
    );
  }
}
