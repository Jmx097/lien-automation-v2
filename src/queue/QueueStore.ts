import { LienRecord } from '../types';

export interface QueueJob {
  id: number;
  fingerprint: string;
  site: string;
  filingNumber: string;
  filingDate: string;
  chunk_id?: string | null;
  status: 'pending' | 'in_progress' | 'done' | 'failed';
  error_code?: string | null;
  retry_count?: number;
  last_attempt_at?: string | null;
  completed_at?: string | null;
  locked_until?: string;
  attempts: number;
  created_at: string;
  updated_at: string;
}

export interface QueueCheckpoint {
  id: number;
  last_processed_id: number | null;
  last_processed_date: string | null;
  chunk_size: number;
  updated_at: string;
}

export interface RunSummaryInput {
  chunk_id: string;
  start_time: string;
  end_time: string;
  expected_count: number;
  processed_count: number;
  failed_count: number;
  timeout_count: number;
  summary_json: string;
}

export interface QueueStore {
  insertMany(jobs: LienRecord[], options?: { chunkId?: string }): Promise<void>;
  claimBatch(limit: number): Promise<QueueJob[]>;
  markDone(ids: number[]): Promise<void>;
  markFailed(ids: number[], backoffMs: number, errorCode?: string): Promise<void>;
  getPendingCount(): Promise<number>;
  saveCheckpoint(input: { lastProcessedId: number | null; lastProcessedDate: string | null; chunkSize: number }): Promise<void>;
  getLatestCheckpoint(): Promise<QueueCheckpoint | null>;
  insertRunSummary(input: RunSummaryInput): Promise<void>;
}
