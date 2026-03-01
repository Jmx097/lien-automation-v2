import { LienRecord } from '../types';

export interface QueueJob {
  id: number;
  fingerprint: string;
  site: string;
  filingNumber: string;
  filingDate: string;
  status: 'queued' | 'processing' | 'done' | 'failed';
  locked_until?: string;
  attempts: number;
  created_at: string;
  updated_at: string;
}

export interface QueueStore {
  insertMany(jobs: LienRecord[]): Promise<void>;
  claimBatch(limit: number): Promise<QueueJob[]>;
  markDone(ids: number[]): Promise<void>;
  markFailed(ids: number[], backoffMs: number): Promise<void>;
  getPendingCount(): Promise<number>;
}
