// src/scraper/chunk-processor.ts
import { preRunHealthCheck } from '../gates/pre-run-health';
import { processRecordWithRetry, DEFAULT_RETRY_POLICY } from '../utils/retry-policy';
import { scrapeCASOS_Enhanced } from './ca_sos_enhanced';
import { LienRecord } from '../types';
import { SQLiteQueueStore } from '../queue/sqlite';
import { log } from '../utils/logger';
import type { RunSummary, ChunkIntegrityResult, PostRunVerifyResult, GateResult } from '../gates/types';

export interface ChunkProcessorOptions {
  chunkId: string;
  startDate: string;
  endDate: string;
  maxRecords: number;
}

export interface ChunkResult {
  success: boolean;
  processedCount: number;
  failedCount: number;
  records: LienRecord[];
  errors: string[];
  summary?: RunSummary;
}

/**
 * Process a chunk of records with pre-run health checks and retry policy
 * This demonstrates the integration of Gate 1 (pre-run health) with the retry policy
 */
export async function processChunk(options: ChunkProcessorOptions): Promise<ChunkResult> {
  const { chunkId, startDate, endDate, maxRecords } = options;
  const errors: string[] = [];
  const records: LienRecord[] = [];
  const queue = new SQLiteQueueStore();
  const startTime = new Date();
  
  console.log(`[${new Date().toISOString()}] Starting chunk ${chunkId}`);

  // Gate 1: Pre-run health check
  const gate1 = await preRunHealthCheck();
  if (!gate1.success) {
    const errorMsg = `Pre-run health check failed: ${gate1.errors?.join(', ') ?? 'unknown error'}`;
    console.error(`[${new Date().toISOString()}] ${errorMsg}`);
    const endTime = new Date();
    const summary: RunSummary = {
      chunk_id: chunkId,
      start_time: startTime.toISOString(),
      end_time: endTime.toISOString(),
      elapsed_seconds: (endTime.getTime() - startTime.getTime()) / 1000,
      expected_count: maxRecords,
      processed_count: 0,
      failed_count: maxRecords,
      timeout_count: 0,
      error_breakdown: {
        timeout: 0,
        '403_forbidden': 0,
        '429_rate_limit': 0,
        selector_fail: 0,
        network_error: 0,
      },
      checkpoint_updated: false,
      gate_results: {
        pre_run_health: gate1,
        chunk_integrity: { success: false, errors: [errorMsg] } as ChunkIntegrityResult,
        post_run_verify: { success: false, errors: ['gate 1 failed; not executed'] } as PostRunVerifyResult,
      },
    };

    await queue.insertRunSummary({
      chunk_id: summary.chunk_id,
      start_time: summary.start_time,
      end_time: summary.end_time,
      expected_count: summary.expected_count,
      processed_count: summary.processed_count,
      failed_count: summary.failed_count,
      timeout_count: summary.timeout_count,
      summary_json: JSON.stringify(summary),
    });

    return {
      success: false,
      processedCount: 0,
      failedCount: maxRecords,
      records: [],
      errors: [errorMsg, ...(gate1.errors ?? [])],
      summary,
    };
  }
  
  console.log(`[${new Date().toISOString()}] Pre-run health check passed`);

  try {
    // Process records with retry policy
    const scrapeOptions = {
      date_start: startDate,
      date_end: endDate,
      max_records: maxRecords,
      chunk_id: chunkId,
    };
    
    // Using retry policy for the entire scrape operation
    const retryResult = await processRecordWithRetry(
      chunkId,
      async () => {
        console.log(`[${new Date().toISOString()}] Initiating scrape for chunk ${chunkId}`);
        return await scrapeCASOS_Enhanced(scrapeOptions);
      },
      DEFAULT_RETRY_POLICY
    );
    
    const endTime = new Date();

    let chunkIntegrity: ChunkIntegrityResult;
    let postRunVerify: PostRunVerifyResult;
    let processedCount = 0;
    let failedCount = 0;
    let timeoutCount = 0;

    if (retryResult.success && retryResult.result) {
      records.push(...retryResult.result);
      processedCount = retryResult.result.length;
      console.log(
        `[${new Date().toISOString()}] Successfully processed chunk ${chunkId} with ${processedCount} records`
      );

      // Gate 2: basic chunk integrity – ensure we have at least one record when expected
      chunkIntegrity = {
        success: processedCount > 0 || maxRecords === 0,
        partial: processedCount > 0 && processedCount < maxRecords,
        missing_ids: [],
      };

      // Gate 3: post-run verification – processed_count matches expected within simple bounds
      postRunVerify = {
        success: processedCount <= maxRecords,
        summary_path: undefined,
        checkpoint_updated: true,
      };

      await queue.saveCheckpoint({
        lastProcessedId: null,
        lastProcessedDate: endDate,
        chunkSize: maxRecords,
      });
    } else {
      const errorMsg =
        retryResult.error && retryResult.error.message
          ? retryResult.error.message
          : 'Unknown error during scraping';
      errors.push(errorMsg);
      console.error(
        `[${new Date().toISOString()}] Failed to process chunk ${chunkId}: ${errorMsg}`
      );
      failedCount = maxRecords;
      timeoutCount = /timeout/i.test(errorMsg) ? maxRecords : 0;

      chunkIntegrity = {
        success: false,
        errors: [errorMsg],
      };
      postRunVerify = {
        success: false,
        errors: ['scrape failed; no post-run verification'],
        checkpoint_updated: false,
      };
    }

    const elapsedSeconds = (endTime.getTime() - startTime.getTime()) / 1000;
    const summary: RunSummary = {
      chunk_id: chunkId,
      start_time: startTime.toISOString(),
      end_time: endTime.toISOString(),
      elapsed_seconds: elapsedSeconds,
      expected_count: maxRecords,
      processed_count: processedCount,
      failed_count: failedCount,
      timeout_count: timeoutCount,
      error_breakdown: {
        timeout: timeoutCount,
        '403_forbidden': 0,
        '429_rate_limit': 0,
        selector_fail: 0,
        network_error: 0,
      },
      checkpoint_updated: postRunVerify.checkpoint_updated ?? false,
      gate_results: {
        pre_run_health: gate1 as GateResult,
        chunk_integrity: chunkIntegrity,
        post_run_verify: postRunVerify,
      },
    };

    await queue.insertRunSummary({
      chunk_id: summary.chunk_id,
      start_time: summary.start_time,
      end_time: summary.end_time,
      expected_count: summary.expected_count,
      processed_count: summary.processed_count,
      failed_count: summary.failed_count,
      timeout_count: summary.timeout_count,
      summary_json: JSON.stringify(summary),
    });

    return {
      success: retryResult.success && processedCount > 0,
      processedCount,
      failedCount,
      records,
      errors,
      summary,
    };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    errors.push(errorMsg);
    console.error(
      `[${new Date().toISOString()}] Unexpected error processing chunk ${chunkId}: ${errorMsg}`
    );

    const endTime = new Date();
    const summary: RunSummary = {
      chunk_id: chunkId,
      start_time: startTime.toISOString(),
      end_time: endTime.toISOString(),
      elapsed_seconds: (endTime.getTime() - startTime.getTime()) / 1000,
      expected_count: maxRecords,
      processed_count: 0,
      failed_count: maxRecords,
      timeout_count: /timeout/i.test(errorMsg) ? maxRecords : 0,
      error_breakdown: {
        timeout: /timeout/i.test(errorMsg) ? maxRecords : 0,
        '403_forbidden': 0,
        '429_rate_limit': 0,
        selector_fail: 0,
        network_error: 0,
      },
      checkpoint_updated: false,
      gate_results: {
        pre_run_health: { success: true } as GateResult,
        chunk_integrity: { success: false, errors: [errorMsg] } as ChunkIntegrityResult,
        post_run_verify: {
          success: false,
          errors: ['unexpected error during chunk processing'],
        } as PostRunVerifyResult,
      },
    };

    await queue.insertRunSummary({
      chunk_id: summary.chunk_id,
      start_time: summary.start_time,
      end_time: summary.end_time,
      expected_count: summary.expected_count,
      processed_count: summary.processed_count,
      failed_count: summary.failed_count,
      timeout_count: summary.timeout_count,
      summary_json: JSON.stringify(summary),
    });

    return {
      success: false,
      processedCount: 0,
      failedCount: maxRecords,
      records: [],
      errors,
      summary,
    };
  }
}

export default processChunk;