// src/scraper/chunk-processor.ts
import { preRunHealthCheck } from '../gates/pre-run-health';
import { processRecordWithRetry, DEFAULT_RETRY_POLICY } from '../utils/retry-policy';
import { scrapeCASOS_Enhanced } from './ca_sos_enhanced';
import { LienRecord } from '../types';

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
}

/**
 * Process a chunk of records with pre-run health checks and retry policy
 * This demonstrates the integration of Gate 1 (pre-run health) with the retry policy
 */
export async function processChunk(options: ChunkProcessorOptions): Promise<ChunkResult> {
  const { chunkId, startDate, endDate, maxRecords } = options;
  const errors: string[] = [];
  const records: LienRecord[] = [];
  
  console.log(`[${new Date().toISOString()}] Starting chunk ${chunkId}`);

  // Gate 1: Pre-run health check
  const health = await preRunHealthCheck();
  if (!health.success) {
    const errorMsg = `Pre-run health check failed: ${health.errors.join(', ')}`;
    console.error(`[${new Date().toISOString()}] ${errorMsg}`);
    return {
      success: false,
      processedCount: 0,
      failedCount: 0,
      records: [],
      errors: [errorMsg, ...health.errors]
    };
  }
  
  console.log(`[${new Date().toISOString()}] Pre-run health check passed`);

  try {
    // Process records with retry policy
    const scrapeOptions = {
      date_start: startDate,
      date_end: endDate,
      max_records: maxRecords
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
    
    if (retryResult.success && retryResult.result) {
      records.push(...retryResult.result);
      console.log(`[${new Date().toISOString()}] Successfully processed chunk ${chunkId} with ${retryResult.result.length} records`);
      
      return {
        success: true,
        processedCount: retryResult.result.length,
        failedCount: 0,
        records: retryResult.result,
        errors: []
      };
    } else {
      const errorMsg = retryResult.error ? retryResult.error.message : 'Unknown error during scraping';
      errors.push(errorMsg);
      console.error(`[${new Date().toISOString()}] Failed to process chunk ${chunkId}: ${errorMsg}`);
      
      return {
        success: false,
        processedCount: 0,
        failedCount: maxRecords,
        records: [],
        errors: [errorMsg]
      };
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    errors.push(errorMsg);
    console.error(`[${new Date().toISOString()}] Unexpected error processing chunk ${chunkId}: ${errorMsg}`);
    
    return {
      success: false,
      processedCount: 0,
      failedCount: maxRecords,
      records: [],
      errors: [errorMsg]
    };
  }
}

export default processChunk;