// tests/integration/gate-integration.test.ts
import { describe, it, expect, vi } from 'vitest';

const mockPreRunHealthCheck = vi.fn();
const mockProcessRecordWithRetry = vi.fn();
const mockScrapeCASOSEnhanced = vi.fn();

vi.mock('../../src/gates/pre-run-health', () => ({
  preRunHealthCheck: mockPreRunHealthCheck,
}));

vi.mock('../../src/utils/retry-policy', () => ({
  DEFAULT_RETRY_POLICY: {},
  processRecordWithRetry: mockProcessRecordWithRetry,
}));

vi.mock('../../src/scraper/ca_sos_enhanced', () => ({
  scrapeCASOS_Enhanced: mockScrapeCASOSEnhanced,
}));

describe('Gate Integration', () => {
  it('should demonstrate the integration of gates with chunk processing', async () => {
    mockPreRunHealthCheck.mockResolvedValue({
      success: true,
      errors: []
    });
    mockScrapeCASOSEnhanced.mockResolvedValue([
      {
        file_number: 'TEST-001',
        debtor_name: 'Test Debtor',
        state: 'CA',
        source: 'ca_sos'
      }
    ]);
    mockProcessRecordWithRetry.mockImplementation(async (_id: string, work: () => Promise<any>) => ({
      success: true,
      result: await work(),
    }));

    const { processChunk } = await import('../../src/scraper/chunk-processor');
    const chunkResult = await processChunk({
      chunkId: 'test-chunk-001',
      startDate: '01/01/2024',
      endDate: '01/31/2024',
      maxRecords: 5
    });

    expect(mockPreRunHealthCheck).toHaveBeenCalledTimes(1);
    expect(mockProcessRecordWithRetry).toHaveBeenCalledTimes(1);
    expect(chunkResult).toEqual(expect.objectContaining({
      success: true,
      processedCount: 1,
      failedCount: 0,
    }));
  });
});
