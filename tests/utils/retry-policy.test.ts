// tests/utils/retry-policy.test.ts
import { executeWithRetry, processRecordWithRetry, calculateDelay, DEFAULT_RETRY_POLICY } from '../../src/utils/retry-policy';
import { describe, it, expect, vi } from 'vitest';

describe('Retry Policy', () => {
  describe('calculateDelay', () => {
    it('should calculate exponential backoff without jitter', () => {
      const config = { ...DEFAULT_RETRY_POLICY, jitter: false };
      
      // Test first few delays
      expect(calculateDelay(0, config)).toBe(1000);  // 1 * 2^0 = 1s
      expect(calculateDelay(1, config)).toBe(2000);  // 1 * 2^1 = 2s
      expect(calculateDelay(2, config)).toBe(4000);  // 1 * 2^2 = 4s
      expect(calculateDelay(3, config)).toBe(8000);  // 1 * 2^3 = 8s
    });

    it('should cap delay at maxDelay', () => {
      const config = { ...DEFAULT_RETRY_POLICY, jitter: false };
      
      // Should be capped at 30s
      expect(calculateDelay(10, config)).toBe(30000);
    });
  });

  describe('executeWithRetry', () => {
    it('should return success immediately if function succeeds', async () => {
      const fn = vi.fn().mockResolvedValue('success');
      
      const result = await executeWithRetry(fn);
      
      expect(result.success).toBe(true);
      expect(result.result).toBe('success');
      expect(result.attemptCount).toBe(1);
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('should retry on failure and eventually succeed', async () => {
      const fn = vi.fn()
        .mockRejectedValueOnce(new Error('First failure'))
        .mockRejectedValueOnce(new Error('Second failure'))
        .mockResolvedValue('success');
      
      const config = { ...DEFAULT_RETRY_POLICY, maxRetries: 3, baseDelay: 10 }; // Short delay for testing
      
      const result = await executeWithRetry(fn, config);
      
      expect(result.success).toBe(true);
      expect(result.result).toBe('success');
      expect(result.attemptCount).toBe(3);
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it('should fail after max retries exceeded', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('Persistent failure'));
      
      const config = { ...DEFAULT_RETRY_POLICY, maxRetries: 2, baseDelay: 10 };
      
      const result = await executeWithRetry(fn, config);
      
      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
      expect(result.attemptCount).toBe(3); // Initial + 2 retries
      expect(fn).toHaveBeenCalledTimes(3);
    });
  });

  describe('processRecordWithRetry', () => {
    it('should process a record with retry logic', async () => {
      const processor = vi.fn().mockResolvedValue({ id: 'test', data: 'processed' });
      
      const result = await processRecordWithRetry('test-record', processor);
      
      expect(result.success).toBe(true);
      expect(result.result).toEqual({ id: 'test', data: 'processed' });
      expect(processor).toHaveBeenCalledWith('test-record');
    });
  });
});