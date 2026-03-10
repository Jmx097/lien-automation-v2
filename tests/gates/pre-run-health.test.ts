// tests/gates/pre-run-health.test.ts
import { preRunHealthCheck } from '../../src/gates/pre-run-health';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

describe('Pre-run Health Check', () => {
  beforeEach(() => {
    // Reset environment variables before each test
    delete process.env.BRIGHT_DATA_PROXY;
    delete process.env.GOOGLE_SHEETS_CREDENTIALS;
    delete process.env.DATABASE_URL;
  });

  afterEach(() => {
    // Clean up after each test
    vi.clearAllMocks();
  });

  it('should fail when required environment variables are missing', async () => {
    const execSyncImpl = vi.fn();
    const fetchImpl = vi.fn().mockResolvedValue({ ok: true });
    const env = {};

    const result = await preRunHealthCheck({ execSyncImpl, fetchImpl: fetchImpl as any, env });
    
    expect(result.success).toBe(false);
    expect(result.errors).toContain('Required environment variable BRIGHT_DATA_PROXY is not set');
    expect(result.errors).toContain('Required environment variable GOOGLE_SHEETS_CREDENTIALS is not set');
    expect(result.errors).toContain('Required environment variable DATABASE_URL is not set');
  });

  it('should pass when all environment variables are set', async () => {
    const env = {
      BRIGHT_DATA_PROXY: 'proxy-url',
      GOOGLE_SHEETS_CREDENTIALS: 'credentials',
      DATABASE_URL: 'database-url',
    };
    const mockExecSync = vi.fn().mockReturnValue('Docker version 1.0');
    mockExecSync
      .mockReturnValueOnce(undefined)
      .mockReturnValueOnce('Up 2 minutes')
      .mockReturnValueOnce(undefined);
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });

    const result = await preRunHealthCheck({
      execSyncImpl: mockExecSync as any,
      fetchImpl: mockFetch as any,
      env,
      canaryUrl: 'https://example.test/health',
    });

    expect(result.success).toBe(true);
    expect(result.errors).toEqual([]);
  });
});
