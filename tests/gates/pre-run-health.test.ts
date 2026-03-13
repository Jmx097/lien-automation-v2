import { describe, it, expect, vi } from 'vitest';
import { preRunHealthCheck } from '../../src/gates/pre-run-health';

describe('Pre-run Health Check', () => {
  it('fails when current runtime env requirements are missing', async () => {
    const result = await preRunHealthCheck({
      execSyncImpl: vi.fn() as any,
      fetchImpl: vi.fn().mockResolvedValue({
        ok: true,
        text: async () => JSON.stringify({ status: 'ready', checks: [], merged_output: { target_reachable: true, fallback_active: false } }),
      }) as any,
      env: {},
      sites: ['ca_sos'],
    });

    expect(result.success).toBe(false);
    expect(result.errors).toContain('Required environment variable SHEET_ID is not set');
    expect(result.errors).toContain('Required environment variable SHEETS_KEY is not set');
    expect(result.errors).toContain('Required environment variable SCHEDULE_RUN_TOKEN is not set');
    expect(result.errors).toContain('One browser transport must be configured: BRIGHTDATA_BROWSER_WS, BRIGHTDATA_PROXY_SERVER, or SBR_CDP_URL');
  });

  it('passes when readiness, version, and runtime dependencies are healthy', async () => {
    const execSyncImpl = vi.fn().mockReturnValue(undefined);
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        text: async () => JSON.stringify({
          status: 'ready',
          checks: [
            { name: 'required_env_present', ok: true },
            { name: 'db_reachable', ok: true, detail: 'scheduler_store=postgres' },
          ],
          merged_output: { target_reachable: true, fallback_active: false },
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        text: async () => JSON.stringify({ git_sha: 'abc123', node_version: 'v22.0.0' }),
      });

    const result = await preRunHealthCheck({
      execSyncImpl: execSyncImpl as any,
      fetchImpl: fetchImpl as any,
      env: {
        SHEET_ID: 'sheet-id',
        SHEETS_KEY: JSON.stringify({ client_email: 'svc@example.com', private_key: 'key' }),
        SCHEDULE_RUN_TOKEN: 'token',
        SBR_CDP_URL: 'wss://example.invalid',
        SCHEDULE_CA_SOS_WEEKLY_DAYS: 'MO,TU,WE,TH,FR',
        SCHEDULE_CA_SOS_MORNING_RUN_HOUR: '10',
        SCHEDULE_CA_SOS_MORNING_RUN_MINUTE: '0',
        SCHEDULE_CA_SOS_AFTERNOON_RUN_HOUR: '14',
        SCHEDULE_CA_SOS_AFTERNOON_RUN_MINUTE: '0',
        SCHEDULE_CA_SOS_EVENING_RUN_HOUR: '22',
        SCHEDULE_CA_SOS_EVENING_RUN_MINUTE: '0',
        SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS: 'MO,TU,WE,TH,FR',
        SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_HOUR: '10',
        SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_MINUTE: '0',
        SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_HOUR: '14',
        SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_MINUTE: '0',
        SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_HOUR: '22',
        SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_MINUTE: '0',
        SCHEDULE_NYC_ACRIS_WEEKLY_DAYS: 'MO,TU,WE,TH,FR',
        SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR: '10',
        SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE: '0',
        SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR: '14',
        SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE: '0',
        SCHEDULE_NYC_ACRIS_EVENING_RUN_HOUR: '22',
        SCHEDULE_NYC_ACRIS_EVENING_RUN_MINUTE: '0',
        REQUIRE_OCR_TOOLS: '0',
      },
      sites: ['nyc_acris'],
    });

    expect(result.success).toBe(true);
    expect(result.errors).toEqual([]);
    expect(result.warnings).toEqual([]);
  });
});
