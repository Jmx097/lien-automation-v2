import fs from 'fs';
import path from 'path';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockGetNextRuns = vi.fn();
const mockGetRunHistory = vi.fn();
const mockGetScheduleState = vi.fn();

vi.mock('../../src/scheduler', () => ({
  getNextRuns: mockGetNextRuns,
  getRunHistory: mockGetRunHistory,
  getScheduleState: mockGetScheduleState,
}));

describe('tri-site proof export', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    const outputDir = path.join(process.cwd(), 'out', 'proof');
    fs.rmSync(outputDir, { recursive: true, force: true });
  });

  it('writes a compliant non-scraping proof artifact from persisted history', async () => {
    mockGetNextRuns.mockReturnValue([{ site: 'ca_sos', schedule: 'daily_morning' }]);
    mockGetScheduleState.mockResolvedValue({
      ca_sos: {
        site_compliant: true,
        rolling_sla_pass_rate_20: 100,
        rolling_sla_status: 'passing',
        rolling_sla_pass_count: 20,
        rolling_sla_window_successful_runs: 20,
        previous_business_day: '2026-03-25',
        previous_business_day_slot_success_count: 3,
        previous_business_day_slots_ok: true,
        effective_max_records: 40,
        connectivity: { status: 'healthy', next_allowed_run_at: undefined, last_failure_reason: undefined },
      },
      maricopa_recorder: {
        site_compliant: true,
        rolling_sla_pass_rate_20: 95,
        rolling_sla_status: 'passing',
        rolling_sla_pass_count: 19,
        rolling_sla_window_successful_runs: 20,
        previous_business_day: '2026-03-25',
        previous_business_day_slot_success_count: 3,
        previous_business_day_slots_ok: true,
        effective_max_records: 75,
        connectivity: { status: 'healthy', next_allowed_run_at: undefined, last_failure_reason: undefined },
      },
      nyc_acris: {
        site_compliant: true,
        rolling_sla_pass_rate_20: 100,
        rolling_sla_status: 'passing',
        rolling_sla_pass_count: 20,
        rolling_sla_window_successful_runs: 20,
        previous_business_day: '2026-03-25',
        previous_business_day_slot_success_count: 3,
        previous_business_day_slots_ok: true,
        effective_max_records: 75,
        connectivity: { status: 'healthy', next_allowed_run_at: undefined, last_failure_reason: undefined },
      },
    });
    mockGetRunHistory.mockResolvedValue([
      {
        id: 'ca-run-1',
        site: 'ca_sos',
        idempotency_key: 'ca_sos:2026-03-25:evening',
        started_at: '2026-03-25T22:00:00.000Z',
        finished_at: '2026-03-25T22:05:00.000Z',
        status: 'success',
        failure_class: undefined,
        partial_reason: undefined,
        records_scraped: 4,
        rows_uploaded: 4,
        sla_score_pct: 100,
        sla_pass: 1,
      },
      {
        id: 'nyc-run-1',
        site: 'nyc_acris',
        idempotency_key: 'nyc_acris:2026-03-25:evening',
        started_at: '2026-03-25T22:00:00.000Z',
        finished_at: '2026-03-25T22:06:00.000Z',
        status: 'success',
        failure_class: undefined,
        partial_reason: undefined,
        records_scraped: 5,
        rows_uploaded: 5,
        sla_score_pct: 98,
        sla_pass: 1,
      },
    ]);

    const outputPath = path.join(process.cwd(), 'out', 'proof', 'tri-site-test.json');
    const { exportTriSiteProof } = await import('../../src/proof/tri_site');
    const summary = await exportTriSiteProof({ outputPath, historyLimit: 50 });

    expect(summary.overall_status).toBe('compliant');
    expect(summary.output_path).toBe(path.resolve(outputPath));
    expect(summary.sites.ca_sos.latest_run_ids.latest_run_id).toBe('ca-run-1');
    expect(fs.existsSync(outputPath)).toBe(true);

    const persisted = JSON.parse(fs.readFileSync(outputPath, 'utf8'));
    expect(persisted.overall_status).toBe('compliant');
    expect(persisted.sites.nyc_acris.effective_max_records).toBe(75);
  });

  it('reports insufficient_data when rolling compliance has not warmed up', async () => {
    mockGetNextRuns.mockReturnValue([]);
    mockGetScheduleState.mockResolvedValue({
      ca_sos: {
        site_compliant: false,
        rolling_sla_pass_rate_20: 0,
        rolling_sla_status: 'insufficient_data',
        rolling_sla_pass_count: 0,
        rolling_sla_window_successful_runs: 0,
        previous_business_day: '2026-03-25',
        previous_business_day_slot_success_count: 0,
        previous_business_day_slots_ok: false,
        effective_max_records: 10,
        connectivity: { status: 'healthy', next_allowed_run_at: undefined, last_failure_reason: undefined },
      },
      maricopa_recorder: {
        site_compliant: false,
        rolling_sla_pass_rate_20: 0,
        rolling_sla_status: 'insufficient_data',
        rolling_sla_pass_count: 0,
        rolling_sla_window_successful_runs: 0,
        previous_business_day: '2026-03-25',
        previous_business_day_slot_success_count: 0,
        previous_business_day_slots_ok: false,
        effective_max_records: 75,
        connectivity: { status: 'healthy', next_allowed_run_at: undefined, last_failure_reason: undefined },
      },
      nyc_acris: {
        site_compliant: false,
        rolling_sla_pass_rate_20: 0,
        rolling_sla_status: 'insufficient_data',
        rolling_sla_pass_count: 0,
        rolling_sla_window_successful_runs: 0,
        previous_business_day: '2026-03-25',
        previous_business_day_slot_success_count: 0,
        previous_business_day_slots_ok: false,
        effective_max_records: 75,
        connectivity: { status: 'healthy', next_allowed_run_at: undefined, last_failure_reason: undefined },
      },
    });
    mockGetRunHistory.mockResolvedValue([]);

    const { exportTriSiteProof } = await import('../../src/proof/tri_site');
    const summary = await exportTriSiteProof({ outputPath: path.join(process.cwd(), 'out', 'proof', 'tri-site-insufficient.json') });

    expect(summary.overall_status).toBe('insufficient_data');
    expect(summary.sites.ca_sos.recent_runs).toEqual([]);
  });
});
