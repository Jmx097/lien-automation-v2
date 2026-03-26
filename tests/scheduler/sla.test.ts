import { describe, expect, it } from 'vitest';
import { buildRunSlaSummary } from '../../src/scheduler/sla';
import type { ScheduledRunRecord } from '../../src/scheduler/store';

function buildRun(overrides: Partial<ScheduledRunRecord> = {}): ScheduledRunRecord {
  return {
    id: 'sched_ca_sos_1',
    site: 'ca_sos',
    idempotency_key: 'ca_sos:2026-03-24:morning',
    slot_time: 'ca_sos:2026-03-24:morning',
    trigger_source: 'external',
    started_at: '2026-03-24T13:00:00.000Z',
    finished_at: '2026-03-24T13:05:00.000Z',
    status: 'success',
    records_scraped: 10,
    records_skipped: 0,
    rows_uploaded: 10,
    amount_found_count: 10,
    amount_missing_count: 0,
    amount_coverage_pct: 100,
    ocr_success_pct: 100,
    row_fail_pct: 0,
    deadline_hit: 0,
    effective_max_records: 10,
    partial: 0,
    retry_exhausted: 0,
    source_tab_title: 'Scheduled_ca_sos',
    master_tab_title: 'Master',
    review_tab_title: 'Review_Queue',
    quarantined_row_count: 0,
    current_run_quarantined_row_count: 0,
    current_run_conflict_row_count: 0,
    retained_prior_review_row_count: 0,
    new_master_row_count: 10,
    purged_review_row_count: 0,
    master_fallback_used: 0,
    artifact_retrieval_enabled: 1,
    artifact_fetch_coverage_pct: 100,
    artifact_readiness_not_met: 0,
    ...overrides,
  };
}

describe('buildRunSlaSummary', () => {
  it('passes a perfect CA scheduled run at or above 95%', () => {
    const summary = buildRunSlaSummary(buildRun());

    expect(summary.pass).toBe(true);
    expect(summary.score_pct).toBeGreaterThanOrEqual(95);
    expect(summary.hard_fail_reason).toBeUndefined();
  });

  it('fails an NYC success with low amount coverage even when row fail is zero', () => {
    const summary = buildRunSlaSummary(buildRun({
      site: 'nyc_acris',
      id: 'sched_nyc_1',
      idempotency_key: 'nyc_acris:2026-03-24:afternoon',
      slot_time: 'nyc_acris:2026-03-24:afternoon',
      amount_coverage_pct: 40,
      ocr_success_pct: 100,
      row_fail_pct: 0,
    }));

    expect(summary.pass).toBe(false);
    expect(summary.score_pct).toBeLessThan(95);
    expect(summary.hard_fail_reason).toBeUndefined();
  });

  it('hard-fails range_result_integrity to zero', () => {
    const summary = buildRunSlaSummary(buildRun({
      site: 'nyc_acris',
      failure_class: 'range_result_integrity',
    }));

    expect(summary.pass).toBe(false);
    expect(summary.score_pct).toBe(0);
    expect(summary.hard_fail_reason).toBe('range_result_integrity');
  });

  it('hard-fails Maricopa when artifact retrieval is disabled', () => {
    const summary = buildRunSlaSummary(buildRun({
      site: 'maricopa_recorder',
      id: 'sched_maricopa_1',
      idempotency_key: 'maricopa_recorder:2026-03-24:morning',
      slot_time: 'maricopa_recorder:2026-03-24:morning',
      artifact_retrieval_enabled: 0,
      artifact_fetch_coverage_pct: 0,
      amount_coverage_pct: 0,
    }));

    expect(summary.pass).toBe(false);
    expect(summary.score_pct).toBe(0);
    expect(summary.hard_fail_reason).toBe('maricopa_artifact_retrieval_disabled');
  });

  it('hard-fails Maricopa when readiness is stale', () => {
    const summary = buildRunSlaSummary(buildRun({
      site: 'maricopa_recorder',
      id: 'sched_maricopa_2',
      idempotency_key: 'maricopa_recorder:2026-03-24:afternoon',
      slot_time: 'maricopa_recorder:2026-03-24:afternoon',
      artifact_readiness_not_met: 1,
      artifact_fetch_coverage_pct: 50,
      amount_coverage_pct: 50,
    }));

    expect(summary.pass).toBe(false);
    expect(summary.score_pct).toBe(0);
    expect(summary.hard_fail_reason).toBe('maricopa_artifact_readiness_not_met');
  });

  it('hard-fails uploaded row mismatches to zero', () => {
    const summary = buildRunSlaSummary(buildRun({
      rows_uploaded: 8,
    }));

    expect(summary.pass).toBe(false);
    expect(summary.score_pct).toBe(0);
    expect(summary.hard_fail_reason).toBe('row_upload_mismatch');
  });
});
