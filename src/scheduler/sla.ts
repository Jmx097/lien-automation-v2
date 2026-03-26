import type { ScheduledRunRecord } from './store';

export const RUN_SLA_POLICY_VERSION = 'tri_site_composite_v1';

export interface RunSlaEvidence {
  source_publish_confirmed: boolean;
  master_sync_confirmed: boolean;
  source_tab_title_present: boolean;
  master_tab_title_present: boolean;
  review_tab_title_present: boolean;
  uploaded_rows_match_scraped_rows: boolean;
}

export interface RunSlaComponents {
  delivery_pct: number;
  integrity_pct: number;
  completeness_pct: number;
  extraction_pct: number;
}

export interface RunSlaSummary {
  score_pct: number;
  pass: boolean;
  policy_version: string;
  hard_fail_reason?: string;
  evidence: RunSlaEvidence;
  components: RunSlaComponents;
}

type RunSlaInput = Pick<
  ScheduledRunRecord,
  | 'site'
  | 'status'
  | 'records_scraped'
  | 'rows_uploaded'
  | 'amount_coverage_pct'
  | 'ocr_success_pct'
  | 'row_fail_pct'
  | 'retry_exhausted'
  | 'failure_class'
  | 'source_tab_title'
  | 'master_tab_title'
  | 'review_tab_title'
  | 'new_master_row_count'
  | 'purged_review_row_count'
  | 'current_run_quarantined_row_count'
  | 'current_run_conflict_row_count'
  | 'retained_prior_review_row_count'
  | 'master_fallback_used'
  | 'artifact_retrieval_enabled'
  | 'artifact_fetch_coverage_pct'
  | 'artifact_readiness_not_met'
>;

function roundMetric(value: number): number {
  return Number(value.toFixed(2));
}

function isZeroVolumeSuccess(run: RunSlaInput): boolean {
  return run.status === 'success' && run.records_scraped === 0 && run.rows_uploaded === 0;
}

export function buildRunSlaEvidence(run: RunSlaInput): RunSlaEvidence {
  const zeroVolumeSuccess = isZeroVolumeSuccess(run);
  const uploadedRowsMatchScrapedRows = run.rows_uploaded === run.records_scraped;
  const sourcePublishConfirmed = Boolean(
    zeroVolumeSuccess ||
    run.records_scraped === 0 ||
    run.source_tab_title ||
    (run.status === 'success' && uploadedRowsMatchScrapedRows)
  );
  const masterSyncConfirmed = Boolean(
    zeroVolumeSuccess ||
    run.records_scraped === 0 ||
    run.master_tab_title ||
    run.review_tab_title ||
    (run.new_master_row_count ?? 0) > 0 ||
    (run.purged_review_row_count ?? 0) > 0 ||
    (run.current_run_quarantined_row_count ?? 0) > 0 ||
    (run.current_run_conflict_row_count ?? 0) > 0 ||
    (run.retained_prior_review_row_count ?? 0) > 0 ||
    (run.master_fallback_used ?? 0) > 0
  );

  return {
    source_publish_confirmed: sourcePublishConfirmed,
    master_sync_confirmed: masterSyncConfirmed,
    source_tab_title_present: Boolean(run.source_tab_title),
    master_tab_title_present: Boolean(run.master_tab_title),
    review_tab_title_present: Boolean(run.review_tab_title),
    uploaded_rows_match_scraped_rows: uploadedRowsMatchScrapedRows,
  };
}

function getHardFailReason(run: RunSlaInput, evidence: RunSlaEvidence): string | undefined {
  if (run.status !== 'success') return 'run_not_successful';
  if (!evidence.uploaded_rows_match_scraped_rows) return 'row_upload_mismatch';
  if ((run.retry_exhausted ?? 0) > 0) return 'retry_budget_exhausted';
  if (run.failure_class === 'range_result_integrity') return 'range_result_integrity';
  if (!evidence.source_publish_confirmed) return 'source_publish_unconfirmed';
  if (!evidence.master_sync_confirmed) return 'master_sync_unconfirmed';
  if (run.site === 'maricopa_recorder' && (run.artifact_retrieval_enabled ?? 0) === 0) {
    return 'maricopa_artifact_retrieval_disabled';
  }
  if (run.site === 'maricopa_recorder' && (run.artifact_readiness_not_met ?? 0) === 1) {
    return 'maricopa_artifact_readiness_not_met';
  }
  return undefined;
}

function buildRunSlaComponents(run: RunSlaInput): RunSlaComponents {
  if (isZeroVolumeSuccess(run)) {
    return {
      delivery_pct: 100,
      integrity_pct: 100,
      completeness_pct: 100,
      extraction_pct: 100,
    };
  }

  const deliveryPct = run.records_scraped > 0
    ? (run.rows_uploaded / Math.max(run.records_scraped, 1)) * 100
    : 0;
  const integrityPct = Math.max(0, 100 - run.row_fail_pct);
  const completenessPct = run.site === 'maricopa_recorder'
    ? Math.min(run.amount_coverage_pct, run.artifact_fetch_coverage_pct ?? 0)
    : run.amount_coverage_pct;
  const extractionPct = run.site === 'maricopa_recorder'
    ? (run.artifact_fetch_coverage_pct ?? 0)
    : run.ocr_success_pct;

  return {
    delivery_pct: roundMetric(deliveryPct),
    integrity_pct: roundMetric(integrityPct),
    completeness_pct: roundMetric(completenessPct),
    extraction_pct: roundMetric(extractionPct),
  };
}

export function buildRunSlaSummary(run: RunSlaInput): RunSlaSummary {
  const evidence = buildRunSlaEvidence(run);
  const components = buildRunSlaComponents(run);
  const hardFailReason = getHardFailReason(run, evidence);
  const scorePct = roundMetric(
    components.delivery_pct * 0.25 +
    components.integrity_pct * 0.25 +
    components.completeness_pct * 0.30 +
    components.extraction_pct * 0.20
  );

  return {
    score_pct: hardFailReason ? 0 : scorePct,
    pass: !hardFailReason && scorePct >= 95,
    policy_version: RUN_SLA_POLICY_VERSION,
    hard_fail_reason: hardFailReason,
    evidence,
    components,
  };
}
