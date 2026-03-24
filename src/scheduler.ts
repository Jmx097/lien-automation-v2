import crypto from 'crypto';
import fs from 'fs/promises';
import path from 'path';
import { log } from './utils/logger';
import { scrapers } from './scraper/index';
import { probeCASOSResultCount } from './scraper/ca_sos_enhanced';
import { ScheduledRunStore, ScheduledRunRecord, type QualityAnomalyAlertRecord } from './scheduler/store';
import {
  classifyMaricopaFailure,
  classifyNYCAcrisFailure,
  createDefaultConnectivityState,
  getNextAllowedRunAt,
  markConnectivityAlerted,
  markConnectivityRecoveryAlerted,
  recordConnectivityFailure,
  recordConnectivitySuccess,
  shouldRunConnectivityProbe,
  shouldSendProlongedBlockedAlert,
  type NYCAcrisFailureClass,
  type SiteConnectivityFailureClass,
  type SiteConnectivityState,
} from './scheduler/connectivity';
import { debugNYCAcrisBootstrap, probeNYCAcrisConnectivity } from './scraper/nyc_acris';
import { probeMaricopaRecorderConnectivity } from './scraper/maricopa_recorder';
import { getMaricopaPersistedStateReadiness } from './scraper/maricopa_artifacts';
import { formatRunTabName, pushToSheetsForTab, syncMasterSheetTab } from './sheets/push';
import { sendNewLeadsNotification } from './notifications/email';
import type { LienRecord, ScrapeResult, ScrapeRunQualitySummary } from './types';
import { supportedSites, type SupportedSite } from './sites';
import type { BrowserTransportMode } from './browser/transport';

const LOOKBACK_DAYS = 7;
const MISSED_RUN_GRACE_MINUTES = 45;
const SCHEDULE_COOLDOWN_MINUTES = 10;
function getEnableScheduleIdempotency(): boolean {
  return process.env.ENABLE_SCHEDULE_IDEMPOTENCY === '1';
}

function getAmountMinCoveragePct(): number {
  return Number(process.env.AMOUNT_MIN_COVERAGE_PCT ?? '95');
}

function isScheduleAutoThrottleEnabled(): boolean {
  return process.env.SCHEDULE_AUTO_THROTTLE !== '0';
}

function getScheduleMaxRecords(): number {
  return Number(process.env.SCHEDULE_MAX_RECORDS ?? '75');
}

function getCAScheduleFallbackMaxRecords(): number {
  return Math.max(1, Number(process.env.SCHEDULE_CA_SOS_FALLBACK_MAX_RECORDS ?? '10'));
}

function getScheduleMaxRecordsFloor(): number {
  return Number(process.env.SCHEDULE_MAX_RECORDS_FLOOR ?? '75');
}

function getScheduleMaxRecordsCeiling(): number {
  return Number(process.env.SCHEDULE_MAX_RECORDS_CEILING ?? '75');
}

function getScheduleRunMaxAttempts(): number {
  return Math.max(1, Number(process.env.SCHEDULE_RUN_MAX_ATTEMPTS ?? '3'));
}

function getScheduleRunBaseDelayMs(): number {
  return Math.max(0, Number(process.env.SCHEDULE_RUN_BASE_DELAY_MS ?? '1000'));
}

function getScheduleRunMaxDelayMs(): number {
  return Math.max(getScheduleRunBaseDelayMs(), Number(process.env.SCHEDULE_RUN_MAX_DELAY_MS ?? '10000'));
}

function getScheduleAnomalyBaselineRuns(): number {
  return Math.max(3, Number(process.env.SCHEDULE_ANOMALY_BASELINE_RUNS ?? '5'));
}

function getScheduleAnomalyMinBaselineRuns(): number {
  return Math.max(1, Number(process.env.SCHEDULE_ANOMALY_MIN_BASELINE_RUNS ?? '3'));
}

function getScheduleAnomalyRecordsDropPct(): number {
  return Math.max(0, Number(process.env.SCHEDULE_ANOMALY_RECORDS_DROP_PCT ?? '40'));
}

function getScheduleAnomalyAmountCoverageDropPts(): number {
  return Math.max(0, Number(process.env.SCHEDULE_ANOMALY_AMOUNT_COVERAGE_DROP_PTS ?? '15'));
}

function getScheduleAnomalyOcrSuccessDropPts(): number {
  return Math.max(0, Number(process.env.SCHEDULE_ANOMALY_OCR_SUCCESS_DROP_PTS ?? '20'));
}

function getScheduleAnomalyRowFailRisePts(): number {
  return Math.max(0, Number(process.env.SCHEDULE_ANOMALY_ROW_FAIL_RISE_PTS ?? '20'));
}

export function isScheduleFailureInjectionEnabled(): boolean {
  return process.env.ENABLE_SCHEDULE_FAILURE_INJECTION === '1';
}

export type Slot = 'morning' | 'afternoon' | 'evening';
type TriggerSource = 'external' | 'manual';
export type RetryableScheduledFailureClass =
  | 'timeout_or_navigation'
  | 'viewer_roundtrip'
  | 'token_or_session_state'
  | 'sheet_export';

interface SiteScheduleConfig {
  site: SupportedSite;
  timezone: string;
  days: string[];
  triggerHour: number;
  triggerMinute: number;
  finishByHour: number;
  finishByMinute: number;
  triggerLeadMinutes: number;
  maxRecords: number;
  slot: Slot;
}

const DEFAULT_WEEKDAYS = 'MO,TU,WE,TH,FR';
const DEFAULT_TIMEZONE = 'America/Denver';

export interface ScheduledRun extends ScheduledRunRecord {
  duplicate_of?: string;
  cooldown_of?: string;
  confidence?: RunConfidenceSummary;
  debug_artifact?: unknown;
}

export interface RunConfidenceSummary {
  status: 'high' | 'medium' | 'low';
  reasons: string[];
  evidence: {
    source_publish_confirmed: boolean;
    master_sync_confirmed: boolean;
    source_tab_title_present: boolean;
    master_tab_title_present: boolean;
    review_tab_title_present: boolean;
    uploaded_rows_match_scraped_rows: boolean;
  };
  metrics: {
    records_scraped: number;
    rows_uploaded: number;
    amount_coverage_pct: number;
    ocr_success_pct: number;
    row_fail_pct: number;
    partial: number;
    retry_exhausted: number;
    quarantined_row_count: number;
    current_run_quarantined_row_count: number;
    current_run_conflict_row_count: number;
    retained_prior_review_row_count: number;
    filtered_out_count: number;
    artifact_fetch_coverage_pct: number;
    artifact_readiness_not_met: number;
    enrichment_mode: string;
    enriched_record_count: number;
    partial_record_count: number;
    new_master_row_count: number;
    purged_review_row_count: number;
  };
}

export interface SiteScheduleState {
  effective_max_records: number;
  target_amount_coverage_pct: number;
  auto_throttle: boolean;
  recent_run_count: number;
  latest_run_started_at?: string;
  latest_run_confidence_status?: RunConfidenceSummary['status'];
  latest_run_confidence_reasons?: string[];
  connectivity: {
    status: SiteConnectivityState['status'];
    next_probe_at?: string;
    next_allowed_run_at?: string;
    last_failure_reason?: string;
    last_success_at?: string;
  };
  recent_quality: Array<{
    id: string;
    started_at: string;
    amount_coverage_pct: number;
    ocr_success_pct: number;
    row_fail_pct: number;
    partial: number;
    deadline_hit: number;
    effective_max_records: number;
  }>;
  latest_anomaly?: {
    run_id: string;
    idempotency_key: string;
    slot: Slot;
    metrics_triggered: string[];
    summary: string;
    detected_at: string;
  };
}

export type ScheduleState = Record<SupportedSite, SiteScheduleState>;

interface RunScheduledScrapeOptions {
  site?: SupportedSite;
  idempotencyKey?: string;
  slot?: Slot;
  triggerSource?: TriggerSource;
  testFailureClass?: RetryableScheduledFailureClass;
  debugBootstrapOnly?: boolean;
  transportModeOverride?: BrowserTransportMode;
}

interface QualityAnomalyBaseline {
  records_scraped: number;
  amount_coverage_pct: number;
  ocr_success_pct: number;
  row_fail_pct: number;
  sample_size: number;
}

interface QualityAnomalyEvaluation {
  metricsTriggered: string[];
  baseline: QualityAnomalyBaseline;
  summary: string;
}

let storeInstance: ScheduledRunStore | null = null;
const NYC_CACHE_DIR = path.resolve(process.cwd(), 'out', 'acris', 'scheduled-cache');

function getStore(): ScheduledRunStore {
  if (!storeInstance) storeInstance = new ScheduledRunStore();
  return storeInstance;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function sleep(ms: number): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatClock(hour: number, minute: number): string {
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
}

function subtractMinutes(hour: number, minute: number, minutes: number): { hour: number; minute: number } {
  const totalMinutes = ((hour * 60 + minute - minutes) % 1440 + 1440) % 1440;
  return {
    hour: Math.floor(totalMinutes / 60),
    minute: totalMinutes % 60,
  };
}

function formatDate(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function sitePrefix(site: SupportedSite): string {
  return `SCHEDULE_${site.toUpperCase()}`;
}

function getSiteEnv(site: SupportedSite, suffix: string, legacyName?: string): string | undefined {
  return process.env[`${sitePrefix(site)}_${suffix}`] ?? (legacyName ? process.env[legacyName] : undefined);
}

function getSiteSchedules(site: SupportedSite): SiteScheduleConfig[] {
  const timezone = getSiteEnv(site, 'TIMEZONE') ?? DEFAULT_TIMEZONE;
  const days = (getSiteEnv(site, 'WEEKLY_DAYS') ?? DEFAULT_WEEKDAYS)
    .split(',')
    .map((value) => value.trim().toUpperCase());
  const maxRecords = Number(getSiteEnv(site, 'MAX_RECORDS') ?? process.env.ACRIS_INITIAL_MAX_RECORDS ?? getScheduleMaxRecords());
  const triggerLeadMinutes = Number(getSiteEnv(site, 'TRIGGER_LEAD_MINUTES') ?? (site === 'ca_sos' ? '180' : '0'));

  const slotTimes: Array<{ slot: Slot; finishByHour: number; finishByMinute: number }> = [
    {
      slot: 'morning',
      finishByHour: Number(getSiteEnv(site, 'MORNING_RUN_HOUR') ?? '10'),
      finishByMinute: Number(getSiteEnv(site, 'MORNING_RUN_MINUTE') ?? '0'),
    },
    {
      slot: 'afternoon',
      finishByHour: Number(getSiteEnv(site, 'AFTERNOON_RUN_HOUR') ?? '14'),
      finishByMinute: Number(getSiteEnv(site, 'AFTERNOON_RUN_MINUTE') ?? '0'),
    },
    {
      slot: 'evening',
      finishByHour: Number(getSiteEnv(site, 'EVENING_RUN_HOUR') ?? '22'),
      finishByMinute: Number(getSiteEnv(site, 'EVENING_RUN_MINUTE') ?? '0'),
    },
  ];

  return slotTimes.map(({ slot, finishByHour, finishByMinute }) => {
    const triggerTime = subtractMinutes(finishByHour, finishByMinute, triggerLeadMinutes);
    return {
      site,
      timezone,
      days,
      triggerHour: triggerTime.hour,
      triggerMinute: triggerTime.minute,
      finishByHour,
      finishByMinute,
      triggerLeadMinutes,
      maxRecords,
      slot,
    };
  });
}

function getSiteSchedule(site: SupportedSite, slot?: Slot): SiteScheduleConfig {
  const schedules = getSiteSchedules(site);
  if (slot) return schedules.find((config) => config.slot === slot) ?? schedules[0];

  const now = getDateParts(new Date(), schedules[0].timezone);
  if (now.hour < schedules[1].finishByHour) return schedules[0];
  if (now.hour < schedules[2].finishByHour) return schedules[1];
  return schedules[2];
}

function getScheduleForSlot(site: SupportedSite, slot: Slot): SiteScheduleConfig {
  return getSiteSchedules(site).find((config) => config.slot === slot) ?? getSiteSchedules(site)[0];
}

function getDateParts(now: Date, timeZone: string): { year: string; month: string; day: string; hour: number; minute: number; weekday: string } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    weekday: 'short',
    hour12: false,
  }).formatToParts(now);

  const pick = (type: string): string => parts.find((part) => part.type === type)?.value ?? '00';

  return {
    year: pick('year'),
    month: pick('month'),
    day: pick('day'),
    hour: Number(pick('hour')),
    minute: Number(pick('minute')),
    weekday: pick('weekday').slice(0, 2).toUpperCase(),
  };
}

function isScheduledDay(config: SiteScheduleConfig, now: Date): boolean {
  return config.days.includes(getDateParts(now, config.timezone).weekday);
}

function buildDefaultIdempotencyKey(site: SupportedSite, now: Date, slot: Slot): string {
  const config = getScheduleForSlot(site, slot);
  const parts = getDateParts(now, config.timezone);
  return `${site}:${parts.year}-${parts.month}-${parts.day}:${slot}`;
}

function buildDeadlineIso(site: SupportedSite, slot: Slot, now: Date): string {
  const config = getScheduleForSlot(site, slot);
  const parts = getDateParts(now, config.timezone);
  return `${parts.year}-${parts.month}-${parts.day}T${String(config.finishByHour).padStart(2, '0')}:${String(config.finishByMinute).padStart(2, '0')}:00 ${config.timezone}`;
}

function isPastDeadline(site: SupportedSite, slot: Slot, now: Date): boolean {
  const config = getScheduleForSlot(site, slot);
  const parts = getDateParts(now, config.timezone);
  if (parts.hour > config.finishByHour) return true;
  if (parts.hour === config.finishByHour && parts.minute >= config.finishByMinute) return true;
  return false;
}

function getLast7DaysRange(): { date_start: string; date_end: string } {
  const now = new Date();
  const end = new Date(now);
  const start = new Date(now);
  start.setDate(start.getDate() - LOOKBACK_DAYS);
  return { date_start: formatDate(start), date_end: formatDate(end) };
}

function computeQualityMetrics(
  records: any[],
  effectiveMaxRecords: number,
  deadlineHit: boolean,
  qualitySummary?: ScrapeRunQualitySummary,
) {
  const amountFound = records.filter((row) => Boolean(row.amount)).length;
  const amountMissing = Math.max(records.length - amountFound, 0);
  const amountCoveragePct = records.length > 0 ? (amountFound / records.length) * 100 : 0;
  const ocrSuccessCount = records.filter((row) => row.amount_reason !== 'ocr_missing' && row.amount_reason !== 'ocr_error').length;
  const ocrSuccessPct = records.length > 0 ? (ocrSuccessCount / records.length) * 100 : 0;
  const discoveredCount = qualitySummary?.discovered_count ?? effectiveMaxRecords;
  const returnedCount = qualitySummary?.returned_count ?? records.length;
  const rowFailPct = discoveredCount > 0 ? ((discoveredCount - returnedCount) / discoveredCount) * 100 : 0;
  const artifactFetchCoveragePct = records.length > 0
    ? ((qualitySummary?.enriched_records ?? 0) / records.length) * 100
    : 0;

  return {
    amountFound,
    amountMissing,
    amountCoveragePct,
    ocrSuccessPct,
    rowFailPct: Math.max(rowFailPct, 0),
    partial: deadlineHit || qualitySummary?.partial_run || returnedCount < effectiveMaxRecords ? 1 : 0,
    discoveredCount,
    returnedCount,
    partialReason: qualitySummary?.partial_reason,
    requestedDateStart: qualitySummary?.requested_date_start,
    requestedDateEnd: qualitySummary?.requested_date_end,
    quarantinedCount: qualitySummary?.quarantined_count ?? 0,
    recordsSkipped: qualitySummary?.skipped_existing_count ?? 0,
    filteredOutCount: qualitySummary?.filtered_out_count ?? 0,
    returnedMinFilingDate: qualitySummary?.returned_min_filing_date,
    returnedMaxFilingDate: qualitySummary?.returned_max_filing_date,
    upstreamMinFilingDate: qualitySummary?.upstream_min_filing_date,
    upstreamMaxFilingDate: qualitySummary?.upstream_max_filing_date,
    artifactRetrievalEnabled: qualitySummary?.artifact_retrieval_enabled === true ? 1 : 0,
    artifactFetchCoveragePct,
    enrichmentMode: qualitySummary?.enrichment_mode ?? 'artifact_enriched',
    artifactReadinessNotMet: qualitySummary?.artifact_readiness_not_met === true ? 1 : 0,
    debugArtifact: qualitySummary?.debug_artifact,
    enrichedRecordCount: qualitySummary?.enriched_records ?? 0,
    partialRecordCount: qualitySummary?.partial_records ?? 0,
  };
}

function roundMetric(value: number): number {
  return Number(value.toFixed(2));
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function parseJsonObject(value?: string): Record<string, number> {
  if (!value) return {};
  try {
    const parsed = JSON.parse(value) as Record<string, unknown>;
    return Object.fromEntries(
      Object.entries(parsed).map(([key, entry]) => [key, typeof entry === 'number' ? entry : Number(entry ?? 0)])
    );
  } catch {
    return {};
  }
}

function normalizeReviewReasonCounts(value?: string): Record<string, number> {
  const counts = parseJsonObject(value);
  return {
    low_confidence: counts.low_confidence ?? 0,
    conflict_ambiguous: counts.conflict_ambiguous ?? 0,
    duplicate_or_existing:
      (counts.duplicate_or_existing ?? 0) +
      (counts.duplicate_against_current_run ?? 0) +
      (counts.duplicate_against_retained_review ?? 0),
    partial_run: counts.partial_run ?? 0,
  };
}

function normalizeReviewReasonCountsFromObject(value?: Record<string, unknown>): Record<string, number> {
  return normalizeReviewReasonCounts(value ? JSON.stringify(value) : undefined);
}

function parseDebugArtifact(value?: string): unknown {
  if (!value) return undefined;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function shouldPersistFailureDebugArtifact(site: SupportedSite, failureClass?: string): boolean {
  return site === 'nyc_acris' && Boolean(
    failureClass &&
    ['range_result_integrity', 'transport_or_bootstrap', 'viewer_roundtrip'].includes(failureClass)
  );
}

function buildNYCScheduledFailureDebugArtifact(run: ScheduledRunRecord, err: unknown): Record<string, unknown> | undefined {
  if (!shouldPersistFailureDebugArtifact(run.site, run.failure_class)) return undefined;

  const context = err as {
    requestedStart?: string;
    requestedEnd?: string;
    upstreamMin?: string;
    upstreamMax?: string;
    returnedRowCount?: number;
    debugArtifact?: unknown;
  };
  const existing = context.debugArtifact;
  if (existing && typeof existing === 'object') {
    return {
      ...(existing as Record<string, unknown>),
      failure_class: run.failure_class,
      requested_date_start: run.requested_date_start,
      requested_date_end: run.requested_date_end,
      upstream_min_filing_date: run.upstream_min_filing_date,
      upstream_max_filing_date: run.upstream_max_filing_date,
      filtered_out_count: run.filtered_out_count,
      partial_reason: run.partial_reason,
    };
  }

  return {
    failure_class: run.failure_class,
    requested_date_start: context.requestedStart ?? run.requested_date_start,
    requested_date_end: context.requestedEnd ?? run.requested_date_end,
    upstream_min_filing_date: context.upstreamMin ?? run.upstream_min_filing_date,
    upstream_max_filing_date: context.upstreamMax ?? run.upstream_max_filing_date,
    filtered_out_count: context.returnedRowCount ?? run.filtered_out_count,
    partial_reason: run.partial_reason,
    error: String((err as any)?.message ?? err ?? ''),
  };
}

function buildRunConfidence(run: ScheduledRunRecord): RunConfidenceSummary {
  const reasons: string[] = [];
  const normalizedReviewReasons = normalizeReviewReasonCounts(run.review_reason_counts_json);
  const sourcePublishConfirmed = Boolean(
    run.records_scraped === 0 ||
    (
      Boolean(run.source_tab_title) ||
      (run.status === 'success' && run.rows_uploaded === run.records_scraped && run.rows_uploaded > 0)
    )
  );
  const masterSyncConfirmed = Boolean(
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
  const uploadedRowsMatchScrapedRows = run.rows_uploaded === run.records_scraped;

  if (run.status !== 'success') reasons.push('run_not_successful');
  if (!sourcePublishConfirmed && run.records_scraped > 0) reasons.push('source_tab_missing');
  if (run.partial === 1) reasons.push('partial_run');
  if ((run.retry_exhausted ?? 0) > 0) reasons.push('retry_budget_exhausted');
  if ((run.master_fallback_used ?? 0) > 0) reasons.push('master_publish_fallback_active');
  if ((run.anomaly_detected ?? 0) > 0) reasons.push('quality_anomaly_detected');
  if (!uploadedRowsMatchScrapedRows) reasons.push('row_upload_mismatch');
  if (run.records_scraped > 0 && !masterSyncConfirmed) reasons.push('master_tab_missing');
  if (run.failure_class === 'range_result_integrity') reasons.push('range_result_integrity');
  if (run.records_scraped > 0 && run.amount_coverage_pct < getAmountMinCoveragePct()) reasons.push('amount_coverage_below_target');
  if (run.records_scraped > 0 && run.ocr_success_pct < 80) reasons.push('ocr_success_below_floor');
  if (((run.current_run_quarantined_row_count ?? 0) + (run.current_run_conflict_row_count ?? 0)) > run.records_scraped && run.records_scraped > 0) {
    reasons.push('quarantine_exceeds_scraped_rows');
  }
  if ((run.current_run_conflict_row_count ?? 0) > 0 || normalizedReviewReasons.conflict_ambiguous > 0) {
    reasons.push('conflict_ambiguous');
  }
  if (normalizedReviewReasons.duplicate_or_existing > 0) reasons.push('duplicate_or_existing');
  if (normalizedReviewReasons.low_confidence > 0 && (run.current_run_quarantined_row_count ?? 0) > 0) {
    reasons.push('low_confidence_extraction_review');
  }
  if (run.site === 'maricopa_recorder' && (run.artifact_retrieval_enabled ?? 0) === 0 && run.records_scraped > 0) {
    reasons.push('artifact_retrieval_disabled');
  }
  if (run.site === 'maricopa_recorder' && (run.artifact_readiness_not_met ?? 0) === 1) {
    reasons.push('artifact_readiness_not_met');
  }
  if (
    run.site === 'maricopa_recorder' &&
    run.records_scraped > 0 &&
    (run.artifact_retrieval_enabled ?? 0) === 1 &&
    (run.artifact_fetch_coverage_pct ?? 0) < Number(process.env.MARICOPA_ARTIFACT_FETCH_COVERAGE_TARGET_PCT ?? '80')
  ) {
    reasons.push('artifact_fetch_coverage_below_target');
  }

  let status: RunConfidenceSummary['status'] = 'high';
  if (
    reasons.some((reason) => [
      'run_not_successful',
      'source_tab_missing',
      'row_upload_mismatch',
      'master_tab_missing',
      'retry_budget_exhausted',
      'master_publish_fallback_active',
      'range_result_integrity',
    ].includes(reason))
  ) {
    status = 'low';
  } else if (reasons.length > 0) {
    status = 'medium';
  }
  if (run.site === 'maricopa_recorder' && (run.artifact_retrieval_enabled ?? 0) === 0 && status === 'high') {
    status = 'medium';
  }

  return {
    status,
    reasons,
    evidence: {
      source_publish_confirmed: sourcePublishConfirmed,
      master_sync_confirmed: masterSyncConfirmed,
      source_tab_title_present: Boolean(run.source_tab_title),
      master_tab_title_present: Boolean(run.master_tab_title),
      review_tab_title_present: Boolean(run.review_tab_title),
      uploaded_rows_match_scraped_rows: uploadedRowsMatchScrapedRows,
    },
    metrics: {
      records_scraped: run.records_scraped,
      rows_uploaded: run.rows_uploaded,
      amount_coverage_pct: run.amount_coverage_pct,
      ocr_success_pct: run.ocr_success_pct,
      row_fail_pct: run.row_fail_pct,
      partial: run.partial,
      retry_exhausted: run.retry_exhausted ?? 0,
      quarantined_row_count: run.quarantined_row_count ?? 0,
      current_run_quarantined_row_count: run.current_run_quarantined_row_count ?? 0,
      current_run_conflict_row_count: run.current_run_conflict_row_count ?? 0,
      retained_prior_review_row_count: run.retained_prior_review_row_count ?? 0,
      filtered_out_count: run.filtered_out_count ?? 0,
      artifact_fetch_coverage_pct: run.artifact_fetch_coverage_pct ?? 0,
      artifact_readiness_not_met: run.artifact_readiness_not_met ?? 0,
      enrichment_mode: run.enrichment_mode ?? '',
      enriched_record_count: run.enriched_record_count ?? 0,
      partial_record_count: run.partial_record_count ?? 0,
      new_master_row_count: run.new_master_row_count ?? 0,
      purged_review_row_count: run.purged_review_row_count ?? 0,
    },
  };
}

async function evaluateQualityAnomaly(site: SupportedSite, currentRun: ScheduledRun): Promise<QualityAnomalyEvaluation | null> {
  const recent = await getStore().getRecentSuccessfulRuns(site, getScheduleAnomalyBaselineRuns() + 1);
  const eligibleBaseline = recent
    .filter((run) => run.id !== currentRun.id && run.partial === 0 && run.deadline_hit === 0)
    .slice(0, getScheduleAnomalyBaselineRuns());

  if (eligibleBaseline.length < getScheduleAnomalyMinBaselineRuns()) {
    log({
      stage: 'scheduled_run_anomaly_skipped',
      site,
      run_id: currentRun.id,
      idempotency_key: currentRun.idempotency_key,
      reason: 'insufficient_baseline',
      baseline_sample_size: eligibleBaseline.length,
      min_baseline_runs: getScheduleAnomalyMinBaselineRuns(),
    });
    return null;
  }

  const baseline: QualityAnomalyBaseline = {
    records_scraped: roundMetric(average(eligibleBaseline.map((run) => run.records_scraped))),
    amount_coverage_pct: roundMetric(average(eligibleBaseline.map((run) => run.amount_coverage_pct))),
    ocr_success_pct: roundMetric(average(eligibleBaseline.map((run) => run.ocr_success_pct))),
    row_fail_pct: roundMetric(average(eligibleBaseline.map((run) => run.row_fail_pct))),
    sample_size: eligibleBaseline.length,
  };

  const metricsTriggered: string[] = [];
  const recordsDropPct = baseline.records_scraped > 0
    ? ((baseline.records_scraped - currentRun.records_scraped) / baseline.records_scraped) * 100
    : 0;
  if (baseline.records_scraped > 0 && recordsDropPct >= getScheduleAnomalyRecordsDropPct()) {
    metricsTriggered.push('records_scraped');
  }
  if ((baseline.amount_coverage_pct - currentRun.amount_coverage_pct) >= getScheduleAnomalyAmountCoverageDropPts()) {
    metricsTriggered.push('amount_coverage_pct');
  }
  if ((baseline.ocr_success_pct - currentRun.ocr_success_pct) >= getScheduleAnomalyOcrSuccessDropPts()) {
    metricsTriggered.push('ocr_success_pct');
  }
  if ((currentRun.row_fail_pct - baseline.row_fail_pct) >= getScheduleAnomalyRowFailRisePts()) {
    metricsTriggered.push('row_fail_pct');
  }

  if (metricsTriggered.length === 0) return null;

  return {
    metricsTriggered,
    baseline,
    summary: `Quality anomaly for ${site}: ${metricsTriggered.join(', ')}`,
  };
}

function isConnectivityManagedSite(site: SupportedSite): boolean {
  return site === 'nyc_acris' || site === 'maricopa_recorder';
}

function isRetryableScheduledFailure(site: SupportedSite, failureClass: SiteConnectivityFailureClass, errorMessage: string): boolean {
  if (site === 'maricopa_recorder') {
    return failureClass === 'challenge_or_interstitial' || failureClass === 'artifact_fetch_failed';
  }

  if (
    failureClass === 'policy_block' ||
    failureClass === 'selector_or_empty_results' ||
    failureClass === 'range_result_integrity'
  ) {
    return false;
  }

  if (failureClass === 'sheet_export') {
    return !/sheet_upload_mismatch/i.test(errorMessage);
  }

  return (
    failureClass === 'timeout_or_navigation' ||
    failureClass === 'viewer_roundtrip' ||
    failureClass === 'token_or_session_state'
  );
}

function getRetryDelayMs(attempt: number): number {
  const raw = getScheduleRunBaseDelayMs() * Math.pow(2, Math.max(attempt - 1, 0));
  return Math.min(raw, getScheduleRunMaxDelayMs());
}

function buildInjectedFailureError(failureClass: RetryableScheduledFailureClass): Error {
  switch (failureClass) {
    case 'timeout_or_navigation':
      return new Error('Injected test failure: navigation timeout while loading scheduled scrape results');
    case 'viewer_roundtrip':
      return new Error('Injected test failure: viewer did not return to acris result page');
    case 'token_or_session_state':
      return new Error('Injected test failure: session expired while opening scheduled scrape results');
    case 'sheet_export':
      return new Error('Injected test failure: googleapis sheets 503');
  }
}

function getSiteRecordBounds(site: SupportedSite): { min: number; max: number } {
  const configuredMax = getSiteSchedule(site).maxRecords;
  return {
    min: clamp(configuredMax, getScheduleMaxRecordsFloor(), getScheduleMaxRecordsCeiling()),
    max: clamp(configuredMax, getScheduleMaxRecordsFloor(), getScheduleMaxRecordsCeiling()),
  };
}

async function resolveEffectiveMaxRecords(site: SupportedSite): Promise<number> {
  const siteDefault = getSiteSchedule(site).maxRecords;
  const state = await getStore().getControlState(site);
  const seeded = state?.effective_max_records ?? siteDefault;
  const bounds = getSiteRecordBounds(site);
  return clamp(seeded, bounds.min, bounds.max);
}

async function hasNYCStableSuccessStreak(required = 3): Promise<boolean> {
  const recent = await getStore().getRecentSuccessfulRuns('nyc_acris', required);
  if (recent.length < required) return false;

  return recent.every((run) =>
    run.status === 'success' &&
    run.records_scraped >= 2 &&
    run.rows_uploaded === run.records_scraped &&
    run.partial === 0 &&
    run.failure_class !== 'policy_block'
  );
}

function isAutoThrottleEnabled(site: SupportedSite): boolean {
  return isScheduleAutoThrottleEnabled() && site !== 'ca_sos';
}

async function maybeAdjustEffectiveMaxRecords(site: SupportedSite, current: number, currentCoveragePct: number): Promise<number> {
  if (!isAutoThrottleEnabled(site)) return current;
  const bounds = getSiteRecordBounds(site);

  if (site === 'nyc_acris' && !(await hasNYCStableSuccessStreak(3))) {
    return clamp(current, bounds.min, bounds.max);
  }

  if (currentCoveragePct < getAmountMinCoveragePct()) {
    return clamp(Math.floor(current * 0.8), bounds.min, bounds.max);
  }

  const recent = await getStore().getRecentSuccessfulRuns(site, 2);
  const streak = [currentCoveragePct, ...recent.map((run) => run.amount_coverage_pct)];
  if (streak.length >= 3 && streak.every((pct) => pct >= getAmountMinCoveragePct() + 2)) {
    return clamp(Math.floor(current * 1.1), bounds.min, bounds.max);
  }

  return current;
}

async function getConnectivityState(site: SupportedSite): Promise<SiteConnectivityState> {
  return await getStore().getConnectivityState(site) ?? createDefaultConnectivityState(site);
}

function getNYCCachePath(idempotencyKey: string): string {
  return path.join(NYC_CACHE_DIR, `${idempotencyKey.replace(/[^\w-]+/g, '_')}.json`);
}

async function saveNYCCachedRecords(idempotencyKey: string, records: LienRecord[]): Promise<void> {
  await fs.mkdir(NYC_CACHE_DIR, { recursive: true });
  await fs.writeFile(getNYCCachePath(idempotencyKey), JSON.stringify(records, null, 2), 'utf8');
}

async function loadNYCCachedRecords(idempotencyKey: string): Promise<LienRecord[] | null> {
  try {
    const raw = await fs.readFile(getNYCCachePath(idempotencyKey), 'utf8');
    return JSON.parse(raw) as LienRecord[];
  } catch {
    return null;
  }
}

async function clearNYCCachedRecords(idempotencyKey: string): Promise<void> {
  await fs.rm(getNYCCachePath(idempotencyKey), { force: true });
}

async function sendMissedRunAlert(site: SupportedSite, slot: Slot, expectedAtIso: string, key: string): Promise<void> {
  const webhook = process.env.SCHEDULE_ALERT_WEBHOOK_URL;
  if (!webhook) {
    log({ stage: 'missed_run_alert_log_only', site, slot, expected_at: expectedAtIso, idempotency_key: key });
    return;
  }

  try {
    const payload = {
      text: `Missed scheduled scrape run for ${site} ${slot}. Expected success by ${expectedAtIso}. idempotency_key=${key}`,
      site,
      slot,
      expected_at: expectedAtIso,
      idempotency_key: key,
    };

    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const body = await res.text();
      log({ stage: 'missed_run_alert_failed', site, slot, status: res.status, response: body });
    }
  } catch (err: any) {
    log({ stage: 'missed_run_alert_error', site, slot, error: String(err?.message ?? err) });
  }
}

async function sendConnectivityAlert(site: SupportedSite, state: SiteConnectivityState, reason: 'blocked' | 'recovered' | 'blocked_4h'): Promise<void> {
  const webhook = process.env.SCHEDULE_ALERT_WEBHOOK_URL;
  const label = reason === 'blocked' ? 'entered blocked state' : reason === 'recovered' ? 'recovered' : 'remains blocked for over 4 hours';
  const text = `Connectivity alert for ${site}: ${label}. status=${state.status} next_probe_at=${state.next_probe_at ?? 'n/a'} reason=${state.last_failure_reason ?? 'n/a'}`;

  if (!webhook) {
    log({ stage: 'connectivity_alert_log_only', site, reason, status: state.status, last_failure_reason: state.last_failure_reason });
    return;
  }

  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        text,
        site,
        reason,
        status: state.status,
        next_probe_at: state.next_probe_at,
        last_failure_reason: state.last_failure_reason,
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      log({ stage: 'connectivity_alert_failed', site, reason, status: res.status, response: body });
    }
  } catch (err: any) {
    log({ stage: 'connectivity_alert_error', site, reason, error: String(err?.message ?? err) });
  }
}

async function sendQualityAnomalyAlert(alert: QualityAnomalyAlertRecord): Promise<{ attempted: boolean; delivered: boolean }> {
  const webhook = process.env.SCHEDULE_ALERT_WEBHOOK_URL;
  if (!webhook) {
    return { attempted: false, delivered: false };
  }

  try {
    const res = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        text: alert.summary,
        site: alert.site,
        slot: alert.slot,
        run_id: alert.run_id,
        idempotency_key: alert.idempotency_key,
        metrics_triggered: alert.metrics_triggered,
        detected_at: alert.detected_at,
        baseline: {
          records_scraped: alert.baseline_records_scraped,
          amount_coverage_pct: alert.baseline_amount_coverage_pct,
          ocr_success_pct: alert.baseline_ocr_success_pct,
          row_fail_pct: alert.baseline_row_fail_pct,
        },
        current: {
          records_scraped: alert.records_scraped,
          amount_coverage_pct: alert.amount_coverage_pct,
          ocr_success_pct: alert.ocr_success_pct,
          row_fail_pct: alert.row_fail_pct,
        },
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      log({ stage: 'quality_anomaly_alert_failed', site: alert.site, run_id: alert.run_id, status: res.status, response: body });
      return { attempted: true, delivered: false };
    }

    return { attempted: true, delivered: true };
  } catch (err: any) {
    log({ stage: 'quality_anomaly_alert_error', site: alert.site, run_id: alert.run_id, error: String(err?.message ?? err) });
    return { attempted: true, delivered: false };
  }
}

async function applyConnectivityFailure(site: SupportedSite, failureClass: SiteConnectivityFailureClass, reason: string): Promise<void> {
  if (!isConnectivityManagedSite(site)) return;
  const outcome = recordConnectivityFailure(await getConnectivityState(site), reason, failureClass);
  let state = outcome.state;
  if (outcome.becameBlocked) {
    await sendConnectivityAlert(site, state, 'blocked');
    state = markConnectivityAlerted(state);
  }

  await getStore().upsertConnectivityState(state);
}

async function applyConnectivitySuccess(site: SupportedSite, mode: 'probe' | 'run'): Promise<void> {
  if (!isConnectivityManagedSite(site)) return;
  const outcome = recordConnectivitySuccess(await getConnectivityState(site), mode);
  let state = outcome.state;
  if (outcome.recovered) {
    await sendConnectivityAlert(site, state, 'recovered');
    state = markConnectivityRecoveryAlerted(state);
  }

  await getStore().upsertConnectivityState(state);
}

function deriveFailureClass(site: SupportedSite, err: unknown): SiteConnectivityFailureClass {
  const message = String((err as any)?.message ?? err ?? '');
  return site === 'maricopa_recorder'
    ? classifyMaricopaFailure(message)
    : classifyNYCAcrisFailure(message);
}

function applyFailureDiagnostics(run: ScheduledRunRecord, err: unknown): void {
  if (run.site !== 'nyc_acris' || run.failure_class !== 'range_result_integrity') return;

  const errorWithContext = err as {
    requestedStart?: string;
    requestedEnd?: string;
    upstreamMin?: string;
    upstreamMax?: string;
    returnedRowCount?: number;
  };

  if (errorWithContext.requestedStart) run.requested_date_start = errorWithContext.requestedStart;
  if (errorWithContext.requestedEnd) run.requested_date_end = errorWithContext.requestedEnd;
  if (errorWithContext.upstreamMin) run.upstream_min_filing_date = errorWithContext.upstreamMin;
  if (errorWithContext.upstreamMax) run.upstream_max_filing_date = errorWithContext.upstreamMax;
  if (typeof errorWithContext.returnedRowCount === 'number') {
    run.filtered_out_count = errorWithContext.returnedRowCount;
    run.discovered_count = errorWithContext.returnedRowCount;
    run.returned_count = 0;
  }
  run.partial_reason = run.partial_reason ?? 'rows_filtered_outside_requested_range';
}

function buildNYCDebugErrorArtifact(artifact: Awaited<ReturnType<typeof debugNYCAcrisBootstrap>>): string {
  return JSON.stringify({
    mode: 'nyc_bootstrap_debug',
    requestedTransportMode: artifact.requestedTransportMode,
    transportMode: artifact.transportMode,
    transportPolicyPurpose: artifact.transportPolicyPurpose,
    ok: artifact.ok,
    detail: artifact.detail,
    failureClass: artifact.failureClass,
    recoveryAction: artifact.recoveryAction,
    bootstrapStrategy: artifact.bootstrapStrategy,
    diagnostic: artifact.diagnostic,
    bootstrapTrace: artifact.bootstrapTrace,
    bootstrapLifecycle: artifact.bootstrapLifecycle,
    transportDiagnostics: artifact.transportDiagnostics,
    warnings: artifact.warnings,
    failures: artifact.failures,
  });
}

function shouldUseCachedNYCRows(site: SupportedSite, previousFailureClass?: string): boolean {
  return site === 'nyc_acris' && previousFailureClass === 'sheet_export';
}

async function getRecordsForScheduledRun(
  site: SupportedSite,
  idempotencyKey: string,
  date_start: string,
  date_end: string,
  effectiveMaxRecords: number,
  slot: Slot,
  previousFailureClass?: string,
) {
  if (shouldUseCachedNYCRows(site, previousFailureClass)) {
    const cached = await loadNYCCachedRecords(idempotencyKey);
    if (cached && cached.length > 0) {
      log({ stage: 'scheduled_run_cached_records_reused', site, idempotency_key: idempotencyKey, records: cached.length });
      return { records: cached, reusedCache: true, qualitySummary: undefined as ScrapeRunQualitySummary | undefined };
    }
  }

  const connectivityAtStart = await getConnectivityState(site);
  const records = await scrapers[site]({
    date_start,
    date_end,
    max_records: effectiveMaxRecords,
    stop_requested: () => isPastDeadline(site, slot, new Date()),
    connectivity_status_at_start: connectivityAtStart.status,
  } as any) as ScrapeResult;

  return { records, reusedCache: false, qualitySummary: records.quality_summary };
}

export async function checkSiteConnectivity(): Promise<void> {
  const sites: SupportedSite[] = ['nyc_acris', 'maricopa_recorder'];

  for (const site of sites) {
    let state = await getConnectivityState(site);

    if (site === 'maricopa_recorder') {
      const readiness = await getMaricopaPersistedStateReadiness();
      if (readiness.refreshRequired) {
        const failureClass = readiness.refreshReason === 'artifact_candidates_missing'
          ? 'artifact_candidates_missing'
          : 'session_missing_or_stale';
        await applyConnectivityFailure(site, failureClass, readiness.detail);
        state = await getConnectivityState(site);
      }
    }

    if (shouldRunConnectivityProbe(state)) {
      if (site === 'nyc_acris') {
        const probe = await probeNYCAcrisConnectivity();
        if (probe.ok) {
          await applyConnectivitySuccess(site, 'probe');
          state = await getConnectivityState(site);
          log({
            stage: 'site_connectivity_probe_success',
            site,
            transport_mode: probe.transportMode,
            probe_recovery_action: probe.recoveryAction,
            probe_bootstrap_strategy: probe.bootstrapStrategy,
            probe_step: probe.diagnostic?.step,
            probe_attempt: probe.diagnostic?.attempt,
            final_url: probe.diagnostic?.finalUrl,
            ready_state: probe.diagnostic?.readyState,
            has_shell_marker: probe.diagnostic?.hasShellMarker,
            has_result_marker: probe.diagnostic?.hasResultMarker,
            has_viewer_iframe: probe.diagnostic?.hasViewerIframe,
            detail: probe.detail,
          });
        } else {
          const failureClass = probe.failureClass ?? classifyNYCAcrisFailure(probe.detail ?? 'probe_failed');
          await applyConnectivityFailure(site, failureClass, probe.detail ?? 'probe_failed');
          state = await getConnectivityState(site);
          log({
            stage: 'site_connectivity_probe_failure',
            site,
            transport_mode: probe.transportMode,
            failure_class: failureClass,
            probe_recovery_action: probe.recoveryAction,
            probe_bootstrap_strategy: probe.bootstrapStrategy,
            probe_step: probe.diagnostic?.step,
            probe_attempt: probe.diagnostic?.attempt,
            final_url: probe.diagnostic?.finalUrl,
            ready_state: probe.diagnostic?.readyState,
            has_shell_marker: probe.diagnostic?.hasShellMarker,
            has_result_marker: probe.diagnostic?.hasResultMarker,
            has_viewer_iframe: probe.diagnostic?.hasViewerIframe,
            detail: probe.detail,
          });
        }
      } else {
        const probe = await probeMaricopaRecorderConnectivity();
        if (probe.ok) {
          await applyConnectivitySuccess(site, 'probe');
          state = await getConnectivityState(site);
          log({ stage: 'site_connectivity_probe_success', site, detail: probe.detail, latest_searchable_date: probe.latestSearchableDate });
        } else {
          await applyConnectivityFailure(site, probe.failureClass ?? 'artifact_fetch_failed', probe.detail);
          state = await getConnectivityState(site);
          log({ stage: 'site_connectivity_probe_failure', site, detail: probe.detail, failure_class: probe.failureClass });
        }
      }
    }

    if (shouldSendProlongedBlockedAlert(state)) {
      await sendConnectivityAlert(site, state, 'blocked_4h');
      await getStore().upsertConnectivityState(markConnectivityAlerted(state));
    }
  }
}

export async function runScheduledScrape(options: RunScheduledScrapeOptions = {}): Promise<ScheduledRun> {
  const site = options.site ?? 'ca_sos';
  const requestedSlot = options.slot;
  const schedule = getSiteSchedule(site, requestedSlot);
  const now = new Date();
  const slot = requestedSlot ?? schedule.slot;
  const idempotencyKey = options.idempotencyKey ?? buildDefaultIdempotencyKey(site, now, slot);
  const triggerSource = options.triggerSource ?? 'external';
  const testFailureClass = options.testFailureClass;
  const debugBootstrapOnly = options.debugBootstrapOnly === true;
  let connectivityAtStart = await getConnectivityState(site);

  if (site === 'maricopa_recorder' && triggerSource === 'external') {
    const readiness = await getMaricopaPersistedStateReadiness();
    if (readiness.refreshRequired) {
      const failureClass = readiness.refreshReason === 'artifact_candidates_missing'
        ? 'artifact_candidates_missing'
        : 'session_missing_or_stale';
      await applyConnectivityFailure(site, failureClass, readiness.detail);
      connectivityAtStart = await getConnectivityState(site);
    }
  }

  const existing = await getStore().getByIdempotencyKey(idempotencyKey);
  if (getEnableScheduleIdempotency() && existing && existing.status !== 'error') {
    log({ stage: 'scheduled_run_duplicate_skipped', site, idempotency_key: idempotencyKey, existing_run_id: existing.id });
    return { ...existing, duplicate_of: existing.id };
  }

  const mostRecent = await getStore().getMostRecentRun(site);
  if (mostRecent) {
    const elapsedMs = Date.now() - new Date(mostRecent.started_at).getTime();
    const cooldownMs = SCHEDULE_COOLDOWN_MINUTES * 60 * 1000;
    if (elapsedMs >= 0 && elapsedMs < cooldownMs && mostRecent.status === 'running') {
      log({
        stage: 'scheduled_run_cooldown_skipped',
        site,
        idempotency_key: idempotencyKey,
        cooldown_of: mostRecent.id,
        cooldown_minutes: SCHEDULE_COOLDOWN_MINUTES,
      });
      return { ...mostRecent, cooldown_of: mostRecent.id };
    }
  }

  const { date_start, date_end } = getLast7DaysRange();
  const deadlineIso = buildDeadlineIso(site, slot, now);
  const seededMaxRecords = await resolveEffectiveMaxRecords(site);
  let effectiveMaxRecords = seededMaxRecords;
  let skipScrape = false;

  if (site === 'ca_sos') {
    try {
      effectiveMaxRecords = await probeCASOSResultCount({ date_start, date_end });
      skipScrape = effectiveMaxRecords === 0;
      log({
        stage: 'scheduled_run_ca_probe_complete',
        site,
        idempotency_key: idempotencyKey,
        result_count: effectiveMaxRecords,
        seeded_max_records: seededMaxRecords,
        skip_scrape: skipScrape,
      });
    } catch (err: any) {
      effectiveMaxRecords = Math.min(seededMaxRecords, getCAScheduleFallbackMaxRecords());
      log({
        stage: 'scheduled_run_ca_probe_failed',
        site,
        idempotency_key: idempotencyKey,
        seeded_max_records: seededMaxRecords,
        fallback_max_records: effectiveMaxRecords,
        error: String(err?.message ?? err),
      });
    }
  }

  const runId = existing?.status === 'error'
    ? existing.id
    : `sched_${site}_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;

  const run: ScheduledRunRecord = {
    id: runId,
    site,
    idempotency_key: idempotencyKey,
    slot_time: idempotencyKey,
    trigger_source: triggerSource,
    started_at: new Date().toISOString(),
    status: 'running',
    records_scraped: 0,
    records_skipped: 0,
    rows_uploaded: 0,
    amount_found_count: 0,
    amount_missing_count: 0,
    amount_coverage_pct: 0,
    ocr_success_pct: 0,
    row_fail_pct: 0,
    deadline_hit: 0,
    effective_max_records: effectiveMaxRecords,
    partial: 0,
    error: undefined,
    failure_class: undefined,
    attempt_count: 1,
    max_attempts: getScheduleRunMaxAttempts(),
    retried: 0,
    retry_exhausted: 0,
    source_tab_title: undefined,
    master_tab_title: undefined,
    review_tab_title: undefined,
    quarantined_row_count: 0,
    current_run_quarantined_row_count: 0,
    current_run_conflict_row_count: 0,
    retained_prior_review_row_count: 0,
    review_reason_counts_json: undefined,
    requested_date_start: date_start,
    requested_date_end: date_end,
    discovered_count: 0,
    returned_count: 0,
    filtered_out_count: 0,
    returned_min_filing_date: undefined,
    returned_max_filing_date: undefined,
    upstream_min_filing_date: undefined,
    upstream_max_filing_date: undefined,
    partial_reason: undefined,
    artifact_retrieval_enabled: 0,
    artifact_fetch_coverage_pct: 0,
    enrichment_mode: undefined,
    artifact_readiness_not_met: 0,
    enriched_record_count: 0,
    partial_record_count: 0,
    new_master_row_count: 0,
    purged_review_row_count: 0,
    lead_alert_attempted: 0,
    lead_alert_delivered: 0,
    master_fallback_used: 0,
    anomaly_detected: 0,
    debug_artifact_json: undefined,
    finished_at: undefined,
  };

  if (debugBootstrapOnly) {
    run.partial = 1;
    run.partial_reason = 'debug_bootstrap_only';
  }

  if (
    !debugBootstrapOnly &&
    isConnectivityManagedSite(site) &&
    triggerSource === 'external' &&
    (connectivityAtStart.status === 'blocked' || connectivityAtStart.status === 'probing')
  ) {
    run.status = 'deferred';
    run.error = `${site}_connectivity_${connectivityAtStart.status}`;
    run.failure_class = connectivityAtStart.last_failure_reason
      ? deriveFailureClass(site, connectivityAtStart.last_failure_reason)
      : (site === 'maricopa_recorder' ? 'session_missing_or_stale' : 'timeout_or_navigation');
    run.finished_at = new Date().toISOString();

    if (existing?.status === 'error') {
      await getStore().updateRun(run);
    } else {
      await getStore().insertRun(run);
    }

    log({
      stage: 'scheduled_run_deferred',
      site,
      run_id: runId,
      idempotency_key: idempotencyKey,
      connectivity_status: connectivityAtStart.status,
      next_allowed_run_at: getNextAllowedRunAt(connectivityAtStart),
    });

    return run;
  }

  if (existing?.status === 'error') {
    await getStore().updateRun(run);
  } else {
    await getStore().insertRun(run);
  }

  log({
    stage: 'scheduled_run_start',
    site,
    run_id: runId,
    idempotency_key: idempotencyKey,
    date_start,
    date_end,
    max_records: effectiveMaxRecords,
    deadline_iso: deadlineIso,
    scheduled_day_match: isScheduledDay(schedule, now),
  });

  try {
    if (debugBootstrapOnly) {
      if (site !== 'nyc_acris') {
        throw new Error('debug_bootstrap_only is supported only for nyc_acris');
      }

      const artifact = await debugNYCAcrisBootstrap({
        transportPolicyPurpose: 'diagnostic',
        transportModeOverride: options.transportModeOverride,
      });

      run.error = buildNYCDebugErrorArtifact(artifact);
      run.failure_class = artifact.ok ? undefined : (artifact.failureClass ?? 'transport_or_bootstrap');
      run.finished_at = new Date().toISOString();
      run.status = artifact.ok ? 'success' : 'error';
      await getStore().updateRun(run);

      if (artifact.ok) {
        log({
          stage: 'scheduled_run_debug_complete',
          site,
          run_id: runId,
          idempotency_key: idempotencyKey,
          requested_transport_mode: artifact.requestedTransportMode,
          transport_mode: artifact.transportMode,
          transport_policy_purpose: artifact.transportPolicyPurpose,
          bootstrap_strategy: artifact.bootstrapStrategy,
          recovery_action: artifact.recoveryAction,
          final_url: artifact.diagnostic?.finalUrl,
        });
      } else {
        log({
          stage: 'scheduled_run_debug_error',
          site,
          run_id: runId,
          idempotency_key: idempotencyKey,
          requested_transport_mode: artifact.requestedTransportMode,
          transport_mode: artifact.transportMode,
          transport_policy_purpose: artifact.transportPolicyPurpose,
          failure_class: artifact.failureClass,
          final_url: artifact.diagnostic?.finalUrl,
          error: artifact.detail,
        });
      }

      return {
        ...run,
        debug_artifact: artifact,
      };
    }

    if (skipScrape) {
      run.status = 'success';
      run.finished_at = new Date().toISOString();
      await getStore().updateRun(run);

      log({
        stage: 'scheduled_run_complete',
        site,
        run_id: runId,
        idempotency_key: idempotencyKey,
        records_scraped: 0,
        rows_uploaded: 0,
        amount_coverage_pct: 0,
        ocr_success_pct: 0,
        row_fail_pct: 0,
        deadline_hit: 0,
        effective_max_records: effectiveMaxRecords,
        partial: 0,
        tab_title: null,
      });

      return run;
    }

    let previousFailureClass = existing?.status === 'error' ? existing.failure_class : undefined;
    let injectedFailureConsumed = false;
    for (let attempt = 1; attempt <= getScheduleRunMaxAttempts(); attempt++) {
      run.attempt_count = attempt;
      run.max_attempts = getScheduleRunMaxAttempts();
      run.retried = attempt > 1 ? 1 : 0;
      run.retry_exhausted = 0;
      run.status = 'running';
      run.finished_at = undefined;
      await getStore().updateRun(run);

      log({
        stage: 'scheduled_run_attempt_start',
        site,
        run_id: runId,
        idempotency_key: idempotencyKey,
        attempt,
        max_attempts: getScheduleRunMaxAttempts(),
      });

      try {
        if (testFailureClass && !injectedFailureConsumed && testFailureClass !== 'sheet_export') {
          injectedFailureConsumed = true;
          log({
            stage: 'scheduled_run_test_failure_injected',
            site,
            run_id: runId,
            idempotency_key: idempotencyKey,
            attempt,
            failure_class: testFailureClass,
          });
          throw buildInjectedFailureError(testFailureClass);
        }

        const { records, reusedCache, qualitySummary } = await getRecordsForScheduledRun(
          site,
          idempotencyKey,
          date_start,
          date_end,
          effectiveMaxRecords,
          slot,
          previousFailureClass,
        );

        if (site === 'nyc_acris' && !reusedCache) {
          await saveNYCCachedRecords(idempotencyKey, records as LienRecord[]);
        }

        if (testFailureClass === 'sheet_export' && !injectedFailureConsumed) {
          injectedFailureConsumed = true;
          log({
            stage: 'scheduled_run_test_failure_injected',
            site,
            run_id: runId,
            idempotency_key: idempotencyKey,
            attempt,
            failure_class: testFailureClass,
          });
          throw buildInjectedFailureError(testFailureClass);
        }

        const deadlineHit = isPastDeadline(site, slot, new Date());
        const quality = computeQualityMetrics(records, effectiveMaxRecords, deadlineHit, qualitySummary);
        const tabTitle = formatRunTabName(`Scheduled_${site}_${slot}_${runId}`, date_start, date_end, new Date());
        const uploadResult = await pushToSheetsForTab(records, tabTitle, {
          runPartial: quality.partial === 1,
          scheduledRunId: runId,
        });
        if (uploadResult.uploaded !== records.length) {
          throw new Error(`sheet_upload_mismatch uploaded=${uploadResult.uploaded} records=${records.length}`);
        }

        const masterSync = await syncMasterSheetTab({ currentSourceTab: uploadResult.tab_title });
        let leadAlertResult = { attempted: false, delivered: false };
        if ((masterSync?.new_master_row_count ?? 0) > 0) {
          leadAlertResult = await sendNewLeadsNotification({
            site,
            run_id: runId,
            idempotency_key: idempotencyKey,
            new_master_row_count: masterSync!.new_master_row_count,
            master_tab_title: masterSync!.tab_title,
            target_spreadsheet_id_suffix: masterSync!.target_spreadsheet_id.slice(-6),
          });
        }

        run.records_scraped = records.length;
        run.records_skipped = quality.recordsSkipped;
        run.rows_uploaded = uploadResult.uploaded;
        run.amount_found_count = quality.amountFound;
        run.amount_missing_count = quality.amountMissing;
        run.amount_coverage_pct = quality.amountCoveragePct;
        run.ocr_success_pct = quality.ocrSuccessPct;
        run.row_fail_pct = quality.rowFailPct;
        run.deadline_hit = deadlineHit ? 1 : 0;
        run.partial = quality.partial;
        run.partial_reason = quality.partialReason;
        run.requested_date_start = quality.requestedDateStart ?? date_start;
        run.requested_date_end = quality.requestedDateEnd ?? date_end;
        run.discovered_count = quality.discoveredCount;
        run.returned_count = quality.returnedCount;
        run.filtered_out_count = quality.filteredOutCount;
        run.returned_min_filing_date = quality.returnedMinFilingDate;
        run.returned_max_filing_date = quality.returnedMaxFilingDate;
        run.upstream_min_filing_date = quality.upstreamMinFilingDate;
        run.upstream_max_filing_date = quality.upstreamMaxFilingDate;
        run.status = 'success';
        run.error = undefined;
        run.failure_class = undefined;
        run.retry_exhausted = 0;
        run.finished_at = new Date().toISOString();
        run.source_tab_title = uploadResult.tab_title;
        run.master_tab_title = masterSync?.tab_title;
        run.review_tab_title = masterSync?.review_tab_title;
        run.quarantined_row_count =
          quality.quarantinedCount +
          (masterSync?.current_run_quarantined_row_count ?? 0) +
          (masterSync?.current_run_conflict_row_count ?? 0);
        run.current_run_quarantined_row_count = quality.quarantinedCount + (masterSync?.current_run_quarantined_row_count ?? 0);
        run.current_run_conflict_row_count = masterSync?.current_run_conflict_row_count ?? 0;
        run.retained_prior_review_row_count = masterSync?.retained_prior_review_row_count ?? 0;
        run.review_reason_counts_json = JSON.stringify(
          normalizeReviewReasonCountsFromObject(masterSync?.review_summary?.review_reason_counts as Record<string, unknown> | undefined)
        );
        run.artifact_retrieval_enabled = quality.artifactRetrievalEnabled;
        run.artifact_fetch_coverage_pct = quality.artifactFetchCoveragePct;
        run.enrichment_mode = quality.enrichmentMode;
        run.artifact_readiness_not_met = quality.artifactReadinessNotMet;
        run.enriched_record_count = quality.enrichedRecordCount;
        run.partial_record_count = quality.partialRecordCount;
        run.new_master_row_count = masterSync?.new_master_row_count ?? 0;
        run.purged_review_row_count = masterSync?.purged_review_row_count ?? 0;
        run.lead_alert_attempted = leadAlertResult.attempted ? 1 : 0;
        run.lead_alert_delivered = leadAlertResult.delivered ? 1 : 0;
        run.master_fallback_used = masterSync?.fallback_used ? 1 : 0;
        run.anomaly_detected = 0;
        run.debug_artifact_json = undefined;

        const anomaly = await evaluateQualityAnomaly(site, run);
        run.anomaly_detected = anomaly ? 1 : 0;
        await getStore().updateRun(run);
        await clearNYCCachedRecords(idempotencyKey).catch(() => null);
        await applyConnectivitySuccess(site, 'run');

        if (anomaly) {
          const anomalyAlert: QualityAnomalyAlertRecord = {
            site,
            idempotency_key: idempotencyKey,
            run_id: runId,
            slot,
            metrics_triggered: anomaly.metricsTriggered,
            summary: anomaly.summary,
            baseline_records_scraped: anomaly.baseline.records_scraped,
            baseline_amount_coverage_pct: anomaly.baseline.amount_coverage_pct,
            baseline_ocr_success_pct: anomaly.baseline.ocr_success_pct,
            baseline_row_fail_pct: anomaly.baseline.row_fail_pct,
            records_scraped: run.records_scraped,
            amount_coverage_pct: run.amount_coverage_pct,
            ocr_success_pct: run.ocr_success_pct,
            row_fail_pct: run.row_fail_pct,
            detected_at: run.finished_at,
          };

          let alertResult = { attempted: false, delivered: false };
          try {
            await getStore().insertQualityAnomalyAlert(anomalyAlert);
            alertResult = await sendQualityAnomalyAlert(anomalyAlert);
          } catch (alertErr: any) {
            log({
              stage: 'quality_anomaly_persist_error',
              site,
              run_id: runId,
              idempotency_key: idempotencyKey,
              error: String(alertErr?.message ?? alertErr),
            });
          }

          log({
            stage: 'scheduled_run_anomaly_detected',
            site,
            run_id: runId,
            idempotency_key: idempotencyKey,
            attempt,
            attempt_count: run.attempt_count,
            metrics_triggered: anomaly.metricsTriggered,
            baseline_sample_size: anomaly.baseline.sample_size,
            baseline_records_scraped: anomaly.baseline.records_scraped,
            baseline_amount_coverage_pct: anomaly.baseline.amount_coverage_pct,
            baseline_ocr_success_pct: anomaly.baseline.ocr_success_pct,
            baseline_row_fail_pct: anomaly.baseline.row_fail_pct,
            records_scraped: run.records_scraped,
            amount_coverage_pct: run.amount_coverage_pct,
            ocr_success_pct: run.ocr_success_pct,
            row_fail_pct: run.row_fail_pct,
            webhook_attempted: alertResult.attempted,
            webhook_delivered: alertResult.delivered,
          });
        }

        let nextCap = effectiveMaxRecords;
        if (site !== 'ca_sos') {
          nextCap = deadlineHit || records.length === 0
            ? effectiveMaxRecords
            : await maybeAdjustEffectiveMaxRecords(site, effectiveMaxRecords, run.amount_coverage_pct);
          await getStore().upsertControlState(site, nextCap);
        }

        log({
          stage: 'scheduled_run_complete',
          site,
          run_id: runId,
          idempotency_key: idempotencyKey,
          attempt,
          max_attempts: getScheduleRunMaxAttempts(),
          records_scraped: records.length,
          rows_uploaded: uploadResult.uploaded,
          amount_coverage_pct: run.amount_coverage_pct,
          ocr_success_pct: run.ocr_success_pct,
          row_fail_pct: run.row_fail_pct,
          deadline_hit: run.deadline_hit,
          effective_max_records: nextCap,
          partial: run.partial,
          partial_reason: run.partial_reason,
          requested_date_start: run.requested_date_start,
          requested_date_end: run.requested_date_end,
          discovered_count: run.discovered_count,
          returned_count: run.returned_count,
          filtered_out_count: run.filtered_out_count,
          returned_min_filing_date: run.returned_min_filing_date,
          returned_max_filing_date: run.returned_max_filing_date,
          upstream_min_filing_date: run.upstream_min_filing_date,
          upstream_max_filing_date: run.upstream_max_filing_date,
          current_run_quarantined_row_count: run.current_run_quarantined_row_count,
          current_run_conflict_row_count: run.current_run_conflict_row_count,
          retained_prior_review_row_count: run.retained_prior_review_row_count,
          artifact_retrieval_enabled: run.artifact_retrieval_enabled,
          artifact_fetch_coverage_pct: run.artifact_fetch_coverage_pct,
          enrichment_mode: run.enrichment_mode,
          artifact_readiness_not_met: run.artifact_readiness_not_met,
          enriched_record_count: run.enriched_record_count,
          partial_record_count: run.partial_record_count,
          retried: run.retried,
          tab_title: uploadResult.tab_title,
          master_tab_title: masterSync?.tab_title,
          master_target_spreadsheet_id_suffix: masterSync?.target_spreadsheet_id?.slice(-6),
          master_fallback_used: masterSync?.fallback_used ?? false,
          new_master_row_count: masterSync?.new_master_row_count ?? 0,
          purged_review_row_count: masterSync?.purged_review_row_count ?? 0,
          lead_alert_attempted: leadAlertResult.attempted,
          lead_alert_delivered: leadAlertResult.delivered,
        });

        return run;
      } catch (err: any) {
        const failureClass = deriveFailureClass(site, err);
        const errorMessage = String(err?.stack ?? err?.message ?? err);

        run.error = errorMessage;
        run.failure_class = failureClass;
        applyFailureDiagnostics(run, err);
        const failureDebugArtifact = buildNYCScheduledFailureDebugArtifact(run, err);
        run.debug_artifact_json = failureDebugArtifact ? JSON.stringify(failureDebugArtifact) : undefined;
        previousFailureClass = failureClass;

        await applyConnectivityFailure(site, failureClass, errorMessage);

        const connectivityAfterFailure = isConnectivityManagedSite(site)
          ? await getConnectivityState(site)
          : null;
        const retryBlockedByCircuit = Boolean(
          connectivityAfterFailure &&
          (connectivityAfterFailure.status === 'blocked' || connectivityAfterFailure.status === 'probing')
        );
        const retryable = isRetryableScheduledFailure(site, failureClass, errorMessage);
        const hasRemainingAttempts = attempt < getScheduleRunMaxAttempts();

        if (retryable && hasRemainingAttempts && !retryBlockedByCircuit) {
          const delayMs = getRetryDelayMs(attempt);
          await getStore().updateRun(run);
          log({
            stage: 'scheduled_run_retry_scheduled',
            site,
            run_id: runId,
            idempotency_key: idempotencyKey,
            attempt,
            max_attempts: getScheduleRunMaxAttempts(),
            failure_class: failureClass,
            backoff_delay_ms: delayMs,
          });
          await sleep(delayMs);
          continue;
        }

        run.status = 'error';
        run.retry_exhausted = retryable && !retryBlockedByCircuit && !hasRemainingAttempts ? 1 : 0;
        run.finished_at = new Date().toISOString();
        await getStore().updateRun(run);
        log({
          stage: 'scheduled_run_error',
          site,
          run_id: runId,
          idempotency_key: idempotencyKey,
          attempt,
          max_attempts: getScheduleRunMaxAttempts(),
          failure_class: failureClass,
          retryable,
          retry_exhausted: run.retry_exhausted,
          retry_blocked_by_circuit: retryBlockedByCircuit,
          error: run.error,
        });
        return run;
      }
    }
  } catch (err: any) {
    const failureClass = deriveFailureClass(site, err);
    run.status = 'error';
    run.error = String(err?.stack ?? err?.message ?? err);
    run.failure_class = failureClass;
    run.finished_at = new Date().toISOString();
    run.retry_exhausted = 1;
    const failureDebugArtifact = buildNYCScheduledFailureDebugArtifact(run, err);
    run.debug_artifact_json = failureDebugArtifact ? JSON.stringify(failureDebugArtifact) : undefined;
    await getStore().updateRun(run);
    await applyConnectivityFailure(site, failureClass, run.error);
    log({ stage: 'scheduled_run_error', site, run_id: runId, idempotency_key: idempotencyKey, error: run.error });
  }

  return run;
}

export async function getRunHistory(limit = 50): Promise<ScheduledRun[]> {
  const runs = await getStore().getRunHistory(limit);
  return runs.map((run) => ({
    ...run,
    debug_artifact: parseDebugArtifact(run.debug_artifact_json),
    confidence: buildRunConfidence(run),
  }));
}

export async function getScheduleState(): Promise<ScheduleState> {
  const entries = await Promise.all(
    supportedSites.map(async (site) => {
      const control = await getStore().getControlState(site);
      const bounds = getSiteRecordBounds(site);
      const defaultMax = getSiteSchedule(site).maxRecords;
      const effectiveMax = control?.effective_max_records ?? clamp(defaultMax, bounds.min, bounds.max);
      const recent = await getStore().getRecentSuccessfulRuns(site, 4);
      const latestRun = await getStore().getMostRecentRun(site);
      const latestRunConfidence = latestRun ? buildRunConfidence(latestRun) : undefined;
      const connectivity = await getConnectivityState(site);
      const latestAnomaly = await getStore().getLatestQualityAnomalyAlert(site);

      const state: SiteScheduleState = {
        effective_max_records: effectiveMax,
        target_amount_coverage_pct: getAmountMinCoveragePct(),
        auto_throttle: isAutoThrottleEnabled(site),
        recent_run_count: recent.length,
        latest_run_started_at: latestRun?.started_at,
        latest_run_confidence_status: latestRunConfidence?.status,
        latest_run_confidence_reasons: latestRunConfidence?.reasons,
        connectivity: {
          status: connectivity.status,
          next_probe_at: connectivity.next_probe_at,
          next_allowed_run_at: getNextAllowedRunAt(connectivity),
          last_failure_reason: connectivity.last_failure_reason,
          last_success_at: connectivity.last_success_at,
        },
        recent_quality: recent.map((run) => ({
          id: run.id,
          started_at: run.started_at,
          amount_coverage_pct: run.amount_coverage_pct,
          ocr_success_pct: run.ocr_success_pct,
          row_fail_pct: run.row_fail_pct,
          partial: run.partial,
          deadline_hit: run.deadline_hit,
          effective_max_records: run.effective_max_records,
        })),
        latest_anomaly: latestAnomaly
          ? {
            run_id: latestAnomaly.run_id,
            idempotency_key: latestAnomaly.idempotency_key,
            slot: latestAnomaly.slot,
            metrics_triggered: latestAnomaly.metrics_triggered,
            summary: latestAnomaly.summary,
            detected_at: latestAnomaly.detected_at,
          }
          : undefined,
      };

      return [site, state] as const;
    })
  );

  return Object.fromEntries(entries) as ScheduleState;
}

export function getNextRuns(): Array<{ site: SupportedSite; schedule: string; days: string; run_time: string; trigger_time: string; finish_by_time: string; deadline_time: string; timezone: string }> {
  return supportedSites.flatMap((site) =>
    getSiteSchedules(site).map((config) => ({
      site,
      schedule: `daily_${config.slot}`,
      days: config.days.join(','),
      run_time: formatClock(config.triggerHour, config.triggerMinute),
      trigger_time: formatClock(config.triggerHour, config.triggerMinute),
      finish_by_time: formatClock(config.finishByHour, config.finishByMinute),
      deadline_time: formatClock(config.finishByHour, config.finishByMinute),
      timezone: config.timezone,
    }))
  );
}

export async function checkMissedRuns(): Promise<void> {
  const now = new Date();

  for (const site of supportedSites) {
    for (const config of getSiteSchedules(site)) {
      if (!isScheduledDay(config, now)) continue;

      const parts = getDateParts(now, config.timezone);
      const dueHour = config.triggerHour + Math.floor((config.triggerMinute + MISSED_RUN_GRACE_MINUTES) / 60);
      const dueMinute = (config.triggerMinute + MISSED_RUN_GRACE_MINUTES) % 60;
      const overdue = parts.hour > dueHour || (parts.hour === dueHour && parts.minute >= dueMinute);
      if (!overdue) continue;

      const key = buildDefaultIdempotencyKey(site, now, config.slot);
      const success = await getStore().getSuccessfulRunByIdempotencyKey(key);
      if (success) continue;

      const existingAlert = await getStore().getMissedAlertByKey(key);
      if (existingAlert) continue;

      const expectedAtIso = `${parts.year}-${parts.month}-${parts.day}T${String(dueHour).padStart(2, '0')}:${String(dueMinute).padStart(2, '0')}:00`;
      await getStore().insertMissedAlert({ site, idempotency_key: key, slot: config.slot, expected_by: expectedAtIso });
      await sendMissedRunAlert(site, config.slot, expectedAtIso, key);
      log({ stage: 'missed_run_alerted', site, slot: config.slot, idempotency_key: key, expected_by: expectedAtIso });
    }
  }
}

export async function getConnectivityHealth(): Promise<Record<SupportedSite, SiteScheduleState['connectivity']>> {
  const entries = await Promise.all(
    supportedSites.map(async (site) => {
      const connectivity = await getConnectivityState(site);
      return [site, {
        status: connectivity.status,
        next_probe_at: connectivity.next_probe_at,
        next_allowed_run_at: getNextAllowedRunAt(connectivity),
        last_failure_reason: connectivity.last_failure_reason,
        last_success_at: connectivity.last_success_at,
      }] as const;
    })
  );

  return Object.fromEntries(entries) as Record<SupportedSite, SiteScheduleState['connectivity']>;
}
