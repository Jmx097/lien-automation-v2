import crypto from 'crypto';
import fs from 'fs/promises';
import path from 'path';
import { log } from './utils/logger';
import { scrapers } from './scraper/index';
import { probeCASOSResultCount } from './scraper/ca_sos_enhanced';
import { ScheduledRunStore, ScheduledRunRecord, type QualityAnomalyAlertRecord } from './scheduler/store';
import {
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
  type SiteConnectivityState,
} from './scheduler/connectivity';
import { probeNYCAcrisConnectivity } from './scraper/nyc_acris';
import { formatRunTabName, pushToSheetsForTab } from './sheets/push';
import type { LienRecord } from './types';
import { supportedSites, type SupportedSite } from './sites';

const LOOKBACK_DAYS = 7;
const MISSED_RUN_GRACE_MINUTES = 45;
const SCHEDULE_COOLDOWN_MINUTES = 10;
const ENABLE_SCHEDULE_IDEMPOTENCY = process.env.ENABLE_SCHEDULE_IDEMPOTENCY === '1';
const AMOUNT_MIN_COVERAGE_PCT = Number(process.env.AMOUNT_MIN_COVERAGE_PCT ?? '95');
const SCHEDULE_AUTO_THROTTLE = process.env.SCHEDULE_AUTO_THROTTLE !== '0';
const SCHEDULE_MAX_RECORDS = Number(process.env.SCHEDULE_MAX_RECORDS ?? '1000');
const SCHEDULE_MAX_RECORDS_FLOOR = Number(process.env.SCHEDULE_MAX_RECORDS_FLOOR ?? '25');
const SCHEDULE_MAX_RECORDS_CEILING = Number(process.env.SCHEDULE_MAX_RECORDS_CEILING ?? '1000');
const SCHEDULE_RUN_MAX_ATTEMPTS = Math.max(1, Number(process.env.SCHEDULE_RUN_MAX_ATTEMPTS ?? '3'));
const SCHEDULE_RUN_BASE_DELAY_MS = Math.max(0, Number(process.env.SCHEDULE_RUN_BASE_DELAY_MS ?? '1000'));
const SCHEDULE_RUN_MAX_DELAY_MS = Math.max(SCHEDULE_RUN_BASE_DELAY_MS, Number(process.env.SCHEDULE_RUN_MAX_DELAY_MS ?? '10000'));
const SCHEDULE_ANOMALY_BASELINE_RUNS = Math.max(3, Number(process.env.SCHEDULE_ANOMALY_BASELINE_RUNS ?? '5'));
const SCHEDULE_ANOMALY_MIN_BASELINE_RUNS = Math.max(1, Number(process.env.SCHEDULE_ANOMALY_MIN_BASELINE_RUNS ?? '3'));
const SCHEDULE_ANOMALY_RECORDS_DROP_PCT = Math.max(0, Number(process.env.SCHEDULE_ANOMALY_RECORDS_DROP_PCT ?? '40'));
const SCHEDULE_ANOMALY_AMOUNT_COVERAGE_DROP_PTS = Math.max(0, Number(process.env.SCHEDULE_ANOMALY_AMOUNT_COVERAGE_DROP_PTS ?? '15'));
const SCHEDULE_ANOMALY_OCR_SUCCESS_DROP_PTS = Math.max(0, Number(process.env.SCHEDULE_ANOMALY_OCR_SUCCESS_DROP_PTS ?? '20'));
const SCHEDULE_ANOMALY_ROW_FAIL_RISE_PTS = Math.max(0, Number(process.env.SCHEDULE_ANOMALY_ROW_FAIL_RISE_PTS ?? '20'));
export const SCHEDULE_FAILURE_INJECTION_ENABLED = process.env.ENABLE_SCHEDULE_FAILURE_INJECTION === '1';

type Slot = 'morning' | 'afternoon';
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

export interface ScheduledRun extends ScheduledRunRecord {
  duplicate_of?: string;
  cooldown_of?: string;
}

export interface SiteScheduleState {
  effective_max_records: number;
  target_amount_coverage_pct: number;
  auto_throttle: boolean;
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

function getSiteSchedule(site: SupportedSite): SiteScheduleConfig {
  if (site === 'nyc_acris') {
    const timezone = getSiteEnv(site, 'TIMEZONE') ?? 'America/New_York';
    const days = (getSiteEnv(site, 'WEEKLY_DAYS') ?? 'TU,WE,TH,FR').split(',').map((value) => value.trim().toUpperCase());
    const runHour = Number(getSiteEnv(site, 'RUN_HOUR') ?? '14');
    const runMinute = Number(getSiteEnv(site, 'RUN_MINUTE') ?? '0');
    const deadlineHour = Number(getSiteEnv(site, 'DEADLINE_HOUR') ?? '18');
    const deadlineMinute = Number(getSiteEnv(site, 'DEADLINE_MINUTE') ?? '0');
    const maxRecords = Number(getSiteEnv(site, 'MAX_RECORDS') ?? process.env.ACRIS_INITIAL_MAX_RECORDS ?? '5');

    return {
      site,
      timezone,
      days,
      triggerHour: runHour,
      triggerMinute: runMinute,
      finishByHour: deadlineHour,
      finishByMinute: deadlineMinute,
      triggerLeadMinutes: 0,
      maxRecords,
      slot: runHour < 12 ? 'morning' : 'afternoon',
    };
  }

  const timezone = getSiteEnv(site, 'TIMEZONE', 'SCHEDULE_TARGET_TIMEZONE') ?? 'America/New_York';
  const days = (getSiteEnv(site, 'WEEKLY_DAYS', 'SCHEDULE_WEEKLY_DAYS') ?? 'TU,WE').split(',').map((value) => value.trim().toUpperCase());
  const finishByHour = Number(getSiteEnv(site, 'RUN_HOUR', 'SCHEDULE_RUN_HOUR') ?? '9');
  const finishByMinute = Number(getSiteEnv(site, 'RUN_MINUTE', 'SCHEDULE_RUN_MINUTE') ?? '0');
  const triggerLeadMinutes = Number(getSiteEnv(site, 'TRIGGER_LEAD_MINUTES') ?? '180');
  const triggerTime = subtractMinutes(finishByHour, finishByMinute, triggerLeadMinutes);
  const maxRecords = Number(getSiteEnv(site, 'MAX_RECORDS') ?? SCHEDULE_MAX_RECORDS);

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
    slot: finishByHour < 12 ? 'morning' : 'afternoon',
  };
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

function isScheduledDay(site: SupportedSite, now: Date): boolean {
  const config = getSiteSchedule(site);
  return config.days.includes(getDateParts(now, config.timezone).weekday);
}

function buildDefaultIdempotencyKey(site: SupportedSite, now: Date, slot: Slot): string {
  const config = getSiteSchedule(site);
  const parts = getDateParts(now, config.timezone);
  return `${site}:${parts.year}-${parts.month}-${parts.day}:${slot}`;
}

function buildDeadlineIso(site: SupportedSite, now: Date): string {
  const config = getSiteSchedule(site);
  const parts = getDateParts(now, config.timezone);
  return `${parts.year}-${parts.month}-${parts.day}T${String(config.finishByHour).padStart(2, '0')}:${String(config.finishByMinute).padStart(2, '0')}:00 ${config.timezone}`;
}

function isPastDeadline(site: SupportedSite, now: Date): boolean {
  const config = getSiteSchedule(site);
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

function computeQualityMetrics(records: any[], effectiveMaxRecords: number, deadlineHit: boolean) {
  const amountFound = records.filter((row) => Boolean(row.amount)).length;
  const amountMissing = Math.max(records.length - amountFound, 0);
  const amountCoveragePct = records.length > 0 ? (amountFound / records.length) * 100 : 0;
  const ocrSuccessCount = records.filter((row) => row.amount_reason !== 'ocr_missing' && row.amount_reason !== 'ocr_error').length;
  const ocrSuccessPct = records.length > 0 ? (ocrSuccessCount / records.length) * 100 : 0;
  const rowFailPct = effectiveMaxRecords > 0 ? ((effectiveMaxRecords - records.length) / effectiveMaxRecords) * 100 : 0;

  return {
    amountFound,
    amountMissing,
    amountCoveragePct,
    ocrSuccessPct,
    rowFailPct: Math.max(rowFailPct, 0),
    partial: deadlineHit || records.length < effectiveMaxRecords ? 1 : 0,
  };
}

function roundMetric(value: number): number {
  return Number(value.toFixed(2));
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

async function evaluateQualityAnomaly(site: SupportedSite, currentRun: ScheduledRun): Promise<QualityAnomalyEvaluation | null> {
  const recent = await getStore().getRecentSuccessfulRuns(site, SCHEDULE_ANOMALY_BASELINE_RUNS + 1);
  const eligibleBaseline = recent
    .filter((run) => run.id !== currentRun.id && run.partial === 0 && run.deadline_hit === 0)
    .slice(0, SCHEDULE_ANOMALY_BASELINE_RUNS);

  if (eligibleBaseline.length < SCHEDULE_ANOMALY_MIN_BASELINE_RUNS) {
    log({
      stage: 'scheduled_run_anomaly_skipped',
      site,
      run_id: currentRun.id,
      idempotency_key: currentRun.idempotency_key,
      reason: 'insufficient_baseline',
      baseline_sample_size: eligibleBaseline.length,
      min_baseline_runs: SCHEDULE_ANOMALY_MIN_BASELINE_RUNS,
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
  if (baseline.records_scraped > 0 && recordsDropPct >= SCHEDULE_ANOMALY_RECORDS_DROP_PCT) {
    metricsTriggered.push('records_scraped');
  }
  if ((baseline.amount_coverage_pct - currentRun.amount_coverage_pct) >= SCHEDULE_ANOMALY_AMOUNT_COVERAGE_DROP_PTS) {
    metricsTriggered.push('amount_coverage_pct');
  }
  if ((baseline.ocr_success_pct - currentRun.ocr_success_pct) >= SCHEDULE_ANOMALY_OCR_SUCCESS_DROP_PTS) {
    metricsTriggered.push('ocr_success_pct');
  }
  if ((currentRun.row_fail_pct - baseline.row_fail_pct) >= SCHEDULE_ANOMALY_ROW_FAIL_RISE_PTS) {
    metricsTriggered.push('row_fail_pct');
  }

  if (metricsTriggered.length === 0) return null;

  return {
    metricsTriggered,
    baseline,
    summary: `Quality anomaly for ${site}: ${metricsTriggered.join(', ')}`,
  };
}

function isRetryableScheduledFailure(site: SupportedSite, failureClass: NYCAcrisFailureClass, errorMessage: string): boolean {
  if (failureClass === 'policy_block' || failureClass === 'selector_or_empty_results') {
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
  const raw = SCHEDULE_RUN_BASE_DELAY_MS * Math.pow(2, Math.max(attempt - 1, 0));
  return Math.min(raw, SCHEDULE_RUN_MAX_DELAY_MS);
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
  if (site === 'nyc_acris') {
    const max = getSiteSchedule(site).maxRecords;
    return { min: 1, max };
  }

  return { min: SCHEDULE_MAX_RECORDS_FLOOR, max: SCHEDULE_MAX_RECORDS_CEILING };
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
  return SCHEDULE_AUTO_THROTTLE && site !== 'ca_sos';
}

async function maybeAdjustEffectiveMaxRecords(site: SupportedSite, current: number, currentCoveragePct: number): Promise<number> {
  if (!isAutoThrottleEnabled(site)) return current;
  const bounds = getSiteRecordBounds(site);

  if (site === 'nyc_acris' && !(await hasNYCStableSuccessStreak(3))) {
    return clamp(current, bounds.min, bounds.max);
  }

  if (currentCoveragePct < AMOUNT_MIN_COVERAGE_PCT) {
    return clamp(Math.floor(current * 0.8), bounds.min, bounds.max);
  }

  const recent = await getStore().getRecentSuccessfulRuns(site, 2);
  const streak = [currentCoveragePct, ...recent.map((run) => run.amount_coverage_pct)];
  if (streak.length >= 3 && streak.every((pct) => pct >= AMOUNT_MIN_COVERAGE_PCT + 2)) {
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

async function applyNYCAcrisFailure(site: SupportedSite, failureClass: NYCAcrisFailureClass, reason: string): Promise<void> {
  if (site !== 'nyc_acris') return;

  const outcome = recordConnectivityFailure(await getConnectivityState(site), reason, failureClass);
  let state = outcome.state;
  if (outcome.becameBlocked) {
    await sendConnectivityAlert(site, state, 'blocked');
    state = markConnectivityAlerted(state);
  }

  await getStore().upsertConnectivityState(state);
}

async function applyNYCAcrisSuccess(site: SupportedSite, mode: 'probe' | 'run'): Promise<void> {
  if (site !== 'nyc_acris') return;

  const outcome = recordConnectivitySuccess(await getConnectivityState(site), mode);
  let state = outcome.state;
  if (outcome.recovered) {
    await sendConnectivityAlert(site, state, 'recovered');
    state = markConnectivityRecoveryAlerted(state);
  }

  await getStore().upsertConnectivityState(state);
}

function deriveFailureClass(err: unknown): NYCAcrisFailureClass {
  return classifyNYCAcrisFailure(String((err as any)?.message ?? err ?? ''));
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
  previousFailureClass?: string,
) {
  if (shouldUseCachedNYCRows(site, previousFailureClass)) {
    const cached = await loadNYCCachedRecords(idempotencyKey);
    if (cached && cached.length > 0) {
      log({ stage: 'scheduled_run_cached_records_reused', site, idempotency_key: idempotencyKey, records: cached.length });
      return { records: cached, reusedCache: true };
    }
  }

  const connectivityAtStart = await getConnectivityState(site);
  const records = await scrapers[site]({
    date_start,
    date_end,
    max_records: effectiveMaxRecords,
    stop_requested: () => isPastDeadline(site, new Date()),
    connectivity_status_at_start: connectivityAtStart.status,
  } as any);

  return { records, reusedCache: false };
}

export async function checkSiteConnectivity(): Promise<void> {
  const site: SupportedSite = 'nyc_acris';
  let state = await getConnectivityState(site);

  if (shouldRunConnectivityProbe(state)) {
    const probe = await probeNYCAcrisConnectivity();
    if (probe.ok) {
      await applyNYCAcrisSuccess(site, 'probe');
      state = await getConnectivityState(site);
      log({ stage: 'site_connectivity_probe_success', site, transport_mode: probe.transportMode, detail: probe.detail });
    } else {
      const failureClass = classifyNYCAcrisFailure(probe.detail ?? 'probe_failed');
      await applyNYCAcrisFailure(site, failureClass, probe.detail ?? 'probe_failed');
      state = await getConnectivityState(site);
      log({ stage: 'site_connectivity_probe_failure', site, transport_mode: probe.transportMode, detail: probe.detail });
    }
  }

  if (shouldSendProlongedBlockedAlert(state)) {
    await sendConnectivityAlert(site, state, 'blocked_4h');
    await getStore().upsertConnectivityState(markConnectivityAlerted(state));
  }
}

export async function runScheduledScrape(options: RunScheduledScrapeOptions = {}): Promise<ScheduledRun> {
  const site = options.site ?? 'ca_sos';
  const schedule = getSiteSchedule(site);
  const now = new Date();
  const slot = options.slot ?? schedule.slot;
  const idempotencyKey = options.idempotencyKey ?? buildDefaultIdempotencyKey(site, now, slot);
  const triggerSource = options.triggerSource ?? 'external';
  const testFailureClass = options.testFailureClass;
  const connectivityAtStart = await getConnectivityState(site);

  const existing = await getStore().getByIdempotencyKey(idempotencyKey);
  if (ENABLE_SCHEDULE_IDEMPOTENCY && existing && existing.status !== 'error') {
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
  const deadlineIso = buildDeadlineIso(site, now);
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
      effectiveMaxRecords = seededMaxRecords;
      log({
        stage: 'scheduled_run_ca_probe_failed',
        site,
        idempotency_key: idempotencyKey,
        seeded_max_records: seededMaxRecords,
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
    max_attempts: SCHEDULE_RUN_MAX_ATTEMPTS,
    retried: 0,
    retry_exhausted: 0,
    finished_at: undefined,
  };

  if (
    site === 'nyc_acris' &&
    triggerSource === 'external' &&
    (connectivityAtStart.status === 'blocked' || connectivityAtStart.status === 'probing')
  ) {
    run.status = 'deferred';
    run.error = `nyc_acris_connectivity_${connectivityAtStart.status}`;
    run.failure_class = connectivityAtStart.last_failure_reason
      ? classifyNYCAcrisFailure(connectivityAtStart.last_failure_reason)
      : 'timeout_or_navigation';
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
    scheduled_day_match: isScheduledDay(site, now),
  });

  try {
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
    for (let attempt = 1; attempt <= SCHEDULE_RUN_MAX_ATTEMPTS; attempt++) {
      run.attempt_count = attempt;
      run.max_attempts = SCHEDULE_RUN_MAX_ATTEMPTS;
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
        max_attempts: SCHEDULE_RUN_MAX_ATTEMPTS,
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

        const { records, reusedCache } = await getRecordsForScheduledRun(
          site,
          idempotencyKey,
          date_start,
          date_end,
          effectiveMaxRecords,
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

        const tabTitle = formatRunTabName(`Scheduled_${site}_${slot}_${runId}`, date_start, date_end, new Date());
        const uploadResult = await pushToSheetsForTab(records, tabTitle);
        if (uploadResult.uploaded !== records.length) {
          throw new Error(`sheet_upload_mismatch uploaded=${uploadResult.uploaded} records=${records.length}`);
        }

        const deadlineHit = isPastDeadline(site, new Date());
        const quality = computeQualityMetrics(records, effectiveMaxRecords, deadlineHit);

        run.records_scraped = records.length;
        run.rows_uploaded = uploadResult.uploaded;
        run.amount_found_count = quality.amountFound;
        run.amount_missing_count = quality.amountMissing;
        run.amount_coverage_pct = quality.amountCoveragePct;
        run.ocr_success_pct = quality.ocrSuccessPct;
        run.row_fail_pct = quality.rowFailPct;
        run.deadline_hit = deadlineHit ? 1 : 0;
        run.partial = quality.partial;
        run.status = 'success';
        run.error = undefined;
        run.failure_class = undefined;
        run.retry_exhausted = 0;
        run.finished_at = new Date().toISOString();

        const anomaly = await evaluateQualityAnomaly(site, run);
        await getStore().updateRun(run);
        await clearNYCCachedRecords(idempotencyKey).catch(() => null);
        await applyNYCAcrisSuccess(site, 'run');

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
          max_attempts: SCHEDULE_RUN_MAX_ATTEMPTS,
          records_scraped: records.length,
          rows_uploaded: uploadResult.uploaded,
          amount_coverage_pct: run.amount_coverage_pct,
          ocr_success_pct: run.ocr_success_pct,
          row_fail_pct: run.row_fail_pct,
          deadline_hit: run.deadline_hit,
          effective_max_records: nextCap,
          partial: run.partial,
          retried: run.retried,
          tab_title: uploadResult.tab_title,
        });

        return run;
      } catch (err: any) {
        const failureClass = deriveFailureClass(err);
        const errorMessage = String(err?.stack ?? err?.message ?? err);

        run.error = errorMessage;
        run.failure_class = failureClass;
        previousFailureClass = failureClass;

        await applyNYCAcrisFailure(site, failureClass, errorMessage);

        const connectivityAfterFailure = site === 'nyc_acris'
          ? await getConnectivityState(site)
          : null;
        const retryBlockedByCircuit = Boolean(
          connectivityAfterFailure &&
          (connectivityAfterFailure.status === 'blocked' || connectivityAfterFailure.status === 'probing')
        );
        const retryable = isRetryableScheduledFailure(site, failureClass, errorMessage);
        const hasRemainingAttempts = attempt < SCHEDULE_RUN_MAX_ATTEMPTS;

        if (retryable && hasRemainingAttempts && !retryBlockedByCircuit) {
          const delayMs = getRetryDelayMs(attempt);
          await getStore().updateRun(run);
          log({
            stage: 'scheduled_run_retry_scheduled',
            site,
            run_id: runId,
            idempotency_key: idempotencyKey,
            attempt,
            max_attempts: SCHEDULE_RUN_MAX_ATTEMPTS,
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
          max_attempts: SCHEDULE_RUN_MAX_ATTEMPTS,
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
    const failureClass = deriveFailureClass(err);
    run.status = 'error';
    run.error = String(err?.stack ?? err?.message ?? err);
    run.failure_class = failureClass;
    run.finished_at = new Date().toISOString();
    run.retry_exhausted = 1;
    await getStore().updateRun(run);
    await applyNYCAcrisFailure(site, failureClass, run.error);
    log({ stage: 'scheduled_run_error', site, run_id: runId, idempotency_key: idempotencyKey, error: run.error });
  }

  return run;
}

export async function getRunHistory(limit = 50): Promise<ScheduledRun[]> {
  return getStore().getRunHistory(limit);
}

export async function getScheduleState(): Promise<ScheduleState> {
  const entries = await Promise.all(
    supportedSites.map(async (site) => {
      const control = await getStore().getControlState(site);
      const bounds = getSiteRecordBounds(site);
      const defaultMax = getSiteSchedule(site).maxRecords;
      const effectiveMax = control?.effective_max_records ?? clamp(defaultMax, bounds.min, bounds.max);
      const recent = await getStore().getRecentSuccessfulRuns(site, 4);
      const connectivity = await getConnectivityState(site);
      const latestAnomaly = await getStore().getLatestQualityAnomalyAlert(site);

      const state: SiteScheduleState = {
        effective_max_records: effectiveMax,
        target_amount_coverage_pct: AMOUNT_MIN_COVERAGE_PCT,
        auto_throttle: isAutoThrottleEnabled(site),
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
  return supportedSites.map((site) => {
    const config = getSiteSchedule(site);
    return {
      site,
      schedule: 'weekly',
      days: config.days.join(','),
      run_time: formatClock(config.triggerHour, config.triggerMinute),
      trigger_time: formatClock(config.triggerHour, config.triggerMinute),
      finish_by_time: formatClock(config.finishByHour, config.finishByMinute),
      deadline_time: formatClock(config.finishByHour, config.finishByMinute),
      timezone: config.timezone,
    };
  });
}

export async function checkMissedRuns(): Promise<void> {
  const now = new Date();

  for (const site of supportedSites) {
    const config = getSiteSchedule(site);
    if (!isScheduledDay(site, now)) continue;

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
