import crypto from 'crypto';
import { log } from './utils/logger';
import { scrapers } from './scraper/index';
import { ScheduledRunStore, ScheduledRunRecord } from './scheduler/store';
import { formatRunTabName, pushToSheetsForTab } from './sheets/push';

const LOOKBACK_DAYS = 7;
const MISSED_RUN_GRACE_MINUTES = 45;
const SCHEDULE_COOLDOWN_MINUTES = 10;
const ENABLE_SCHEDULE_IDEMPOTENCY = process.env.ENABLE_SCHEDULE_IDEMPOTENCY === '1';
const TARGET_TIMEZONE = process.env.SCHEDULE_TARGET_TIMEZONE ?? 'America/New_York';
const SCHEDULE_WEEKLY_DAYS = (process.env.SCHEDULE_WEEKLY_DAYS ?? 'TU,WE').split(',').map((d) => d.trim().toUpperCase());
const SCHEDULE_RUN_HOUR = Number(process.env.SCHEDULE_RUN_HOUR ?? '9');
const SCHEDULE_RUN_MINUTE = Number(process.env.SCHEDULE_RUN_MINUTE ?? '0');
const SCHEDULE_DEADLINE_HOUR = Number(process.env.SCHEDULE_DEADLINE_HOUR ?? '13');
const SCHEDULE_DEADLINE_MINUTE = Number(process.env.SCHEDULE_DEADLINE_MINUTE ?? '0');
const AMOUNT_MIN_COVERAGE_PCT = Number(process.env.AMOUNT_MIN_COVERAGE_PCT ?? '95');
const SCHEDULE_AUTO_THROTTLE = process.env.SCHEDULE_AUTO_THROTTLE !== '0';
const SCHEDULE_MAX_RECORDS = Number(process.env.SCHEDULE_MAX_RECORDS ?? '1000');
const SCHEDULE_MAX_RECORDS_FLOOR = Number(process.env.SCHEDULE_MAX_RECORDS_FLOOR ?? '25');
const SCHEDULE_MAX_RECORDS_CEILING = Number(process.env.SCHEDULE_MAX_RECORDS_CEILING ?? '1000');

const MORNING_RUN_HOUR = Number(process.env.SCHEDULE_MORNING_HOUR ?? '7');
const MORNING_RUN_MINUTE = Number(process.env.SCHEDULE_MORNING_MINUTE ?? '30');
const AFTERNOON_RUN_HOUR = Number(process.env.SCHEDULE_AFTERNOON_HOUR ?? '19');
const AFTERNOON_RUN_MINUTE = Number(process.env.SCHEDULE_AFTERNOON_MINUTE ?? '30');
const AFTERNOON_DEADLINE_HOUR = Number(process.env.SCHEDULE_AFTERNOON_DEADLINE_HOUR ?? '23');
const AFTERNOON_DEADLINE_MINUTE = Number(process.env.SCHEDULE_AFTERNOON_DEADLINE_MINUTE ?? '59');

type Slot = 'morning' | 'afternoon';
type TriggerSource = 'external' | 'manual';

export interface ScheduledRun extends ScheduledRunRecord {
  duplicate_of?: string;
  cooldown_of?: string;
}

export interface ScheduleState {
  effective_max_records: number;
  target_amount_coverage_pct: number;
  auto_throttle: boolean;
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
}

interface RunScheduledScrapeOptions {
  idempotencyKey?: string;
  slot?: Slot;
  triggerSource?: TriggerSource;
}

let storeInstance: ScheduledRunStore | null = null;

function getStore(): ScheduledRunStore {
  if (!storeInstance) storeInstance = new ScheduledRunStore();
  return storeInstance;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function formatDate(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
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

  const pick = (type: string): string => parts.find((p) => p.type === type)?.value ?? '00';
  const weekdayShort = pick('weekday').slice(0, 2).toUpperCase();

  return {
    year: pick('year'),
    month: pick('month'),
    day: pick('day'),
    hour: Number(pick('hour')),
    minute: Number(pick('minute')),
    weekday: weekdayShort,
  };
}

function isScheduledDay(now: Date): boolean {
  return SCHEDULE_WEEKLY_DAYS.includes(getDateParts(now, TARGET_TIMEZONE).weekday);
}

function resolveSlot(now: Date): Slot {
  const parts = getDateParts(now, TARGET_TIMEZONE);
  return parts.hour < 12 ? 'morning' : 'afternoon';
}

function buildDefaultIdempotencyKey(now: Date, slot: Slot): string {
  const p = getDateParts(now, TARGET_TIMEZONE);
  return `${p.year}-${p.month}-${p.day}:${slot}`;
}

function getDeadlineParts(slot: Slot): { hour: number; minute: number } {
  if (slot === 'afternoon') {
    return {
      hour: AFTERNOON_DEADLINE_HOUR,
      minute: AFTERNOON_DEADLINE_MINUTE,
    };
  }

  return {
    hour: SCHEDULE_DEADLINE_HOUR,
    minute: SCHEDULE_DEADLINE_MINUTE,
  };
}

function buildDeadlineIso(now: Date, slot: Slot): string {
  const p = getDateParts(now, TARGET_TIMEZONE);
  const deadline = getDeadlineParts(slot);
  const hh = String(deadline.hour).padStart(2, '0');
  const mm = String(deadline.minute).padStart(2, '0');
  return `${p.year}-${p.month}-${p.day}T${hh}:${mm}:00 ${TARGET_TIMEZONE}`;
}

function isPastDeadline(slot: Slot, now: Date): boolean {
  const p = getDateParts(now, TARGET_TIMEZONE);
  const deadline = getDeadlineParts(slot);
  if (p.hour > deadline.hour) return true;
  if (p.hour === deadline.hour && p.minute >= deadline.minute) return true;
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
  const amountFound = records.filter((r) => Boolean(r.amount)).length;
  const amountMissing = Math.max(records.length - amountFound, 0);
  const amountCoveragePct = records.length > 0 ? (amountFound / records.length) * 100 : 0;
  const ocrSuccessCount = records.filter((r) => r.amount_reason !== 'ocr_missing' && r.amount_reason !== 'ocr_error').length;
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

function resolveEffectiveMaxRecords(): number {
  const state = getStore().getControlState();
  const seeded = state?.effective_max_records ?? SCHEDULE_MAX_RECORDS;
  return clamp(seeded, SCHEDULE_MAX_RECORDS_FLOOR, SCHEDULE_MAX_RECORDS_CEILING);
}

function maybeAdjustEffectiveMaxRecords(current: number, currentCoveragePct: number): number {
  if (!SCHEDULE_AUTO_THROTTLE) return current;

  if (currentCoveragePct < AMOUNT_MIN_COVERAGE_PCT) {
    return clamp(Math.floor(current * 0.8), SCHEDULE_MAX_RECORDS_FLOOR, SCHEDULE_MAX_RECORDS_CEILING);
  }

  const recent = getStore().getRecentSuccessfulRuns(2);
  const streak = [currentCoveragePct, ...recent.map((r) => r.amount_coverage_pct)];
  if (streak.length >= 3 && streak.every((pct) => pct >= AMOUNT_MIN_COVERAGE_PCT + 2)) {
    return clamp(Math.floor(current * 1.1), SCHEDULE_MAX_RECORDS_FLOOR, SCHEDULE_MAX_RECORDS_CEILING);
  }

  return current;
}

async function sendMissedRunAlert(slot: Slot, expectedAtIso: string, key: string): Promise<void> {
  const webhook = process.env.SCHEDULE_ALERT_WEBHOOK_URL;
  if (!webhook) {
    log({ stage: 'missed_run_alert_log_only', slot, expected_at: expectedAtIso, idempotency_key: key });
    return;
  }

  try {
    const payload = {
      text: `Missed scheduled scrape run for ${slot}. Expected success by ${expectedAtIso} (${TARGET_TIMEZONE}). idempotency_key=${key}`,
      slot,
      slot_time: key,
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
      log({ stage: 'missed_run_alert_failed', slot, status: res.status, response: body });
    }
  } catch (err: any) {
    log({ stage: 'missed_run_alert_error', slot, error: String(err?.message ?? err) });
  }
}

export async function runScheduledScrape(options: RunScheduledScrapeOptions = {}): Promise<ScheduledRun> {
  const now = new Date();
  const slot = options.slot ?? resolveSlot(now);
  const idempotencyKey = options.idempotencyKey ?? buildDefaultIdempotencyKey(now, slot);
  const triggerSource = options.triggerSource ?? 'external';

  const existing = getStore().getByIdempotencyKey(idempotencyKey);
  if (ENABLE_SCHEDULE_IDEMPOTENCY && existing && existing.status !== 'error') {
    log({ stage: 'scheduled_run_duplicate_skipped', idempotency_key: idempotencyKey, existing_run_id: existing.id });
    return { ...existing, duplicate_of: existing.id };
  }

  const mostRecent = getStore().getMostRecentRun();
  if (mostRecent) {
    const elapsedMs = Date.now() - new Date(mostRecent.started_at).getTime();
    const cooldownMs = SCHEDULE_COOLDOWN_MINUTES * 60 * 1000;
    if (elapsedMs >= 0 && elapsedMs < cooldownMs && mostRecent.status === 'running') {
      log({
        stage: 'scheduled_run_cooldown_skipped',
        idempotency_key: idempotencyKey,
        cooldown_of: mostRecent.id,
        cooldown_minutes: SCHEDULE_COOLDOWN_MINUTES,
      });
      return { ...mostRecent, cooldown_of: mostRecent.id };
    }
  }

  const effectiveMaxRecords = resolveEffectiveMaxRecords();
  const deadlineIso = buildDeadlineIso(now, slot);
  
  const { date_start, date_end } = getLast7DaysRange();

  const runId = existing?.status === 'error'
    ? existing.id
    : `sched_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;

  const run: ScheduledRunRecord = {
    id: runId,
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
    finished_at: undefined,
  };

  if (existing?.status === 'error') {
    getStore().updateRun(run);
    log({ stage: 'scheduled_run_retry_start', run_id: runId, idempotency_key: idempotencyKey, previous_status: existing.status });
  } else {
    getStore().insertRun(run);
  }

  log({
    stage: 'scheduled_run_start',
    run_id: runId,
    idempotency_key: idempotencyKey,
    date_start,
    date_end,
    max_records: effectiveMaxRecords,
    deadline_iso: deadlineIso,
    scheduled_day_match: isScheduledDay(now),
  });

  try {
    const scraper = (scrapers as any).ca_sos;
    const records = await scraper({
      date_start,
      date_end,
      max_records: effectiveMaxRecords,
      stop_requested: () => isPastDeadline(slot, new Date()),
    });

    const tabTitle = formatRunTabName(`Scheduled_${slot}_${runId}`, date_start, date_end, new Date());
    const uploadResult = await pushToSheetsForTab(records, tabTitle);

    if (uploadResult.uploaded !== records.length) {
      throw new Error(`sheet_upload_mismatch uploaded=${uploadResult.uploaded} records=${records.length}`);
    }

    const deadlineHit = isPastDeadline(slot, new Date());
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
    run.finished_at = new Date().toISOString();

    getStore().updateRun(run);

    const nextCap = deadlineHit || records.length === 0
      ? effectiveMaxRecords
      : maybeAdjustEffectiveMaxRecords(effectiveMaxRecords, run.amount_coverage_pct);
    if (nextCap !== effectiveMaxRecords) {
      getStore().upsertControlState(nextCap);
      log({
        stage: 'schedule_cap_adjusted',
        previous_effective_max_records: effectiveMaxRecords,
        next_effective_max_records: nextCap,
        amount_coverage_pct: run.amount_coverage_pct,
        threshold_pct: AMOUNT_MIN_COVERAGE_PCT,
      });
    } else {
      getStore().upsertControlState(effectiveMaxRecords);
    }

    log({
      stage: 'scheduled_run_complete',
      run_id: runId,
      idempotency_key: idempotencyKey,
      records_scraped: records.length,
      rows_uploaded: uploadResult.uploaded,
      amount_found_count: run.amount_found_count,
      amount_missing_count: run.amount_missing_count,
      amount_coverage_pct: run.amount_coverage_pct,
      ocr_success_pct: run.ocr_success_pct,
      row_fail_pct: run.row_fail_pct,
      deadline_hit: run.deadline_hit,
      effective_max_records: run.effective_max_records,
      partial: run.partial,
      tab_title: uploadResult.tab_title,
      duration_seconds: (Date.now() - new Date(run.started_at).getTime()) / 1000,
    });
  } catch (err: any) {
    run.status = 'error';
    run.error = String(err?.stack ?? err?.message ?? err);
    run.finished_at = new Date().toISOString();

    getStore().updateRun(run);

    log({ stage: 'scheduled_run_error', run_id: runId, idempotency_key: idempotencyKey, error: run.error, rows_uploaded: run.rows_uploaded });
  }

  return run;
}

export function getRunHistory(limit = 50): ScheduledRun[] {
  return getStore().getRunHistory(limit);
}

export function getScheduleState(): ScheduleState {
  const control = getStore().getControlState();
  const effectiveMax = control?.effective_max_records ?? clamp(SCHEDULE_MAX_RECORDS, SCHEDULE_MAX_RECORDS_FLOOR, SCHEDULE_MAX_RECORDS_CEILING);
  const recent = getStore().getRecentSuccessfulRuns(4);

  return {
    effective_max_records: effectiveMax,
    target_amount_coverage_pct: AMOUNT_MIN_COVERAGE_PCT,
    auto_throttle: SCHEDULE_AUTO_THROTTLE,
    recent_quality: recent.map((r) => ({
      id: r.id,
      started_at: r.started_at,
      amount_coverage_pct: r.amount_coverage_pct,
      ocr_success_pct: r.ocr_success_pct,
      row_fail_pct: r.row_fail_pct,
      partial: r.partial,
      deadline_hit: r.deadline_hit,
      effective_max_records: r.effective_max_records,
    })),
  };
}

export function getNextRuns(): { schedule: string; days: string; run_time: string; deadline_time: string; timezone: string } {
  return {
    schedule: 'weekly',
    days: SCHEDULE_WEEKLY_DAYS.join(','),
    run_time: `${String(SCHEDULE_RUN_HOUR).padStart(2, '0')}:${String(SCHEDULE_RUN_MINUTE).padStart(2, '0')}`,
    deadline_time: `${String(SCHEDULE_DEADLINE_HOUR).padStart(2, '0')}:${String(SCHEDULE_DEADLINE_MINUTE).padStart(2, '0')}`,
    timezone: TARGET_TIMEZONE,
  };
}

function expectedKeyForSlot(now: Date, slot: Slot): string {
  return buildDefaultIdempotencyKey(now, slot);
}

export async function checkMissedRuns(): Promise<void> {
  const now = new Date();
  if (!isScheduledDay(now)) return;

  const parts = getDateParts(now, TARGET_TIMEZONE);

  const checks: Array<{ slot: Slot; dueHour: number; dueMinute: number }> = [
    { slot: 'morning', dueHour: MORNING_RUN_HOUR, dueMinute: MORNING_RUN_MINUTE + MISSED_RUN_GRACE_MINUTES },
    { slot: 'afternoon', dueHour: AFTERNOON_RUN_HOUR, dueMinute: AFTERNOON_RUN_MINUTE + MISSED_RUN_GRACE_MINUTES },
  ];

  for (const check of checks) {
    const dueHour = check.dueHour + Math.floor(check.dueMinute / 60);
    const dueMinute = check.dueMinute % 60;
    const overdue = parts.hour > dueHour || (parts.hour === dueHour && parts.minute >= dueMinute);
    if (!overdue) continue;

    const key = expectedKeyForSlot(now, check.slot);
    const success = getStore().getSuccessfulRunByIdempotencyKey(key);
    if (success) continue;

    const existingAlert = getStore().getMissedAlertByKey(key);
    if (existingAlert) continue;

    const expectedAtIso = `${parts.year}-${parts.month}-${parts.day}T${String(dueHour).padStart(2, '0')}:${String(dueMinute).padStart(2, '0')}:00`;
    getStore().insertMissedAlert({ idempotency_key: key, slot: check.slot, expected_by: expectedAtIso });
    await sendMissedRunAlert(check.slot, expectedAtIso, key);
    log({ stage: 'missed_run_alerted', slot: check.slot, idempotency_key: key, expected_by: expectedAtIso });
  }
}



