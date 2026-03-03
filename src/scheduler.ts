import crypto from 'crypto';
import { log } from './utils/logger';
import { scrapers } from './scraper/index';
import { ScheduledRunStore, ScheduledRunRecord } from './scheduler/store';
import { formatRunTabName, pushToSheetsForTab } from './sheets/push';

const SCHEDULE_MAX_RECORDS = Number(process.env.SCHEDULE_MAX_RECORDS ?? '1000');
const LOOKBACK_DAYS = 7;
const MISSED_RUN_GRACE_MINUTES = 45;
const SCHEDULE_COOLDOWN_MINUTES = 10;
const ENABLE_SCHEDULE_IDEMPOTENCY = process.env.ENABLE_SCHEDULE_IDEMPOTENCY === '1';
const MORNING_RUN_HOUR = Number(process.env.SCHEDULE_MORNING_HOUR ?? '7');
const MORNING_RUN_MINUTE = Number(process.env.SCHEDULE_MORNING_MINUTE ?? '30');
const AFTERNOON_RUN_HOUR = Number(process.env.SCHEDULE_AFTERNOON_HOUR ?? '19');
const AFTERNOON_RUN_MINUTE = Number(process.env.SCHEDULE_AFTERNOON_MINUTE ?? '30');

type Slot = 'morning' | 'afternoon';
type TriggerSource = 'external' | 'manual';

export interface ScheduledRun extends ScheduledRunRecord {
  duplicate_of?: string;
  cooldown_of?: string;
}

interface RunScheduledScrapeOptions {
  idempotencyKey?: string;
  slot?: Slot;
  triggerSource?: TriggerSource;
}

const store = new ScheduledRunStore();

function formatDate(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function getNyDateParts(now: Date): { year: string; month: string; day: string; hour: number; minute: number } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(now);

  const pick = (type: string): string => parts.find((p) => p.type === type)?.value ?? '00';
  return {
    year: pick('year'),
    month: pick('month'),
    day: pick('day'),
    hour: Number(pick('hour')),
    minute: Number(pick('minute')),
  };
}

function resolveSlot(now: Date): Slot {
  const ny = getNyDateParts(now);
  if (ny.hour < 12) return 'morning';
  return 'afternoon';
}

function buildDefaultIdempotencyKey(now: Date, slot: Slot): string {
  const ny = getNyDateParts(now);
  return `${ny.year}-${ny.month}-${ny.day}:${slot}`;
}

function getLast7DaysRange(): { date_start: string; date_end: string } {
  const now = new Date();
  const end = new Date(now);
  const start = new Date(now);
  start.setDate(start.getDate() - LOOKBACK_DAYS);
  return { date_start: formatDate(start), date_end: formatDate(end) };
}

async function sendMissedRunAlert(slot: Slot, expectedAtIso: string, key: string): Promise<void> {
  const webhook = process.env.SCHEDULE_ALERT_WEBHOOK_URL;
  if (!webhook) {
    log({ stage: 'missed_run_alert_log_only', slot, expected_at: expectedAtIso, idempotency_key: key });
    return;
  }

  try {
    const payload = {
      text: `Missed scheduled scrape run for ${slot}. Expected success by ${expectedAtIso} (ET). idempotency_key=${key}`,
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

  const existing = store.getByIdempotencyKey(idempotencyKey);
  if (ENABLE_SCHEDULE_IDEMPOTENCY && existing && existing.status !== 'error') {
    log({ stage: 'scheduled_run_duplicate_skipped', idempotency_key: idempotencyKey, existing_run_id: existing.id });
    return { ...existing, duplicate_of: existing.id };
  }

  const mostRecent = store.getMostRecentRun();
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
    error: undefined,
    finished_at: undefined,
  };

  if (existing?.status === 'error') {
    store.updateRun(run);
    log({ stage: 'scheduled_run_retry_start', run_id: runId, idempotency_key: idempotencyKey, previous_status: existing.status });
  } else {
    store.insertRun(run);
  }

  log({ stage: 'scheduled_run_start', run_id: runId, idempotency_key: idempotencyKey, date_start, date_end, max_records: SCHEDULE_MAX_RECORDS });

  try {
    const scraper = (scrapers as any).ca_sos;
    const records = await scraper({ date_start, date_end, max_records: SCHEDULE_MAX_RECORDS });
    const tabTitle = formatRunTabName(`Scheduled_${slot}_${runId}`, date_start, date_end, new Date());
    const uploadResult = await pushToSheetsForTab(records, tabTitle);

    run.records_scraped = records.length;
    run.rows_uploaded = uploadResult.uploaded;
    run.status = 'success';
    run.finished_at = new Date().toISOString();

    store.updateRun(run);

    log({
      stage: 'scheduled_run_complete',
      run_id: runId,
      idempotency_key: idempotencyKey,
      records_scraped: records.length,
      rows_uploaded: uploadResult.uploaded,
      tab_title: uploadResult.tab_title,
      duration_seconds: (Date.now() - new Date(run.started_at).getTime()) / 1000,
    });
  } catch (err: any) {
    run.status = 'error';
    run.error = String(err?.stack ?? err?.message ?? err);
    run.finished_at = new Date().toISOString();

    store.updateRun(run);

    log({ stage: 'scheduled_run_error', run_id: runId, idempotency_key: idempotencyKey, error: run.error, rows_uploaded: run.rows_uploaded });
  }

  return run;
}

export function getRunHistory(limit = 50): ScheduledRun[] {
  return store.getRunHistory(limit);
}

export function getNextRuns(): { morning: string; afternoon: string } {
  return {
    morning: `${String(MORNING_RUN_HOUR).padStart(2, '0')}:${String(MORNING_RUN_MINUTE).padStart(2, '0')} America/New_York daily`,
    afternoon: `${String(AFTERNOON_RUN_HOUR).padStart(2, '0')}:${String(AFTERNOON_RUN_MINUTE).padStart(2, '0')} America/New_York daily`,
  };
}

function expectedKeyForSlot(now: Date, slot: Slot): string {
  return buildDefaultIdempotencyKey(now, slot);
}

export async function checkMissedRuns(): Promise<void> {
  const now = new Date();
  const ny = getNyDateParts(now);

  const checks: Array<{ slot: Slot; dueHour: number; dueMinute: number }> = [
    { slot: 'morning', dueHour: MORNING_RUN_HOUR, dueMinute: MORNING_RUN_MINUTE + MISSED_RUN_GRACE_MINUTES },
    { slot: 'afternoon', dueHour: AFTERNOON_RUN_HOUR, dueMinute: AFTERNOON_RUN_MINUTE + MISSED_RUN_GRACE_MINUTES },
  ];

  for (const check of checks) {
    const dueHour = check.dueHour + Math.floor(check.dueMinute / 60);
    const dueMinute = check.dueMinute % 60;
    const overdue = ny.hour > dueHour || (ny.hour === dueHour && ny.minute >= dueMinute);
    if (!overdue) continue;

    const key = expectedKeyForSlot(now, check.slot);
    const success = store.getSuccessfulRunByIdempotencyKey(key);
    if (success) continue;

    const existingAlert = store.getMissedAlertByKey(key);
    if (existingAlert) continue;

    const expectedAtIso = `${ny.year}-${ny.month}-${ny.day}T${String(dueHour).padStart(2, '0')}:${String(dueMinute).padStart(2, '0')}:00-05:00`;
    store.insertMissedAlert({ idempotency_key: key, slot: check.slot, expected_by: expectedAtIso });
    await sendMissedRunAlert(check.slot, expectedAtIso, key);
    log({ stage: 'missed_run_alerted', slot: check.slot, idempotency_key: key, expected_by: expectedAtIso });
  }
}
