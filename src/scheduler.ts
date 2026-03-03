import crypto from 'crypto';
import { log } from './utils/logger';
import { scrapers } from './scraper/index';
import { ScheduledRunStore, ScheduledRunRecord } from './scheduler/store';

const SCHEDULE_MAX_RECORDS = 10;
const LOOKBACK_DAYS = 7;
const MISSED_RUN_GRACE_MINUTES = 45;

type Slot = 'morning' | 'afternoon';
type TriggerSource = 'external' | 'manual';

export interface ScheduledRun extends ScheduledRunRecord {
  duplicate_of?: string;
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
  if (existing && existing.status !== 'error') {
    log({ stage: 'scheduled_run_duplicate_skipped', idempotency_key: idempotencyKey, existing_run_id: existing.id });
    return { ...existing, duplicate_of: existing.id };
  }

  const { date_start, date_end } = getLast7DaysRange();
  const runId = `sched_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;

  const run: ScheduledRunRecord = {
    id: runId,
    idempotency_key: idempotencyKey,
    slot,
    trigger_source: triggerSource,
    started_at: new Date().toISOString(),
    status: 'running',
    records_scraped: 0,
    records_skipped: 0,
  };

  store.insertRun(run);

  log({ stage: 'scheduled_run_start', run_id: runId, idempotency_key: idempotencyKey, date_start, date_end, max_records: SCHEDULE_MAX_RECORDS });

  try {
    const scraper = (scrapers as any).ca_sos;
    const records = await scraper({ date_start, date_end, max_records: SCHEDULE_MAX_RECORDS });

    run.records_scraped = records.length;
    run.status = 'success';
    run.finished_at = new Date().toISOString();

    store.updateRun(run);

    log({
      stage: 'scheduled_run_complete',
      run_id: runId,
      idempotency_key: idempotencyKey,
      records_scraped: records.length,
      duration_seconds: (Date.now() - new Date(run.started_at).getTime()) / 1000,
    });
  } catch (err: any) {
    run.status = 'error';
    run.error = err.message;
    run.finished_at = new Date().toISOString();

    store.updateRun(run);

    log({ stage: 'scheduled_run_error', run_id: runId, idempotency_key: idempotencyKey, error: err.message });
  }

  return run;
}

export function getRunHistory(limit = 50): ScheduledRun[] {
  return store.getRunHistory(limit);
}

export function getNextRuns(): { morning: string; afternoon: string } {
  return {
    morning: '07:30 AM America/New_York daily',
    afternoon: '02:30 PM America/New_York daily',
  };
}

function expectedKeyForSlot(now: Date, slot: Slot): string {
  return buildDefaultIdempotencyKey(now, slot);
}

export async function checkMissedRuns(): Promise<void> {
  const now = new Date();
  const ny = getNyDateParts(now);

  const checks: Array<{ slot: Slot; dueHour: number; dueMinute: number }> = [
    { slot: 'morning', dueHour: 7, dueMinute: 30 + MISSED_RUN_GRACE_MINUTES },
    { slot: 'afternoon', dueHour: 14, dueMinute: 30 + MISSED_RUN_GRACE_MINUTES },
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
