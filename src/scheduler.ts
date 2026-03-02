import { log } from './utils/logger';
import { scrapers } from './scraper/index';
import { pushToSheets } from './sheets/push';

const SCHEDULE_MAX_RECORDS = 10;
const LOOKBACK_DAYS = 7;

interface ScheduledRun {
  id: string;
  started_at: string;
  finished_at?: string;
  status: 'running' | 'success' | 'error';
  records_scraped: number;
  records_skipped: number;
  error?: string;
}

const runHistory: ScheduledRun[] = [];

function formatDate(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function getLast7DaysRange(): { date_start: string; date_end: string } {
  const now = new Date();
  const end = new Date(now);
  const start = new Date(now);
  start.setDate(start.getDate() - LOOKBACK_DAYS);
  return { date_start: formatDate(start), date_end: formatDate(end) };
}

export async function runScheduledScrape(): Promise<ScheduledRun> {
  const { date_start, date_end } = getLast7DaysRange();
  const runId = `sched_${Date.now()}`;

  const run: ScheduledRun = {
    id: runId,
    started_at: new Date().toISOString(),
    status: 'running',
    records_scraped: 0,
    records_skipped: 0,
  };
  runHistory.push(run);
  if (runHistory.length > 50) runHistory.shift();

  log({ stage: 'scheduled_run_start', run_id: runId, date_start, date_end, max_records: SCHEDULE_MAX_RECORDS });

  try {
    const scraper = (scrapers as any).ca_sos;
    const records = await scraper({ date_start, date_end, max_records: SCHEDULE_MAX_RECORDS });

    run.records_scraped = records.length;
    run.status = 'success';
    run.finished_at = new Date().toISOString();

    log({
      stage: 'scheduled_run_complete',
      run_id: runId,
      records_scraped: records.length,
      duration_seconds: (Date.now() - new Date(run.started_at).getTime()) / 1000,
    });
  } catch (err: any) {
    run.status = 'error';
    run.error = err.message;
    run.finished_at = new Date().toISOString();

    log({ stage: 'scheduled_run_error', run_id: runId, error: err.message });
  }

  return run;
}

export function getRunHistory(): ScheduledRun[] {
  return [...runHistory].reverse();
}

export function getNextRuns(): { morning: string; afternoon: string } {
  const now = new Date();

  const morningEST = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  morningEST.setHours(7, 30, 0, 0);

  const afternoonEST = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  afternoonEST.setHours(14, 30, 0, 0);

  return {
    morning: '07:30 AM EST daily',
    afternoon: '02:30 PM EST daily',
  };
}

let schedulerInterval: ReturnType<typeof setInterval> | null = null;
let schedulerRunning = false;

export function startScheduler(): void {
  if (schedulerInterval) return;

  log({ stage: 'scheduler_started', schedule: '07:30 EST + 14:30 EST' });

  schedulerInterval = setInterval(async () => {
    if (schedulerRunning) return;

    const now = new Date();
    const estString = now.toLocaleString('en-US', { timeZone: 'America/New_York', hour12: false });
    const estTime = new Date(estString);
    const hour = estTime.getHours();
    const minute = estTime.getMinutes();

    const isMorningWindow = hour === 7 && minute >= 25 && minute <= 35;
    const isAfternoonWindow = hour === 14 && minute >= 25 && minute <= 35;

    if (!isMorningWindow && !isAfternoonWindow) return;

    const lastRun = runHistory[runHistory.length - 1];
    if (lastRun) {
      const elapsed = Date.now() - new Date(lastRun.started_at).getTime();
      if (elapsed < 3 * 60 * 60 * 1000) return;
    }

    schedulerRunning = true;
    try {
      await runScheduledScrape();
    } finally {
      schedulerRunning = false;
    }
  }, 60_000);
}

export function stopScheduler(): void {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    schedulerInterval = null;
    log({ stage: 'scheduler_stopped' });
  }
}
