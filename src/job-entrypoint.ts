import dotenv from 'dotenv';
import { scrapers } from './scraper';
import { pushRunToNewSheetTab } from './sheets/push';
import { log } from './utils/logger';
import { resolveTransportMode } from './browser/transport';

dotenv.config();

type SupportedSite = keyof typeof scrapers;

interface JobConfig {
  site: SupportedSite;
  date_start: string;
  date_end: string;
  max_records?: number;
  job_id: string;
}

const DEFAULT_SITE: SupportedSite = 'ca_sos';
const DEFAULT_LOOKBACK_DAYS = 7;

function formatDate(input: Date): string {
  const mm = String(input.getMonth() + 1).padStart(2, '0');
  const dd = String(input.getDate()).padStart(2, '0');
  const yyyy = input.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function computeDefaultRange(lookbackDays: number): { date_start: string; date_end: string } {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - lookbackDays);

  return {
    date_start: formatDate(start),
    date_end: formatDate(end),
  };
}

function requireEnv(name: 'SHEETS_KEY' | 'SHEET_ID'): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function resolveConfig(): JobConfig {
  requireEnv('SHEETS_KEY');
  requireEnv('SHEET_ID');
  if (resolveTransportMode() === 'local' && !process.env.BRIGHTDATA_PROXY_SERVER?.trim() && !process.env.SBR_CDP_URL?.trim()) {
    throw new Error('Missing browser transport environment configuration');
  }

  const lookbackDays = Number(process.env.JOB_LOOKBACK_DAYS ?? DEFAULT_LOOKBACK_DAYS);
  const maxRecords = process.env.JOB_MAX_RECORDS ? Number(process.env.JOB_MAX_RECORDS) : undefined;

  if (!Number.isFinite(lookbackDays) || lookbackDays < 0) {
    throw new Error(`Invalid JOB_LOOKBACK_DAYS value: ${process.env.JOB_LOOKBACK_DAYS}`);
  }

  if (maxRecords !== undefined && (!Number.isFinite(maxRecords) || maxRecords < 1)) {
    throw new Error(`Invalid JOB_MAX_RECORDS value: ${process.env.JOB_MAX_RECORDS}`);
  }

  const fallbackRange = computeDefaultRange(lookbackDays);

  return {
    site: (process.env.JOB_SITE as SupportedSite | undefined) ?? DEFAULT_SITE,
    date_start: process.env.JOB_DATE_START ?? fallbackRange.date_start,
    date_end: process.env.JOB_DATE_END ?? fallbackRange.date_end,
    max_records: maxRecords,
    job_id: process.env.CLOUD_RUN_EXECUTION ?? `local-${Date.now()}`,
  };
}

async function main(): Promise<void> {
  const startedAt = new Date();
  const config = resolveConfig();
  const scraper = scrapers[config.site];

  if (!scraper) {
    throw new Error(`Unsupported JOB_SITE value: ${config.site}. Supported: ${Object.keys(scrapers).join(', ')}`);
  }

  log({
    stage: 'cloud_run_job_start',
    job_id: config.job_id,
    site: config.site,
    date_start: config.date_start,
    date_end: config.date_end,
    max_records: config.max_records ?? null,
    started_at: startedAt.toISOString(),
  });

  try {
    const records = await scraper({
      date_start: config.date_start,
      date_end: config.date_end,
      max_records: config.max_records,
    });

    const sheetResult = await pushRunToNewSheetTab(records, {
      label: `${config.site}_job_${config.job_id}`,
      date_start: config.date_start,
      date_end: config.date_end,
      run_started_at: startedAt,
    });
    const finishedAt = new Date();

    log({
      stage: 'cloud_run_job_complete',
      job_id: config.job_id,
      started_at: startedAt.toISOString(),
      finished_at: finishedAt.toISOString(),
      duration_seconds: (finishedAt.getTime() - startedAt.getTime()) / 1000,
      records_scraped: records.length,
      rows_uploaded: sheetResult.uploaded,
      error: null,
    });
  } catch (err: any) {
    const finishedAt = new Date();

    log({
      stage: 'cloud_run_job_error',
      job_id: config.job_id,
      started_at: startedAt.toISOString(),
      finished_at: finishedAt.toISOString(),
      duration_seconds: (finishedAt.getTime() - startedAt.getTime()) / 1000,
      records_scraped: 0,
      rows_uploaded: 0,
      error: err?.message ?? String(err),
    });

    throw err;
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
