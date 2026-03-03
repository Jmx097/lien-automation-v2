#!/usr/bin/env node
// scripts/run-chunk-range.ts
//
// Helper to run a date range as multiple gated chunks using processChunk().
// Does NOT change cron or existing /scrape behavior.

import dotenv from 'dotenv';
import { processChunk } from '../src/scraper/chunk-processor';

dotenv.config();

function parseDate(input: string): Date {
  const [m, d, y] = input.split('/').map((v) => parseInt(v, 10));
  if (!m || !d || !y) {
    throw new Error(`Invalid date format (expected MM/DD/YYYY): ${input}`);
  }
  return new Date(y, m - 1, d);
}

function formatDate(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function addDays(d: Date, days: number): Date {
  const copy = new Date(d.getTime());
  copy.setDate(copy.getDate() + days);
  return copy;
}

function buildDateWindows(
  start: string,
  end: string,
  windowDays: number
): { startDate: string; endDate: string }[] {
  const startDate = parseDate(start);
  const endDate = parseDate(end);

  if (endDate < startDate) {
    throw new Error(`end_date (${end}) must be >= start_date (${start})`);
  }

  const windows: { startDate: string; endDate: string }[] = [];
  let cursor = new Date(startDate.getTime());

  while (cursor <= endDate) {
    const windowStart = new Date(cursor.getTime());
    const windowEnd = addDays(windowStart, windowDays - 1);
    const clampedEnd = windowEnd > endDate ? endDate : windowEnd;

    windows.push({
      startDate: formatDate(windowStart),
      endDate: formatDate(clampedEnd),
    });

    cursor = addDays(clampedEnd, 1);
  }

  return windows;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  const site = args[0] || 'ca_sos';
  const start = args[1];
  const end = args[2];
  const windowDays = args[3] ? parseInt(args[3], 10) : 7;
  const maxRecords = args[4] ? parseInt(args[4], 10) : 25;

  if (!start || !end) {
    // eslint-disable-next-line no-console
    console.error(
      'Usage: run-chunk-range <site> <start MM/DD/YYYY> <end MM/DD/YYYY> [windowDays=7] [maxRecords=25]'
    );
    process.exit(1);
  }

  if (site !== 'ca_sos') {
    // eslint-disable-next-line no-console
    console.error(`Only site=ca_sos is supported for chunked range runs (got: ${site})`);
    process.exit(1);
  }

  const windows = buildDateWindows(start, end, windowDays);

  // eslint-disable-next-line no-console
  console.log(
    `Running chunked range for ${site} from ${start} to ${end} in ${windows.length} window(s) of up to ${windowDays} day(s), maxRecords=${maxRecords}`
  );

  let totalProcessed = 0;
  let totalFailed = 0;

  for (const window of windows) {
    const chunkId = `chunk-${site}-${window.startDate.replace(/\//g, '-')}_to_${window.endDate.replace(
      /\//g,
      '-'
    )}`;

    // eslint-disable-next-line no-console
    console.log(
      `\n=== Processing chunk ${chunkId} (${window.startDate} -> ${window.endDate}) ===`
    );

    const result = await processChunk({
      chunkId,
      startDate: window.startDate,
      endDate: window.endDate,
      maxRecords,
    });

    // eslint-disable-next-line no-console
    console.log(
      `Result: success=${result.success} processed=${result.processedCount} failed=${result.failedCount}`
    );
    if (result.errors.length) {
      // eslint-disable-next-line no-console
      console.log(`Errors: ${result.errors.join('; ')}`);
    }

    totalProcessed += result.processedCount;
    totalFailed += result.failedCount;
  }

  // eslint-disable-next-line no-console
  console.log(
    `\nCompleted chunked range: totalProcessed=${totalProcessed} totalFailed=${totalFailed}`
  );
}

if (require.main === module) {
  // eslint-disable-next-line no-console
  main().catch((err) => {
    console.error('Fatal error in run-chunk-range:', err);
    process.exit(1);
  });
}

