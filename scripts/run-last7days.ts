import dotenv from 'dotenv';
import { scrapeCASOS_Enhanced } from '../src/scraper/ca_sos_enhanced';
import { pushToSheets } from '../src/sheets/push';

dotenv.config();

function formatDateMMDDYYYY(d: Date): string {
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const yyyy = d.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function getMaxRecordsFromArgsOrEnv(): number | undefined {
  const argValue = process.argv[2];
  const envValue = process.env.MAX_RECORDS_OVERRIDE;

  const raw = argValue ?? envValue;
  if (!raw) return undefined;

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    console.warn(
      `Ignoring invalid max records override "${raw}". Using default of 10 instead for this run.`
    );
    return 10;
  }

  return parsed;
}

async function main() {
  const today = new Date();
  const start = new Date(today);
  start.setDate(today.getDate() - 6);

  const date_start = formatDateMMDDYYYY(start);
  const date_end = formatDateMMDDYYYY(today);

  const overrideMax = getMaxRecordsFromArgsOrEnv();
  const effectiveMax = overrideMax ?? 10;

  console.log(
    `Running CA SOS scrape for last 7 days: ${date_start} -> ${date_end} (max_records=${effectiveMax}${
      overrideMax ? ' override' : ' default'
    })`
  );

  const records = await scrapeCASOS_Enhanced({
    date_start,
    date_end,
    ...(overrideMax !== undefined ? { max_records: overrideMax } : {}),
  });

  console.log(`Scrape complete. Records scraped: ${records.length}. Pushing to Sheets...`);

  const result = await pushToSheets(records);

  console.log(`Push complete. Rows uploaded: ${result.uploaded}.`);
}

main().catch((err) => {
  console.error('Error running last-7-days scrape:', err);
  process.exit(1);
});

