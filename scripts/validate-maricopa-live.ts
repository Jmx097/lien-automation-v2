import dotenv from 'dotenv';
import { fetchLatestMaricopaSearchableDate, scrapeMaricopaRecorder } from '../src/scraper/maricopa_recorder';

dotenv.config();

async function main(): Promise<void> {
  const positiveDateStart = process.env.MARICOPA_POSITIVE_DATE_START ?? '01/01/2026';
  const positiveDateEnd = process.env.MARICOPA_POSITIVE_DATE_END ?? '02/13/2026';
  const zeroDateStart = process.env.MARICOPA_ZERO_DATE_START ?? '01/01/2030';
  const zeroDateEnd = process.env.MARICOPA_ZERO_DATE_END ?? '01/02/2030';
  const maxRecords = Number(process.env.MARICOPA_VALIDATION_MAX_RECORDS ?? '2');

  const latestSearchableDate = await fetchLatestMaricopaSearchableDate();
  const positiveRows = await scrapeMaricopaRecorder({
    date_start: positiveDateStart,
    date_end: positiveDateEnd,
    max_records: maxRecords,
  });
  const zeroRows = await scrapeMaricopaRecorder({
    date_start: zeroDateStart,
    date_end: zeroDateEnd,
    max_records: maxRecords,
  });

  console.log(
    JSON.stringify(
      {
        latest_searchable_date: latestSearchableDate,
        positive_range: {
          date_start: positiveDateStart,
          date_end: positiveDateEnd,
          record_count: positiveRows.length,
          first_file_number: positiveRows[0]?.file_number ?? null,
        },
        zero_range: {
          date_start: zeroDateStart,
          date_end: zeroDateEnd,
          record_count: zeroRows.length,
        },
      },
      null,
      2,
    ),
  );
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
