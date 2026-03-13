import dotenv from 'dotenv';
import { scrapeMaricopaRecorder } from '../src/scraper/maricopa_recorder';
import { pushRunToNewSheetTab } from '../src/sheets/push';

dotenv.config();

function formatDate(input: Date): string {
  const mm = String(input.getMonth() + 1).padStart(2, '0');
  const dd = String(input.getDate()).padStart(2, '0');
  const yyyy = input.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

async function main(): Promise<void> {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 7);

  const date_start = process.env.JOB_DATE_START ?? formatDate(start);
  const date_end = process.env.JOB_DATE_END ?? formatDate(end);
  const max_records = Number(process.env.JOB_MAX_RECORDS ?? process.env.MARICOPA_MAX_RECORDS ?? '5');

  const rows = await scrapeMaricopaRecorder({
    date_start,
    date_end,
    max_records,
  });

  const upload = await pushRunToNewSheetTab(rows, {
    label: 'maricopa_recorder_canary',
    date_start,
    date_end,
    run_started_at: new Date(),
  });

  console.log(
    JSON.stringify(
      {
        date_start,
        date_end,
        max_records,
        records_scraped: rows.length,
        rows_uploaded: upload.uploaded,
        tab_title: upload.tab_title,
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
