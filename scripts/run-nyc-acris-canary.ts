import dotenv from 'dotenv';
import { scrapeNYCAcris } from '../src/scraper/nyc_acris';
import { pushRunToNewSheetTab, syncMasterSheetTab } from '../src/sheets/push';

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
  const max_records = Number(process.env.JOB_MAX_RECORDS ?? process.env.ACRIS_INITIAL_MAX_RECORDS ?? '5');

  const rows = await scrapeNYCAcris({
    date_start,
    date_end,
    max_records,
  });

  const upload = await pushRunToNewSheetTab(rows, {
    label: 'nyc_acris_canary',
    date_start,
    date_end,
    run_started_at: new Date(),
  });
  const masterSync = await syncMasterSheetTab({
    tabTitle: 'Master_canary',
    reviewTabTitle: 'Review_Queue_canary',
    includePrefixes: ['nyc_acris_canary_'],
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
        master_tab_title: masterSync.tab_title,
        review_tab_title: masterSync.review_tab_title,
        quarantined_row_count: masterSync.quarantined_row_count,
        new_master_row_count: masterSync.new_master_row_count,
      },
      null,
      2
    )
  );
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
