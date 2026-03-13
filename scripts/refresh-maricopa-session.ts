import dotenv from 'dotenv';
import { createIsolatedBrowserContext } from '../src/browser/transport';
import { saveMaricopaSessionState } from '../src/scraper/maricopa_artifacts';

dotenv.config();

const RESULTS_URL =
  'https://recorder.maricopa.gov/recording/document-search-results.html?lastNames=&firstNames=&middleNameIs=&documentTypeSelector=code&documentCode=FL&beginDate=2026-01-01&endDate=2026-02-13';

async function main(): Promise<void> {
  const handle = await createIsolatedBrowserContext({
    headless: process.env.HEADLESS?.toLowerCase() === 'true' ? true : false,
  });

  try {
    const page = await handle.context.newPage();
    await page.goto(RESULTS_URL, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(2000);

    let rows = await page.locator('table tbody tr').count().catch(() => 0);
    if (rows === 0) {
      await page.waitForTimeout(30000);
      rows = await page.locator('table tbody tr').count().catch(() => 0);
    }

    const storageState = await handle.context.storageState();
    const saved = await saveMaricopaSessionState(storageState, handle.mode, page.url());

    console.log(
      JSON.stringify(
        {
          transport_mode: handle.mode,
          final_url: page.url(),
          row_count: rows,
          session: saved,
        },
        null,
        2,
      ),
    );
  } finally {
    await handle.close();
  }
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
