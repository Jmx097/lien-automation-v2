import dotenv from 'dotenv';
import { createIsolatedBrowserContext } from '../src/browser/transport';

dotenv.config();

const RESULTS_URL =
  'https://recorder.maricopa.gov/recording/document-search-results.html?lastNames=&firstNames=&middleNameIs=&documentTypeSelector=code&documentCode=FL&beginDate=2026-01-01&endDate=2026-02-13';

async function main(): Promise<void> {
  const handle = await createIsolatedBrowserContext();
  const requests: Array<{ method: string; url: string; status?: number }> = [];

  try {
    const page = await handle.context.newPage();
    page.on('response', async (response) => {
      const url = response.url();
      if (!/maricopa\.gov/i.test(url)) return;
      requests.push({
        method: response.request().method(),
        url,
        status: response.status(),
      });
    });

    await page.goto(RESULTS_URL, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(2000);

    const rows = await page.locator('table tbody tr').count().catch(() => 0);
    if (rows > 0) {
      await page.locator('table tbody tr').first().click().catch(() => null);
      await page.waitForTimeout(2000);
    }

    console.log(
      JSON.stringify(
        {
          transport_mode: handle.mode,
          final_url: page.url(),
          row_count: rows,
          maricopa_requests: requests.filter((request) => /publicapi\.recorder\.maricopa\.gov|recorder\.maricopa\.gov/i.test(request.url)),
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
