import dotenv from 'dotenv';
import {
  discoverMaricopaArtifactCandidates,
  loadMaricopaSessionState,
  saveMaricopaArtifactCandidates,
} from '../src/scraper/maricopa_artifacts';
import { createIsolatedBrowserContext } from '../src/browser/transport';

dotenv.config({ quiet: true });

const RESULTS_URL =
  'https://recorder.maricopa.gov/recording/document-search-results.html?lastNames=&firstNames=&middleNameIs=&documentTypeSelector=code&documentCode=FL&beginDate=2026-01-01&endDate=2026-02-13';

async function main(): Promise<void> {
  const session = await loadMaricopaSessionState();
  if (!session) {
    throw new Error('No persisted Maricopa session state found. Run npm run refresh:maricopa-session first.');
  }

  const handle = await createIsolatedBrowserContext({
    contextOptions: { storageState: session.storage_state_path },
  });
  const requests: Array<{ method: string; url: string; status?: number; resourceType?: string }> = [];

  try {
    const page = await handle.context.newPage();
    page.on('response', (response) => {
      const url = response.url();
      if (!/maricopa\.gov/i.test(url)) return;
      requests.push({
        method: response.request().method(),
        url,
        status: response.status(),
        resourceType: response.request().resourceType(),
      });
    });

    await page.goto(RESULTS_URL, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(2000);

    const rows = await page.locator('table tbody tr').count().catch(() => 0);
    let recordingNumber: string | undefined;

    if (rows > 0) {
      const firstRow = page.locator('table tbody tr').first();
      const rowText = await firstRow.textContent().catch(() => '');
      recordingNumber = rowText?.match(/\b\d{11}\b/)?.[0];
      await firstRow.click().catch(() => null);
      await page.waitForTimeout(3000);
    }

    const candidates = discoverMaricopaArtifactCandidates(requests, recordingNumber);
    await saveMaricopaArtifactCandidates(candidates);

    console.log(
      JSON.stringify(
        {
          transport_mode: handle.mode,
          session_captured_at: session.captured_at,
          final_url: page.url(),
          row_count: rows,
          recording_number: recordingNumber ?? null,
          candidate_count: candidates.length,
          candidates,
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
