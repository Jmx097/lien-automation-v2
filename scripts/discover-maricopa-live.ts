import dotenv from 'dotenv';
import {
  discoverMaricopaArtifactCandidates,
  filterValidMaricopaArtifactCandidates,
  loadMaricopaSessionState,
  type MaricopaArtifactCandidate,
  saveMaricopaArtifactCandidates,
} from '../src/scraper/maricopa_artifacts';
import { createIsolatedBrowserContext } from '../src/browser/transport';

dotenv.config({ quiet: true });

const RESULTS_URL =
  'https://recorder.maricopa.gov/recording/document-search-results.html?lastNames=&firstNames=&middleNameIs=&documentTypeSelector=code&documentCode=FL&beginDate=2026-01-01&endDate=2026-02-13';

function buildModalCandidates(recordingNumber: string, hrefs: string[]): MaricopaArtifactCandidate[] {
  return filterValidMaricopaArtifactCandidates(
    hrefs.map((href) => ({
      urlTemplate: href.replace(recordingNumber, '{recordingNumber}'),
      sampleUrl: href,
      kind: /preview\/pdf/i.test(href) ? 'pdf' : /document-preview/i.test(href) ? 'image' : 'document',
    })),
    recordingNumber,
  );
}

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
      const firstLink = page.locator('table tbody tr a.record-link').first();
      const rowText = await firstLink.textContent().catch(() => '');
      recordingNumber = rowText?.match(/\b\d{11}\b/)?.[0];
      await firstLink.click().catch(() => null);
      await page.waitForTimeout(1500);

      if (recordingNumber) {
        await page.waitForSelector(`a[href*="${recordingNumber}"]`, { timeout: 5000 }).catch(() => null);
      }
    }

    const modalHrefs = recordingNumber
      ? await page.locator(`a[href*="${recordingNumber}"]`).evaluateAll((links) =>
        links
          .map((link) => (link as HTMLAnchorElement).href)
          .filter((href) => Boolean(href)),
      ).catch(() => [])
      : [];
    const networkCandidates = discoverMaricopaArtifactCandidates(requests, recordingNumber);
    const modalCandidates = recordingNumber ? buildModalCandidates(recordingNumber, modalHrefs) : [];
    const candidates = filterValidMaricopaArtifactCandidates(
      [...modalCandidates, ...networkCandidates].filter((candidate, index, all) =>
        all.findIndex((other) => other.urlTemplate === candidate.urlTemplate) === index,
      ),
      recordingNumber,
    );

    if (!recordingNumber) {
      throw new Error('Could not determine a Maricopa recording number from the first result row.');
    }
    if (candidates.length === 0) {
      throw new Error(`No recording-specific Maricopa artifact candidates were captured for ${recordingNumber}.`);
    }

    await saveMaricopaArtifactCandidates(candidates);

    console.log(
      JSON.stringify(
        {
          transport_mode: handle.mode,
          session_captured_at: session.captured_at,
          final_url: page.url(),
          row_count: rows,
          recording_number: recordingNumber ?? null,
          modal_candidate_count: modalCandidates.length,
          network_candidate_count: networkCandidates.length,
          candidate_count: candidates.length,
          candidates,
          modal_hrefs: modalHrefs,
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
