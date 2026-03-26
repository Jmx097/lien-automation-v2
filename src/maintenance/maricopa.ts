import {
  createIsolatedBrowserContext,
  type BrowserTransportMode,
} from '../browser/transport';
import {
  discoverMaricopaArtifactCandidates,
  filterValidMaricopaArtifactCandidates,
  getMaricopaPersistedStateReadiness,
  loadMaricopaSessionState,
  saveMaricopaArtifactCandidates,
  saveMaricopaSessionState,
  type MaricopaArtifactCandidate,
  type MaricopaPersistedStateReadiness,
  type MaricopaCapturedRequest,
} from '../scraper/maricopa_artifacts';

const RESULTS_URL =
  'https://recorder.maricopa.gov/recording/document-search-results.html?lastNames=&firstNames=&middleNameIs=&documentTypeSelector=code&documentCode=FL&beginDate=2026-01-01&endDate=2026-02-13';

type MaricopaMaintenanceOperation = 'session_refresh' | 'artifact_discovery' | 'self_heal';

export interface MaricopaMaintenanceResult {
  operation: MaricopaMaintenanceOperation;
  ok: boolean;
  detail: string;
  blocking_reason?: string;
  transport_mode?: BrowserTransportMode;
  final_url?: string;
  row_count?: number;
  session_captured_at?: string;
  artifact_candidate_count: number;
  modal_candidate_count?: number;
  network_candidate_count?: number;
  recording_number?: string;
  refresh_required: boolean;
  refresh_reason?: MaricopaPersistedStateReadiness['refreshReason'];
  readiness: MaricopaPersistedStateReadiness;
  attempted_refresh: boolean;
  attempted_discovery: boolean;
}

interface MaricopaMaintenanceOptions {
  transportModeOverride?: BrowserTransportMode;
}

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

async function safeReadiness(): Promise<MaricopaPersistedStateReadiness> {
  try {
    return await getMaricopaPersistedStateReadiness();
  } catch (err: unknown) {
    const detail = err instanceof Error ? err.message : String(err);
    return {
      artifactRetrievalEnabled: true,
      sessionPresent: false,
      sessionFresh: false,
      sessionMaxAgeMinutes: Number(process.env.MARICOPA_SESSION_MAX_AGE_MINUTES ?? '720'),
      artifactCandidatesPresent: false,
      artifactCandidatesFresh: false,
      artifactCandidateCount: 0,
      artifactCandidateMaxAgeMinutes: Number(process.env.MARICOPA_ARTIFACT_CANDIDATE_MAX_AGE_MINUTES ?? '1440'),
      refreshRequired: true,
      refreshReason: 'session_missing_or_stale',
      detail,
    };
  }
}

function buildResult(
  operation: MaricopaMaintenanceOperation,
  readiness: MaricopaPersistedStateReadiness,
  extras: Partial<Omit<MaricopaMaintenanceResult, 'operation' | 'ok' | 'detail' | 'artifact_candidate_count' | 'refresh_required' | 'refresh_reason' | 'readiness'>> = {},
): MaricopaMaintenanceResult {
  return {
    operation,
    ok: !readiness.refreshRequired,
    detail: readiness.detail,
    artifact_candidate_count: readiness.artifactCandidateCount,
    refresh_required: readiness.refreshRequired,
    refresh_reason: readiness.refreshReason,
    readiness,
    attempted_refresh: extras.attempted_refresh ?? false,
    attempted_discovery: extras.attempted_discovery ?? false,
    ...extras,
  };
}

export async function refreshMaricopaSessionState(
  options: MaricopaMaintenanceOptions = {},
): Promise<MaricopaMaintenanceResult> {
  let transportMode: BrowserTransportMode | undefined;
  let finalUrl: string | undefined;
  let rowCount = 0;
  let sessionCapturedAt: string | undefined;

  try {
    const handle = await createIsolatedBrowserContext({
      site: 'maricopa_recorder',
      transportModeOverride: options.transportModeOverride,
    });
    transportMode = handle.mode;

    try {
      const page = await handle.context.newPage();
      await page.goto(RESULTS_URL, { waitUntil: 'domcontentloaded' });
      await page.waitForTimeout(2000);

      rowCount = await page.locator('table tbody tr').count().catch(() => 0);
      if (rowCount === 0) {
        await page.waitForTimeout(30000);
        rowCount = await page.locator('table tbody tr').count().catch(() => 0);
      }

      finalUrl = page.url();
      const storageState = await handle.context.storageState();
      const saved = await saveMaricopaSessionState(storageState, handle.mode, finalUrl);
      sessionCapturedAt = saved.captured_at;
    } finally {
      await handle.close();
    }

    const readiness = await safeReadiness();
    return buildResult('session_refresh', readiness, {
      transport_mode: transportMode,
      final_url: finalUrl,
      row_count: rowCount,
      session_captured_at: sessionCapturedAt,
      attempted_refresh: true,
      blocking_reason: readiness.refreshRequired ? readiness.detail : undefined,
    });
  } catch (err: unknown) {
    const readiness = await safeReadiness();
    return buildResult('session_refresh', readiness, {
      transport_mode: transportMode,
      final_url: finalUrl,
      row_count: rowCount,
      session_captured_at: sessionCapturedAt,
      attempted_refresh: true,
      blocking_reason: err instanceof Error ? err.message : String(err),
    });
  }
}

export async function discoverMaricopaArtifacts(
  options: MaricopaMaintenanceOptions = {},
): Promise<MaricopaMaintenanceResult> {
  let transportMode: BrowserTransportMode | undefined;
  let finalUrl: string | undefined;
  let rowCount = 0;
  let recordingNumber: string | undefined;
  let modalCandidateCount = 0;
  let networkCandidateCount = 0;

  try {
    const session = await loadMaricopaSessionState();
    if (!session) {
      const readiness = await safeReadiness();
      return buildResult('artifact_discovery', readiness, {
        attempted_discovery: true,
        blocking_reason: 'No persisted Maricopa session state found. Run session refresh first.',
      });
    }

    const handle = await createIsolatedBrowserContext({
      site: 'maricopa_recorder',
      transportModeOverride: options.transportModeOverride,
      contextOptions: { storageState: session.storage_state_path },
    });
    transportMode = handle.mode;
    const requests: MaricopaCapturedRequest[] = [];

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

      rowCount = await page.locator('table tbody tr').count().catch(() => 0);
      if (rowCount > 0) {
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

      const modalCandidates = recordingNumber ? buildModalCandidates(recordingNumber, modalHrefs) : [];
      const networkCandidates = discoverMaricopaArtifactCandidates(requests, recordingNumber);
      modalCandidateCount = modalCandidates.length;
      networkCandidateCount = networkCandidates.length;

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
      finalUrl = page.url();
    } finally {
      await handle.close();
    }

    const readiness = await safeReadiness();
    return buildResult('artifact_discovery', readiness, {
      transport_mode: transportMode,
      final_url: finalUrl,
      row_count: rowCount,
      recording_number: recordingNumber,
      modal_candidate_count: modalCandidateCount,
      network_candidate_count: networkCandidateCount,
      attempted_discovery: true,
      blocking_reason: readiness.refreshRequired ? readiness.detail : undefined,
    });
  } catch (err: unknown) {
    const readiness = await safeReadiness();
    return buildResult('artifact_discovery', readiness, {
      transport_mode: transportMode,
      final_url: finalUrl,
      row_count: rowCount,
      recording_number: recordingNumber,
      modal_candidate_count: modalCandidateCount,
      network_candidate_count: networkCandidateCount,
      attempted_discovery: true,
      blocking_reason: err instanceof Error ? err.message : String(err),
    });
  }
}

export async function healMaricopaScheduledRunReadiness(): Promise<MaricopaMaintenanceResult> {
  const initialReadiness = await safeReadiness();
  if (!initialReadiness.refreshRequired) {
    return buildResult('self_heal', initialReadiness);
  }

  let attemptedRefresh = false;
  let attemptedDiscovery = false;
  let latestReadiness = initialReadiness;
  let transportMode: BrowserTransportMode | undefined;
  let finalUrl: string | undefined;
  let rowCount = 0;
  let sessionCapturedAt: string | undefined;
  let recordingNumber: string | undefined;
  let modalCandidateCount = 0;
  let networkCandidateCount = 0;
  let blockingReason: string | undefined;

  if (latestReadiness.refreshReason === 'session_missing_or_stale') {
    const refreshed = await refreshMaricopaSessionState();
    attemptedRefresh = true;
    transportMode = refreshed.transport_mode ?? transportMode;
    finalUrl = refreshed.final_url ?? finalUrl;
    rowCount = refreshed.row_count ?? rowCount;
    sessionCapturedAt = refreshed.session_captured_at ?? sessionCapturedAt;
    latestReadiness = refreshed.readiness;
    blockingReason = refreshed.blocking_reason;
  }

  if (
    latestReadiness.refreshRequired &&
    (latestReadiness.refreshReason === 'artifact_candidates_missing' || latestReadiness.refreshReason === 'artifact_candidates_stale')
  ) {
    const discovered = await discoverMaricopaArtifacts();
    attemptedDiscovery = true;
    transportMode = discovered.transport_mode ?? transportMode;
    finalUrl = discovered.final_url ?? finalUrl;
    rowCount = discovered.row_count ?? rowCount;
    recordingNumber = discovered.recording_number ?? recordingNumber;
    modalCandidateCount = discovered.modal_candidate_count ?? modalCandidateCount;
    networkCandidateCount = discovered.network_candidate_count ?? networkCandidateCount;
    latestReadiness = discovered.readiness;
    blockingReason = discovered.blocking_reason ?? blockingReason;
  }

  return buildResult('self_heal', latestReadiness, {
    transport_mode: transportMode,
    final_url: finalUrl,
    row_count: rowCount,
    session_captured_at: sessionCapturedAt,
    recording_number: recordingNumber,
    modal_candidate_count: modalCandidateCount,
    network_candidate_count: networkCandidateCount,
    attempted_refresh: attemptedRefresh,
    attempted_discovery: attemptedDiscovery,
    blocking_reason: latestReadiness.refreshRequired ? (blockingReason ?? latestReadiness.detail) : undefined,
  });
}
