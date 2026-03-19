import { scrapeCASOS_Enhanced } from '../scraper/ca_sos_enhanced';
import {
  fetchLatestMaricopaSearchableDate,
  scrapeMaricopaRecorder,
} from '../scraper/maricopa_recorder';
import {
  getMaricopaPersistedStateReadiness,
  isFreshMaricopaSession,
  loadMaricopaArtifactCandidates,
  loadMaricopaSessionState,
  type MaricopaPersistedStateReadiness,
} from '../scraper/maricopa_artifacts';
import { scrapeNYCAcris, validateNYCAcrisSelectors } from '../scraper/nyc_acris';
import { pushRunToNewSheetTab, syncMasterSheetTab } from '../sheets/push';
import type { SupportedSite } from '../sites';
import type { LienRecord } from '../types';

export interface RecentCanaryWindow {
  date_start: string;
  date_end: string;
  max_records: number;
}

export interface SiteCanarySummary {
  site: SupportedSite;
  date_start: string;
  date_end: string;
  max_records: number;
  records_scraped: number;
  complete_records: number;
  incomplete_records: number;
  rows_uploaded: number;
  source_tab_title: string;
  master_tab_title: string;
  review_tab_title: string;
  quarantined_row_count: number;
  new_master_row_count: number;
}

export interface NYCAcrisValidationSummary {
  transport_mode: string;
  result_pages_visited: number;
  validated_docs: number;
  attempted_docs: number;
  warnings: string[];
  failures: string[];
}

export interface MaricopaProofReadinessSummary {
  artifact_retrieval_enabled: boolean;
  session_present: boolean;
  session_fresh: boolean;
  session_captured_at?: string;
  discovery_candidate_count: number;
  refresh_required: boolean;
  refresh_reason?: string;
  detail: string;
  latest_searchable_date: string;
}

export interface LiveProofSiteResult {
  site: SupportedSite;
  status: 'ok' | 'error';
  validation?: NYCAcrisValidationSummary;
  readiness?: MaricopaProofReadinessSummary;
  canary?: SiteCanarySummary;
  blocking_reason?: string;
}

export interface AllSitesLiveProofSummary {
  run_started_at: string;
  run_finished_at: string;
  success_count: number;
  error_count: number;
  results: LiveProofSiteResult[];
}

interface SharedCanaryRunnerConfig {
  site: SupportedSite;
  label: string;
  syncPrefixes: string[];
  maxRecordsEnvVar: string;
  defaultMaxRecords: number;
  scrape: (window: RecentCanaryWindow) => Promise<LienRecord[]>;
}

interface SharedCanaryDeps {
  pushRun: typeof pushRunToNewSheetTab;
  syncMaster: typeof syncMasterSheetTab;
  now: () => Date;
  scrape: SharedCanaryRunnerConfig['scrape'];
}

interface MaricopaReadinessDeps {
  getReadiness: typeof getMaricopaPersistedStateReadiness;
  loadSession: typeof loadMaricopaSessionState;
  loadCandidates: typeof loadMaricopaArtifactCandidates;
  fetchLatestDate: typeof fetchLatestMaricopaSearchableDate;
}

const DEFAULT_CANARY_DAYS = 7;

function formatDate(input: Date): string {
  const mm = String(input.getMonth() + 1).padStart(2, '0');
  const dd = String(input.getDate()).padStart(2, '0');
  const yyyy = input.getFullYear();
  return `${mm}/${dd}/${yyyy}`;
}

function countIncompleteRecords(rows: LienRecord[]): number {
  return rows.filter((row) => Boolean(row.error)).length;
}

export function resolveRecentCanaryWindow(maxRecordsEnvVar: string, defaultMaxRecords: number, now = new Date()): RecentCanaryWindow {
  const end = new Date(now);
  const start = new Date(now);
  start.setDate(start.getDate() - DEFAULT_CANARY_DAYS);

  return {
    date_start: process.env.JOB_DATE_START ?? formatDate(start),
    date_end: process.env.JOB_DATE_END ?? formatDate(end),
    max_records: Number(process.env.JOB_MAX_RECORDS ?? process.env[maxRecordsEnvVar] ?? String(defaultMaxRecords)),
  };
}

async function runSharedCanary(config: SharedCanaryRunnerConfig, deps?: Partial<SharedCanaryDeps>): Promise<SiteCanarySummary> {
  const resolvedDeps: SharedCanaryDeps = {
    pushRun: deps?.pushRun ?? pushRunToNewSheetTab,
    syncMaster: deps?.syncMaster ?? syncMasterSheetTab,
    now: deps?.now ?? (() => new Date()),
    scrape: deps?.scrape ?? config.scrape,
  };
  const window = resolveRecentCanaryWindow(config.maxRecordsEnvVar, config.defaultMaxRecords, resolvedDeps.now());
  const rows = await resolvedDeps.scrape(window);
  const upload = await resolvedDeps.pushRun(rows, {
    label: config.label,
    date_start: window.date_start,
    date_end: window.date_end,
    run_started_at: resolvedDeps.now(),
  });
  const masterSync = await resolvedDeps.syncMaster({
    includePrefixes: config.syncPrefixes,
  });
  const incompleteRecords = countIncompleteRecords(rows);

  return {
    site: config.site,
    date_start: window.date_start,
    date_end: window.date_end,
    max_records: window.max_records,
    records_scraped: rows.length,
    complete_records: rows.length - incompleteRecords,
    incomplete_records: incompleteRecords,
    rows_uploaded: upload.uploaded,
    source_tab_title: upload.tab_title,
    master_tab_title: masterSync.tab_title,
    review_tab_title: masterSync.review_tab_title,
    quarantined_row_count: masterSync.quarantined_row_count,
    new_master_row_count: masterSync.new_master_row_count,
  };
}

export async function runCASOSCanary(deps?: Partial<SharedCanaryDeps>): Promise<SiteCanarySummary> {
  return runSharedCanary(
    {
      site: 'ca_sos',
      label: 'ca_sos_canary',
      syncPrefixes: ['Scheduled_', 'ca_sos_canary_'],
      maxRecordsEnvVar: 'SCHEDULE_MAX_RECORDS_FLOOR',
      defaultMaxRecords: 5,
      scrape: ({ date_start, date_end, max_records }) =>
        scrapeCASOS_Enhanced({
          date_start,
          date_end,
          max_records,
        }),
    },
    deps,
  );
}

export async function runNYCAcrisCanary(deps?: Partial<SharedCanaryDeps>): Promise<SiteCanarySummary> {
  return runSharedCanary(
    {
      site: 'nyc_acris',
      label: 'nyc_acris_canary',
      syncPrefixes: ['Scheduled_', 'nyc_acris_canary_'],
      maxRecordsEnvVar: 'ACRIS_INITIAL_MAX_RECORDS',
      defaultMaxRecords: 5,
      scrape: ({ date_start, date_end, max_records }) =>
        scrapeNYCAcris({
          date_start,
          date_end,
          max_records,
        }),
    },
    deps,
  );
}

export async function validateNYCAcrisLive(): Promise<NYCAcrisValidationSummary> {
  const maxDocuments = Number(process.env.ACRIS_VALIDATION_MAX_DOCS ?? '2');
  const manifest = await validateNYCAcrisSelectors({
    max_documents: maxDocuments,
    transportPolicyPurpose: 'diagnostic',
  });

  return {
    transport_mode: manifest.transportMode,
    result_pages_visited: manifest.resultPagesVisited,
    validated_docs: manifest.documents.length,
    attempted_docs: manifest.attemptedDocs ?? manifest.documents.length,
    warnings: manifest.warnings,
    failures: manifest.failures,
  };
}

export async function getMaricopaProofReadiness(
  deps?: Partial<MaricopaReadinessDeps>,
): Promise<MaricopaProofReadinessSummary> {
  const resolvedDeps: MaricopaReadinessDeps = {
    getReadiness: deps?.getReadiness ?? getMaricopaPersistedStateReadiness,
    loadSession: deps?.loadSession ?? loadMaricopaSessionState,
    loadCandidates: deps?.loadCandidates ?? loadMaricopaArtifactCandidates,
    fetchLatestDate: deps?.fetchLatestDate ?? fetchLatestMaricopaSearchableDate,
  };

  const [readiness, session, candidates, latestSearchableDate] = await Promise.all([
    resolvedDeps.getReadiness(),
    resolvedDeps.loadSession(),
    resolvedDeps.loadCandidates(),
    resolvedDeps.fetchLatestDate(),
  ]);

  return {
    artifact_retrieval_enabled: readiness.artifactRetrievalEnabled,
    session_present: readiness.sessionPresent,
    session_fresh: session?.captured_at ? isFreshMaricopaSession(session.captured_at) : readiness.sessionFresh,
    session_captured_at: readiness.sessionCapturedAt ?? session?.captured_at,
    discovery_candidate_count: readiness.artifactCandidateCount,
    refresh_required: readiness.refreshRequired,
    refresh_reason: readiness.refreshReason,
    detail: readiness.detail,
    latest_searchable_date: latestSearchableDate,
  };
}

function assertMaricopaProofReadiness(readiness: MaricopaProofReadinessSummary): void {
  if (!readiness.artifact_retrieval_enabled) {
    throw new Error(
      'Maricopa live proof requires MARICOPA_ENABLE_ARTIFACT_RETRIEVAL to stay enabled so rows can be fully verified.',
    );
  }
  if (readiness.refresh_required) {
    throw new Error(readiness.detail);
  }
}

export async function runMaricopaCanary(
  deps?: Partial<SharedCanaryDeps>,
  readinessDeps?: Partial<MaricopaReadinessDeps>,
): Promise<SiteCanarySummary> {
  const readiness = await getMaricopaProofReadiness(readinessDeps);
  assertMaricopaProofReadiness(readiness);

  return runSharedCanary(
    {
      site: 'maricopa_recorder',
      label: 'maricopa_recorder_canary',
      syncPrefixes: ['Scheduled_', 'maricopa_recorder_canary_'],
      maxRecordsEnvVar: 'MARICOPA_MAX_RECORDS',
      defaultMaxRecords: 5,
      scrape: ({ date_start, date_end, max_records }) =>
        scrapeMaricopaRecorder({
          date_start,
          date_end,
          max_records,
        }),
    },
    deps,
  );
}

export async function runAllSitesLiveProof(): Promise<AllSitesLiveProofSummary> {
  const startedAt = new Date().toISOString();
  const results: LiveProofSiteResult[] = [];

  try {
    const canary = await runCASOSCanary();
    results.push({ site: 'ca_sos', status: 'ok', canary });
  } catch (error: unknown) {
    results.push({
      site: 'ca_sos',
      status: 'error',
      blocking_reason: error instanceof Error ? error.message : String(error),
    });
  }

  try {
    const validation = await validateNYCAcrisLive();
    const canary = await runNYCAcrisCanary();
    results.push({ site: 'nyc_acris', status: 'ok', validation, canary });
  } catch (error: unknown) {
    results.push({
      site: 'nyc_acris',
      status: 'error',
      blocking_reason: error instanceof Error ? error.message : String(error),
    });
  }

  try {
    const readiness = await getMaricopaProofReadiness();
    assertMaricopaProofReadiness(readiness);
    const canary = await runMaricopaCanary();
    results.push({ site: 'maricopa_recorder', status: 'ok', readiness, canary });
  } catch (error: unknown) {
    const fallbackReadiness = await getMaricopaProofReadiness().catch(() => null);
    results.push({
      site: 'maricopa_recorder',
      status: 'error',
      readiness: fallbackReadiness ?? undefined,
      blocking_reason: error instanceof Error ? error.message : String(error),
    });
  }

  const successCount = results.filter((result) => result.status === 'ok').length;
  return {
    run_started_at: startedAt,
    run_finished_at: new Date().toISOString(),
    success_count: successCount,
    error_count: results.length - successCount,
    results,
  };
}
