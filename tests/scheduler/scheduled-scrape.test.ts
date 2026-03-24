import { beforeEach, describe, expect, it, vi } from 'vitest';
import fs from 'fs';
import path from 'path';

const mockScraper = vi.fn();
const mockProbeCASOSResultCount = vi.fn();
const mockPushToSheetsForTab = vi.fn();
const mockSyncMasterSheetTab = vi.fn();
const mockLog = vi.fn();
const mockFetch = vi.fn();
const mockProbeMaricopaRecorderConnectivity = vi.fn();
const mockGetMaricopaPersistedStateReadiness = vi.fn();
const mockProbeNYCAcrisConnectivity = vi.fn();
const mockDebugNYCAcrisBootstrap = vi.fn();

const runs = new Map<string, any>();
let controlState: any = null;
const connectivityState = new Map<string, any>();
const anomalyAlerts = new Map<string, any>();
const scheduledCacheDir = path.join(process.cwd(), 'out', 'acris', 'scheduled-cache');

vi.mock('../../src/scraper/index', () => ({
  scrapers: {
    ca_sos: mockScraper,
    maricopa_recorder: mockScraper,
    nyc_acris: mockScraper,
  },
}));

vi.mock('../../src/scraper/ca_sos_enhanced', () => ({
  probeCASOSResultCount: mockProbeCASOSResultCount,
}));

vi.mock('../../src/scraper/maricopa_recorder', () => ({
  probeMaricopaRecorderConnectivity: mockProbeMaricopaRecorderConnectivity,
}));

vi.mock('../../src/scraper/nyc_acris', () => ({
  probeNYCAcrisConnectivity: mockProbeNYCAcrisConnectivity,
  debugNYCAcrisBootstrap: mockDebugNYCAcrisBootstrap,
}));

vi.mock('../../src/scraper/maricopa_artifacts', () => ({
  getMaricopaPersistedStateReadiness: mockGetMaricopaPersistedStateReadiness,
}));

vi.mock('../../src/sheets/push', () => ({
  formatRunTabName: vi.fn(() => 'tab-name'),
  pushToSheetsForTab: mockPushToSheetsForTab,
  syncMasterSheetTab: mockSyncMasterSheetTab,
}));

vi.mock('../../src/utils/logger', () => ({
  log: mockLog,
}));

vi.mock('../../src/scheduler/store', () => {
  class ScheduledRunStore {
    insertRun(run: any) {
      runs.set(run.idempotency_key, { ...run });
    }

    updateRun(run: any) {
      runs.set(run.idempotency_key, { ...run });
    }

    getByIdempotencyKey(idempotencyKey: string) {
      return runs.get(idempotencyKey) ?? null;
    }

    getMostRecentRun(site?: string) {
      const items = Array.from(runs.values()).filter((run: any) => !site || run.site === site);
      return items[items.length - 1] ?? null;
    }

    getSuccessfulRunByIdempotencyKey(idempotencyKey: string) {
      const run = runs.get(idempotencyKey);
      if (run?.status === 'success') return run;
      return null;
    }

    getRunHistory(limit = 50) {
      return Array.from(runs.values()).slice(0, limit);
    }

    getRecentSuccessfulRuns(site: string, limit = 4) {
      return Array.from(runs.values()).filter((r: any) => r.status === 'success' && r.site === site).slice(0, limit);
    }

    upsertControlState(site: string, effectiveMaxRecords: number) {
      controlState = { site, effective_max_records: effectiveMaxRecords };
    }

    getControlState(site: string) {
      return controlState?.site === site ? controlState : null;
    }

    upsertConnectivityState(state: any) {
      connectivityState.set(state.site, { ...state });
    }

    getConnectivityState(site: string) {
      return connectivityState.get(site) ?? null;
    }

    listConnectivityStates() {
      return Array.from(connectivityState.values());
    }

    insertMissedAlert() {}

    getMissedAlertByKey() {
      return null;
    }

    insertQualityAnomalyAlert(alert: any) {
      anomalyAlerts.set(`${alert.idempotency_key}:quality_anomaly`, { ...alert });
    }

    getLatestQualityAnomalyAlert(site: string) {
      const alerts = Array.from(anomalyAlerts.values()).filter((alert: any) => alert.site === site);
      return alerts[alerts.length - 1] ?? null;
    }
  }

  return {
    ScheduledRunStore,
  };
});

describe('runScheduledScrape', () => {
  beforeEach(() => {
    vi.resetModules();
    runs.clear();
    controlState = null;
    connectivityState.clear();
    anomalyAlerts.clear();
    vi.clearAllMocks();
    mockProbeCASOSResultCount.mockReset();
    mockProbeMaricopaRecorderConnectivity.mockReset();
    mockProbeNYCAcrisConnectivity.mockReset();
    mockDebugNYCAcrisBootstrap.mockReset();
    mockGetMaricopaPersistedStateReadiness.mockReset();
    mockGetMaricopaPersistedStateReadiness.mockResolvedValue({
      artifactRetrievalEnabled: false,
      sessionPresent: false,
      sessionFresh: false,
      artifactCandidatesPresent: false,
      artifactCandidateCount: 0,
      refreshRequired: false,
      refreshReason: 'artifact_retrieval_disabled',
      detail: 'Maricopa artifact retrieval is disabled by configuration.',
    });
    mockFetch.mockReset();
    vi.stubGlobal('fetch', mockFetch);
    fs.rmSync(scheduledCacheDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
    process.env.SCHEDULE_RUN_MAX_ATTEMPTS = '3';
    process.env.SCHEDULE_RUN_BASE_DELAY_MS = '0';
    process.env.SCHEDULE_RUN_MAX_DELAY_MS = '0';
    delete process.env.SCHEDULE_ALERT_WEBHOOK_URL;
    delete process.env.ENABLE_SCHEDULE_FAILURE_INJECTION;
  });

  async function seedSuccessfulBaseline(site: 'ca_sos' | 'nyc_acris', metrics: Array<{
    id: string;
    records_scraped: number;
    amount_coverage_pct: number;
    ocr_success_pct: number;
    row_fail_pct: number;
    partial?: number;
    deadline_hit?: number;
  }>) {
    const { ScheduledRunStore } = await import('../../src/scheduler/store');
    const store = new ScheduledRunStore();
    for (const [index, metric] of metrics.entries()) {
      const day = String(index + 1).padStart(2, '0');
      await store.insertRun({
        id: metric.id,
        site,
        idempotency_key: `${site}:${metric.id}`,
        slot_time: `${site}:${metric.id}`,
        trigger_source: 'manual',
        started_at: new Date(`2026-03-${day}T12:00:00.000Z`).toISOString(),
        finished_at: new Date(`2026-03-${day}T12:05:00.000Z`).toISOString(),
        status: 'success',
        records_scraped: metric.records_scraped,
        records_skipped: 0,
        rows_uploaded: metric.records_scraped,
        amount_found_count: Math.round((metric.records_scraped * metric.amount_coverage_pct) / 100),
        amount_missing_count: Math.max(metric.records_scraped - Math.round((metric.records_scraped * metric.amount_coverage_pct) / 100), 0),
        amount_coverage_pct: metric.amount_coverage_pct,
        ocr_success_pct: metric.ocr_success_pct,
        row_fail_pct: metric.row_fail_pct,
        deadline_hit: metric.deadline_hit ?? 0,
        effective_max_records: metric.records_scraped,
        partial: metric.partial ?? 0,
      });
    }
  }

  it('uploads scraped records to sheets and persists quality metrics', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(2);
    const records = [
      { filing_number: '1', amount: '100', amount_reason: 'ok' },
      { filing_number: '2', amount: '200', amount_reason: 'ok' },
    ];
    Object.assign(records, {
      quality_summary: {
        requested_date_start: '03/06/2026',
        requested_date_end: '03/13/2026',
        discovered_count: 2,
        returned_count: 2,
        quarantined_count: 1,
        partial_run: true,
        partial_reason: 'quarantined_failed_rows',
        skipped_existing_count: 3,
      },
    });
    mockScraper.mockResolvedValueOnce(records);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 2, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: 'Master',
      row_count: 2,
      source_tabs: 1,
      target_spreadsheet_id: 'source-sheet',
      fallback_used: true,
    });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: '2026-03-03:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(1);
    expect(result.status).toBe('success');
    expect(result.rows_uploaded).toBe(2);
    expect(result.records_scraped).toBe(2);
    expect(result.records_skipped).toBe(3);
    expect(result.discovered_count).toBe(2);
    expect(result.returned_count).toBe(2);
    expect(result.partial_reason).toBe('quarantined_failed_rows');
    expect(result.quarantined_row_count).toBe(1);
    expect(result.amount_found_count).toBe(2);
    expect(result.amount_coverage_pct).toBeGreaterThan(90);
    expect(mockSyncMasterSheetTab).toHaveBeenCalledTimes(1);

    const { getRunHistory } = await import('../../src/scheduler');
    const history = await getRunHistory(1);
    expect(history[0].confidence?.status).toBe('low');
    expect(history[0].confidence?.reasons).toContain('master_publish_fallback_active');
    expect(history[0].requested_date_start).toBe('03/06/2026');
    expect(history[0].requested_date_end).toBe('03/13/2026');
    expect(history[0].partial_reason).toBe('quarantined_failed_rows');
  });

  it('supports NYC bootstrap-only debug runs without writing sheets', async () => {
    mockDebugNYCAcrisBootstrap.mockResolvedValueOnce({
      requestedTransportMode: 'legacy-sbr-cdp',
      transportPolicyPurpose: 'diagnostic',
      transportMode: 'legacy-sbr-cdp',
      ok: false,
      detail: 'dead_bootstrap_page about:blank before first navigation',
      failureClass: 'transport_or_bootstrap',
      recoveryAction: 'retry_fresh_context',
      bootstrapStrategy: 'direct_document_type',
      diagnostic: {
        finalUrl: 'about:blank',
      },
      bootstrapTrace: ['bootstrap_page_created url=about:blank'],
      bootstrapLifecycle: [{ step: 'bootstrap_before_new_page', at: new Date().toISOString() }],
      transportDiagnostics: [{ stage: 'create_browser_context', status: 'succeeded' }],
      failures: ['dead_bootstrap_page about:blank before first navigation'],
      warnings: ['bootstrap_recovery strategy=direct_document_type recovery=retry_fresh_context'],
    });

    const { runScheduledScrape } = await import('../../src/scheduler');
    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-19:afternoon:debug',
      slot: 'afternoon',
      triggerSource: 'manual',
      debugBootstrapOnly: true,
      transportModeOverride: 'legacy-sbr-cdp',
    });

    expect(mockDebugNYCAcrisBootstrap).toHaveBeenCalledWith({
      transportPolicyPurpose: 'diagnostic',
      transportModeOverride: 'legacy-sbr-cdp',
    });
    expect(mockPushToSheetsForTab).not.toHaveBeenCalled();
    expect(result.status).toBe('error');
    expect(result.partial_reason).toBe('debug_bootstrap_only');
    expect(result.debug_artifact).toEqual(
      expect.objectContaining({
        requestedTransportMode: 'legacy-sbr-cdp',
        transportMode: 'legacy-sbr-cdp',
        ok: false,
      }),
    );
    expect(result.error).toContain('"mode":"nyc_bootstrap_debug"');
    expect(connectivityState.size).toBe(0);
  });

  it('keeps NYC connectivity state unchanged for successful bootstrap-only debug runs', async () => {
    connectivityState.set('nyc_acris', {
      site: 'nyc_acris',
      status: 'blocked',
      consecutive_failures: 2,
      blocked_until: '2026-03-20T00:00:00.000Z',
      last_failure_reason: 'transport_or_bootstrap',
      updated_at: '2026-03-19T00:00:00.000Z',
    });

    mockDebugNYCAcrisBootstrap.mockResolvedValueOnce({
      requestedTransportMode: 'legacy-sbr-cdp',
      transportPolicyPurpose: 'diagnostic',
      transportMode: 'legacy-sbr-cdp',
      ok: true,
      detail: 'loaded NYC bootstrap session',
      recoveryAction: 'retry_fresh_context',
      bootstrapStrategy: 'direct_document_type',
      diagnostic: {
        finalUrl: 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentType',
      },
      bootstrapTrace: ['bootstrap_page_created url=about:blank'],
      bootstrapLifecycle: [{ step: 'bootstrap_before_new_page', at: new Date().toISOString() }],
      transportDiagnostics: [{ stage: 'create_browser_context', status: 'succeeded' }],
      failures: [],
      warnings: [],
    });

    const priorState = connectivityState.get('nyc_acris');
    const { runScheduledScrape } = await import('../../src/scheduler');
    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-19:afternoon:debug-success',
      slot: 'afternoon',
      triggerSource: 'manual',
      debugBootstrapOnly: true,
      transportModeOverride: 'legacy-sbr-cdp',
    });

    expect(result.status).toBe('success');
    expect(connectivityState.get('nyc_acris')).toEqual(priorState);
  });

  it('sends a new leads notification when Master gains new rows', async () => {
    process.env.LEAD_ALERT_WEBHOOK_URL = 'https://example.invalid/lead-email';
    process.env.LEAD_ALERT_EMAIL_TO = 'antigravity1@timberlinetax.com';
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => 'ok',
    });
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: 'Master',
      row_count: 1,
      source_tabs: 1,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 0,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 1,
      purged_review_row_count: 0,
    });

    const { runScheduledScrape } = await import('../../src/scheduler');

    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-12:morning:new-leads-email',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockFetch).toHaveBeenCalledWith(
      'https://example.invalid/lead-email',
      expect.objectContaining({
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      })
    );
    const payload = JSON.parse(String(mockFetch.mock.calls[0][1]?.body));
    expect(payload.to).toBe('antigravity1@timberlinetax.com');
    expect(payload.subject).toBe('New leads!');
    expect(payload.html).toContain('New leads!');
    expect(payload.new_master_row_count).toBe(1);
  });

  it('reports a high-confidence run when source and destination publishing succeed without fallback', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: 'Master',
      row_count: 1,
      source_tabs: 1,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 0,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 1,
      purged_review_row_count: 0,
    });

    const { runScheduledScrape, getRunHistory } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-12:morning:high-confidence',
      slot: 'morning',
      triggerSource: 'manual',
    });

    const history = await getRunHistory(1);
    expect(history[0].source_tab_title).toBe('tab-name');
    expect(history[0].master_tab_title).toBe('Master');
    expect(['high', 'medium']).toContain(history[0].confidence?.status);
    expect(history[0].confidence?.evidence).toEqual(expect.objectContaining({
      source_publish_confirmed: true,
      master_sync_confirmed: true,
      source_tab_title_present: true,
      master_tab_title_present: true,
      uploaded_rows_match_scraped_rows: true,
    }));
    expect(history[0].confidence?.reasons).not.toEqual(expect.arrayContaining([
      'run_not_successful',
      'source_tab_missing',
      'row_upload_mismatch',
      'master_publish_fallback_active',
    ]));
  });

  it('does not downgrade confidence for retained prior review rows alone', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: 'Master',
      row_count: 1,
      source_tabs: 12,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 200,
      current_run_quarantined_row_count: 0,
      current_run_conflict_row_count: 0,
      retained_prior_review_row_count: 200,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 1,
      purged_review_row_count: 0,
      review_summary: {
        accepted_row_count: 1,
        quarantined_row_count: 200,
        purged_review_row_count: 0,
        review_reason_counts: { low_confidence: 200 },
        current_run_quarantined_row_count: 0,
        current_run_conflict_row_count: 0,
        retained_prior_review_row_count: 200,
      },
    });

    const { runScheduledScrape, getRunHistory } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-12:evening:retained-review-only',
      slot: 'evening',
      triggerSource: 'manual',
    });

    const history = await getRunHistory(1);
    expect(history[0].quarantined_row_count).toBe(0);
    expect(history[0].current_run_quarantined_row_count).toBe(0);
    expect(history[0].retained_prior_review_row_count).toBe(200);
    expect(history[0].confidence?.reasons).not.toContain('quarantine_exceeds_scraped_rows');
  });

  it('treats successful publish and sync telemetry as evidence even when tab titles are absent', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: undefined });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: undefined,
      row_count: 0,
      source_tabs: 1,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 1,
      current_run_quarantined_row_count: 1,
      current_run_conflict_row_count: 0,
      retained_prior_review_row_count: 0,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 0,
      purged_review_row_count: 0,
      review_summary: {
        accepted_row_count: 0,
        quarantined_row_count: 1,
        purged_review_row_count: 0,
        review_reason_counts: { low_confidence: 1 },
        current_run_quarantined_row_count: 1,
        current_run_conflict_row_count: 0,
        retained_prior_review_row_count: 0,
      },
    });

    const { runScheduledScrape, getRunHistory } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-12:afternoon:evidence-without-titles',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    const history = await getRunHistory(1);
    expect(history[0].confidence?.evidence).toEqual(expect.objectContaining({
      source_publish_confirmed: true,
      master_sync_confirmed: true,
      source_tab_title_present: false,
      master_tab_title_present: false,
      review_tab_title_present: true,
    }));
    expect(history[0].confidence?.reasons).not.toContain('source_tab_missing');
    expect(history[0].confidence?.reasons).not.toContain('master_tab_missing');
  });

  it('persists scheduled-run evidence for every supported site', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValue([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValue({ uploaded: 1, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValue({
      tab_title: 'Master',
      row_count: 1,
      source_tabs: 1,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 0,
      current_run_quarantined_row_count: 0,
      current_run_conflict_row_count: 0,
      retained_prior_review_row_count: 0,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 1,
      purged_review_row_count: 0,
      review_summary: {
        accepted_row_count: 1,
        quarantined_row_count: 0,
        purged_review_row_count: 0,
        review_reason_counts: {},
        current_run_quarantined_row_count: 0,
        current_run_conflict_row_count: 0,
        retained_prior_review_row_count: 0,
      },
    });

    const { runScheduledScrape, getRunHistory, getScheduleState } = await import('../../src/scheduler');
    for (const site of ['ca_sos', 'maricopa_recorder', 'nyc_acris'] as const) {
      await runScheduledScrape({
        site,
        idempotencyKey: `${site}:2026-03-12:morning:persisted-evidence`,
        slot: 'morning',
        triggerSource: 'manual',
      });
    }

    const history = await getRunHistory(10);
    const state = await getScheduleState();

    for (const site of ['ca_sos', 'maricopa_recorder', 'nyc_acris'] as const) {
      const persisted = history.find((run) => run.site === site);
      expect(persisted).toEqual(expect.objectContaining({
        source_tab_title: 'tab-name',
        master_tab_title: 'Master',
        review_tab_title: 'Review_Queue',
      }));
      expect(persisted?.confidence?.evidence).toEqual(expect.objectContaining({
        source_publish_confirmed: true,
        master_sync_confirmed: true,
      }));
      expect(state[site].recent_run_count).toBeGreaterThan(0);
      expect(state[site].latest_run_started_at).toBeTruthy();
      expect(state[site].latest_run_confidence_status).toBeTruthy();
    }
  });

  it('fails the run when sheet upload count mismatches', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 0, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: '2026-03-03:afternoon',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.error).toContain('sheet_upload_mismatch');
  });

  it('uses the probed CA result count as the scheduled max_records', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(31);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-03:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockProbeCASOSResultCount).toHaveBeenCalledTimes(1);
    expect(mockScraper).toHaveBeenCalledWith(expect.objectContaining({ max_records: 31 }));
  });

  it('short-circuits CA scheduled runs when the probe finds zero results', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(0);

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-10:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.records_scraped).toBe(0);
    expect(mockScraper).not.toHaveBeenCalled();
    expect(mockPushToSheetsForTab).not.toHaveBeenCalled();
  });

  it('falls back to a conservative CA scheduled cap when the probe fails', async () => {
    controlState = { site: 'ca_sos', effective_max_records: 55 };
    mockProbeCASOSResultCount.mockRejectedValueOnce(new Error('probe failed'));
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-11:morning',
      slot: 'morning',
      triggerSource: 'manual',
    });

    expect(mockScraper).toHaveBeenCalledWith(expect.objectContaining({ max_records: 10 }));
  });

  it('defers blocked nyc scheduled runs before scraping', async () => {
    connectivityState.set('nyc_acris', {
      site: 'nyc_acris',
      status: 'blocked',
      policy_block_count: 2,
      timeout_count: 0,
      empty_result_count: 0,
      consecutive_probe_successes: 0,
      next_probe_at: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
      last_failure_reason: 'Requested URL is restricted in accordance with robots.txt (brob)',
    });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-03:afternoon',
      slot: 'afternoon',
      triggerSource: 'external',
    });

    expect(result.status).toBe('deferred');
    expect(mockScraper).not.toHaveBeenCalled();
    expect(result.failure_class).toBe('policy_block');
  });

  it('defers blocked Maricopa scheduled runs before scraping', async () => {
    connectivityState.set('maricopa_recorder', {
      site: 'maricopa_recorder',
      status: 'blocked',
      policy_block_count: 2,
      timeout_count: 0,
      empty_result_count: 0,
      consecutive_probe_successes: 0,
      next_probe_at: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
      last_failure_reason: 'Maricopa session is stale (captured_at=2026-03-10). Run refresh:maricopa-session on the droplet.',
    });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'maricopa_recorder',
      idempotencyKey: 'maricopa_recorder:2026-03-03:afternoon',
      slot: 'afternoon',
      triggerSource: 'external',
    });

    expect(result.status).toBe('deferred');
    expect(mockScraper).not.toHaveBeenCalled();
    expect(result.failure_class).toBe('session_missing_or_stale');
  });

  it('logs compact NYC probe diagnostics when connectivity probing fails', async () => {
    connectivityState.set('nyc_acris', {
      site: 'nyc_acris',
      status: 'blocked',
      policy_block_count: 0,
      timeout_count: 3,
      empty_result_count: 0,
      consecutive_probe_successes: 0,
      next_probe_at: new Date(Date.now() - 1_000).toISOString(),
      last_failure_reason: 'prior bootstrap failure',
    });
    mockProbeNYCAcrisConnectivity.mockResolvedValueOnce({
      ok: false,
      detail: 'NYC probe_index_page page not ready: {"finalUrl":"about:blank","reason":"unexpected_url"}',
      transportMode: 'legacy-sbr-cdp',
      failureClass: 'transport_or_bootstrap',
      recoveryAction: 'retry_fresh_context',
      bootstrapStrategy: 'direct_document_type',
      diagnostic: {
        step: 'probe_index_page',
        attempt: 2,
        kind: 'index',
        expectedPath: '/DS/DocumentSearch/Index',
        finalUrl: 'about:blank',
        title: '',
        readyState: 'unavailable',
        htmlLength: 0,
        bodyTextLength: 0,
        hasToken: false,
        hasShellMarker: false,
        hasResultMarker: false,
        hasViewerIframe: false,
        ok: false,
        reason: 'unexpected_url',
      },
    });

    const { checkSiteConnectivity } = await import('../../src/scheduler');

    await checkSiteConnectivity();

    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'site_connectivity_probe_failure',
      site: 'nyc_acris',
      failure_class: 'transport_or_bootstrap',
      probe_recovery_action: 'retry_fresh_context',
      probe_bootstrap_strategy: 'direct_document_type',
      probe_step: 'probe_index_page',
      probe_attempt: 2,
      final_url: 'about:blank',
      ready_state: 'unavailable',
      has_shell_marker: false,
      has_result_marker: false,
      has_viewer_iframe: false,
    }));
  });

  it('reuses cached nyc rows when the previous failure was sheet export', async () => {
    const cacheDir = path.join(process.cwd(), 'out', 'acris', 'scheduled-cache');
    fs.mkdirSync(cacheDir, { recursive: true });
    fs.writeFileSync(
      path.join(cacheDir, 'nyc_acris_cached-sheet-retry_afternoon.json'),
      JSON.stringify([{ filing_number: '1', amount: '100', amount_reason: 'ok' }], null, 2),
      'utf8'
    );

    runs.set('nyc_acris:cached-sheet-retry:afternoon', {
      id: 'prior-run',
      site: 'nyc_acris',
      idempotency_key: 'nyc_acris:cached-sheet-retry:afternoon',
      status: 'error',
      failure_class: 'sheet_export',
    });
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:cached-sheet-retry:afternoon',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(mockScraper).not.toHaveBeenCalled();
    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(1);
  });

  it('retries transient scraper failures and succeeds within the run budget', async () => {
    mockScraper
      .mockRejectedValueOnce(new Error('viewer did not return to acris result page'))
      .mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:retry-success',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.retried).toBe(1);
    expect(result.attempt_count).toBe(2);
    expect(mockScraper).toHaveBeenCalledTimes(2);
  });

  it('retries transient sheet export failures by reusing cached nyc rows', async () => {
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab
      .mockRejectedValueOnce(new Error('googleapis sheets 503'))
      .mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:sheet-retry',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(result.retried).toBe(1);
    expect(result.attempt_count).toBe(2);
    expect(mockScraper).toHaveBeenCalledTimes(1);
    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(2);
  });

  it('does not retry non-retryable selector failures', async () => {
    mockScraper.mockRejectedValueOnce(new Error('No ACRIS rows found during live selector validation'));

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:selector-fail',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.failure_class).toBe('selector_or_empty_results');
    expect(result.retry_exhausted).toBe(0);
    expect(mockScraper).toHaveBeenCalledTimes(1);
  });

  it('records NYC range-integrity failures with date diagnostics', async () => {
    const err = Object.assign(
      new Error('ACRIS returned 10 rows outside requested range 03/08/2026-03/15/2026 upstream_range=03/04/2026-03/06/2026'),
      {
        requestedStart: '03/08/2026',
        requestedEnd: '03/15/2026',
        upstreamMin: '03/04/2026',
        upstreamMax: '03/06/2026',
        returnedRowCount: 10,
      }
    );
    mockScraper.mockRejectedValueOnce(err);

    const { runScheduledScrape, getRunHistory } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:range-integrity',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.failure_class).toBe('range_result_integrity');
    expect(result.filtered_out_count).toBe(10);
    expect(result.upstream_min_filing_date).toBe('03/04/2026');
    expect(result.upstream_max_filing_date).toBe('03/06/2026');

    const history = await getRunHistory(1);
    expect(history[0].failure_class).toBe('range_result_integrity');
    expect(history[0].filtered_out_count).toBe(10);
    expect(history[0].requested_date_start).toBe('03/08/2026');
    expect(history[0].requested_date_end).toBe('03/15/2026');
    expect(history[0].upstream_min_filing_date).toBe('03/04/2026');
    expect(history[0].upstream_max_filing_date).toBe('03/06/2026');
    expect(history[0].partial_reason).toBe('rows_filtered_outside_requested_range');
    expect(history[0].debug_artifact).toEqual(expect.objectContaining({
      failure_class: 'range_result_integrity',
      requested_date_start: '03/08/2026',
      requested_date_end: '03/15/2026',
      upstream_min_filing_date: '03/04/2026',
      upstream_max_filing_date: '03/06/2026',
      filtered_out_count: 10,
    }));
    expect(history[0].confidence?.status).toBe('low');
    expect(history[0].confidence?.reasons).toContain('range_result_integrity');
  });

  it('marks retry exhaustion after repeated transient failures', async () => {
    process.env.SCHEDULE_RUN_MAX_ATTEMPTS = '2';
    mockScraper
      .mockRejectedValueOnce(new Error('viewer did not return to acris result page'))
      .mockRejectedValueOnce(new Error('viewer did not return to acris result page'));

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:retry-exhausted',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('error');
    expect(result.failure_class).toBe('viewer_roundtrip');
    expect(result.retry_exhausted).toBe(1);
    expect(result.attempt_count).toBe(2);
    expect(mockScraper).toHaveBeenCalledTimes(2);
  });

  it('injects one transient viewer failure and succeeds on retry', async () => {
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:injected-viewer-retry',
      slot: 'afternoon',
      triggerSource: 'manual',
      testFailureClass: 'viewer_roundtrip',
    });

    expect(result.status).toBe('success');
    expect(result.attempt_count).toBe(2);
    expect(result.retried).toBe(1);
    expect(mockScraper).toHaveBeenCalledTimes(1);
    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(1);
  });

  it('injects one transient sheet export failure and succeeds from cached nyc rows', async () => {
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');

    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:injected-sheet-retry',
      slot: 'afternoon',
      triggerSource: 'manual',
      testFailureClass: 'sheet_export',
    });

    expect(result.status).toBe('success');
    expect(result.attempt_count).toBe(2);
    expect(result.retried).toBe(1);
    expect(mockScraper).toHaveBeenCalledTimes(1);
    expect(mockPushToSheetsForTab).toHaveBeenCalledTimes(1);
  });

  it('skips anomaly detection when there are fewer than three eligible baseline runs', async () => {
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-1', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-2', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0, partial: 1 },
    ]);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:anomaly-skip',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(anomalyAlerts.size).toBe(0);
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'scheduled_run_anomaly_skipped',
      reason: 'insufficient_baseline',
    }));
  });

  it('detects a sharp records_scraped regression without failing the run', async () => {
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-11', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-12', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-13', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
    ]);
    mockScraper.mockResolvedValueOnce([
      { filing_number: '1', amount: '100', amount_reason: 'ok' },
      { filing_number: '2', amount: '200', amount_reason: 'ok' },
    ]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 2, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:records-regression',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(anomalyAlerts.get('nyc_acris:2026-03-11:records-regression:quality_anomaly')?.metrics_triggered)
      .toContain('records_scraped');
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'scheduled_run_anomaly_detected',
      metrics_triggered: expect.arrayContaining(['records_scraped']),
    }));
  });

  it('detects amount coverage regressions', async () => {
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-21', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-22', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-23', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
    ]);
    mockScraper.mockResolvedValueOnce([
      { filing_number: '1', amount: null, amount_reason: 'missing' },
      { filing_number: '2', amount: null, amount_reason: 'missing' },
      { filing_number: '3', amount: null, amount_reason: 'missing' },
      { filing_number: '4', amount: null, amount_reason: 'missing' },
      { filing_number: '5', amount: '500', amount_reason: 'ok' },
    ]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 5, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:amount-regression',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(anomalyAlerts.get('nyc_acris:2026-03-11:amount-regression:quality_anomaly')?.metrics_triggered)
      .toContain('amount_coverage_pct');
  });

  it('detects OCR success regressions', async () => {
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-31', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-32', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-33', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
    ]);
    mockScraper.mockResolvedValueOnce([
      { filing_number: '1', amount: '100', amount_reason: 'ocr_missing' },
      { filing_number: '2', amount: '200', amount_reason: 'ocr_missing' },
      { filing_number: '3', amount: '300', amount_reason: 'ocr_missing' },
      { filing_number: '4', amount: '400', amount_reason: 'ocr_missing' },
      { filing_number: '5', amount: '500', amount_reason: 'ok' },
    ]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 5, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:ocr-regression',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(anomalyAlerts.get('nyc_acris:2026-03-11:ocr-regression:quality_anomaly')?.metrics_triggered)
      .toContain('ocr_success_pct');
  });

  it('detects row failure regressions', async () => {
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-41', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-42', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-43', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
    ]);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:row-fail-regression',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(anomalyAlerts.get('nyc_acris:2026-03-11:row-fail-regression:quality_anomaly')?.metrics_triggered)
      .toContain('row_fail_pct');
  });

  it('does not alert on normal variance', async () => {
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-51', records_scraped: 5, amount_coverage_pct: 95, ocr_success_pct: 95, row_fail_pct: 5 },
      { id: 'baseline-52', records_scraped: 5, amount_coverage_pct: 96, ocr_success_pct: 96, row_fail_pct: 4 },
      { id: 'baseline-53', records_scraped: 5, amount_coverage_pct: 97, ocr_success_pct: 97, row_fail_pct: 3 },
    ]);
    const records = [
      { filing_number: '1', amount: '100', amount_reason: 'ok' },
      { filing_number: '2', amount: '200', amount_reason: 'ok' },
      { filing_number: '3', amount: '300', amount_reason: 'ok' },
      { filing_number: '4', amount: '400', amount_reason: 'ok' },
      { filing_number: '5', amount: '500', amount_reason: 'ok' },
    ];
    Object.assign(records, {
      quality_summary: {
        discovered_count: 5,
        returned_count: 5,
      },
    });
    mockScraper.mockResolvedValueOnce(records);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 5, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:normal-variance',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(anomalyAlerts.size).toBe(0);
  });

  it('persists Maricopa enrichment telemetry even when artifact retrieval is disabled', async () => {
    const records = [{ filing_number: '1', amount: null, amount_reason: 'ok' }];
    Object.assign(records, {
      quality_summary: {
        requested_date_start: '03/06/2026',
        requested_date_end: '03/13/2026',
        discovered_count: 1,
        returned_count: 1,
        quarantined_count: 0,
        partial_run: true,
        partial_reason: 'artifact_or_ocr_incomplete',
        enriched_records: 0,
        partial_records: 1,
        artifact_retrieval_enabled: false,
        enrichment_mode: 'api_only',
        artifact_readiness_not_met: false,
      },
    });
    mockScraper.mockResolvedValueOnce(records);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: 'Master',
      row_count: 0,
      source_tabs: 1,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 1,
      current_run_quarantined_row_count: 1,
      current_run_conflict_row_count: 0,
      retained_prior_review_row_count: 0,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 0,
      purged_review_row_count: 0,
      review_summary: {
        accepted_row_count: 0,
        quarantined_row_count: 1,
        purged_review_row_count: 0,
        review_reason_counts: { partial_run: 1, low_confidence: 1 },
        current_run_quarantined_row_count: 1,
        current_run_conflict_row_count: 0,
        retained_prior_review_row_count: 0,
      },
    });

    const { runScheduledScrape, getRunHistory } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'maricopa_recorder',
      idempotencyKey: 'maricopa_recorder:2026-03-11:telemetry',
      slot: 'evening',
      triggerSource: 'manual',
    });

    const history = await getRunHistory(1);
    expect(history[0].artifact_retrieval_enabled).toBe(0);
    expect(history[0].artifact_fetch_coverage_pct).toBe(0);
    expect(history[0].enrichment_mode).toBe('api_only');
    expect(history[0].artifact_readiness_not_met).toBe(0);
    expect(history[0].enriched_record_count).toBe(0);
    expect(history[0].partial_record_count).toBe(1);
    expect(history[0].confidence?.reasons).toContain('artifact_retrieval_disabled');
    expect(history[0].confidence?.status).toBe('medium');
  });

  it('tracks normalized review reason buckets without forcing low confidence for conflicts alone', async () => {
    mockProbeCASOSResultCount.mockResolvedValueOnce(1);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: 'Master',
      row_count: 0,
      source_tabs: 1,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 0,
      current_run_quarantined_row_count: 0,
      current_run_conflict_row_count: 1,
      retained_prior_review_row_count: 12,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 0,
      purged_review_row_count: 0,
      review_summary: {
        accepted_row_count: 0,
        quarantined_row_count: 0,
        purged_review_row_count: 0,
        review_reason_counts: {
          conflict_ambiguous: 1,
          duplicate_against_current_run: 3,
          partial_run: 0,
          low_confidence: 0,
        },
        current_run_quarantined_row_count: 0,
        current_run_conflict_row_count: 1,
        retained_prior_review_row_count: 12,
      },
    });

    const { runScheduledScrape, getRunHistory } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'ca_sos',
      idempotencyKey: 'ca_sos:2026-03-12:afternoon:review-buckets',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    const history = await getRunHistory(1);
    expect(history[0].review_reason_counts_json).toBe(JSON.stringify({
      low_confidence: 0,
      conflict_ambiguous: 1,
      duplicate_or_existing: 3,
      partial_run: 0,
    }));
    expect(history[0].confidence?.reasons).toContain('conflict_ambiguous');
    expect(history[0].confidence?.reasons).toContain('duplicate_or_existing');
    expect(history[0].confidence?.status).toBe('medium');
  });

  it('flags Maricopa artifact readiness and fetch coverage when enrichment is enabled but incomplete', async () => {
    const records = [{ filing_number: '1', amount: null, amount_reason: 'ok' }, { filing_number: '2', amount: '200', amount_reason: 'ok' }];
    Object.assign(records, {
      quality_summary: {
        requested_date_start: '03/06/2026',
        requested_date_end: '03/13/2026',
        discovered_count: 2,
        returned_count: 2,
        quarantined_count: 0,
        partial_run: true,
        partial_reason: 'artifact_or_ocr_incomplete',
        enriched_records: 1,
        partial_records: 1,
        artifact_retrieval_enabled: true,
        artifact_fetch_coverage_pct: 50,
        enrichment_mode: 'artifact_enriched',
        artifact_readiness_not_met: true,
      },
    });
    mockScraper.mockResolvedValueOnce(records);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 2, tab_title: 'tab-name' });
    mockSyncMasterSheetTab.mockResolvedValueOnce({
      tab_title: 'Master',
      row_count: 1,
      source_tabs: 1,
      target_spreadsheet_id: 'target-sheet',
      fallback_used: false,
      quarantined_row_count: 1,
      current_run_quarantined_row_count: 1,
      current_run_conflict_row_count: 0,
      retained_prior_review_row_count: 0,
      review_tab_title: 'Review_Queue',
      new_master_row_count: 1,
      purged_review_row_count: 0,
      review_summary: {
        accepted_row_count: 1,
        quarantined_row_count: 1,
        purged_review_row_count: 0,
        review_reason_counts: { partial_run: 1, low_confidence: 1 },
        current_run_quarantined_row_count: 1,
        current_run_conflict_row_count: 0,
        retained_prior_review_row_count: 0,
      },
    });

    const { runScheduledScrape, getRunHistory } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'maricopa_recorder',
      idempotencyKey: 'maricopa_recorder:2026-03-11:artifact-coverage',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    const history = await getRunHistory(1);
    expect(history[0].artifact_retrieval_enabled).toBe(1);
    expect(history[0].artifact_fetch_coverage_pct).toBe(50);
    expect(history[0].enrichment_mode).toBe('artifact_enriched');
    expect(history[0].artifact_readiness_not_met).toBe(1);
    expect(history[0].confidence?.reasons).toEqual(expect.arrayContaining([
      'artifact_readiness_not_met',
      'artifact_fetch_coverage_below_target',
    ]));
    expect(history[0].confidence?.status).toBe('medium');
  });

  it('continues successfully when anomaly webhook delivery fails', async () => {
    process.env.SCHEDULE_ALERT_WEBHOOK_URL = 'https://example.invalid/webhook';
    mockFetch.mockResolvedValueOnce({ ok: false, status: 500, text: async () => 'boom' });
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-61', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-62', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-63', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
    ]);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    const result = await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:webhook-failure',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(result.status).toBe('success');
    expect(mockFetch.mock.calls.length).toBeGreaterThanOrEqual(1);
    expect(mockFetch.mock.calls.some((call) => call[0] === 'https://example.invalid/webhook')).toBe(true);
    expect(mockLog).toHaveBeenCalledWith(expect.objectContaining({
      stage: 'scheduled_run_anomaly_detected',
      webhook_attempted: true,
      webhook_delivered: false,
    }));
  });

  it('writes at most one anomaly alert record per run', async () => {
    await seedSuccessfulBaseline('nyc_acris', [
      { id: 'baseline-71', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-72', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
      { id: 'baseline-73', records_scraped: 5, amount_coverage_pct: 100, ocr_success_pct: 100, row_fail_pct: 0 },
    ]);
    mockScraper.mockResolvedValueOnce([{ filing_number: '1', amount: '100', amount_reason: 'ok' }]);
    mockPushToSheetsForTab.mockResolvedValueOnce({ uploaded: 1, tab_title: 'tab-name' });

    const { runScheduledScrape } = await import('../../src/scheduler');
    await runScheduledScrape({
      site: 'nyc_acris',
      idempotencyKey: 'nyc_acris:2026-03-11:dedupe',
      slot: 'afternoon',
      triggerSource: 'manual',
    });

    expect(anomalyAlerts.size).toBe(1);
  });
});
