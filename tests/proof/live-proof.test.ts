import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { LienRecord } from '../../src/types';
import {
  getMaricopaProofReadiness,
  runCASOSCanary,
  runMaricopaCanary,
} from '../../src/proof/live-proof';

function buildRow(overrides: Partial<LienRecord> = {}): LienRecord {
  return {
    state: 'CA',
    source: 'ca_sos',
    county: 'Los Angeles',
    ucc_type: 'Tax Lien',
    debtor_name: 'ACME LLC',
    debtor_address: '123 Main St, Los Angeles, CA 90001',
    file_number: '20260001',
    secured_party_name: 'IRS',
    secured_party_address: 'PO Box 1',
    status: 'Active',
    filing_date: '03/13/2026',
    lapse_date: '03/13/2031',
    document_type: 'Lien',
    pdf_filename: '20260001.pdf',
    processed: true,
    confidence_score: 0.95,
    ...overrides,
  };
}

describe('live proof helpers', () => {
  beforeEach(() => {
    delete process.env.JOB_DATE_START;
    delete process.env.JOB_DATE_END;
    delete process.env.JOB_MAX_RECORDS;
    process.env.SCHEDULE_MAX_RECORDS_FLOOR = '25';
    process.env.MARICOPA_MAX_RECORDS = '5';
  });

  it('runs the CA SOS canary through shared source-tab and merged-tab sync', async () => {
    const pushRun = vi.fn().mockResolvedValue({
      uploaded: 2,
      tab_title: 'ca_sos_canary_03-06-2026_to_03-13-2026_20260313T110000_Pacific',
    });
    const syncMaster = vi.fn().mockResolvedValue({
      tab_title: 'Master',
      review_tab_title: 'Review_Queue',
      quarantined_row_count: 1,
      new_master_row_count: 1,
    });
    const scrape = vi.fn().mockResolvedValue([
      buildRow({ source: 'ca_sos', file_number: '20260001' }),
      buildRow({ source: 'ca_sos', file_number: '20260002', error: 'panel_failed' }),
    ]);

    const summary = await runCASOSCanary({
      now: () => new Date('2026-03-13T12:00:00Z'),
      pushRun,
      syncMaster,
      scrape,
    });

    expect(scrape).toHaveBeenCalledWith({
      date_start: '03/06/2026',
      date_end: '03/13/2026',
      max_records: 25,
    });
    expect(pushRun).toHaveBeenCalledWith(
      expect.any(Array),
      expect.objectContaining({
        label: 'ca_sos_canary',
        date_start: '03/06/2026',
        date_end: '03/13/2026',
      }),
    );
    expect(syncMaster).toHaveBeenCalledWith({
      includePrefixes: ['Scheduled_', 'ca_sos_canary_'],
    });
    expect(summary).toEqual({
      site: 'ca_sos',
      date_start: '03/06/2026',
      date_end: '03/13/2026',
      max_records: 25,
      records_scraped: 2,
      complete_records: 1,
      incomplete_records: 1,
      rows_uploaded: 2,
      source_tab_title: 'ca_sos_canary_03-06-2026_to_03-13-2026_20260313T110000_Pacific',
      master_tab_title: 'Master',
      review_tab_title: 'Review_Queue',
      quarantined_row_count: 1,
      new_master_row_count: 1,
    });
  });

  it('summarizes Maricopa readiness from persisted state helpers', async () => {
    const capturedAt = new Date().toISOString();
    const readiness = await getMaricopaProofReadiness({
      getReadiness: vi.fn().mockResolvedValue({
        artifactRetrievalEnabled: true,
        sessionPresent: true,
        sessionFresh: true,
        sessionCapturedAt: capturedAt,
        artifactCandidatesPresent: true,
        artifactCandidateCount: 2,
        refreshRequired: false,
        detail: 'Maricopa persisted session and artifact candidates are available.',
      }),
      loadSession: vi.fn().mockResolvedValue({
        version: 1,
        captured_at: capturedAt,
        transport_mode: 'brightdata-browser-api',
        source_url: 'https://example.test',
        cookie_summary: [],
        storage_state_path: 'out/maricopa/session/storage-state.json',
      }),
      loadCandidates: vi.fn().mockResolvedValue([
        { urlTemplate: 'https://example.test/{recordingNumber}.pdf', sampleUrl: 'https://example.test/1.pdf', kind: 'pdf' },
        { urlTemplate: 'https://example.test/{recordingNumber}.png', sampleUrl: 'https://example.test/1.png', kind: 'image' },
      ]),
      fetchLatestDate: vi.fn().mockResolvedValue('2026-03-12'),
    });

    expect(readiness).toEqual({
      artifact_retrieval_enabled: true,
      session_present: true,
      session_fresh: true,
      session_captured_at: capturedAt,
      discovery_candidate_count: 2,
      refresh_required: false,
      refresh_reason: undefined,
      detail: 'Maricopa persisted session and artifact candidates are available.',
      latest_searchable_date: '2026-03-12',
    });
  });

  it('fails Maricopa canary early when artifact retrieval is disabled', async () => {
    await expect(
      runMaricopaCanary(
        {
          now: () => new Date('2026-03-13T12:00:00Z'),
          pushRun: vi.fn(),
          syncMaster: vi.fn(),
          scrape: vi.fn(),
        },
        {
          getReadiness: vi.fn().mockResolvedValue({
            artifactRetrievalEnabled: false,
            sessionPresent: true,
            sessionFresh: true,
            sessionCapturedAt: '2026-03-13T10:00:00.000Z',
            artifactCandidatesPresent: true,
            artifactCandidateCount: 1,
            refreshRequired: false,
            refreshReason: 'artifact_retrieval_disabled',
            detail: 'Maricopa artifact retrieval is disabled by configuration.',
          }),
          loadSession: vi.fn().mockResolvedValue({
            version: 1,
            captured_at: '2026-03-13T10:00:00.000Z',
            transport_mode: 'brightdata-browser-api',
            source_url: 'https://example.test',
            cookie_summary: [],
            storage_state_path: 'out/maricopa/session/storage-state.json',
          }),
          loadCandidates: vi.fn().mockResolvedValue([{ urlTemplate: 'https://example.test/{recordingNumber}.pdf', sampleUrl: 'https://example.test/1.pdf', kind: 'pdf' }]),
          fetchLatestDate: vi.fn().mockResolvedValue('2026-03-12'),
        },
      ),
    ).rejects.toThrow(
      'Maricopa live proof requires MARICOPA_ENABLE_ARTIFACT_RETRIEVAL to stay enabled so rows can be fully verified.',
    );
  });

  it('runs Maricopa canary with shared Review_Queue sync once readiness is healthy', async () => {
    const pushRun = vi.fn().mockResolvedValue({
      uploaded: 1,
      tab_title: 'maricopa_recorder_canary_03-06-2026_to_03-13-2026_20260313T110000_Pacific',
    });
    const syncMaster = vi.fn().mockResolvedValue({
      tab_title: 'Master',
      review_tab_title: 'Review_Queue',
      quarantined_row_count: 1,
      new_master_row_count: 0,
    });
    const scrape = vi.fn().mockResolvedValue([
      buildRow({
        state: 'AZ',
        source: 'maricopa_recorder',
        county: 'Maricopa',
        file_number: '20260017884',
        debtor_address: '',
        error: 'address_missing',
      }),
    ]);

    const summary = await runMaricopaCanary(
      {
        now: () => new Date('2026-03-13T12:00:00Z'),
        pushRun,
        syncMaster,
        scrape,
      },
      {
        getReadiness: vi.fn().mockResolvedValue({
          artifactRetrievalEnabled: true,
          sessionPresent: true,
          sessionFresh: true,
          sessionCapturedAt: '2026-03-13T10:00:00.000Z',
          artifactCandidatesPresent: true,
          artifactCandidateCount: 2,
          refreshRequired: false,
          detail: 'Maricopa persisted session and artifact candidates are available.',
        }),
        loadSession: vi.fn().mockResolvedValue({
          version: 1,
          captured_at: '2026-03-13T10:00:00.000Z',
          transport_mode: 'brightdata-browser-api',
          source_url: 'https://example.test',
          cookie_summary: [],
          storage_state_path: 'out/maricopa/session/storage-state.json',
        }),
        loadCandidates: vi.fn().mockResolvedValue([
          { urlTemplate: 'https://example.test/{recordingNumber}.pdf', sampleUrl: 'https://example.test/1.pdf', kind: 'pdf' },
        ]),
        fetchLatestDate: vi.fn().mockResolvedValue('2026-03-12'),
      },
    );

    expect(syncMaster).toHaveBeenCalledWith({
      includePrefixes: ['Scheduled_', 'maricopa_recorder_canary_'],
    });
    expect(summary.site).toBe('maricopa_recorder');
    expect(summary.incomplete_records).toBe(1);
    expect(summary.quarantined_row_count).toBe(1);
    expect(summary.review_tab_title).toBe('Review_Queue');
  });

  it('surfaces invalid Maricopa candidate state as a readiness block', async () => {
    const readiness = await getMaricopaProofReadiness({
      getReadiness: vi.fn().mockResolvedValue({
        artifactRetrievalEnabled: true,
        sessionPresent: true,
        sessionFresh: true,
        sessionCapturedAt: '2026-03-13T10:00:00.000Z',
        artifactCandidatesPresent: false,
        artifactCandidateCount: 0,
        refreshRequired: true,
        refreshReason: 'artifact_candidates_missing',
        detail: 'Maricopa artifact candidates are present but invalid. Rerun discover:maricopa-live to capture preview/document endpoints.',
      }),
      loadSession: vi.fn().mockResolvedValue({
        version: 1,
        captured_at: '2026-03-13T10:00:00.000Z',
        transport_mode: 'brightdata-browser-api',
        source_url: 'https://example.test',
        cookie_summary: [],
        storage_state_path: 'out/maricopa/session/storage-state.json',
      }),
      loadCandidates: vi.fn().mockResolvedValue([
        { urlTemplate: 'https://recorder.maricopa.gov/recording/document-search-results.html?documentCode=FL', sampleUrl: 'https://recorder.maricopa.gov/recording/document-search-results.html?documentCode=FL', kind: 'document' },
      ]),
      fetchLatestDate: vi.fn().mockResolvedValue('2026-03-12'),
    });

    expect(readiness.refresh_required).toBe(true);
    expect(readiness.detail).toContain('present but invalid');
  });
});
