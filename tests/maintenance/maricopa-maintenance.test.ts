import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockCreateIsolatedBrowserContext = vi.fn();
const mockGetMaricopaPersistedStateReadiness = vi.fn();
const mockLoadMaricopaSessionState = vi.fn();
const mockSaveMaricopaSessionState = vi.fn();
const mockSaveMaricopaArtifactCandidates = vi.fn();
const mockDiscoverMaricopaArtifactCandidates = vi.fn();
const mockFilterValidMaricopaArtifactCandidates = vi.fn();

vi.mock('../../src/browser/transport', () => ({
  createIsolatedBrowserContext: mockCreateIsolatedBrowserContext,
}));

vi.mock('../../src/scraper/maricopa_artifacts', () => ({
  discoverMaricopaArtifactCandidates: mockDiscoverMaricopaArtifactCandidates,
  filterValidMaricopaArtifactCandidates: mockFilterValidMaricopaArtifactCandidates,
  getMaricopaPersistedStateReadiness: mockGetMaricopaPersistedStateReadiness,
  loadMaricopaSessionState: mockLoadMaricopaSessionState,
  saveMaricopaArtifactCandidates: mockSaveMaricopaArtifactCandidates,
  saveMaricopaSessionState: mockSaveMaricopaSessionState,
}));

describe('Maricopa maintenance helpers', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    mockGetMaricopaPersistedStateReadiness.mockResolvedValue({
      artifactRetrievalEnabled: true,
      sessionPresent: true,
      sessionFresh: true,
      sessionCapturedAt: '2026-03-26T12:00:00.000Z',
      sessionAgeMinutes: 1,
      sessionMaxAgeMinutes: 720,
      artifactCandidatesPresent: true,
      artifactCandidatesFresh: true,
      artifactCandidateCount: 2,
      artifactCandidatesUpdatedAt: '2026-03-26T12:05:00.000Z',
      artifactCandidateAgeMinutes: 1,
      artifactCandidateMaxAgeMinutes: 1440,
      refreshRequired: false,
      detail: 'Maricopa persisted session and artifact candidates are available.',
    });
    mockLoadMaricopaSessionState.mockResolvedValue({
      version: 1,
      captured_at: '2026-03-26T12:00:00.000Z',
      transport_mode: 'legacy-sbr-cdp',
      source_url: 'https://example.test',
      cookie_summary: [],
      storage_state_path: 'out/maricopa/session/storage-state.json',
    });
    mockFilterValidMaricopaArtifactCandidates.mockImplementation((candidates: unknown[]) => candidates);
    mockDiscoverMaricopaArtifactCandidates.mockReturnValue([]);
  });

  it('refreshes Maricopa session state and returns readiness metadata', async () => {
    const page = {
      goto: vi.fn().mockResolvedValue(undefined),
      waitForTimeout: vi.fn().mockResolvedValue(undefined),
      locator: vi.fn().mockReturnValue({
        count: vi.fn().mockResolvedValue(3),
      }),
      url: vi.fn().mockReturnValue('https://recorder.maricopa.gov/recording/document-search-results.html'),
    };
    mockCreateIsolatedBrowserContext.mockResolvedValue({
      mode: 'legacy-sbr-cdp',
      context: {
        newPage: vi.fn().mockResolvedValue(page),
        storageState: vi.fn().mockResolvedValue({ cookies: [] }),
      },
      close: vi.fn().mockResolvedValue(undefined),
    });
    mockSaveMaricopaSessionState.mockResolvedValue({
      version: 1,
      captured_at: '2026-03-26T12:00:00.000Z',
      transport_mode: 'legacy-sbr-cdp',
      source_url: 'https://recorder.maricopa.gov/recording/document-search-results.html',
      cookie_summary: [],
      storage_state_path: 'out/maricopa/session/storage-state.json',
    });

    const { refreshMaricopaSessionState } = await import('../../src/maintenance/maricopa');
    const result = await refreshMaricopaSessionState();

    expect(result.ok).toBe(true);
    expect(result.operation).toBe('session_refresh');
    expect(result.transport_mode).toBe('legacy-sbr-cdp');
    expect(result.row_count).toBe(3);
    expect(result.session_captured_at).toBe('2026-03-26T12:00:00.000Z');
  });

  it('blocks artifact discovery when no persisted session is available', async () => {
    mockLoadMaricopaSessionState.mockResolvedValue(null);
    mockGetMaricopaPersistedStateReadiness.mockResolvedValue({
      artifactRetrievalEnabled: true,
      sessionPresent: false,
      sessionFresh: false,
      sessionMaxAgeMinutes: 720,
      artifactCandidatesPresent: false,
      artifactCandidatesFresh: false,
      artifactCandidateCount: 0,
      artifactCandidateMaxAgeMinutes: 1440,
      refreshRequired: true,
      refreshReason: 'session_missing_or_stale',
      detail: 'Maricopa session is missing. Run refresh:maricopa-session on the droplet.',
    });

    const { discoverMaricopaArtifacts } = await import('../../src/maintenance/maricopa');
    const result = await discoverMaricopaArtifacts();

    expect(result.ok).toBe(false);
    expect(result.operation).toBe('artifact_discovery');
    expect(result.refresh_reason).toBe('session_missing_or_stale');
    expect(result.blocking_reason).toContain('Run session refresh first');
    expect(mockCreateIsolatedBrowserContext).not.toHaveBeenCalled();
  });
});
