import fs from 'fs';
import path from 'path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  discoverMaricopaArtifactCandidates,
  filterValidMaricopaArtifactCandidates,
  isFreshMaricopaSession,
  isUsableMaricopaArtifactPayload,
  validateMaricopaArtifactCandidate,
} from '../../src/scraper/maricopa_artifacts';

const fixtureDir = path.join(process.cwd(), 'tests', 'fixtures', 'maricopa');

describe('maricopa artifact helpers', () => {
  afterEach(() => {
    vi.resetModules();
    vi.unmock('../../src/scheduler/store');
  });

  it('discovers at least one recording-specific artifact endpoint from captured network requests', () => {
    const requests = JSON.parse(fs.readFileSync(path.join(fixtureDir, 'artifact-requests.json'), 'utf8'));
    const candidates = discoverMaricopaArtifactCandidates(requests, '20260017884');

    expect(candidates).toHaveLength(1);
    expect(candidates[0]).toMatchObject({
      kind: 'image',
      urlTemplate: 'https://recorder.maricopa.gov/recording/api/document/{recordingNumber}/page/1.png',
    });
  });

  it('rejects noisy non-artifact candidates from results pages and analytics', () => {
    const candidates = filterValidMaricopaArtifactCandidates([
      {
        urlTemplate: 'https://recorder.maricopa.gov/recording/document-search-results.html?documentCode=FL',
        sampleUrl: 'https://recorder.maricopa.gov/recording/document-search-results.html?documentCode=FL',
        kind: 'document',
      },
      {
        urlTemplate: 'https://analytics.google.com/g/collect?v=2',
        sampleUrl: 'https://analytics.google.com/g/collect?v=2',
        kind: 'document',
      },
      {
        urlTemplate: 'https://publicapi.recorder.maricopa.gov/preview/pdf?recordingNumber={recordingNumber}&suffix=',
        sampleUrl: 'https://publicapi.recorder.maricopa.gov/preview/pdf?recordingNumber=20260017884&suffix=',
        kind: 'pdf',
      },
    ], '20260017884');

    expect(candidates).toEqual([
      {
        urlTemplate: 'https://publicapi.recorder.maricopa.gov/preview/pdf?recordingNumber={recordingNumber}&suffix=',
        sampleUrl: 'https://publicapi.recorder.maricopa.gov/preview/pdf?recordingNumber=20260017884&suffix=',
        kind: 'pdf',
      },
    ]);
  });

  it('marks direct preview URLs as valid candidates', () => {
    expect(
      validateMaricopaArtifactCandidate(
        {
          urlTemplate: 'https://publicapi.recorder.maricopa.gov/preview/pdf?recordingNumber={recordingNumber}&suffix=',
          sampleUrl: 'https://publicapi.recorder.maricopa.gov/preview/pdf?recordingNumber=20260017884&suffix=',
          kind: 'pdf',
        },
        '20260017884',
      ),
    ).toEqual({ valid: true });
  });

  it('marks recording-specific page image URLs as valid candidates', () => {
    expect(
      validateMaricopaArtifactCandidate(
        {
          urlTemplate: 'https://recorder.maricopa.gov/recording/api/document/{recordingNumber}/page/1.png',
          sampleUrl: 'https://recorder.maricopa.gov/recording/api/document/20260017884/page/1.png',
          kind: 'image',
        },
        '20260017884',
      ),
    ).toEqual({ valid: true });
  });

  it('treats recent session metadata as fresh', () => {
    expect(isFreshMaricopaSession(new Date().toISOString())).toBe(true);
  });

  it('reports refresh required when artifact retrieval is enabled but no persisted state exists', async () => {
    const sqliteDbPath = path.join(process.cwd(), 'tests', 'fixtures', 'tmp-maricopa-artifacts.db');
    const maricopaOutDir = path.join(process.cwd(), 'tests', 'fixtures', 'tmp-maricopa-out');
    process.env.MARICOPA_ENABLE_ARTIFACT_RETRIEVAL = '1';
    delete process.env.DATABASE_URL;
    process.env.SQLITE_DB_PATH = sqliteDbPath;
    process.env.MARICOPA_OUT_DIR = maricopaOutDir;

    vi.doMock('../../src/scheduler/store', () => ({
      ScheduledRunStore: class {
        async getSiteStateArtifact() {
          return null;
        }
        async close() {
          return null;
        }
      },
    }));

    const { getMaricopaPersistedStateReadiness } = await import('../../src/scraper/maricopa_artifacts');

    const readiness = await getMaricopaPersistedStateReadiness();

    expect(readiness.refreshRequired).toBe(true);
    expect(readiness.refreshReason).toBe('session_missing_or_stale');
    delete process.env.MARICOPA_ENABLE_ARTIFACT_RETRIEVAL;
    delete process.env.SQLITE_DB_PATH;
    delete process.env.MARICOPA_OUT_DIR;
    fs.rmSync(sqliteDbPath, { force: true });
    fs.rmSync(maricopaOutDir, { recursive: true, force: true });
  });

  it('treats persisted but invalid candidates as not ready', async () => {
    const sqliteDbPath = path.join(process.cwd(), 'tests', 'fixtures', 'tmp-maricopa-artifacts-invalid.db');
    const maricopaOutDir = path.join(process.cwd(), 'tests', 'fixtures', 'tmp-maricopa-out-invalid');
    process.env.MARICOPA_ENABLE_ARTIFACT_RETRIEVAL = '1';
    delete process.env.DATABASE_URL;
    process.env.SQLITE_DB_PATH = sqliteDbPath;
    process.env.MARICOPA_OUT_DIR = maricopaOutDir;

    vi.doMock('../../src/scheduler/store', () => ({
      ScheduledRunStore: class {
        async getSiteStateArtifact() {
          return null;
        }
        async close() {
          return null;
        }
      },
    }));

    const { getMaricopaPersistedStateReadiness } = await import('../../src/scraper/maricopa_artifacts');

    fs.mkdirSync(path.join(maricopaOutDir, 'session'), { recursive: true });
    fs.writeFileSync(
      path.join(maricopaOutDir, 'session', 'storage-state.json'),
      JSON.stringify({ cookies: [], origins: [] }),
      'utf8',
    );
    fs.writeFileSync(
      path.join(maricopaOutDir, 'session', 'session-meta.json'),
      JSON.stringify({
        version: 1,
        captured_at: new Date().toISOString(),
        transport_mode: 'brightdata-browser-api',
        source_url: 'https://example.test',
        cookie_summary: [],
        storage_state_path: path.join(maricopaOutDir, 'session', 'storage-state.json'),
      }),
      'utf8',
    );
    fs.writeFileSync(
      path.join(maricopaOutDir, 'discovery-candidates.json'),
      JSON.stringify([
        {
          urlTemplate: 'https://recorder.maricopa.gov/recording/document-search-results.html?documentCode=FL',
          sampleUrl: 'https://recorder.maricopa.gov/recording/document-search-results.html?documentCode=FL',
          kind: 'document',
        },
      ]),
      'utf8',
    );

    const readiness = await getMaricopaPersistedStateReadiness();

    expect(readiness.refreshRequired).toBe(true);
    expect(readiness.refreshReason).toBe('artifact_candidates_missing');
    expect(readiness.detail).toContain('present but invalid');

    delete process.env.MARICOPA_ENABLE_ARTIFACT_RETRIEVAL;
    delete process.env.SQLITE_DB_PATH;
    delete process.env.MARICOPA_OUT_DIR;
    fs.rmSync(sqliteDbPath, { force: true });
    fs.rmSync(maricopaOutDir, { recursive: true, force: true });
  });

  it('rejects HTML or non-artifact download bodies before they are saved', () => {
    const pdfBody = Buffer.from('%PDF-1.7\n1 0 obj\n<<>>\nendobj\nxref\ntrailer\n%%EOF\n' + ' '.repeat(48), 'latin1');
    const htmlBody = Buffer.from('<html><body>Just a moment...</body></html>', 'utf8');
    const pngBody = Buffer.from([
      0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
      0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
      0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
      0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
      0xde,
    ]);

    expect(isUsableMaricopaArtifactPayload(pdfBody, 'application/pdf', 'https://example.test/doc.pdf')).toBe(true);
    expect(isUsableMaricopaArtifactPayload(htmlBody, 'text/html', 'https://example.test/doc.pdf')).toBe(false);
    expect(isUsableMaricopaArtifactPayload(Buffer.from('not a pdf', 'utf8'), 'application/pdf', 'https://example.test/doc.pdf')).toBe(false);
    expect(isUsableMaricopaArtifactPayload(pngBody, 'image/png', 'https://example.test/doc.png')).toBe(true);
  });
});
