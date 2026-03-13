import fs from 'fs';
import path from 'path';
import { describe, expect, it, vi } from 'vitest';
import {
  discoverMaricopaArtifactCandidates,
  getMaricopaPersistedStateReadiness,
  isFreshMaricopaSession,
} from '../../src/scraper/maricopa_artifacts';

const fixtureDir = path.join(process.cwd(), 'tests', 'fixtures', 'maricopa');

describe('maricopa artifact helpers', () => {
  it('discovers candidate artifact endpoints from captured network requests', () => {
    const requests = JSON.parse(fs.readFileSync(path.join(fixtureDir, 'artifact-requests.json'), 'utf8'));
    const candidates = discoverMaricopaArtifactCandidates(requests, '20260017884');

    expect(candidates).toHaveLength(2);
    expect(candidates[0]).toMatchObject({
      kind: 'pdf',
      urlTemplate: 'https://recorder.maricopa.gov/recording/api/document/{recordingNumber}/preview.pdf',
    });
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

    const readiness = await getMaricopaPersistedStateReadiness();

    expect(readiness.refreshRequired).toBe(true);
    expect(readiness.refreshReason).toBe('session_missing_or_stale');
    delete process.env.MARICOPA_ENABLE_ARTIFACT_RETRIEVAL;
    delete process.env.SQLITE_DB_PATH;
    delete process.env.MARICOPA_OUT_DIR;
    fs.rmSync(sqliteDbPath, { force: true });
    fs.rmSync(maricopaOutDir, { recursive: true, force: true });
  });
});
