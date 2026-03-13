import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';
import { discoverMaricopaArtifactCandidates, isFreshMaricopaSession } from '../../src/scraper/maricopa_artifacts';

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
});
