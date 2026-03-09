import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';
import { extractDocIdsFromResultsHtml, extractViewerArtifactFromHtml, resolveNYCAcrisDelay } from '../../src/scraper/nyc_acris';

const fixtureDir = path.join(process.cwd(), 'tests', 'fixtures', 'acris');

describe('nyc acris fixture parsing', () => {
  it('extracts document ids from a results fixture with extra third-party noise', () => {
    const html = fs.readFileSync(path.join(fixtureDir, 'document-type-result.html'), 'utf8');
    expect(extractDocIdsFromResultsHtml(html)).toEqual(['2026022700399005', '2026022700399004']);
  });

  it('extracts document ids from IMG button result rows', () => {
    const html = fs.readFileSync(path.join(fixtureDir, 'document-type-result-live-buttons.html'), 'utf8');
    expect(extractDocIdsFromResultsHtml(html)).toEqual(['2026022700399005', '2026022700399004']);
  });

  it('extracts the mainframe viewer source and image urls', () => {
    const html = fs.readFileSync(path.join(fixtureDir, 'document-image-view.html'), 'utf8');
    const artifact = extractViewerArtifactFromHtml(html);

    expect(artifact.title).toContain('New York Web Public Inquiry');
    expect(artifact.viewerSrc).toContain('/DS/DocumentSearch/DocumentImageVtu');
    expect(artifact.imageUrls).toEqual(['https://a836-acris.nyc.gov/DS/DocumentSearch/GetImage?img=1']);
  });

  it('keeps pacing jitter within the configured bounds', () => {
    expect(resolveNYCAcrisDelay(2000, 4000, 0)).toBe(2000);
    expect(resolveNYCAcrisDelay(2000, 4000, 1)).toBe(4000);
    expect(resolveNYCAcrisDelay(8000, 15000, 0.5)).toBeGreaterThanOrEqual(8000);
    expect(resolveNYCAcrisDelay(8000, 15000, 0.5)).toBeLessThanOrEqual(15000);
  });
});
