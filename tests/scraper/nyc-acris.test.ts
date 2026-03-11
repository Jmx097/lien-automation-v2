import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';
import {
  extractDocIdsFromResultsHtml,
  extractViewerArtifactFromHtml,
  inspectNYCAcrisPageReadiness,
  resolveNYCAcrisDelay,
} from '../../src/scraper/nyc_acris';

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

  it('accepts index html when ACRIS shell markers are present', () => {
    const html = `
      <html>
        <head>
          <script src="/DS/Scripts/Global.js"></script>
          <script src="/DS/Scripts/login.js"></script>
        </head>
        <body></body>
      </html>
    `;

    expect(inspectNYCAcrisPageReadiness(html, 'index')).toMatchObject({
      ok: true,
      hasShellMarker: true,
      reason: 'shell_marker_present',
    });
  });

  it('accepts document type html only when the anti-forgery token is present', () => {
    const html = `
      <html>
        <body>
          <form>
            <input type="hidden" name="__RequestVerificationToken" value="abc123" />
          </form>
        </body>
      </html>
    `;

    expect(inspectNYCAcrisPageReadiness(html, 'document_type')).toMatchObject({
      ok: true,
      hasToken: true,
      reason: 'token_present',
    });
  });

  it('accepts result-page html when doc ids and hidden paging fields are present', () => {
    const html = `
      <html>
        <body>
          <form>
            <input type="hidden" name="__RequestVerificationToken" value="abc123" />
            <input type="hidden" name="hid_page" value="1" />
          </form>
          <table>
            <tr><td><a href="/DS/DocumentSearch/DocumentImageView?doc_id=2026022700399005">doc</a></td></tr>
          </table>
        </body>
      </html>
    `;

    expect(inspectNYCAcrisPageReadiness(html, 'results')).toMatchObject({
      ok: true,
      hasToken: true,
      hasResultMarker: true,
      reason: 'result_markers_present',
    });
  });

  it('accepts image-view html when the mainframe viewer iframe is present', () => {
    const html = `
      <html>
        <body>
          <iframe name="mainframe" src="/DS/DocumentSearch/DocumentImageVtu?doc_id=2026022700399005"></iframe>
        </body>
      </html>
    `;

    expect(inspectNYCAcrisPageReadiness(html, 'image_view')).toMatchObject({
      ok: true,
      hasViewerIframe: true,
      reason: 'viewer_iframe_present',
    });
  });

  it('rejects partial html that lacks shell markers and token', () => {
    const html = '<html><body>Loading...</body></html>';

    expect(inspectNYCAcrisPageReadiness(html, 'index')).toMatchObject({
      ok: false,
      hasShellMarker: false,
      hasToken: false,
      reason: 'insufficient_page_content',
    });
    expect(inspectNYCAcrisPageReadiness(html, 'document_type')).toMatchObject({
      ok: false,
      hasToken: false,
      reason: 'missing_token',
    });
    expect(inspectNYCAcrisPageReadiness(html, 'results')).toMatchObject({
      ok: false,
      hasResultMarker: false,
      reason: 'missing_result_markers',
    });
    expect(inspectNYCAcrisPageReadiness(html, 'image_view')).toMatchObject({
      ok: false,
      hasViewerIframe: false,
      reason: 'missing_viewer_iframe',
    });
  });

  it('keeps pacing jitter within the configured bounds', () => {
    expect(resolveNYCAcrisDelay(2000, 4000, 0)).toBe(2000);
    expect(resolveNYCAcrisDelay(2000, 4000, 1)).toBe(4000);
    expect(resolveNYCAcrisDelay(8000, 15000, 0.5)).toBeGreaterThanOrEqual(8000);
    expect(resolveNYCAcrisDelay(8000, 15000, 0.5)).toBeLessThanOrEqual(15000);
  });
});
