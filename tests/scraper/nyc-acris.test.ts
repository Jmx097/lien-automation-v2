import fs from 'fs';
import path from 'path';
import { describe, expect, it } from 'vitest';
import {
  chooseBetterDebtorName,
  extractNYCAcrisDetailFromHtml,
  extractNYCAcrisFieldsFromText,
  extractDocIdsFromResultsHtml,
  extractViewerArtifactFromHtml,
  inspectNYCAcrisPageReadiness,
  isPlausibleDebtorName,
  isUnexpectedViewerPageUrl,
  normalizeOcrAddress,
  resolveNYCAcrisDelay,
  sanitizeDebtorName,
  shouldRetryViewerOpen,
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

  it('extracts structured detail-page fields for audited ACRIS records', () => {
    const html = `
      <html>
        <body>
          <h1>Detailed Document Information</h1>
          <table>
            <tr><td>DOC. DATE:</td><td>2/25/2026</td></tr>
            <tr><td>RECORDED / FILED:</td><td>3/12/2026 5:06:05 PM</td></tr>
            <tr><td>DOC. AMOUNT:</td><td>$75,585.60</td></tr>
          </table>
          <div>PARTY 1</div>
          <table>
            <tr>
              <td>ALTAGRACIA JIMENEZ, SUSANA</td>
              <td>3018 KINGSBRIDGE AVE APT 1N</td>
              <td></td>
              <td>BRONX</td>
              <td>NY</td>
              <td>10463-5104</td>
            </tr>
          </table>
          <div>PARTY 2</div>
          <table>
            <tr>
              <td>INTERNAL REVENUE SERVICE</td>
              <td>135 HIGH STREET, STOP 155</td>
              <td></td>
              <td>HARTFORD</td>
              <td>CT</td>
              <td>06103</td>
            </tr>
          </table>
        </body>
      </html>
    `;

    expect(extractNYCAcrisDetailFromHtml(html)).toEqual({
      filingDate: '2/25/2026',
      recordedFiledAt: '3/12/2026 5:06:05 PM',
      debtorName: 'ALTAGRACIA JIMENEZ, SUSANA',
      debtorAddress: '3018 KINGSBRIDGE AVE APT 1N, BRONX, NY 10463-5104',
      securedPartyName: 'INTERNAL REVENUE SERVICE',
      securedPartyAddress: '135 HIGH STREET, STOP 155, HARTFORD, CT 06103',
      amount: '75585',
    });
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

  it('accepts detail-page html when detail markers are present', () => {
    const html = `
      <html>
        <body>
          <h1>Detailed Document Information</h1>
          <div>DOC. DATE</div>
          <div>PARTY 1</div>
        </body>
      </html>
    `;

    expect(inspectNYCAcrisPageReadiness(html, 'detail')).toMatchObject({
      ok: true,
      reason: 'detail_markers_present',
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

  it('detects transient chrome error viewer pages for retry', () => {
    expect(isUnexpectedViewerPageUrl('chrome-error://chromewebdata/')).toBe(true);
    expect(
      shouldRetryViewerOpen({
        ok: false,
        finalUrl: 'chrome-error://chromewebdata/',
        reason: 'unexpected_url',
      })
    ).toBe(true);
    expect(
      shouldRetryViewerOpen({
        ok: true,
        finalUrl: 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentImageView?doc_id=1',
        reason: 'viewer_iframe_present',
      })
    ).toBe(false);
  });

  it('extracts NYC OCR lead type and taxpayer name from text', () => {
    const text = `
      NOTICE OF FEDERAL TAX LIEN
      Name of Taxpayer ACME HOLDINGS LLC
      Residence 123 MAIN ST
      Total Amount Due $123,456.78
    `;

    expect(extractNYCAcrisFieldsFromText(text)).toEqual({
      leadType: 'Lien',
      taxpayerName: 'ACME HOLDINGS LLC',
      taxpayerAddress: '123 MAIN ST',
    });
  });

  it('extracts OCR residence text until the next known form section', () => {
    const text = `
      NOTICE OF FEDERAL TAX LIEN
      Name of Taxpayer DOMINIQUE PIERRE LOUIS
      Residence 123 MAIN ST BROOKLYN NY 11201
      Tax Period 2024
      Kind of Tax 1040
    `;

    expect(extractNYCAcrisFieldsFromText(text)).toEqual({
      leadType: 'Lien',
      taxpayerName: 'DOMINIQUE PIERRE LOUIS',
      taxpayerAddress: '123 MAIN ST BROOKLYN NY 11201',
    });
  });

  it('preserves a multiline residence block as street plus city state zip', () => {
    const text = `
      Name of Taxpayer DOMINIQUE PIERRE LOUIS
      Residence 340 E 31ST ST APT A2
      BROOKLYN, NY 11226-7986
      IMPORTANT RELEASE INFORMATION
    `;

    expect(extractNYCAcrisFieldsFromText(text)).toEqual({
      leadType: undefined,
      taxpayerName: 'DOMINIQUE PIERRE LOUIS',
      taxpayerAddress: '340 E 31ST ST APT A2, BROOKLYN, NY 11226-7986',
    });
  });

  it('trims short OCR suffix noise after a valid zip code', () => {
    expect(normalizeOcrAddress('160 COLUMBIA HTS APT 10C BROOKLYN, NY 11201-2189 . 1')).toBe(
      '160 COLUMBIA HTS APT 10C BROOKLYN, NY 11201-2189'
    );
  });

  it('trims underscore OCR suffix noise after a valid zip code', () => {
    expect(normalizeOcrAddress('160 COLUMBIA HTS APT 10C BROOKLYN, NY 11201-2189 _')).toBe(
      '160 COLUMBIA HTS APT 10C BROOKLYN, NY 11201-2189'
    );
  });

  it('does not invent an address when OCR only contains the residence label', () => {
    const text = `
      NOTICE OF FEDERAL TAX LIEN
      Name of Taxpayer ACME HOLDINGS LLC
      Residence
      IMPORTANT - SEE REVERSE
    `;

    expect(extractNYCAcrisFieldsFromText(text)).toEqual({
      leadType: 'Lien',
      taxpayerName: 'ACME HOLDINGS LLC',
      taxpayerAddress: undefined,
    });
  });

  it('still blanks pure timestamp-like debtor names from result rows', () => {
    expect(sanitizeDebtorName('03/11/2026 11:59:59 PM')).toBe('');
    expect(sanitizeDebtorName('Recorded: 03/11/2026 11:59:59 PM')).toBe('');
    expect(isPlausibleDebtorName('Last Updated: 03/11/2026 11:59:59 PM')).toBe(false);
  });

  it('keeps plausible business and personal debtor names', () => {
    expect(sanitizeDebtorName('ACME HOLDINGS LLC')).toBe('ACME HOLDINGS LLC');
    expect(sanitizeDebtorName('John Q Smith')).toBe('John Q Smith');
    expect(isPlausibleDebtorName('ACME HOLDINGS LLC')).toBe(true);
    expect(isPlausibleDebtorName('John Q Smith')).toBe(true);
  });

  it('prefers OCR taxpayer names only when they improve the debtor value', () => {
    expect(chooseBetterDebtorName('03/11/2026 11:59:59 PM', 'ACME HOLDINGS LLC')).toBe('ACME HOLDINGS LLC');
    expect(chooseBetterDebtorName('ACME LLC', 'Recorded: 03/11/2026')).toBe('ACME LLC');
    expect(chooseBetterDebtorName('ACME LLC', 'ACME HOLDINGS LLC')).toBe('ACME HOLDINGS LLC');
  });

  it('keeps lower-confidence debtor detail instead of blanking it when useful text remains', () => {
    expect(sanitizeDebtorName('Party 1: Dominique Pierre Louis')).toBe('Dominique Pierre Louis');
    expect(sanitizeDebtorName('Taxpayer Name: Sami Rabiaa')).toBe('Sami Rabiaa');
    expect(sanitizeDebtorName('Recorded owner Dominique Pierre Louis')).toBe('owner Dominique Pierre Louis');
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
