import fs from 'fs';
import path from 'path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  buildMaricopaSearchUrl,
  clampMaricopaDateRange,
  fetchLatestMaricopaSearchableDate,
  isChallengeBody,
  mapMaricopaDetailToLienRecord,
  MaricopaScrapeError,
  normalizeMaricopaDate,
  parseMaricopaIndexDate,
  scrapeMaricopaRecorder,
  toMaricopaIsoDate,
} from '../../src/scraper/maricopa_recorder';

const fixtureDir = path.join(process.cwd(), 'tests', 'fixtures', 'maricopa');

describe('maricopa recorder mapping', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('converts request dates to the API format', () => {
    expect(toMaricopaIsoDate('01/12/2026')).toBe('2026-01-12');
  });

  it('normalizes Maricopa result dates to MM/DD/YYYY', () => {
    expect(normalizeMaricopaDate('1-2-2026')).toBe('01/02/2026');
  });

  it('parses latest searchable dates from the index endpoint format', () => {
    expect(parseMaricopaIndexDate('3/10/2026')).toBe('2026-03-10');
  });

  it('builds the live search URL with maxResults=500', () => {
    const url = new URL(buildMaricopaSearchUrl('2026-01-01', '2026-02-13', 1));
    expect(url.searchParams.get('pageSize')).toBe('20');
    expect(url.searchParams.get('maxResults')).toBe('500');
    expect(url.searchParams.get('documentCode')).toBe('FL');
  });

  it('clamps future date ranges to the latest searchable date', () => {
    expect(clampMaricopaDateRange('2030-01-01', '2030-01-02', '2026-03-10')).toEqual({
      date_start_iso: '2030-01-01',
      date_end_iso: '2026-03-10',
      clamped: true,
      empty: true,
    });
  });

  it('detects challenge bodies from Cloudflare interstitials', () => {
    expect(isChallengeBody('Just a moment... Enable JavaScript and cookies to continue')).toBe(true);
    expect(isChallengeBody('Security check required before proceeding')).toBe(true);
    expect(isChallengeBody('{"searchResults":[]}')).toBe(false);
  });

  it('maps a Maricopa detail payload into the shared lien record shape', () => {
    const detail = JSON.parse(fs.readFileSync(path.join(fixtureDir, 'document-detail.json'), 'utf8'));
    const record = mapMaricopaDetailToLienRecord(detail);

    expect(record).toMatchObject({
      state: 'AZ',
      source: 'maricopa_recorder',
      county: 'Maricopa',
      ucc_type: 'Federal Tax Lien',
      debtor_name: 'BASCH DAVID',
      debtor_address: '',
      file_number: '20260017884',
      secured_party_name: 'INTERNAL REVENUE SERVICE',
      filing_date: '01/12/2026',
      document_type: 'FED TAX L',
      lead_type: 'Lien',
      processed: true,
    });
    expect(record.confidence_score).toBeGreaterThan(0.3);
    expect(record.confidence_score).toBeLessThan(0.5);
  });

  it('maps OCR enrichment into complete Maricopa records', () => {
    const detail = JSON.parse(fs.readFileSync(path.join(fixtureDir, 'document-detail.json'), 'utf8'));
    const record = mapMaricopaDetailToLienRecord(detail, {
      artifactUrl: 'https://example.test/20260017884.pdf',
      artifactPath: 'C:\\temp\\20260017884.pdf',
      artifactContentType: 'application/pdf',
      amount: '12345',
      amountConfidence: 0.98,
      amountReason: 'ok',
      leadType: 'Lien',
      debtorName: 'DAVID BASCH',
      debtorAddress: '123 MAIN ST, PHOENIX, AZ 85003',
    });

    expect(record).toMatchObject({
      debtor_address: '123 MAIN ST, PHOENIX, AZ 85003',
      amount: '12345',
      amount_reason: 'ok',
      lead_type: 'Lien',
      error: '',
    });
    expect(record.confidence_score).toBeGreaterThan(0.8);
  });

  it('fetches and normalizes the latest searchable date', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      text: async () => '"3/10/2026"',
    }));

    await expect(fetchLatestMaricopaSearchableDate()).resolves.toBe('2026-03-10');
  });

  it('surfaces blocked challenge bodies as a retryable Maricopa-specific failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      text: async () => 'Just a moment... Enable JavaScript and cookies to continue',
    }));

    await expect(fetchLatestMaricopaSearchableDate()).rejects.toMatchObject({
      name: 'MaricopaScrapeError',
      kind: 'challenge_blocked',
      retryable: true,
    } satisfies Partial<MaricopaScrapeError>);
  });

  it('returns zero rows when the requested range is entirely after the latest searchable date', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        text: async () => '"3/10/2026"',
      });
    vi.stubGlobal('fetch', fetchMock);

    const rows = await scrapeMaricopaRecorder({
      date_start: '01/01/2030',
      date_end: '01/02/2030',
      max_records: 2,
    });

    expect(rows).toEqual([]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
