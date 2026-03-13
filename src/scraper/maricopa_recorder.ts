import type { LienRecord } from '../types';
import { log } from '../utils/logger';

const MARICOPA_API_BASE = 'https://publicapi.recorder.maricopa.gov';
const MARICOPA_SEARCH_PAGE_SIZE = 20;
const MARICOPA_SEARCH_MAX_RESULTS = 500;
const MARICOPA_DEFAULT_MAX_RECORDS = Number(process.env.MARICOPA_MAX_RECORDS ?? '1000');
const MARICOPA_DOC_CODE = process.env.MARICOPA_DOCUMENT_CODE ?? 'FL';

export interface ScrapeOptions {
  date_start: string;
  date_end: string;
  max_records?: number;
  stop_requested?: () => boolean;
}

interface MaricopaSearchRow {
  names: string;
  recordingNumber: number | string;
  recordingSuffix: string;
  recordingDate: string;
  documentCode: string;
  docketBook: string;
  pageMap: string;
}

interface MaricopaSearchResponse {
  searchResults: MaricopaSearchRow[];
  totalResults: number;
}

export interface MaricopaDocumentDetail {
  names: string[];
  documentCodes: string[];
  recordingDate: string;
  recordingNumber: string;
  pageAmount: number;
  docketBook: number;
  pageMap: number;
  affidavitPresent: boolean;
  affidavitPageAmount: number;
  restricted: boolean;
}

export type MaricopaFailureKind =
  | 'challenge_blocked'
  | 'blocked_html'
  | 'http_error'
  | 'invalid_json';

export class MaricopaScrapeError extends Error {
  readonly kind: MaricopaFailureKind;
  readonly retryable: boolean;
  readonly url?: string;
  readonly status?: number;

  constructor(
    message: string,
    options: {
      kind: MaricopaFailureKind;
      retryable: boolean;
      url?: string;
      status?: number;
    },
  ) {
    super(message);
    this.name = 'MaricopaScrapeError';
    this.kind = options.kind;
    this.retryable = options.retryable;
    this.url = options.url;
    this.status = options.status;
  }
}

function parseMmDdYyyy(input: string): { month: string; day: string; year: string } {
  const match = input.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) {
    throw new Error(`Maricopa expects MM/DD/YYYY dates, received ${input}`);
  }

  return {
    month: match[1],
    day: match[2],
    year: match[3],
  };
}

export function toMaricopaIsoDate(input: string): string {
  const { month, day, year } = parseMmDdYyyy(input);
  return `${year}-${month}-${day}`;
}

export function normalizeMaricopaDate(input: string): string {
  const match = input.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (!match) return input;

  const [, month, day, year] = match;
  return `${month.padStart(2, '0')}/${day.padStart(2, '0')}/${year}`;
}

export function parseMaricopaIndexDate(input: string): string {
  const match = input.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) {
    throw new Error(`Unable to parse Maricopa searchable date ${input}`);
  }

  const [, month, day, year] = match;
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
}

function compareIsoDates(left: string, right: string): number {
  return left.localeCompare(right);
}

export function clampMaricopaDateRange(
  dateStartIso: string,
  dateEndIso: string,
  latestSearchableIso: string,
): { date_start_iso: string; date_end_iso: string; clamped: boolean; empty: boolean } {
  const effectiveEnd = compareIsoDates(dateEndIso, latestSearchableIso) > 0
    ? latestSearchableIso
    : dateEndIso;
  const empty = compareIsoDates(dateStartIso, effectiveEnd) > 0;
  return {
    date_start_iso: dateStartIso,
    date_end_iso: effectiveEnd,
    clamped: effectiveEnd !== dateEndIso,
    empty,
  };
}

export function buildMaricopaSearchUrl(dateStart: string, dateEnd: string, pageNumber: number): string {
  const url = new URL('/documents/search', MARICOPA_API_BASE);
  url.searchParams.set('businessNames', '');
  url.searchParams.set('firstNames', '');
  url.searchParams.set('lastNames', '');
  url.searchParams.set('middleNameIs', '');
  url.searchParams.set('documentTypeSelector', 'code');
  url.searchParams.set('documentCode', MARICOPA_DOC_CODE);
  url.searchParams.set('beginDate', dateStart);
  url.searchParams.set('endDate', dateEnd);
  url.searchParams.set('pageNumber', String(pageNumber));
  url.searchParams.set('pageSize', String(MARICOPA_SEARCH_PAGE_SIZE));
  url.searchParams.set('maxResults', String(MARICOPA_SEARCH_MAX_RESULTS));
  return url.toString();
}

export function isChallengeBody(body: string): boolean {
  return /security check|captcha|just a moment|cloudflare|enable javascript and cookies to continue/i.test(body);
}

function classifyHttpFailure(url: string, status: number, body: string): MaricopaScrapeError {
  if (isChallengeBody(body)) {
    return new MaricopaScrapeError(
      'Maricopa returned a challenge page. Refresh the browser session and retry.',
      {
        kind: 'challenge_blocked',
        retryable: true,
        url,
        status,
      },
    );
  }

  return new MaricopaScrapeError(
    `Maricopa API request failed (${status}) for ${url}`,
    {
      kind: 'http_error',
      retryable: status >= 500 || status === 429,
      url,
      status,
    },
  );
}

function classifyBodyFailure(url: string, body: string): MaricopaScrapeError | null {
  if (isChallengeBody(body)) {
    return new MaricopaScrapeError(
      'Maricopa returned a challenge page. Refresh the browser session and retry.',
      {
        kind: 'challenge_blocked',
        retryable: true,
        url,
      },
    );
  }

  if (/^\s*</.test(body)) {
    return new MaricopaScrapeError(
      'Maricopa returned HTML instead of JSON.',
      {
        kind: 'blocked_html',
        retryable: true,
        url,
      },
    );
  }

  return null;
}

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...init,
    headers: {
      accept: 'application/json',
      ...init?.headers,
    },
  });

  const raw = await response.text();
  if (!response.ok) {
    throw classifyHttpFailure(url, response.status, raw);
  }

  const bodyFailure = classifyBodyFailure(url, raw);
  if (bodyFailure) throw bodyFailure;

  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new MaricopaScrapeError(
      `Maricopa returned invalid JSON for ${url}`,
      {
        kind: 'invalid_json',
        retryable: true,
        url,
      },
    );
  }
}

async function fetchText(url: string, init?: RequestInit): Promise<string> {
  const response = await fetch(url, {
    ...init,
    headers: {
      accept: 'text/plain,application/json',
      ...init?.headers,
    },
  });

  const raw = await response.text();
  if (!response.ok) {
    throw classifyHttpFailure(url, response.status, raw);
  }

  const bodyFailure = classifyBodyFailure(url, raw);
  if (bodyFailure) throw bodyFailure;

  return raw.trim().replace(/^"+|"+$/g, '');
}

function normalizePartyNames(names: string[]): { debtorName: string; securedPartyName: string } {
  const cleaned = names.map((name) => name.trim()).filter(Boolean);
  const irsIndex = cleaned.findIndex((name) => /internal revenue service|\birs\b/i.test(name));
  const securedPartyName = irsIndex >= 0 ? cleaned[irsIndex] : cleaned[1] ?? 'Internal Revenue Service';
  const debtorName = cleaned.find((name, index) => index !== irsIndex) ?? cleaned[0] ?? '';

  return {
    debtorName,
    securedPartyName,
  };
}

function inferLeadType(documentCodes: string[]): 'Lien' | 'Release' {
  const joined = documentCodes.join(' ').toUpperCase();
  if (/RELEASE|WITHDRAWAL|NON ATTACHMENT|DISCHARGE/.test(joined)) return 'Release';
  return 'Lien';
}

function computeConfidenceScore(detail: MaricopaDocumentDetail): number {
  const { debtorName } = normalizePartyNames(detail.names);
  if (!debtorName) return 0.24;
  if (detail.names.length >= 2) return 0.42;
  return 0.34;
}

export function mapMaricopaDetailToLienRecord(detail: MaricopaDocumentDetail): LienRecord {
  const { debtorName, securedPartyName } = normalizePartyNames(detail.names);
  const leadType = inferLeadType(detail.documentCodes);

  return {
    state: 'AZ',
    source: 'maricopa_recorder',
    county: 'Maricopa',
    ucc_type: 'Federal Tax Lien',
    debtor_name: debtorName,
    debtor_address: '',
    file_number: detail.recordingNumber,
    secured_party_name: securedPartyName,
    secured_party_address: '',
    status: leadType === 'Release' ? 'Released' : 'Active',
    filing_date: normalizeMaricopaDate(detail.recordingDate),
    lapse_date: '12/31/9999',
    document_type: detail.documentCodes.join(', ') || 'FED TAX L',
    pdf_filename: '',
    processed: true,
    confidence_score: computeConfidenceScore(detail),
    lead_type: leadType,
  };
}

async function fetchDocumentDetail(recordingNumber: string): Promise<MaricopaDocumentDetail> {
  return fetchJson<MaricopaDocumentDetail>(`${MARICOPA_API_BASE}/documents/${recordingNumber}`);
}

async function fetchSearchPage(dateStartIso: string, dateEndIso: string, pageNumber: number): Promise<MaricopaSearchResponse> {
  return fetchJson<MaricopaSearchResponse>(buildMaricopaSearchUrl(dateStartIso, dateEndIso, pageNumber));
}

export async function fetchLatestMaricopaSearchableDate(): Promise<string> {
  const raw = await fetchText(`${MARICOPA_API_BASE}/documents/index`);
  return parseMaricopaIndexDate(raw);
}

export async function scrapeMaricopaRecorder(options: ScrapeOptions): Promise<LienRecord[]> {
  const dateStartIso = toMaricopaIsoDate(options.date_start);
  const dateEndIso = toMaricopaIsoDate(options.date_end);
  const latestSearchableIso = await fetchLatestMaricopaSearchableDate();
  const resolvedRange = clampMaricopaDateRange(dateStartIso, dateEndIso, latestSearchableIso);
  const maxRecords = Math.max(
    1,
    Math.min(options.max_records ?? MARICOPA_DEFAULT_MAX_RECORDS, MARICOPA_DEFAULT_MAX_RECORDS),
  );
  const allRows: MaricopaSearchRow[] = [];

  log({
    stage: 'scraper_start',
    site: 'maricopa_recorder',
    date_start: options.date_start,
    date_end: options.date_end,
    effective_date_end: resolvedRange.date_end_iso,
    latest_searchable_date: latestSearchableIso,
    max_records: maxRecords,
  });

  if (resolvedRange.clamped) {
    log({
      stage: 'maricopa_date_end_clamped',
      site: 'maricopa_recorder',
      requested_date_end: dateEndIso,
      effective_date_end: resolvedRange.date_end_iso,
      latest_searchable_date: latestSearchableIso,
    });
  }

  if (resolvedRange.empty) {
    log({
      stage: 'scraper_complete',
      site: 'maricopa_recorder',
      total_results: 0,
      records_scraped: 0,
      pages_visited: 0,
      reason: 'requested_range_after_latest_searchable_date',
    });
    return [];
  }

  let pageNumber = 1;
  let totalResults = 0;

  while (allRows.length < maxRecords) {
    if (options.stop_requested?.()) {
      log({
        stage: 'scraper_stop_requested',
        site: 'maricopa_recorder',
        page_number: pageNumber,
        records_collected: allRows.length,
      });
      break;
    }

    const page = await fetchSearchPage(
      resolvedRange.date_start_iso,
      resolvedRange.date_end_iso,
      pageNumber,
    );
    totalResults = page.totalResults;
    if (page.searchResults.length === 0) break;

    allRows.push(...page.searchResults);

    if (
      allRows.length >= maxRecords ||
      allRows.length >= totalResults ||
      page.searchResults.length < MARICOPA_SEARCH_PAGE_SIZE
    ) {
      break;
    }

    pageNumber += 1;
  }

  const limitedRows = allRows.slice(0, maxRecords);
  const records: LienRecord[] = [];

  for (const row of limitedRows) {
    if (options.stop_requested?.()) break;
    const detail = await fetchDocumentDetail(String(row.recordingNumber));
    records.push(mapMaricopaDetailToLienRecord(detail));
  }

  log({
    stage: 'scraper_complete',
    site: 'maricopa_recorder',
    total_results: totalResults,
    records_scraped: records.length,
    pages_visited: allRows.length === 0 ? 0 : pageNumber,
    latest_searchable_date: latestSearchableIso,
  });

  return records;
}
