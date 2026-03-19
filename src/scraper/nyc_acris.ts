import fs from 'fs/promises';
import path from 'path';
import Bottleneck from 'bottleneck';
import type { BrowserContext, Page } from 'playwright';
import { execFileSync } from 'child_process';
import {
  createIsolatedBrowserContext,
  type BrowserTransportMode,
  type TransportDiagnostic,
} from '../browser/transport';
import { extractAmountFromText, type AmountReason } from './amount-extraction';
import { checkOCRRuntime, getOCRBinaryCommands } from './ocr-runtime';
import type { NYCAcrisFailureClass, SiteConnectivityStatus } from '../scheduler/connectivity';
import { classifyNYCAcrisFailure } from '../scheduler/connectivity';
import { attachScrapeQualitySummary, type LienRecord, type ScrapeResult } from '../types';
import { log } from '../utils/logger';
import { redactSecret, sanitizeErrorMessage } from '../utils/redaction';

const BASE = 'https://a836-acris.nyc.gov';
const PATHS = {
  index: '/DS/DocumentSearch/Index',
  documentType: '/DS/DocumentSearch/DocumentType',
  detail: '/DS/DocumentSearch/DocumentDetail',
  result: '/DS/DocumentSearch/DocumentTypeResult',
  imageView: '/DS/DocumentSearch/DocumentImageView',
};

const SEARCH_PROFILE = {
  hid_doctype: process.env.ACRIS_HIDDOCTYPE || 'FL',
  hid_doctype_name: process.env.ACRIS_HIDDOCTYPENAME || 'FEDERAL LIEN-IRS',
  hid_selectdate: process.env.ACRIS_HIDSELECTDATE || '7',
  hid_borough: process.env.ACRIS_HIDBOROUGH || '0',
  hid_borough_name: process.env.ACRIS_HIDBOROUGHNAME || 'ALL BOROUGHS',
  hid_max_rows: process.env.ACRIS_HIDMAXROWS || '10',
  hid_SearchType: process.env.ACRIS_HIDSEARCHTYPE || 'DOCTYPE',
  hid_ISIntranet: process.env.ACRIS_HIDISINTRANET || 'N',
  hid_sort: process.env.ACRIS_HIDSORT || '',
} as const;

type SearchProfile = Record<keyof typeof SEARCH_PROFILE, string>;

const OUT_DIR = process.env.ACRIS_OUT_DIR || path.resolve(process.cwd(), 'out', 'acris');
const MAX_RESULT_PAGES = Number(process.env.ACRIS_MAX_RESULT_PAGES ?? '3');
const INITIAL_MAX_RESULT_PAGES = Number(process.env.ACRIS_INITIAL_MAX_RESULT_PAGES ?? '1');
const INITIAL_MAX_RECORDS = Number(process.env.ACRIS_INITIAL_MAX_RECORDS ?? '5');
const ENFORCE_INITIAL_CAP = process.env.ACRIS_ENFORCE_INITIAL_CAP !== '0';
const CHECKPOINT_DIR = path.join(OUT_DIR, 'checkpoints');
const NYC_ACRIS_ACTION_DELAY_MIN_MS = Number(process.env.NYC_ACRIS_ACTION_DELAY_MIN_MS ?? '2000');
const NYC_ACRIS_ACTION_DELAY_MAX_MS = Number(process.env.NYC_ACRIS_ACTION_DELAY_MAX_MS ?? '4000');
const NYC_ACRIS_DOC_DELAY_MIN_MS = Number(process.env.NYC_ACRIS_DOC_DELAY_MIN_MS ?? '8000');
const NYC_ACRIS_DOC_DELAY_MAX_MS = Number(process.env.NYC_ACRIS_DOC_DELAY_MAX_MS ?? '15000');
const NYC_ACRIS_PAGE_DELAY_MIN_MS = Number(process.env.NYC_ACRIS_PAGE_DELAY_MIN_MS ?? '15000');
const NYC_ACRIS_PAGE_DELAY_MAX_MS = Number(process.env.NYC_ACRIS_PAGE_DELAY_MAX_MS ?? '30000');
const NYC_ACRIS_SESSION_MAX_MINUTES = Number(process.env.NYC_ACRIS_SESSION_MAX_MINUTES ?? '20');
const NYC_ACRIS_IMAGE_VIEW_RETRIES = Number(process.env.NYC_ACRIS_IMAGE_VIEW_RETRIES ?? '2');
const NYC_ACRIS_OCR_MAX_PAGES = Number(process.env.NYC_ACRIS_OCR_MAX_PAGES ?? '2');
const NYC_ACRIS_PROBE_BOOTSTRAP_TIMEOUT_MS = Number(process.env.NYC_ACRIS_PROBE_BOOTSTRAP_TIMEOUT_MS ?? '120000');
const NYC_ACRIS_VALIDATION_BOOTSTRAP_TIMEOUT_MS = Number(process.env.NYC_ACRIS_VALIDATION_BOOTSTRAP_TIMEOUT_MS ?? '120000');
const NYC_ACRIS_VALIDATION_RESULT_PAGE_TIMEOUT_MS = Number(process.env.NYC_ACRIS_VALIDATION_RESULT_PAGE_TIMEOUT_MS ?? '120000');
const NYC_ACRIS_VALIDATION_EXTRACT_ROWS_TIMEOUT_MS = Number(process.env.NYC_ACRIS_VALIDATION_EXTRACT_ROWS_TIMEOUT_MS ?? '30000');
const NYC_ACRIS_VALIDATION_VIEWER_TIMEOUT_MS = Number(process.env.NYC_ACRIS_VALIDATION_VIEWER_TIMEOUT_MS ?? '180000');
const NYC_ACRIS_VALIDATION_RELOAD_TIMEOUT_MS = Number(process.env.NYC_ACRIS_VALIDATION_RELOAD_TIMEOUT_MS ?? '120000');
const NYC_ACRIS_BOOTSTRAP_NEW_PAGE_TIMEOUT_MS = Number(process.env.NYC_ACRIS_BOOTSTRAP_NEW_PAGE_TIMEOUT_MS ?? '30000');
const NYC_ACRIS_BOOTSTRAP_NAVIGATION_TIMEOUT_MS = Number(process.env.NYC_ACRIS_BOOTSTRAP_NAVIGATION_TIMEOUT_MS ?? '90000');
const nycAcrisLimiter = new Bottleneck({ maxConcurrent: 1 });

export interface ScrapeOptions {
  date_start: string;
  date_end: string;
  max_records?: number;
  stop_requested?: () => boolean;
  connectivity_status_at_start?: SiteConnectivityStatus;
}

export interface ValidationOptions {
  max_documents?: number;
  headed?: boolean;
  connectivity_status_at_start?: SiteConnectivityStatus;
  onStageEvent?: (event: NYCAcrisStageEvent) => void;
}

interface SearchState {
  pageNum: number;
  profile: SearchProfile;
  requestDateRange?: {
    start: string;
    end: string;
  };
}

interface ResultRowCandidate {
  docId: string;
  filingDate: string;
  debtorName: string;
  securedPartyName: string;
  documentType: string;
  rowText: string;
  cells: string[];
}

interface ViewerArtifact {
  docId: string;
  detailUrl?: string;
  imageViewUrl: string;
  viewerSrc: string | null;
  imageUrls: string[];
  title: string;
  totalPages?: number;
  filingDate?: string;
  recordedFiledAt?: string;
  detailDebtorName?: string;
  detailDebtorAddress?: string;
  detailSecuredPartyName?: string;
  detailSecuredPartyAddress?: string;
  amount?: string;
  amountConfidence?: number;
  amountReason?: AmountReason;
  leadType?: string;
  taxpayerName?: string;
  taxpayerAddress?: string;
}

interface NetworkEvent {
  method: string;
  url: string;
  resourceType: string;
}

interface ValidationStep {
  step: string;
  ok: boolean;
  detail?: string;
  startedAt?: string;
  finishedAt?: string;
  durationMs?: number;
  timeoutMs?: number;
}

export interface NYCAcrisStageEvent {
  step: string;
  status: 'started' | 'succeeded' | 'failed';
  at: string;
  detail?: string;
  durationMs?: number;
  timeoutMs?: number;
}

type NYCAcrisPageKind = 'index' | 'document_type' | 'detail' | 'results' | 'image_view';

interface PageReadyDiagnostic {
  step: string;
  attempt: number;
  kind: NYCAcrisPageKind;
  expectedPath: string;
  finalUrl: string;
  title: string;
  readyState: string;
  htmlLength: number;
  bodyTextLength: number;
  hasToken: boolean;
  hasShellMarker: boolean;
  hasResultMarker: boolean;
  hasViewerIframe: boolean;
  ok: boolean;
  reason: string;
}

export interface NYCAcrisCheckpointState {
  version: 1;
  pageNum: number;
  docIndex: number;
  docId?: string;
  updatedAt: string;
}

interface RunManifest {
  startedAt: string;
  finishedAt?: string;
  transportMode: BrowserTransportMode;
  resultPagesVisited: number;
  docIds: string[];
  documents: ViewerArtifact[];
  warnings: string[];
  failures: string[];
  network: NetworkEvent[];
  bootstrapTrace?: string[];
  navigationDiagnostics?: PageReadyDiagnostic[];
  transportDiagnostics?: TransportDiagnostic[];
  validationSteps?: ValidationStep[];
  failureClass?: NYCAcrisFailureClass;
  sessionDurationMs?: number;
  attemptedDocs?: number;
  completedDocs?: number;
  connectivityStatusAtStart?: SiteConnectivityStatus;
  checkpoint?: {
    pageNum: number;
    docIndex: number;
    docId?: string;
  };
  requestedDateStart?: string;
  requestedDateEnd?: string;
  discoveredCount?: number;
  returnedCount?: number;
  filteredOutCount?: number;
  returnedMinFilingDate?: string;
  returnedMaxFilingDate?: string;
  upstreamMinFilingDate?: string;
  upstreamMaxFilingDate?: string;
}

class NYCAcrisRangeIntegrityError extends Error {
  readonly requestedStart: string;
  readonly requestedEnd: string;
  readonly upstreamMin?: string;
  readonly upstreamMax?: string;
  readonly returnedRowCount: number;

  constructor(options: {
    requestedStart: string;
    requestedEnd: string;
    upstreamMin?: string;
    upstreamMax?: string;
    returnedRowCount: number;
  }) {
    super(
      `ACRIS returned ${options.returnedRowCount} rows outside requested range ${options.requestedStart}-${options.requestedEnd}` +
      ` upstream_range=${options.upstreamMin ?? 'unknown'}-${options.upstreamMax ?? 'unknown'}`
    );
    this.name = 'NYCAcrisRangeIntegrityError';
    this.requestedStart = options.requestedStart;
    this.requestedEnd = options.requestedEnd;
    this.upstreamMin = options.upstreamMin;
    this.upstreamMax = options.upstreamMax;
    this.returnedRowCount = options.returnedRowCount;
  }
}

interface ProbeResult {
  ok: boolean;
  detail?: string;
  transportMode: BrowserTransportMode;
  failureClass?: NYCAcrisFailureClass;
  diagnostic?: PageReadyDiagnostic;
  recoveryAction: ProbeRecoveryAction;
  bootstrapStrategy: ProbeBootstrapStrategy;
  steps?: ValidationStep[];
  bootstrapTrace?: string[];
  failures?: string[];
  warnings?: string[];
}

export interface ProbeOptions {
  headed?: boolean;
  onStageEvent?: (event: NYCAcrisStageEvent) => void;
}

interface OcrExtraction {
  amount?: string;
  amountConfidence?: number;
  amountReason: AmountReason;
  leadType?: string;
  taxpayerName?: string;
  taxpayerAddress?: string;
}

interface DetailExtraction {
  filingDate?: string;
  recordedFiledAt?: string;
  debtorName?: string;
  debtorAddress?: string;
  securedPartyName?: string;
  securedPartyAddress?: string;
  amount?: string;
}

interface AcrisDateParts {
  month: string;
  day: string;
  year: string;
}

type ProbeRecoveryAction = 'none' | 'retry_new_page' | 'retry_fresh_context';
type ProbeBootstrapStrategy = 'index_then_document_type' | 'direct_document_type';

interface BootstrapSessionResult {
  handle: Awaited<ReturnType<typeof createAcrisContext>>;
  page: Page;
  recoveryAction: ProbeRecoveryAction;
  bootstrapStrategy: ProbeBootstrapStrategy;
  diagnostic?: PageReadyDiagnostic;
}

function unique<T>(items: T[]): T[] {
  return [...new Set(items)];
}

function nowIso(): string {
  return new Date().toISOString();
}

class NYCAcrisStageTimeoutError extends Error {
  readonly step: string;
  readonly timeoutMs: number;

  constructor(step: string, timeoutMs: number, detail?: string) {
    super(detail ? `NYC ${step} timed out after ${timeoutMs}ms: ${detail}` : `NYC ${step} timed out after ${timeoutMs}ms`);
    this.name = 'NYCAcrisStageTimeoutError';
    this.step = step;
    this.timeoutMs = timeoutMs;
  }
}

function emitNYCAcrisStageEvent(
  onStageEvent: ValidationOptions['onStageEvent'] | ProbeOptions['onStageEvent'] | undefined,
  event: NYCAcrisStageEvent,
): void {
  onStageEvent?.(event);
}

export function resolveNYCAcrisResumeStartIndex(
  selectedRows: Pick<ResultRowCandidate, 'docId'>[],
  checkpoint?: Pick<NYCAcrisCheckpointState, 'docIndex' | 'docId'> | null,
): number {
  if (!checkpoint) return 0;

  if (checkpoint.docId) {
    const foundIndex = selectedRows.findIndex((row) => row.docId === checkpoint.docId);
    if (foundIndex < 0) {
      throw new Error(`Checkpoint docId ${checkpoint.docId} is not present in the current NYC ACRIS result set`);
    }

    const expectedNextIndex = foundIndex + 1;
    if (checkpoint.docIndex !== expectedNextIndex) {
      throw new Error(
        `Checkpoint index mismatch for NYC ACRIS docId ${checkpoint.docId}: checkpoint docIndex=${checkpoint.docIndex}, expected ${expectedNextIndex}`,
      );
    }

    return expectedNextIndex;
  }

  if (!Number.isFinite(checkpoint.docIndex)) return 0;
  return Math.min(Math.max(0, checkpoint.docIndex), selectedRows.length);
}

interface ProcessSelectedRowsOptions {
  selectedRows: ResultRowCandidate[];
  checkpoint?: NYCAcrisCheckpointState | null;
  pageNum: number;
  stopRequested?: () => boolean;
  extractArtifact: (row: ResultRowCandidate, index: number) => Promise<ViewerArtifact>;
  saveCheckpoint: (checkpoint: NYCAcrisCheckpointState) => Promise<void>;
  onWarning?: (warning: string) => void;
  onFailure?: (failure: string) => void;
  waitForDocDelay?: () => Promise<void>;
}

interface ProcessSelectedRowsResult {
  documents: ViewerArtifact[];
  stopped: boolean;
  attemptedDocs: number;
}

export async function processSelectedRows(options: ProcessSelectedRowsOptions): Promise<ProcessSelectedRowsResult> {
  const documents: ViewerArtifact[] = [];
  let stopped = false;
  let attemptedDocs = 0;
  const startIndex = resolveNYCAcrisResumeStartIndex(options.selectedRows, options.checkpoint);

  for (let index = startIndex; index < options.selectedRows.length; index++) {
    if (options.stopRequested?.()) {
      stopped = true;
      options.onWarning?.(`stop_requested before viewer fetch for ${options.selectedRows[index].docId}`);
      if (documents.length === 0 && !options.checkpoint?.docId && options.checkpoint?.docIndex !== 0) {
        await options.saveCheckpoint({
          version: 1,
          pageNum: options.pageNum,
          docIndex: index,
          updatedAt: nowIso(),
        });
      }
      break;
    }

    try {
      attemptedDocs = index + 1;
      const artifact = await options.extractArtifact(options.selectedRows[index], index);
      documents.push(artifact);
      await options.saveCheckpoint({
        version: 1,
        pageNum: options.pageNum,
        docIndex: index + 1,
        docId: options.selectedRows[index].docId,
        updatedAt: nowIso(),
      });

      if (index < options.selectedRows.length - 1) {
        await (options.waitForDocDelay ?? waitForDocDelay)();
      }
    } catch (err: unknown) {
      const failure = `doc ${options.selectedRows[index].docId}: ${sanitizeErrorMessage(err)}`;
      options.onFailure?.(failure);
      throw err;
    }
  }

  return { documents, stopped, attemptedDocs };
}

export function resolveNYCAcrisDelay(minMs: number, maxMs: number, randomValue = Math.random()): number {
  if (maxMs <= minMs) return minMs;
  return minMs + Math.round((maxMs - minMs) * randomValue);
}

export function isUnexpectedViewerPageUrl(url: string): boolean {
  return /^chrome-error:\/\//i.test(url) || /^about:error/i.test(url);
}

export function shouldRetryViewerOpen(
  diagnostic: Pick<PageReadyDiagnostic, 'finalUrl' | 'reason' | 'ok'>,
): boolean {
  if (diagnostic.ok) return false;
  return isUnexpectedViewerPageUrl(diagnostic.finalUrl) || diagnostic.reason === 'unexpected_url';
}

export function inspectNYCAcrisPageReadiness(
  html: string,
  kind: NYCAcrisPageKind,
): Pick<PageReadyDiagnostic, 'htmlLength' | 'bodyTextLength' | 'hasToken' | 'hasShellMarker' | 'hasResultMarker' | 'hasViewerIframe' | 'ok' | 'reason'> {
  const normalized = html.toLowerCase();
  const bodyText = normalizeText(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
  );
  const hasToken = /name=["'](?:__RequestVerificationToken|RequestVerificationToken)["']/i.test(html);
  const hasShellMarker =
    /document search/i.test(html) ||
    /new york web public inquiry/i.test(html) ||
    /\/ds\/scripts\/global\.js/i.test(normalized) ||
    /\/ds\/scripts\/login\.js/i.test(normalized) ||
    /\/ds\/scripts\/menu\.js/i.test(normalized);
  const hasResultMarker =
    /documenttyperesult/i.test(html) ||
    /documentimageview\?doc_id=\d{16}/i.test(html) ||
    /go_image\(["']\d{16}["']\)/i.test(html) ||
    /name=["']hid_page["']/i.test(html);
  const hasViewerIframe = /<iframe[^>]+name=["']mainframe["']/i.test(html);
  const htmlLength = html.length;
  const bodyTextLength = bodyText.length;

  if (kind === 'document_type') {
    return {
      htmlLength,
      bodyTextLength,
      hasToken,
      hasShellMarker,
      hasResultMarker,
      hasViewerIframe,
      ok: hasToken,
      reason: hasToken ? 'token_present' : 'missing_token',
    };
  }

  if (kind === 'results') {
    const ok = hasResultMarker && hasToken;
    return {
      htmlLength,
      bodyTextLength,
      hasToken,
      hasShellMarker,
      hasResultMarker,
      hasViewerIframe,
      ok,
      reason: ok ? 'result_markers_present' : 'missing_result_markers',
    };
  }

  if (kind === 'detail') {
    const ok =
      /detailed\s+document\s+information/i.test(html) &&
      /party\s*1/i.test(html) &&
      /doc\.\s*date/i.test(html);
    return {
      htmlLength,
      bodyTextLength,
      hasToken,
      hasShellMarker,
      hasResultMarker,
      hasViewerIframe,
      ok,
      reason: ok ? 'detail_markers_present' : 'missing_detail_markers',
    };
  }

  if (kind === 'image_view') {
    const ok = hasViewerIframe || /documentimageview/i.test(html);
    return {
      htmlLength,
      bodyTextLength,
      hasToken,
      hasShellMarker,
      hasResultMarker,
      hasViewerIframe,
      ok,
      reason: ok ? (hasViewerIframe ? 'viewer_iframe_present' : 'image_view_shell_present') : 'missing_viewer_iframe',
    };
  }

  if (hasShellMarker) {
    return {
      htmlLength,
      bodyTextLength,
      hasToken,
      hasShellMarker,
      hasResultMarker,
      hasViewerIframe,
      ok: true,
      reason: 'shell_marker_present',
    };
  }

  if (htmlLength >= 1000 || bodyTextLength >= 200) {
    return {
      htmlLength,
      bodyTextLength,
      hasToken,
      hasShellMarker,
      hasResultMarker,
      hasViewerIframe,
      ok: true,
      reason: 'sufficient_page_content',
    };
  }

  return {
    htmlLength,
    bodyTextLength,
    hasToken,
    hasShellMarker,
    hasResultMarker,
    hasViewerIframe,
    ok: false,
    reason: 'insufficient_page_content',
  };
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function getSessionBudgetMs(): number {
  return NYC_ACRIS_SESSION_MAX_MINUTES * 60 * 1000;
}

async function waitForActionDelay(): Promise<void> {
  await sleep(resolveNYCAcrisDelay(NYC_ACRIS_ACTION_DELAY_MIN_MS, NYC_ACRIS_ACTION_DELAY_MAX_MS));
}

async function waitForDocDelay(): Promise<void> {
  await sleep(resolveNYCAcrisDelay(NYC_ACRIS_DOC_DELAY_MIN_MS, NYC_ACRIS_DOC_DELAY_MAX_MS));
}

async function waitForNextPageDelay(): Promise<void> {
  await sleep(resolveNYCAcrisDelay(NYC_ACRIS_PAGE_DELAY_MIN_MS, NYC_ACRIS_PAGE_DELAY_MAX_MS));
}

function throwIfSessionBudgetExceeded(startedAtMs: number): void {
  if (Date.now() - startedAtMs > getSessionBudgetMs()) {
    throw new Error('session_budget_exceeded');
  }
}

function normalizeText(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

function parseAcrisDateParts(value: string): AcrisDateParts | null {
  const match = value.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) return null;
  const [, month, day, year] = match;
  const mm = month.padStart(2, '0');
  const dd = day.padStart(2, '0');
  const yyyy = year;
  const parsed = new Date(Date.UTC(Number(yyyy), Number(mm) - 1, Number(dd)));
  if (
    Number.isNaN(parsed.getTime()) ||
    parsed.getUTCFullYear() !== Number(yyyy) ||
    parsed.getUTCMonth() !== Number(mm) - 1 ||
    parsed.getUTCDate() !== Number(dd)
  ) {
    return null;
  }
  return { month: mm, day: dd, year: yyyy };
}

export function normalizeAcrisDate(value: string): string | undefined {
  const parsed = parseAcrisDateParts(value);
  if (!parsed) return undefined;
  return `${parsed.month}/${parsed.day}/${parsed.year}`;
}

function toAcrisDateKey(value: string): string | undefined {
  const parsed = parseAcrisDateParts(value);
  if (!parsed) return undefined;
  return `${parsed.year}-${parsed.month}-${parsed.day}`;
}

function summarizeDateRange(rows: ResultRowCandidate[]): { min?: string; max?: string } {
  const normalized = rows
    .map((row) => normalizeAcrisDate(row.filingDate))
    .filter((value): value is string => Boolean(value));
  if (normalized.length === 0) return {};
  const sorted = normalized
    .map((value) => ({ value, key: toAcrisDateKey(value)! }))
    .sort((left, right) => left.key.localeCompare(right.key));
  return {
    min: sorted[0]?.value,
    max: sorted[sorted.length - 1]?.value,
  };
}

export function filterRowsByAcrisDateRange(
  rows: ResultRowCandidate[],
  options: Pick<ScrapeOptions, 'date_start' | 'date_end'>,
): { rows: ResultRowCandidate[]; filteredOutCount: number; hadOutOfRangeRows: boolean } {
  const startKey = toAcrisDateKey(options.date_start);
  const endKey = toAcrisDateKey(options.date_end);
  if (!startKey || !endKey) {
    throw new Error(`Invalid ACRIS date range ${options.date_start} - ${options.date_end}`);
  }

  let filteredOutCount = 0;
  let hadOutOfRangeRows = false;
  const filteredRows = rows.filter((row) => {
    const rowKey = toAcrisDateKey(row.filingDate);
    if (!rowKey) {
      filteredOutCount += 1;
      hadOutOfRangeRows = true;
      return false;
    }
    const inRange = rowKey >= startKey && rowKey <= endKey;
    if (!inRange) {
      filteredOutCount += 1;
      hadOutOfRangeRows = true;
    }
    return inRange;
  });

  return { rows: filteredRows, filteredOutCount, hadOutOfRangeRows };
}

export function isCompletelyOutOfRangeAcrisResultSet(
  rows: ResultRowCandidate[],
  options: Pick<ScrapeOptions, 'date_start' | 'date_end'>,
): boolean {
  if (rows.length === 0) return false;
  const filtered = filterRowsByAcrisDateRange(rows, options);
  return filtered.rows.length === 0 && filtered.hadOutOfRangeRows;
}

function trimTrailingAddressNoiseAfterZip(value: string): string {
  const zipPattern = /\b\d{5}(?:-\d{4})?\b/g;
  let match: RegExpExecArray | null;
  let trimmed = value;

  while ((match = zipPattern.exec(value)) !== null) {
    const zipEnd = match.index + match[0].length;
    const suffix = value.slice(zipEnd);
    if (!suffix) {
      trimmed = value.slice(0, zipEnd);
      continue;
    }

    const normalizedSuffix = suffix.trim();
    if (!normalizedSuffix) {
      trimmed = value.slice(0, zipEnd);
      continue;
    }

    if (/^[._,;:|/\\()\-[\]\s]*\d{0,2}[A-Za-z]?$/.test(normalizedSuffix)) {
      trimmed = value.slice(0, zipEnd);
    }
  }

  return trimmed;
}

export function normalizeOcrAddress(value: string | undefined): string | undefined {
  if (!value) return undefined;

  const normalized = value
    .replace(/\r/g, '\n')
    .replace(/\b(?:total\s+amount\s+due|tax\s+period|important|kind\s+of\s+tax|serial\s+number|unpaid\s+balance)\b[\s\S]*$/i, ' ')
    .replace(/\bResidence\b[:\-\s]*/gi, ' ')
    .replace(/\b(Address|Taxpayer Address)\b[:\-\s]*/gi, ' ')
    .replace(/_+/g, ' ')
    .replace(/[|]+/g, ' ')
    .replace(/\n+/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/\s+([,.;:])/g, '$1')
    .trim()
    .replace(/([,.;:])(?=[A-Za-z])/g, '$1 ')
    .replace(/\s+,/g, ',')
    .replace(/,\s*,+/g, ',')
    .replace(/\s{2,}/g, ' ')
    .trim();

  const cleaned = trimTrailingAddressNoiseAfterZip(normalized)
    .replace(/\s+([,.;:])/g, '$1')
    .replace(/,\s*,+/g, ',')
    .replace(/[.;:,]+$/g, '')
    .trim();

  if (!cleaned) return undefined;
  if (!/\d/.test(cleaned)) return undefined;
  if (cleaned.length < 8) return undefined;
  if (/^(tax period|important|kind of tax)\b/i.test(cleaned)) return undefined;
  return cleaned;
}

function extractResidenceBlock(text: string): string | undefined {
  const lines = text
    .replace(/\r/g, '\n')
    .split('\n')
    .map((line) => line.replace(/[|]+/g, ' ').trim())
    .filter(Boolean);

  const stopPattern = /^(important|tax period|kind of tax|serial number|unpaid balance|place of filing|this notice was prepared|recording and endorsement cover page|document id:|fees and taxes|property data|cross reference data)\b/i;
  const cityStateZipPattern = /^[A-Z][A-Z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$/i;
  const cityStateZipNoCommaPattern = /^[A-Z][A-Z .'-]+\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?$/i;

  for (let index = 0; index < lines.length; index++) {
    const line = lines[index];
    const residenceMatch = line.match(/\b(?:Residence|Address|Taxpayer Address)\b[:\-\s]*(.*)$/i);
    if (!residenceMatch) continue;

    const collected: string[] = [];
    const remainder = residenceMatch[1]?.trim();
    if (remainder) {
      collected.push(remainder);
    }

    for (let nextIndex = index + 1; nextIndex < lines.length && collected.length < 3; nextIndex++) {
      const nextLine = lines[nextIndex];
      if (stopPattern.test(nextLine)) break;
      if (!/[A-Za-z]/.test(nextLine)) break;

      if (cityStateZipPattern.test(nextLine) || cityStateZipNoCommaPattern.test(nextLine)) {
        collected.push(nextLine);
        break;
      }

      if (/\d/.test(nextLine) || /\b(?:APT|UNIT|FL|FLOOR|SUITE|STE|PO BOX)\b/i.test(nextLine)) {
        collected.push(nextLine);
        continue;
      }

      break;
    }

    if (collected.length === 0) continue;

    const rawAddress =
      collected.length >= 2 && (cityStateZipPattern.test(collected[1]) || cityStateZipNoCommaPattern.test(collected[1]))
        ? `${collected[0]}, ${collected[1]}`
        : collected.join(' ');

    const normalized = normalizeOcrAddress(rawAddress);
    if (normalized) return normalized;
  }

  return undefined;
}

export function isPlausibleDebtorName(value: string): boolean {
  const normalized = normalizeText(value);
  if (!normalized) return false;
  if (/[:;]/.test(normalized)) return false;
  if (/\b\d{1,2}\/\d{1,2}\/\d{4}\b/.test(normalized)) return false;
  if (/\b\d{1,2}:\d{2}(?::\d{2})?\s*[AP]M\b/i.test(normalized)) return false;
  if (/(?:^|\s)\d{5,}(?:\s|$)/.test(normalized)) return false;
  if ((normalized.match(/\d/g) ?? []).length >= 4) return false;
  if (!/^[A-Za-z0-9&.,'()\-\/ ]+$/.test(normalized)) return false;

  const tokens = normalized
    .split(/\s+/)
    .map((token) => token.replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9]+$/g, ''))
    .filter(Boolean);

  if (tokens.length === 0) return false;

  const alphaTokens = tokens.filter((token) => /[A-Za-z]/.test(token));
  if (alphaTokens.length === 0) return false;

  const businessSuffixes = new Set([
    'LLC', 'INC', 'INC.', 'CORP', 'CORP.', 'CORPORATION', 'CO', 'CO.', 'COMPANY', 'LTD', 'LTD.', 'LP', 'LLP', 'LLLP',
    'PLLC', 'PC', 'P.C.', 'TRUST', 'HOLDINGS', 'GROUP', 'PARTNERS', 'PARTNERSHIP', 'VENTURES', 'ENTERPRISES',
    'ENTERPRISE', 'ASSOCIATES', 'PROPERTIES', 'REALTY', 'FUND', 'BANK',
  ]);
  const honorifics = new Set(['MR', 'MRS', 'MS', 'MISS', 'DR', 'JR', 'SR', 'II', 'III', 'IV']);
  const hasBusinessSuffix = alphaTokens.some((token) => businessSuffixes.has(token.toUpperCase()));
  const longAlphaTokens = alphaTokens.filter((token) => token.replace(/[^A-Za-z]/g, '').length >= 2);
  const hasPersonLikeShape =
    longAlphaTokens.length >= 2 &&
    alphaTokens.every((token) => token.length > 1 || /^[A-Z]$/i.test(token) || honorifics.has(token.toUpperCase()));

  return hasBusinessSuffix || hasPersonLikeShape;
}

function stripDebtorNoiseLabels(value: string): string {
  return value
    .replace(/\b(?:name of taxpayer|taxpayer name|recorded|filed|doc(?:ument)?\s+date|party\s*[12]|remarks)\b[:\-\s]*/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function sanitizeDebtorName(value: string): string {
  const normalized = stripDebtorNoiseLabels(normalizeText(value));
  if (!normalized) return '';
  if (/^(0|n\/a|null)$/i.test(normalized)) return '';
  if (!/[a-z]/i.test(normalized)) return '';
  if (/^\d{1,2}\/\d{1,2}\/\d{4}(?:\s+\d{1,2}:\d{2}:\d{2}\s*[AP]M)?$/i.test(normalized)) return '';
  if (/^\d{16}$/.test(normalized)) return '';
  if (/\d/.test(normalized) && normalized.length > 80) return '';
  if (normalized.length > 120) return '';
  if (/^(view|all boroughs|internal revenue service|irs)$/i.test(normalized)) {
    return '';
  }
  if (/^(?:last updated|updated)\b/i.test(normalized)) return '';
  return normalized;
}

function scoreDebtorNameConfidence(value: string): number {
  if (!value) return 0.2;
  if (isPlausibleDebtorName(value)) return 0.8;
  if (/[a-z]/i.test(value) && !/\b\d{1,2}:\d{2}(?::\d{2})?\s*[AP]M\b/i.test(value)) return 0.55;
  return 0.3;
}

function resolveRecordConfidenceScore(
  rowDebtorName: string,
  artifact?: Pick<ViewerArtifact, 'amountConfidence' | 'amountReason' | 'taxpayerName' | 'taxpayerAddress'>
): number | undefined {
  const candidates: number[] = [];

  if (typeof artifact?.amountConfidence === 'number') {
    candidates.push(artifact.amountConfidence);
  } else if (artifact?.amountReason === 'ok') {
    candidates.push(0.75);
  } else if (artifact?.amountReason === 'amount_low_confidence') {
    candidates.push(0.5);
  }

  if (artifact?.taxpayerName) {
    candidates.push(isPlausibleDebtorName(artifact.taxpayerName) ? 0.85 : 0.65);
  } else {
    candidates.push(scoreDebtorNameConfidence(rowDebtorName));
  }

  if (artifact?.taxpayerAddress) {
    candidates.push(/\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b/i.test(artifact.taxpayerAddress) ? 0.85 : 0.7);
  }

  if (candidates.length === 0) return undefined;
  return Math.max(...candidates);
}

export function buildSearchPayload(
  pageNum: number,
  profile: SearchProfile = { ...SEARCH_PROFILE },
  dateRange?: { start: string; end: string },
): Record<string, string> {
  const start = dateRange ? parseAcrisDateParts(dateRange.start) : null;
  const end = dateRange ? parseAcrisDateParts(dateRange.end) : null;
  return {
    ...profile,
    hid_selectdate: start && end ? 'DR' : profile.hid_selectdate,
    hid_datefromm: start?.month ?? '',
    hid_datefromd: start?.day ?? '',
    hid_datefromy: start?.year ?? '',
    hid_datetom: end?.month ?? '',
    hid_datetod: end?.day ?? '',
    hid_datetoy: end?.year ?? '',
    hid_page: String(pageNum),
    hid_ReqID: '',
  };
}

export function buildResultPageRequestFields(
  currentFields: Record<string, string>,
  token: string,
  pageNum: number,
  profile: SearchProfile = { ...SEARCH_PROFILE },
  dateRange?: { start: string; end: string },
): Record<string, string> {
  return {
    ...currentFields,
    __RequestVerificationToken: token,
    ...buildSearchPayload(pageNum, profile, dateRange),
  };
}

function formatDocIdDate(docId: string): string {
  const match = docId.match(/^(\d{4})(\d{2})(\d{2})/);
  if (!match) return '';
  return `${match[2]}/${match[3]}/${match[1]}`;
}

function getCheckpointKey(options: ScrapeOptions): string {
  return `${options.date_start}_${options.date_end}`.replace(/[^\w-]+/g, '_');
}

function getCheckpointPath(options: ScrapeOptions): string {
  return path.join(CHECKPOINT_DIR, `${getCheckpointKey(options)}.json`);
}

async function ensureDir(dir: string): Promise<void> {
  await fs.mkdir(dir, { recursive: true });
}

async function loadCheckpoint(options: ScrapeOptions): Promise<NYCAcrisCheckpointState | null> {
  try {
    const file = getCheckpointPath(options);
    const raw = await fs.readFile(file, 'utf8');
    const parsed = JSON.parse(raw) as NYCAcrisCheckpointState;
    if (parsed.version === 1) return parsed;
    return null;
  } catch {
    return null;
  }
}

async function saveCheckpoint(options: ScrapeOptions, checkpoint: NYCAcrisCheckpointState): Promise<void> {
  await ensureDir(CHECKPOINT_DIR);
  await fs.writeFile(getCheckpointPath(options), JSON.stringify(checkpoint, null, 2), 'utf8');
}

async function clearCheckpoint(options: ScrapeOptions): Promise<void> {
  await fs.rm(getCheckpointPath(options), { force: true });
}

async function writeManifest(manifest: RunManifest, suffix = `run-${Date.now()}.json`): Promise<string> {
  await ensureDir(OUT_DIR);
  const file = path.join(OUT_DIR, suffix);
  await fs.writeFile(file, JSON.stringify(manifest, null, 2), 'utf8');
  return file;
}

async function getToken(page: Page): Promise<string> {
  const locator = page.locator('input[name="__RequestVerificationToken"], input[name="RequestVerificationToken"]').first();
  await locator.waitFor({ state: 'attached', timeout: 30000 });
  const token = await locator.inputValue();
  if (!token) throw new Error('Missing anti-forgery token');
  return token;
}

function formatDiagnostic(diag: PageReadyDiagnostic): string {
  return JSON.stringify({
    step: diag.step,
    attempt: diag.attempt,
    kind: diag.kind,
    expectedPath: diag.expectedPath,
    finalUrl: diag.finalUrl,
    title: diag.title,
    readyState: diag.readyState,
    htmlLength: diag.htmlLength,
    bodyTextLength: diag.bodyTextLength,
    hasToken: diag.hasToken,
    hasShellMarker: diag.hasShellMarker,
    hasResultMarker: diag.hasResultMarker,
    hasViewerIframe: diag.hasViewerIframe,
    ok: diag.ok,
    reason: diag.reason,
  });
}

function formatTransportDiagnostic(diag: TransportDiagnostic): string {
  return JSON.stringify({
    stage: diag.stage,
    status: diag.status,
    transportMode: diag.transportMode,
    attempt: diag.attempt,
    at: diag.at,
    timeoutMs: diag.timeoutMs,
    durationMs: diag.durationMs,
    detail: diag.detail,
  });
}

function getLatestNavigationDiagnostic(manifest?: RunManifest): PageReadyDiagnostic | undefined {
  return manifest?.navigationDiagnostics?.[manifest.navigationDiagnostics.length - 1];
}

function getLatestTransportDiagnostic(manifest?: RunManifest): TransportDiagnostic | undefined {
  return manifest?.transportDiagnostics?.[manifest.transportDiagnostics.length - 1];
}

function getLatestValidationStep(
  manifest?: RunManifest,
  options?: { excludeStep?: string },
): ValidationStep | undefined {
  if (!manifest?.validationSteps?.length) return undefined;
  for (let index = manifest.validationSteps.length - 1; index >= 0; index--) {
    const step = manifest.validationSteps[index];
    if (options?.excludeStep && step.step === options.excludeStep) {
      continue;
    }
    return step;
  }
  return undefined;
}

function hasOnlyBootstrapPageCreationTrace(manifest?: RunManifest): boolean {
  const trace = manifest?.bootstrapTrace ?? [];
  return (
    trace.length > 0 &&
    trace.every(
      (entry) =>
        entry.startsWith('bootstrap_page_created url=about:blank') ||
        entry.startsWith('framenavigated url=about:blank'),
    )
  );
}

function isDeadBootstrapPageDiagnostic(diagnostic?: PageReadyDiagnostic): boolean {
  if (!diagnostic) return false;
  return (
    diagnostic.finalUrl === 'about:blank' &&
    diagnostic.reason === 'unexpected_url' &&
    diagnostic.readyState === 'unavailable' &&
    diagnostic.hasToken === false &&
    diagnostic.hasShellMarker === false &&
    diagnostic.hasResultMarker === false &&
    diagnostic.hasViewerIframe === false
  );
}

function isDeadBootstrapPage(manifest?: RunManifest, diagnostic?: PageReadyDiagnostic, message?: string): boolean {
  const traceOnlyBlankPageCreation = hasOnlyBootstrapPageCreationTrace(manifest);
  const blankTimeoutWithoutDiagnostic =
    !diagnostic &&
    traceOnlyBlankPageCreation &&
    /timed out after|page not ready|unexpected_url|about:blank/i.test(message ?? '');

  return (
    blankTimeoutWithoutDiagnostic ||
    (
      isDeadBootstrapPageDiagnostic(diagnostic) &&
      traceOnlyBlankPageCreation &&
      /timed out after|page not ready|unexpected_url|about:blank/i.test(message ?? '')
    )
  );
}

function describeRecentManifestContext(manifest?: RunManifest): string | undefined {
  const parts: string[] = [];
  const latestTransportDiagnostic = getLatestTransportDiagnostic(manifest);
  if (latestTransportDiagnostic) {
    parts.push(`latest_transport=${formatTransportDiagnostic(latestTransportDiagnostic)}`);
  }
  const latestValidationStep = getLatestValidationStep(manifest);
  if (latestValidationStep) {
    parts.push(
      `latest_step=${JSON.stringify({
        step: latestValidationStep.step,
        ok: latestValidationStep.ok,
        detail: latestValidationStep.detail,
        startedAt: latestValidationStep.startedAt,
        finishedAt: latestValidationStep.finishedAt,
        durationMs: latestValidationStep.durationMs,
        timeoutMs: latestValidationStep.timeoutMs,
      })}`,
    );
  }
  const latestDiagnostic = getLatestNavigationDiagnostic(manifest);
  if (latestDiagnostic) {
    parts.push(`latest_navigation=${formatDiagnostic(latestDiagnostic)}`);
  }
  const latestBootstrapTrace = manifest?.bootstrapTrace?.[manifest.bootstrapTrace.length - 1];
  if (latestBootstrapTrace) {
    parts.push(`latest_bootstrap_trace=${latestBootstrapTrace}`);
  }
  const latestFailure = manifest?.failures?.[manifest.failures.length - 1];
  if (latestFailure) {
    parts.push(`last_failure=${latestFailure}`);
  }
  const latestWarning = manifest?.warnings?.[manifest.warnings.length - 1];
  if (latestWarning) {
    parts.push(`last_warning=${latestWarning}`);
  }
  return parts.length > 0 ? parts.join(' ') : undefined;
}

async function runNYCAcrisStage<T>(
  manifest: RunManifest | undefined,
  options: {
    step: string;
    timeoutMs: number;
    onStageEvent?: (event: NYCAcrisStageEvent) => void;
    startDetail?: string;
  },
  run: () => Promise<T>,
  describeSuccess?: (result: T) => string | undefined,
): Promise<T> {
  const startedAt = nowIso();
  const startedAtMs = Date.now();
  const stepRecord: ValidationStep = {
    step: options.step,
    ok: false,
    detail: options.startDetail,
    startedAt,
    timeoutMs: options.timeoutMs,
  };
  manifest?.validationSteps?.push(stepRecord);
  emitNYCAcrisStageEvent(options.onStageEvent, {
    step: options.step,
    status: 'started',
    at: startedAt,
    detail: options.startDetail,
    timeoutMs: options.timeoutMs,
  });

  let timer: NodeJS.Timeout | undefined;

  try {
    const result = await Promise.race([
      run(),
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => {
          const parts = [describeRecentManifestContext(manifest)];
          const latestOtherStep = getLatestValidationStep(manifest, { excludeStep: options.step });
          if (latestOtherStep) {
            parts.unshift(
              `inner_step=${JSON.stringify({
                step: latestOtherStep.step,
                ok: latestOtherStep.ok,
                detail: latestOtherStep.detail,
                startedAt: latestOtherStep.startedAt,
                finishedAt: latestOtherStep.finishedAt,
                durationMs: latestOtherStep.durationMs,
                timeoutMs: latestOtherStep.timeoutMs,
              })}`,
            );
          }
          reject(new NYCAcrisStageTimeoutError(options.step, options.timeoutMs, parts.filter(Boolean).join(' ')));
        }, options.timeoutMs);
      }),
    ]);

    const durationMs = Date.now() - startedAtMs;
    const successDetail = describeSuccess?.(result);
    stepRecord.ok = true;
    stepRecord.finishedAt = nowIso();
    stepRecord.durationMs = durationMs;
    stepRecord.detail = successDetail ?? stepRecord.detail;
    emitNYCAcrisStageEvent(options.onStageEvent, {
      step: options.step,
      status: 'succeeded',
      at: stepRecord.finishedAt,
      detail: successDetail,
      durationMs,
      timeoutMs: options.timeoutMs,
    });
    return result;
  } catch (err: unknown) {
    const durationMs = Date.now() - startedAtMs;
    const detail = sanitizeErrorMessage(err);
    stepRecord.ok = false;
    stepRecord.finishedAt = nowIso();
    stepRecord.durationMs = durationMs;
    stepRecord.detail = detail;
    emitNYCAcrisStageEvent(options.onStageEvent, {
      step: options.step,
      status: 'failed',
      at: stepRecord.finishedAt,
      detail,
      durationMs,
      timeoutMs: options.timeoutMs,
    });
    throw err;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function isClosedBrowserLifecycleError(message?: string): boolean {
  return /target page, context or browser has been closed|browser has been closed|context has been closed|page has been closed/i.test(message ?? '');
}

function shouldRetryBootstrapDiagnostic(diagnostic?: PageReadyDiagnostic, message?: string): boolean {
  if (!diagnostic) {
    return /about:blank|unexpected_url|page not ready|net::err|chrome-error/i.test(message ?? '') ||
      isClosedBrowserLifecycleError(message);
  }

  const blankBootstrap =
    diagnostic.reason === 'unexpected_url' &&
    (!diagnostic.finalUrl || diagnostic.finalUrl === 'about:blank' || diagnostic.finalUrl.startsWith('chrome-error://'));
  const emptyShell =
    !diagnostic.hasShellMarker &&
    !diagnostic.hasToken &&
    !diagnostic.hasResultMarker &&
    !diagnostic.hasViewerIframe &&
    diagnostic.bodyTextLength === 0;

  return blankBootstrap || emptyShell || diagnostic.readyState === 'unavailable';
}

function summarizeProbeDiagnostic(diagnostic?: PageReadyDiagnostic) {
  return {
    probe_step: diagnostic?.step,
    probe_attempt: diagnostic?.attempt,
    final_url: diagnostic?.finalUrl,
    ready_state: diagnostic?.readyState,
    has_shell_marker: diagnostic?.hasShellMarker,
    has_result_marker: diagnostic?.hasResultMarker,
    has_viewer_iframe: diagnostic?.hasViewerIframe,
  };
}

function inferBootstrapStrategyFromDiagnostic(diagnostic?: PageReadyDiagnostic): ProbeBootstrapStrategy {
  if (diagnostic?.step === 'load_document_type_page_direct' || diagnostic?.step === 'probe_document_type_page_direct') {
    return 'direct_document_type';
  }
  return 'index_then_document_type';
}

function inferBootstrapRecoveryAction(manifest: RunManifest): ProbeRecoveryAction {
  const warnings = manifest.warnings.join(' ');
  if (/bootstrap_recovery .*recovery=retry_fresh_context|bootstrap_recovery retry_fresh_context/.test(warnings)) {
    return 'retry_fresh_context';
  }
  if (/bootstrap_recovery .*recovery=retry_new_page|bootstrap_recovery retry_new_page/.test(warnings)) {
    return 'retry_new_page';
  }
  return 'none';
}

function buildBootstrapAttemptLabel(strategy: ProbeBootstrapStrategy, recoveryAction: ProbeRecoveryAction): string {
  return `strategy=${strategy} recovery=${recoveryAction}`;
}

export function summarizeAmountReasonCounts(rows: Pick<LienRecord, 'amount_reason'>[]): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const row of rows) {
    const key = row.amount_reason ?? 'unknown';
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return counts;
}

function parseViewerTotalPages(viewerSrc: string | null): number {
  if (!viewerSrc) return 1;

  try {
    const resolved = new URL(viewerSrc, BASE);
    const raw = resolved.searchParams.get('searchCriteriaStringValue');
    if (!raw) return 1;
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const total = Number(parsed.hid_TotalPages ?? 1);
    return Number.isFinite(total) && total > 0 ? total : 1;
  } catch {
    return 1;
  }
}

async function collectPageReadyDiagnostic(
  page: Page,
  step: string,
  kind: NYCAcrisPageKind,
  expectedPath: string,
  attempt: number,
): Promise<PageReadyDiagnostic> {
  const html = await page.content().catch(() => '');
  const title = await page.title().catch(() => '');
  const readyState = await page.evaluate(() => document.readyState).catch(() => 'unavailable');
  const readiness = inspectNYCAcrisPageReadiness(html, kind);
  const finalUrl = page.url();
  const pathReached = finalUrl.toLowerCase().includes(expectedPath.toLowerCase());

  return {
    step,
    attempt,
    kind,
    expectedPath,
    finalUrl,
    title,
    readyState,
    htmlLength: readiness.htmlLength,
    bodyTextLength: readiness.bodyTextLength,
    hasToken: readiness.hasToken,
    hasShellMarker: readiness.hasShellMarker,
    hasResultMarker: readiness.hasResultMarker,
    hasViewerIframe: readiness.hasViewerIframe,
    ok: pathReached && readiness.ok,
    reason: pathReached ? readiness.reason : 'unexpected_url',
  };
}

async function submitHiddenPostForm(page: Page, actionUrl: string, fields: Record<string, string>): Promise<void> {
  await page.evaluate(
    ({ actionUrl, fields }) => {
      const form = document.createElement('form');
      form.method = 'POST';
      form.action = actionUrl;
      form.style.display = 'none';

      for (const [name, value] of Object.entries(fields)) {
        const input = document.createElement('input');
        input.type = 'hidden';
        input.name = name;
        input.value = value ?? '';
        form.appendChild(input);
      }

      document.body.appendChild(form);
      form.submit();
    },
    { actionUrl, fields }
  );
}

async function fetchBinaryFromPage(page: Page, url: string): Promise<Buffer | null> {
  const b64 = await page.evaluate(async (resourceUrl: string) => {
    try {
      const response = await fetch(resourceUrl, { credentials: 'include' });
      if (!response.ok) return '';
      const buffer = await response.arrayBuffer();
      const bytes = new Uint8Array(buffer);
      let binary = '';
      for (let index = 0; index < bytes.length; index++) {
        binary += String.fromCharCode(bytes[index]);
      }
      return btoa(binary);
    } catch {
      return '';
    }
  }, url).catch(() => '');

  if (!b64) return null;
  return Buffer.from(b64, 'base64');
}

async function ocrImageFile(imagePath: string): Promise<string> {
  const outputBase = imagePath.replace(/\.[^.]+$/, '_ocr');
  const { tesseract } = getOCRBinaryCommands();
  try {
    execFileSync(tesseract, [imagePath, outputBase, '--psm', '6'], { stdio: 'ignore', timeout: 30000 });
    const textPath = `${outputBase}.txt`;
    const text = await fs.readFile(textPath, 'utf8').catch(() => '');
    await fs.rm(textPath, { force: true }).catch(() => null);
    return text;
  } catch {
    await fs.rm(`${outputBase}.txt`, { force: true }).catch(() => null);
    return '';
  }
}

export function extractNYCAcrisFieldsFromText(text: string): Pick<OcrExtraction, 'leadType' | 'taxpayerName' | 'taxpayerAddress'> {
  let leadType: string | undefined;
  if (/certificate\s+of\s+release\s+of\s+federal/i.test(text) || /form\s+668\s*\(?\s*z\s*\)?/i.test(text)) {
    leadType = 'Release';
  } else if (/notice\s+of\s+federal\s+tax\s+li/i.test(text) || /form\s+668\s*\(?\s*y\s*\)?/i.test(text)) {
    leadType = 'Lien';
  }

  const taxpayerMatch =
    text.match(/name\s+of\s+taxpayer\s+(.+?)(?:\n|residence|address)/is) ??
    text.match(/taxpayer\s+name\s*[:\-]?\s*(.+?)(?:\n|residence|address)/is);
  const taxpayerName = taxpayerMatch?.[1]?.replace(/\s+/g, ' ').trim();

  const taxpayerAddress = extractResidenceBlock(text);

  return {
    leadType,
    taxpayerName: taxpayerName || undefined,
    taxpayerAddress,
  };
}

export function chooseBetterDebtorName(currentName: string, candidateName: string | undefined): string {
  if (!candidateName) return sanitizeDebtorName(currentName);
  const current = sanitizeDebtorName(currentName);
  const candidate = sanitizeDebtorName(candidateName);
  if (!candidate) return current;
  if (!current) return candidate;
  if (!isPlausibleDebtorName(current) && isPlausibleDebtorName(candidate)) return candidate;
  if (isPlausibleDebtorName(current) && !isPlausibleDebtorName(candidate)) return current;
  if (candidate.length >= current.length + 6 && /\s/.test(candidate)) return candidate;
  return current;
}

function joinAddressParts(parts: Array<string | undefined>): string | undefined {
  const [address1, address2, city, state, zip] = parts.map((part) => normalizeText(part ?? ''));
  const streetParts = [address1, address2].filter(Boolean);
  const locality = [city, state].filter(Boolean).join(', ');
  const tail = [locality, zip].filter(Boolean).join(' ');
  const combined = [...streetParts, tail].filter(Boolean);
  if (combined.length === 0) return undefined;
  return normalizeOcrAddress(combined.join(', ')) ?? normalizeOcrAddress(combined.join(' '));
}

function normalizeCurrencyToWholeDollars(raw: string | undefined): string | undefined {
  if (!raw) return undefined;
  const numeric = Number.parseFloat(raw.replace(/[$,\s]/g, ''));
  if (!Number.isFinite(numeric)) return undefined;
  return String(Math.trunc(numeric));
}

function normalizePartyNameFromDetail(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const normalized = sanitizeDebtorName(value.replace(/\s+/g, ' ').trim());
  return normalized || undefined;
}

function normalizeFreeformPartyName(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const normalized = normalizeText(value);
  return normalized || undefined;
}

export function extractNYCAcrisDetailFromHtml(html: string): DetailExtraction {
  const readLabel = (labelPattern: RegExp): string | undefined => {
    const match = html.match(labelPattern);
    const raw = match?.[1]
      ?.replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    return raw || undefined;
  };

  const readParty = (label: string, normalizeName: (value: string | undefined) => string | undefined) => {
    const scrollBlockMatch = html.match(
      new RegExp(`${label}[\\s\\S]*?<div[^>]+overflow:\\s*scroll[^>]*>([\\s\\S]*?)<\\/div>`, 'i')
    );
    const fallbackTableMatch = html.match(
      new RegExp(`${label}[\\s\\S]*?<table[^>]*>([\\s\\S]*?)<\\/table>`, 'i')
    );
    const block = scrollBlockMatch?.[1] ?? fallbackTableMatch?.[1] ?? '';
    const rows = [...block.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)].map((match) => match[1]);
    const cells =
      rows
        .map((row) =>
          [...row.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)]
            .map((match) =>
              normalizeText(
                match[1]
                  .replace(/<[^>]+>/g, ' ')
                  .replace(/&nbsp;/gi, ' ')
              )
            )
        )
        .filter((rowCells) => rowCells.some((cell) => cell && !/^(?:NAME|ADDRESS 1|ADDRESS 2|CITY|STATE|ZIP|COUNTRY)$/i.test(cell)))
        .sort((left, right) => right.length - left.length)[0] ?? [];

    if (cells.length < 5) {
      return { name: undefined, address: undefined };
    }

    const [name, address1, address2, city, state, zip] = cells;
    return {
      name: normalizeName(name),
      address: joinAddressParts([address1, address2, city, state, zip]),
    };
  };

  const party1 = readParty('PARTY\\s*1', normalizePartyNameFromDetail);
  const party2 = readParty('PARTY\\s*2', normalizeFreeformPartyName);

  return {
    filingDate: readLabel(/DOC\.\s*DATE:\s*<\/td>\s*<td[^>]*>\s*([\s\S]*?)\s*<\/td>/i),
    recordedFiledAt: readLabel(/RECORDED\s*\/\s*FILED:\s*<\/td>\s*<td[^>]*>\s*([\s\S]*?)\s*<\/td>/i),
    debtorName: party1.name,
    debtorAddress: party1.address,
    securedPartyName: party2.name,
    securedPartyAddress: party2.address,
    amount: normalizeCurrencyToWholeDollars(
      readLabel(/DOC\.\s*AMOUNT:\s*<\/td>\s*<td[^>]*>\s*([\s\S]*?)\s*<\/td>/i)
    ),
  };
}

async function extractOcrFromViewer(page: Page, artifact: ViewerArtifact, manifest?: RunManifest): Promise<OcrExtraction> {
  const runtime = checkOCRRuntime();
  if (!runtime.ok) {
    return { amountReason: 'ocr_missing' };
  }

  const totalPages = Math.max(1, Math.min(artifact.totalPages ?? 1, NYC_ACRIS_OCR_MAX_PAGES));
  const ocrDir = path.join(OUT_DIR, 'ocr');
  await ensureDir(ocrDir);

  let fullText = '';
  for (let pageNum = 1; pageNum <= totalPages; pageNum++) {
    const imageUrl = `${BASE}/DS/DocumentSearch/GetImage?doc_id=${artifact.docId}&page=${pageNum}`;
    const buffer = await fetchBinaryFromPage(page, imageUrl);
    if (!buffer || buffer.length < 50) {
      manifest?.warnings.push(`viewer_ocr_image_missing ${artifact.docId} page=${pageNum}`);
      continue;
    }

    const imagePath = path.join(ocrDir, `${artifact.docId}_page${pageNum}.png`);
    await fs.writeFile(imagePath, buffer);
    const text = await ocrImageFile(imagePath);
    await fs.rm(imagePath, { force: true }).catch(() => null);
    if (text.trim()) {
      fullText += `${text}\n`;
    }
  }

  if (!fullText.trim()) {
    return { amountReason: 'ocr_no_text' };
  }

  const amountResult = extractAmountFromText(fullText, Number(process.env.AMOUNT_MIN_CONFIDENCE ?? '0.75'));
  const fields = extractNYCAcrisFieldsFromText(fullText);
  return {
    amount: amountResult.amount,
    amountConfidence: amountResult.confidence,
    amountReason: amountResult.reason,
    leadType: fields.leadType,
    taxpayerName: fields.taxpayerName,
    taxpayerAddress: fields.taxpayerAddress,
  };
}

async function submitHiddenPostUntilReady(
  page: Page,
  manifest: RunManifest | undefined,
  options: {
    actionUrl: string;
    fields: Record<string, string>;
    expectedPath: string;
    step: string;
    kind: NYCAcrisPageKind;
    readyTimeoutMs?: number;
  },
): Promise<PageReadyDiagnostic> {
  const readyTimeoutMs = options.readyTimeoutMs ?? 45000;
  await submitHiddenPostForm(page, options.actionUrl, options.fields);

  const deadline = Date.now() + readyTimeoutMs;
  let lastDiagnostic: PageReadyDiagnostic | null = null;
  while (Date.now() <= deadline) {
    lastDiagnostic = await collectPageReadyDiagnostic(page, options.step, options.kind, options.expectedPath, 1);
    manifest?.navigationDiagnostics?.push(lastDiagnostic);
    if (lastDiagnostic.ok) {
      return lastDiagnostic;
    }
    await sleep(750);
  }

  if (lastDiagnostic) {
    throw new Error(`NYC ${options.step} page not ready: ${formatDiagnostic(lastDiagnostic)}`);
  }

  throw new Error(`NYC ${options.step} page not ready after submit`);
}

async function submitHiddenPostToResults(
  page: Page,
  manifest: RunManifest | undefined,
  actionUrl: string,
  fields: Record<string, string>,
  step: string,
): Promise<PageReadyDiagnostic> {
  return submitHiddenPostUntilReady(page, manifest, {
    actionUrl,
    fields,
    expectedPath: PATHS.result,
    step,
    kind: 'results',
  });
}

async function submitHiddenPostToDetail(
  page: Page,
  manifest: RunManifest | undefined,
  actionUrl: string,
  fields: Record<string, string>,
  step: string,
): Promise<PageReadyDiagnostic> {
  return submitHiddenPostUntilReady(page, manifest, {
    actionUrl,
    fields,
    expectedPath: PATHS.detail,
    step,
    kind: 'detail',
  });
}

async function submitHiddenPostToImageView(
  page: Page,
  manifest: RunManifest | undefined,
  actionUrl: string,
  fields: Record<string, string>,
  step: string,
): Promise<PageReadyDiagnostic> {
  return submitHiddenPostUntilReady(page, manifest, {
    actionUrl,
    fields,
    expectedPath: PATHS.imageView,
    step,
    kind: 'image_view',
  });
}

async function recoverResultsSession(page: Page, state: SearchState, manifest?: RunManifest): Promise<void> {
  await initializeSearchSession(page, state, manifest);
  await loadResultPage(page, state.pageNum || 1, state, manifest);
}

async function resubmitFirstResultPageFromCanonicalSearch(
  page: Page,
  state: SearchState,
  manifest?: RunManifest,
): Promise<void> {
  state.pageNum = 1;
  await initializeSearchSession(page, state, manifest);
  await loadResultPage(page, 1, state, manifest);
}

async function gotoPageUntilReady(
  page: Page,
  manifest: RunManifest | undefined,
  options: {
    url: string;
    expectedPath: string;
    step: string;
    kind: NYCAcrisPageKind;
    attempts?: number;
    gotoTimeoutMs?: number;
    readyTimeoutMs?: number;
  },
): Promise<PageReadyDiagnostic> {
  const attempts = options.attempts ?? 2;
  const gotoTimeoutMs = options.gotoTimeoutMs ?? 30000;
  const readyTimeoutMs = options.readyTimeoutMs ?? 15000;
  let lastDiagnostic: PageReadyDiagnostic | null = null;
  let lastError: unknown;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    let gotoWatchdog: NodeJS.Timeout | undefined;
    let gotoWatchdogFired = false;
    try {
      const gotoPromise = page.goto(options.url, { waitUntil: 'commit', timeout: gotoTimeoutMs });
      const watchdogPromise = new Promise<never>((_, reject) => {
        gotoWatchdog = setTimeout(() => {
          gotoWatchdogFired = true;
          manifest?.warnings.push(`goto_watchdog_fired step=${options.step} attempt=${attempt} url=${options.url}`);
          page.close({ runBeforeUnload: false }).catch(() => {});
          reject(new Error(`NYC ${options.step} goto watchdog fired after ${gotoTimeoutMs}ms url=${options.url}`));
        }, gotoTimeoutMs);
      });
      await Promise.race([gotoPromise, watchdogPromise]);
    } catch (err: unknown) {
      lastError = err;
      if (!gotoWatchdogFired && err instanceof Error && /page\.goto: Timeout/i.test(err.message)) {
        lastError = new Error(`NYC ${options.step} goto timed out after ${gotoTimeoutMs}ms url=${options.url}`);
      }
    } finally {
      if (gotoWatchdog) {
        clearTimeout(gotoWatchdog);
      }
    }

    if (gotoWatchdogFired) {
      throw lastError instanceof Error ? lastError : new Error(String(lastError ?? `Failed to load ${options.url}`));
    }

    const deadline = Date.now() + readyTimeoutMs;
    while (Date.now() <= deadline) {
      lastDiagnostic = await collectPageReadyDiagnostic(page, options.step, options.kind, options.expectedPath, attempt);
      manifest?.navigationDiagnostics?.push(lastDiagnostic);
      if (lastDiagnostic.ok) {
        return lastDiagnostic;
      }
      await sleep(750);
    }
  }

  if (lastDiagnostic) {
    throw new Error(`NYC ${options.step} page not ready: ${formatDiagnostic(lastDiagnostic)}`);
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError ?? `Failed to load ${options.url}`));
}

async function gotoIndexPage(page: Page, manifest?: RunManifest): Promise<void> {
  await gotoPageUntilReady(page, manifest, {
    url: `${BASE}${PATHS.index}`,
    expectedPath: PATHS.index,
    step: 'load_index_page',
    kind: 'index',
    attempts: 1,
  });
}

async function gotoDocumentTypePage(page: Page, manifest?: RunManifest): Promise<void> {
  await gotoPageUntilReady(page, manifest, {
    url: `${BASE}${PATHS.documentType}`,
    expectedPath: PATHS.documentType,
    step: 'load_document_type_page',
    kind: 'document_type',
    attempts: 1,
  });
}

async function gotoDocumentTypeDirect(page: Page, manifest?: RunManifest): Promise<void> {
  await gotoPageUntilReady(page, manifest, {
    url: `${BASE}${PATHS.documentType}`,
    expectedPath: PATHS.documentType,
    step: 'load_document_type_page_direct',
    kind: 'document_type',
    attempts: 1,
  });
}

async function loadResultPage(page: Page, pageNum: number, state: SearchState, manifest?: RunManifest): Promise<void> {
  if (pageNum > 1) {
    await waitForNextPageDelay();
  } else {
    await waitForActionDelay();
  }
  const token = await getToken(page);
  const currentFields = await collectHiddenFields(page).catch(() => ({}));
  const profile = state.profile;
  await submitHiddenPostToResults(page, manifest, `${BASE}${PATHS.result}`, {
    ...buildResultPageRequestFields(currentFields, token, pageNum, profile, state.requestDateRange),
  }, `submit_result_page_${pageNum}`);
  state.pageNum = pageNum;
}

async function collectHiddenFields(page: Page): Promise<Record<string, string>> {
  return page.evaluate(() => {
    const inputs = Array.from(document.querySelectorAll<HTMLInputElement>('form input[type="hidden"]'));
    return Object.fromEntries(
      inputs
        .map((input) => [input.name, input.value ?? ''])
        .filter(([name]) => Boolean(name))
    );
  });
}

function selectAuthoritativeCell(cells: string[], predicates: Array<(value: string) => boolean>): string {
  for (const predicate of predicates) {
    const match = cells.find((value) => predicate(value));
    if (match) return match;
  }
  return '';
}

export function extractDocIdsFromResultsHtml(html: string): string[] {
  return unique(
    [
      ...[...html.matchAll(/DocumentImageView\?doc_id=(\d{16})/gi)].map((match) => match[1]),
      ...[...html.matchAll(/go_image\(["'](\d{16})["']\)/gi)].map((match) => match[1]),
      ...[...html.matchAll(/go_image\((?:&quot;|&#34;)(\d{16})(?:&quot;|&#34;)\)/gi)].map((match) => match[1]),
    ]
      .filter((docId) => /^\d{16}$/.test(docId))
  );
}

export function extractViewerArtifactFromHtml(html: string): Pick<ViewerArtifact, 'viewerSrc' | 'imageUrls' | 'title'> {
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  const viewerMatch = html.match(/<iframe[^>]+name=["']mainframe["'][^>]+src=["']([^"']+)["']/i);
  const imageUrls = unique(
    [...html.matchAll(/<img[^>]+src=["']([^"']+)["']/gi)].map((match) => match[1]).filter(Boolean)
  );

  return {
    title: titleMatch?.[1]?.trim() ?? '',
    viewerSrc: viewerMatch?.[1] ?? null,
    imageUrls,
  };
}

async function extractResultRows(page: Page): Promise<ResultRowCandidate[]> {
  const html = await page.content();
  const fallbackIds = extractDocIdsFromResultsHtml(html);

  const rows = await page.evaluate(() => {
    const rowNodes = Array.from(document.querySelectorAll<HTMLTableRowElement>('tr'));
    return rowNodes
      .map((row) => {
        const imageButton = row.querySelector<HTMLInputElement>('input[title="ViewImage"], input[name="IMG"]');
        const anchor = row.querySelector<HTMLAnchorElement>('a[href*="DocumentImageView?doc_id="]');
        const onclick = imageButton?.getAttribute('onclick') ?? '';
        const href = anchor?.getAttribute('href') ?? '';
        const docId = onclick.match(/go_image\("(\d{16})"\)|go_image\('(\d{16})'\)/i)?.[1]
          ?? onclick.match(/go_image\("(\d{16})"\)|go_image\('(\d{16})'\)/i)?.[2]
          ?? href.match(/doc_id=(\d{16})/i)?.[1]
          ?? '';
        const cells = Array.from(row.querySelectorAll('td,th'))
          .map((node) => (node.textContent ?? '').trim())
          .filter(Boolean);
        const rowText = (row.textContent ?? imageButton?.textContent ?? anchor?.textContent ?? '').trim();

        return { docId, cells, rowText };
      })
      .filter((row) => Boolean(row.docId));
  });

  const fallbackRows = fallbackIds.map((docId) => {
    const rowTextMatch = html.match(new RegExp(`go_image\\(["']${docId}["']\\)[\\s\\S]*?<\\/tr>`, 'i'));
    const rowText = rowTextMatch?.[0] ?? '';

    return {
      docId,
      cells: [] as string[],
      rowText,
    };
  });

  const mapped = [...rows, ...fallbackRows]
    .filter((row) => /^\d{16}$/.test(row.docId))
    .map((row) => {
      const normalizedCells = row.cells.map(normalizeText).filter(Boolean);
      const filingDate = selectAuthoritativeCell(normalizedCells, [(value) => /^\d{2}\/\d{2}\/\d{4}$/.test(value)]) || formatDocIdDate(row.docId);
      const documentType = selectAuthoritativeCell(normalizedCells, [(value) => /federal lien-irs/i.test(value)]) || 'FEDERAL LIEN-IRS';
      const securedPartyName = selectAuthoritativeCell(normalizedCells, [
        (value) => /internal revenue service/i.test(value),
        (value) => /^irs$/i.test(value),
      ]) || 'Internal Revenue Service';
      const debtorName = selectAuthoritativeCell(normalizedCells, [
        (value) => /[a-z]/i.test(value) &&
          !/^\d{16}$/.test(value) &&
          !/^\d{2}\/\d{2}\/\d{4}$/.test(value) &&
          !/federal lien-irs/i.test(value) &&
          !/internal revenue service|^irs$/i.test(value) &&
          !/all boroughs/i.test(value) &&
          !/\b(view|borough|block|reel|file|crfn|lot|partial|doc\s+date|recorded|filed|pages|party|remarks|doc\s+amount)\b/i.test(value),
      ]);

      return {
        docId: row.docId,
        filingDate,
        debtorName: sanitizeDebtorName(debtorName),
        securedPartyName,
        documentType,
        rowText: normalizeText(row.rowText),
        cells: normalizedCells,
      };
    });

  if (mapped.length > 0) {
    const seen = new Set<string>();
    return mapped.filter((row) => {
      if (seen.has(row.docId)) return false;
      seen.add(row.docId);
      return true;
    });
  }

  return fallbackIds.map((docId) => ({
    docId,
    filingDate: formatDocIdDate(docId),
    debtorName: '',
    securedPartyName: 'Internal Revenue Service',
    documentType: 'FEDERAL LIEN-IRS',
    rowText: '',
    cells: [],
  }));
}

async function openImageViewFromResults(page: Page, state: SearchState, docId: string, manifest?: RunManifest): Promise<Record<string, string>> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= NYC_ACRIS_IMAGE_VIEW_RETRIES + 1; attempt++) {
    try {
      await waitForActionDelay();
      const token = await getToken(page);
      const currentFields = await collectHiddenFields(page);
      const imageViewFields = {
        ...currentFields,
        ...buildSearchPayload(state.pageNum, state.profile),
        __RequestVerificationToken: token,
      };

      const diagnostic = await submitHiddenPostToImageView(
        page,
        manifest,
        `${BASE}${PATHS.imageView}?doc_id=${docId}`,
        imageViewFields,
        `open_image_view_${docId}`
      );

      if (shouldRetryViewerOpen(diagnostic) && attempt <= NYC_ACRIS_IMAGE_VIEW_RETRIES) {
        manifest?.warnings.push(`viewer_open_transient ${docId} attempt=${attempt} url=${diagnostic.finalUrl}`);
        await recoverResultsSession(page, state, manifest);
        continue;
      }

      return imageViewFields;
    } catch (err: unknown) {
      lastError = err;
      const message = sanitizeErrorMessage(err);
      manifest?.warnings.push(`viewer_open_transient ${docId} attempt=${attempt} error=${message}`);
      if (attempt > NYC_ACRIS_IMAGE_VIEW_RETRIES) {
        break;
      }
      await recoverResultsSession(page, state, manifest).catch(() => null);
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`viewer_open_transient ${docId}`);
}

async function openDetailViewFromResults(page: Page, state: SearchState, docId: string, manifest?: RunManifest): Promise<void> {
  await waitForActionDelay();
  const token = await getToken(page);
  const currentFields = await collectHiddenFields(page);
  const detailFields = {
    ...currentFields,
    ...buildSearchPayload(state.pageNum, state.profile),
    __RequestVerificationToken: token,
  };

  await submitHiddenPostToDetail(
    page,
    manifest,
    `${BASE}${PATHS.detail}?doc_id=${docId}`,
    detailFields,
    `open_detail_view_${docId}`
  );
}

async function openImageViewFromDetail(page: Page, docId: string, manifest?: RunManifest): Promise<void> {
  await waitForActionDelay();
  const token = await getToken(page);
  const hiddenFields = await collectHiddenFields(page);
  const imageViewFields: Record<string, string> = {
    ...hiddenFields,
    __RequestVerificationToken: token,
  };

  await submitHiddenPostToImageView(
    page,
    manifest,
    `${BASE}${PATHS.imageView}?doc_id=${docId}`,
    imageViewFields,
    `open_image_view_from_detail_${docId}`
  );
}

async function returnToResultsFromViewer(page: Page, docId: string, manifest?: RunManifest): Promise<void> {
  await waitForActionDelay();
  const token = await getToken(page);
  const hiddenFields = await collectHiddenFields(page);
  const returnFields: Record<string, string> = {
    ...hiddenFields,
    __RequestVerificationToken: token,
  };

  delete returnFields.hid_DocID;
  delete returnFields.hid_PrintType;
  delete returnFields.hid_URL;
  delete returnFields.hid_Cov;
  delete returnFields.hid_Sup;
  delete returnFields.hid_Tax;

  await submitHiddenPostToResults(
    page,
    manifest,
    `${BASE}${PATHS.result}?page=${hiddenFields.hid_page || '1'}`,
    returnFields,
    `return_to_results_${docId}`
  );

  const html = await page.content();
  if (!html.includes(docId) && !html.includes('DocumentTypeResult')) {
    throw new Error('Failed to return to ACRIS result page with live session state');
  }
}

async function extractViewerArtifactInSession(page: Page, state: SearchState, docId: string, manifest?: RunManifest): Promise<ViewerArtifact> {
  await openDetailViewFromResults(page, state, docId, manifest);
  const detailHtml = await page.content();
  const detail = extractNYCAcrisDetailFromHtml(detailHtml);
  const detailUrl = page.url();
  await openImageViewFromDetail(page, docId, manifest);

  const viewerSrc = (await page.locator('iframe[name="mainframe"]').getAttribute('src').catch(() => null)) ?? null;
  const title = await page.title();
  const frame = page.frame({ name: 'mainframe' });
  let imageUrls: string[] = [];

  if (frame) {
    await frame.waitForLoadState('domcontentloaded').catch(() => null);
    imageUrls = unique(
      await frame
        .locator('img')
        .evaluateAll((nodes) => nodes.map((node) => (node as HTMLImageElement).src).filter(Boolean))
        .catch(() => [])
    );
  }

  const artifact: ViewerArtifact = {
    docId,
    detailUrl,
    imageViewUrl: page.url(),
    viewerSrc,
    imageUrls,
    title,
    totalPages: parseViewerTotalPages(viewerSrc),
    filingDate: detail.filingDate,
    recordedFiledAt: detail.recordedFiledAt,
    detailDebtorName: detail.debtorName,
    detailDebtorAddress: detail.debtorAddress,
    detailSecuredPartyName: detail.securedPartyName,
    detailSecuredPartyAddress: detail.securedPartyAddress,
    amount: detail.amount,
  };

  const ocr = await extractOcrFromViewer(page, artifact, manifest);
  artifact.amount = artifact.amount ?? ocr.amount;
  artifact.amountConfidence = ocr.amountConfidence;
  artifact.amountReason = ocr.amountReason;
  artifact.leadType = ocr.leadType;
  artifact.taxpayerName = ocr.taxpayerName;
  artifact.taxpayerAddress = ocr.taxpayerAddress;

  await returnToResultsFromViewer(page, docId, manifest);
  return artifact;
}

async function hardenContext(context: BrowserContext, manifest: RunManifest): Promise<void> {
  context.on('request', (request) => {
    const url = request.url();
    if (/a836-acris\.nyc\.gov/i.test(url)) {
      manifest.network.push({
        method: request.method(),
        url: url.replace(/(__?RequestVerificationToken=)[^&]+/gi, '$1[REDACTED]'),
        resourceType: request.resourceType(),
      });
    }
  });

  context.on('requestfailed', (request) => {
    const url = request.url();
    if (/a836-acris\.nyc\.gov/i.test(url)) {
      manifest.failures.push(`requestfailed ${request.method()} ${url}`);
    }
  });

  await context.route('**/*', async (route) => {
    const url = route.request().url();
    if (
      /prodregistryv2\.org/i.test(url) ||
      /translate-pa\.googleapis\.com/i.test(url) ||
      /translate\.googleapis\.com/i.test(url) ||
      /fonts\.gstatic\.com/i.test(url)
    ) {
      return route.abort();
    }
    return route.continue();
  });
}

function pushBootstrapTrace(manifest: RunManifest, entry: string): void {
  if (!manifest.bootstrapTrace) {
    manifest.bootstrapTrace = [];
  }
  manifest.bootstrapTrace.push(entry);
  if (manifest.bootstrapTrace.length > 50) {
    manifest.bootstrapTrace.splice(0, manifest.bootstrapTrace.length - 50);
  }
}

function attachBootstrapPageTracing(page: Page, manifest: RunManifest): void {
  page.on('framenavigated', (frame) => {
    if (frame !== page.mainFrame()) return;
    pushBootstrapTrace(manifest, `framenavigated url=${frame.url() || 'about:blank'}`);
  });

  page.on('response', (response) => {
    const request = response.request();
    if (request.frame() !== page.mainFrame()) return;
    const url = response.url();
    if (!/a836-acris\.nyc\.gov/i.test(url)) return;
    pushBootstrapTrace(manifest, `response status=${response.status()} method=${request.method()} url=${url}`);
  });

  page.on('requestfailed', (request) => {
    if (request.frame() !== page.mainFrame()) return;
    const url = request.url();
    if (!/a836-acris\.nyc\.gov/i.test(url)) return;
    pushBootstrapTrace(
      manifest,
      `requestfailed method=${request.method()} url=${url} error=${request.failure()?.errorText ?? 'unknown'}`
    );
  });

  page.on('console', (message) => {
    const text = normalizeText(message.text());
    if (!text) return;
    pushBootstrapTrace(manifest, `console type=${message.type()} text=${text.slice(0, 300)}`);
  });

  page.on('pageerror', (error) => {
    pushBootstrapTrace(manifest, `pageerror error=${sanitizeErrorMessage(error).slice(0, 300)}`);
  });
}

function toLienRecord(row: ResultRowCandidate, artifact?: ViewerArtifact): LienRecord {
  const debtorName = chooseBetterDebtorName(
    chooseBetterDebtorName(row.debtorName, artifact?.detailDebtorName),
    artifact?.taxpayerName
  );
  const debtorAddress =
    normalizeOcrAddress(artifact?.detailDebtorAddress) ??
    normalizeOcrAddress(artifact?.taxpayerAddress) ??
    '';
  return {
    state: 'NY',
    source: 'nyc_acris',
    county: 'New York City',
    ucc_type: 'Federal Tax Lien',
    debtor_name: debtorName,
    debtor_address: debtorAddress,
    file_number: row.docId,
    secured_party_name: artifact?.detailSecuredPartyName ?? row.securedPartyName,
    secured_party_address: normalizeOcrAddress(artifact?.detailSecuredPartyAddress) ?? '',
    status: 'Active',
    filing_date: artifact?.filingDate || row.filingDate || formatDocIdDate(row.docId),
    lapse_date: '12/31/9999',
    document_type: row.documentType,
    pdf_filename: '',
    processed: true,
    error: '',
    amount: artifact?.amount,
    amount_confidence: artifact?.amountConfidence,
    amount_reason: artifact?.amountReason ?? 'ocr_missing',
    confidence_score: resolveRecordConfidenceScore(debtorName, artifact),
    lead_type: artifact?.leadType ?? 'Lien',
  };
}

function resolveSafeRunLimits(requestedMaxRecords?: number): { maxRecords: number; maxPages: number } {
  const requested = requestedMaxRecords ?? INITIAL_MAX_RECORDS;
  if (!ENFORCE_INITIAL_CAP) {
    return {
      maxRecords: requested,
      maxPages: MAX_RESULT_PAGES,
    };
  }

  return {
    maxRecords: Math.min(requested, INITIAL_MAX_RECORDS),
    maxPages: Math.min(MAX_RESULT_PAGES, INITIAL_MAX_RESULT_PAGES),
  };
}

async function initializeSearchSession(
  page: Page,
  state: SearchState,
  manifest?: RunManifest,
  strategy: ProbeBootstrapStrategy = 'index_then_document_type',
): Promise<void> {
  if (strategy === 'direct_document_type') {
    await gotoDocumentTypeDirect(page, manifest);
  } else {
    await gotoIndexPage(page, manifest);
    await gotoDocumentTypePage(page, manifest);
  }
  state.profile = { ...SEARCH_PROFILE };
}

async function captureValidationEvidence(page: Page, name: string): Promise<void> {
  await ensureDir(OUT_DIR);
  const sanitized = name.replace(/[^\w-]+/g, '_');
  const html = await page.content().catch(() => '');
  await fs.writeFile(path.join(OUT_DIR, `${sanitized}.html`), html, 'utf8').catch(() => null);
}

async function createAcrisContext(options?: { headed?: boolean }) {
  return createIsolatedBrowserContext({
    headless: options?.headed ? false : undefined,
  });
}

async function createBootstrapPage(
  handle: Awaited<ReturnType<typeof createAcrisContext>>,
  manifest: RunManifest,
  options?: { onStageEvent?: (event: NYCAcrisStageEvent) => void },
): Promise<Page> {
  const page = await runNYCAcrisStage(
    manifest,
    {
      step: 'bootstrap_create_page',
      timeoutMs: NYC_ACRIS_BOOTSTRAP_NEW_PAGE_TIMEOUT_MS,
      onStageEvent: options?.onStageEvent,
    },
    () => handle.context.newPage(),
  );
  page.setDefaultTimeout(45000);
  attachBootstrapPageTracing(page, manifest);
  pushBootstrapTrace(manifest, `bootstrap_page_created url=${page.url() || 'about:blank'}`);
  return page;
}

async function initializeBootstrapSearchSession(
  page: Page,
  state: SearchState,
  manifest: RunManifest,
  strategy: ProbeBootstrapStrategy,
  options?: { onStageEvent?: (event: NYCAcrisStageEvent) => void },
): Promise<void> {
  await runNYCAcrisStage(
    manifest,
    {
      step: strategy === 'direct_document_type' ? 'bootstrap_load_document_type_direct' : 'bootstrap_load_index_page',
      timeoutMs: NYC_ACRIS_BOOTSTRAP_NAVIGATION_TIMEOUT_MS,
      onStageEvent: options?.onStageEvent,
      startDetail: `strategy=${strategy}`,
    },
    async () => {
      if (strategy === 'direct_document_type') {
        await gotoDocumentTypeDirect(page, manifest);
      } else {
        await gotoIndexPage(page, manifest);
      }
    },
    () => `strategy=${strategy}`,
  );

  if (strategy !== 'direct_document_type') {
    await runNYCAcrisStage(
      manifest,
      {
        step: 'bootstrap_load_document_type_page',
        timeoutMs: NYC_ACRIS_BOOTSTRAP_NAVIGATION_TIMEOUT_MS,
        onStageEvent: options?.onStageEvent,
        startDetail: `strategy=${strategy}`,
      },
      () => gotoDocumentTypePage(page, manifest),
      () => `strategy=${strategy}`,
    );
  }

  state.profile = { ...SEARCH_PROFILE };
}
async function openBootstrapPage(
  handle: Awaited<ReturnType<typeof createAcrisContext>>,
  state: SearchState,
  manifest: RunManifest,
  strategy: ProbeBootstrapStrategy = 'index_then_document_type',
  options?: { onStageEvent?: (event: NYCAcrisStageEvent) => void },
): Promise<Page> {
  const page = await createBootstrapPage(handle, manifest, options);
  await initializeBootstrapSearchSession(page, state, manifest, strategy, options);
  return page;
}

async function bootstrapSearchSession(
  state: SearchState,
  manifest: RunManifest,
  options?: { headed?: boolean; onStageEvent?: (event: NYCAcrisStageEvent) => void; preferDirectDocumentType?: boolean },
): Promise<BootstrapSessionResult> {
  let handle = await createAcrisContext({ headed: options?.headed });
  manifest.transportMode = handle.mode;
  manifest.transportDiagnostics = handle.diagnostics;
  await hardenContext(handle.context, manifest);

  let recoveryAction: ProbeRecoveryAction = 'none';
  let bootstrapStrategy: ProbeBootstrapStrategy = options?.preferDirectDocumentType
    ? 'direct_document_type'
    : 'index_then_document_type';
  let page = await createBootstrapPage(handle, manifest, options);

  try {
    await initializeBootstrapSearchSession(page, state, manifest, bootstrapStrategy, options);
    return { handle, page, recoveryAction, bootstrapStrategy, diagnostic: getLatestNavigationDiagnostic(manifest) };
  } catch (initialErr: unknown) {
    const initialDiagnostic = getLatestNavigationDiagnostic(manifest);
    const initialMessage = sanitizeErrorMessage(initialErr);
    const hardResetRequired = /goto watchdog fired/i.test(initialMessage);
    const deadBootstrapPage = isDeadBootstrapPage(manifest, initialDiagnostic, initialMessage);
    if (!shouldRetryBootstrapDiagnostic(initialDiagnostic, initialMessage)) {
      await page.close().catch(() => {});
      await handle.close().catch(() => {});
      throw initialErr;
    }

    manifest.warnings.push(
      `bootstrap_recovery ${buildBootstrapAttemptLabel(
        bootstrapStrategy,
        deadBootstrapPage || hardResetRequired ? 'retry_fresh_context' : 'retry_new_page',
      )} ${initialMessage}`,
    );
    await page.close().catch(() => {});
    recoveryAction = deadBootstrapPage || hardResetRequired ? 'retry_fresh_context' : 'retry_new_page';

    if (deadBootstrapPage || hardResetRequired) {
      await handle.close().catch(() => {});
      handle = await createAcrisContext({ headed: options?.headed });
      manifest.transportMode = handle.mode;
      manifest.transportDiagnostics = handle.diagnostics;
      manifest.bootstrapTrace = [];
      await hardenContext(handle.context, manifest);
      try {
        page = await openBootstrapPage(handle, state, manifest, bootstrapStrategy, options);
        return { handle, page, recoveryAction, bootstrapStrategy, diagnostic: getLatestNavigationDiagnostic(manifest) };
      } catch (freshContextDirectErr: unknown) {
        const freshContextDirectMessage = sanitizeErrorMessage(freshContextDirectErr);
        const freshContextDirectDiagnostic = getLatestNavigationDiagnostic(manifest);
        if (deadBootstrapPage && isDeadBootstrapPage(manifest, freshContextDirectDiagnostic, freshContextDirectMessage)) {
          await handle.close().catch(() => {});
          throw new Error('dead_bootstrap_page about:blank before first navigation');
        }
        throw freshContextDirectErr;
      }
    }

    try {
      page = await openBootstrapPage(handle, state, manifest, bootstrapStrategy, options);
      return { handle, page, recoveryAction, bootstrapStrategy, diagnostic: getLatestNavigationDiagnostic(manifest) };
    } catch (pageRetryErr: unknown) {
      const pageRetryDiagnostic = getLatestNavigationDiagnostic(manifest);
      const pageRetryMessage = sanitizeErrorMessage(pageRetryErr);
      if (
        !isClosedBrowserLifecycleError(pageRetryMessage) &&
        !shouldRetryBootstrapDiagnostic(pageRetryDiagnostic, pageRetryMessage)
      ) {
        await page.close().catch(() => {});
        await handle.close().catch(() => {});
        throw pageRetryErr;
      }

      manifest.warnings.push(`bootstrap_recovery ${buildBootstrapAttemptLabel(bootstrapStrategy, 'retry_fresh_context')} ${pageRetryMessage}`);
      await page.close().catch(() => {});
      await handle.close().catch(() => {});

      handle = await createAcrisContext({ headed: options?.headed });
      manifest.transportMode = handle.mode;
      manifest.transportDiagnostics = handle.diagnostics;
      await hardenContext(handle.context, manifest);
      recoveryAction = 'retry_fresh_context';
      try {
        page = await openBootstrapPage(handle, state, manifest, bootstrapStrategy, options);
        return { handle, page, recoveryAction, bootstrapStrategy, diagnostic: getLatestNavigationDiagnostic(manifest) };
      } catch (freshContextErr: unknown) {
        const freshContextDiagnostic = getLatestNavigationDiagnostic(manifest);
        const freshContextMessage = sanitizeErrorMessage(freshContextErr);
        if (!shouldRetryBootstrapDiagnostic(freshContextDiagnostic, freshContextMessage)) {
          await handle.close().catch(() => {});
          throw freshContextErr;
        }

        manifest.warnings.push(`bootstrap_recovery ${buildBootstrapAttemptLabel('direct_document_type', 'retry_fresh_context')} ${freshContextMessage}`);
        await handle.close().catch(() => {});

        handle = await createAcrisContext({ headed: options?.headed });
        manifest.transportMode = handle.mode;
        manifest.transportDiagnostics = handle.diagnostics;
        await hardenContext(handle.context, manifest);
        bootstrapStrategy = 'direct_document_type';

        try {
          page = await openBootstrapPage(handle, state, manifest, bootstrapStrategy, options);
          return { handle, page, recoveryAction, bootstrapStrategy, diagnostic: getLatestNavigationDiagnostic(manifest) };
        } catch (directFallbackErr: unknown) {
          await handle.close().catch(() => {});
          throw directFallbackErr;
        }
      }
    }
  }
}

export async function validateNYCAcrisSelectors(options: ValidationOptions = {}): Promise<RunManifest> {
  return nycAcrisLimiter.schedule(async () => {
    const startedAtMs = Date.now();
    const manifest: RunManifest = {
      startedAt: nowIso(),
      transportMode: 'local',
      resultPagesVisited: 0,
      docIds: [],
      documents: [],
      warnings: [],
      failures: [],
      network: [],
      navigationDiagnostics: [],
      validationSteps: [],
      attemptedDocs: 0,
      completedDocs: 0,
      connectivityStatusAtStart: options.connectivity_status_at_start ?? 'healthy',
    };

    const state: SearchState = {
      pageNum: 1,
      profile: { ...SEARCH_PROFILE },
    };
    let handle: Awaited<ReturnType<typeof createAcrisContext>> | null = null;
    let page: Page | null = null;

    try {
      throwIfSessionBudgetExceeded(startedAtMs);
      const bootstrap = await runNYCAcrisStage(
        manifest,
        {
          step: 'bootstrap_search_session',
          timeoutMs: NYC_ACRIS_VALIDATION_BOOTSTRAP_TIMEOUT_MS,
          onStageEvent: options.onStageEvent,
        },
        () => bootstrapSearchSession(state, manifest, {
          headed: options.headed,
          onStageEvent: options.onStageEvent,
          preferDirectDocumentType: true,
        }),
        (result) =>
          `recovery_action=${result.recoveryAction} bootstrap_strategy=${result.bootstrapStrategy} final_url=${result.diagnostic?.finalUrl ?? 'unknown'}`,
      );
      handle = bootstrap.handle;
      page = bootstrap.page;

      throwIfSessionBudgetExceeded(startedAtMs);
      await runNYCAcrisStage(
        manifest,
        {
          step: 'load_result_page_1',
          timeoutMs: NYC_ACRIS_VALIDATION_RESULT_PAGE_TIMEOUT_MS,
          onStageEvent: options.onStageEvent,
        },
        () => loadResultPage(page!, 1, state, manifest),
      );
      manifest.resultPagesVisited = 1;

      const rows = await runNYCAcrisStage(
        manifest,
        {
          step: 'extract_result_rows',
          timeoutMs: NYC_ACRIS_VALIDATION_EXTRACT_ROWS_TIMEOUT_MS,
          onStageEvent: options.onStageEvent,
        },
        () => extractResultRows(page!),
        (result) => `rows=${result.length}`,
      );
      if (rows.length === 0) {
        throw new Error('No ACRIS rows found during live selector validation');
      }

      manifest.docIds = rows.map((row) => row.docId);

      const targetRows = rows.slice(0, Math.min(options.max_documents ?? 2, rows.length));
      for (let index = 0; index < targetRows.length; index++) {
        throwIfSessionBudgetExceeded(startedAtMs);
        const row = targetRows[index];
        manifest.attemptedDocs = index + 1;
        const artifact = await runNYCAcrisStage(
          manifest,
          {
            step: `viewer_roundtrip_${row.docId}`,
            timeoutMs: NYC_ACRIS_VALIDATION_VIEWER_TIMEOUT_MS,
            onStageEvent: options.onStageEvent,
            startDetail: `doc_id=${row.docId}`,
          },
          () => extractViewerArtifactInSession(page!, state, row.docId, manifest),
          (result) => `doc_id=${result.docId} image_count=${result.imageUrls.length} total_pages=${result.totalPages ?? 1}`,
        );
        manifest.documents.push(artifact);
        manifest.completedDocs = manifest.documents.length;

        if (index < targetRows.length - 1) {
          await waitForDocDelay();
        }
      }

      if (MAX_RESULT_PAGES > 1) {
        throwIfSessionBudgetExceeded(startedAtMs);
        await runNYCAcrisStage(
          manifest,
          {
            step: 'reload_result_page_after_viewer',
            timeoutMs: NYC_ACRIS_VALIDATION_RELOAD_TIMEOUT_MS,
            onStageEvent: options.onStageEvent,
          },
          () => loadResultPage(page!, 1, state, manifest),
        );
      }

      manifest.finishedAt = nowIso();
      manifest.sessionDurationMs = Date.now() - startedAtMs;
      await writeManifest(manifest, `validation-${Date.now()}.json`);
      return manifest;
    } catch (err: unknown) {
      const message = sanitizeErrorMessage(err);
      manifest.finishedAt = nowIso();
      manifest.failureClass = classifyNYCAcrisFailure(message);
      manifest.failures.push(message);
      manifest.sessionDurationMs = Date.now() - startedAtMs;
      if (page) {
        await captureValidationEvidence(page, 'acris-validation-failure');
      }
      await writeManifest(manifest, `validation-failed-${Date.now()}.json`).catch(() => null);
      throw err;
    } finally {
      await page?.close().catch(() => {});
      await handle?.close().catch(() => {});
    }
  });
}

export async function scrapeNYCAcris(options: ScrapeOptions): Promise<ScrapeResult> {
  return nycAcrisLimiter.schedule(async () => {
    const startedAtMs = Date.now();
    const limits = resolveSafeRunLimits(options.max_records);
    const checkpoint = await loadCheckpoint(options);
    const manifest: RunManifest = {
      startedAt: nowIso(),
      transportMode: 'local',
      resultPagesVisited: checkpoint?.pageNum ?? 0,
      docIds: [],
      documents: [],
      warnings: [],
      failures: [],
      network: [],
      navigationDiagnostics: [],
      attemptedDocs: 0,
      completedDocs: 0,
      connectivityStatusAtStart: options.connectivity_status_at_start ?? 'healthy',
      checkpoint: checkpoint ? { pageNum: checkpoint.pageNum, docIndex: checkpoint.docIndex, docId: checkpoint.docId } : undefined,
      requestedDateStart: normalizeAcrisDate(options.date_start) ?? options.date_start,
      requestedDateEnd: normalizeAcrisDate(options.date_end) ?? options.date_end,
    };

    const state: SearchState = {
      pageNum: checkpoint?.pageNum ?? 1,
      profile: { ...SEARCH_PROFILE },
      requestDateRange: {
        start: normalizeAcrisDate(options.date_start) ?? options.date_start,
        end: normalizeAcrisDate(options.date_end) ?? options.date_end,
      },
    };
    let handle: Awaited<ReturnType<typeof createAcrisContext>> | null = null;
    let page: Page | null = null;
    let bootstrapRecoveryAction: ProbeRecoveryAction = 'none';
    let bootstrapDiagnostic: PageReadyDiagnostic | undefined;

    try {
      throwIfSessionBudgetExceeded(startedAtMs);
      const bootstrap = await bootstrapSearchSession(state, manifest);
      handle = bootstrap.handle;
      page = bootstrap.page;
      bootstrapRecoveryAction = bootstrap.recoveryAction;
      bootstrapDiagnostic = bootstrap.diagnostic;

      const collectedRows: ResultRowCandidate[] = [];
      let retriedFirstPageForRangeIntegrity = false;
      for (let pageNum = 1; pageNum <= limits.maxPages; pageNum++) {
        throwIfSessionBudgetExceeded(startedAtMs);

        if (options.stop_requested?.()) {
          manifest.warnings.push(`stop_requested on result page ${pageNum}`);
          break;
        }

        await loadResultPage(page, pageNum, state, manifest);
        manifest.resultPagesVisited = pageNum;
        let rows = await extractResultRows(page);

        if (
          pageNum === 1 &&
          !checkpoint &&
          state.requestDateRange &&
          isCompletelyOutOfRangeAcrisResultSet(rows, {
            date_start: state.requestDateRange.start,
            date_end: state.requestDateRange.end,
          })
        ) {
          if (!retriedFirstPageForRangeIntegrity) {
            retriedFirstPageForRangeIntegrity = true;
            manifest.warnings.push(
              `result_window_mismatch_retry requested=${state.requestDateRange.start}-${state.requestDateRange.end}`
            );
            await resubmitFirstResultPageFromCanonicalSearch(page, state, manifest);
            rows = await extractResultRows(page);
          }

          if (
            isCompletelyOutOfRangeAcrisResultSet(rows, {
              date_start: state.requestDateRange.start,
              date_end: state.requestDateRange.end,
            })
          ) {
            const retriedRange = summarizeDateRange(rows);
            manifest.upstreamMinFilingDate = retriedRange.min;
            manifest.upstreamMaxFilingDate = retriedRange.max;
            manifest.filteredOutCount = rows.length;
            throw new NYCAcrisRangeIntegrityError({
              requestedStart: state.requestDateRange.start,
              requestedEnd: state.requestDateRange.end,
              upstreamMin: retriedRange.min,
              upstreamMax: retriedRange.max,
              returnedRowCount: rows.length,
            });
          }
        }

        const freshRows = rows.filter((row) => !collectedRows.some((existing) => existing.docId === row.docId));

        if (rows.length === 0) {
          manifest.warnings.push(`No rows found on result page ${pageNum}`);
          break;
        }

        collectedRows.push(...freshRows);
        if (freshRows.length === 0) {
          manifest.warnings.push(`No new rows found on result page ${pageNum}`);
          break;
        }

        if (collectedRows.length >= limits.maxRecords) break;
      }

      manifest.discoveredCount = collectedRows.length;
      const rangeBeforeFilter = summarizeDateRange(collectedRows);
      manifest.upstreamMinFilingDate = rangeBeforeFilter.min;
      manifest.upstreamMaxFilingDate = rangeBeforeFilter.max;
      const filtered = filterRowsByAcrisDateRange(collectedRows, options);
      manifest.filteredOutCount = filtered.filteredOutCount;
      const filteredRange = summarizeDateRange(filtered.rows);
      manifest.returnedMinFilingDate = filteredRange.min;
      manifest.returnedMaxFilingDate = filteredRange.max;

      if (filtered.rows.length === 0 && collectedRows.length > 0 && filtered.hadOutOfRangeRows) {
        const requestedStart = state.requestDateRange?.start ?? options.date_start;
        const requestedEnd = state.requestDateRange?.end ?? options.date_end;
        throw new NYCAcrisRangeIntegrityError({
          requestedStart,
          requestedEnd,
          upstreamMin: rangeBeforeFilter.min,
          upstreamMax: rangeBeforeFilter.max,
          returnedRowCount: collectedRows.length,
        });
      }

      const selectedRows = filtered.rows.slice(0, limits.maxRecords);
      manifest.docIds = selectedRows.map((row) => row.docId);
      manifest.returnedCount = selectedRows.length;

      const processedDocs = await processSelectedRows({
        selectedRows,
        checkpoint,
        pageNum: state.pageNum,
        stopRequested: options.stop_requested,
        extractArtifact: (row) => extractViewerArtifactInSession(page!, state, row.docId, manifest),
        saveCheckpoint: async (nextCheckpoint) => {
          manifest.checkpoint = {
            pageNum: nextCheckpoint.pageNum,
            docIndex: nextCheckpoint.docIndex,
            docId: nextCheckpoint.docId,
          };
          await saveCheckpoint(options, nextCheckpoint);
        },
        onWarning: (warning) => manifest.warnings.push(warning),
        onFailure: (failure) => manifest.failures.push(failure),
        waitForDocDelay,
      });
      manifest.documents.push(...processedDocs.documents);
      manifest.attemptedDocs = processedDocs.attemptedDocs;
      manifest.completedDocs = manifest.documents.length;

      if (!processedDocs.stopped) {
        await clearCheckpoint(options);
      }

      const artifactsByDocId = new Map(manifest.documents.map((artifact) => [artifact.docId, artifact]));
      manifest.finishedAt = nowIso();
      manifest.sessionDurationMs = Date.now() - startedAtMs;
      const manifestFile = await writeManifest({
        ...manifest,
        warnings: unique(manifest.warnings),
        failures: unique(manifest.failures),
        network: manifest.network.slice(0, 200),
      });

      const outputRows = selectedRows.map((row) => toLienRecord(row, artifactsByDocId.get(row.docId)));
      log({
        stage: 'scraper_complete',
        site: 'nyc_acris',
        transport_mode: manifest.transportMode,
        browser_ws_present: Boolean(process.env.BRIGHTDATA_BROWSER_WS),
        browser_ws_redacted: redactSecret(process.env.BRIGHTDATA_BROWSER_WS),
        proxy_server_present: Boolean(process.env.BRIGHTDATA_PROXY_SERVER),
        proxy_server_redacted: redactSecret(process.env.BRIGHTDATA_PROXY_SERVER),
        records_scraped: selectedRows.length,
        requested_date_start: manifest.requestedDateStart,
        requested_date_end: manifest.requestedDateEnd,
        discovered_count: manifest.discoveredCount ?? collectedRows.length,
        returned_count: manifest.returnedCount ?? selectedRows.length,
        filtered_out_count: manifest.filteredOutCount ?? 0,
        upstream_min_filing_date: manifest.upstreamMinFilingDate,
        upstream_max_filing_date: manifest.upstreamMaxFilingDate,
        returned_min_filing_date: manifest.returnedMinFilingDate,
        returned_max_filing_date: manifest.returnedMaxFilingDate,
        failure_count: manifest.failures.length,
        manifest_file: manifestFile,
        initial_cap_enforced: ENFORCE_INITIAL_CAP,
        session_duration_ms: manifest.sessionDurationMs,
        probe_recovery_action: bootstrapRecoveryAction,
        probe_bootstrap_strategy: inferBootstrapStrategyFromDiagnostic(bootstrapDiagnostic),
        amount_reason_counts: summarizeAmountReasonCounts(outputRows),
        ...summarizeProbeDiagnostic(bootstrapDiagnostic),
      });

      return attachScrapeQualitySummary(
        outputRows,
        {
          requested_date_start: manifest.requestedDateStart ?? options.date_start,
          requested_date_end: manifest.requestedDateEnd ?? options.date_end,
          discovered_count: manifest.discoveredCount ?? collectedRows.length,
          returned_count: manifest.returnedCount ?? selectedRows.length,
          quarantined_count: 0,
          partial_run: processedDocs.stopped || (manifest.filteredOutCount ?? 0) > 0,
          partial_reason: processedDocs.stopped
            ? 'stop_requested'
            : manifest.filteredOutCount && manifest.filteredOutCount > 0
              ? 'rows_filtered_outside_requested_range'
              : undefined,
          filtered_out_count: manifest.filteredOutCount ?? 0,
          returned_min_filing_date: manifest.returnedMinFilingDate,
          returned_max_filing_date: manifest.returnedMaxFilingDate,
          upstream_min_filing_date: manifest.upstreamMinFilingDate,
          upstream_max_filing_date: manifest.upstreamMaxFilingDate,
        }
      );
    } catch (err: unknown) {
      const message = sanitizeErrorMessage(err);
      manifest.finishedAt = nowIso();
      manifest.failureClass = classifyNYCAcrisFailure(message);
      manifest.failures.push(message);
      manifest.sessionDurationMs = Date.now() - startedAtMs;
      await writeManifest(manifest, `run-failed-${Date.now()}.json`).catch(() => null);
      throw err;
    } finally {
      await page?.close().catch(() => {});
      await handle?.close().catch(() => {});
    }
  });
}

export async function probeNYCAcrisConnectivity(options: ProbeOptions = {}): Promise<ProbeResult> {
  return nycAcrisLimiter.schedule(async () => {
    const manifest: RunManifest = {
      startedAt: nowIso(),
      transportMode: 'local',
      resultPagesVisited: 0,
      docIds: [],
      documents: [],
      warnings: [],
      failures: [],
      network: [],
      navigationDiagnostics: [],
      validationSteps: [],
    };
    const state: SearchState = {
      pageNum: 1,
      profile: { ...SEARCH_PROFILE },
    };
    let handle: Awaited<ReturnType<typeof createAcrisContext>> | null = null;
    let page: Page | null = null;

    try {
      const bootstrap = await runNYCAcrisStage(
        manifest,
        {
          step: 'probe_bootstrap_search_session',
          timeoutMs: NYC_ACRIS_PROBE_BOOTSTRAP_TIMEOUT_MS,
          onStageEvent: options.onStageEvent,
        },
        () => bootstrapSearchSession(state, manifest, {
          headed: options.headed,
          onStageEvent: options.onStageEvent,
          preferDirectDocumentType: true,
        }),
        (result) =>
          `recovery_action=${result.recoveryAction} bootstrap_strategy=${result.bootstrapStrategy} final_url=${result.diagnostic?.finalUrl ?? 'unknown'}`,
      );
      handle = bootstrap.handle;
      page = bootstrap.page;
      const diagnostic = bootstrap.diagnostic;
      return {
        ok: true,
        detail: diagnostic ? `loaded ${diagnostic.finalUrl} ${formatDiagnostic(diagnostic)}` : 'loaded NYC bootstrap session',
        transportMode: handle.mode,
        diagnostic,
        recoveryAction: bootstrap.recoveryAction,
        bootstrapStrategy: bootstrap.bootstrapStrategy,
        steps: manifest.validationSteps,
        bootstrapTrace: manifest.bootstrapTrace,
        failures: manifest.failures,
        warnings: manifest.warnings,
      };
    } catch (err: unknown) {
      const message = sanitizeErrorMessage(err);
      manifest.failures.push(message);
      const diagnostic = getLatestNavigationDiagnostic(manifest);
      return {
        ok: false,
        detail: message,
        transportMode: handle?.mode ?? manifest.transportMode,
        diagnostic,
        failureClass: classifyNYCAcrisFailure(message),
        recoveryAction: inferBootstrapRecoveryAction(manifest),
        bootstrapStrategy: inferBootstrapStrategyFromDiagnostic(diagnostic),
        steps: manifest.validationSteps,
        bootstrapTrace: manifest.bootstrapTrace,
        failures: manifest.failures,
        warnings: manifest.warnings,
      };
    } finally {
      await page?.close().catch(() => {});
      await handle?.close().catch(() => {});
    }
  });
}
