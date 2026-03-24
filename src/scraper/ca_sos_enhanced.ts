import { Page } from 'playwright';
import { attachScrapeQualitySummary, type LienRecord, type ScrapeResult } from '../types';
import { log } from '../utils/logger';
import { SQLiteQueueStore } from '../queue/sqlite';
import fs from 'fs';
import path from 'path';
import { execFileSync } from 'child_process';
import crypto from 'crypto';
import { captureFileTypeSelectionFailureDebug } from './file_type_debug';
import { selectFileType } from './selectors/fileType';
import { extractAmountFromText as extractAmountByConfidence, AmountReason } from './amount-extraction';
import { checkOCRRuntime, getOCRBinaryCommands } from './ocr-runtime';
import { createIsolatedBrowserContext } from '../browser/transport';

interface ScrapeOptions {
  date_start: string;
  date_end: string;
  max_records?: number;
  chunk_size?: number;
  start_index?: number;
  max_chunk_retries?: number;
  checkpoint_key?: string;
  deadline_at_iso?: string;
  stop_requested?: () => boolean;
}

interface CASOSSearchResult {
  rowCount: number;
  hasNoResults: boolean;
  resultCount: number;
  diagnostics?: CASOSResultsDiagnostics;
}

interface ScrapeCheckpoint {
  next_index: number;
  updated_at: string;
}

interface QuarantinedCASOSRow {
  row_index: number;
  file_number?: string;
  filing_date?: string;
  reason: string;
  quarantined_at: string;
}

interface CASOSResultsDiagnostics {
  finalUrl: string;
  title: string;
  readyState: string;
  resultsContainerVisible: boolean;
  rowCount: number;
  drawerButtonCount: number;
  resultCountText?: string;
  resultCount?: number | null;
  noResultsVisible: boolean;
  visibleErrorText?: string;
}

function humanDelay() {
  return new Promise(resolve => setTimeout(resolve, 800 + Math.random() * 400));
}

function computeFingerprint(source: string, fileNumber: string, filingDate: string): string {
  return crypto.createHash('sha256').update(`${source}-${fileNumber}-${filingDate}`).digest('hex');
}

function getCheckpointPath(key: string): string {
  const checkpointDir = path.join(process.cwd(), 'data/checkpoints');
  if (!fs.existsSync(checkpointDir)) {
    fs.mkdirSync(checkpointDir, { recursive: true });
  }
  const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, '_');
  return path.join(checkpointDir, `ca_sos_${safeKey}.json`);
}

function getQuarantinePath(key: string): string {
  const checkpointDir = path.join(process.cwd(), 'data/checkpoints');
  if (!fs.existsSync(checkpointDir)) {
    fs.mkdirSync(checkpointDir, { recursive: true });
  }
  const safeKey = key.replace(/[^a-zA-Z0-9_-]/g, '_');
  return path.join(checkpointDir, `ca_sos_${safeKey}_quarantine.json`);
}

function loadCheckpoint(key: string): ScrapeCheckpoint | null {
  try {
    const checkpointPath = getCheckpointPath(key);
    if (!fs.existsSync(checkpointPath)) return null;
    const parsed = JSON.parse(fs.readFileSync(checkpointPath, 'utf8')) as ScrapeCheckpoint;
    if (typeof parsed.next_index === 'number') {
      return parsed;
    }
    return null;
  } catch {
    return null;
  }
}

function saveCheckpoint(key: string, nextIndex: number): void {
  const checkpointPath = getCheckpointPath(key);
  const payload: ScrapeCheckpoint = {
    next_index: nextIndex,
    updated_at: new Date().toISOString(),
  };
  fs.writeFileSync(checkpointPath, JSON.stringify(payload, null, 2));
}

function clearCheckpoint(key: string): void {
  const checkpointPath = getCheckpointPath(key);
  if (fs.existsSync(checkpointPath)) {
    fs.unlinkSync(checkpointPath);
  }
}

async function backoffDelay(attempt: number): Promise<void> {
  const base = 1000;
  const jitter = Math.floor(Math.random() * 250);
  const delayMs = Math.min(base * (2 ** attempt) + jitter, 10000);
  await new Promise((resolve) => setTimeout(resolve, delayMs));
}

interface PdfExtraction {
  amount?: string;
  amountConfidence?: number;
  amountReason?: AmountReason;
  ocrStatus?: 'ok' | 'ocr_missing' | 'ocr_no_text' | 'ocr_error';
  leadType?: string;
  taxpayerName?: string;
  residence?: string;
}

let ocrToolingChecked = false;

function clampConfidence(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function normalizeText(value?: string): string {
  return (value ?? '')
    .replace(/\s+/g, ' ')
    .trim();
}

function toIsoDateFromMmDdYyyy(value: string): string | null {
  const match = value.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) return null;
  return `${match[3]}-${match[1]}-${match[2]}`;
}

function isMmDdYyyyAfter(left: string, right: string): boolean {
  const leftIso = toIsoDateFromMmDdYyyy(left);
  const rightIso = toIsoDateFromMmDdYyyy(right);
  if (!leftIso || !rightIso) return false;
  return leftIso > rightIso;
}

function appendRecordError(record: LienRecord, code: string): void {
  const existing = normalizeText(record.error);
  record.error = existing ? `${existing};${code}` : code;
}

function normalizeComparableText(value?: string): string {
  return normalizeText(value)
    .toUpperCase()
    .replace(/[^A-Z0-9 ]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isPlausiblePartyName(value: string): boolean {
  const normalized = normalizeComparableText(value);
  if (!normalized) return false;
  if (/[:;]/.test(normalized)) return false;
  if (/\b\d{1,2}\/\d{1,2}\/\d{4}\b/.test(normalized)) return false;
  if (/\b\d{1,2}:\d{2}(?::\d{2})?\s*[AP]M\b/i.test(normalized)) return false;
  if ((normalized.match(/\d/g) ?? []).length >= 4) return false;

  const tokens = normalized
    .split(/\s+/)
    .map((token) => token.replace(/^[^A-Z0-9]+|[^A-Z0-9]+$/g, ''))
    .filter(Boolean);

  if (tokens.length === 0) return false;

  const alphaTokens = tokens.filter((token) => /[A-Z]/.test(token));
  if (alphaTokens.length === 0) return false;

  const businessSuffixes = new Set([
    'LLC', 'INC', 'INC.', 'CORP', 'CORP.', 'CORPORATION', 'CO', 'CO.', 'COMPANY', 'LTD', 'LTD.', 'LP', 'LLP', 'PLLC',
    'PC', 'P.C.', 'TRUST', 'HOLDINGS', 'GROUP', 'PARTNERS', 'PARTNERSHIP', 'VENTURES', 'ENTERPRISES', 'REALTY',
  ]);
  const honorifics = new Set(['MR', 'MRS', 'MS', 'MISS', 'DR', 'JR', 'SR', 'II', 'III', 'IV']);
  const hasBusinessSuffix = alphaTokens.some((token) => businessSuffixes.has(token.toUpperCase()));
  const longAlphaTokens = alphaTokens.filter((token) => token.replace(/[^A-Z]/g, '').length >= 2);
  const hasPersonLikeShape =
    longAlphaTokens.length >= 2 &&
    alphaTokens.every((token) => token.length > 1 || /^[A-Z]$/i.test(token) || honorifics.has(token.toUpperCase()));

  return hasBusinessSuffix || hasPersonLikeShape;
}

function scorePartyNameConfidence(value?: string): number {
  const normalized = normalizeText(value);
  if (!normalized) return 0.2;
  if (isPlausiblePartyName(normalized)) return 0.8;
  if (/[A-Za-z]/.test(normalized) && !/\b\d{1,2}:\d{2}(?::\d{2})?\s*[AP]M\b/i.test(normalized)) return 0.55;
  return 0.3;
}

function scoreAddressConfidence(value?: string): number {
  const normalized = normalizeText(value);
  if (!normalized) return 0.2;
  if (/\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b/i.test(normalized)) return 0.85;
  if (/\d/.test(normalized) && /[A-Za-z]/.test(normalized) && /,/.test(normalized)) return 0.72;
  if (/\d/.test(normalized) && /[A-Za-z]/.test(normalized)) return 0.62;
  return 0.45;
}

function scoreNameAgreement(primary?: string, secondary?: string): number | undefined {
  const left = normalizeComparableText(primary);
  const right = normalizeComparableText(secondary);
  if (!left || !right) return undefined;
  if (left === right) return 0.92;

  const leftTokens = new Set(left.split(/\s+/).filter(Boolean));
  const rightTokens = new Set(right.split(/\s+/).filter(Boolean));
  const shared = Array.from(leftTokens).filter((token) => rightTokens.has(token));
  const denominator = Math.max(leftTokens.size, rightTokens.size, 1);
  const overlap = shared.length / denominator;

  if (overlap >= 0.75) return 0.86;
  if (overlap >= 0.5) return 0.75;
  return undefined;
}

export function resolveCARecordConfidenceScore(
  debtorName: string,
  debtorAddress: string,
  pdfData: Pick<PdfExtraction, 'amountConfidence' | 'amountReason' | 'taxpayerName' | 'residence'>
): number | undefined {
  const candidates: number[] = [];

  if (typeof pdfData.amountConfidence === 'number') {
    candidates.push(pdfData.amountConfidence);
  } else if (pdfData.amountReason === 'ok') {
    candidates.push(0.75);
  } else if (pdfData.amountReason === 'amount_low_confidence') {
    candidates.push(0.5);
  }

  const ocrName = normalizeText(pdfData.taxpayerName);
  const ocrResidence = normalizeText(pdfData.residence);

  if (ocrName) {
    candidates.push(scorePartyNameConfidence(ocrName) + 0.05);
    const agreement = scoreNameAgreement(debtorName, ocrName);
    if (typeof agreement === 'number') candidates.push(agreement);
  } else {
    candidates.push(scorePartyNameConfidence(debtorName));
  }

  if (ocrResidence) {
    candidates.push(scoreAddressConfidence(ocrResidence));
  } else {
    candidates.push(scoreAddressConfidence(debtorAddress));
  }

  if (candidates.length === 0) return undefined;
  return Number(clampConfidence(Math.max(...candidates)).toFixed(2));
}

function ocrPdf(pdfPath: string): string {
  const dir = path.dirname(pdfPath);
  const base = path.basename(pdfPath, '.pdf');
  const imgPrefix = path.join(dir, `${base}_page`);
  const ocrOutput = path.join(dir, `${base}_ocr`);
  const commands = getOCRBinaryCommands();

  try {
    if (!ocrToolingChecked) {
      const runtime = checkOCRRuntime();
      log({
        stage: 'ocr_tooling_check',
        tesseract_installed: !runtime.missing.includes('tesseract'),
        pdftoppm_installed: !runtime.missing.includes('pdftoppm'),
        tesseract_command: runtime.commands?.tesseract,
        pdftoppm_command: runtime.commands?.pdftoppm,
      });
      ocrToolingChecked = true;
      if (!runtime.ok) return '';
    }

    execFileSync(commands.pdftoppm, ['-png', '-r', '300', pdfPath, imgPrefix], { stdio: 'ignore', timeout: 15000 });

    const imgFiles = fs.readdirSync(dir)
      .filter(f => f.startsWith(`${base}_page`) && f.endsWith('.png'))
      .sort()
      .map(f => path.join(dir, f));

    if (imgFiles.length === 0) return '';

    let fullText = '';
    for (const imgFile of imgFiles) {
      execFileSync(commands.tesseract, [imgFile, ocrOutput, '--psm', '6'], { stdio: 'ignore', timeout: 30000 });
      if (fs.existsSync(`${ocrOutput}.txt`)) {
        fullText += fs.readFileSync(`${ocrOutput}.txt`, 'utf-8') + '\n';
        fs.unlinkSync(`${ocrOutput}.txt`);
      }
      fs.unlinkSync(imgFile);
    }

    return fullText;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    log({ stage: 'ocr_error', error: message });
    return '';
  }
}

async function extractFromPDF(pdfPath: string): Promise<PdfExtraction> {
  try {
    const runtime = checkOCRRuntime();
    if (!runtime.ok) return { amountReason: 'ocr_missing', ocrStatus: 'ocr_missing' };

    const text = ocrPdf(pdfPath);
    if (!text.trim()) return { amountReason: 'ocr_no_text', ocrStatus: 'ocr_no_text' };

    log({ stage: 'pdf_ocr_extracted', length: text.length, preview: text.substring(0, 200) });

    const amountResult = extractAmountByConfidence(text, Number(process.env.AMOUNT_MIN_CONFIDENCE ?? '0.75'));

    if (amountResult.amount) {
      log({ stage: 'pdf_amount_matched', parsed_amount: amountResult.amount, confidence: amountResult.confidence });
    }

    if (!amountResult.amount) {
      log({
        stage: 'pdf_amount_not_found',
        reason: amountResult.reason,
        confidence: amountResult.confidence,
        text_preview: text.substring(0, 500),
      });
    }

    let leadType: string | undefined;
    if (/Form\s+668\s*\(?\s*Z\s*\)?/i.test(text) || /Certificate\s+of\s+Release\s+of\s+Federal/i.test(text)) {
      leadType = 'Release';
    } else if (/Form\s+668\s*\(?\s*Y\s*\)?/i.test(text) || /Notice\s+of\s+Federal\s+Tax\s+Li/i.test(text)) {
      leadType = 'Lien';
    }

    const nameMatch = text.match(/Name\s+of\s+Taxpayer\s+(.+?)(?:\n|Residence)/is);
    const taxpayerName = nameMatch ? nameMatch[1].trim() : undefined;

    const residenceMatch = text.match(/Residence\s+(.+?)(?:\n.*?(?:Tax Period|IMPORTANT|Kind of Tax))/is);
    const residence = residenceMatch ? residenceMatch[1].trim() : undefined;

    log({ stage: 'pdf_fields_extracted', amount: amountResult.amount, leadType, taxpayerName: taxpayerName?.substring(0, 50), residence: residence?.substring(0, 50) });
    return {
      amount: amountResult.amount,
      amountConfidence: amountResult.confidence,
      amountReason: amountResult.reason,
      ocrStatus: 'ok',
      leadType,
      taxpayerName,
      residence,
    };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    log({ stage: 'pdf_parse_error', error: message });
    return { amountReason: 'ocr_error', ocrStatus: 'ocr_error' };
  }
}

async function dismissHistoryModal(page: Page): Promise<void> {
  const modal = page.locator('div.history-modal[role="dialog"]');
  if (!(await modal.isVisible({ timeout: 1000 }).catch(() => false))) return;

  const modalCloseBtn = modal.locator('button[aria-label="Close"], button.close, button.btn-close, .modal-header button').first();
  if (await modalCloseBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
    await modalCloseBtn.click({ force: true }).catch(() => {});
    await page.waitForTimeout(500);
  }

  if (await modal.isVisible({ timeout: 500 }).catch(() => false)) {
    await page.keyboard.press('Escape');
    await page.waitForTimeout(500);
  }

  if (await modal.isVisible({ timeout: 500 }).catch(() => false)) {
    await page.evaluate(() => {
      const m = document.querySelector('div.history-modal[role="dialog"]');
      if (m) (m as HTMLElement).style.display = 'none';
      const backdrops = document.querySelectorAll('.modal-backdrop');
      backdrops.forEach(b => (b as HTMLElement).style.display = 'none');
    });
    await page.waitForTimeout(300);
  }

  log({ stage: 'history_modal_dismissed' });
}

export function parseCASOSResultsCount(text: string): number | null {
  const match = text.match(/Results:\s*(\d+)/i);
  if (!match) return null;

  const parsed = Number.parseInt(match[1], 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseCASOSLatestProcessedDate(text: string): string | null {
  const match = text.match(/processed through:\s*(\d{2}\/\d{2}\/\d{4})/i);
  return match?.[1] ?? null;
}

async function configureCASOSSearchPage(page: Page): Promise<void> {
  page.setDefaultTimeout(30000);
  page.setDefaultNavigationTimeout(60000);
  await page.route('**/*', (route) => {
    const type = route.request().resourceType();
    if (['image', 'font', 'media'].includes(type)) {
      return route.abort();
    }
    return route.continue();
  });
}

async function fillCASOSSearchForm(page: Page, date_start: string, date_end: string): Promise<void> {
  const searchInput = page.getByLabel('Search by name or file number');
  await searchInput.waitFor({ state: 'visible', timeout: 60000 });
  await humanDelay();

  await searchInput.fill('Internal Revenue Service');
  await humanDelay();

  const advancedBtn = page.getByRole('button', { name: /Advanced/i });
  await advancedBtn.waitFor({ state: 'visible', timeout: 30000 });
  await advancedBtn.click();
  await humanDelay();

  const fileTypeSelected = await selectFileType(page, {
    log,
    onFailure: () => captureFileTypeSelectionFailureDebug(page),
  });

  if (!fileTypeSelected) {
    throw new Error('Could not find/select File Type control after opening Advanced search.');
  }

  const dateStartInput = page.getByRole('textbox', { name: 'File Date: Start' });
  const dateEndInput = page.getByRole('textbox', { name: 'File Date: End' });
  await dateStartInput.fill(date_start);
  await humanDelay();
  await dateEndInput.fill(date_end);
  await dateEndInput.press('Tab');
  await humanDelay();
}

async function navigateToCASOSSearch(page: Page): Promise<void> {
  await page.goto('https://bizfileonline.sos.ca.gov/search/ucc', {
    waitUntil: 'networkidle',
    timeout: 60000,
  });
}

async function resolveCASOSRequestedDateRange(
  page: Page,
  date_start: string,
  date_end: string,
): Promise<{ dateStart: string; dateEnd: string; latestProcessedDate?: string }> {
  const processedText = normalizeText((await page.locator('text=/processed through:/i').first().textContent().catch(() => '')) ?? '');
  const latestProcessedDate = parseCASOSLatestProcessedDate(processedText);
  if (!latestProcessedDate) {
    return { dateStart: date_start, dateEnd: date_end };
  }

  const clampedEnd = isMmDdYyyyAfter(date_end, latestProcessedDate) ? latestProcessedDate : date_end;
  if (clampedEnd !== date_end) {
    log({
      stage: 'ca_sos_date_end_clamped',
      requested_date_end: date_end,
      effective_date_end: clampedEnd,
      latest_processed_date: latestProcessedDate,
    });
  }

  return {
    dateStart: date_start,
    dateEnd: clampedEnd,
    latestProcessedDate,
  };
}

async function submitCASOSSearch(page: Page, date_start: string, date_end: string): Promise<{ effectiveDateEnd: string; latestProcessedDate?: string }> {
  await navigateToCASOSSearch(page);
  const resolvedRange = await resolveCASOSRequestedDateRange(page, date_start, date_end);
  log({
    stage: 'ca_sos_search_submit_started',
    date_start,
    date_end,
    effective_date_end: resolvedRange.dateEnd,
    latest_processed_date: resolvedRange.latestProcessedDate,
  });
  await fillCASOSSearchForm(page, resolvedRange.dateStart, resolvedRange.dateEnd);
  await page.getByRole('button', { name: 'Search' }).click();
  await page.waitForLoadState('networkidle');
  await humanDelay();
  log({
    stage: 'ca_sos_search_submit_finished',
    date_start,
    date_end,
    effective_date_end: resolvedRange.dateEnd,
    latest_processed_date: resolvedRange.latestProcessedDate,
  });
  return { effectiveDateEnd: resolvedRange.dateEnd, latestProcessedDate: resolvedRange.latestProcessedDate };
}

export async function inspectCASOSResultsState(page: Page): Promise<CASOSResultsDiagnostics> {
  const rowsLocator = page.locator('.div-table-row');
  const rowButtons = page.locator('.interactive-cell-button');
  const resultsTextLocator = page.locator('text=/Results:\\s*\\d+/');
  const noResultsLocator = page.locator('text=/No\\s+(records|results)\\s+(were\\s+)?found/i');
  const resultsContainer = page.locator('.div-table, [role="table"], .search-results, table').first();
  const errorLocator = page.locator('text=/Error|Something went wrong|Access denied|temporarily unavailable/i').first();

  const resultCountText = normalizeText((await resultsTextLocator.first().textContent().catch(() => '')) ?? '');
  const visibleErrorText = normalizeText((await errorLocator.textContent().catch(() => '')) ?? '');

  return {
    finalUrl: page.url(),
    title: await page.title().catch(() => ''),
    readyState: await page.evaluate(() => document.readyState).catch(() => 'unavailable'),
    resultsContainerVisible: await resultsContainer.isVisible().catch(() => false),
    rowCount: await rowsLocator.count().catch(() => 0),
    drawerButtonCount: await rowButtons.count().catch(() => 0),
    resultCountText: resultCountText || undefined,
    resultCount: resultCountText ? parseCASOSResultsCount(resultCountText) : null,
    noResultsVisible: await noResultsLocator.isVisible().catch(() => false),
    visibleErrorText: visibleErrorText || undefined,
  };
}

export function interpretCASOSResultsState(
  state: CASOSResultsDiagnostics,
  options: { allowContainerOnlyCount?: boolean } = {},
): CASOSSearchResult | null {
  if (state.noResultsVisible) {
    return {
      rowCount: 0,
      hasNoResults: true,
      resultCount: 0,
      diagnostics: state,
    };
  }

  if (typeof state.resultCount === 'number') {
    if (state.resultCount === 0) {
      return {
        rowCount: 0,
        hasNoResults: true,
        resultCount: 0,
        diagnostics: state,
      };
    }

    if (
      state.rowCount > 0 ||
      state.drawerButtonCount > 0 ||
      (options.allowContainerOnlyCount === true && state.resultsContainerVisible)
    ) {
      return {
        rowCount: Math.max(state.rowCount, state.drawerButtonCount),
        hasNoResults: false,
        resultCount: state.resultCount,
        diagnostics: state,
      };
    }
  }

  if ((state.rowCount > 0 || state.drawerButtonCount > 0) && !state.visibleErrorText) {
    const inferredCount = Math.max(state.rowCount, state.drawerButtonCount);
    return {
      rowCount: inferredCount,
      hasNoResults: false,
      resultCount: inferredCount,
      diagnostics: state,
    };
  }

  return null;
}

async function waitForResultsOrNoResults(
  page: Page,
  options: { allowContainerOnlyCount?: boolean } = {},
): Promise<CASOSSearchResult> {
  const start = Date.now();
  const timeoutMs = 30_000;
  let latestState: CASOSResultsDiagnostics | null = null;

  while (Date.now() - start < timeoutMs) {
    latestState = await inspectCASOSResultsState(page);
    const interpreted = interpretCASOSResultsState(latestState, options);
    if (interpreted) {
      log({
        stage: 'ca_sos_results_ready',
        final_url: latestState.finalUrl,
        row_count: interpreted.rowCount,
        drawer_button_count: latestState.drawerButtonCount,
        result_count: interpreted.resultCount,
        no_results: interpreted.hasNoResults,
        results_container_visible: latestState.resultsContainerVisible,
      });
      return interpreted;
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  log({
    stage: 'results_visibility_timeout',
    diagnostics: latestState,
  });
  if (latestState?.resultCount !== null && latestState?.resultCount !== undefined) {
    throw new Error(`results_rows_not_visible_after_search result_count=${latestState.resultCount}`);
  }
  if (latestState?.visibleErrorText) {
    throw new Error(`results_not_visible_after_search error_text=${latestState.visibleErrorText}`);
  }
  throw new Error('results_not_visible_after_search');
}

async function executeCASOSSearch(
  page: Page,
  date_start: string,
  date_end: string,
  options: { allowContainerOnlyCount?: boolean } = {},
): Promise<CASOSSearchResult> {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= 2; attempt += 1) {
    try {
      if (attempt === 1) {
        await submitCASOSSearch(page, date_start, date_end);
      } else {
        log({ stage: 'ca_sos_search_retry_in_page', date_start, date_end, attempt });
        await navigateToCASOSSearch(page);
        const resolvedRange = await resolveCASOSRequestedDateRange(page, date_start, date_end);
        await fillCASOSSearchForm(page, resolvedRange.dateStart, resolvedRange.dateEnd);
        await page.getByRole('button', { name: 'Search' }).click();
        await page.waitForLoadState('networkidle').catch(() => null);
        await humanDelay();
      }

      return await waitForResultsOrNoResults(page, options);
    } catch (err: unknown) {
      lastError = err instanceof Error ? err : new Error(String(err));
      log({
        stage: 'ca_sos_search_attempt_failed',
        date_start,
        date_end,
        attempt,
        error: lastError.message,
      });
      if (attempt >= 2) break;
    }
  }

  throw lastError ?? new Error('ca_sos_search_failed');
}

export async function probeCASOSResultCount(options: Pick<ScrapeOptions, 'date_start' | 'date_end'>): Promise<number> {
  const handle = await createIsolatedBrowserContext();
  const context = handle.context;
  const page = await context.newPage();

  try {
    await configureCASOSSearchPage(page);
    const results = await executeCASOSSearch(page, options.date_start, options.date_end, { allowContainerOnlyCount: true });
    log({
      stage: 'ca_sos_probe_complete',
      date_start: options.date_start,
      date_end: options.date_end,
      result_count: results.resultCount,
      row_count: results.rowCount,
      no_results: results.hasNoResults,
    });
    return results.resultCount;
  } finally {
    await context.close();
    await handle.close();
  }
}

function loadQuarantinedRows(key: string): QuarantinedCASOSRow[] {
  try {
    const quarantinePath = getQuarantinePath(key);
    if (!fs.existsSync(quarantinePath)) return [];
    const parsed = JSON.parse(fs.readFileSync(quarantinePath, 'utf8'));
    return Array.isArray(parsed) ? parsed as QuarantinedCASOSRow[] : [];
  } catch {
    return [];
  }
}

function saveQuarantinedRows(key: string, rows: QuarantinedCASOSRow[]): void {
  const quarantinePath = getQuarantinePath(key);
  fs.writeFileSync(quarantinePath, JSON.stringify(rows, null, 2));
}

function clearQuarantinedRows(key: string): void {
  const quarantinePath = getQuarantinePath(key);
  if (fs.existsSync(quarantinePath)) {
    fs.unlinkSync(quarantinePath);
  }
}

async function closeDrawer(page: Page): Promise<void> {
  const drawer = page.locator('div.drawer.show');
  if (!(await drawer.isVisible({ timeout: 1000 }).catch(() => false))) return;

  const closeBtn = drawer.locator('button.close-button');
  if (await closeBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
    await closeBtn.click({ force: true }).catch(() => {});
    await page.waitForTimeout(1000);
  }

  if (await drawer.isVisible({ timeout: 500 }).catch(() => false)) {
    await page.evaluate(() => {
      const d = document.querySelector('div.drawer.show');
      if (d) d.classList.remove('show');
    });
    await page.waitForTimeout(300);
  }
}

async function getDetailField(page: Page, label: string): Promise<string> {
  const drawer = page.locator('table.details-list');
  const row = drawer.locator(`tr.detail`).filter({ has: page.locator(`td.label:has-text("${label}")`) });
  return (await row.locator('td.value').textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
}

function buildBaseCARecord(input: {
  fileNumber: string;
  filingDateText: string;
  lapseDateText: string;
  statusText: string;
  debtorName: string;
  debtorAddress: string;
  securedPartyName: string;
  securedPartyAddress: string;
}): LienRecord {
  return {
    state: 'CA',
    source: 'ca_sos',
    county: '',
    ucc_type: 'Federal Tax Lien',
    debtor_name: input.debtorName,
    debtor_address: input.debtorAddress,
    file_number: input.fileNumber,
    secured_party_name: input.securedPartyName,
    secured_party_address: input.securedPartyAddress,
    status: input.statusText || 'Active',
    filing_date: input.filingDateText,
    lapse_date: input.lapseDateText || '12/31/9999',
    document_type: 'Notice of Federal Tax Lien',
    pdf_filename: '',
    processed: true,
    error: '',
  };
}

async function processDetailRow(page: Page, rowIndex: number): Promise<LienRecord | null> {
  let fileNumber = '';
  let filingDateText = '';
  let debtorName = '';
  let debtorAddress = '';
  let securedPartyName = '';
  let securedPartyAddress = '';
  try {
    log({ stage: 'detail_process_start', row: rowIndex });

    const rows = page.locator('.div-table-row');
    const row = rows.nth(rowIndex);

    const cells = row.locator('.div-table-cell .cell');
    fileNumber = (await cells.nth(2).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
    filingDateText = (await cells.nth(5).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
    const lapseDateText = (await cells.nth(6).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
    const statusText = (await cells.nth(4).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';

    await row.locator('.interactive-cell-button').first().click();

    const drawer = page.locator('div.drawer.show');
    await drawer.waitFor({ state: 'visible', timeout: 15000 });
    await humanDelay();

    debtorName = await getDetailField(page, 'Debtor Name');
    debtorAddress = await getDetailField(page, 'Debtor Address');
    securedPartyName = await getDetailField(page, 'Secured Party Name');
    securedPartyAddress = await getDetailField(page, 'Secured Party Address');

    log({ stage: 'detail_extracted_basic', file_number: fileNumber });

    const record = buildBaseCARecord({
      fileNumber,
      filingDateText,
      lapseDateText,
      statusText,
      debtorName,
      debtorAddress,
      securedPartyName,
      securedPartyAddress,
    });

    if (!record.file_number || !record.filing_date || !record.debtor_name) {
      throw new Error('detail_base_fields_missing');
    }

    let pdfData: PdfExtraction = {};
    let partialReason: string | undefined;

    const historyBtn = drawer.locator('button[aria-label="View History"]');
    if (await historyBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
      try {
        await historyBtn.click();

        const historyModal = page.locator('div.history-modal[role="dialog"]');
        await historyModal.waitFor({ state: 'visible', timeout: 10000 });
        await humanDelay();

        const downloadLink = historyModal.getByRole('link', { name: /Download/i }).first();
        if (await downloadLink.isVisible({ timeout: 5000 }).catch(() => false)) {
          const downloadDir = path.join(process.cwd(), 'data/downloads');
          if (!fs.existsSync(downloadDir)) {
            fs.mkdirSync(downloadDir, { recursive: true });
          }
          const pdfPath = path.join(downloadDir, `${fileNumber}.pdf`);

          try {
            const dlHref = await downloadLink.getAttribute('href');
            let pdfBuffer: Buffer | null = null;

            if (dlHref) {
              const fetchUrl = dlHref.startsWith('http') ? dlHref : `https://bizfileonline.sos.ca.gov${dlHref}`;
              const b64 = await page.evaluate(async (url: string) => {
                try {
                  const resp = await fetch(url, { credentials: 'include' });
                  const buf = await resp.arrayBuffer();
                  const bytes = new Uint8Array(buf);
                  let binary = '';
                  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
                  return btoa(binary);
                } catch { return ''; }
              }, fetchUrl);

              if (b64) pdfBuffer = Buffer.from(b64, 'base64');
            }

            if (pdfBuffer && pdfBuffer.length > 500) {
              fs.writeFileSync(pdfPath, pdfBuffer);
              log({ stage: 'detail_pdf_downloaded', file_number: fileNumber, size: pdfBuffer.length });
              pdfData = await extractFromPDF(pdfPath);
              log({ stage: 'detail_pdf_parsed', file_number: fileNumber, amount: pdfData.amount, lead_type: pdfData.leadType });
              try { fs.unlinkSync(pdfPath); } catch { /* ignore cleanup errors */ }
            } else {
              log({ stage: 'detail_pdf_skipped', file_number: fileNumber, size: pdfBuffer?.length ?? 0 });
              partialReason = partialReason ?? 'pdf_unavailable';
              appendRecordError(record, 'pdf_unavailable');
            }
          } catch (err: unknown) {
            const message = err instanceof Error ? err.message : String(err);
            log({ stage: 'detail_pdf_failed', file_number: fileNumber, error: message });
            partialReason = partialReason ?? 'pdf_fetch_failed';
            appendRecordError(record, 'pdf_fetch_failed');
          }
        }
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        log({ stage: 'detail_history_failed', file_number: fileNumber, error: message });
        partialReason = partialReason ?? 'history_modal_failed';
        appendRecordError(record, 'history_modal_failed');
      }

      await dismissHistoryModal(page);
    } else {
      partialReason = partialReason ?? 'history_unavailable';
      appendRecordError(record, 'history_unavailable');
    }

    await closeDrawer(page);
    await humanDelay();

    record.amount = pdfData.amount;
    record.amount_confidence = pdfData.amountConfidence;
    record.amount_reason = pdfData.amountReason;
    record.confidence_score = resolveCARecordConfidenceScore(debtorName, debtorAddress, pdfData);
    record.lead_type = pdfData.leadType;

    if (pdfData.amountReason && pdfData.amountReason !== 'ok') {
      partialReason = partialReason ?? 'ocr_or_pdf_incomplete';
      appendRecordError(record, pdfData.amountReason);
    }

    if (partialReason) {
      log({
        stage: 'detail_row_degraded_published',
        file_number: fileNumber,
        partial_reason: partialReason,
        error: record.error,
      });
    }

    log({ stage: 'detail_mapped', file_number: record.file_number });
    return record;

  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    log({ stage: 'detail_row_failed', row: rowIndex, error: message });

    await dismissHistoryModal(page).catch(() => {});
    await closeDrawer(page).catch(() => {});

    if (fileNumber && filingDateText && debtorName) {
      const degradedRecord = buildBaseCARecord({
        fileNumber,
        filingDateText,
        lapseDateText: '',
        statusText: 'Active',
        debtorName,
        debtorAddress,
        securedPartyName,
        securedPartyAddress,
      });
      appendRecordError(degradedRecord, 'detail_enrichment_failed');
      log({
        stage: 'detail_row_degraded_from_catch',
        row: rowIndex,
        file_number: fileNumber,
        error: degradedRecord.error,
      });
      return degradedRecord;
    }

    return null;
  }
}

export async function scrapeCASOS_Enhanced(options: ScrapeOptions): Promise<ScrapeResult> {
  const {
    date_start,
    date_end,
    max_records = 1000,
    chunk_size = 25,
    start_index = 0,
    max_chunk_retries = 5,
    checkpoint_key = `${date_start}_${date_end}`,
    deadline_at_iso,
    stop_requested,
  } = options;
  const queue = new SQLiteQueueStore();
  const processedRecords: LienRecord[] = [];
  const checkpoint = loadCheckpoint(checkpoint_key);
  const quarantinedRows = loadQuarantinedRows(checkpoint_key);
  let nextIndex = Math.max(start_index, checkpoint?.next_index ?? 0);
  let knownRowCount = Number.POSITIVE_INFINITY;
  let successfulChunks = 0;
  let failedChunks = 0;
  let skippedExistingRecords = 0;
  let firstChunkStart: number | null = null;
  let lastChunkStart: number | null = null;
  let partialRecords = 0;
  let degradedPublishedRecords = 0;
  let historyFailureCount = 0;
  let pdfFailureCount = 0;
  let ocrFailureCount = 0;
  const deadlineMs = deadline_at_iso ? new Date(deadline_at_iso).getTime() : null;
  const requireOCR = process.env.REQUIRE_OCR_TOOLS !== '0';
  if (requireOCR) {
    const ocrState = checkOCRRuntime();
    if (!ocrState.ok) {
      throw new Error(`OCR runtime not ready: ${ocrState.detail ?? ocrState.missing.join(', ')}`);
    }
  }

  log({
    stage: 'scraper_start',
    site: 'ca_sos',
    date_start,
    date_end,
    chunk_size,
    max_chunk_retries,
    start_index,
    resume_index: nextIndex,
  });

  while (processedRecords.length < max_records && nextIndex < knownRowCount) {
    if (stop_requested?.()) {
      log({ stage: 'scraper_stop_requested', processed: processedRecords.length, next_index: nextIndex });
      break;
    }
    if (deadlineMs !== null && Date.now() >= deadlineMs) {
      log({ stage: 'scraper_deadline_reached', processed: processedRecords.length, next_index: nextIndex, deadline_at_iso });
      break;
    }
    const chunkStart = nextIndex;
    let chunkEndExclusive = chunkStart + chunk_size;
    let lastErr: Error | null = null;
    let chunkRowCount = knownRowCount;
    const chunkRecords: LienRecord[] = [];
    let confirmedNextIndex = nextIndex;

    if (firstChunkStart === null) firstChunkStart = chunkStart;
    lastChunkStart = chunkStart;

    for (let attempt = 0; attempt < max_chunk_retries; attempt++) {
      log({
        stage: 'chunk_start',
        chunk_start: chunkStart,
        chunk_end_exclusive: chunkEndExclusive,
        attempt: attempt + 1,
      });

      const handle = await createIsolatedBrowserContext();
      const context = handle.context;
      const page = await context.newPage();

      try {
        await configureCASOSSearchPage(page);
        const resultsInfo = await executeCASOSSearch(page, date_start, date_end, { allowContainerOnlyCount: false });
        chunkRowCount = resultsInfo.rowCount;
        knownRowCount = Number.isFinite(knownRowCount)
          ? Math.min(knownRowCount, resultsInfo.resultCount)
          : resultsInfo.resultCount;

        if (resultsInfo.hasNoResults || chunkRowCount === 0) {
          log({
            stage: 'chunk_no_results',
            chunk_start: chunkStart,
            chunk_end_exclusive: chunkEndExclusive,
          });
          nextIndex = chunkEndExclusive;
          lastErr = null;
          break;
        }
        chunkEndExclusive = Math.min(chunkEndExclusive, knownRowCount);

        log({
          stage: 'chunk_results_found',
          row_count: chunkRowCount,
          chunk_start: chunkStart,
          chunk_end_exclusive: chunkEndExclusive,
          already_processed: processedRecords.length,
        });

        let consecutiveFailures = 0;
        for (let i = chunkStart; i < chunkEndExclusive; i++) {
          if (stop_requested?.()) break;
          if (deadlineMs !== null && Date.now() >= deadlineMs) break;
          if (processedRecords.length + chunkRecords.length >= max_records) {
            break;
          }

          const cells = page.locator('.div-table-row').nth(i).locator('.div-table-cell .cell');
          const peekFileNumber = (await cells.nth(2).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
          const peekFilingDate = (await cells.nth(5).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';

          if (peekFileNumber && peekFilingDate) {
            const fp = computeFingerprint('ca_sos', peekFileNumber, peekFilingDate);
            if (queue.hasFingerprint(fp)) {
              skippedExistingRecords += 1;
              saveCheckpoint(checkpoint_key, i + 1);
              confirmedNextIndex = i + 1;
              continue;
            }
          }

          let record: LienRecord | null = null;
          for (let attempt = 1; attempt <= 2; attempt += 1) {
            record = await processDetailRow(page, i);
            if (record) break;
            log({
              stage: 'detail_row_retry',
              site: 'ca_sos',
              row: i,
              attempt,
              file_number: peekFileNumber,
              filing_date: peekFilingDate,
            });
          }

          if (record) {
            consecutiveFailures = 0;
            chunkRecords.push(record);
            if (record.error) {
              degradedPublishedRecords += 1;
              partialRecords += 1;
              if (record.error.includes('history_')) historyFailureCount += 1;
              if (record.error.includes('pdf_')) pdfFailureCount += 1;
              if (record.error.includes('ocr_')) ocrFailureCount += 1;
            }
          } else {
            consecutiveFailures++;
            quarantinedRows.push({
              row_index: i,
              file_number: peekFileNumber || undefined,
              filing_date: peekFilingDate || undefined,
              reason: 'detail_row_failed_after_retries',
              quarantined_at: new Date().toISOString(),
            });
            saveQuarantinedRows(checkpoint_key, quarantinedRows);
            log({
              stage: 'detail_row_quarantined',
              site: 'ca_sos',
              row: i,
              file_number: peekFileNumber,
              filing_date: peekFilingDate,
              reason: 'detail_row_failed_after_retries',
            });
          }

          saveCheckpoint(checkpoint_key, i + 1);
          confirmedNextIndex = i + 1;
          await humanDelay();
        }

        nextIndex = confirmedNextIndex;
        lastErr = null;
        successfulChunks += 1;
        break;
      } catch (err: unknown) {
        lastErr = err instanceof Error ? err : new Error(String(err));
        log({
          stage: 'chunk_error',
          chunk_start: chunkStart,
          chunk_end_exclusive: chunkEndExclusive,
          attempt: attempt + 1,
          error: lastErr.message,
        });
        await backoffDelay(attempt);
      } finally {
        await page.close().catch(() => {});
        await handle.close().catch(() => {});
      }
    }

    processedRecords.push(...chunkRecords);

    if (lastErr) {
      log({
        stage: 'chunk_persistent_failure',
        chunk_start: chunkStart,
        chunk_end_exclusive: chunkEndExclusive,
        row_count: chunkRowCount,
        error: lastErr.message,
      });
      nextIndex = confirmedNextIndex;
      saveCheckpoint(checkpoint_key, nextIndex);
      failedChunks += 1;
      if (failedChunks >= 8) {
        log({
          stage: 'scraper_abort_after_failed_chunks',
          failed_chunks: failedChunks,
          processed: processedRecords.length,
        });
        break;
      }
    }
  }

  if (processedRecords.length >= max_records || nextIndex >= knownRowCount) {
    clearCheckpoint(checkpoint_key);
    clearQuarantinedRows(checkpoint_key);
  }

  log({
    stage: 'scraper_complete',
    processed: processedRecords.length,
    scraped_records: processedRecords.length,
    quarantined_records: quarantinedRows.length,
    skipped_existing_records: skippedExistingRecords,
    next_index: nextIndex,
    row_count: Number.isFinite(knownRowCount) ? knownRowCount : null,
    total_chunks_successful: successfulChunks,
    total_chunks_failed: failedChunks,
    first_chunk_start: firstChunkStart,
    last_chunk_start: lastChunkStart,
    partial_records: partialRecords,
    degraded_published_records: degradedPublishedRecords,
    history_failure_count: historyFailureCount,
    pdf_failure_count: pdfFailureCount,
    ocr_failure_count: ocrFailureCount,
  });

  return attachScrapeQualitySummary(processedRecords, {
    requested_date_start: date_start,
    requested_date_end: date_end,
    discovered_count: Number.isFinite(knownRowCount) ? knownRowCount : undefined,
    returned_count: processedRecords.length,
    quarantined_count: quarantinedRows.length,
    partial_run: quarantinedRows.length > 0 || failedChunks > 0 || partialRecords > 0 || nextIndex < knownRowCount,
    partial_reason: quarantinedRows.length > 0
      ? 'quarantined_failed_rows'
      : partialRecords > 0
        ? 'degraded_rows_published'
      : failedChunks > 0
        ? 'chunk_failures_remaining'
        : nextIndex < knownRowCount
          ? 'deadline_or_stop_before_completion'
          : undefined,
    skipped_existing_count: skippedExistingRecords,
    partial_records: partialRecords,
    enriched_records: processedRecords.length - partialRecords,
    debug_artifact: {
      degraded_published_records: degradedPublishedRecords,
      history_failure_count: historyFailureCount,
      pdf_failure_count: pdfFailureCount,
      ocr_failure_count: ocrFailureCount,
    },
  });
}










