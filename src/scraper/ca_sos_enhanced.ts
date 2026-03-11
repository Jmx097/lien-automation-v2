import { Page } from 'playwright';
import { LienRecord } from '../types';
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
}

interface ScrapeCheckpoint {
  next_index: number;
  updated_at: string;
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

async function submitCASOSSearch(page: Page, date_start: string, date_end: string): Promise<void> {
  await page.goto('https://bizfileonline.sos.ca.gov/search/ucc', {
    waitUntil: 'networkidle',
    timeout: 60000,
  });

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

  await page.getByRole('button', { name: 'Search' }).click();
  await page.waitForLoadState('networkidle');
  await humanDelay();
}

async function waitForResultsOrNoResults(page: Page): Promise<CASOSSearchResult> {
  const rowsLocator = page.locator('.div-table-row');
  const resultsTextLocator = page.locator('text=/Results:\\s*\\d+/');
  const noResultsLocator = page.locator('text=/No\\s+records\\s+found/i');

  const start = Date.now();
  const timeoutMs = 30_000;
  let latestResultCount: number | null = null;

  while (Date.now() - start < timeoutMs) {
    const rowCount = await rowsLocator.count();

    const hasResultsText = await resultsTextLocator.isVisible({ timeout: 500 }).catch(() => false);
    if (hasResultsText) {
      const resultsText = (await resultsTextLocator.first().textContent().catch(() => '')) ?? '';
      latestResultCount = parseCASOSResultsCount(resultsText);
      if (latestResultCount === null) {
        throw new Error(`results_count_parse_failed text=${resultsText}`);
      }
      if (latestResultCount === 0) {
        return { rowCount: 0, hasNoResults: true, resultCount: 0 };
      }
      if (rowCount > 0) {
        return { rowCount, hasNoResults: false, resultCount: latestResultCount };
      }
    }

    const hasNoResults = await noResultsLocator.isVisible({ timeout: 500 }).catch(() => false);
    if (hasNoResults) {
      return { rowCount: 0, hasNoResults: true, resultCount: 0 };
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  log({ stage: 'results_visibility_timeout' });
  if (latestResultCount !== null) {
    throw new Error(`results_rows_not_visible_after_search result_count=${latestResultCount}`);
  }
  throw new Error('results_not_visible_after_search');
}

export async function probeCASOSResultCount(options: Pick<ScrapeOptions, 'date_start' | 'date_end'>): Promise<number> {
  const handle = await createIsolatedBrowserContext();
  const context = handle.context;
  const page = await context.newPage();

  try {
    await configureCASOSSearchPage(page);
    await submitCASOSSearch(page, options.date_start, options.date_end);
    const results = await waitForResultsOrNoResults(page);
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

async function processDetailRow(page: Page, rowIndex: number): Promise<LienRecord | null> {
  try {
    log({ stage: 'detail_process_start', row: rowIndex });

    const rows = page.locator('.div-table-row');
    const row = rows.nth(rowIndex);

    const cells = row.locator('.div-table-cell .cell');
    const fileNumber = (await cells.nth(2).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
    const filingDateText = (await cells.nth(5).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
    const lapseDateText = (await cells.nth(6).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
    const statusText = (await cells.nth(4).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';

    await row.locator('.interactive-cell-button').first().click();

    const drawer = page.locator('div.drawer.show');
    await drawer.waitFor({ state: 'visible', timeout: 15000 });
    await humanDelay();

    const debtorName = await getDetailField(page, 'Debtor Name');
    const debtorAddress = await getDetailField(page, 'Debtor Address');
    const securedPartyName = await getDetailField(page, 'Secured Party Name');
    const securedPartyAddress = await getDetailField(page, 'Secured Party Address');

    log({ stage: 'detail_extracted_basic', file_number: fileNumber });

    let pdfData: PdfExtraction = {};

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
            }
          } catch (err: unknown) {
            const message = err instanceof Error ? err.message : String(err);
            log({ stage: 'detail_pdf_failed', file_number: fileNumber, error: message });
          }
        }
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        log({ stage: 'detail_history_failed', file_number: fileNumber, error: message });
      }

      await dismissHistoryModal(page);
    }

    await closeDrawer(page);
    await humanDelay();

    const record: LienRecord = {
      state: 'CA',
      source: 'ca_sos',
      county: '',
      ucc_type: 'Federal Tax Lien',
      debtor_name: debtorName,
      debtor_address: debtorAddress,
      file_number: fileNumber,
      secured_party_name: securedPartyName,
      secured_party_address: securedPartyAddress,
      status: statusText || 'Active',
      filing_date: filingDateText,
      lapse_date: lapseDateText || '12/31/9999',
      document_type: 'Notice of Federal Tax Lien',
      pdf_filename: '',
      processed: true,
      error: '',
      amount: pdfData.amount,
      amount_confidence: pdfData.amountConfidence,
      amount_reason: pdfData.amountReason,
      confidence_score: pdfData.amountConfidence,
      lead_type: pdfData.leadType,
    };

    log({ stage: 'detail_mapped', file_number: record.file_number });
    return record;

  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    log({ stage: 'detail_row_failed', row: rowIndex, error: message });

    await dismissHistoryModal(page).catch(() => {});
    await closeDrawer(page).catch(() => {});
    return null;
  }
}

export async function scrapeCASOS_Enhanced(options: ScrapeOptions): Promise<LienRecord[]> {
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
  let nextIndex = Math.max(start_index, checkpoint?.next_index ?? 0);
  let knownRowCount = Number.POSITIVE_INFINITY;
  let successfulChunks = 0;
  let failedChunks = 0;
  let firstChunkStart: number | null = null;
  let lastChunkStart: number | null = null;
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
        await submitCASOSSearch(page, date_start, date_end);

        const resultsInfo = await waitForResultsOrNoResults(page);
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
              saveCheckpoint(checkpoint_key, i + 1);
              continue;
            }
          }

          const record = await processDetailRow(page, i);
          if (record) {
            consecutiveFailures = 0;
            chunkRecords.push(record);
          } else {
            consecutiveFailures++;
            if (consecutiveFailures >= 5) {
              throw new Error(`chunk_failed_consecutive_rows_${consecutiveFailures}`);
            }
          }

          saveCheckpoint(checkpoint_key, i + 1);
          await humanDelay();
        }

        nextIndex = chunkEndExclusive;
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
      nextIndex = chunkEndExclusive;
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
  }

  log({
    stage: 'scraper_complete',
    processed: processedRecords.length,
    next_index: nextIndex,
    row_count: Number.isFinite(knownRowCount) ? knownRowCount : null,
    total_chunks_successful: successfulChunks,
    total_chunks_failed: failedChunks,
    first_chunk_start: firstChunkStart,
    last_chunk_start: lastChunkStart,
  });

  return processedRecords;
}










