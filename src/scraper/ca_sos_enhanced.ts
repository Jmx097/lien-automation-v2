import { chromium, Page } from 'playwright';
import { LienRecord } from '../types';
import { log } from '../utils/logger';
import { SQLiteQueueStore } from '../queue/sqlite';
import { pushToSheets } from '../sheets/push';
import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import crypto from 'crypto';
import { captureFileTypeSelectionFailureDebug } from './file_type_debug';
import { selectFileType } from './selectors/fileType';

const SBR_CDP_URL = process.env.SBR_CDP_URL!;

interface ScrapeOptions {
  date_start: string;
  date_end: string;
  max_records?: number;
}

function humanDelay() {
  return new Promise(resolve => setTimeout(resolve, 800 + Math.random() * 400));
}

function computeFingerprint(source: string, fileNumber: string, filingDate: string): string {
  return crypto.createHash('sha256').update(`${source}-${fileNumber}-${filingDate}`).digest('hex');
}

function getRandomSessionCDP(): string {
  const sessionId = `session_${Date.now()}_${crypto.randomBytes(8).toString('hex')}`;
  const separator = SBR_CDP_URL.includes('?') ? '&' : '?';
  return `${SBR_CDP_URL}${separator}session=${sessionId}`;
}

interface PdfExtraction {
  amount?: string;
  leadType?: string;
  taxpayerName?: string;
  residence?: string;
}

function ocrPdf(pdfPath: string): string {
  const dir = path.dirname(pdfPath);
  const base = path.basename(pdfPath, '.pdf');
  const imgPrefix = path.join(dir, `${base}_page`);
  const ocrOutput = path.join(dir, `${base}_ocr`);

  try {
    execSync(`pdftoppm -png -r 300 "${pdfPath}" "${imgPrefix}"`, { timeout: 15000 });

    const imgFiles = fs.readdirSync(dir)
      .filter(f => f.startsWith(`${base}_page`) && f.endsWith('.png'))
      .sort()
      .map(f => path.join(dir, f));

    if (imgFiles.length === 0) return '';

    let fullText = '';
    for (const imgFile of imgFiles) {
      execSync(`tesseract "${imgFile}" "${ocrOutput}" --psm 6 2>/dev/null`, { timeout: 30000 });
      if (fs.existsSync(`${ocrOutput}.txt`)) {
        fullText += fs.readFileSync(`${ocrOutput}.txt`, 'utf-8') + '\n';
        fs.unlinkSync(`${ocrOutput}.txt`);
      }
      fs.unlinkSync(imgFile);
    }

    return fullText;
  } catch (err: any) {
    log({ stage: 'ocr_error', error: err.message });
    return '';
  }
}

async function extractFromPDF(pdfPath: string): Promise<PdfExtraction> {
  try {
    const text = ocrPdf(pdfPath);
    if (!text) return {};

    log({ stage: 'pdf_ocr_extracted', length: text.length, preview: text.substring(0, 200) });

    let amount: string | undefined;
    const totalMatch = text.match(/Total\s*\|?\s*\$\s*([\d,]+(?:\.\d+)?)/i);
    if (totalMatch) {
      const raw = totalMatch[1].replace(/,/g, '');
      amount = String(Math.floor(parseFloat(raw)));
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

    log({ stage: 'pdf_fields_extracted', amount, leadType, taxpayerName: taxpayerName?.substring(0, 50), residence: residence?.substring(0, 50) });
    return { amount, leadType, taxpayerName, residence };
  } catch (err: any) {
    log({ stage: 'pdf_parse_error', error: err.message });
    return {};
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
              try { fs.unlinkSync(pdfPath); } catch {}
            } else {
              log({ stage: 'detail_pdf_skipped', file_number: fileNumber, size: pdfBuffer?.length ?? 0 });
            }
          } catch (err: any) {
            log({ stage: 'detail_pdf_failed', file_number: fileNumber, error: err.message });
          }
        }
      } catch (err: any) {
        log({ stage: 'detail_history_failed', file_number: fileNumber, error: err.message });
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
      lead_type: pdfData.leadType,
    };

    log({ stage: 'detail_mapped', file_number: record.file_number });
    return record;

  } catch (err: any) {
    log({ stage: 'detail_row_failed', row: rowIndex, error: err.message });

    await dismissHistoryModal(page).catch(() => {});
    await closeDrawer(page).catch(() => {});
    return null;
  }
}

export async function scrapeCASOS_Enhanced(options: ScrapeOptions): Promise<LienRecord[]> {
  const { date_start, date_end, max_records = 10 } = options;
  const queue = new SQLiteQueueStore();
  const processedRecords: LienRecord[] = [];

  log({ stage: 'scraper_start', site: 'ca_sos', date_start, date_end });

  const cdpUrl = getRandomSessionCDP();
  log({ stage: 'cdp_session', has_session_param: cdpUrl.includes('session=') });

  const browser = await chromium.connectOverCDP(cdpUrl);
  const context = browser.contexts()[0];
  const page = await context.newPage();

  try {
    await page.goto('https://bizfileonline.sos.ca.gov/search/ucc', {
      waitUntil: 'networkidle',
      timeout: 90000
    });

    const searchInput = page.getByLabel('Search by name or file number');
    await searchInput.waitFor({ state: 'visible', timeout: 90000 });
    await humanDelay();

    log({ stage: 'fill_search' });
    await searchInput.fill('Internal Revenue Service');
    await humanDelay();

    const advancedBtn = page.getByRole('button', { name: /Advanced/i });
    await advancedBtn.waitFor({ state: 'visible' });
    await advancedBtn.click();
    await humanDelay();

    const fileTypeSelected = await selectFileType(page, {
      log,
      onFailure: () => captureFileTypeSelectionFailureDebug(page),
    });

    if (!fileTypeSelected) {
      throw new Error('Could not find/select File Type control after opening Advanced search.');
    }
    await humanDelay();

    const dateStartInput = page.getByRole('textbox', { name: 'File Date: Start' });
    const dateEndInput = page.getByRole('textbox', { name: 'File Date: End' });
    await dateStartInput.fill(date_start);
    await humanDelay();
    await dateEndInput.fill(date_end);
    await dateEndInput.press('Tab');
    await humanDelay();

    log({ stage: 'submit_search' });
    await page.getByRole('button', { name: 'Search' }).click();
    await page.waitForLoadState('networkidle');
    await humanDelay();

    const resultLocator = page.locator('text=/Results:\\s*\\d+/');
    await resultLocator.waitFor({ state: 'visible', timeout: 30000 });

    const rowCount = await page.locator('.div-table-row').count();
    const toProcess = Math.min(rowCount, max_records);

    log({ stage: 'scraper_results_found', count: rowCount, processing: toProcess });

    let consecutiveFailures = 0;
    let newRecordCount = 0;
    for (let i = 0; i < rowCount; i++) {
      if (newRecordCount >= max_records) break;

      if (page.isClosed()) {
        log({ stage: 'scraper_page_closed', row: i });
        break;
      }

      const cells = page.locator('.div-table-row').nth(i).locator('.div-table-cell .cell');
      const peekFileNumber = (await cells.nth(2).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
      const peekFilingDate = (await cells.nth(5).textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';

      if (peekFileNumber && peekFilingDate) {
        const fp = computeFingerprint('ca_sos', peekFileNumber, peekFilingDate);
        if (queue.hasFingerprint(fp)) {
          log({ stage: 'scraper_skip_duplicate', row: i, file_number: peekFileNumber });
          continue;
        }
      }

      const record = await processDetailRow(page, i);

      if (record) {
        consecutiveFailures = 0;
        await pushToSheets([record]);
        log({ stage: 'scraper_pushed_sheet', file_number: record.file_number });

        await queue.insertMany([record]);
        processedRecords.push(record);
        newRecordCount++;
      } else {
        consecutiveFailures++;
        if (consecutiveFailures >= 3) {
          log({ stage: 'scraper_too_many_failures', consecutive: consecutiveFailures, stopping: true });
          break;
        }
      }

      await humanDelay();
    }

    log({ stage: 'scraper_complete', processed: processedRecords.length });

  } catch (err: any) {
    log({ stage: 'scraper_error', error: err.message });
    throw err;
  } finally {
    await page.close().catch(() => {});
    await browser.close().catch(() => {});
  }

  return processedRecords;
}
