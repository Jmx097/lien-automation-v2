import { chromium, Page } from 'playwright';
import { LienRecord } from '../types';
import { log } from '../utils/logger';
import { SQLiteQueueStore } from '../queue/sqlite';
import { pushToSheets } from '../sheets/push';
import fs from 'fs';
import path from 'path';
import * as pdfParse from 'pdf-parse';
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

async function extractFromPDF(pdfPath: string): Promise<PdfExtraction> {
  try {
    const fileBuffer = fs.readFileSync(pdfPath);
    const uint8 = new Uint8Array(fileBuffer.buffer, fileBuffer.byteOffset, fileBuffer.byteLength);
    const { PDFParse } = require('pdf-parse') as { PDFParse: any };
    const parser = new PDFParse(uint8);
    await parser.load();
    const rawText = await parser.getText();
    const text: string = typeof rawText === 'string' ? rawText : JSON.stringify(rawText);

    log({ stage: 'pdf_text_extracted', length: text.length, preview: text.substring(0, 300), type: typeof rawText });

    const totalMatch = text.match(/Total\s*\$?\s*([\d,]+(?:\.\d+)?)/i);
    let amount: string | undefined;
    if (totalMatch) {
      const raw = totalMatch[1].replace(/,/g, '');
      amount = String(Math.floor(parseFloat(raw)));
    }

    let leadType: string | undefined;
    if (/Certificate\s+of\s+Release/i.test(text)) {
      leadType = 'Release';
    } else if (/Notice\s+of\s+Federal\s+Tax\s+Li/i.test(text)) {
      leadType = 'Lien';
    }

    const nameMatch = text.match(/Name\s+of\s+Taxpayer\s+(.+?)(?:\n|Residence)/is);
    const taxpayerName = nameMatch ? nameMatch[1].trim() : undefined;

    const residenceMatch = text.match(/Residence\s+(.+?)(?:\n.*?IMPORTANT|$)/is);
    const residence = residenceMatch ? residenceMatch[1].trim() : undefined;

    await parser.destroy();
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
            const [download] = await Promise.all([
              page.waitForEvent('download', { timeout: 15000 }),
              downloadLink.click()
            ]);

            let saved = false;
            try {
              await download.saveAs(pdfPath);
              if (fs.existsSync(pdfPath) && fs.statSync(pdfPath).size > 100) saved = true;
            } catch { /* remote CDP - saveAs may not work */ }

            if (!saved) {
              try {
                const readable = await download.createReadStream();
                if (readable) {
                  const chunks: Buffer[] = [];
                  for await (const chunk of readable) chunks.push(Buffer.from(chunk));
                  const buf = Buffer.concat(chunks);
                  if (buf.length > 100) {
                    fs.writeFileSync(pdfPath, buf);
                    saved = true;
                  }
                }
              } catch { /* stream not available */ }
            }

            if (saved) {
              log({ stage: 'detail_pdf_downloaded', file_number: fileNumber, size: fs.statSync(pdfPath).size });
              pdfData = await extractFromPDF(pdfPath);
              log({ stage: 'detail_pdf_parsed', file_number: fileNumber, amount: pdfData.amount, lead_type: pdfData.leadType });
              try { fs.unlinkSync(pdfPath); } catch {}
            } else {
              log({ stage: 'detail_pdf_skipped', file_number: fileNumber });
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
    for (let i = 0; i < toProcess; i++) {
      if (page.isClosed()) {
        log({ stage: 'scraper_page_closed', row: i });
        break;
      }

      const record = await processDetailRow(page, i);

      if (record) {
        consecutiveFailures = 0;
        await pushToSheets([record]);
        log({ stage: 'scraper_pushed_sheet', file_number: record.file_number });

        await queue.insertMany([record]);

        processedRecords.push(record);
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
