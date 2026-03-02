import { chromium, Page } from 'playwright';
import { LienRecord } from '../types';
import { log } from '../utils/logger';
import { SQLiteQueueStore } from '../queue/sqlite';
import { pushToSheets } from '../sheets/push';
import fs from 'fs';
import path from 'path';
import * as pdfParse from 'pdf-parse';
import crypto from 'crypto';

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

async function extractFromPDF(pdfPath: string): Promise<{ amount?: string; lienDate?: string }> {
  try {
    const dataBuffer = fs.readFileSync(pdfPath);
    const data = await (pdfParse as any)(dataBuffer);
    const text = data.text;
    
    const amountMatch = text.match(/Total[:\s]+\$?([\d,]+\.?\d*)/i);
    const amount = amountMatch ? amountMatch[1].replace(/,/g, '') : undefined;
    
    const dateMatches = text.match(/(\d{1,2}\/\d{1,2}\/\d{4})/g);
    const lienDate = dateMatches && dateMatches.length > 0 ? dateMatches[0] : undefined;
    
    return { amount, lienDate };
  } catch (err: any) {
    log({ stage: 'pdf_parse_error', error: err.message });
    return {};
  }
}

async function processDetailRow(page: Page, rowIndex: number): Promise<LienRecord | null> {
  try {
    log({ stage: 'detail_process_start', row: rowIndex });
    
    const rows = page.locator('.div-table-row');
    const row = rows.nth(rowIndex);
    await row.locator('.interactive-button').first().click();
    await page.waitForLoadState('networkidle');
    await humanDelay();
    
    const fileNumber = await page.locator('text=/File Number/i').locator('..').locator('+ *').textContent().catch(() => '') || '';
    const debtorName = await page.locator('text=/Debtor Name/i').locator('..').locator('+ *').textContent().catch(() => '') || '';
    const debtorAddress = await page.locator('text=/Debtor Address/i').locator('..').locator('+ *').textContent().catch(() => '') || '';
    const securedPartyName = await page.locator('text=/Secured Party Name/i').locator('..').locator('+ *').textContent().catch(() => '') || '';
    const securedPartyAddress = await page.locator('text=/Secured Party Address/i').locator('..').locator('+ *').textContent().catch(() => '') || '';
    const filingDateText = await page.locator('text=/Filing Date/i').locator('..').locator('+ *').textContent().catch(() => '') || '';
    
    log({ stage: 'detail_extracted_basic', file_number: fileNumber.trim() });
    
    const historyBtn = page.getByRole('button', { name: /View History/i });
    let amount: string | undefined;
    let lienDate: string | undefined;
    
    if (await historyBtn.isVisible().catch(() => false)) {
      await historyBtn.click();
      await page.getByRole('dialog', { name: 'History' }).waitFor({ state: 'visible', timeout: 8000 });
      await humanDelay();
      
      const modal = page.getByRole('dialog', { name: 'History' });
      const downloadLink = modal.getByRole('link', { name: /Download/i });
      
      if (await downloadLink.isVisible().catch(() => false)) {
        const downloadDir = path.join(process.cwd(), 'data/downloads');
        if (!fs.existsSync(downloadDir)) {
          fs.mkdirSync(downloadDir, { recursive: true });
        }
        
        const pdfPath = path.join(downloadDir, `${fileNumber.trim()}.pdf`);
        
        try {
          const [download] = await Promise.all([
            page.waitForEvent('download', { timeout: 15000 }),
            downloadLink.click()
          ]);
          
          await download.saveAs(pdfPath);
          log({ stage: 'detail_pdf_downloaded', file_number: fileNumber.trim() });
          
          const pdfData = await extractFromPDF(pdfPath);
          amount = pdfData.amount;
          lienDate = pdfData.lienDate;
          
          fs.unlinkSync(pdfPath);
          log({ stage: 'detail_pdf_deleted', file_number: fileNumber.trim() });
        } catch (err: any) {
          log({ stage: 'detail_pdf_failed', file_number: fileNumber.trim(), error: err.message });
        }
      }
      
      const closeBtn = modal.getByRole('button', { name: /Close|Back/i }).first();
      if (await closeBtn.isVisible().catch(() => false)) {
        await closeBtn.click();
        await page.waitForLoadState('networkidle');
      }
    }
    
    await page.goBack({ waitUntil: 'networkidle' });
    await humanDelay();
    
    const record: LienRecord = {
      state: 'CA',
      source: 'ca_sos',
      county: '',
      ucc_type: 'Federal Tax Lien',
      debtor_name: debtorName.trim(),
      debtor_address: debtorAddress.trim(),
      file_number: fileNumber.trim(),
      secured_party_name: securedPartyName.trim(),
      secured_party_address: securedPartyAddress.trim(),
      status: 'Active',
      filing_date: filingDateText.trim(),
      lapse_date: lienDate || '12/31/9999',
      document_type: 'Notice of Federal Tax Lien',
      pdf_filename: '',
      processed: true,
      error: ''
    };
    
    log({ stage: 'detail_mapped', file_number: record.file_number });
    
    return record;
    
  } catch (err: any) {
    log({ stage: 'detail_row_failed', row: rowIndex, error: err.message });
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

    const fileTypeSelectCandidates = [
      { method: 'label', locator: page.getByLabel(/file type/i) },
      { method: 'aria_label_select', locator: page.locator('select[aria-label*="File Type" i]') },
      { method: 'name_or_id_select', locator: page.locator('select[name*="fileType" i], select[id*="fileType" i]') },
      { method: 'combobox_role', locator: page.getByRole('combobox', { name: /file type/i }) },
    ];

    let fileTypeSelected = false;
    for (const candidate of fileTypeSelectCandidates) {
      if ((await candidate.locator.count()) === 0) {
        continue;
      }

      const control = candidate.locator.first();
      try {
        await control.waitFor({ state: 'visible', timeout: 2000 });
        await control.selectOption({ label: 'Federal Tax Lien' }, { timeout: 2000 });
        fileTypeSelected = true;
        log({ stage: 'file_type_selected', method: candidate.method });
        break;
      } catch (err: any) {
        log({ stage: 'file_type_candidate_failed', method: candidate.method, error: err?.message ?? String(err) });
      }
    }

    if (!fileTypeSelected) {
      fileTypeSelected = await page.evaluate(() => {
        const normalized = (value: string | null | undefined) => (value ?? '').trim().toLowerCase();
        const selects = Array.from(document.querySelectorAll('select')) as HTMLSelectElement[];

        const byNameOrId = selects.find((sel) => {
          const id = normalized(sel.id);
          const name = normalized(sel.name);
          return id.includes('filetype') || id.includes('file_type') || name.includes('filetype') || name.includes('file_type');
        });

        const withLabel = selects.find((sel) => {
          const id = sel.id;
          if (!id) return false;
          const label = document.querySelector(`label[for="${id}"]`);
          return normalized(label?.textContent).includes('file type');
        });

        const fallback = byNameOrId ?? withLabel ?? selects.find((sel) => {
          const text = normalized(sel.getAttribute('aria-label'));
          return text.includes('file type');
        });

        if (!fallback) return false;

        const option = Array.from(fallback.options).find((opt) => normalized(opt.text).includes('federal tax lien'));
        if (!option) return false;

        fallback.value = option.value;
        fallback.dispatchEvent(new Event('input', { bubbles: true }));
        fallback.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
      });

      if (fileTypeSelected) {
        log({ stage: 'file_type_selected', method: 'dom_fallback' });
      }
    }

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

    for (let i = 0; i < toProcess; i++) {
      const record = await processDetailRow(page, i);
      
      if (record) {
        await pushToSheets([record]);
        log({ stage: 'scraper_pushed_sheet', file_number: record.file_number });
        
        await queue.insertMany([record]);
        
        processedRecords.push(record);
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
