import { chromium } from 'playwright';
import { log } from '../utils/logger';
import fs from 'fs';
import path from 'path';

const SBR_CDP_URL = process.env.SBR_CDP_URL;

interface DetailResult {
  file_number: string;
  filing_date: string;
  debtor_name: string;
  debtor_address: string;
  secured_party_name: string;
  secured_party_address: string;
  pdf_path?: string;
}

export async function scrapeCASOSDetail(fileNumber: string): Promise<DetailResult> {
  if (!SBR_CDP_URL) {
    throw new Error('SBR_CDP_URL not configured');
  }

  const browser = await chromium.connectOverCDP(SBR_CDP_URL);
  const context = browser.contexts()[0];
  const page = await context.newPage();

  try {
    log({ stage: 'detail_navigate', file_number: fileNumber });
    
    await page.goto('https://bizfileonline.sos.ca.gov/search/business', { 
      waitUntil: 'domcontentloaded',
      timeout: 30000 
    });

    // Search by file number
    await page.getByRole('textbox', { name: /file number/i }).fill(fileNumber);
    await page.getByRole('button', { name: /search/i }).click();
    
    await page.waitForSelector('.div-table-row', { timeout: 30000 });
    
    // Click into first result row
    const firstRow = page.locator('.div-table-row').first();
    await firstRow.locator('.interactive-button').click();
    
    await page.waitForLoadState('domcontentloaded');
    
    // Extract basic info from detail page
    const debtorName = await page.locator('text=/Debtor Name/i').locator('..').locator('following-sibling::*').first().textContent() || '';
    const debtorAddress = await page.locator('text=/Debtor Address/i').locator('..').locator('following-sibling::*').first().textContent() || '';
    const securedPartyName = await page.locator('text=/Secured Party Name/i').locator('..').locator('following-sibling::*').first().textContent() || '';
    const securedPartyAddress = await page.locator('text=/Secured Party Address/i').locator('..').locator('following-sibling::*').first().textContent() || '';
    
    log({ stage: 'detail_extracted_basic', file_number: fileNumber });
    
    // Click "View History" button
    await page.getByRole('button', { name: /view history/i }).click();
    await page.waitForLoadState('domcontentloaded');
    
    // Look for Download button/link in history view
    const downloadButton = page.locator('button:has-text("Download"), a:has-text("Download")').first();
    
    let pdfPath: string | undefined;
    
    if (await downloadButton.isVisible({ timeout: 5000 })) {
      const downloadDir = path.join(process.cwd(), 'data/downloads');
      if (!fs.existsSync(downloadDir)) {
        fs.mkdirSync(downloadDir, { recursive: true });
      }
      
      const [download] = await Promise.all([
        page.waitForEvent('download', { timeout: 15000 }),
        downloadButton.click()
      ]);
      
      pdfPath = path.join(downloadDir, `${fileNumber}.pdf`);
      await download.saveAs(pdfPath);
      
      log({ stage: 'detail_pdf_downloaded', file_number: fileNumber, path: pdfPath });
    } else {
      log({ stage: 'detail_no_download', file_number: fileNumber });
    }
    
    await page.close();
    
    return {
      file_number: fileNumber,
      filing_date: '',  // Extract from PDF or page
      debtor_name: debtorName.trim(),
      debtor_address: debtorAddress.trim(),
      secured_party_name: securedPartyName.trim(),
      secured_party_address: securedPartyAddress.trim(),
      pdf_path: pdfPath
    };
    
  } catch (err: any) {
    await page.close().catch(() => {});
    throw new Error(`Detail scrape failed: ${err.message}`);
  } finally {
    await browser.close().catch(() => {});
  }
}
