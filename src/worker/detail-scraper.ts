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

    const firstRow = page.locator('.div-table-row').first();
    await firstRow.locator('.interactive-cell-button').first().click();

    const drawer = page.locator('div.drawer.show');
    await drawer.waitFor({ state: 'visible', timeout: 15000 });

    const getField = async (label: string) => {
      const row = drawer.locator('table.details-list tr.detail').filter({
        has: page.locator(`td.label:has-text("${label}")`)
      });
      return (await row.locator('td.value').textContent({ timeout: 5000 }).catch(() => ''))?.trim() ?? '';
    };

    const debtorName = await getField('Debtor Name');
    const debtorAddress = await getField('Debtor Address');
    const securedPartyName = await getField('Secured Party Name');
    const securedPartyAddress = await getField('Secured Party Address');

    log({ stage: 'detail_extracted_basic', file_number: fileNumber });

    let pdfPath: string | undefined;

    const historyBtn = drawer.locator('button[aria-label="View History"]');
    if (await historyBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
      await historyBtn.click();
      await page.waitForTimeout(3000);

      const downloadLink = page.getByRole('link', { name: /Download/i }).first();
      if (await downloadLink.isVisible({ timeout: 5000 }).catch(() => false)) {
        const downloadDir = path.join(process.cwd(), 'data/downloads');
        if (!fs.existsSync(downloadDir)) {
          fs.mkdirSync(downloadDir, { recursive: true });
        }

        const [download] = await Promise.all([
          page.waitForEvent('download', { timeout: 15000 }),
          downloadLink.click()
        ]);

        pdfPath = path.join(downloadDir, `${fileNumber}.pdf`);
        await download.saveAs(pdfPath);
        log({ stage: 'detail_pdf_downloaded', file_number: fileNumber, path: pdfPath });
      } else {
        log({ stage: 'detail_no_download', file_number: fileNumber });
      }

      await page.keyboard.press('Escape');
      await page.waitForTimeout(500);
    }

    const closeBtn = drawer.locator('button.close-button');
    if (await closeBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
      await closeBtn.click();
      await page.waitForTimeout(1000);
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
