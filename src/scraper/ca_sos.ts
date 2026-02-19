import { chromium as playwrightChromium, Page, Locator } from "playwright-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import path from "path";
import fs from "fs";
import { limiter } from "../utils/rateLimit";
import { humanDelay } from "../utils/delay";
import { log } from "../utils/logger";
import { LienRecord } from "../types";

export interface ScrapeConfig {
  date_start: string;
  date_end: string;
  max_records?: number;
  output_dir?: string;
  resume_cursor?: {
    page: number;
    row_index: number;
  };
}

export class TooManyResultsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TooManyResultsError";
  }
}

// Screenshot helper
async function screenshot(page: Page, label: string): Promise<void> {
  try {
    const base = process.env.NODE_ENV === "production" ? "/app" : ".";
    const filepath = path.join(base, `debug-${label}-${Date.now()}.png`);
    await page.screenshot({ path: filepath, fullPage: true });
    log({ stage: "screenshot_saved", path: filepath, label });
  } catch (_) {}
}

// Safe text helper
async function safeText(locator: Locator): Promise<string> {
  try {
    return (await locator.textContent({ timeout: 3000 }))?.trim() ?? "";
  } catch {
    return "";
  }
}

// Multi-strategy field extractor
async function getField(page: Page, label: string): Promise<string> {
  const strategies = [
    async () => {
      const dt = page.locator(`dt:has-text("${label}")`).first();
      if (await dt.isVisible({ timeout: 2000 }).catch(() => false)) {
        return (await dt.locator("+ dd").textContent({ timeout: 2000 }))?.trim() ?? "";
      }
      return "";
    },
    async () => {
      const el = page.locator(`*:has-text("${label}")`).last();
      const parent = el.locator("..");
      const children = parent.locator("*");
      const count = await children.count().catch(() => 0);
      for (let i = 0; i < count - 1; i++) {
        const text = await safeText(children.nth(i));
        if (text.includes(label)) {
          return safeText(children.nth(i + 1));
        }
      }
      return "";
    }
  ];
  for (const strategy of strategies) {
    try {
      const result = await strategy();
      if (result) return result;
    } catch {}
  }
  return "";
}

export async function scrapeCASOS(config: ScrapeConfig): Promise<LienRecord[]> {
  playwrightChromium.use(StealthPlugin());

  const browser = await playwrightChromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
  });

  const context = await browser.newContext({ 
    acceptDownloads: true,
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
  });
  
  const page = await context.newPage();
  page.setDefaultNavigationTimeout(90000);
  page.setDefaultTimeout(90000);

  const outputDir = config.output_dir ?? "./downloads";
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const records: LienRecord[] = [];
  let totalCollected = 0;
  const maxRecords = config.max_records ?? 1000;
  let { page: startPage, row_index: startRow } = config.resume_cursor ?? { page: 1, row_index: 0 };

  try {
    log({ stage: "navigate", url: "https://bizfileonline.sos.ca.gov/search/ucc" });
    await limiter.schedule(() =>
      page.goto("https://bizfileonline.sos.ca.gov/search/ucc", {
        waitUntil: "networkidle",
        timeout: 90000
      })
    );

    const searchInput = page.getByLabel("Search by name or file number");
    await searchInput.waitFor({ state: "visible", timeout: 90000 });
    await humanDelay();

    log({ stage: "fill_search" });
    await searchInput.fill("Internal Revenue Service");
    await humanDelay();

    const advancedBtn = page.getByRole("button", { name: /Advanced/i });
    await advancedBtn.waitFor({ state: "visible" });
    await advancedBtn.click();

    const fileTypeSelect = page.getByLabel("File Type");
    await fileTypeSelect.waitFor({ state: "visible" });
    await fileTypeSelect.selectOption({ label: "Federal Tax Lien" });
    await humanDelay();

    const dateStartInput = page.getByLabel("File Date: Start");
    const dateEndInput = page.getByLabel("File Date: End");
    await dateStartInput.fill(config.date_start);
    await humanDelay();
    await dateEndInput.fill(config.date_end);
    await dateEndInput.press("Tab");
    await humanDelay();

    log({ stage: "submit_search" });
    await page.getByRole("button", { name: "Search" }).click();
    await page.waitForLoadState("networkidle");
    await humanDelay();

    const resultLocator = page.locator("text=/Results:\\s*\\d+/");
    await resultLocator.waitFor({ state: "visible", timeout: 30000 });
    const resultText = (await resultLocator.textContent()) ?? "";
    const totalCount = parseInt(resultText.match(/\d+/)?.[0] ?? "0");
    log({ stage: "results_found", total: totalCount });

    if (totalCount > 1000) {
      throw new TooManyResultsError(`Search returned ${totalCount} results. Splitting range.`);
    }

    if (totalCount === 0) return [];

    if (startPage > 1) {
      await page.getByRole("button", { name: String(startPage) }).click();
      await page.waitForLoadState("networkidle");
    }

    let currentPage = startPage;
    let hasNextPage = true;

    while (hasNextPage && totalCollected < maxRecords) {
      const rows = page.locator("table tbody tr");
      const rowCount = await rows.count();
      const rowStart = currentPage === startPage ? startRow : 0;

      for (let i = rowStart; i < rowCount; i++) {
        if (totalCollected >= maxRecords) {
          hasNextPage = false;
          break;
        }
        try {
          const record = await processRow(page, rows.nth(i), i, currentPage, outputDir);
          if (record) {
            records.push(record);
            totalCollected++;
            log({ stage: "record_collected", total: totalCollected, file_number: record.file_number });
          }
        } catch (err) {
          log({ stage: "row_error", error: String(err) });
        }
      }

      const nextBtn = page.getByRole("button", { name: "Next Page" });
      if (await nextBtn.isVisible() && totalCollected < maxRecords) {
        await nextBtn.click();
        await page.waitForLoadState("networkidle");
        await humanDelay();
        currentPage++;
      } else {
        hasNextPage = false;
      }
    }

    return records;
  } catch (err) {
    await screenshot(page, "fatal-error");
    throw err;
  } finally {
    await browser.close();
  }
}

async function processRow(page: Page, row: Locator, rowIndex: number, pageNum: number, outputDir: string): Promise<LienRecord | null> {
  const cells = row.locator("td");
  const ucc_type = await safeText(cells.nth(0));
  const file_number = await safeText(cells.nth(2));
  const status = await safeText(cells.nth(4));
  const filing_date = await safeText(cells.nth(5));
  const lapse_date = await safeText(cells.nth(6));

  if (!file_number) return null;

  const chevron = row.locator("button").first();
  let panelOpened = false;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await chevron.click();
      await page.locator('[class*="detail"]').filter({ hasText: file_number }).waitFor({ state: "visible", timeout: 8000 });
      panelOpened = true;
      break;
    } catch { await humanDelay(); }
  }

  if (!panelOpened) return buildRecord({ file_number, error: "panel_failed" });

  const debtor_name = await getField(page, "Debtor Name");
  const debtor_address = await getField(page, "Debtor Address");
  const secured_party_name = await getField(page, "Secured Party Name");
  const secured_party_address = await getField(page, "Secured Party Address");

  const historyBtn = page.getByRole("button", { name: /View History/i });
  let historyOpened = false;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await historyBtn.click();
      await page.getByRole("dialog", { name: "History" }).waitFor({ state: "visible", timeout: 8000 });
      historyOpened = true;
      break;
    } catch { await humanDelay(); }
  }

  let document_type = "";
  let pdf_filename = "";

  if (historyOpened) {
    const modal = page.getByRole("dialog", { name: "History" });
    document_type = await getField(page, "Document Type");
    const downloadLink = modal.getByRole("link", { name: /Download/i });
    if (await downloadLink.isVisible().catch(() => false)) {
      try {
        const safeDate = filing_date.replace(/\//g, "");
        pdf_filename = `${file_number}_${safeDate}.pdf`;
        const [download] = await Promise.all([
          page.waitForEvent("download", { timeout: 30000 }),
          downloadLink.click()
        ]);
        await download.saveAs(path.join(outputDir, pdf_filename));
      } catch (err) {
        log({ stage: "pdf_fail", error: String(err) });
      }
    }
    await page.keyboard.press("Escape");
  }

  await closePanel(page);
  return buildRecord({
    ucc_type, file_number, status, filing_date, lapse_date,
    debtor_name, debtor_address, secured_party_name, secured_party_address,
    document_type, pdf_filename, processed: true
  });
}

async function closePanel(page: Page) {
  try {
    await page.locator('[aria-label="Close"], button:has-text("×")').last().click();
    await page.waitForTimeout(300);
  } catch { await page.keyboard.press("Escape"); }
}

function buildRecord(fields: Partial<LienRecord> & { file_number: string }): LienRecord {
  return {
    state: "CA",
    ucc_type: fields.ucc_type ?? "",
    debtor_name: fields.debtor_name ?? "",
    debtor_address: fields.debtor_address ?? "",
    file_number: fields.file_number,
    secured_party_name: fields.secured_party_name ?? "",
    secured_party_address: fields.secured_party_address ?? "",
    status: fields.status ?? "",
    filing_date: fields.filing_date ?? "",
    lapse_date: fields.lapse_date ?? "",
    document_type: fields.document_type ?? "",
    pdf_filename: fields.pdf_filename ?? "",
    processed: fields.processed ?? false,
    error: fields.error
  };
}
