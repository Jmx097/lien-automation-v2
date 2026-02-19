import { chromium, Page, Locator } from "playwright";
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

export async function scrapeCASOS(config: ScrapeConfig): Promise<LienRecord[]> {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"]
  });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();
  page.setDefaultNavigationTimeout(60000);
  page.setDefaultTimeout(60000);

  const outputDir = config.output_dir ?? "./downloads";
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const records: LienRecord[] = [];
  let totalCollected = 0;
  const maxRecords = config.max_records ?? 1000;
  let { page: startPage, row_index: startRow } =
    config.resume_cursor ?? { page: 1, row_index: 0 };

  try {
    log({ stage: "navigate" });

    await limiter.schedule(() =>
      page.goto("https://bizfileonline.sos.ca.gov/search/ucc", {
        waitUntil: "domcontentloaded",
        timeout: 60000
      })
    );

    const searchInput = page.locator(
      "input[placeholder*='name or file'], input[aria-label*='name or file'], input[type='search'], .search-input input"
    ).first();

    await searchInput.waitFor({ state: "visible", timeout: 60000 });
    await humanDelay();

    log({ stage: "fill_search" });
    await searchInput.fill("Internal Revenue Service");
    await humanDelay();

    await page.getByRole("button", { name: /Advanced/i }).click();
    await page.getByLabel("File Type").waitFor({ state: "visible" });
    await humanDelay();

    await page.getByLabel("File Type").selectOption({ label: "Federal Tax Lien" });
    await humanDelay();

    await page.getByLabel("File Date: Start").fill(config.date_start);
    await page.getByLabel("File Date: End").fill(config.date_end);
    await page.getByLabel("File Date: End").press("Tab");
    await humanDelay();

    log({ stage: "submit_search" });
    await page.getByRole("button", { name: "Search" }).click();
    await page.waitForLoadState("domcontentloaded");
    await humanDelay();

    const resultLocator = page.locator("text=/Results:\\s*\\d+/");
    await resultLocator.waitFor({ state: "visible", timeout: 15000 });
    const resultText = (await resultLocator.textContent()) ?? "";
    const totalCount = parseInt(resultText.match(/\d+/)?.[0] ?? "0");

    log({ stage: "results_found", total: totalCount });

    if (totalCount > 1000) {
      throw new TooManyResultsError(
        `Search returned ${totalCount} results. Halve the date range and retry.`
      );
    }
    if (totalCount === 0) {
      log({ stage: "no_results" });
      return [];
    }

    if (startPage > 1) {
      await page.getByRole("button", { name: String(startPage) }).click();
      await page.waitForLoadState("domcontentloaded");
    }

    let currentPage = startPage;
    let hasNextPage = true;

    while (hasNextPage && totalCollected < maxRecords) {
      const rows = page.locator("table tbody tr");
      const rowCount = await rows.count();
      const rowStart = currentPage === startPage ? startRow : 0;

      log({ stage: "page_start", currentPage, rowCount });

      for (let i = rowStart; i < rowCount; i++) {
        if (totalCollected >= maxRecords) { hasNextPage = false; break; }

        const record = await processRow(page, rows.nth(i), i, currentPage, outputDir);
        if (record) {
          records.push(record);
          totalCollected++;
          log({ stage: "record_collected", total: totalCollected, file_number: record.file_number, error: record.error });
        }
      }

      const nextBtn = page.getByRole("button", { name: "Next Page" });
      const nextVisible = await nextBtn.isVisible().catch(() => false);
      if (nextVisible && totalCollected < maxRecords) {
        await nextBtn.click();
        await page.waitForLoadState("domcontentloaded");
        await humanDelay();
        currentPage++;
        startRow = 0;
      } else {
        hasNextPage = false;
      }
    }

    log({ stage: "scrape_done", total_collected: totalCollected });
    return records;

  } catch (err) {
    log({ stage: "error", error: String(err) });
    try {
      await page.screenshot({ path: "/app/error-screenshot.png", fullPage: true });
      log({ stage: "screenshot_saved", path: "/app/error-screenshot.png" });
    } catch (_) {}
    throw err;
  } finally {
    await browser.close();
  }
}

async function processRow(
  page: Page,
  row: Locator,
  rowIndex: number,
  pageNum: number,
  outputDir: string
): Promise<LienRecord | null> {

  const cells = row.locator("td");
  const ucc_type    = (await cells.nth(0).textContent())?.trim() ?? "";
  const file_number = (await cells.nth(2).textContent())?.trim() ?? "";
  const status      = (await cells.nth(4).textContent())?.trim() ?? "";
  const filing_date = (await cells.nth(5).textContent())?.trim() ?? "";
  const lapse_date  = (await cells.nth(6).textContent())?.trim() ?? "";

  if (!file_number) return null;

  const chevron = row.locator("button").first();
  let panelOpened = false;

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await chevron.click();
      await page
        .locator('[class*="detail"], [class*="panel"], [class*="side"]')
        .filter({ hasText: file_number })
        .waitFor({ state: "visible", timeout: 8000 });
      panelOpened = true;
      break;
    } catch {
      if (attempt === 0) await humanDelay();
    }
  }

  if (!panelOpened) {
    log({ stage: "panel_failed", file_number, pageNum, rowIndex });
    return buildRecord({ ucc_type, file_number, status, filing_date, lapse_date, error: "panel_failed" });
  }

  const getField = async (label: string) => {
    try {
      return (await page.locator(`text=${label}`).locator("..").locator("+ *").textContent()) ?? "";
    } catch { return ""; }
  };

  const debtor_name           = (await getField("Debtor Name")).trim();
  const debtor_address        = (await getField("Debtor Address")).trim();
  const secured_party_name    = (await getField("Secured Party Name")).trim();
  const secured_party_address = (await getField("Secured Party Address")).trim();

  const historyBtn = page.getByRole("button", { name: /View History/i });
  let historyOpened = false;

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await historyBtn.click();
      await page.getByRole("dialog", { name: "History" }).waitFor({ state: "visible", timeout: 8000 });
      historyOpened = true;
      break;
    } catch {
      if (attempt === 0) await humanDelay();
    }
  }

  if (!historyOpened) {
    log({ stage: "history_failed", file_number });
    await closePanel(page);
    return buildRecord({ ucc_type, file_number, status, filing_date, lapse_date, debtor_name, debtor_address, secured_party_name, secured_party_address, error: "history_failed" });
  }

  const modal = page.getByRole("dialog", { name: "History" });
  const document_type = ((await modal.locator("text=Document Type").locator("..").locator("+ *").textContent().catch(() => "")) ?? "").trim();

  let pdf_filename = "";
  const downloadLink = modal.getByRole("link", { name: /Download/i });
  const linkExists = await downloadLink.isVisible().catch(() => false);

  if (linkExists) {
    try {
      const safeDate = filing_date.replace(/\//g, "");
      pdf_filename = `${file_number}_${safeDate}.pdf`;
      const [download] = await Promise.all([
        page.waitForEvent("download", { timeout: 30000 }),
        downloadLink.click()
      ]);
      await download.saveAs(path.join(outputDir, pdf_filename));
      log({ stage: "pdf_downloaded", file_number, pdf_filename });
    } catch (err) {
      log({ stage: "pdf_download_failed", file_number, error: String(err) });
      pdf_filename = "";
    }
  } else {
    log({ stage: "no_download_available", file_number });
  }

  try {
    await modal.getByRole("button", { name: /close|×/i }).click();
    await modal.waitFor({ state: "hidden", timeout: 5000 });
  } catch {
    await page.keyboard.press("Escape");
  }

  await closePanel(page);

  return buildRecord({ ucc_type, file_number, status, filing_date, lapse_date, debtor_name, debtor_address, secured_party_name, secured_party_address, document_type, pdf_filename, processed: true });
}

async function closePanel(page: Page) {
  try {
    await page.locator('[aria-label="Close"], button:has-text("×")').last().click();
    await page.waitForTimeout(300);
  } catch {
    await page.keyboard.press("Escape");
  }
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
