import { chromium } from "playwright";
import { Page, Locator } from "playwright";
import path from "path";
import fs from "fs";
import { limiter } from "../utils/rateLimit";
import { humanDelay } from "../utils/delay";
import { log } from "../utils/logger";
import { LienRecord } from "../types";
import crypto from 'crypto';

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

// Helper: safe text extraction
async function safeText(locator: Locator): Promise<string> {
  try {
    return (await locator.textContent({ timeout: 3000 }))?.trim() ?? "";
  } catch {
    return "";
  }
}

// Multi-strategy field extractor
async function getField(page: Page | Locator, label: string): Promise<string> {
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
  const browser = await createBrowser();

  const context = await browser.newContext({
    acceptDownloads: true,
  });

  const page = await context.newPage();
  page.setDefaultNavigationTimeout(90000);
  page.setDefaultTimeout(90000);

  const outputDir = config.output_dir ?? "./downloads";
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const records: LienRecord[] = [];
  let totalCollected = 0;
  const maxRecords = config.max_records ?? 1000;
  let { page: startPage, row_index: startRow } =
    config.resume_cursor ?? { page: 1, row_index: 0 };

  try {
    log({ stage: "navigate", url: "https://bizfileonline.sos.ca.gov/search/ucc" });

    await limiter.schedule(() =>
      page.goto("https://bizfileonline.sos.ca.gov/search/ucc", {
        waitUntil: "networkidle",
        timeout: 90000,
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

    const dateStartInput = page.getByRole("textbox", { name: "File Date: Start" });
    const dateEndInput = page.getByRole("textbox", { name: "File Date: End" });
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
      throw new TooManyResultsError(
        `Search returned ${totalCount} results. Splitting range.`
      );
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
        if (totalCollected >= maxRecords) break;

        const row = rows.nth(i);
        try {
          const record = await processRow(
            page,
            row,
            i,
            currentPage,
            outputDir
          );
          if (record) {
            records.push(record);
            totalCollected++;
          }
        } catch (err) {
          log({
            stage: "row_error",
            page: currentPage,
            row_index: i,
            error: String(err),
          });
        }
      }

      const nextBtn = page.getByRole("button", { name: "Next" });
      if (await nextBtn.isVisible().catch(() => false)) {
        await nextBtn.click();
        await page.waitForLoadState("networkidle");
        currentPage++;
      } else {
        hasNextPage = false;
      }
    }

    return records;
  } catch (err) {
    log({ stage: "error", error: String(err) });
    throw err;
  } finally {
    await context.close();
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
  const file_number = await safeText(row.locator("td").nth(1));
  if (!file_number) return null;

  await row.click();
  await page.waitForLoadState("networkidle");

  let panelOpened = false;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const panel = page.locator("section[role='dialog'], div[role='dialog']").first();
      if (await panel.isVisible({ timeout: 5000 }).catch(() => false)) {
        panelOpened = true;
        break;
      }
    } catch {
      await humanDelay();
    }
  }

  if (!panelOpened) {
    return buildRecord({ file_number, error: "panel_failed" });
  }

  const debtor_name = await getField(page, "Debtor Name");
  const debtor_address = await getField(page, "Debtor Address");
  const secured_party_name = await getField(page, "Secured Party Name");
  const secured_party_address = await getField(page, "Secured Party Address");
  const filing_date = await getField(page, "Filing Date");
  const lapse_date = await getField(page, "Lapse Date");
  const status = await getField(page, "Status");
  const ucc_type = await getField(page, "UCC Type");

  const historyBtn = page.getByRole("button", { name: /View History/i });
  let historyOpened = false;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await historyBtn.click();
      await page
        .getByRole("dialog", { name: "History" })
        .waitFor({ state: "visible", timeout: 8000 });
      historyOpened = true;
      break;
    } catch {
      await humanDelay();
    }
  }

  let document_type = "";
  let pdf_filename = "";

  if (historyOpened) {
    const modal = page.getByRole("dialog", { name: "History" });

    document_type = await getField(modal, "Document Type");

    const downloadLink = modal.getByRole("link", { name: /Download/i });

    if (await downloadLink.isVisible().catch(() => false)) {
      try {
        const safeDate = filing_date.replace(/\//g, "");
        pdf_filename = `${file_number}_${safeDate}.pdf`;

        const [download] = await Promise.all([
          page.waitForEvent("download"),
          downloadLink.click(),
        ]);

        await download.saveAs(path.join(outputDir, pdf_filename));
      } catch (err) {
        log({ stage: "download_error", file_number, error: String(err) });
      }
    }

    try {
      await page.keyboard.press("Escape");
    } catch {
      // ignore
    }
  }

  await closePanel(page);

  return buildRecord({
    file_number,
    debtor_name,
    debtor_address,
    secured_party_name,
    secured_party_address,
    filing_date,
    lapse_date,
    status,
    ucc_type,
    document_type,
    pdf_filename,
  });
}

async function closePanel(page: Page) {
  const closeBtn = page
    .getByRole("button", { name: /Close|Back/i })
    .first();
  if (await closeBtn.isVisible().catch(() => false)) {
    await closeBtn.click();
    await page.waitForLoadState("networkidle");
  }
}

function buildRecord(
  fields: Partial<LienRecord> & { file_number: string }
): LienRecord {
  return {
    file_number: fields.file_number,
    debtor_name: fields.debtor_name ?? "",
    debtor_address: fields.debtor_address ?? "",
    secured_party_name: fields.secured_party_name ?? "",
    secured_party_address: fields.secured_party_address ?? "",
    filing_date: fields.filing_date ?? "",
    lapse_date: fields.lapse_date ?? "",
    status: fields.status ?? "",
    ucc_type: fields.ucc_type ?? "",
    document_type: fields.document_type ?? "",
    pdf_filename: fields.pdf_filename ?? "",
    error: fields.error,
    state: fields.state ?? "",
    source: fields.source ?? "ca_sos",
    processed: fields.processed ?? false,
  };
}

async function createBrowser() {
  const url = process.env.SBR_CDP_URL;
  if (!url) {
    throw new Error("SBR_CDP_URL is not set");
  }
  return chromium.connectOverCDP(url);
}
