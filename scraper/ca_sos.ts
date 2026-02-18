import { chromium, Page } from "playwright";
import { limiter } from "../utils/rateLimit";
import { humanDelay } from "../utils/delay";
import { withRetry } from "../utils/retry";
import { log } from "../utils/logger";

interface ScrapeConfig {
  date_start: string;
  date_end: string;
  max_records?: number;
}

export async function scrapeCASOS(config: ScrapeConfig) {
  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"]
  });

  const page = await browser.newPage();

  try {
    log({ stage: "navigate" });

    await limiter.schedule(() =>
      page.goto("https://bizfileonline.sos.ca.gov/search/ucc", {
        waitUntil: "networkidle"
      })
    );

    await humanDelay();

    // ============================
    // MVP: MOCK DATA RETURN
    // ============================
    // This validates Cloud Run + Sheets first.
    // We replace this block with full automation next.

    const mockResults = Array.from({ length: 5 }).map((_, i) => ({
      ucc_type: "Federal Tax Lien",
      debtor_name: `JOHN SMITH ${i}`,
      file_number: `CA-FILE-${Date.now()}-${i}`,
      status: "Active",
      filing_date: config.date_start,
      lapse_date: "01/15/2031"
    }));

    log({ stage: "mock_complete", count: mockResults.length });

    return mockResults;

  } catch (err) {
    log({ stage: "error", error: String(err) });
    throw err;
  } finally {
    await browser.close();
  }
}
