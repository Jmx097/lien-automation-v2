import express from "express";
import { scrapers } from "./scraper/index";
import { pushToSheets } from "./sheets/push";
import { log } from "./utils/logger";

const app = express();
app.use(express.json());

app.post("/scrape", async (req, res) => {
  const startTime = Date.now();

  try {
    const { site, date_start, date_end, max_records } = req.body;

    const scraper = scrapers[site];

    if (!site || !scraper) {
      return res.status(400).json({
        error: `Unknown site: ${site}. Supported: ${Object.keys(scrapers).join(", ")}`
      });
    }

    if (!date_start || !date_end) {
      return res.status(400).json({
        error: "date_start and date_end required"
      });
    }

    log({ stage: "scrape_start", site, date_start, date_end });

    const results = await scraper({
      date_start,
      date_end,
      max_records
    });

    const sheetResult = await pushToSheets(results);

    const duration = (Date.now() - startTime) / 1000;

    log({
      stage: "scrape_complete",
      duration_seconds: duration,
      records: results.length
    });

    return res.json({
      success: true,
      records_scraped: results.length,
      rows_uploaded: sheetResult.uploaded,
      duration_seconds: duration
    });

  } catch (err: any) {
    log({ stage: "fatal_error", error: String(err) });

    return res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

app.get("/health", (_req, res) => {
  res.json({ status: "healthy", uptime: process.uptime() });
});

app.post("/scrape-all", async (req, res) => {
  const { date_start, date_end, max_records } = req.body;
  const results: any[] = [];
  for (const [site, scraper] of Object.entries(scrapers)) {
    try {
      const records = await (scraper as Function)({ date_start, date_end, max_records });
      results.push({ site, success: true, records: records.length });
    } catch (err: any) {
      results.push({ site, success: false, error: err.message });
    }
  }
  return res.json({ results });
});

app.listen(8080, () => {
  console.log("Server running on port 8080");
});