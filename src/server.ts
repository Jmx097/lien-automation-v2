import express from "express";
import { scrapeCASOS } from "./scraper/ca_sos";
import { pushToSheets } from "./sheets/push";
import { log } from "./utils/logger";

const app = express();
app.use(express.json());

app.post("/scrape", async (req, res) => {
  const startTime = Date.now();

  try {
    const { site, date_start, date_end, max_records } = req.body;

    if (!site || site !== "ca_sos") {
      return res.status(400).json({
        error: "MVP supports only site: ca_sos"
      });
    }

    if (!date_start || !date_end) {
      return res.status(400).json({
        error: "date_start and date_end required"
      });
    }

    log({ stage: "scrape_start", site, date_start, date_end });

    const results = await scrapeCASOS({
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

app.listen(8080, () => {
  console.log("Server running on port 8080");
});