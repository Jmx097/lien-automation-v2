import express from "express";
import { scrapers } from "./scraper/index";
import { pushToSheets } from "./sheets/push";
import { log } from "./utils/logger";
import dotenv from 'dotenv';
import { SQLiteQueueStore } from "./queue/sqlite";
import { checkMissedRuns, getNextRuns, getRunHistory, runScheduledScrape } from "./scheduler";
import { getScheduleReadinessReport } from "./schedule/readiness";

dotenv.config();

if (!process.env.SBR_CDP_URL) {
  console.error('Missing SBR_CDP_URL environment variable for Bright Data Scraping Browser');
  process.exit(1);
}

const queue = new SQLiteQueueStore();
const app = express();
app.use(express.json());

const runtimeVersion = {
  git_sha: process.env.GIT_SHA ?? "unknown",
  app_version: process.env.npm_package_version ?? "unknown",
  node_version: process.version
};

app.post("/scrape", async (req, res) => {
  const startTime = Date.now();

  try {
    const { site, date_start, date_end, max_records } = req.body;

    const scraper = (scrapers as any)[site];

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

app.post("/enqueue", async (req, res) => {
  const startTime = Date.now();

  try {
    const { site, date_start, date_end, max_records } = req.body;

    const scraper = (scrapers as any)[site];

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

    log({ stage: "enqueue_start", site, date_start, date_end });

    const results = await scraper({
      date_start,
      date_end,
      max_records
    });

    await queue.insertMany(results);

    const pending = await queue.getPendingCount();
    const duration = (Date.now() - startTime) / 1000;

    log({
      stage: "enqueue_complete",
      duration_seconds: duration,
      records_enqueued: results.length,
      total_pending: pending
    });

    return res.json({
      success: true,
      records_enqueued: results.length,
      total_pending: pending,
      duration_seconds: duration
    });

  } catch (err: any) {
    log({ stage: "enqueue_error", error: String(err) });

    return res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

app.get("/health", (_req, res) => {
  res.json({ status: "healthy", uptime: process.uptime() });
});

app.get("/version", (_req, res) => {
  res.json({
    status: "ok",
    ...runtimeVersion
  });
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

app.get("/schedule/health", (_req, res) => {
  const report = getScheduleReadinessReport();
  const statusCode = report.status === "ready" ? 200 : 503;
  res.status(statusCode).json(report);
});

app.get("/schedule", (req, res) => {
  const limitParam = Number.parseInt(String(req.query.limit ?? '50'), 10);
  const limit = Number.isFinite(limitParam) ? Math.min(Math.max(limitParam, 1), 200) : 50;

  res.json({
    next_runs: getNextRuns(),
    history: getRunHistory(limit),
    persisted: true,
  });
});

app.post("/schedule/run", async (req, res) => {
  try {
    const configuredToken = process.env.SCHEDULE_RUN_TOKEN;
    if (!configuredToken) {
      return res.status(500).json({ error: 'SCHEDULE_RUN_TOKEN is not configured' });
    }

    const authHeader = req.headers.authorization;
    const schedulerTokenHeader = req.headers['x-scheduler-token'];
    const bearerToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
    const suppliedToken = bearerToken ?? (Array.isArray(schedulerTokenHeader) ? schedulerTokenHeader[0] : schedulerTokenHeader);

    if (suppliedToken !== configuredToken) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const slot = req.body?.slot === 'morning' || req.body?.slot === 'afternoon' ? req.body.slot : undefined;
    const idempotencyKey = typeof req.body?.idempotency_key === 'string' ? req.body.idempotency_key : undefined;

    const result = await runScheduledScrape({
      slot,
      idempotencyKey,
      triggerSource: 'external',
    });
    return res.json(result);
  } catch (err: any) {
    return res.status(500).json({ error: err.message });
  }
});

app.listen(8080, () => {
  log({
    stage: "startup",
    port: 8080,
    ...runtimeVersion
  });
  console.log("Server running on port 8080");

  setInterval(() => {
    checkMissedRuns().catch((err) => log({ stage: 'missed_run_check_error', error: String(err) }));
  }, 60_000);
});

app.post("/scrape-enhanced", async (req, res) => {
  const startTime = Date.now();

  try {
    const { site, date_start, date_end, max_records } = req.body;

    const scraper = (scrapers as any)[site];

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

    log({ stage: "scrape_enhanced_start", site, date_start, date_end });

    const results = await scraper({
      date_start,
      date_end,
      max_records
    });

    const duration = (Date.now() - startTime) / 1000;

    log({
      stage: "scrape_enhanced_complete",
      duration_seconds: duration,
      records: results.length
    });

    return res.json({
      success: true,
      records_processed: results.length,
      duration_seconds: duration
    });

  } catch (err: any) {
    log({ stage: "scrape_enhanced_error", error: String(err) });

    return res.status(500).json({
      success: false,
      error: err.message
    });
  }
});
