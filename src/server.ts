import express from "express";
import { scrapers } from "./scraper/index";
import { pushRunToNewSheetTab } from "./sheets/push";
import { log } from "./utils/logger";
import dotenv from 'dotenv';
import { SQLiteQueueStore } from "./queue/sqlite";
import {
  checkMissedRuns,
  checkSiteConnectivity,
  getNextRuns,
  getRunHistory,
  getScheduleState,
  runScheduledScrape,
  SCHEDULE_FAILURE_INJECTION_ENABLED,
  type RetryableScheduledFailureClass,
} from "./scheduler";
import { getScheduleReadinessReport } from "./schedule/readiness";
import { ensureDatabaseReady } from "./db/init";
import { validateScheduleRunRequest, validateScrapeRequest, type ValidationIssue } from "./http/validation";

dotenv.config();

if (!process.env.SBR_CDP_URL && !process.env.BRIGHTDATA_BROWSER_WS && !process.env.BRIGHTDATA_PROXY_SERVER) {
  console.warn('WARN: browser transport not set - Bright Data/browser automation disabled');
}

try {
  const dbPath = ensureDatabaseReady();
  console.log('SQLite DB initialized at ' + dbPath);
} catch (err: any) {
  console.error('Database initialization failed: ' + (err?.message ?? String(err)));
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

function isMissingRuntimeConfigError(err: unknown): boolean {
  const message = String((err as any)?.message ?? err ?? '');
  return /SBR_CDP_URL|Missing\s+.*environment variable|not configured/i.test(message);
}

function errorStatusFor(err: unknown): number {
  return isMissingRuntimeConfigError(err) ? 503 : 500;
}

function sendValidationError(res: express.Response, issues: ValidationIssue[]) {
  return res.status(400).json({
    error: 'Validation failed',
    issues,
  });
}

app.post("/scrape", async (req, res) => {
  const startTime = Date.now();

  try {
    const validation = validateScrapeRequest(req.body);
    if (!validation.ok) return sendValidationError(res, validation.issues);
    const { site, date_start, date_end, max_records } = validation.value;
    const scraper = (scrapers as any)[site];

    log({ stage: "scrape_start", site, date_start, date_end });

    const results = await scraper({
      date_start,
      date_end,
      max_records
    });

    const sheetResult = await pushRunToNewSheetTab(results, {
      label: `${site}_manual`,
      date_start,
      date_end,
      run_started_at: new Date(startTime),
    });

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
      tab_title: sheetResult.tab_title,
      duration_seconds: duration
    });

  } catch (err: any) {
    log({ stage: "fatal_error", error: String(err) });

    return res.status(errorStatusFor(err)).json({
      success: false,
      error: err.message
    });
  }
});

app.post("/enqueue", async (req, res) => {
  const startTime = Date.now();

  try {
    const validation = validateScrapeRequest(req.body);
    if (!validation.ok) return sendValidationError(res, validation.issues);
    const { site, date_start, date_end, max_records } = validation.value;
    const scraper = (scrapers as any)[site];

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

    return res.status(errorStatusFor(err)).json({
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
  const validation = validateScrapeRequest({
    site: 'ca_sos',
    date_start: req.body?.date_start,
    date_end: req.body?.date_end,
    max_records: req.body?.max_records,
  });
  if (!validation.ok) return sendValidationError(res, validation.issues.filter((issue) => issue.field !== 'site'));
  const { date_start, date_end, max_records } = validation.value;
  const results: any[] = [];
  for (const [site, scraper] of Object.entries(scrapers)) {
    try {
      const records = await (scraper as Function)({ date_start, date_end, max_records });
      results.push({ site, success: true, records: records.length });
    } catch (err: any) {
      results.push({ site, success: false, error: err.message, status: errorStatusFor(err) });
    }
  }
  return res.json({ results });
});

app.get("/schedule/health", async (_req, res) => {
  const report = await getScheduleReadinessReport();
  const statusCode = report.status === "ready" ? 200 : 503;
  res.status(statusCode).json(report);
});

app.get("/schedule", async (req, res) => {
  const limitParam = Number.parseInt(String(req.query.limit ?? '50'), 10);
  const limit = Number.isFinite(limitParam) ? Math.min(Math.max(limitParam, 1), 200) : 50;

  res.json({
    next_runs: getNextRuns(),
    history: await getRunHistory(limit),
    state: await getScheduleState(),
    persisted: true,
  });
});

app.get("/schedule/confidence", async (req, res) => {
  const limitParam = Number.parseInt(String(req.query.limit ?? '20'), 10);
  const limit = Number.isFinite(limitParam) ? Math.min(Math.max(limitParam, 1), 200) : 20;
  const history = await getRunHistory(limit);

  res.json({
    generated_at: new Date().toISOString(),
    runs: history.map((run) => ({
      id: run.id,
      site: run.site,
      started_at: run.started_at,
      finished_at: run.finished_at,
      status: run.status,
      confidence: run.confidence,
      source_tab_title: run.source_tab_title,
      master_tab_title: run.master_tab_title,
      review_tab_title: run.review_tab_title,
      failure_class: run.failure_class,
    })),
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

    const validation = validateScheduleRunRequest(req.body);
    if (!validation.ok) return sendValidationError(res, validation.issues);
    const slot = validation.value.slot;
    const site = validation.value.site;
    const idempotencyKey = validation.value.idempotency_key;
    const requestedTestFailureClass = validation.value.test_retry_failure_class;
    const testFailureClass = requestedTestFailureClass as RetryableScheduledFailureClass | undefined;

    if (requestedTestFailureClass && !SCHEDULE_FAILURE_INJECTION_ENABLED) {
      return res.status(400).json({ error: 'ENABLE_SCHEDULE_FAILURE_INJECTION is not enabled' });
    }

    const result = await runScheduledScrape({
      site: site as any,
      slot,
      idempotencyKey,
      triggerSource: 'external',
      testFailureClass,
    });
    return res.json(result);
  } catch (err: any) {
    return res.status(errorStatusFor(err)).json({ error: err.message });
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
    checkSiteConnectivity().catch((err) => log({ stage: 'site_connectivity_check_error', error: String(err) }));
  }, 60_000);
});

app.post("/scrape-enhanced", async (req, res) => {
  const startTime = Date.now();

  try {
    const validation = validateScrapeRequest(req.body);
    if (!validation.ok) return sendValidationError(res, validation.issues);
    const { site, date_start, date_end, max_records } = validation.value;
    const scraper = (scrapers as any)[site];

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

    return res.status(errorStatusFor(err)).json({
      success: false,
      error: err.message
    });
  }
});
