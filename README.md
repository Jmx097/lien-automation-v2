# Lien Automation v2

A web scraper and automation tool for retrieving lien records from multiple public filing systems, with automatic export to Google Sheets.

## Overview

This project provides an Express.js API server that scrapes lien data from the CA Secretary of State's UCC filing system and automatically pushes the results to Google Sheets for analysis and tracking.

**Current Status**: Production-oriented API with live CA SOS scraping, NYC ACRIS support, queueing, schedule readiness checks, externally triggered per-site schedule runs, and persisted scheduler run history backed by SQLite locally or Postgres when `DATABASE_URL` is set.

## Planning Documents

- [Project Plan & Delivery Summary](PR_SUMMARY.md)
- [Phase 2 Plan (Operational Maturity)](PHASE2_PLAN.md)

## Features

- **Multi-site scraping API**: Supports CA SOS and NYC ACRIS via Playwright-based browser automation.
- **Direct and queued execution paths**: `POST /scrape` pushes to Sheets immediately, while `POST /enqueue` stores records in SQLite-backed queue storage.
- **Enhanced scrape mode**: `POST /scrape-enhanced` runs scrape processing without sheet upload in the same response flow.
- **Scheduler readiness + introspection**:
  - `GET /schedule/health` reports readiness checks and configuration status.
  - `GET /schedule` returns next run windows and persisted schedule history.
- **Externally triggered schedule runs**: `POST /schedule/run` supports authenticated scheduler triggers with slot/idempotency controls.
- **Persisted scheduler history**: scheduled run records are stored in SQLite locally and can use Postgres-backed durable state in hosted environments when `DATABASE_URL` is configured.
- **Structured logging and retry behavior**: runtime logs and scraper safeguards for operational reliability.

## Architecture

The project is organized into three main modules:

- **Scraper** (`src/scraper/`): Handles web scraping logic for CA SOS and NYC ACRIS
- **Sheets** (`src/sheets/`): Manages Google Sheets API integration
- **Utils** (`src/utils/`): Utility functions for delays, rate limiting, retries, and logging
- **Server** (`src/server.ts`): Express.js API server with `/scrape` endpoint

## API Endpoints

The following routes are defined by the Express server.

### GET /health

Basic liveness check.

### GET /version

Runtime version metadata (`git_sha`, app version, Node version).

### POST /scrape

Runs a scrape for a single site/date range and uploads results to Google Sheets.

**Request Body**:
```json
{
  "site": "ca_sos",
  "date_start": "01/01/2024",
  "date_end": "01/31/2024",
  "max_records": 100
}
```

**Required Fields**: `site`, `date_start`, `date_end`

**Response**:
```json
{
  "success": true,
  "records_scraped": 5,
  "rows_uploaded": 5,
  "duration_seconds": 2.5
}
```

### POST /enqueue

Runs a scrape and writes results to the SQLite queue store for downstream processing.

### POST /scrape-all

Runs scrape attempts for all configured scrapers and returns per-site outcomes.

### POST /scrape-enhanced

Runs scrape processing and returns record counts without a Sheets upload step in that route.

### GET /schedule/health

Returns scheduler readiness status and checks (including env/config dependencies).

### GET /schedule

Returns upcoming schedule windows and persisted run history (`history`) from the scheduler store. Each `next_runs` entry now includes the trigger time used by the external scheduler plus the target CA/NYC finish-by time.

### POST /schedule/run

Authenticated endpoint for external scheduler triggers. Accepts optional `site`, `slot` (`morning`/`afternoon`), and `idempotency_key`. Scheduled runs create a brand-new sheet tab per run.

When `ENABLE_SCHEDULE_FAILURE_INJECTION=1`, operators may also send `test_retry_failure_class` with one of `timeout_or_navigation`, `viewer_roundtrip`, `token_or_session_state`, or `sheet_export` to force exactly one retryable failure on the first attempt of that logical run. This is intended for controlled canaries only.

## What is production-ready vs requires credentials/external dependencies

### Production-ready in this repository

- API route surface and request handling (`/health`, `/version`, `/scrape`, `/enqueue`, `/scrape-all`, `/scrape-enhanced`, `/schedule/health`, `/schedule`, `/schedule/run`).
- SQLite-backed queue persistence plus scheduler persistence that uses SQLite locally and Postgres when `DATABASE_URL` is present, including schedule history responses and missed-run checks.
- Local validation tooling (`doctor`, type checks, selector smoke tests, and health smoke script).

### Requires credentials or external services

- **Live browser scraping execution** requires one of `BRIGHTDATA_BROWSER_WS`, `BRIGHTDATA_PROXY_SERVER`, or `SBR_CDP_URL`.
- **Google Sheets upload paths** require both `SHEETS_KEY` and `SHEET_ID`.
- **Director-facing merged sheet publishing** uses `MERGED_SHEET_ID` when set, or falls back to the built-in target sheet ID currently configured in the app.
- **Authenticated scheduled runs** require `SCHEDULE_RUN_TOKEN` and an external scheduler (cron/Cloud Scheduler/systemd timer).

## SQLite Queue DB Initialization

- Queue store path remains `data/db/lien-queue.db` for compatibility.
- `SQLiteQueueStore` auto-creates the parent directory (`data/db`) and initializes the SQLite schema on startup.
- `node src/queue/init-db.js` remains safe to run and can still be used as an explicit bootstrap/migration step, but queue/scheduler operations no longer depend on running it first.

## Environment Variables

Required environment variables:

- `SHEETS_KEY`: Google service account credentials (JSON string)
- `SHEET_ID`: Target Google Sheets spreadsheet ID
- One browser transport:
  - `BRIGHTDATA_BROWSER_WS`
  - `BRIGHTDATA_PROXY_SERVER` plus `BRIGHTDATA_PROXY_USERNAME` and `BRIGHTDATA_PROXY_PASSWORD`
  - `SBR_CDP_URL` (legacy compatibility)
- `SCHEDULE_RUN_TOKEN`: Auth token for `POST /schedule/run`

Important optional environment variables:

- `MERGED_SHEET_ID` points the scheduled-run merged `Master` publish at a separate Google Sheet. When omitted, the app uses the currently configured default destination sheet and falls back to the source workbook `Master` tab if that destination is not reachable yet.
- `DATABASE_URL` enables the Postgres-backed scheduler store. When unset, the scheduler store remains on local SQLite.
- In hosted Cloud Run, the first-pass secret migration now expects `DATABASE_URL`, `SCHEDULE_RUN_TOKEN`, `SHEETS_KEY`, and `SBR_CDP_URL` to be supplied via Secret Manager-backed env vars while keeping the same runtime variable names.
- `SCHEDULE_RUN_MAX_ATTEMPTS` controls the scheduler-level retry budget for one logical scheduled run. Default: `3`.
- `SCHEDULE_RUN_BASE_DELAY_MS` sets the initial retry backoff between retryable scheduled-run attempts. Default: `1000`.
- `SCHEDULE_RUN_MAX_DELAY_MS` caps the scheduler retry backoff. Default: `10000`.
- `SCHEDULE_ANOMALY_BASELINE_RUNS` controls how many recent successful runs are inspected when building the anomaly baseline. Default: `5`.
- `SCHEDULE_ANOMALY_MIN_BASELINE_RUNS` is the minimum number of eligible successful runs required before anomaly detection activates. Default: `3`.
- `SCHEDULE_ANOMALY_RECORDS_DROP_PCT`, `SCHEDULE_ANOMALY_AMOUNT_COVERAGE_DROP_PTS`, `SCHEDULE_ANOMALY_OCR_SUCCESS_DROP_PTS`, and `SCHEDULE_ANOMALY_ROW_FAIL_RISE_PTS` tune successful-run anomaly thresholds. Defaults: `40`, `15`, `20`, and `20`.
- `ENABLE_SCHEDULE_FAILURE_INJECTION=1` enables a canary-only test hook for `POST /schedule/run`. Leave it unset for normal production operation.

- `SCHEDULE_CA_SOS_WEEKLY_DAYS`, `SCHEDULE_CA_SOS_RUN_HOUR`, `SCHEDULE_CA_SOS_RUN_MINUTE`, `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES`, `SCHEDULE_CA_SOS_TIMEZONE`
- `SCHEDULE_NYC_ACRIS_WEEKLY_DAYS`, `SCHEDULE_NYC_ACRIS_RUN_HOUR`, `SCHEDULE_NYC_ACRIS_RUN_MINUTE`, `SCHEDULE_NYC_ACRIS_DEADLINE_HOUR`, `SCHEDULE_NYC_ACRIS_DEADLINE_MINUTE`, `SCHEDULE_NYC_ACRIS_TIMEZONE`, `SCHEDULE_NYC_ACRIS_MAX_RECORDS`
- `SCHEDULE_MAX_RECORDS`, `SCHEDULE_MAX_RECORDS_FLOOR`, `SCHEDULE_MAX_RECORDS_CEILING`
- `ACRIS_MAX_RESULT_PAGES`, `ACRIS_INITIAL_MAX_RECORDS`, `ACRIS_INITIAL_MAX_RESULT_PAGES`, `ACRIS_OUT_DIR`
- `TESSERACT_PATH`, `PDFTOPPM_PATH`, `REQUIRE_OCR_TOOLS`

For CA SOS scheduled runs specifically:

- `SCHEDULE_CA_SOS_RUN_HOUR` / `SCHEDULE_CA_SOS_RUN_MINUTE` are the target finish-by time.
- `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES` controls how much earlier the external scheduler should call `POST /schedule/run` so the CA probe + scrape can finish ahead of that time. Default: `180`.
- Scheduled CA runs size themselves from the live `Results: N` value on the search results page. If the probe finds `0`, the run completes successfully without scraping rows or uploading a sheet tab.

## OCR Runtime Notes

- OCR-backed amount extraction requires both `tesseract` and `pdftoppm`.
- By default the app looks for those binaries on `PATH`.
- On Windows/dev machines you can also point directly at the binaries with:
  - `TESSERACT_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe`
  - `PDFTOPPM_PATH=C:\path\to\poppler\Library\bin\pdftoppm.exe`
- `GET /schedule/health` reports `ocr_runtime_ready=false` when those binaries are missing or not executable.
- `GET /schedule/health` now also reports whether the source workbook is reachable plus whether merged-output publishing is using the destination sheet or source-workbook fallback mode.
- `REQUIRE_OCR_TOOLS=1` keeps scheduled readiness strict. Set `REQUIRE_OCR_TOOLS=0` only if you intentionally want to allow runs without OCR-backed extraction.

## Data Schema

Scraped records include the following fields:
- UCC Type (e.g., "Federal Tax Lien")
- Debtor Name
- Debtor Address
- File Number
- Secured Party Name
- Secured Party Address
- Status
- Filing Date
- Lapse Date
- Document Type
- PDF Filename

Data is exported to Google Sheets with a state column prepended.

## Technology Stack

- **Runtime**: Node.js with TypeScript
- **Web Framework**: Express.js
- **Scraping**: Playwright (Chromium)
- **API Integration**: Google Sheets API (googleapis)
- **Orchestration**: External scheduler trigger (`POST /schedule/run`) with durable run history backed by SQLite locally or Postgres in hosted deployments
- **Monitoring**: Qwen cost tracking and analysis

## Server

The server runs on port 8080 by default.

## Production Resource Footprint (Memory/Disk)

If your host is running near memory or disk limits, you can lower usage without reducing scrape quality:

- Default `docker-compose.yml` now runs only the core `lien-scraper` service.
- Docker log rotation is enabled (`max-size: 10m`, `max-file: 5`) to prevent unbounded container log growth.

Operational cleanup commands (safe housekeeping):

```bash
# Reclaim unused image/layer/cache space
docker system prune -af

# Reclaim only unused build cache (more conservative)
docker builder prune -af

# Vacuum journald if systemd logs are large
sudo journalctl --vacuum-time=7d
```

These settings reduce overhead while keeping scraper behavior and output unchanged.

## Notes

- The scraper is configured to run in headless mode with sandbox disabled for container compatibility.
- Human-like delays are implemented between actions to mimic natural browsing patterns.
- Supported sites are `ca_sos` and `nyc_acris`.
- NYC ACRIS uses a session-preserving browser flow with hidden-form submits, fresh anti-forgery tokens, and viewer extraction via `iframe[name="mainframe"]`.
- The scraper supports Bright Data Browser API first, direct proxy mode second, and legacy `SBR_CDP_URL` as a compatibility fallback.

## Test Commands

Use the local checks independently (doctor, types, selector fixture, runtime smoke):

```bash
npm run test:types
npm run test:selector-smoke
npm run test:smoke
npm run doctor
```

- `test:selector-smoke` runs a non-production Playwright fixture test for file-type selector variants (combobox, labeled select, and DOM fallback) with no external dependencies.
- `test:smoke` runs the cross-platform `scripts/smoke-health.js` health probe and checks the live `/health` route.
- `doctor` runs the cross-platform `scripts/doctor.js` preflight and verifies local prerequisites before smoke/runtime checks.
- `npm test` runs `test:types` + `test:selector-smoke` companion checks.

## Setup Troubleshooting (stale clone symptom)

If `npm run` only shows:

- `test -> echo "Error: no test specified" && exit 1`

and does not show `doctor`, `test:types`, and `test:smoke`, your local checkout is behind. Sync your branch and verify scripts:

```bash
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
git fetch --all --prune
git checkout work || true
git pull --rebase
npm run
```

You should see `doctor` (wired to `scripts/doctor.sh`) and `test:smoke` (wired to `scripts/smoke-health.sh`).


## Run Logging + 2026 Week Test Example

Track server logs to a file while still seeing output:

```bash
mkdir -p logs
npm run dev 2>&1 | tee logs/server-$(date +%F-%H%M%S).log
```

Example one-week 2026 test request:

```bash
curl -sS -X POST http://127.0.0.1:8080/scrape   -H 'Content-Type: application/json'   -d '{"site":"ca_sos","date_start":"01/05/2026","date_end":"01/11/2026","max_records":25}'
```

## 2026 Range Test (writes to a new Sheet tab)

If you want a reproducible CA SOS scrape that **creates a new tab** in the same spreadsheet for that run (tab name includes **label + date range + Pacific timestamp**), use:

```bash
npm run test:ca-sos-range
```

Defaults:

- **LABEL**: `California` (customize for county-specific tabs)
- **DATE_START**: `02/02/2026`
- **DATE_END**: `03/02/2026`
- **MAX_RECORDS**: `25` (set `MAX_RECORDS=0` to remove the cap)

Examples:

```bash
# Default California run
DATE_START="02/02/2026" DATE_END="03/02/2026" MAX_RECORDS=25 npm run test:ca-sos-range

# Custom county label
LABEL="Los Angeles County" DATE_START="02/02/2026" DATE_END="03/02/2026" MAX_RECORDS=25 npm run test:ca-sos-range
```

This uses the same required environment variables (`SBR_CDP_URL`, `SHEETS_KEY`, `SHEET_ID`) and appends the results to a freshly created tab via the Google Sheets API.

## NYC ACRIS Live Validation

Before enabling full NYC production cadence, use the dedicated Browser API validation flow:

```bash
npm run validate:nyc-acris-live
```

- Requires `BRIGHTDATA_BROWSER_WS`.
- Runs a single uninterrupted NYC session through results, viewer iframe extraction, and return-to-results checks.
- Writes a redacted validation manifest under `out/acris/`.

For a capped end-to-end canary that also writes to Sheets:

```bash
npm run canary:nyc-acris
```

## Last 7 Days Helper (default max 10)

For a quick end-to-end run that scrapes the **last 7 calendar days (including today)** from CA SOS and pushes directly to Sheets using the existing mapping:

```bash
npx ts-node scripts/run-last7days.ts
```

- **Default cap**: 10 records per run (via the scraper’s `max_records` default).
- **Temporary override** (this run only):
  - Using env: `MAX_RECORDS_OVERRIDE=50 npx ts-node scripts/run-last7days.ts`
  - Using CLI: `npx ts-node scripts/run-last7days.ts 50` (takes precedence over env)

If an invalid override (non-numeric or ≤ 0) is provided, the script falls back to using 10 for that run.

## Production Startup Path (single source of truth)

Production startup is standardized on **systemd** only. Do not use ad-hoc startup scripts, PM2 ecosystems, or mixed process managers for production.

- Service: `deploy/systemd/lien-automation-api.service`
- Scheduler trigger: `deploy/systemd/lien-automation-schedule.timer` -> `deploy/systemd/lien-automation-schedule.service`
- Runbook: [`docs/runbooks/systemd-production-runbook.md`](docs/runbooks/systemd-production-runbook.md)

## GitHub-Centered Deployments

Use GitHub as the deployment source of truth for both hosted environments:

- **Cloud Run service**: run `.github/workflows/deploy-cloud-run-service.yml` with the target ref.
- **DigitalOcean droplet**: run `.github/workflows/deploy-droplet.yml` with the target ref.
- **Droplet rollout logic** lives in `scripts/ops/deploy-droplet.sh` and assumes the server has a clean git checkout plus systemd units already installed.

Required GitHub configuration:

- **Actions secrets (Cloud Run)**: `GCP_PROJECT_ID`, `GCP_REGION`, `GAR_REPOSITORY`, `GCP_SA_KEY_JSON`, `SHEET_ID`, `CLOUDSQL_INSTANCE_CONNECTION_NAME`, `DATABASE_URL_SECRET_REF`, `SCHEDULE_RUN_TOKEN_SECRET_REF`, `SHEETS_KEY_SECRET_REF`, `SBR_CDP_URL_SECRET_REF`
- **Actions secrets (Droplet)**: `DO_HOST`, `DO_USERNAME`, `DO_SSH_KEY`, `DO_APP_DIRECTORY`
- **Actions secrets or variables (optional scheduler tuning)**: `SCHEDULE_TARGET_TIMEZONE`, `SCHEDULE_WEEKLY_DAYS`, `SCHEDULE_RUN_HOUR`, `SCHEDULE_RUN_MINUTE`, `SCHEDULE_DEADLINE_HOUR`, `SCHEDULE_DEADLINE_MINUTE`, `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES`, `AMOUNT_MIN_COVERAGE_PCT`, `SCHEDULE_AUTO_THROTTLE`, `SCHEDULE_MAX_RECORDS`, `SCHEDULE_MAX_RECORDS_FLOOR`, `SCHEDULE_MAX_RECORDS_CEILING`, `REQUIRE_OCR_TOOLS`

Cloud Run workflow expectation:

- GitHub Actions deploys should preserve the Cloud SQL instance attachment, inject `DATABASE_URL` from Secret Manager, and publish the workflow commit SHA to `/version` via `GIT_SHA`.
- The current runtime secret mapping is `DATABASE_URL`, `SCHEDULE_RUN_TOKEN`, `SHEETS_KEY`, and `SBR_CDP_URL` from Secret Manager-backed env vars; `SHEET_ID` and schedule tuning remain plain env vars.
- The Cloud Run runtime service account must have `roles/secretmanager.secretAccessor` on those runtime secrets.
- Avoid printing raw Cloud Run environment values during troubleshooting; hosted env output includes secrets such as scheduler tokens, Sheets credentials, and database credentials.

## Schedule Source of Truth

Production scheduling now uses **an external scheduler** targeting one authenticated endpoint: `POST /schedule/run`.

- **Execution target:** `POST /schedule/run` only
- **Authentication:** required via `Authorization: Bearer $SCHEDULE_RUN_TOKEN` (or `x-scheduler-token`)
- **Timezone:** `America/New_York`
- **CA SOS semantics:** `SCHEDULE_CA_SOS_RUN_HOUR` / `SCHEDULE_CA_SOS_RUN_MINUTE` are finish-by times, and the external trigger should use `GET /schedule` `trigger_time` (finish-by minus `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES`)
- **Trigger times:** CA SOS Tue/Wed at `06:00` by default for a `09:00` finish-by; NYC ACRIS Tue/Wed/Thu/Fri at `14:00`
- **Idempotency:** optional; set `ENABLE_SCHEDULE_IDEMPOTENCY=1` to key runs by `YYYY-MM-DD:slot` (`morning`/`afternoon`) and skip duplicates. Default is off so each scheduled trigger creates a fresh run/tab

### External scheduler configuration (per-site triggers)

Linux cron example:

```cron
# CA SOS Tue/Wed 06:00 America/New_York (default 3-hour lead for a 09:00 finish-by time)
0 6 * * 2,3 curl -fsS -X POST http://127.0.0.1:8080/schedule/run \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"site":"ca_sos","slot":"morning"}'

# NYC ACRIS Tue/Wed/Thu/Fri 14:00 America/New_York
0 14 * * 2,3,4,5 curl -fsS -X POST http://127.0.0.1:8080/schedule/run \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"site":"nyc_acris","slot":"afternoon"}'
```

Cloud Scheduler equivalent: create one job per site with the appropriate schedule and JSON body.

### Runtime safeguards

1. Run records are stored in the scheduler store table `scheduled_runs` (SQLite locally, Postgres when `DATABASE_URL` is set), including `slot_time`, `started_at`, `finished_at`, `status`, `records_scraped`, `rows_uploaded`, and `error`.
2. Duplicate triggers for the same `idempotency_key` are ignored unless the prior run ended in `error`; cooldown checks also read persisted DB state.
3. Missed-run monitoring checks for successful morning/afternoon runs and creates alert records in `scheduler_alerts`.
4. Optional outbound alert webhook can be enabled with `SCHEDULE_ALERT_WEBHOOK_URL`. It is used for missed runs, connectivity alerts, and successful-run quality anomalies.
5. `GET /schedule/health` returns schedule readiness checks (required env vars, scheduler-store reachability, and Google Sheets credential parsing).
6. Successful scheduled runs write raw `Scheduled_*` tabs into the source workbook (`SHEET_ID`) and then publish a merged `Master` dataset to the destination workbook (`MERGED_SHEET_ID` or the built-in default target). If the destination workbook is not reachable yet, the merged `Master` publish falls back to the source workbook so scheduled runs remain operational.
7. Scheduled runs now keep one logical run record across retryable failures, recording `attempt_count`, `max_attempts`, `retried`, and `retry_exhausted` while using bounded backoff for transient scrape and sheet-export failures.
8. Successful scheduled runs also compare their row-count and quality metrics against recent successful baselines and emit advisory anomaly alerts without changing the final run status.

## Monitoring Setup Notes

- `scripts/cloud/setup-monitoring.sh` now treats the log-based metrics, alert policies, notification channel reuse, and uptime check as the default monitoring path.
- BigQuery sink creation is optional and disabled by default via `ENABLE_BIGQUERY_SINK=0` because the local `bq` tool can fail independently of the app.
- If `bq` currently crashes with `AttributeError: module 'absl.flags' has no attribute 'FLAGS'`, treat that as a local Cloud SDK tooling issue rather than a production app/runtime issue.

## Secret Manager Notes

- Local development still uses plain env vars directly; Secret Manager is only part of the hosted Cloud Run deploy path.
- `scripts/cloud/deploy-cloud-run-service.sh` now expects Secret Manager refs for `DATABASE_URL`, `SCHEDULE_RUN_TOKEN`, `SHEETS_KEY`, and `SBR_CDP_URL`, and injects them with `--set-secrets`.
- `scripts/cloud/sync-cloud-run-secrets-to-secret-manager.ps1` can bootstrap those runtime secrets from the currently deployed Cloud Run service without printing their values.
- Use `DRY_RUN=1 bash scripts/cloud/deploy-cloud-run-service.sh` to validate the deploy argument shape without printing secret values.


## Cloud Run Job Deployment

For production one-shot scraping on Cloud Run Jobs (with two Cloud Scheduler triggers, retry policy, and monitoring sink setup), see [`docs/deployment/cloud-run-job.md`](docs/deployment/cloud-run-job.md).

## Code Quality: Linting and Formatting

This project now includes linting and code formatting tools to ensure code quality and consistency.

### Tools Included

- **ESLint** with TypeScript support for static code analysis
- **Prettier** for automatic code formatting

### Available Scripts

```bash
# Check for linting issues
npm run lint

# Fix linting issues automatically
npm run lint:fix

# Format code with Prettier
npm run format
```

### Configuration Files

- `eslint.config.js` - Modern ESLint configuration
- `.prettierrc` - Prettier formatting rules

For detailed information about the linting setup, see [LINTING_SETUP.md](LINTING_SETUP.md).

## Preflight: runtime version must match local commit

Before running any scrape test, verify the running container version matches your local commit SHA.

```bash
cd "$(git rev-parse --show-toplevel)"
LOCAL_SHA=$(git rev-parse --short HEAD)
RUNTIME_SHA=$(curl -fsS http://127.0.0.1:8080/version | node -e 'process.stdin.once("data", d => console.log(JSON.parse(d).git_sha ?? "unknown"))')
echo "local=$LOCAL_SHA runtime=$RUNTIME_SHA"
```

`curl /version` **must** report the same SHA as `git rev-parse --short HEAD` before any scrape test.

If SHAs do not match, force a rebuild:

```bash
docker compose down && docker compose build --no-cache && docker compose up -d
```

## Verify Running Build Version

Set and pass the current Git SHA into Docker Compose, then verify via the API:

```bash
export GIT_SHA=$(git rev-parse --short HEAD)
docker compose down
docker compose build --no-cache
docker compose up -d
curl -sS http://127.0.0.1:8080/version
```

Expected response includes the same `git_sha` you exported. Operators should always run `export GIT_SHA=$(git rev-parse --short HEAD)` before `docker compose build`.

## Contribution Note: avoid backup artifacts

Please do not commit editor or manual backup files (for example `*.bak`, `*.broken`, or `*.bak-*`).
These are local artifacts and should remain untracked.

