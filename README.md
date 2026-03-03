# Lien Automation v2

A web scraper and automation tool for retrieving UCC (Uniform Commercial Code) filings and lien records from the California Secretary of State website, with automatic export to Google Sheets.

## Overview

This project provides an Express.js API server that scrapes lien data from the CA Secretary of State's UCC filing system and automatically pushes the results to Google Sheets for analysis and tracking.

**Current Status**: Production-oriented API with live CA SOS scraping, queueing, schedule readiness checks, externally triggered schedule runs, and persisted scheduler run history in SQLite.

## Features

- **Live CA SOS scraping API**: Uses Playwright with Chromium (via Bright Data SBR CDP) to scrape filing records.
- **Direct and queued execution paths**: `POST /scrape` pushes to Sheets immediately, while `POST /enqueue` stores records in SQLite-backed queue storage.
- **Enhanced scrape mode**: `POST /scrape-enhanced` runs scrape processing without sheet upload in the same response flow.
- **Scheduler readiness + introspection**:
  - `GET /schedule/health` reports readiness checks and configuration status.
  - `GET /schedule` returns next run windows and persisted schedule history.
- **Externally triggered schedule runs**: `POST /schedule/run` supports authenticated scheduler triggers with slot/idempotency controls.
- **Persisted scheduler history**: scheduled run records are stored in SQLite and used for idempotency/cooldown and monitoring.
- **Structured logging and retry behavior**: runtime logs and scraper safeguards for operational reliability.

## Architecture

The project is organized into three main modules:

- **Scraper** (`src/scraper/`): Handles web scraping logic for CA SOS website
- **Sheets** (`src/sheets/`): Manages Google Sheets API integration
- **Utils** (`src/utils/`): Utility functions for delays, rate limiting, retries, and logging
- **Server** (`src/server.ts`): Express.js API server with `/scrape` endpoint

Additionally, the project now includes:

- **Mission Control** (`mission-control/`): AI Agent Orchestration Dashboard for task management
- **Agents** (`agents/`): Custom MCP agents for chunk scraping, validation, and uploading
- **Cost Tracking** (`scripts/qwen-cost-tracker.js`): Monitor Qwen API usage and costs

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

Returns upcoming schedule windows and persisted run history (`history`) from the scheduler store.

### POST /schedule/run

Authenticated endpoint for external scheduler triggers. Accepts optional `slot` (`morning`/`afternoon`) and `idempotency_key`.

## What is production-ready vs requires credentials/external dependencies

### Production-ready in this repository

- API route surface and request handling (`/health`, `/version`, `/scrape`, `/enqueue`, `/scrape-all`, `/scrape-enhanced`, `/schedule/health`, `/schedule`, `/schedule/run`).
- SQLite-backed queue + scheduler persistence, including schedule history responses and missed-run checks.
- Local validation tooling (`doctor`, type checks, selector smoke tests, and health smoke script).

### Requires credentials or external services

- **Live CA SOS scraping execution** requires a working `SBR_CDP_URL` (Bright Data Scraping Browser).
- **Google Sheets upload paths** require both `SHEETS_KEY` and `SHEET_ID`.
- **Authenticated scheduled runs** require `SCHEDULE_RUN_TOKEN` and an external scheduler (cron/Cloud Scheduler/systemd timer).

## Environment Variables

Required environment variables:

- `SHEETS_KEY`: Google service account credentials (JSON string)
- `SHEET_ID`: Target Google Sheets spreadsheet ID
- `SBR_CDP_URL`: Bright Data Scraping Browser Playwright connection string (wss://brd-customer-hl_57a9fdd9-zone-lien_automation_v3:7g1mw53lymza@brd.superproxy.io:9222)

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
- **Orchestration**: Mission Control with OpenClaw Gateway
- **Monitoring**: Qwen cost tracking and analysis

## Server

The server runs on port 8080 by default.

## Mission Control Integration

This project now includes Mission Control integration for multi-agent orchestration:

- **Dashboard**: Access at `http://localhost:4000`
- **Task Management**: Create, plan, and dispatch scraping tasks
- **Agent Orchestration**: Automated chunk scraping, validation, and uploading
- **Real-time Monitoring**: Track agent activity and task progress
- **Cost Tracking**: Monitor Qwen API usage and expenses

For detailed information about the Mission Control integration, see [MISSION_CONTROL_INTEGRATION.md](MISSION_CONTROL_INTEGRATION.md).

## Notes

- The scraper is configured to run in headless mode with sandbox disabled for container compatibility.
- Human-like delays are implemented between actions to mimic natural browsing patterns.
- Only California SOS (`ca_sos`) is supported in the current version.
- On `TooManyResultsError`, halve the date range and retry.
- The scraper uses Bright Data Scraping Browser via the `SBR_CDP_URL` connection string for remote Playwright sessions.
- `SBR_CDP_URL`: Bright Data Scraping Browser Playwright connection string (wss://brd-customer-hl_57a9fdd9-zone-lien_automation_v3:7g1mw53lymza@brd.superproxy.io:9222)

## Test Commands

Use the local checks independently (doctor, types, selector fixture, runtime smoke):

```bash
npm run test:types
npm run test:selector-smoke
npm run test:smoke
npm run doctor
```

- `test:selector-smoke` runs a non-production Playwright fixture test for file-type selector variants (combobox, labeled select, and DOM fallback) with no external dependencies.
- `test:smoke` runs `scripts/smoke-health.sh` and checks the live `/health` route.
- `doctor` runs `scripts/doctor.sh` and verifies local prerequisites before smoke/runtime checks.
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

## Production Startup Path (single source of truth)

Production startup is standardized on **systemd** only. Do not use ad-hoc startup scripts, PM2 ecosystems, or mixed process managers for production.

- Service: `deploy/systemd/lien-automation-api.service`
- Scheduler trigger: `deploy/systemd/lien-automation-schedule.timer` -> `deploy/systemd/lien-automation-schedule.service`
- Runbook: [`docs/runbooks/systemd-production-runbook.md`](docs/runbooks/systemd-production-runbook.md)

## Schedule Source of Truth

Production scheduling now uses **an external scheduler** targeting one authenticated endpoint: `POST /schedule/run`.

- **Execution target:** `POST /schedule/run` only
- **Authentication:** required via `Authorization: Bearer $SCHEDULE_RUN_TOKEN` (or `x-scheduler-token`)
- **Timezone:** `America/New_York`
- **Trigger times:** exactly two daily runs: `07:30` and `14:30`
- **Idempotency:** `runScheduledScrape()` keys runs by `YYYY-MM-DD:slot` (`morning`/`afternoon`) and skips duplicates

### External scheduler configuration (exactly two triggers)

Linux cron example:

```cron
# 07:30 America/New_York
30 7 * * * curl -fsS -X POST http://127.0.0.1:8080/schedule/run \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"slot":"morning"}'

# 14:30 America/New_York
30 14 * * * curl -fsS -X POST http://127.0.0.1:8080/schedule/run \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"slot":"afternoon"}'
```

Cloud Scheduler equivalent: create two jobs (`morning`, `afternoon`) with the same endpoint/token and JSON body containing the matching `slot`.

### Runtime safeguards

1. Run records are stored in SQLite table `scheduled_runs` (not in-memory), including `slot_time`, `started_at`, `finished_at`, `status`, `records_scraped`, `rows_uploaded`, and `error`.
2. Duplicate triggers for the same `idempotency_key` are ignored unless the prior run ended in `error`; cooldown checks also read persisted DB state.
3. Missed-run monitoring checks for successful morning/afternoon runs and creates alert records in `scheduler_alerts`.
4. Optional outbound alert webhook can be enabled with `SCHEDULE_ALERT_WEBHOOK_URL`.
5. `GET /schedule/health` returns schedule readiness checks (required env vars, SQLite reachability, and Google Sheets credential parsing).


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
