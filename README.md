# Lien Automation v2

A web scraper and automation tool for retrieving UCC (Uniform Commercial Code) filings and lien records from the California Secretary of State website, with automatic export to Google Sheets.

## Overview

This project provides an Express.js API server that scrapes lien data from the CA Secretary of State's UCC filing system and automatically pushes the results to Google Sheets for analysis and tracking.

**Current Status**: MVP phase with mock data implementation. The infrastructure validates Cloud Run deployment and Google Sheets integration before full automation is implemented.

## Features

- **Web Scraping**: Uses Playwright with Chromium for headless browser automation
- **Rate Limiting**: Built-in rate limiting to prevent overwhelming target servers
- **Retry Logic**: Automatic retry mechanisms for failed requests
- **Google Sheets Integration**: Direct data export to Google Sheets
- **Structured Logging**: JSON-formatted logs with timestamps

## Architecture

The project is organized into three main modules:

- **Scraper** (`src/scraper/`): Handles web scraping logic for CA SOS website
- **Sheets** (`src/sheets/`): Manages Google Sheets API integration
- **Utils** (`src/utils/`): Utility functions for delays, rate limiting, retries, and logging
- **Server** (`src/server.ts`): Express.js API server with `/scrape` endpoint

## API Usage

### POST /scrape

Initiates a scraping job for the specified date range.

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

## Server

The server runs on port 8080 by default.

## Notes

- This is currently an MVP. Full scraping automation is in progress.
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

If you want a reproducible CA SOS scrape that **creates a new tab** in the same spreadsheet for that run (tab name includes **California + date range + Pacific timestamp**), use:

```bash
npm run test:ca-sos-range
```

Defaults:

- **DATE_START**: `02/02/2026`
- **DATE_END**: `03/02/2026`
- **MAX_RECORDS**: `25` (set `MAX_RECORDS=0` to remove the cap)

Example:

```bash
DATE_START="02/02/2026" DATE_END="03/02/2026" MAX_RECORDS=25 npm run test:ca-sos-range
```

This uses the same required environment variables (`SBR_CDP_URL`, `SHEETS_KEY`, `SHEET_ID`) and appends the results to a freshly created tab via the Google Sheets API.

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
