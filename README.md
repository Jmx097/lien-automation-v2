# Lien Automation v2

A web scraper and automation tool for retrieving lien records from multiple public filing systems, with automatic export to Google Sheets.

## Overview

This project provides an Express.js API server that scrapes lien data from the CA Secretary of State's UCC filing system and automatically pushes the results to Google Sheets for analysis and tracking.

**Current Status**: Production-oriented API with live CA SOS scraping, NYC ACRIS support, Maricopa Recorder support, queueing, schedule readiness checks, externally triggered per-site schedule runs, and persisted scheduler run history backed by SQLite locally or Postgres when `DATABASE_URL` is set.

## Planning Documents

- [Project Plan & Delivery Summary](PR_SUMMARY.md)
- [Phase 2 Plan (Operational Maturity)](PHASE2_PLAN.md)

## Features

- **Multi-site scraping API**: Supports CA SOS, NYC ACRIS, and Maricopa Recorder.
- **Direct and queued execution paths**: `POST /scrape` pushes to Sheets immediately, while `POST /enqueue` stores records in SQLite-backed queue storage.
- **Enhanced scrape mode**: `POST /scrape-enhanced` runs scrape processing without sheet upload in the same response flow.
- **Scheduler readiness + introspection**:
  - `GET /schedule/health` reports readiness checks and configuration status.
  - `GET /schedule` returns next run windows and persisted schedule history, including per-run confidence summaries.
- `GET /schedule/confidence` returns a lightweight operator view of recent run confidence outcomes.
- `npm run proof:scheduled-evidence` triggers a repeatable scheduled-run proof flow that writes fresh per-site evidence for `/schedule` and `/schedule/confidence`.
- **Externally triggered schedule runs**: `POST /schedule/run` supports authenticated scheduler triggers with slot/idempotency controls.
- **Persisted scheduler history**: scheduled run records are stored in SQLite locally and can use Postgres-backed durable state in hosted environments when `DATABASE_URL` is configured.
- **Structured logging and retry behavior**: runtime logs and scraper safeguards for operational reliability.

## Architecture

The project is organized into three main modules:

- **Scraper** (`src/scraper/`): Handles web scraping logic for CA SOS, NYC ACRIS, and Maricopa Recorder
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
  "site": "maricopa_recorder",
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

### GET /schedule/confidence

Returns a compact recent-runs view intended for operator dashboards, including confidence status, reasons, and key tab/failure metadata.

### POST /schedule/run

Authenticated endpoint for external scheduler triggers. Accepts optional `site`, `slot` (`morning`/`afternoon`/`evening`), and `idempotency_key`. Scheduled runs create a brand-new sheet tab per run.

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
- **Director-facing lead alerts** can send a `New leads!` email via `LEAD_ALERT_WEBHOOK_URL` or direct Resend delivery when `LEAD_ALERT_RESEND_API_KEY` and `LEAD_ALERT_EMAIL_FROM` are configured. `LEAD_ALERT_EMAIL_TO` defaults to `antigravity1@timberlinetax.com`.
- **Authenticated scheduled runs** require `SCHEDULE_RUN_TOKEN` and an external scheduler (cron/Cloud Scheduler/systemd timer).
- **Operational alerting** should configure both `LEAD_ALERT_WEBHOOK_URL` and `SCHEDULE_ALERT_WEBHOOK_URL` so lead emails, missed-run alerts, connectivity alerts, and anomaly alerts are all externally visible.

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
- `REVIEW_QUEUE_RETENTION_DAYS` controls how long quarantined rows stay in `Review_Queue` before they are dropped during the next merge rebuild. Default: `7`.
- `DIRECTOR_MIN_CONFIDENCE_ACCEPT` controls when a row is considered fully confident for `Master` publishing. Default: `0.85`.
- `DIRECTOR_MIN_CONFIDENCE_REVIEW` controls when a low-confidence row must be sent to `Review_Queue` even if it is otherwise clean. Default: `0.75`.
- `LEAD_ALERT_EMAIL_TO` controls who receives the `New leads!` notification. Default: `antigravity1@timberlinetax.com`.
- `LEAD_ALERT_WEBHOOK_URL` lets the app post the generic HTML notification payload to an external mailer/automation layer.
- `LEAD_ALERT_RESEND_API_KEY` plus `LEAD_ALERT_EMAIL_FROM` enables direct Resend delivery for the `New leads!` email.
- `SCHEDULE_ALERT_WEBHOOK_URL` enables missed-run, connectivity, and quality-anomaly alerts from the scheduler.
- `DATABASE_URL` enables the Postgres-backed scheduler store. When unset, the scheduler store remains on local SQLite.
- In hosted Cloud Run, the first-pass secret migration now expects `DATABASE_URL`, `SCHEDULE_RUN_TOKEN`, `SHEETS_KEY`, and `SBR_CDP_URL` to be supplied via Secret Manager-backed env vars while keeping the same runtime variable names.
- `NYC_ACRIS_TRANSPORT_MODE` selects the NYC production transport. Supported values: `legacy-sbr-cdp`, `brightdata-browser-api`, `brightdata-proxy`, `local`. Default production recommendation: `legacy-sbr-cdp`.
- `SCHEDULE_RUN_MAX_ATTEMPTS` controls the scheduler-level retry budget for one logical scheduled run. Default: `3`.
- `SCHEDULE_RUN_BASE_DELAY_MS` sets the initial retry backoff between retryable scheduled-run attempts. Default: `1000`.
- `SCHEDULE_RUN_MAX_DELAY_MS` caps the scheduler retry backoff. Default: `10000`.
- `SCHEDULE_ANOMALY_BASELINE_RUNS` controls how many recent successful runs are inspected when building the anomaly baseline. Default: `5`.
- `SCHEDULE_ANOMALY_MIN_BASELINE_RUNS` is the minimum number of eligible successful runs required before anomaly detection activates. Default: `3`.
- `SCHEDULE_ANOMALY_RECORDS_DROP_PCT`, `SCHEDULE_ANOMALY_AMOUNT_COVERAGE_DROP_PTS`, `SCHEDULE_ANOMALY_OCR_SUCCESS_DROP_PTS`, and `SCHEDULE_ANOMALY_ROW_FAIL_RISE_PTS` tune successful-run anomaly thresholds. Defaults: `40`, `15`, `20`, and `20`.
- `ENABLE_SCHEDULE_FAILURE_INJECTION=1` enables a canary-only test hook for `POST /schedule/run`. Leave it unset for normal production operation.
- `SITE_ID_CA_SOS=20`, `SITE_ID_NYC_ACRIS=12`, and `SITE_ID_MARICOPA_RECORDER=13` control downstream sheet site IDs.
- `MARICOPA_DOCUMENT_CODE` defaults to `FL`.
- `MARICOPA_MAX_RECORDS` caps Maricopa direct-scrape volume when `max_records` is omitted.
- `MARICOPA_ENABLE_ARTIFACT_RETRIEVAL=1` enables Maricopa artifact lookup + OCR enrichment. Set `0` to force API-only fallback rows.
- `MARICOPA_ARTIFACT_URL_TEMPLATE` can pin a discovered public artifact URL shape using `{recordingNumber}` as the placeholder.
- `MARICOPA_SESSION_MAX_AGE_MINUTES`, `MARICOPA_RETRY_ATTEMPTS`, `MARICOPA_RETRY_BASE_DELAY_MS`, `MARICOPA_RETRY_MAX_DELAY_MS`, and `MARICOPA_OCR_MAX_PAGES` tune Maricopa session freshness, retry behavior, and OCR scope.
- `MARICOPA_OUT_DIR` overrides the default `out/maricopa` workspace for session state, discovery artifacts, and downloaded artifacts.

- `SCHEDULE_CA_SOS_WEEKLY_DAYS`, `SCHEDULE_CA_SOS_MORNING_RUN_HOUR`, `SCHEDULE_CA_SOS_MORNING_RUN_MINUTE`, `SCHEDULE_CA_SOS_AFTERNOON_RUN_HOUR`, `SCHEDULE_CA_SOS_AFTERNOON_RUN_MINUTE`, `SCHEDULE_CA_SOS_EVENING_RUN_HOUR`, `SCHEDULE_CA_SOS_EVENING_RUN_MINUTE`, `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES`, `SCHEDULE_CA_SOS_TIMEZONE`
- `SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS`, `SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_HOUR`, `SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_MINUTE`, `SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_HOUR`, `SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_MINUTE`, `SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_HOUR`, `SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_MINUTE`, `SCHEDULE_MARICOPA_RECORDER_TIMEZONE`, `SCHEDULE_MARICOPA_RECORDER_MAX_RECORDS`
- `SCHEDULE_NYC_ACRIS_WEEKLY_DAYS`, `SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR`, `SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE`, `SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR`, `SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE`, `SCHEDULE_NYC_ACRIS_EVENING_RUN_HOUR`, `SCHEDULE_NYC_ACRIS_EVENING_RUN_MINUTE`, `SCHEDULE_NYC_ACRIS_TIMEZONE`, `SCHEDULE_NYC_ACRIS_MAX_RECORDS`
- `NYC_ACRIS_TRANSPORT_MODE`
- `SCHEDULE_MAX_RECORDS`, `SCHEDULE_MAX_RECORDS_FLOOR`, `SCHEDULE_MAX_RECORDS_CEILING`
- `ACRIS_MAX_RESULT_PAGES`, `ACRIS_INITIAL_MAX_RECORDS`, `ACRIS_INITIAL_MAX_RESULT_PAGES`, `ACRIS_OUT_DIR`
- `TESSERACT_PATH`, `PDFTOPPM_PATH`, `REQUIRE_OCR_TOOLS`

For CA SOS scheduled runs specifically:

- `SCHEDULE_CA_SOS_*_RUN_HOUR` / `SCHEDULE_CA_SOS_*_RUN_MINUTE` are the target finish-by times for the `morning`, `afternoon`, and `evening` slots.
- `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES` controls how much earlier the external scheduler should call `POST /schedule/run` so the CA probe + scrape can finish ahead of that time. Default: `180`.
- Scheduled CA runs size themselves from the live `Results: N` value on the search results page. If the probe finds `0`, the run completes successfully without scraping rows or uploading a sheet tab.
- Cloud Run deploy defaults schedule CA SOS, Maricopa Recorder, and NYC ACRIS on weekdays only in `America/Denver`, with finish-by targets of `10:00`, `14:00`, and `22:00`.
- Active schedule configuration should be set with explicit per-site env vars (`SCHEDULE_CA_SOS_*`, `SCHEDULE_MARICOPA_RECORDER_*`, `SCHEDULE_NYC_ACRIS_*`) rather than relying on legacy global fallback vars.

## Operator Thread Handoff

To keep operational threads compact, prefer carrying forward only:

- current deployed state
- completed validations
- current blocker
- immediate next actions
- exact service URL or key command only when needed

Avoid pasting full endpoint payloads, repeated change inventories, or long historical logs into the next thread unless they are still the active blocker.

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
- Supported sites are `ca_sos`, `nyc_acris`, and `maricopa_recorder`.
- NYC ACRIS uses a session-preserving browser flow with hidden-form submits, fresh anti-forgery tokens, and viewer extraction via `iframe[name="mainframe"]`.
- NYC execution can use a site-specific transport policy. Production should pin `NYC_ACRIS_TRANSPORT_MODE=legacy-sbr-cdp` until Browser API proves stable in hosted runs again.
- Maricopa Recorder uses the site’s public JSON endpoints for search/detail. It clamps future `date_end` values to the latest searchable date returned by `publicapi.recorder.maricopa.gov/documents/index`.
- Maricopa currently exposes names and recording metadata through the observed public API, but not debtor address or amount fields. Those remain blank/low-confidence until a public artifact/detail endpoint is confirmed.
- Maricopa may return Cloudflare challenge/interstitial pages. The scraper detects these and raises a Maricopa-specific retryable failure instead of treating the body as valid JSON.
- The scraper still supports Bright Data Browser API, direct proxy mode, and legacy `SBR_CDP_URL`, but NYC production execution now prefers the site-specific transport policy over the global Browser API-first order.

## Test Commands

## Maricopa Notes

- Maricopa can use a discovered public artifact URL plus a persisted browser session to download a record artifact, run OCR, and populate debtor address, amount, and lead type when available.
- Maricopa rows that still cannot be completed are marked with internal reasons such as `artifact_not_found`, `ocr_no_text`, or `address_missing` so downstream review logic can quarantine them.

Use the local checks independently (doctor, types, selector fixture, runtime smoke):

```bash
npm run test:types
npm run test:selector-smoke
npm run test:smoke
npm run doctor
npm run refresh:maricopa-session
npm run discover:maricopa-live
npm run validate:maricopa-live
```

- `test:selector-smoke` runs a non-production Playwright fixture test for file-type selector variants (combobox, labeled select, and DOM fallback) with no external dependencies.
- `test:smoke` runs the cross-platform `scripts/smoke-health.js` health probe and checks the live `/health` route.
- `doctor` runs the cross-platform `scripts/doctor.js` preflight and verifies local prerequisites before smoke/runtime checks.
- `npm test` runs `test:types` + `test:selector-smoke` companion checks.
- `refresh:maricopa-session` opens the Maricopa results page, waits for the challenge-cleared table, and saves Playwright storage state under `out/maricopa/session/`.
- `discover:maricopa-live` uses the persisted browser session to capture Maricopa network requests and stores candidate public artifact URL templates under `out/maricopa/discovery-candidates.json`.
- `validate:maricopa-live` reports session freshness, discovery candidate availability, complete vs incomplete row counts, and first sample complete/incomplete rows.

## Maricopa Workflow

Use this order before enabling sustained Maricopa runs:

```bash
npm run refresh:maricopa-session
npm run discover:maricopa-live
npm run validate:maricopa-live
npm run canary:maricopa
```

- `refresh:maricopa-session` is the operator step that persists a legitimately cleared browser session.
- `discover:maricopa-live` should be rerun whenever the site changes its preview/image/document request shape.
- `validate:maricopa-live` confirms both API reachability and whether the current artifact setup is producing complete OCR-backed records.
- `canary:maricopa` now fails fast if artifact retrieval is disabled or persisted session/discovery state is stale, then uploads both complete and review-bound rows through the standard sheet pipeline and syncs into shared `Master` / `Review_Queue`.
- When `DATABASE_URL` is set, Maricopa session state and discovered artifact candidates are persisted in the scheduler database so Cloud Run can reuse them across instances. Run `refresh:maricopa-session` and `discover:maricopa-live` against the production `DATABASE_URL` before turning on `MARICOPA_ENABLE_ARTIFACT_RETRIEVAL`.
- `GET /schedule/health` now includes a top-level `maricopa` section showing session presence/freshness, artifact candidate availability, `refresh_required`, and the latest Maricopa connectivity timestamps.
- When Maricopa artifact retrieval is enabled, stale/missing Maricopa state now blocks scheduled Maricopa runs and moves the site into the same blocked/probing recovery flow used for NYC ACRIS.

## Maricopa Droplet Refresh

Use the DigitalOcean droplet as the canonical operator refresh environment for Maricopa:

```bash
scripts/ops/refresh-maricopa-state.sh
```

- The script runs `refresh:maricopa-session`, `discover:maricopa-live`, and `validate:maricopa-live` in order.
- It exits nonzero if the results table never appears, if discovery captures no artifact candidates, or if validation still shows a stale session.
- Cloud Run daily runs do not need your laptop once the droplet refresh has written fresh Maricopa state into the shared scheduler database.

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

## CA SOS Live Proof

Use the CA SOS canary when you want the same proof path as NYC and Maricopa:

```bash
npm run canary:ca-sos
```

- Scrapes a capped recent window.
- Writes a `ca_sos_canary_*` source tab into `SHEET_ID`.
- Syncs accepted rows into shared `Master` and quarantined rows into shared `Review_Queue`.
- Prints a JSON summary with the source tab title plus merged-tab accepted/quarantine counts.

## NYC ACRIS Live Validation

Before enabling full NYC production cadence, use the dedicated Browser API validation flow:

```bash
npm run validate:nyc-acris-live
```

- Requires `BRIGHTDATA_BROWSER_WS`.
- Runs a single uninterrupted NYC session through results, viewer iframe extraction, and return-to-results checks.
- Writes a redacted validation manifest under `out/acris/`.

For explicit bootstrap transport comparisons without changing production defaults:

```bash
NYC_ACRIS_PROBE_TRANSPORT_MODE=brightdata-browser-api npm run probe:nyc-bootstrap
NYC_ACRIS_PROBE_TRANSPORT_MODE=legacy-sbr-cdp npm run probe:nyc-bootstrap
```

- `probe:nyc-bootstrap` runs in diagnostic mode and can force either transport explicitly.
- Scheduled NYC runs and `npm run canary:nyc-acris` follow `NYC_ACRIS_TRANSPORT_MODE` instead; production should keep that pinned to `legacy-sbr-cdp` for now.

For hosted Cloud Run diagnosis, use the authenticated bootstrap debug surface:

```bash
bash scripts/cloud/verify-cloud-run-service.sh
NYC_DEBUG_TRANSPORT_MODE=brightdata-browser-api bash scripts/cloud/debug-nyc-acris-bootstrap.sh
NYC_DEBUG_TRANSPORT_MODE=legacy-sbr-cdp bash scripts/cloud/debug-nyc-acris-bootstrap.sh
```

- `POST /debug/nyc-acris/bootstrap` reuses the scheduler token auth model and returns runtime git SHA plus a redacted NYC bootstrap artifact.
- The artifact includes requested/effective transport, bootstrap trace, bootstrap lifecycle events, navigation diagnostic, transport diagnostics, warnings, and failures.
- If hosted bootstrap succeeds on the desired transport, run `bash scripts/cloud/run-nyc-acris-prod-canary.sh` next.
- If hosted bootstrap fails with `about:blank before first navigation`, treat it as a bootstrap/environment incident rather than a selector incident.

For a capped end-to-end canary that also writes to Sheets:

```bash
npm run canary:nyc-acris
```

- Writes a `nyc_acris_canary_*` source tab.
- Syncs into shared `Master` / `Review_Queue`.
- Prints the same summary shape as the CA SOS and Maricopa canaries.

## All-Sites Live Proof

To see all three sites in one operator run:

```bash
npm run proof:all-sites-live
```

- Runs CA SOS canary.
- Runs NYC live validation, then NYC canary.
- Checks Maricopa persisted-state readiness, then runs Maricopa canary if ready.
- Prints one final JSON summary covering per-site status, source tab, accepted counts, quarantined counts, and any blocking reason.
- Exits nonzero if any site is blocked or fails.

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
- Optional Cloud Run alerting secrets: `LIEN_AUTOMATION_LEAD_ALERT_WEBHOOK_URL`, `SCHEDULE_ALERT_WEBHOOK_URL`, `LEAD_ALERT_EMAIL_FROM`, `LEAD_ALERT_RESEND_API_KEY`
- **Actions secrets (Droplet)**: `DO_HOST`, `DO_USERNAME`, `DO_SSH_KEY`, `DO_APP_DIRECTORY`
- **Actions secrets or variables (optional scheduler tuning)**: `SCHEDULE_CA_SOS_TIMEZONE`, `SCHEDULE_CA_SOS_WEEKLY_DAYS`, `SCHEDULE_CA_SOS_MORNING_RUN_HOUR`, `SCHEDULE_CA_SOS_MORNING_RUN_MINUTE`, `SCHEDULE_CA_SOS_AFTERNOON_RUN_HOUR`, `SCHEDULE_CA_SOS_AFTERNOON_RUN_MINUTE`, `SCHEDULE_CA_SOS_EVENING_RUN_HOUR`, `SCHEDULE_CA_SOS_EVENING_RUN_MINUTE`, `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES`, `SCHEDULE_MARICOPA_RECORDER_TIMEZONE`, `SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS`, `SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_HOUR`, `SCHEDULE_MARICOPA_RECORDER_MORNING_RUN_MINUTE`, `SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_HOUR`, `SCHEDULE_MARICOPA_RECORDER_AFTERNOON_RUN_MINUTE`, `SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_HOUR`, `SCHEDULE_MARICOPA_RECORDER_EVENING_RUN_MINUTE`, `SCHEDULE_MARICOPA_RECORDER_MAX_RECORDS`, `SCHEDULE_NYC_ACRIS_TIMEZONE`, `SCHEDULE_NYC_ACRIS_WEEKLY_DAYS`, `SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR`, `SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE`, `SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR`, `SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE`, `SCHEDULE_NYC_ACRIS_EVENING_RUN_HOUR`, `SCHEDULE_NYC_ACRIS_EVENING_RUN_MINUTE`, `SCHEDULE_NYC_ACRIS_MAX_RECORDS`, `AMOUNT_MIN_COVERAGE_PCT`, `SCHEDULE_AUTO_THROTTLE`, `SCHEDULE_MAX_RECORDS`, `SCHEDULE_MAX_RECORDS_FLOOR`, `SCHEDULE_MAX_RECORDS_CEILING`, `REQUIRE_OCR_TOOLS`

Cloud Run workflow expectation:

- GitHub Actions deploys should preserve the Cloud SQL instance attachment, inject `DATABASE_URL` from Secret Manager, and publish the workflow commit SHA to `/version` via `GIT_SHA`.
- The current runtime secret mapping is `DATABASE_URL`, `SCHEDULE_RUN_TOKEN`, `SHEETS_KEY`, and `SBR_CDP_URL` from Secret Manager-backed env vars; `SHEET_ID` and schedule tuning remain plain env vars.
- The Cloud Run runtime service account must have `roles/secretmanager.secretAccessor` on those runtime secrets.
- Avoid printing raw Cloud Run environment values during troubleshooting; hosted env output includes secrets such as scheduler tokens, Sheets credentials, and database credentials.

## Schedule Source of Truth

Production scheduling now uses **an external scheduler** targeting one authenticated endpoint: `POST /schedule/run`.

- **Execution target:** `POST /schedule/run` only
- **Authentication:** required via `Authorization: Bearer $SCHEDULE_RUN_TOKEN` (or `x-scheduler-token`)
- **Timezone:** `America/Denver` for all three sites in the default Cloud Run workflow configuration.
- **CA SOS semantics:** `SCHEDULE_CA_SOS_*_RUN_HOUR` / `SCHEDULE_CA_SOS_*_RUN_MINUTE` are finish-by times, and the external trigger should use `GET /schedule` `trigger_time` (finish-by minus `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES`)
- **Trigger times:** CA SOS defaults to `07:00`, `11:00`, and `19:00` in `America/Denver` for `10:00`, `14:00`, and `22:00` finish-by targets. Maricopa and NYC ACRIS default to `10:00`, `14:00`, and `22:00` in `America/Denver`. All three default to `MO,TU,WE,TH,FR` in the Cloud Run workflow.
- **Idempotency:** optional; set `ENABLE_SCHEDULE_IDEMPOTENCY=1` to key runs by `YYYY-MM-DD:slot` (`morning`/`afternoon`/`evening`) and skip duplicates. Default is off so each scheduled trigger creates a fresh run/tab

### External scheduler configuration (per-site triggers)

Linux cron example:

```cron
# CA SOS weekdays 07:00 America/Denver (default 3-hour lead for a 10:00 finish-by time)
0 7 * * 1-5 curl -fsS -X POST http://127.0.0.1:8080/schedule/run \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"site":"ca_sos","slot":"morning"}'

# Maricopa weekdays 14:00 America/Denver
0 14 * * 1-5 curl -fsS -X POST http://127.0.0.1:8080/schedule/run \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"site":"maricopa_recorder","slot":"afternoon"}'

# NYC ACRIS weekdays 22:00 America/Denver
0 22 * * 1-5 curl -fsS -X POST http://127.0.0.1:8080/schedule/run \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"site":"nyc_acris","slot":"evening"}'
```

Cloud Scheduler equivalent: create one job per site/slot with the schedule returned by `GET /schedule`, since CA SOS trigger times are derived from finish-by targets plus `SCHEDULE_CA_SOS_TRIGGER_LEAD_MINUTES`.

For operator evidence backfills outside CI, run `npm run proof:scheduled-evidence`. The script executes scheduled runs with deterministic proof idempotency keys, then prints the resulting persisted run evidence, `GET /schedule` next-run config, and per-site state summary.

Verification helpers:

```bash
bash scripts/cloud/verify-cloud-run-service.sh
bash scripts/cloud/create-scheduler-jobs.sh
bash scripts/cloud/verify-cloud-scheduler-jobs.sh
```

- `create-scheduler-jobs.sh` is the production reconciliation path for Cloud Scheduler and should be run after Cloud Run deploys or any schedule-setting change.
- `verify-cloud-scheduler-jobs.sh` now expects exactly 9 scheduler jobs in the target region: 3 weekday Mountain-time jobs per site.

### Runtime safeguards

1. Run records are stored in the scheduler store table `scheduled_runs` (SQLite locally, Postgres when `DATABASE_URL` is set), including `slot_time`, `started_at`, `finished_at`, `status`, `records_scraped`, `rows_uploaded`, and `error`.
2. Duplicate triggers for the same `idempotency_key` are ignored unless the prior run ended in `error`; cooldown checks also read persisted DB state.
3. Missed-run monitoring checks for successful morning/afternoon/evening runs and creates alert records in `scheduler_alerts`.
4. Optional outbound alert webhook can be enabled with `SCHEDULE_ALERT_WEBHOOK_URL`. It is used for missed runs, connectivity alerts, and successful-run quality anomalies.
5. `GET /schedule/health` returns schedule readiness checks (required env vars, scheduler-store reachability, and Google Sheets credential parsing).
6. Successful scheduled runs write raw `Scheduled_*` tabs into the source workbook (`SHEET_ID`) and then publish a filtered merged `Master` dataset to the destination workbook (`MERGED_SHEET_ID` or the built-in default target). If the destination workbook is not reachable yet, the merged `Master` publish falls back to the source workbook so scheduled runs remain operational.
7. The director-facing `Master` sheet publishes rows that pass structural validation and duplicate resolution. `partial_run` is now treated as a soft review signal instead of an automatic quarantine, and confidence thresholds are configurable through `DIRECTOR_MIN_CONFIDENCE_ACCEPT` / `DIRECTOR_MIN_CONFIDENCE_REVIEW`. Questionable rows are written to `Review_Queue` instead of being mixed into `Master`.
8. Scheduled runs now keep one logical run record across retryable failures, recording `attempt_count`, `max_attempts`, `retried`, and `retry_exhausted` while using bounded backoff for transient scrape and sheet-export failures.
9. Successful scheduled runs also compare their row-count and quality metrics against recent successful baselines and emit advisory anomaly alerts without changing the final run status.

## Monitoring Setup Notes

- `scripts/cloud/setup-monitoring.sh` now treats the log-based metrics, alert policies, notification channel reuse, and uptime check as the default monitoring path.
- BigQuery sink creation is optional and disabled by default via `ENABLE_BIGQUERY_SINK=0` because the local `bq` tool can fail independently of the app.
- If `bq` currently crashes with `AttributeError: module 'absl.flags' has no attribute 'FLAGS'`, treat that as a local Cloud SDK tooling issue rather than a production app/runtime issue.

## Secret Manager Notes

- Local development still uses plain env vars directly; Secret Manager is only part of the hosted Cloud Run deploy path.
- `scripts/cloud/deploy-cloud-run-service.sh` now expects Secret Manager refs for `DATABASE_URL`, `SCHEDULE_RUN_TOKEN`, `SHEETS_KEY`, and `SBR_CDP_URL`, and injects them with `--set-secrets`.
- For GitHub Actions `*_SECRET_REF` values, prefer storing the bare Secret Manager secret name such as `scheduler-run-token`. Canonical supported formats are:
  - bare secret name: `scheduler-run-token`
  - secret resource: `projects/<project>/secrets/<name>`
  - version resource: `projects/<project>/secrets/<name>/versions/<version>`
- Avoid storing encoded or wrapped variants (for example escaped slashes, percent-encoded paths, or JSON blobs) in GitHub secrets. The deploy workflow now validates `SCHEDULE_RUN_TOKEN_SECRET_REF` before scheduler reconciliation and fails fast on malformed resource-like values.
- Recommended default: keep `SCHEDULE_RUN_TOKEN_SECRET_REF` as the bare secret name in GitHub Actions unless the secret truly lives in a different GCP project.
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

