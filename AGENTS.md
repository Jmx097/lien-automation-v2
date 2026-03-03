# Agents

## Cursor Cloud specific instructions

### Overview

Lien Automation v2 is a Node.js/TypeScript Express API that scrapes CA Secretary of State UCC/lien filings via Playwright (Bright Data Scraping Browser) and pushes results to Google Sheets. See `README.md` for full API docs and data schema.

### Prerequisites

- Node.js 22 (see `.nvmrc`); minimum 20.16.0.
- `npm install` for dependencies.
- SQLite DB init: `node src/queue/init-db.js` (idempotent, safe to re-run). Parent `data/db` directory is auto-created by `SQLiteQueueStore` as needed.

### Running the dev server

The server requires `SBR_CDP_URL` env var or it will `process.exit(1)`. For local dev/testing without Bright Data, export a dummy value:

```bash
export SBR_CDP_URL="wss://example.invalid"
npm run dev
```

The server listens on port 8080. Key endpoints: `GET /health`, `GET /version`, `POST /scrape`, `POST /enqueue`, `POST /scrape-all`, `POST /scrape-enhanced`.

### Test commands

All documented in `package.json` scripts:

| Command | What it does |
|---|---|
| `npm test` | Runs `test:types` + `test:selector-smoke` |
| `npm run test:types` | TypeScript type check (`tsc --noEmit`) |
| `npm run test:selector-smoke` | Fixture-based selector test (no external deps) |
| `npm run test:smoke` | Starts dev server, hits `/health`, verifies response |
| `npm run doctor` | Preflight: checks Node version, scripts, toolchain |
| `npm run build` | Full TypeScript compile to `dist/` |

### Gotchas

- `npm run test:smoke` sets `SBR_CDP_URL` to a dummy value internally, so it works without external credentials.
- Actual scraping (`/scrape`, `/enqueue`) requires real `SBR_CDP_URL`, `SHEETS_KEY`, and `SHEET_ID` env vars.
- `SHEETS_KEY` must be a raw JSON string (the service account key), **not** wrapped in extra quotes. The code strips leading/trailing single quotes, but it's best to set it correctly.
- The SQLite DB path stays `data/db/lien-queue.db` for compatibility. `SQLiteQueueStore` now auto-creates the parent directory, but `init-db.js` is still required to create schema tables (`queue_jobs`, `scheduled_runs`, `scheduler_alerts`).
- `npm run build` outputs to `dist/`. The dev server uses `ts-node` directly (no build step needed for dev).
- Scrape requests take ~5-12 minutes for 24 records. Each record involves: open drawer → extract details → open history modal → attempt PDF download → close modal → close drawer.
- CA SOS website uses a **drawer panel** (not page navigation) for record details. Selectors: `.interactive-cell-button` to open, `button.close-button` to close, `div.drawer.show` to detect.
- The history view opens a fullscreen `div.history-modal[role="dialog"]`. Must be dismissed (force-click close button or DOM removal) before the drawer can be closed.
- CA SOS PDFs are **image-based** (scanned documents). `pdf-parse` can only extract filing metadata (File No, Date), not the lien content (Amount, Taxpayer Name). OCR would be needed for full extraction.
- Bright Data browser sessions can be unstable. The scraper stops after 3 consecutive row failures to avoid wasting session time.
