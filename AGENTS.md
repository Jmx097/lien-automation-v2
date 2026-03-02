# Agents

## Cursor Cloud specific instructions

### Overview

Lien Automation v2 is a Node.js/TypeScript Express API that scrapes CA Secretary of State UCC/lien filings via Playwright (Bright Data Scraping Browser) and pushes results to Google Sheets. See `README.md` for full API docs and data schema.

### Prerequisites

- Node.js 22 (see `.nvmrc`); minimum 20.16.0.
- `npm install` for dependencies.
- SQLite DB init: `mkdir -p data/db && node src/queue/init-db.js` (idempotent, safe to re-run).

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
- `SHEETS_KEY` must be a raw JSON string (the service account key), **not** wrapped in extra quotes. If the env var value starts with `'` or `"` around the JSON object, `pushToSheets` will throw a JSON parse error.
- The SQLite DB at `data/db/lien-queue.db` is auto-created by `init-db.js`; server startup instantiates `SQLiteQueueStore` which expects the file to exist.
- `npm run build` outputs to `dist/`. The dev server uses `ts-node` directly (no build step needed for dev).
- Scrape requests can take several minutes due to Bright Data browser session setup and per-row detail scraping (~30s timeout per row).
