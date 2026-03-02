# Testing Playbook

## Fast path (exact commands)

Run this from the repo root.

```bash
cd "$(git rev-parse --show-toplevel)"

git fetch --all --prune
git pull --rebase

# Verify you are in the expected repo + branch and scripts exist
pwd
git remote -v
git branch --show-current
npm run
```

You should see: `doctor`, `test:types`, `test:smoke`.

If those scripts are missing, you are likely in a stale clone or wrong folder.

---


## When `npm run` shows old scripts (most common failure)

If `npm run` shows this:

- `test -> echo "Error: no test specified" && exit 1`
- and **does not** list `doctor`, `test:types`, `test:smoke`

then you are on an older checkout/branch. Run exactly:

```bash
cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
pwd
git remote -v
git branch --show-current
git fetch --all --prune
git checkout work || true
git pull --rebase
npm run
```

You should then see `doctor`, `test:types`, and `test:smoke`.

---

## If `nvm` is NOT installed (your current case)

Install Node 22 from NodeSource and switch default `node`/`npm` binaries:

```bash
cd "$(git rev-parse --show-toplevel)"

sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt-get install -y nodejs

node -v
npm -v
```

If Node is still 18 after install, force shell hash refresh:

```bash
hash -r
which node
node -v
```

---

## Full local validation (exact commands)

```bash
cd "$(git rev-parse --show-toplevel)"

# clean partial install from interrupted npm ci
rm -rf node_modules package-lock.json
npm install

mkdir -p data/db
node src/queue/init-db.js

npm run doctor
npm run test:types
npm run test:smoke
npm run build
```

---


## Track logs while the run is active

Use separate terminals so you can watch server and worker output in real time and persist logs to files.

Terminal A (server):

```bash
cd "$(git rev-parse --show-toplevel)"
mkdir -p logs
npm run dev 2>&1 | tee logs/server-$(date +%F-%H%M%S).log
```

Terminal C (worker):

```bash
cd "$(git rev-parse --show-toplevel)"
mkdir -p logs
npm run worker 2>&1 | tee logs/worker-$(date +%F-%H%M%S).log
```

Optional live follow on latest log files:

```bash
tail -f logs/server-*.log
```

---

## Example test window: one week in 2026

Use this payload for a 7-day span:

```bash
curl -sS -X POST http://127.0.0.1:8080/scrape   -H 'Content-Type: application/json'   -d '{
    "site":"ca_sos",
    "date_start":"01/05/2026",
    "date_end":"01/11/2026",
    "max_records":25
  }'
```

Queue variant:

```bash
curl -sS -X POST http://127.0.0.1:8080/enqueue   -H 'Content-Type: application/json'   -d '{
    "site":"ca_sos",
    "date_start":"01/05/2026",
    "date_end":"01/11/2026",
    "max_records":25
  }'
```

---

## End-to-end API checks (exact commands)

Set runtime variables (replace placeholders):

```bash
export SBR_CDP_URL='wss://<bright-data-cdp-url>'
export SHEET_ID='<google-sheet-id>'
export SHEETS_KEY='{"type":"service_account",...}'
```

Start server in terminal A:

```bash
cd "$(git rev-parse --show-toplevel)"
mkdir -p logs
npm run dev
```

Run requests in terminal B:

```bash
curl -sS http://127.0.0.1:8080/health

curl -sS -X POST http://127.0.0.1:8080/scrape \
  -H 'Content-Type: application/json' \
  -d '{
    "site":"ca_sos",
    "date_start":"01/01/2024",
    "date_end":"01/03/2024",
    "max_records":5
  }'

curl -sS -X POST http://127.0.0.1:8080/enqueue \
  -H 'Content-Type: application/json' \
  -d '{
    "site":"ca_sos",
    "date_start":"01/01/2024",
    "date_end":"01/03/2024",
    "max_records":5
  }'
```

Run worker in terminal C:

```bash
cd "$(git rev-parse --show-toplevel)"
mkdir -p logs
npm run worker
```

---

## Mapping to your reported errors

- `Command 'nvm' not found`  
  Use the NodeSource Node 22 install block above.

- `npm WARN EBADENGINE ... current: node v18`  
  Node is still too old; confirm `node -v` is 22.x before `npm install`.

- `Cannot find module 'better-sqlite3'`  
  Install was interrupted/failed. Re-run clean install commands.

- `Missing script: doctor/test:types/test:smoke`  
  You are not on the latest branch contents. Run:

```bash
git fetch --all --prune
git pull --rebase
npm run
```

- `sh: 1: tsc: not found`  
  `node_modules` not installed; run `npm install` again after Node 22 is active.
