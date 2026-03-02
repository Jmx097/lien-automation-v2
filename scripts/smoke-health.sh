#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8080}"
LOG_FILE="${LOG_FILE:-/tmp/lien-automation-health.log}"

export SBR_CDP_URL="${SBR_CDP_URL:-wss://example.invalid}"

mkdir -p data/db
node src/queue/init-db.js >/dev/null

npm run dev >"$LOG_FILE" 2>&1 &
SERVER_PID=$!

cleanup() {
  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in {1..30}; do
  if curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

RESPONSE="$(curl -fsS "http://127.0.0.1:${PORT}/health")"
echo "$RESPONSE"

if [[ "$RESPONSE" != *'"status":"healthy"'* ]]; then
  echo "Health check response did not include expected status" >&2
  exit 1
fi
