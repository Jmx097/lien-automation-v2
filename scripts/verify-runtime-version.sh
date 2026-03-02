#!/usr/bin/env bash
set -euo pipefail

VERSION_URL="${VERSION_URL:-http://127.0.0.1:8080/version}"

if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: node is required to parse $VERSION_URL response" >&2
  exit 2
fi

LOCAL_SHA="$(git rev-parse --short HEAD)"
RUNTIME_SHA="$(curl -fsS "$VERSION_URL" | node -e 'process.stdin.once("data", d => console.log(JSON.parse(d).git_sha ?? "unknown"))')"

echo "local_sha=$LOCAL_SHA runtime_sha=$RUNTIME_SHA url=$VERSION_URL"

if [[ "$RUNTIME_SHA" != "$LOCAL_SHA" ]]; then
  echo "ERROR: runtime git_sha mismatch. Rebuild with: docker compose down && docker compose build --no-cache && docker compose up -d" >&2
  exit 1
fi

echo "OK: runtime git_sha matches local commit"
