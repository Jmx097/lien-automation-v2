#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${SERVICE_NAME:=lien-automation}"
: "${SCHEDULE_RUN_TOKEN:?Set SCHEDULE_RUN_TOKEN}"

: "${CANARY_SITE:=nyc_acris}"
: "${CANARY_SLOT:=afternoon}"
: "${CANARY_IDEMPOTENCY_KEY:=nyc_acris:2026-03-11:afternoon:prod-canary}"

SERVICE_URL="${SERVICE_URL:-$(gcloud run services describe "${SERVICE_NAME}" --project="${GCP_PROJECT_ID}" --region="${GCP_REGION}" --format='value(status.url)')}"

payload="$(node -e "console.log(JSON.stringify({site: process.env.CANARY_SITE, slot: process.env.CANARY_SLOT, idempotency_key: process.env.CANARY_IDEMPOTENCY_KEY}))")"

curl -fsS -X POST "${SERVICE_URL}/schedule/run" \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "${payload}"
