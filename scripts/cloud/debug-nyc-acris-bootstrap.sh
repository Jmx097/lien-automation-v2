#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${SERVICE_NAME:=lien-automation}"
: "${SCHEDULE_RUN_TOKEN:?Set SCHEDULE_RUN_TOKEN}"
: "${NYC_DEBUG_TRANSPORT_MODE:=}"

SERVICE_URL="${SERVICE_URL:-$(gcloud run services describe "${SERVICE_NAME}" --project="${GCP_PROJECT_ID}" --region="${GCP_REGION}" --format='value(status.url)')}"

payload="$(node -e "const payload = {}; if (process.env.NYC_DEBUG_TRANSPORT_MODE) payload.transport_mode_override = process.env.NYC_DEBUG_TRANSPORT_MODE; console.log(JSON.stringify(payload));")"

curl -fsS -X POST "${SERVICE_URL}/debug/nyc-acris/bootstrap" \
  -H "Authorization: Bearer ${SCHEDULE_RUN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "${payload}"
