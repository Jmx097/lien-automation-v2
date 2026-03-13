#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${JOB_NAME:=lien-scraper-schedule-run}"
: "${TIME_ZONE:=America/New_York}"
: "${API_BASE_URL:?Set API_BASE_URL, e.g. https://your-service-url}"
: "${SCHEDULE_RUN_TOKEN:?Set SCHEDULE_RUN_TOKEN}"
: "${CA_MORNING_JOB_NAME:=${JOB_NAME}-ca-sos-morning}"
: "${CA_AFTERNOON_JOB_NAME:=${JOB_NAME}-ca-sos-afternoon}"
: "${NYC_MORNING_JOB_NAME:=${JOB_NAME}-nyc-acris-morning}"
: "${NYC_AFTERNOON_JOB_NAME:=${JOB_NAME}-nyc-acris-afternoon}"

# Retry policy requirements.
: "${RETRY_COUNT:=3}"
: "${MAX_RETRY_DURATION:=1800s}"
: "${MIN_BACKOFF:=30s}"
: "${MAX_BACKOFF:=300s}"

RUN_URI="${API_BASE_URL%/}/schedule/run"

create_or_update_job () {
  local name="$1"
  local schedule="$2"
  local body="$3"

  if gcloud scheduler jobs describe "${name}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "${name}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --schedule="${schedule}" \
      --time-zone="${TIME_ZONE}" \
      --uri="${RUN_URI}" \
      --http-method=POST \
      --max-retry-attempts="${RETRY_COUNT}" \
      --max-retry-duration="${MAX_RETRY_DURATION}" \
      --min-backoff="${MIN_BACKOFF}" \
      --max-backoff="${MAX_BACKOFF}" \
      --update-headers="Content-Type=application/json,x-scheduler-token=${SCHEDULE_RUN_TOKEN}" \
      --message-body="${body}"
  else
    gcloud scheduler jobs create http "${name}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --schedule="${schedule}" \
      --time-zone="${TIME_ZONE}" \
      --uri="${RUN_URI}" \
      --http-method=POST \
      --max-retry-attempts="${RETRY_COUNT}" \
      --max-retry-duration="${MAX_RETRY_DURATION}" \
      --min-backoff="${MIN_BACKOFF}" \
      --max-backoff="${MAX_BACKOFF}" \
      --headers="Content-Type=application/json,x-scheduler-token=${SCHEDULE_RUN_TOKEN}" \
      --message-body="${body}"
  fi
}

# Daily morning + afternoon runs for each site.
create_or_update_job "${CA_MORNING_JOB_NAME}" "0 6 * * *" '{"site":"ca_sos","slot":"morning"}'
create_or_update_job "${CA_AFTERNOON_JOB_NAME}" "0 12 * * *" '{"site":"ca_sos","slot":"afternoon"}'
create_or_update_job "${NYC_MORNING_JOB_NAME}" "0 10 * * *" '{"site":"nyc_acris","slot":"morning"}'
create_or_update_job "${NYC_AFTERNOON_JOB_NAME}" "0 14 * * *" '{"site":"nyc_acris","slot":"afternoon"}'

echo "Scheduler jobs upserted for ${RUN_URI} (CA 06:00+12:00, NYC 10:00+14:00 daily ${TIME_ZONE})."


