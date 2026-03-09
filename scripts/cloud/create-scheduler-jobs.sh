#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${JOB_NAME:=lien-scraper-schedule-run}"
: "${TIME_ZONE:=America/New_York}"
: "${API_BASE_URL:?Set API_BASE_URL, e.g. https://your-service-url}"
: "${SCHEDULE_RUN_TOKEN:?Set SCHEDULE_RUN_TOKEN}"
: "${CA_JOB_NAME:=${JOB_NAME}-ca-sos}"
: "${NYC_JOB_NAME:=${JOB_NAME}-nyc-acris}"

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
      --update-headers="Content-Type=application/json,Authorization=Bearer ${SCHEDULE_RUN_TOKEN}" \
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
      --headers="Content-Type=application/json,Authorization=Bearer ${SCHEDULE_RUN_TOKEN}" \
      --message-body="${body}"
  fi
}

# CA Tue/Wed at 09:00 local timezone.
create_or_update_job "${CA_JOB_NAME}" "0 9 * * 2,3" '{"site":"ca_sos","slot":"morning"}'

# NYC Tue/Wed/Thu/Fri at 14:00 local timezone.
create_or_update_job "${NYC_JOB_NAME}" "0 14 * * 2,3,4,5" '{"site":"nyc_acris","slot":"afternoon"}'

echo "Scheduler jobs upserted for ${RUN_URI} (CA Tue/Wed 09:00, NYC Tue-Fri 14:00 ${TIME_ZONE})."


