#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${JOB_NAME:=lien-scraper-schedule-run}"
: "${API_BASE_URL:?Set API_BASE_URL, e.g. https://your-service-url}"
: "${SCHEDULE_RUN_TOKEN:?Set SCHEDULE_RUN_TOKEN}"
: "${SCHEDULE_CA_SOS_TIMEZONE:=America/Denver}"
: "${SCHEDULE_MARICOPA_RECORDER_TIMEZONE:=America/Denver}"
: "${SCHEDULE_NYC_ACRIS_TIMEZONE:=America/Denver}"
: "${CA_MORNING_JOB_NAME:=${JOB_NAME}-ca-sos-morning}"
: "${CA_AFTERNOON_JOB_NAME:=${JOB_NAME}-ca-sos-afternoon}"
: "${CA_EVENING_JOB_NAME:=${JOB_NAME}-ca-sos-evening}"
: "${MARICOPA_MORNING_JOB_NAME:=${JOB_NAME}-maricopa-recorder-morning}"
: "${MARICOPA_AFTERNOON_JOB_NAME:=${JOB_NAME}-maricopa-recorder-afternoon}"
: "${MARICOPA_EVENING_JOB_NAME:=${JOB_NAME}-maricopa-recorder-evening}"
: "${NYC_MORNING_JOB_NAME:=${JOB_NAME}-nyc-acris-morning}"
: "${NYC_AFTERNOON_JOB_NAME:=${JOB_NAME}-nyc-acris-afternoon}"
: "${NYC_EVENING_JOB_NAME:=${JOB_NAME}-nyc-acris-evening}"

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
  local time_zone="$4"

  if gcloud scheduler jobs describe "${name}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "${name}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --schedule="${schedule}" \
      --time-zone="${time_zone}" \
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
      --time-zone="${time_zone}" \
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

# Weekday runs for each site. CA keeps a 3-hour lead so it finishes by 10:00 / 14:00 / 22:00 Mountain time.
create_or_update_job "${CA_MORNING_JOB_NAME}" "0 7 * * 1-5" '{"site":"ca_sos","slot":"morning"}' "${SCHEDULE_CA_SOS_TIMEZONE}"
create_or_update_job "${CA_AFTERNOON_JOB_NAME}" "0 11 * * 1-5" '{"site":"ca_sos","slot":"afternoon"}' "${SCHEDULE_CA_SOS_TIMEZONE}"
create_or_update_job "${CA_EVENING_JOB_NAME}" "0 19 * * 1-5" '{"site":"ca_sos","slot":"evening"}' "${SCHEDULE_CA_SOS_TIMEZONE}"
create_or_update_job "${MARICOPA_MORNING_JOB_NAME}" "0 10 * * 1-5" '{"site":"maricopa_recorder","slot":"morning"}' "${SCHEDULE_MARICOPA_RECORDER_TIMEZONE}"
create_or_update_job "${MARICOPA_AFTERNOON_JOB_NAME}" "0 14 * * 1-5" '{"site":"maricopa_recorder","slot":"afternoon"}' "${SCHEDULE_MARICOPA_RECORDER_TIMEZONE}"
create_or_update_job "${MARICOPA_EVENING_JOB_NAME}" "0 22 * * 1-5" '{"site":"maricopa_recorder","slot":"evening"}' "${SCHEDULE_MARICOPA_RECORDER_TIMEZONE}"
create_or_update_job "${NYC_MORNING_JOB_NAME}" "0 10 * * 1-5" '{"site":"nyc_acris","slot":"morning"}' "${SCHEDULE_NYC_ACRIS_TIMEZONE}"
create_or_update_job "${NYC_AFTERNOON_JOB_NAME}" "0 14 * * 1-5" '{"site":"nyc_acris","slot":"afternoon"}' "${SCHEDULE_NYC_ACRIS_TIMEZONE}"
create_or_update_job "${NYC_EVENING_JOB_NAME}" "0 22 * * 1-5" '{"site":"nyc_acris","slot":"evening"}' "${SCHEDULE_NYC_ACRIS_TIMEZONE}"

echo "Scheduler jobs upserted for ${RUN_URI} (weekday Mountain schedule, finish-by 10:00/14:00/22:00; CA trigger lead 3 hours)."


