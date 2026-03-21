#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${JOB_NAME:=lien-scraper-schedule-run}"
: "${API_BASE_URL:?Set API_BASE_URL, e.g. https://your-service-url}"
: "${SCHEDULE_RUN_TOKEN:?Set SCHEDULE_RUN_TOKEN}"

# Retry policy requirements.
: "${RETRY_COUNT:=3}"
: "${MAX_RETRY_DURATION:=1800s}"
: "${MIN_BACKOFF:=30s}"
: "${MAX_BACKOFF:=300s}"

RUN_URI="${API_BASE_URL%/}/schedule/run"
JOB_SPECS_JSON="$(node scripts/cloud/scheduler-job-specs.js)"

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

while IFS=$'\t' read -r name schedule time_zone body; do
  create_or_update_job "${name}" "${schedule}" "${body}" "${time_zone}"
done < <(
  printf '%s' "${JOB_SPECS_JSON}" | node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(0,'utf8')); for (const spec of payload.specs ?? []) { process.stdout.write([spec.jobName, spec.schedule, spec.timeZone, JSON.stringify(spec.body)].join('\t') + '\n'); }"
)

echo "Scheduler jobs upserted for ${RUN_URI} using derived job specs from scheduler environment."


