#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${JOB_NAME:=lien-scraper-job}"
: "${TIME_ZONE:=America/New_York}"
: "${SCHEDULER_SA_EMAIL:?Set SCHEDULER_SA_EMAIL (service account for Cloud Scheduler OIDC/OAuth auth)}"

# Retry policy requirements.
: "${RETRY_COUNT:=3}"
: "${MAX_RETRY_DURATION:=1800s}"
: "${MIN_BACKOFF:=30s}"
: "${MAX_BACKOFF:=300s}"

RUN_URI="https://${GCP_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${GCP_PROJECT_ID}/jobs/${JOB_NAME}:run"

create_or_update_job () {
  local name="$1"
  local schedule="$2"

  if gcloud scheduler jobs describe "${name}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "${name}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --schedule="${schedule}" \
      --time-zone="${TIME_ZONE}" \
      --uri="${RUN_URI}" \
      --http-method=POST \
      --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
      --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
      --max-retry-attempts="${RETRY_COUNT}" \
      --max-retry-duration="${MAX_RETRY_DURATION}" \
      --min-backoff="${MIN_BACKOFF}" \
      --max-backoff="${MAX_BACKOFF}" \
      --headers="Content-Type=application/json" \
      --message-body='{}'
  else
    gcloud scheduler jobs create http "${name}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --schedule="${schedule}" \
      --time-zone="${TIME_ZONE}" \
      --uri="${RUN_URI}" \
      --http-method=POST \
      --oauth-service-account-email="${SCHEDULER_SA_EMAIL}" \
      --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
      --max-retry-attempts="${RETRY_COUNT}" \
      --max-retry-duration="${MAX_RETRY_DURATION}" \
      --min-backoff="${MIN_BACKOFF}" \
      --max-backoff="${MAX_BACKOFF}" \
      --headers="Content-Type=application/json" \
      --message-body='{}'
  fi
}

create_or_update_job "${JOB_NAME}-morning" "30 7 * * *"
create_or_update_job "${JOB_NAME}-afternoon" "30 19 * * *"

echo "Scheduler jobs upserted for ${JOB_NAME} in ${TIME_ZONE}."
