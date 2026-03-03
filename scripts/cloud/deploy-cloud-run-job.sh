#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${JOB_NAME:=lien-scraper-job}"
: "${IMAGE_URI:?Set IMAGE_URI, e.g. us-central1-docker.pkg.dev/<project>/<repo>/lien-automation:v1}"

# Required runtime env vars for the scraper job.
: "${SBR_CDP_URL:?Set SBR_CDP_URL}"
: "${SHEETS_KEY:?Set SHEETS_KEY}"
: "${SHEET_ID:?Set SHEET_ID}"

# Optional tuning knobs.
: "${JOB_LOOKBACK_DAYS:=7}"
: "${JOB_MAX_RECORDS:=25}"
: "${TASK_TIMEOUT:=3600s}"
: "${MAX_RETRIES:=2}"

ENV_VARS="SBR_CDP_URL=${SBR_CDP_URL},SHEETS_KEY=${SHEETS_KEY},SHEET_ID=${SHEET_ID},JOB_LOOKBACK_DAYS=${JOB_LOOKBACK_DAYS},JOB_MAX_RECORDS=${JOB_MAX_RECORDS},JOB_SITE=ca_sos"

gcloud run jobs deploy "${JOB_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --image="${IMAGE_URI}" \
  --command="npm" \
  --args="run,start:job" \
  --tasks=1 \
  --parallelism=1 \
  --task-timeout="${TASK_TIMEOUT}" \
  --max-retries="${MAX_RETRIES}" \
  --set-env-vars="${ENV_VARS}"

echo "Deployed Cloud Run Job: ${JOB_NAME}"
