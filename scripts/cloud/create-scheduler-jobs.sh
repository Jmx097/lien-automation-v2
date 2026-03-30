#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${JOB_NAME:=lien-scraper-schedule-run}"
: "${API_BASE_URL:?Set API_BASE_URL, e.g. https://your-service-url}"
: "${SCHEDULE_RUN_TOKEN:?Set SCHEDULE_RUN_TOKEN}"
: "${PRUNE_STALE_MANAGED_JOBS:=1}"

# Retry policy requirements.
: "${RETRY_COUNT:=3}"
: "${MAX_RETRY_DURATION:=1800s}"
: "${MIN_BACKOFF:=30s}"
: "${MAX_BACKOFF:=300s}"

JOB_SPECS_JSON="$(node scripts/cloud/scheduler-job-specs.js)"

prune_stale_jobs () {
  local stale_jobs
  stale_jobs="$(
    gcloud scheduler jobs list --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" --format=json \
      | node -e "const fs=require('fs'); const jobs=JSON.parse(fs.readFileSync(0,'utf8')); const specs=JSON.parse(process.argv[1]); const expected=new Set((specs.specs ?? []).map((spec) => spec.jobName)); const prefix=String(specs.jobPrefix ?? '') + '-'; const names=(Array.isArray(jobs) ? jobs : []).map((job) => String(job.name ?? '').split('/').pop() ?? '').filter(Boolean); const stale=names.filter((name) => name.startsWith(prefix) && !expected.has(name)); process.stdout.write(stale.join('\n'));" "${JOB_SPECS_JSON}"
  )"

  if [[ -z "${stale_jobs}" ]]; then
    return
  fi

  echo "Pruning stale scheduler jobs for prefix ${JOB_NAME}:"
  printf '%s\n' "${stale_jobs}"
  while IFS= read -r job_name; do
    [[ -n "${job_name}" ]] || continue
    gcloud scheduler jobs delete "${job_name}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --quiet
  done <<< "${stale_jobs}"
}

create_or_update_job () {
  local name="$1"
  local schedule="$2"
  local time_zone="$3"
  local uri="$4"
  local body="$5"

  if gcloud scheduler jobs describe "${name}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    gcloud scheduler jobs update http "${name}" \
      --location="${GCP_REGION}" \
      --project="${GCP_PROJECT_ID}" \
      --schedule="${schedule}" \
      --time-zone="${time_zone}" \
      --uri="${uri}" \
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
      --uri="${uri}" \
      --http-method=POST \
      --max-retry-attempts="${RETRY_COUNT}" \
      --max-retry-duration="${MAX_RETRY_DURATION}" \
      --min-backoff="${MIN_BACKOFF}" \
      --max-backoff="${MAX_BACKOFF}" \
      --headers="Content-Type=application/json,x-scheduler-token=${SCHEDULE_RUN_TOKEN}" \
      --message-body="${body}"
  fi
}

while IFS=$'\t' read -r name schedule time_zone uri body; do
  create_or_update_job "${name}" "${schedule}" "${time_zone}" "${uri}" "${body}"
done < <(
  printf '%s' "${JOB_SPECS_JSON}" | API_BASE_URL="${API_BASE_URL}" node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(0,'utf8')); const base=(process.env.API_BASE_URL ?? '').replace(/\/$/, ''); for (const spec of payload.specs ?? []) { process.stdout.write([spec.jobName, spec.schedule, spec.timeZone, \`\${base}\${spec.path ?? '/schedule/run'}\`, JSON.stringify(spec.body ?? {})].join('\t') + '\n'); }"
)

if [[ "${PRUNE_STALE_MANAGED_JOBS}" == "1" ]]; then
  prune_stale_jobs
else
  echo "Skipping stale scheduler job pruning (PRUNE_STALE_MANAGED_JOBS=${PRUNE_STALE_MANAGED_JOBS})."
fi

echo "Scheduler jobs upserted using derived job specs from scheduler environment."

