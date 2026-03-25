#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${API_BASE_URL:?Set API_BASE_URL}"
: "${JOB_NAME:=lien-scraper-schedule-run}"

RUN_URI="${API_BASE_URL%/}/schedule/run"
JOB_SPECS_JSON="$(node scripts/cloud/scheduler-job-specs.js)"

listed_jobs_json="$(gcloud scheduler jobs list --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" --format=json)"
managed_job_summary="$(
  printf '%s' "${listed_jobs_json}" | node -e "const fs=require('fs'); const jobs=JSON.parse(fs.readFileSync(0,'utf8')); const specs=JSON.parse(process.argv[1]); const expectedNames=new Set((specs.specs ?? []).map((spec) => spec.jobName)); const prefix=String(specs.jobPrefix ?? '') + '-'; const names=(Array.isArray(jobs) ? jobs : []).map((job) => String(job.name ?? '').split('/').pop() ?? '').filter(Boolean); const missing=[...expectedNames].filter((name) => !names.includes(name)); const unexpected=names.filter((name) => name.startsWith(prefix) && !expectedNames.has(name)); process.stdout.write(JSON.stringify({ expectedCount: expectedNames.size, managedCount: names.filter((name) => name.startsWith(prefix)).length, missing, unexpected }));" "${JOB_SPECS_JSON}"
)"

missing_jobs="$(printf '%s' "${managed_job_summary}" | node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(0,'utf8')); process.stdout.write((payload.missing ?? []).join('\n'))")"
unexpected_jobs="$(printf '%s' "${managed_job_summary}" | node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(0,'utf8')); process.stdout.write((payload.unexpected ?? []).join('\n'))")"

if [[ -n "${missing_jobs}" ]]; then
  echo "Missing expected scheduler jobs:" >&2
  printf '%s\n' "${missing_jobs}" >&2
  exit 1
fi

if [[ -n "${unexpected_jobs}" ]]; then
  echo "Found unexpected managed scheduler jobs for prefix ${JOB_NAME}:" >&2
  printf '%s\n' "${unexpected_jobs}" >&2
  exit 1
fi

verify_job() {
  local name="$1"
  local expected_schedule="$2"
  local expected_body="$3"
  local expected_timezone="$4"

  local description
  description="$(gcloud scheduler jobs describe "${name}" --location="${GCP_REGION}" --project="${GCP_PROJECT_ID}" --format=json)"

  local actual_schedule actual_timezone actual_uri actual_method actual_body
  actual_schedule="$(printf '%s' "${description}" | node -e "const fs=require('fs');const p=JSON.parse(fs.readFileSync(0,'utf8'));process.stdout.write(String(p.schedule ?? ''))")"
  actual_timezone="$(printf '%s' "${description}" | node -e "const fs=require('fs');const p=JSON.parse(fs.readFileSync(0,'utf8'));process.stdout.write(String(p.timeZone ?? ''))")"
  actual_uri="$(printf '%s' "${description}" | node -e "const fs=require('fs');const p=JSON.parse(fs.readFileSync(0,'utf8'));process.stdout.write(String(p.httpTarget?.uri ?? ''))")"
  actual_method="$(printf '%s' "${description}" | node -e "const fs=require('fs');const p=JSON.parse(fs.readFileSync(0,'utf8'));process.stdout.write(String(p.httpTarget?.httpMethod ?? ''))")"
  actual_body="$(printf '%s' "${description}" | node -e "const fs=require('fs');const p=JSON.parse(fs.readFileSync(0,'utf8'));const body=p.httpTarget?.body ? Buffer.from(String(p.httpTarget.body), 'base64').toString('utf8') : '';process.stdout.write(body)")"
  local bodies_match
  bodies_match="$(node -e "const actual=JSON.parse(process.argv[1]); const expected=JSON.parse(process.argv[2]); process.stdout.write(JSON.stringify(actual) === JSON.stringify(expected) ? 'true' : 'false');" "${actual_body}" "${expected_body}")"

  [[ "${actual_schedule}" == "${expected_schedule}" ]] || { echo "${name}: expected schedule ${expected_schedule}, got ${actual_schedule}" >&2; exit 1; }
  [[ "${actual_timezone}" == "${expected_timezone}" ]] || { echo "${name}: expected time zone ${expected_timezone}, got ${actual_timezone}" >&2; exit 1; }
  [[ "${actual_uri}" == "${RUN_URI}" ]] || { echo "${name}: expected URI ${RUN_URI}, got ${actual_uri}" >&2; exit 1; }
  [[ "${actual_method}" == "POST" ]] || { echo "${name}: expected HTTP method POST, got ${actual_method}" >&2; exit 1; }
  [[ "${bodies_match}" == "true" ]] || { echo "${name}: expected body ${expected_body}, got ${actual_body}" >&2; exit 1; }
}

while IFS=$'\t' read -r name schedule time_zone body; do
  verify_job "${name}" "${schedule}" "${body}" "${time_zone}"
done < <(
  printf '%s' "${JOB_SPECS_JSON}" | node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(0,'utf8')); for (const spec of payload.specs ?? []) { process.stdout.write([spec.jobName, spec.schedule, spec.timeZone, JSON.stringify(spec.body)].join('\t') + '\n'); }"
)

echo "Cloud Scheduler jobs verified for ${RUN_URI}"
