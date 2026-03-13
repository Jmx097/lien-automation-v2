#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${API_BASE_URL:?Set API_BASE_URL}"
: "${JOB_NAME:=lien-scraper-schedule-run}"
: "${TIME_ZONE:=America/New_York}"
: "${CA_MORNING_JOB_NAME:=${JOB_NAME}-ca-sos-morning}"
: "${CA_AFTERNOON_JOB_NAME:=${JOB_NAME}-ca-sos-afternoon}"
: "${NYC_MORNING_JOB_NAME:=${JOB_NAME}-nyc-acris-morning}"
: "${NYC_AFTERNOON_JOB_NAME:=${JOB_NAME}-nyc-acris-afternoon}"

RUN_URI="${API_BASE_URL%/}/schedule/run"

verify_job() {
  local name="$1"
  local expected_schedule="$2"
  local expected_body="$3"

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
  [[ "${actual_timezone}" == "${TIME_ZONE}" ]] || { echo "${name}: expected time zone ${TIME_ZONE}, got ${actual_timezone}" >&2; exit 1; }
  [[ "${actual_uri}" == "${RUN_URI}" ]] || { echo "${name}: expected URI ${RUN_URI}, got ${actual_uri}" >&2; exit 1; }
  [[ "${actual_method}" == "POST" ]] || { echo "${name}: expected HTTP method POST, got ${actual_method}" >&2; exit 1; }
  [[ "${bodies_match}" == "true" ]] || { echo "${name}: expected body ${expected_body}, got ${actual_body}" >&2; exit 1; }
}

verify_job "${CA_MORNING_JOB_NAME}" "0 6 * * *" '{"site":"ca_sos","slot":"morning"}'
verify_job "${CA_AFTERNOON_JOB_NAME}" "0 12 * * *" '{"site":"ca_sos","slot":"afternoon"}'
verify_job "${NYC_MORNING_JOB_NAME}" "0 10 * * *" '{"site":"nyc_acris","slot":"morning"}'
verify_job "${NYC_AFTERNOON_JOB_NAME}" "0 14 * * *" '{"site":"nyc_acris","slot":"afternoon"}'

echo "Cloud Scheduler jobs verified for ${RUN_URI}"
