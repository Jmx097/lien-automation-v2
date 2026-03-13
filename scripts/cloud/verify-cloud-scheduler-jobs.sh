#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${API_BASE_URL:?Set API_BASE_URL}"
: "${JOB_NAME:=lien-scraper-schedule-run}"
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

RUN_URI="${API_BASE_URL%/}/schedule/run"

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

verify_job "${CA_MORNING_JOB_NAME}" "0 7 * * 1-5" '{"site":"ca_sos","slot":"morning"}' "${SCHEDULE_CA_SOS_TIMEZONE}"
verify_job "${CA_AFTERNOON_JOB_NAME}" "0 11 * * 1-5" '{"site":"ca_sos","slot":"afternoon"}' "${SCHEDULE_CA_SOS_TIMEZONE}"
verify_job "${CA_EVENING_JOB_NAME}" "0 19 * * 1-5" '{"site":"ca_sos","slot":"evening"}' "${SCHEDULE_CA_SOS_TIMEZONE}"
verify_job "${MARICOPA_MORNING_JOB_NAME}" "0 10 * * 1-5" '{"site":"maricopa_recorder","slot":"morning"}' "${SCHEDULE_MARICOPA_RECORDER_TIMEZONE}"
verify_job "${MARICOPA_AFTERNOON_JOB_NAME}" "0 14 * * 1-5" '{"site":"maricopa_recorder","slot":"afternoon"}' "${SCHEDULE_MARICOPA_RECORDER_TIMEZONE}"
verify_job "${MARICOPA_EVENING_JOB_NAME}" "0 22 * * 1-5" '{"site":"maricopa_recorder","slot":"evening"}' "${SCHEDULE_MARICOPA_RECORDER_TIMEZONE}"
verify_job "${NYC_MORNING_JOB_NAME}" "0 10 * * 1-5" '{"site":"nyc_acris","slot":"morning"}' "${SCHEDULE_NYC_ACRIS_TIMEZONE}"
verify_job "${NYC_AFTERNOON_JOB_NAME}" "0 14 * * 1-5" '{"site":"nyc_acris","slot":"afternoon"}' "${SCHEDULE_NYC_ACRIS_TIMEZONE}"
verify_job "${NYC_EVENING_JOB_NAME}" "0 22 * * 1-5" '{"site":"nyc_acris","slot":"evening"}' "${SCHEDULE_NYC_ACRIS_TIMEZONE}"

echo "Cloud Scheduler jobs verified for ${RUN_URI}"
