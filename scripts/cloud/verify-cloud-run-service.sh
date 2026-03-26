#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${SERVICE_NAME:=lien-automation}"
: "${EXPECTED_GIT_SHA:?Set EXPECTED_GIT_SHA}"
: "${EXPECTED_SCHEDULER_STORE:=}"
: "${EXPECT_READY:=1}"
: "${EXPECT_TARGET_REACHABLE:=1}"
: "${EXPECT_FALLBACK_ACTIVE:=0}"

SERVICE_URL="${SERVICE_URL:-$(gcloud run services describe "${SERVICE_NAME}" --project="${GCP_PROJECT_ID}" --region="${GCP_REGION}" --format='value(status.url)')}"

health_json="$(curl -fsS "${SERVICE_URL}/health")"
schedule_json="$(curl -fsS "${SERVICE_URL}/schedule")"
version_json="$(curl -fsS "${SERVICE_URL}/version")"

schedule_health_file="$(mktemp)"
cleanup() {
  rm -f "${schedule_health_file}"
}
trap cleanup EXIT

schedule_health_status="$(curl -sS -o "${schedule_health_file}" -w "%{http_code}" "${SERVICE_URL}/schedule/health")"
schedule_health_json="$(cat "${schedule_health_file}")"

if [[ "${schedule_health_status}" != "200" && "${schedule_health_status}" != "503" ]]; then
  echo "Expected /schedule/health to return HTTP 200 or 503, received ${schedule_health_status}" >&2
  exit 1
fi

node_json_eval() {
  local expression="$1"
  node -e "const fs = require('fs'); const input = fs.readFileSync(0, 'utf8'); const parsed = JSON.parse(input); ${expression}"
}

runtime_sha="$(printf '%s' "${version_json}" | node_json_eval "console.log(parsed.git_sha ?? 'unknown')")"
persisted_flag="$(printf '%s' "${schedule_json}" | node_json_eval "console.log(parsed.persisted === true ? 'true' : 'false')")"
history_shape_ok="$(printf '%s' "${schedule_json}" | node_json_eval "console.log(Array.isArray(parsed.history) ? 'true' : 'false')")"
scheduler_store="$(printf '%s' "${schedule_health_json}" | node_json_eval "const checks = Array.isArray(parsed.checks) ? parsed.checks : []; const db = checks.find((check) => check.name === 'db_reachable'); const detail = String(db?.detail ?? ''); const match = detail.match(/scheduler_store=([^,\\s]+)/); console.log(match?.[1] ?? 'unknown')")"
schedule_ready="$(printf '%s' "${schedule_health_json}" | node_json_eval "console.log(parsed.status === 'ready' ? 'true' : 'false')")"
target_reachable="$(printf '%s' "${schedule_health_json}" | node_json_eval "console.log(parsed.merged_output?.target_reachable === true ? 'true' : 'false')")"
fallback_active="$(printf '%s' "${schedule_health_json}" | node_json_eval "console.log(parsed.merged_output?.fallback_active === true ? 'true' : 'false')")"

if [[ "${runtime_sha}" != "${EXPECTED_GIT_SHA}" ]]; then
  echo "Expected git_sha ${EXPECTED_GIT_SHA} but service reports ${runtime_sha}" >&2
  exit 1
fi

if [[ "${persisted_flag}" != "true" ]]; then
  echo "Expected /schedule to report persisted=true" >&2
  exit 1
fi

if [[ "${history_shape_ok}" != "true" ]]; then
  echo "Expected /schedule history to be an array" >&2
  exit 1
fi

if [[ -n "${EXPECTED_SCHEDULER_STORE}" && "${scheduler_store}" != "${EXPECTED_SCHEDULER_STORE}" ]]; then
  echo "Expected scheduler store ${EXPECTED_SCHEDULER_STORE} but service reports ${scheduler_store}" >&2
  exit 1
fi

if [[ "${EXPECT_READY}" == "1" && "${schedule_ready}" != "true" ]]; then
  echo "Expected /schedule/health to report status=ready" >&2
  exit 1
fi

if [[ "${EXPECT_TARGET_REACHABLE}" == "1" && "${target_reachable}" != "true" ]]; then
  echo "Expected merged output target_reachable=true" >&2
  exit 1
fi

if [[ "${EXPECT_FALLBACK_ACTIVE}" == "0" && "${fallback_active}" != "false" ]]; then
  echo "Expected merged output fallback_active=false" >&2
  exit 1
fi

printf '%s\n' "${health_json}"
printf '%s\n' "${schedule_health_json}"
printf '%s\n' "${schedule_json}"
printf '%s\n' "${version_json}"
