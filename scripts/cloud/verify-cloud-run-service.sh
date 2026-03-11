#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${SERVICE_NAME:=lien-automation}"
: "${EXPECTED_GIT_SHA:?Set EXPECTED_GIT_SHA}"
: "${EXPECTED_SCHEDULER_STORE:=}"

SERVICE_URL="${SERVICE_URL:-$(gcloud run services describe "${SERVICE_NAME}" --project="${GCP_PROJECT_ID}" --region="${GCP_REGION}" --format='value(status.url)')}"

health_json="$(curl -fsS "${SERVICE_URL}/health")"
schedule_health_json="$(curl -fsS "${SERVICE_URL}/schedule/health")"
schedule_json="$(curl -fsS "${SERVICE_URL}/schedule")"
version_json="$(curl -fsS "${SERVICE_URL}/version")"

runtime_sha="$(printf '%s' "${version_json}" | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).git_sha ?? 'unknown'))")"
persisted_flag="$(printf '%s' "${schedule_json}" | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).persisted === true ? 'true' : 'false'))")"
history_shape_ok="$(printf '%s' "${schedule_json}" | node -e "process.stdin.once('data', d => { const parsed = JSON.parse(d); console.log(Array.isArray(parsed.history) ? 'true' : 'false'); })")"
scheduler_store="$(printf '%s' "${schedule_health_json}" | node -e "process.stdin.once('data', d => { const parsed = JSON.parse(d); const checks = Array.isArray(parsed.checks) ? parsed.checks : []; const db = checks.find((check) => check.name === 'db_reachable'); const detail = String(db?.detail ?? ''); const match = detail.match(/scheduler_store=([^,\\s]+)/); console.log(match?.[1] ?? 'unknown'); })")"

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

printf '%s\n' "${health_json}"
printf '%s\n' "${schedule_health_json}"
printf '%s\n' "${schedule_json}"
printf '%s\n' "${version_json}"
