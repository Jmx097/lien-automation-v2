#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${SERVICE_NAME:=lien-automation}"
: "${EXPECTED_GIT_SHA:?Set EXPECTED_GIT_SHA}"

SERVICE_URL="${SERVICE_URL:-$(gcloud run services describe "${SERVICE_NAME}" --project="${GCP_PROJECT_ID}" --region="${GCP_REGION}" --format='value(status.url)')}"

health_json="$(curl -fsS "${SERVICE_URL}/health")"
schedule_health_json="$(curl -fsS "${SERVICE_URL}/schedule/health")"
schedule_json="$(curl -fsS "${SERVICE_URL}/schedule")"
version_json="$(curl -fsS "${SERVICE_URL}/version")"

runtime_sha="$(printf '%s' "${version_json}" | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).git_sha ?? 'unknown'))")"

if [[ "${runtime_sha}" != "${EXPECTED_GIT_SHA}" ]]; then
  echo "Expected git_sha ${EXPECTED_GIT_SHA} but service reports ${runtime_sha}" >&2
  exit 1
fi

printf '%s\n' "${health_json}"
printf '%s\n' "${schedule_health_json}"
printf '%s\n' "${schedule_json}"
printf '%s\n' "${version_json}"
