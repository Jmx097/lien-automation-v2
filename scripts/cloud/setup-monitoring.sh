#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${SERVICE_NAME:=lien-automation}"
: "${BQ_LOCATION:=US}"
: "${BQ_DATASET:=lien_automation_monitoring}"
: "${LOG_SINK_NAME:=lien-service-run-metadata-sink}"
: "${UPTIME_CHECK_NAME:=Lien Automation Schedule Health}"
: "${ENABLE_BIGQUERY_SINK:=0}"

CHANNEL_ID="${NOTIFICATION_CHANNEL:-}"

if [[ -z "${CHANNEL_ID}" && -z "${NOTIFICATION_EMAIL:-}" ]]; then
  echo "Set NOTIFICATION_CHANNEL or NOTIFICATION_EMAIL" >&2
  exit 1
fi

SERVICE_URL="${SERVICE_URL:-$(gcloud run services describe "${SERVICE_NAME}" --project="${GCP_PROJECT_ID}" --region="${GCP_REGION}" --format='value(status.url)')}"
SERVICE_HOST="${SERVICE_URL#https://}"
SERVICE_HOST="${SERVICE_HOST#http://}"
SERVICE_HOST="${SERVICE_HOST%%/*}"
SERVICE_FILTER="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${SERVICE_NAME}\""

ERROR_POLICY_FILE="$(mktemp)"
MISSED_POLICY_FILE="$(mktemp)"
CONNECTIVITY_POLICY_FILE="$(mktemp)"

cleanup() {
  rm -f "${ERROR_POLICY_FILE}" "${MISSED_POLICY_FILE}" "${CONNECTIVITY_POLICY_FILE}"
}
trap cleanup EXIT

monitoring_api() {
  local method="$1"
  local url="$2"
  local payload="${3:-}"
  local token
  token="$(gcloud auth print-access-token)"

  if [[ -n "${payload}" ]]; then
    curl -fsS -X "${method}" \
      -H "Authorization: Bearer ${token}" \
      -H "Content-Type: application/json" \
      -d "${payload}" \
      "${url}"
    return
  fi

  curl -fsS -X "${method}" \
    -H "Authorization: Bearer ${token}" \
    "${url}"
}

ensure_channel() {
  if [[ -n "${CHANNEL_ID}" ]]; then
    return
  fi

  local channels_json
  channels_json="$(monitoring_api GET "https://monitoring.googleapis.com/v3/projects/${GCP_PROJECT_ID}/notificationChannels?pageSize=200")"

  CHANNEL_ID="$(
    printf '%s' "${channels_json}" | node -e "
      const fs = require('fs');
      const email = process.argv[1];
      const payload = JSON.parse(fs.readFileSync(0, 'utf8'));
      const channels = Array.isArray(payload.notificationChannels) ? payload.notificationChannels : [];
      const match = channels.find((channel) =>
        channel.type === 'email' &&
        channel.labels &&
        channel.labels.email_address === email
      );
      process.stdout.write(match?.name ?? '');
    " "${NOTIFICATION_EMAIL}"
  )"

  if [[ -n "${CHANNEL_ID}" ]]; then
    return
  fi

  local create_payload
  create_payload="$(cat <<JSON
{
  "type": "email",
  "displayName": "Lien Automation OnCall Email",
  "labels": {
    "email_address": "${NOTIFICATION_EMAIL}"
  },
  "enabled": true
}
JSON
)"

  CHANNEL_ID="$(
    monitoring_api POST "https://monitoring.googleapis.com/v3/projects/${GCP_PROJECT_ID}/notificationChannels" "${create_payload}" | node -e "
      const fs = require('fs');
      const payload = JSON.parse(fs.readFileSync(0, 'utf8'));
      process.stdout.write(payload.name ?? '');
    "
  )"
}

create_metric() {
  local name="$1"
  local description="$2"
  local filter="$3"

  gcloud logging metrics create "${name}" \
    --project="${GCP_PROJECT_ID}" \
    --description="${description}" \
    --log-filter="${filter}" \
    2>/dev/null || gcloud logging metrics update "${name}" \
      --project="${GCP_PROJECT_ID}" \
      --description="${description}" \
      --log-filter="${filter}"
}

upsert_alert_policy() {
  local display_name="$1"
  local file="$2"
  local policy_name
  policy_name="$(
    gcloud monitoring policies list --project="${GCP_PROJECT_ID}" --format=json | node -e "
      const fs = require('fs');
      const displayName = process.argv[1];
      const policies = JSON.parse(fs.readFileSync(0, 'utf8'));
      const match = Array.isArray(policies) ? policies.find((policy) => policy.displayName === displayName) : undefined;
      process.stdout.write(match?.name ?? '');
    " "${display_name}"
  )"

  if [[ -n "${policy_name}" ]]; then
    gcloud monitoring policies update "${policy_name}" --project="${GCP_PROJECT_ID}" --policy-from-file="${file}" >/dev/null
    return
  fi

  gcloud monitoring policies create --project="${GCP_PROJECT_ID}" --policy-from-file="${file}" >/dev/null
}

setup_bigquery_sink() {
  if [[ "${ENABLE_BIGQUERY_SINK}" != "1" ]]; then
    echo "Skipping BigQuery sink setup (ENABLE_BIGQUERY_SINK=0)."
    return
  fi

  if ! bq version >/dev/null 2>&1; then
    echo "Skipping BigQuery sink setup because the local bq tool is not healthy." >&2
    return
  fi

  bq --location="${BQ_LOCATION}" mk --dataset --description="Cloud Run service metadata sink" "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>/dev/null || true

  gcloud logging sinks create "${LOG_SINK_NAME}" \
    "bigquery.googleapis.com/projects/${GCP_PROJECT_ID}/datasets/${BQ_DATASET}" \
    --project="${GCP_PROJECT_ID}" \
    --log-filter="${SERVICE_FILTER} AND (jsonPayload.stage=\"scheduled_run_start\" OR jsonPayload.stage=\"scheduled_run_complete\" OR jsonPayload.stage=\"scheduled_run_error\" OR jsonPayload.stage=\"missed_run_alerted\" OR jsonPayload.stage=\"site_connectivity_probe_failure\")" \
    --use-partitioned-tables \
    2>/dev/null || gcloud logging sinks update "${LOG_SINK_NAME}" \
      "bigquery.googleapis.com/projects/${GCP_PROJECT_ID}/datasets/${BQ_DATASET}" \
      --project="${GCP_PROJECT_ID}" \
      --log-filter="${SERVICE_FILTER} AND (jsonPayload.stage=\"scheduled_run_start\" OR jsonPayload.stage=\"scheduled_run_complete\" OR jsonPayload.stage=\"scheduled_run_error\" OR jsonPayload.stage=\"missed_run_alerted\" OR jsonPayload.stage=\"site_connectivity_probe_failure\")" \
      --use-partitioned-tables

  local sink_writer
  sink_writer="$(gcloud logging sinks describe "${LOG_SINK_NAME}" --project="${GCP_PROJECT_ID}" --format='value(writerIdentity)')"
  bq --location="${BQ_LOCATION}" update --dataset --add_iam_member="member:${sink_writer},role:roles/bigquery.dataEditor" "${GCP_PROJECT_ID}:${BQ_DATASET}"
}

replace_uptime_check() {
  local existing_check
  existing_check="$(
    gcloud monitoring uptime list-configs --project="${GCP_PROJECT_ID}" --format=json | node -e "
      const fs = require('fs');
      const displayName = process.argv[1];
      const checks = JSON.parse(fs.readFileSync(0, 'utf8'));
      const match = Array.isArray(checks) ? checks.find((check) => check.displayName === displayName) : undefined;
      process.stdout.write(match?.name ?? '');
    " "${UPTIME_CHECK_NAME}"
  )"

  if [[ -n "${existing_check}" ]]; then
    gcloud monitoring uptime delete "${existing_check}" --project="${GCP_PROJECT_ID}" --quiet >/dev/null
  fi

  local create_payload
  create_payload="$(cat <<JSON
{
  "displayName": "${UPTIME_CHECK_NAME}",
  "monitoredResource": {
    "type": "uptime_url",
    "labels": {
      "host": "${SERVICE_HOST}",
      "project_id": "${GCP_PROJECT_ID}"
    }
  },
  "httpCheck": {
    "path": "/schedule/health",
    "port": 443,
    "useSsl": true,
    "requestMethod": "GET",
    "acceptedResponseStatusCodes": [
      {
        "statusValue": 200
      }
    ]
  },
  "period": "300s",
  "timeout": "10s",
  "checkerType": "STATIC_IP_CHECKERS"
}
JSON
)"

  monitoring_api POST "https://monitoring.googleapis.com/v3/projects/${GCP_PROJECT_ID}/uptimeCheckConfigs" "${create_payload}" >/dev/null

  gcloud monitoring uptime list-configs \
    --project="${GCP_PROJECT_ID}" \
    --filter="displayName=\"${UPTIME_CHECK_NAME}\"" \
    --format="table(displayName,httpCheck.path,monitoredResource.labels.host)"
}

create_metric \
  "lien_scheduled_run_errors" \
  "Count scheduler run errors on the Cloud Run service" \
  "${SERVICE_FILTER} AND jsonPayload.stage=\"scheduled_run_error\""

create_metric \
  "lien_missed_run_alerts" \
  "Count missed scheduled run alerts on the Cloud Run service" \
  "${SERVICE_FILTER} AND jsonPayload.stage=\"missed_run_alerted\""

create_metric \
  "lien_nyc_connectivity_failures" \
  "Count NYC ACRIS connectivity probe failures on the Cloud Run service" \
  "${SERVICE_FILTER} AND jsonPayload.stage=\"site_connectivity_probe_failure\""

ensure_channel
setup_bigquery_sink

cat > "${ERROR_POLICY_FILE}" <<JSON
{
  "displayName": "Lien Automation Scheduled Run Errors",
  "combiner": "OR",
  "documentation": {
    "content": "Alert when the Cloud Run service logs scheduled_run_error.",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "scheduled_run_error > 0",
      "conditionThreshold": {
        "filter": "metric.type=\\"logging.googleapis.com/user/lien_scheduled_run_errors\\" resource.type=\\"cloud_run_revision\\"",
        "comparison": "COMPARISON_GT",
        "thresholdValue": 0,
        "duration": "0s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE"
          }
        ],
        "trigger": {
          "count": 1
        }
      }
    }
  ],
  "notificationChannels": ["${CHANNEL_ID}"],
  "enabled": true,
  "alertStrategy": { "autoClose": "1800s" }
}
JSON

cat > "${MISSED_POLICY_FILE}" <<JSON
{
  "displayName": "Lien Automation Missed Scheduled Runs",
  "combiner": "OR",
  "documentation": {
    "content": "Alert when the Cloud Run service logs missed_run_alerted.",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "missed_run_alerted > 0",
      "conditionThreshold": {
        "filter": "metric.type=\\"logging.googleapis.com/user/lien_missed_run_alerts\\" resource.type=\\"cloud_run_revision\\"",
        "comparison": "COMPARISON_GT",
        "thresholdValue": 0,
        "duration": "0s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE"
          }
        ],
        "trigger": {
          "count": 1
        }
      }
    }
  ],
  "notificationChannels": ["${CHANNEL_ID}"],
  "enabled": true,
  "alertStrategy": { "autoClose": "1800s" }
}
JSON

cat > "${CONNECTIVITY_POLICY_FILE}" <<JSON
{
  "displayName": "Lien Automation NYC Connectivity Failures",
  "combiner": "OR",
  "documentation": {
    "content": "Alert when the Cloud Run service logs repeated NYC ACRIS connectivity probe failures.",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "site_connectivity_probe_failure > 0",
      "conditionThreshold": {
        "filter": "metric.type=\\"logging.googleapis.com/user/lien_nyc_connectivity_failures\\" resource.type=\\"cloud_run_revision\\"",
        "comparison": "COMPARISON_GT",
        "thresholdValue": 0,
        "duration": "0s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE"
          }
        ],
        "trigger": {
          "count": 1
        }
      }
    }
  ],
  "notificationChannels": ["${CHANNEL_ID}"],
  "enabled": true,
  "alertStrategy": { "autoClose": "1800s" }
}
JSON

upsert_alert_policy "Lien Automation Scheduled Run Errors" "${ERROR_POLICY_FILE}"
upsert_alert_policy "Lien Automation Missed Scheduled Runs" "${MISSED_POLICY_FILE}"
upsert_alert_policy "Lien Automation NYC Connectivity Failures" "${CONNECTIVITY_POLICY_FILE}"
replace_uptime_check

echo "Monitoring configured for Cloud Run service ${SERVICE_NAME} at ${SERVICE_URL}"
