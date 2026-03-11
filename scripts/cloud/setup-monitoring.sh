#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:?Set GCP_REGION}"
: "${SERVICE_NAME:=lien-automation}"
: "${BQ_LOCATION:=US}"
: "${BQ_DATASET:=lien_automation_monitoring}"
: "${LOG_SINK_NAME:=lien-service-run-metadata-sink}"
: "${UPTIME_CHECK_NAME:=Lien Automation Schedule Health}"

CHANNEL_ID="${NOTIFICATION_CHANNEL:-}"

if [[ -z "${CHANNEL_ID}" && -z "${NOTIFICATION_EMAIL:-}" ]]; then
  echo "Set NOTIFICATION_CHANNEL or NOTIFICATION_EMAIL" >&2
  exit 1
fi

SERVICE_URL="${SERVICE_URL:-$(gcloud run services describe "${SERVICE_NAME}" --project="${GCP_PROJECT_ID}" --region="${GCP_REGION}" --format='value(status.url)')}"
SERVICE_HOST="${SERVICE_URL#https://}"
SERVICE_HOST="${SERVICE_HOST#http://}"
SERVICE_HOST="${SERVICE_HOST%%/*}"

ensure_channel() {
  if [[ -n "${CHANNEL_ID}" ]]; then
    return
  fi

  if ! gcloud alpha monitoring channels list --project="${GCP_PROJECT_ID}" >/dev/null 2>&1; then
    echo "NOTIFICATION_CHANNEL not provided and gcloud alpha is unavailable for channel creation" >&2
    exit 1
  fi

  CHANNEL_ID="$(gcloud alpha monitoring channels list \
    --project="${GCP_PROJECT_ID}" \
    --filter="labels.email_address=${NOTIFICATION_EMAIL}" \
    --format='value(name)' | head -n1)"

  if [[ -z "${CHANNEL_ID}" ]]; then
    CHANNEL_ID="$(gcloud alpha monitoring channels create \
      --project="${GCP_PROJECT_ID}" \
      --display-name="Lien Automation OnCall Email" \
      --type=email \
      --channel-labels=email_address="${NOTIFICATION_EMAIL}" \
      --format='value(name)')"
  fi
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

create_alert_policy() {
  local file="$1"
  gcloud monitoring policies create --project="${GCP_PROJECT_ID}" --policy-from-file="${file}" 2>/dev/null || true
}

bq --location="${BQ_LOCATION}" mk --dataset --description="Cloud Run service metadata sink" "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>/dev/null || true

SERVICE_FILTER="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${SERVICE_NAME}\""

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

SINK_WRITER="$(gcloud logging sinks describe "${LOG_SINK_NAME}" --project="${GCP_PROJECT_ID}" --format='value(writerIdentity)')"
bq --location="${BQ_LOCATION}" update --dataset --add_iam_member="member:${SINK_WRITER},role:roles/bigquery.dataEditor" "${GCP_PROJECT_ID}:${BQ_DATASET}"

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

cat > /tmp/lien-service-error-policy.json <<JSON
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

cat > /tmp/lien-service-missed-run-policy.json <<JSON
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

cat > /tmp/lien-service-connectivity-policy.json <<JSON
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

create_alert_policy /tmp/lien-service-error-policy.json
create_alert_policy /tmp/lien-service-missed-run-policy.json
create_alert_policy /tmp/lien-service-connectivity-policy.json

gcloud monitoring uptime create "${UPTIME_CHECK_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --resource-type=uptime-url \
  --resource-labels=host="${SERVICE_HOST}",project_id="${GCP_PROJECT_ID}" \
  --protocol=https \
  --path=/schedule/health \
  --request-method=get \
  --period=5 \
  --timeout=10 \
  --status-codes=200 \
  2>/dev/null || gcloud monitoring uptime update "${UPTIME_CHECK_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --resource-type=uptime-url \
    --resource-labels=host="${SERVICE_HOST}",project_id="${GCP_PROJECT_ID}" \
    --protocol=https \
    --path=/schedule/health \
    --request-method=get \
    --period=5 \
    --timeout=10 \
    --status-codes=200

echo "Monitoring configured for Cloud Run service ${SERVICE_NAME} at ${SERVICE_URL}"
