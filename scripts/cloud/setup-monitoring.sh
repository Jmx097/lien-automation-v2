#!/usr/bin/env bash
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${BQ_LOCATION:=US}"
: "${BQ_DATASET:=lien_automation_monitoring}"
: "${LOG_SINK_NAME:=lien-job-run-metadata-sink}"
: "${LOG_METRIC_NAME:=cloud_run_job_failures}"
: "${NOTIFICATION_EMAIL:?Set NOTIFICATION_EMAIL}"

bq --location="${BQ_LOCATION}" mk --dataset --description="Cloud Run Job metadata sink" "${GCP_PROJECT_ID}:${BQ_DATASET}" 2>/dev/null || true

gcloud logging sinks create "${LOG_SINK_NAME}" \
  "bigquery.googleapis.com/projects/${GCP_PROJECT_ID}/datasets/${BQ_DATASET}" \
  --project="${GCP_PROJECT_ID}" \
  --log-filter='resource.type="cloud_run_job" AND (jsonPayload.stage="cloud_run_job_start" OR jsonPayload.stage="cloud_run_job_complete" OR jsonPayload.stage="cloud_run_job_error")' \
  --use-partitioned-tables \
  2>/dev/null || gcloud logging sinks update "${LOG_SINK_NAME}" \
    "bigquery.googleapis.com/projects/${GCP_PROJECT_ID}/datasets/${BQ_DATASET}" \
    --project="${GCP_PROJECT_ID}" \
    --log-filter='resource.type="cloud_run_job" AND (jsonPayload.stage="cloud_run_job_start" OR jsonPayload.stage="cloud_run_job_complete" OR jsonPayload.stage="cloud_run_job_error")' \
    --use-partitioned-tables

SINK_WRITER=$(gcloud logging sinks describe "${LOG_SINK_NAME}" --project="${GCP_PROJECT_ID}" --format='value(writerIdentity)')
bq --location="${BQ_LOCATION}" update --dataset --add_iam_member="member:${SINK_WRITER},role:roles/bigquery.dataEditor" "${GCP_PROJECT_ID}:${BQ_DATASET}"

gcloud logging metrics create "${LOG_METRIC_NAME}" \
  --project="${GCP_PROJECT_ID}" \
  --description="Count Cloud Run Job failures" \
  --log-filter='resource.type="cloud_run_job" AND jsonPayload.stage="cloud_run_job_error"' \
  2>/dev/null || true

CHANNEL_ID=$(gcloud alpha monitoring channels create \
  --project="${GCP_PROJECT_ID}" \
  --display-name="Lien Automation OnCall Email" \
  --type=email \
  --channel-labels=email_address="${NOTIFICATION_EMAIL}" \
  --format='value(name)' 2>/dev/null || true)

if [[ -z "${CHANNEL_ID}" ]]; then
  CHANNEL_ID=$(gcloud alpha monitoring channels list --project="${GCP_PROJECT_ID}" --filter="labels.email_address=${NOTIFICATION_EMAIL}" --format='value(name)' | head -n1)
fi

cat > /tmp/lien-job-alert-policy.json <<JSON
{
  "displayName": "Lien Automation Cloud Run Job Failures",
  "documentation": {
    "content": "Alert when Cloud Run job executions fail.",
    "mimeType": "text/markdown"
  },
  "conditions": [
    {
      "displayName": "Job failure count > 0",
      "conditionThreshold": {
        "filter": "metric.type=\"logging.googleapis.com/user/${LOG_METRIC_NAME}\" resource.type=\"cloud_run_job\"",
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
  "notificationChannels": [
    "${CHANNEL_ID}"
  ],
  "enabled": true,
  "alertStrategy": {
    "autoClose": "1800s"
  }
}
JSON

gcloud monitoring policies create --project="${GCP_PROJECT_ID}" --policy-from-file=/tmp/lien-job-alert-policy.json 2>/dev/null || true

echo "Monitoring + persistent sink configured. Dataset=${BQ_DATASET}, sink=${LOG_SINK_NAME}, metric=${LOG_METRIC_NAME}"
