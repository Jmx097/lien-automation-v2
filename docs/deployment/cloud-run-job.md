# Cloud Run Job + Cloud Scheduler Deployment

This deployment packages scraping as a one-shot execution (`src/job-entrypoint.ts`) and schedules it via Cloud Scheduler.

## 1) Build and push image

```bash
export GCP_PROJECT_ID="<project-id>"
export GCP_REGION="us-central1"
export IMAGE_URI="${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/lien-automation/lien-automation-v2:$(git rev-parse --short HEAD)"

gcloud auth configure-docker "${GCP_REGION}-docker.pkg.dev"
docker build -t "${IMAGE_URI}" .
docker push "${IMAGE_URI}"
```

## 2) Deploy Cloud Run Job (required env vars)

```bash
export SBR_CDP_URL='wss://<bright-data-cdp-url>'
export SHEETS_KEY='{"type":"service_account",...}'
export SHEET_ID='<google-sheet-id>'

export JOB_NAME='lien-scraper-job'
export JOB_LOOKBACK_DAYS='7'
export JOB_MAX_RECORDS='25'

bash scripts/cloud/deploy-cloud-run-job.sh
```

The deployment sets all required vars: `SBR_CDP_URL`, `SHEETS_KEY`, `SHEET_ID`.

## 3) Create two Cloud Scheduler jobs (timezone explicit)

```bash
export SCHEDULER_SA_EMAIL='scheduler-invoker@<project-id>.iam.gserviceaccount.com'
export TIME_ZONE='America/New_York'

bash scripts/cloud/create-scheduler-jobs.sh
```

This creates/updates:

- `${JOB_NAME}-morning` at `30 7 * * *`
- `${JOB_NAME}-afternoon` at `30 19 * * *`

Both execute Cloud Run Job API `.../jobs/${JOB_NAME}:run`.

## 4) Retry policy + max retry duration + failure notifications

### Retry policy on Scheduler

`create-scheduler-jobs.sh` configures:

- `--max-retry-attempts` (default `3`)
- `--max-retry-duration` (default `1800s`)
- `--min-backoff` (default `30s`)
- `--max-backoff` (default `300s`)

Override by exporting env vars before running the script.

### Failure notifications

```bash
export NOTIFICATION_EMAIL='alerts@example.com'
bash scripts/cloud/setup-monitoring.sh
```

This script creates:

- BigQuery dataset as a persistent sink target.
- Logging sink for `cloud_run_job` run metadata logs.
- Log-based metric for job failures (`cloud_run_job_error`).
- Monitoring alert policy that emails the configured channel.

## 5) Persistent run metadata sink for monitoring

The one-shot entrypoint logs structured lifecycle metadata:

- `cloud_run_job_start`
- `cloud_run_job_complete`
- `cloud_run_job_error`

Each includes start/end timestamps, duration, records count, and error field. The logging sink persists these events to BigQuery for dashboards and historical monitoring.

## Manual verification

```bash
# Trigger a single execution

gcloud run jobs execute "${JOB_NAME}" --region "${GCP_REGION}" --project "${GCP_PROJECT_ID}" --wait

# Read recent executions

gcloud run jobs executions list --job "${JOB_NAME}" --region "${GCP_REGION}" --project "${GCP_PROJECT_ID}"

# Check scheduler jobs

gcloud scheduler jobs describe "${JOB_NAME}-morning" --location "${GCP_REGION}" --project "${GCP_PROJECT_ID}"
gcloud scheduler jobs describe "${JOB_NAME}-afternoon" --location "${GCP_REGION}" --project "${GCP_PROJECT_ID}"
```
