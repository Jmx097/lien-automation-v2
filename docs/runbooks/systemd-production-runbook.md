# Production Runbook (Systemd)

This repository supports **one production startup path**: `systemd`.

## Components under supervision

1. **API/Job runner**: `lien-automation-api.service`
   - Runs the Express API (`npm start`) and executes scheduled jobs when `/schedule/run` is called.
   - For NYC ACRIS production reliability, set `NYC_ACRIS_TRANSPORT_MODE=legacy-sbr-cdp` in the service environment until Browser API is revalidated.
2. **Orchestration dependency**: `lien-automation-schedule.timer`
   - Fires at **07:30** and **19:30** America/New_York.
   - Triggers `lien-automation-schedule.service`, which calls the API schedule endpoint.

## Install unit files

```bash
sudo cp deploy/systemd/lien-automation-api.service /etc/systemd/system/
sudo cp deploy/systemd/lien-automation-schedule.service /etc/systemd/system/
sudo cp deploy/systemd/lien-automation-schedule.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

## Start

```bash
sudo systemctl enable --now lien-automation-api.service
sudo systemctl enable --now lien-automation-schedule.timer
```

## Stop

```bash
sudo systemctl stop lien-automation-schedule.timer lien-automation-api.service
```

## Restart

```bash
sudo systemctl restart lien-automation-api.service
sudo systemctl restart lien-automation-schedule.timer
```

## Logs

```bash
sudo journalctl -u lien-automation-api.service -f
sudo journalctl -u lien-automation-schedule.service -f
```

## Schedule verification

Use this single verification command to confirm timer activity and readiness checks:

```bash
bash scripts/ops/verify-schedule.sh
```

The script verifies:
- timer status and next run time,
- API health,
- schedule readiness checks (`/schedule/health`), including required env, SQLite reachability, and sheets credential parsing.

## NYC Bootstrap Verification

When NYC ACRIS is failing before first navigation or returning `about:blank`, verify hosted bootstrap before retrying the full canary:

```bash
bash scripts/cloud/verify-cloud-run-service.sh
NYC_DEBUG_TRANSPORT_MODE=brightdata-browser-api bash scripts/cloud/debug-nyc-acris-bootstrap.sh
NYC_DEBUG_TRANSPORT_MODE=legacy-sbr-cdp bash scripts/cloud/debug-nyc-acris-bootstrap.sh
```

Interpretation:
- If hosted debug succeeds on one transport, keep `NYC_ACRIS_TRANSPORT_MODE` pinned there and then run `bash scripts/cloud/run-nyc-acris-prod-canary.sh`.
- If hosted debug fails on both transports with the same `about:blank before first navigation` shape, treat it as a bootstrap/environment incident, not a selector regression.
