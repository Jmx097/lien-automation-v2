# Production Runbook (Systemd)

This repository supports **one production startup path**: `systemd`.

## Components under supervision

1. **API/Job runner**: `lien-automation-api.service`
   - Runs the Express API (`npm start`) and executes scheduled jobs when `/schedule/run` is called.
2. **Orchestration dependency**: `lien-automation-schedule.timer`
   - Fires at **07:30** and **14:30** America/New_York.
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
