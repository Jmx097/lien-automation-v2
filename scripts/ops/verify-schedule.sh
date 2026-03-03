#!/usr/bin/env bash
set -euo pipefail

echo "[1/3] systemd timer status"
systemctl status lien-automation-schedule.timer --no-pager

echo "\n[2/3] next scheduled runs"
systemctl list-timers lien-automation-schedule.timer --no-pager

echo "\n[3/3] API checks"
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/schedule/health
