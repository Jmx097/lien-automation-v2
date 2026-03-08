#!/usr/bin/env bash
set -euo pipefail

: "${APP_DIR:=/opt/lien-automation-v2}"
: "${GIT_REF:=main}"
: "${API_SERVICE:=lien-automation-api.service}"
: "${SCHEDULE_TIMER:=lien-automation-schedule.timer}"
: "${APP_ENV_FILE:=/etc/lien-automation-v2/lien-automation.env}"

if [[ ! -d "${APP_DIR}/.git" ]]; then
  echo "APP_DIR does not contain a git checkout: ${APP_DIR}" >&2
  exit 1
fi

cd "${APP_DIR}"

git fetch --tags origin
git checkout --force "${GIT_REF}"

npm ci
npm run build

if [[ -f "${APP_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${APP_ENV_FILE}"
  set +a
fi

node src/queue/init-db.js

sudo systemctl restart "${API_SERVICE}"
sudo systemctl restart "${SCHEDULE_TIMER}"
sudo systemctl status "${API_SERVICE}" --no-pager --lines=20
