#!/usr/bin/env bash
set -euo pipefail

: "${APP_DIR:=/opt/lien-automation-v2}"
: "${APP_ENV_FILE:=/etc/lien-automation-v2/lien-automation.env}"

if [[ ! -d "${APP_DIR}" ]]; then
  echo "APP_DIR does not exist: ${APP_DIR}" >&2
  exit 1
fi

cd "${APP_DIR}"

if [[ -f "${APP_ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${APP_ENV_FILE}"
  set +a
fi

SESSION_JSON="$(npm run --silent refresh:maricopa-session)"
node -e "const data = JSON.parse(process.argv[1]); if (!Number.isFinite(data.row_count) || data.row_count < 1) { console.error('Maricopa refresh did not reach a populated results table.'); process.exit(1); }" "${SESSION_JSON}"

DISCOVERY_JSON="$(npm run --silent discover:maricopa-live)"
node -e "const data = JSON.parse(process.argv[1]); if (!Number.isFinite(data.candidate_count) || data.candidate_count < 1) { console.error('Maricopa discovery captured no artifact candidates.'); process.exit(1); }" "${DISCOVERY_JSON}"

VALIDATION_JSON="$(npm run --silent validate:maricopa-live)"
node -e "const data = JSON.parse(process.argv[1]); if (!data?.session?.fresh) { console.error('Maricopa validation reports a stale session.'); process.exit(1); } if (!Number.isFinite(data?.discovery_candidates?.count) || data.discovery_candidates.count < 1) { console.error('Maricopa validation reports no artifact candidates.'); process.exit(1); }" "${VALIDATION_JSON}"

printf '%s\n' "${VALIDATION_JSON}"
