#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

echo "[doctor] repo: $(pwd)"
echo "[doctor] branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

echo "[doctor] node: $(node -v 2>/dev/null || echo 'missing')"
echo "[doctor] npm:  $(npm -v 2>/dev/null || echo 'missing')"

NODE_MAJOR="$(node -p "process.versions.node.split('.')[0]" 2>/dev/null || echo 0)"
NODE_MINOR="$(node -p "process.versions.node.split('.')[1]" 2>/dev/null || echo 0)"
if (( NODE_MAJOR < 20 )) || (( NODE_MAJOR == 20 && NODE_MINOR < 16 )); then
  echo "[doctor][error] Node >=20.16.0 required (recommended: 22 LTS)."
  echo "[doctor][hint] If nvm exists: nvm install 22 && nvm use 22"
  echo "[doctor][hint] If nvm is missing (Ubuntu/Debian): curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - && sudo apt-get install -y nodejs"
  exit 1
fi

if [[ ! -f package.json ]]; then
  echo "[doctor][error] package.json not found. Are you in repo root?"
  exit 1
fi

HAS_TEST_TYPES="$(node -e "const p=require('./package.json'); process.stdout.write(p.scripts&&p.scripts['test:types']?'yes':'no')")"
HAS_TEST_SMOKE="$(node -e "const p=require('./package.json'); process.stdout.write(p.scripts&&p.scripts['test:smoke']?'yes':'no')")"
HAS_DOCTOR="$(node -e "const p=require('./package.json'); process.stdout.write(p.scripts&&p.scripts['doctor']?'yes':'no')")"

if [[ "$HAS_TEST_TYPES" != "yes" || "$HAS_TEST_SMOKE" != "yes" || "$HAS_DOCTOR" != "yes" ]]; then
  echo "[doctor][error] package.json scripts are outdated (missing doctor/test:types/test:smoke)."
  echo "[doctor][hint] confirm repo root with: pwd && git remote -v"
  echo "[doctor][hint] sync latest branch: git fetch --all --prune && git pull --rebase"
  echo "[doctor][hint] verify scripts: npm run"
  exit 1
fi

if [[ ! -x node_modules/.bin/tsc ]]; then
  echo "[doctor][error] typescript binary missing (node_modules/.bin/tsc not found)."
  echo "[doctor][hint] run: npm install"
  exit 1
fi

echo "[doctor] OK: runtime, scripts, and local toolchain look good."
