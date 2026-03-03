# Codex + Plan-to-Board Runbook

## What changed

- Added OpenAI Codex provider to OpenClaw config while preserving Qwen as primary.
- Created `MCP_GATE_PIPELINE_PLAN.md` derived from `PHASE2_PLAN.md`.
- Seeded `Internal Dev Hardening` workspace with parent/child gate tasks.
- Verified `Run Tracking Operations` workspace remained intact.

## Files changed

- `/home/jon/.openclaw/openclaw.json`
- `/root/lien-automation-v2/MCP_GATE_PIPELINE_PLAN.md`
- `/root/lien-automation-v2/docs/CODEX_PLAN_TO_BOARD_RUNBOOK.md`

## Codex model path

Discovered models now include:
- `openai-direct/codex-mini-latest`
- `qwen-portal/coder-model` (current default)
- `qwen-portal/vision-model`

## How to use Codex in OpenClaw/Mission Control

1. Open Mission Control settings/model picker.
2. Select `openai-direct/codex-mini-latest` for the target agent/task.
3. Keep default as Qwen unless you intentionally want Codex-by-default.

## Board mapping

Workspace: `Internal Dev Hardening`

Parent cards:
- Gate 1: Pre-Run Health
- Gate 2: Chunk Integrity
- Gate 3: Post-Run Verification
- Checkpoint/Resume Reliability
- Run Summary and Reporting

Child cards include implementation, test coverage, and runtime validation for each gate area.

## Safety checks completed

- Existing cron schedule unchanged:
  - `0 13 * * * /root/lien-automation-v2-1/scripts/scheduled-ca-scraper.sh >> /var/log/lien-scraper.log 2>&1`
  - `0 20 * * * /root/lien-automation-v2-1/scripts/scheduled-ca-scraper.sh >> /var/log/lien-scraper.log 2>&1`
- `Run Tracking Operations` cards still present and active.
- OpenClaw remote model discovery reports Codex + Qwen models.

## Backup and rollback

Backup folder:
- `/root/lien-automation-v2/backups/rollout-2026-03-03`

Rollback steps:
1. Restore OpenClaw config backup:
   - `cp /root/lien-automation-v2/backups/rollout-2026-03-03/openclaw.json.bak /home/jon/.openclaw/openclaw.json`
2. Restart OpenClaw gateway service.
3. If needed, restore Mission Control env backup from same backup folder.

## Residual risk

- Codex provider uses `env:OPENAI_API_KEY`; ensure the key is available to OpenClaw runtime environment.
- If key is missing/invalid, model discovery may still list Codex but runtime calls can fail.

## Next recommended action

- Add `OPENAI_API_KEY` to the OpenClaw runtime environment and run a single Codex-backed test task in `Internal Dev Hardening` before making Codex default.
