# MCP Gate Pipeline Plan

This plan is derived from `PHASE2_PLAN.md` and organized for gate-driven execution, resilience, and reporting.

## Objectives

1. Move queue processing from monolithic runs to chunked execution.
2. Enforce gate checks so failed chunks are isolated and recoverable.
3. Add checkpoint/resume behavior to prevent long-run timeout loss.
4. Produce run-level and chunk-level reporting for operations visibility.

## Gate Model

### Gate 1: Pre-Run Health

Pass criteria:
- Queue database reachable.
- Required env vars present.
- Scraper dependencies available.
- External connectivity canary succeeds.

Fail action:
- Do not enqueue run.
- Emit `gate1_failed` event with failure reasons.

### Gate 2: Chunk Integrity

Pass criteria:
- Chunk definition valid (`chunk_id`, date window/count bounds).
- Expected record window is non-empty or explicitly marked empty.
- Chunk metadata persisted before processing starts.

Fail action:
- Mark chunk `failed` with `error_code` and `retry_count` increment.
- Route chunk to retry policy.

### Gate 3: Post-Run Verification

Pass criteria:
- `processed_count + failed_count == expected_count` (or explicit reconciliation rule).
- Upload/write acknowledgements complete.
- Run summary row persisted.

Fail action:
- Mark run `review` required.
- Emit verification failure event for board visibility.

## Schema and Data Contract

## Queue Jobs
Required fields:
- `chunk_id TEXT`
- `status TEXT DEFAULT 'pending'` with lifecycle: `pending | in_progress | done | failed`
- `error_code TEXT`
- `retry_count INTEGER DEFAULT 0`
- `last_attempt_at TIMESTAMP`
- `completed_at TIMESTAMP`

## Checkpoints
Purpose:
- Resume long scrapes without reprocessing completed ranges.

Columns:
- `id INTEGER PRIMARY KEY`
- `last_processed_id INTEGER`
- `last_processed_date TEXT`
- `chunk_size INTEGER DEFAULT 25`
- `updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

## Run Summaries
Purpose:
- Durable operational reporting.

Columns:
- `id INTEGER PRIMARY KEY`
- `chunk_id TEXT`
- `start_time TIMESTAMP`
- `end_time TIMESTAMP`
- `expected_count INTEGER`
- `processed_count INTEGER`
- `failed_count INTEGER`
- `timeout_count INTEGER`
- `summary_json TEXT`
- `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

## Execution Flow

1. Validate pre-run health (Gate 1).
2. Define chunks (count/date windows).
3. Persist chunk metadata and status `pending`.
4. For each chunk:
   - Set status `in_progress`
   - Execute scraper window
   - Apply chunk integrity checks (Gate 2)
   - Record retries/backoff when needed
5. Aggregate to run summary and verify (Gate 3).
6. Mark run/chunks complete and emit monitoring events.

## Retry and Timeout Policy

- Use bounded retries with exponential backoff + jitter.
- On timeout:
  - Increment `retry_count`
  - Set `error_code='timeout'`
  - Save `last_attempt_at`
- On max attempts reached:
  - Mark chunk `failed`
  - Require human review in board workflow.

## Checkpoint and Resume Rules

- Write checkpoint after each successfully persisted chunk.
- Resume from last checkpoint on restart/redeploy.
- Never reset completed chunk state unless explicit operator action.

## Reporting Outputs

Minimum reporting views:
- Per run: expected vs processed vs failed counts.
- Per chunk: status, retry_count, error_code, attempt timestamps.
- Timeout trend: count and affected chunks.

## Mission Control Mapping

Workspace: `Internal Dev Hardening`

Parent cards:
- Gate 1 Pre-Run Health
- Gate 2 Chunk Integrity
- Gate 3 Post-Run Verification
- Checkpoint/Resume Reliability
- Run Summary and Reporting

Child cards per parent:
- Implementation
- Test coverage
- Validation in staging/runtime

## Delivery Sequence

1. Apply schema updates.
2. Add checkpoint table and run summary table.
3. Implement chunk processor gate hooks.
4. Wire retry/timeout handling.
5. Add tests and verification scripts.
6. Publish reporting views and board mappings.
