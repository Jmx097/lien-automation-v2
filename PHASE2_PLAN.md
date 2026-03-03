# Phase 2 Plan (Operational Maturity)

**Last reviewed:** 2026-03-03 (UTC)  
**Owner:** Platform Engineering (Lien Automation v2)

## Context
The original Phase 2 draft focused on queue chunking schema changes and checkpoint tables. Since then, major scheduler and persistence work has already shipped (PRs #24, #26, #27, #28, #29, #31, #32, #33), so this plan is now reframed around operational maturity.

## Completed foundation items (already delivered)

- [x] External schedule trigger endpoint with idempotency controls (**PR #24**, commit `271d165`)
- [x] Scheduled run upload integration in execution lifecycle (**PR #26**, commit `75b5d60`)
- [x] Persisted schedule history + cooldown checks in SQLite (**PR #27**, commit `8445912`)
- [x] Production startup standard + schedule readiness checks (**PR #28**, commit `7bcf521`)
- [x] Scheduled run retry correctness after errors (**PR #29**, commit `3e7de4f`)
- [x] DB directory auto-creation for queue reliability (**PR #31**, commit `044315b`)
- [x] Last-7-days helper + SBR env loading fix (**PR #32**, commit `e59f3a5`)

## Active next milestone set

### Milestone A — Reliability hardening
**Goal:** Reduce failed/partial runs under unstable browser sessions.

- [ ] Define and enforce run-level retry budget and backoff policy for scheduler-triggered runs.
- [ ] Add deterministic failure classes (`network`, `selector`, `modal-timeout`, `upload`) for postmortems.
- [ ] Implement stop/resume guardrails for consecutive row failures with clear operator signals.
- [ ] Add regression tests around scheduler idempotency + retry interactions.

### Milestone B — Observability baseline
**Goal:** Make run health visible without code inspection.

- [ ] Standardize structured log fields across scrape, queue, and scheduler paths.
- [ ] Emit run summary counters (attempted, scraped, uploaded, failed, duration) per run.
- [ ] Publish a minimal operator runbook for interpreting `/schedule` and `/schedule/health` outputs.
- [ ] Add a lightweight error-rate and run-duration trend report script.

### Milestone C — Recovery workflows
**Goal:** Enable safe replay and manual repair when a run partially fails.

- [ ] Document and implement replay flow for failed scheduled runs with idempotency safety.
- [ ] Add “requeue failed subset” utility for queue-backed paths.
- [ ] Define manual recovery checklist for Sheets upload failures.
- [ ] Add validation check that compares scraped vs uploaded counts before closeout.

### Milestone D — OCR roadmap (conditional)
**Goal:** Decide whether OCR investment is justified for scanned CA SOS PDFs.

- [ ] Quantify current extraction gap from image-based PDFs (metadata-only vs required lien fields).
- [ ] Evaluate 1–2 OCR options (accuracy, runtime, cost) on a small labeled sample.
- [ ] Recommend go/no-go decision with success metrics and rollout constraints.

## Definition of done for this phase

Phase 2 is complete when:
1. Reliability + observability + recovery milestones have measurable acceptance criteria and passing checks.
2. Operator workflows are documented well enough to handle routine failures without source-code deep dives.
3. OCR direction is explicitly decided (adopt/defer) with rationale captured.

## Review cadence
- Weekly plan review during active implementation.
- Immediate update required after each merged PR that changes scheduler, queue, scraping reliability, or document extraction behavior.
