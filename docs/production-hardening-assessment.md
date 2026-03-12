# Production Hardening Assessment (CA SOS + NYC ACRIS)

## Scope

This assessment reviews the current workspace for reliability, data quality, and operational confidence for the two existing production sites:

- `ca_sos`
- `nyc_acris`

The goal is to identify what is already strong, what still introduces production risk, and what should be prioritized to reach consistently high-confidence runs.

---

## Executive Summary

The codebase already has several mature production controls (scheduler readiness checks, persisted schedule history, connectivity state machine, OCR runtime checks, and confidence scoring for extracted fields). The fastest path to materially higher reliability is **closing a few consistency and operations gaps**:

1. **Unify and modernize pre-run health checks** so they align with actual runtime env vars and execution model.
2. **Strengthen API input/runtime guardrails** (strict request schema validation + fail-fast dependency checks per route).
3. **Codify site-specific SLO/SLA signals** (success rate, row-level extraction confidence, retry behavior, and circuit/open recovery metrics).
4. **Harden operational runbooks with alert thresholds tied to scheduler/connectivity states** (not just ad hoc logs).

If these are addressed, the platform should move from “production-oriented and resilient” to “production-hardened with measurable confidence.”

---

## What is already strong

- Multi-site support is explicit and centralized (`ca_sos`, `nyc_acris`) with per-site export config controls.
- Scheduler readiness includes required env checks, credential parsing, DB reachability, and OCR runtime checks.
- Connectivity state for NYC ACRIS includes blocked/degraded/probing transitions with configurable thresholds and cooldown behavior.
- CA SOS extraction includes confidence scoring and OCR-backed fallback paths.
- The repository already includes local health/testing commands (`doctor`, typecheck, selector smoke) and broad unit/integration coverage directories.

These are good foundations for high-confidence production operations.

---

## Gaps that should be tightened for reliable, accurate runs

## 1) Config + health-check drift (high priority)

### Observed risk
The pre-run gate documents/checks older environment variable names and Docker assumptions that do not match the current runtime architecture and scheduler readiness behavior.

### Why this matters
When preflight checks drift from real runtime dependencies, you can get false positives (“looks healthy” but run fails) and false negatives (blocking healthy runs).

### Recommended tightening
- Replace legacy gate env checks with the same env contract used by schedule readiness and runtime routes.
- Make gate checks site-aware (CA SOS vs NYC ACRIS) so only relevant dependencies are required per run type.
- Add a single source of truth helper for required env vars used by:
  - `/schedule/health`
  - pre-run gate
  - deployment doctor scripts

---

## 2) API-level runtime safety (high priority)

### Observed risk
Route handlers perform manual shape checks but do not enforce a strict request schema with typed coercion and uniform validation errors.

### Why this matters
In production, malformed payloads and edge-case values are common and can create inconsistent behavior across routes (`/scrape`, `/enqueue`, `/scrape-enhanced`, `/schedule/run`).

### Recommended tightening
- Introduce shared request schema validation (e.g., zod or equivalent).
- Enforce date format and date-range validity (`date_start <= date_end`).
- Enforce bounded `max_records` with site-specific min/max safety ceilings.
- Return stable error shape for all 4xx validation failures.

---

## 3) Reliability engineering for site workflows (high priority)

### CA SOS

#### Observed risk
CA SOS uses complex drawer/modal + OCR extraction paths. While confidence scoring exists, there is no explicit production policy layer to act on low-confidence record batches.

#### Recommended tightening
- Define and enforce run-level confidence policy:
  - min acceptable median confidence
  - max low-confidence row percentage
  - fail/alert/quarantine behavior when thresholds are breached
- Emit explicit metrics for OCR status buckets (`ok`, `ocr_missing`, `ocr_no_text`, `ocr_error`) and extraction reason categories.
- Add scheduled “golden fixture replay” to detect selector drift before business-hour runs.

### NYC ACRIS

#### Observed risk
ACRIS has stronger connectivity state handling, but high-signal events (token/session issues, viewer roundtrip failures) are not yet converted into formal SLO alerts with escalation policy.

#### Recommended tightening
- Promote failure classes to alert dimensions with paging thresholds.
- Track blocked/degraded durations and recovery time as first-class KPIs.
- Add canary probes that run on a fixed cadence independent of full scrape windows.

---

## 4) Observability and confidence reporting (medium-high priority)

### Observed risk
There is good structured logging, but no single “confidence dashboard contract” that standardizes per-run KPIs for operators and downstream consumers.

### Recommended tightening
Create a run summary artifact per scheduled run with:

- site, window, trigger metadata
- records discovered vs records extracted
- per-field confidence distributions
- retry count + failure class sequence
- connectivity status transitions
- sheet export result and row mismatch stats

Persist this alongside scheduler history and expose via a lightweight read endpoint for ops dashboards.

---

## 5) Operational controls + incident readiness (medium priority)

### Observed risk
Runbooks exist, but practical incident workflows can still be tightened around triage speed and deterministic rollback/disable switches.

### Recommended tightening
- Add explicit “kill switches” per site and per downstream integration (scrape disable, sheet upload disable, OCR optionality) with audit logs.
- Document and test a one-command “safe mode” (probe-only, no export).
- Add post-incident template capturing failure class, blast radius, time to detect, time to recover, and permanent fix.

---

## Prioritized hardening roadmap

## Phase A (next 3–5 days)
- Unify env/dependency contract across pre-run gate, readiness endpoint, and doctor tooling.
- Add strict request schema validation to all write endpoints.
- Define and codify run acceptance thresholds (confidence + extraction completeness) for both sites.

## Phase B (next 1–2 weeks)
- Add run-summary persistence and dashboard endpoint.
- Add alerting on connectivity state transitions and prolonged blocked/degraded states.
- Implement deterministic canary probes for each site.

## Phase C (next 2–4 weeks)
- Add automated fixture replay suite (CA + NYC) in CI/nightly.
- Add safe-mode controls and incident playbook rehearsals.
- Add weekly drift report: selector stability, confidence trends, and failure class trends.

---

## Suggested “production confidence” scorecard

Track these as pass/fail gates for each scheduled run:

- **Availability**: run completed without fatal error.
- **Completeness**: extracted record count within expected variance from discovered result count.
- **Accuracy proxy**: confidence thresholds met for critical fields.
- **Stability**: retry/failure class volume within normal baseline.
- **Export integrity**: uploaded rows match accepted records.

A run is “high confidence” only if all gates pass.

---

## Immediate next actions (recommended)

1. Refactor `src/gates/pre-run-health.ts` + `src/gates/README.md` to align with current runtime env names and scheduler readiness semantics.
2. Add a shared request validation module and wire it into `/scrape`, `/enqueue`, `/scrape-enhanced`, and `/schedule/run`.
3. Add confidence policy thresholds and alert emissions for CA SOS and NYC ACRIS run summaries.
4. Expose run confidence summaries from scheduler history for operator dashboards.

