# Multi-Agent Orchestration Assessment

## Executive Summary

Recommendation: **remain script-centric for now** and continue hardening the existing scheduler/queue architecture.

The current codebase already includes the core orchestration primitives (idempotency, retries/backoff, cooldown, connectivity probes, readiness checks, persisted run history, and anomaly alerting) that many teams introduce before considering a multi-agent layer.

## Current State (What Already Exists)

- Multi-site scraping API with direct and queued modes.
- External scheduler trigger surface (`POST /schedule/run`) with auth token support.
- Scheduler-level idempotency and cooldown controls.
- Retry attempts with failure-class handling and circuit-style retry blocking for site connectivity issues.
- Persisted run history and scheduler state in SQLite locally or Postgres when `DATABASE_URL` is set.
- Readiness endpoint that validates env/config/DB/OCR/runtime prerequisites.
- Queue + worker pipeline with claims, retries, and backoff.

## Why Multi-Agent Is Low ROI Right Now

1. **Primary pain is external instability and scrape runtime**, not orchestration feature gaps.
2. **Existing scheduler already behaves like a robust orchestrator** for current scope (`ca_sos`, `nyc_acris`).
3. **There is still consolidation work in health/readiness logic** worth finishing before introducing another abstraction layer.

## Recommended Path

### Near Term (Now)

- Keep single-orchestrator architecture.
- Prioritize:
  - readiness/health contract unification,
  - stronger operational alerts on retry exhaustion/failure classes,
  - continued scrape resilience improvements and resumability.

### Introduce Orchestration Layer Only If Triggers Appear

Adopt a thin orchestration framework (DAG/state machine) when one or more are true:

- 3+ independent scraping domains with materially different flows.
- Cross-site dependencies requiring dynamic run planning.
- Human-in-the-loop branching that is becoming brittle in imperative code.
- Need for horizontal fan-out with strict SLA coordination across workers.

## Decision

**Stay script-centric now.**

Re-evaluate after additional site expansion or when workflow coupling/coordination complexity materially increases.
