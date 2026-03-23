# Tri-site schedule strength diagnosis (CA SOS, NYC ACRIS, Maricopa)

## Scope

This diagnosis evaluates whether the current scheduler implementation can sustain **3 runs/day per site** with an operational target near **95% quality accuracy**.

“Strength” is assessed on four dimensions:

1. Schedule coverage (can each site run morning/afternoon/evening)
2. Reliability controls (retry/circuit-breaker/probe behavior)
3. Quality controls (confidence gates, anomaly checks, review quarantine)
4. Observability (health + confidence + persisted history)

## Findings summary

- **CA SOS: Strong** for 3x/day operation.
  - Has site-specific morning/afternoon/evening schedule config and trigger lead-time support.
  - Uses retries, run confidence scoring, and anomaly detection.
  - Dynamic cap behavior is CA-specific and avoids aggressive auto-throttling.

- **NYC ACRIS: Medium-Strong** for 3x/day operation.
  - Full 3x/day scheduling support and confidence scoring exist.
  - Includes robust connectivity classification and circuit-breaker/probing flow for transport/policy/session/viewer failures.
  - Most risk remains external (transport policy blocking, session/token churn).

- **Maricopa Recorder: Medium** (can be Strong only when artifact/session state is actively maintained).
  - Full 3x/day scheduling support exists.
  - Confidence flags artifact retrieval disabled as a quality reason.
  - Readiness hard-fails when artifact retrieval is enabled but session/discovery state is stale or missing.
  - Operationally strongest when refresh/discovery workflows are run regularly.

## Why 3x/day is structurally supported

- The scheduler defines three slots (`morning`, `afternoon`, `evening`) per site and computes trigger and finish-by times.
- Cloud scheduler specs explicitly generate 9 jobs total (3 sites x 3 slots), with weekday defaults.

## Why “95% accuracy” is only partially encoded today

- The scheduler currently treats **95** as `AMOUNT_MIN_COVERAGE_PCT` default, i.e., amount-field coverage threshold, not a universal record-accuracy metric.
- Confidence status can still be `medium`/`high` depending on other evidence and reasons.
- Director publish thresholds default to 0.85 accept / 0.75 review; this is separate from the 95% amount coverage target.

## Reliability and quality controls in place

- Retry budget defaults to 3 attempts with bounded backoff.
- Connectivity state machine supports `healthy`, `degraded`, `blocked`, `probing`.
- Site-specific failure classification drives block/degrade/probe decisions.
- Quality anomaly detection compares successful runs against a rolling baseline.
- `/schedule/health`, `/schedule`, and `/schedule/confidence` expose readiness, state/history, and confidence reasons.

## Diagnosis scorecard (code-level readiness)

| Site | 3x/day schedule support | Reliability controls | Quality controls | Net strength |
|---|---|---|---|---|
| CA SOS | High | High | High | **Strong** |
| NYC ACRIS | High | High | Medium-High | **Medium-Strong** |
| Maricopa | High | Medium-High | Medium (depends on artifact/session freshness) | **Medium** |

## Gaps to close for true “95% accuracy” SLA

1. Define a single explicit SLA metric in code (for example, weighted composite from amount coverage + row failure + OCR success + review conflict rate).
2. Add per-site rolling compliance KPI at scheduler level (e.g., “>=95% SLA for last 20 successful runs”).
3. Fail/alert on SLA breach directly, not only on anomaly deltas.
4. Add automatic Maricopa refresh/discovery recertification cadence and age-based enforcement for artifact candidates.

## Minimal operator checks (daily)

1. `GET /schedule/health` must be `ready` and show no blocked site connectivity.
2. `GET /schedule/confidence?limit=60` should show mostly `high` or acceptable `medium` reasons.
3. Verify each site has three successful runs in the prior business day (`morning`, `afternoon`, `evening`).
4. For Maricopa, confirm persisted session freshness and artifact candidates presence.

## Practical interpretation

- If the question is “can the current workspace support 3x/day pulls?” -> **Yes, structurally yes for all three sites**.
- If the question is “is 95% end-to-end accuracy guaranteed today?” -> **Not explicitly guaranteed**; only parts are encoded (notably 95% amount coverage), and true SLA-level enforcement still needs a unified accuracy contract.
## Repeatable re-analysis command

Use the new CLI analyzer to compute a fresh per-site score from persisted scheduler history:

```bash
npx ts-node scripts/analyze-tri-site-strength.ts
```

Optional tuning:

- `TRI_SITE_LOOKBACK_DAYS` (default `14`)
- `TRI_SITE_HISTORY_LIMIT` (default `1000`)

The analyzer reports:

- success rate
- weekday slot cadence hit rate (3 slots/day baseline)
- quality pass rate (`amount_coverage_pct >= 95`, `row_fail_pct <= 5`, and no retry exhaustion)
- averages for amount coverage / OCR success / row failure
- derived confidence label (`strong` / `medium` / `weak` / `insufficient_data`)
