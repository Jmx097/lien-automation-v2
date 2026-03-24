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

## Workspace evidence snapshot (March 24, 2026 UTC)

The current local workspace can only support a **code-readiness diagnosis**, not a production-performance diagnosis, because no scheduler run history is present in the local SQLite scheduler store.

- `scripts/analyze-tri-site-strength.ts` computes per-site confidence from persisted scheduler history (`ScheduledRunStore#getRunHistory`) and applies:
  - Success threshold
  - Cadence threshold for 3 weekday slots/day
  - Quality pass rule: amount coverage >=95, row fail <=5, retry not exhausted
- Running the script in this workspace reports `insufficient_data` for all three sites because `total_runs=0` for each site in the lookback window.

Interpretation:

- The scheduler implementation is structurally ready for 3x/day across CA SOS, NYC ACRIS, and Maricopa.
- This specific workspace cannot prove or disprove 95% real-world accuracy without importing historical run records (or running live canaries and persisting results first).

## Deep-dive evidence matrix (what is strong vs. what is still risky)

### Shared scheduler backbone (all three sites)

Strength signals:

- Three slots are first-class scheduler concepts (`morning`, `afternoon`, `evening`), with per-site env-driven hours and weekdays.
- Retry budget defaults to 3 attempts.
- Connectivity state machine supports degrade/block/probe recovery with probe success requirements.
- Quality anomaly thresholds are present for records, amount coverage, OCR success, and row-fail drift.

Residual risk:

- “95” is currently an amount-coverage threshold, not a single SLA contract for end-to-end accuracy.

### CA SOS

Why this is the strongest implementation:

- Dedicated lead-time trigger support (`TRIGGER_LEAD_MINUTES`) tuned for CA.
- Confidence and anomaly machinery applies cleanly to CA scheduled runs.
- Auto-throttle behavior intentionally avoids aggressive CA down-throttling in tests.

Primary remaining risk:

- CA PDFs are image-heavy, so amount extraction quality still depends on OCR coverage and parser confidence, not deterministic text extraction.

### NYC ACRIS

Why it is medium-strong:

- Extensive bootstrap/viewer/session diagnostics and failure-class mapping exist (policy, transport/bootstrap, token/session, viewer roundtrip, range integrity).
- Connectivity classification feeds circuit-break/probe behavior.
- Session budget and staged validation probes exist to detect instability earlier.

Primary remaining risks:

- External anti-bot/policy volatility can still force blocked/degraded periods.
- Viewer roundtrip and token/session churn can impact run consistency during peak windows.

### Maricopa Recorder

Why it is medium (not strong by default):

- Artifact retrieval is explicit and quality-aware.
- Persisted readiness checks require both session freshness and artifact candidates.
- Failure classes include immediate block conditions (stale/missing session, missing candidates, challenge/interstitial, OCR runtime unavailable).

Primary remaining risks:

- If refresh/discovery cadences are not maintained, readiness can fail before scrape quality is evaluated.
- Artifact/OCR path is operationally sensitive (session freshness + runtime dependencies + challenge behavior).

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

## No-back-and-forth operator plan (implement once, monitor daily)

1. **Define one enforceable SLA metric now**:
   - `accuracy_sla = weighted( amount_coverage_pct, (100-row_fail_pct), ocr_success_pct, upload_match_rate )`
   - Mark run failed-for-sla if `accuracy_sla < 95`, even when scrape technically succeeded.
2. **Add rolling compliance guardrail**:
   - Site considered healthy only if `>=95%` SLA on last 20 successful runs.
   - Emit alert when any site drops below guardrail.
3. **Lock 3x/day proof requirement**:
   - Daily assertion: each site has successful `morning+afternoon+evening` slots for previous business day.
4. **Harden Maricopa ops**:
   - Schedule automatic `refresh:maricopa-session` and `discover:maricopa-live`.
   - Fail fast when freshness/candidate age exceeds policy.
5. **Keep NYC recovery tight**:
   - Treat repeated `policy_block` and `transport_or_bootstrap` as immediate operator page events after threshold.
6. **Weekly evidence package**:
   - Export `/schedule/health`, `/schedule/confidence?limit=60`, and recent run history JSON into an auditable artifact.

## Minimal operator checks (daily)

1. `GET /schedule/health` must be `ready` and show no blocked site connectivity.
2. `GET /schedule/confidence?limit=60` should show mostly `high` or acceptable `medium` reasons.
3. Verify each site has three successful runs in the prior business day (`morning`, `afternoon`, `evening`).
4. For Maricopa, confirm persisted session freshness and artifact candidates presence.

## Practical interpretation

- If the question is “can the current workspace support 3x/day pulls?” -> **Yes, structurally yes for all three sites**.
- If the question is “is 95% end-to-end accuracy guaranteed today?” -> **Not explicitly guaranteed**; only parts are encoded (notably 95% amount coverage), and true SLA-level enforcement still needs a unified accuracy contract.
