# CRM Station 1–2 Command Center

## Purpose

The governed CRM is the authoritative tracking plane for Station 1 qualification and Station 2 proposal/send readiness. The command-center endpoint is intentionally **read-only**: it converts persisted approval state into an explicit review queue, but cannot approve, enrich, draft, export, or send anything.

## Read-only status contract

`GET /crm/stations/overview?limit=50`

Authentication: the existing CRM Bearer token is required. The endpoint remains fail-closed when the CRM database or token is not configured.

Response sections:

- `stations.metrics` — authoritative counts across the full CRM database.
  - Station 1: account-review and contact-review pending.
  - Station 2: draft-review pending, send-review pending, and records whose complete approval chain authorizes outreach.
  - Blocked: terminal rejections, including the gate at which each was rejected.
- `stations.reviewQueue` — newest records (up to `limit`, maximum 100), each with exactly one derived `queueState` and its `nextGate`.
- `governance.mode` — always `read_only`.
- `freshness.checked_at` — the UTC timestamp for the read.

Queue states are mutually exclusive:

| State | Meaning | Operator action |
| --- | --- | --- |
| `station_1_account_review` | New source intake awaits account fit decision. | Review account gate only. |
| `station_1_contact_review` | Account is approved; contact research may be considered. | Review contact-research gate only. |
| `station_2_draft_review` | Account and contact gates are approved; a draft may be reviewed. | Review draft gate only. |
| `station_2_send_review` | Draft is approved; final send decision awaits review. | Review send gate only. |
| `outreach_authorized` | All four approvals are persisted as approved. | This is a status signal, not an automatic send. |
| `blocked_rejected` | At least one approval is rejected. | Terminal; no subsequent action is available. |

## Approval boundary

Only `POST /crm/approvals` records a gate decision. The service enforces the linear order:

1. account approval
2. contact-research approval
3. draft approval
4. send approval

Every decision is irreversible and written to the append-only approval ledger. The overview endpoint performs no mutation and does not turn an `outreach_authorized` status into an external action.

## Activation and public operator-view gate

The API is currently loopback-only under the `plinko-crm` systemd service. The intended `crm-api.plinkosolutions.com` hostname must not be changed casually: at the time this document was written it resolved to a Vercel `DEPLOYMENT_NOT_FOUND` response, and the repository nginx example is not enabled on the host.

Before exposing a browser operator surface, require a reviewed change that:

1. Confirms the canonical hostname and its current owner.
2. Uses a credential-protected, server-side session or proxy; never embeds `CRM_API_TOKEN` in browser JavaScript.
3. Enables the exact nginx vhost only after DNS/certificate ownership is verified.
4. Deploys the isolated CRM bundle and takes a planned, service-only restart.
5. Verifies `/crm/stations/overview` with an authorized request, then verifies the public protected view and its freshness display.
6. Confirms that no read-only status surface exposes an approval or send control.

This preserves the intended morning review workflow: all pending gates remain pending until an operator makes an explicit recorded decision.
