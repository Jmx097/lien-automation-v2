import { randomUUID } from "node:crypto";
import { Pool } from "pg";

export type CampaignChannel = "call" | "email" | "linkedin";
export type CampaignStatus = "draft" | "active" | "paused" | "closed";
export type Approval = "pending" | "approved" | "rejected";

function text(value: unknown, field: string, required = true): string | null {
  if (typeof value !== "string") {
    if (required) throw new CampaignWorkbenchError(`${field} is required`);
    return null;
  }
  const normalized = value.trim();
  if (!normalized && required)
    throw new CampaignWorkbenchError(`${field} is required`);
  return normalized || null;
}

function channel(value: unknown): CampaignChannel {
  if (value === "call" || value === "email" || value === "linkedin")
    return value;
  throw new CampaignWorkbenchError("channel must be call, email, or linkedin");
}

export class CampaignWorkbenchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CampaignWorkbenchError";
  }
}
export class CampaignWorkbenchNotFoundError extends CampaignWorkbenchError {
  constructor(message: string) {
    super(message);
    this.name = "CampaignWorkbenchNotFoundError";
  }
}

export class CampaignWorkbench {
  constructor(private readonly pool: Pool) {}
  static fromConnectionString(connectionString: string): CampaignWorkbench {
    return new CampaignWorkbench(new Pool({ connectionString }));
  }

  private async event(
    actor: string,
    type: string,
    entityType: string,
    entityId: string,
    ids: { accountId?: string; contactId?: string; campaignId?: string },
    metadata: Record<string, unknown> = {},
  ): Promise<void> {
    await this.pool.query(
      `INSERT INTO crm_activity_events (id, actor, event_type, entity_type, entity_id, account_id, contact_id, campaign_id, metadata)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)`,
      [
        randomUUID(),
        actor,
        type,
        entityType,
        entityId,
        ids.accountId ?? null,
        ids.contactId ?? null,
        ids.campaignId ?? null,
        JSON.stringify(metadata),
      ],
    );
  }

  async createContact(input: Record<string, unknown>, actor: string) {
    const accountId = text(input.account_id, "account_id")!;
    const account = await this.pool.query(
      "SELECT id FROM crm_accounts WHERE id = $1",
      [accountId],
    );
    if (!account.rowCount)
      throw new CampaignWorkbenchNotFoundError("Account was not found");
    const id = randomUUID();
    const result = await this.pool.query(
      `INSERT INTO crm_contacts (id, account_id, source, external_reference, full_name, title, email, phone, linkedin_url)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [
        id,
        accountId,
        text(input.source, "source")!,
        text(input.external_reference, "external_reference", false),
        text(input.full_name, "full_name")!,
        text(input.title, "title", false),
        text(input.email, "email", false),
        text(input.phone, "phone", false),
        text(input.linkedin_url, "linkedin_url", false),
      ],
    );
    await this.event(
      actor,
      "contact.intake_created",
      "contact",
      id,
      { accountId, contactId: id },
      { source: result.rows[0].source },
    );
    return result.rows[0];
  }

  async decideContact(contactId: string, decision: Approval, actor: string) {
    if (decision !== "approved" && decision !== "rejected")
      throw new CampaignWorkbenchError("decision must be approved or rejected");
    const result = await this.pool.query(
      `UPDATE crm_contacts c SET contact_approval=$2, disposition=CASE WHEN $2='approved' THEN 'active' ELSE disposition END, updated_at=NOW()
       FROM crm_accounts a WHERE c.id=$1 AND a.id=c.account_id AND a.account_approval='approved' AND c.contact_approval='pending'
       RETURNING c.*`,
      [contactId, decision],
    );
    if (!result.rowCount)
      throw new CampaignWorkbenchError(
        "Contact approval requires an approved account and a pending contact decision",
      );
    await this.event(
      actor,
      `contact.${decision}`,
      "contact",
      contactId,
      { accountId: result.rows[0].account_id, contactId },
      {},
    );
    return result.rows[0];
  }

  async setDisposition(contactId: string, disposition: unknown, actor: string) {
    if (
      !["review", "active", "do_not_contact", "invalid"].includes(
        String(disposition),
      )
    )
      throw new CampaignWorkbenchError("Invalid contact disposition");
    const result = await this.pool.query(
      "UPDATE crm_contacts SET disposition=$2, updated_at=NOW() WHERE id=$1 RETURNING *",
      [contactId, disposition],
    );
    if (!result.rowCount)
      throw new CampaignWorkbenchNotFoundError("Contact was not found");
    await this.event(
      actor,
      "contact.disposition_changed",
      "contact",
      contactId,
      { accountId: result.rows[0].account_id, contactId },
      { disposition },
    );
    return result.rows[0];
  }

  async createCampaign(input: Record<string, unknown>, actor: string) {
    const id = randomUUID();
    const result = await this.pool.query(
      `INSERT INTO crm_campaigns (id, name, channel, purpose) VALUES ($1,$2,$3,$4) RETURNING *`,
      [
        id,
        text(input.name, "name")!,
        channel(input.channel),
        text(input.purpose, "purpose", false),
      ],
    );
    await this.event(
      actor,
      "campaign.created",
      "campaign",
      id,
      { campaignId: id },
      { channel: result.rows[0].channel },
    );
    return result.rows[0];
  }

  async addMember(campaignId: string, contactId: string, actor: string) {
    const campaign = await this.pool.query(
      "SELECT id FROM crm_campaigns WHERE id=$1 AND status IN ('draft','active')",
      [campaignId],
    );
    if (!campaign.rowCount)
      throw new CampaignWorkbenchError(
        "Campaign was not found or cannot accept members",
      );
    const id = randomUUID();
    try {
      const result = await this.pool.query(
        `INSERT INTO crm_campaign_memberships (id,campaign_id,contact_id) VALUES ($1,$2,$3) RETURNING *`,
        [id, campaignId, contactId],
      );
      const contact = await this.pool.query(
        "SELECT account_id FROM crm_contacts WHERE id=$1",
        [contactId],
      );
      await this.event(
        actor,
        "campaign.member_added",
        "membership",
        id,
        { accountId: contact.rows[0]?.account_id, contactId, campaignId },
        {},
      );
      return result.rows[0];
    } catch (error) {
      if ((error as { code?: string }).code === "23505")
        throw new CampaignWorkbenchError("Contact is already in this campaign");
      throw error;
    }
  }

  async createDraft(membershipId: string, content: unknown, actor: string) {
    if (!content || typeof content !== "object" || Array.isArray(content))
      throw new CampaignWorkbenchError("content must be a JSON object");
    const membership = await this.pool.query(
      `SELECT m.*, c.account_id, c.id AS contact_id FROM crm_campaign_memberships m JOIN crm_contacts c ON c.id=m.contact_id WHERE m.id=$1 AND m.status='queued'`,
      [membershipId],
    );
    if (!membership.rowCount)
      throw new CampaignWorkbenchError(
        "Queued campaign membership was not found",
      );
    const next = await this.pool.query<{ revision: number }>(
      "SELECT COALESCE(MAX(revision),0)+1 AS revision FROM crm_outreach_drafts WHERE membership_id=$1",
      [membershipId],
    );
    const id = randomUUID();
    const revision = next.rows[0].revision;
    const result = await this.pool.query(
      `INSERT INTO crm_outreach_drafts (id,membership_id,revision,content) VALUES ($1,$2,$3,$4::jsonb) RETURNING *`,
      [id, membershipId, revision, JSON.stringify(content)],
    );
    await this.event(
      actor,
      "draft.created",
      "draft",
      id,
      {
        accountId: membership.rows[0].account_id,
        contactId: membership.rows[0].contact_id,
        campaignId: membership.rows[0].campaign_id,
      },
      { revision },
    );
    return result.rows[0];
  }

  async decideDraft(draftId: string, decision: Approval, actor: string) {
    if (decision !== "approved" && decision !== "rejected")
      throw new CampaignWorkbenchError("decision must be approved or rejected");
    const result = await this.pool.query(
      `UPDATE crm_outreach_drafts SET approval=$2 WHERE id=$1 AND approval='pending' RETURNING *`,
      [draftId, decision],
    );
    if (!result.rowCount)
      throw new CampaignWorkbenchError(
        "Draft was not found or already decided",
      );
    const ctx = await this.contextForDraft(draftId);
    await this.event(actor, `draft.${decision}`, "draft", draftId, ctx, {
      revision: result.rows[0].revision,
    });
    return result.rows[0];
  }

  async createAttempt(membershipId: string, draftId: string, actor: string) {
    const id = randomUUID();
    const result = await this.pool.query(
      `INSERT INTO crm_outreach_attempts (id,membership_id,draft_id) VALUES ($1,$2,$3) RETURNING *`,
      [id, membershipId, draftId],
    );
    const ctx = await this.contextForDraft(draftId);
    await this.event(actor, "attempt.planned", "attempt", id, ctx, {
      draft_id: draftId,
    });
    return result.rows[0];
  }

  async decideAttempt(attemptId: string, decision: Approval, actor: string) {
    if (decision !== "approved" && decision !== "rejected")
      throw new CampaignWorkbenchError("decision must be approved or rejected");
    const result = await this.pool.query(
      `UPDATE crm_outreach_attempts SET approval=$2,status=CASE WHEN $2='approved' THEN 'approved_to_execute' ELSE 'cancelled' END,updated_at=NOW() WHERE id=$1 AND approval='pending' RETURNING *`,
      [attemptId, decision],
    );
    if (!result.rowCount)
      throw new CampaignWorkbenchError(
        "Attempt was not found or already decided",
      );
    const ctx = await this.contextForAttempt(attemptId);
    await this.event(
      actor,
      `attempt.${decision}`,
      "attempt",
      attemptId,
      ctx,
      {},
    );
    return result.rows[0];
  }

  async recordManualExecution(
    attemptId: string,
    outcomeNote: unknown,
    actor: string,
  ) {
    const note = text(outcomeNote, "outcome_note")!;
    const result = await this.pool.query(
      `UPDATE crm_outreach_attempts SET status='executed_manually',outcome_note=$2,executed_at=NOW(),executed_by=$3,updated_at=NOW() WHERE id=$1 AND approval='approved' AND status='approved_to_execute' RETURNING *`,
      [attemptId, note, actor],
    );
    if (!result.rowCount)
      throw new CampaignWorkbenchError(
        "Manual execution requires an approved, unexecuted attempt",
      );
    const ctx = await this.contextForAttempt(attemptId);
    await this.event(
      actor,
      "attempt.executed_manually",
      "attempt",
      attemptId,
      ctx,
      { outcome_note: note },
    );
    return result.rows[0];
  }

  async overview() {
    const [campaigns, contacts, activity] = await Promise.all([
      this.pool.query(
        `SELECT c.*, COUNT(m.id)::int AS member_count FROM crm_campaigns c LEFT JOIN crm_campaign_memberships m ON m.campaign_id=c.id GROUP BY c.id ORDER BY c.created_at DESC`,
      ),
      this.pool.query(
        `SELECT c.*, a.display_name AS account_name FROM crm_contacts c JOIN crm_accounts a ON a.id=c.account_id ORDER BY c.created_at DESC LIMIT 100`,
      ),
      this.pool.query(
        `SELECT * FROM crm_activity_events ORDER BY occurred_at DESC LIMIT 100`,
      ),
    ]);
    return {
      campaigns: campaigns.rows,
      contacts: contacts.rows,
      activity: activity.rows,
      freshness: { checked_at: new Date().toISOString() },
      governance: {
        mode: "manual_only",
        delivery: "No provider action or automatic outreach is available.",
      },
    };
  }

  private async contextForDraft(draftId: string) {
    const r = await this.pool.query(
      `SELECT c.account_id,c.id contact_id,m.campaign_id FROM crm_outreach_drafts d JOIN crm_campaign_memberships m ON m.id=d.membership_id JOIN crm_contacts c ON c.id=m.contact_id WHERE d.id=$1`,
      [draftId],
    );
    if (!r.rowCount)
      throw new CampaignWorkbenchNotFoundError("Draft was not found");
    return {
      accountId: r.rows[0].account_id,
      contactId: r.rows[0].contact_id,
      campaignId: r.rows[0].campaign_id,
    };
  }
  private async contextForAttempt(attemptId: string) {
    const r = await this.pool.query(
      `SELECT c.account_id,c.id contact_id,m.campaign_id FROM crm_outreach_attempts a JOIN crm_campaign_memberships m ON m.id=a.membership_id JOIN crm_contacts c ON c.id=m.contact_id WHERE a.id=$1`,
      [attemptId],
    );
    if (!r.rowCount)
      throw new CampaignWorkbenchNotFoundError("Attempt was not found");
    return {
      accountId: r.rows[0].account_id,
      contactId: r.rows[0].contact_id,
      campaignId: r.rows[0].campaign_id,
    };
  }
}
