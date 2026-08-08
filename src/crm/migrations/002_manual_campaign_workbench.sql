-- Additive manual-only campaign workbench. No provider credentials, queue workers, or delivery functions.

CREATE TABLE IF NOT EXISTS crm_contacts (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES crm_accounts(id),
  source TEXT NOT NULL CHECK (length(trim(source)) > 0),
  external_reference TEXT,
  full_name TEXT NOT NULL CHECK (length(trim(full_name)) > 0),
  title TEXT,
  email TEXT,
  phone TEXT,
  linkedin_url TEXT,
  contact_approval TEXT NOT NULL DEFAULT 'pending' CHECK (contact_approval IN ('pending', 'approved', 'rejected')),
  disposition TEXT NOT NULL DEFAULT 'review' CHECK (disposition IN ('review', 'active', 'do_not_contact', 'invalid')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS crm_contacts_account_created_idx ON crm_contacts (account_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS crm_contacts_source_reference_unique
  ON crm_contacts (account_id, source, external_reference) WHERE external_reference IS NOT NULL;

CREATE TABLE IF NOT EXISTS crm_campaigns (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL CHECK (length(trim(name)) > 0),
  channel TEXT NOT NULL CHECK (channel IN ('call', 'email', 'linkedin')),
  status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'paused', 'closed')),
  purpose TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crm_campaign_memberships (
  id UUID PRIMARY KEY,
  campaign_id UUID NOT NULL REFERENCES crm_campaigns(id),
  contact_id UUID NOT NULL REFERENCES crm_contacts(id),
  status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'paused', 'completed', 'suppressed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (campaign_id, contact_id)
);
CREATE INDEX IF NOT EXISTS crm_campaign_memberships_campaign_idx ON crm_campaign_memberships (campaign_id, created_at DESC);

CREATE TABLE IF NOT EXISTS crm_outreach_drafts (
  id UUID PRIMARY KEY,
  membership_id UUID NOT NULL REFERENCES crm_campaign_memberships(id),
  revision INTEGER NOT NULL CHECK (revision > 0),
  content JSONB NOT NULL,
  approval TEXT NOT NULL DEFAULT 'pending' CHECK (approval IN ('pending', 'approved', 'rejected')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (membership_id, revision)
);

CREATE TABLE IF NOT EXISTS crm_outreach_attempts (
  id UUID PRIMARY KEY,
  membership_id UUID NOT NULL REFERENCES crm_campaign_memberships(id),
  draft_id UUID NOT NULL REFERENCES crm_outreach_drafts(id),
  approval TEXT NOT NULL DEFAULT 'pending' CHECK (approval IN ('pending', 'approved', 'rejected')),
  status TEXT NOT NULL DEFAULT 'planned' CHECK (status IN ('planned', 'approved_to_execute', 'executed_manually', 'failed', 'cancelled')),
  outcome_note TEXT,
  executed_at TIMESTAMPTZ,
  executed_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crm_activity_events (
  id UUID PRIMARY KEY,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  actor TEXT NOT NULL CHECK (length(trim(actor)) > 0),
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id UUID NOT NULL,
  account_id UUID REFERENCES crm_accounts(id),
  contact_id UUID REFERENCES crm_contacts(id),
  campaign_id UUID REFERENCES crm_campaigns(id),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS crm_activity_events_account_idx ON crm_activity_events (account_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS crm_activity_events_campaign_idx ON crm_activity_events (campaign_id, occurred_at DESC);

CREATE OR REPLACE FUNCTION crm_reject_activity_mutation()
RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'crm_activity_events is append-only'; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS crm_activity_events_no_mutation ON crm_activity_events;
CREATE TRIGGER crm_activity_events_no_mutation BEFORE UPDATE OR DELETE ON crm_activity_events
FOR EACH ROW EXECUTE FUNCTION crm_reject_activity_mutation();

-- Campaign work cannot be created for unapproved account/contact or DNC records.
CREATE OR REPLACE FUNCTION crm_validate_campaign_membership()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM crm_contacts c JOIN crm_accounts a ON a.id = c.account_id
    WHERE c.id = NEW.contact_id AND a.account_approval = 'approved'
      AND c.contact_approval = 'approved' AND c.disposition = 'active'
  ) THEN RAISE EXCEPTION 'campaign membership requires approved active contact under approved account'; END IF;
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS crm_campaign_membership_governance ON crm_campaign_memberships;
CREATE TRIGGER crm_campaign_membership_governance BEFORE INSERT OR UPDATE ON crm_campaign_memberships
FOR EACH ROW EXECUTE FUNCTION crm_validate_campaign_membership();

CREATE OR REPLACE FUNCTION crm_validate_outreach_attempt()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM crm_outreach_drafts d WHERE d.id = NEW.draft_id AND d.membership_id = NEW.membership_id AND d.approval = 'approved') THEN
    RAISE EXCEPTION 'manual attempt requires the exact approved draft revision';
  END IF;
  IF NEW.status = 'executed_manually' AND (NEW.approval <> 'approved' OR NEW.executed_at IS NULL OR NEW.executed_by IS NULL) THEN
    RAISE EXCEPTION 'manual execution requires approved attempt and operator attribution';
  END IF;
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS crm_outreach_attempt_governance ON crm_outreach_attempts;
CREATE TRIGGER crm_outreach_attempt_governance BEFORE INSERT OR UPDATE ON crm_outreach_attempts
FOR EACH ROW EXECUTE FUNCTION crm_validate_outreach_attempt();
