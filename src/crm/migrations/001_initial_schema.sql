CREATE TABLE IF NOT EXISTS crm_accounts (
  id UUID PRIMARY KEY,
  source TEXT NOT NULL CHECK (length(trim(source)) > 0),
  intake_type TEXT NOT NULL DEFAULT 'source_intake' CHECK (intake_type = 'source_intake'),
  display_name TEXT,
  external_reference TEXT,
  account_approval TEXT NOT NULL DEFAULT 'pending' CHECK (account_approval IN ('pending', 'approved', 'rejected')),
  contact_approval TEXT NOT NULL DEFAULT 'pending' CHECK (contact_approval IN ('pending', 'approved', 'rejected')),
  draft_approval TEXT NOT NULL DEFAULT 'pending' CHECK (draft_approval IN ('pending', 'approved', 'rejected')),
  send_approval TEXT NOT NULL DEFAULT 'pending' CHECK (send_approval IN ('pending', 'approved', 'rejected')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crm_approval_decisions (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES crm_accounts(id),
  gate TEXT NOT NULL CHECK (gate IN ('account', 'contact', 'draft', 'send')),
  decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected')),
  actor TEXT NOT NULL CHECK (length(trim(actor)) > 0),
  decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS crm_approval_decisions_account_decided_idx
  ON crm_approval_decisions (account_id, decided_at);

CREATE OR REPLACE FUNCTION crm_enforce_account_approval_transition()
RETURNS TRIGGER AS $$
BEGIN
  -- Source intake is never allowed to self-promote an account.
  IF TG_OP = 'INSERT' AND (NEW.account_approval <> 'pending' OR NEW.contact_approval <> 'pending' OR NEW.draft_approval <> 'pending' OR NEW.send_approval <> 'pending') THEN
    RAISE EXCEPTION 'CRM accounts must begin with all approvals pending';
  END IF;

  IF TG_OP = 'UPDATE' THEN
    IF (OLD.account_approval <> 'pending' AND NEW.account_approval <> OLD.account_approval)
       OR (OLD.contact_approval <> 'pending' AND NEW.contact_approval <> OLD.contact_approval)
       OR (OLD.draft_approval <> 'pending' AND NEW.draft_approval <> OLD.draft_approval)
       OR (OLD.send_approval <> 'pending' AND NEW.send_approval <> OLD.send_approval) THEN
      RAISE EXCEPTION 'CRM approval decisions are irreversible';
    END IF;
    IF NEW.contact_approval <> OLD.contact_approval AND OLD.account_approval <> 'approved' THEN
      RAISE EXCEPTION 'contact approval requires approved account';
    END IF;
    IF NEW.draft_approval <> OLD.draft_approval AND (OLD.account_approval <> 'approved' OR OLD.contact_approval <> 'approved') THEN
      RAISE EXCEPTION 'draft approval requires approved account and contact';
    END IF;
    IF NEW.send_approval <> OLD.send_approval AND (OLD.account_approval <> 'approved' OR OLD.contact_approval <> 'approved' OR OLD.draft_approval <> 'approved') THEN
      RAISE EXCEPTION 'send approval requires approved account, contact and draft';
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS crm_accounts_approval_transition ON crm_accounts;
CREATE TRIGGER crm_accounts_approval_transition
  BEFORE INSERT OR UPDATE ON crm_accounts
  FOR EACH ROW EXECUTE FUNCTION crm_enforce_account_approval_transition();

CREATE OR REPLACE FUNCTION crm_enforce_approval_decision_consistency()
RETURNS TRIGGER AS $$
DECLARE current_decision TEXT;
BEGIN
  SELECT CASE NEW.gate
    WHEN 'account' THEN account_approval WHEN 'contact' THEN contact_approval
    WHEN 'draft' THEN draft_approval WHEN 'send' THEN send_approval END
    INTO current_decision FROM crm_accounts WHERE id = NEW.account_id;
  IF current_decision IS DISTINCT FROM NEW.decision THEN
    RAISE EXCEPTION 'approval audit decision must match current account state';
  END IF;
  IF EXISTS (SELECT 1 FROM crm_approval_decisions WHERE account_id = NEW.account_id AND gate = NEW.gate) THEN
    RAISE EXCEPTION 'only one approval decision is allowed per account gate';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER crm_approval_decisions_consistent
  BEFORE INSERT ON crm_approval_decisions
  FOR EACH ROW EXECUTE FUNCTION crm_enforce_approval_decision_consistency();

CREATE OR REPLACE FUNCTION crm_reject_approval_decision_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'crm_approval_decisions is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS crm_approval_decisions_no_update ON crm_approval_decisions;
CREATE TRIGGER crm_approval_decisions_no_update
  BEFORE UPDATE OR DELETE ON crm_approval_decisions
  FOR EACH ROW EXECUTE FUNCTION crm_reject_approval_decision_mutation();
