export type ApprovalStatus = 'pending' | 'approved' | 'rejected';
export type ApprovalGate = 'account' | 'contact' | 'draft' | 'send';
export type ApprovalDecision = Exclude<ApprovalStatus, 'pending'>;

export interface Account {
  id: string;
  source: string;
  displayName: string | null;
  externalReference: string | null;
  accountApproval: ApprovalStatus;
  contactApproval: ApprovalStatus;
  draftApproval: ApprovalStatus;
  sendApproval: ApprovalStatus;
  createdAt: Date;
  updatedAt: Date;
}

export interface CreateSourceIntakeAccountInput {
  source: string;
  displayName?: string;
  externalReference?: string;
}

export interface RecordApprovalInput {
  accountId: string;
  gate: ApprovalGate;
  decision: ApprovalDecision;
  actor: string;
}

/** The actionable CRM position for a record; it never authorizes a transition. */
export type StationQueueState =
  | 'station_1_account_review'
  | 'station_1_contact_review'
  | 'station_2_draft_review'
  | 'station_2_send_review'
  | 'outreach_authorized'
  | 'blocked_rejected';

export interface StationMetrics {
  totalAccounts: number;
  station1: {
    accountReviewPending: number;
    contactReviewPending: number;
  };
  station2: {
    draftReviewPending: number;
    sendReviewPending: number;
    outreachAuthorized: number;
  };
  blocked: {
    totalRejected: number;
    accountRejected: number;
    contactRejected: number;
    draftRejected: number;
    sendRejected: number;
  };
}

export interface StationQueueItem {
  account: Account;
  queueState: StationQueueState;
  nextGate: ApprovalGate | null;
}

export interface StationOverview {
  metrics: StationMetrics;
  reviewQueue: StationQueueItem[];
}

export type ApprovalSnapshot = Pick<
  Account,
  'accountApproval' | 'contactApproval' | 'draftApproval' | 'sendApproval'
>;

export interface CrmRepository {
  createSourceIntakeAccount(input: CreateSourceIntakeAccountInput): Promise<Account>;
  /** Latest source-intake accounts for the governed staff review queue. */
  listAccounts(limit: number): Promise<Account[]>;
  /** Authoritative aggregate counts for the Station 1–2 command center. */
  getStationMetrics(): Promise<StationMetrics>;
  getAccount(id: string): Promise<Account | null>;
  /**
   * Atomically verifies the approval snapshot, changes one pending gate, and
   * inserts the immutable audit record. False means another write won the race.
   */
  recordApproval(input: RecordApprovalInput & { expectedApprovals: ApprovalSnapshot }): Promise<boolean>;
  checkReady(): Promise<boolean>;
}
