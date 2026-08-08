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

export type ApprovalSnapshot = Pick<
  Account,
  'accountApproval' | 'contactApproval' | 'draftApproval' | 'sendApproval'
>;

export interface CrmRepository {
  createSourceIntakeAccount(input: CreateSourceIntakeAccountInput): Promise<Account>;
  /** Latest source-intake accounts for the governed staff review queue. */
  listAccounts(limit: number): Promise<Account[]>;
  getAccount(id: string): Promise<Account | null>;
  /**
   * Atomically verifies the approval snapshot, changes one pending gate, and
   * inserts the immutable audit record. False means another write won the race.
   */
  recordApproval(input: RecordApprovalInput & { expectedApprovals: ApprovalSnapshot }): Promise<boolean>;
  checkReady(): Promise<boolean>;
}
