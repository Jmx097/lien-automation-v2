import type {
  Account,
  ApprovalDecision,
  ApprovalGate,
  ApprovalSnapshot,
  CreateSourceIntakeAccountInput,
  CrmRepository,
  RecordApprovalInput,
  StationOverview,
  StationQueueItem,
} from './types';

const GATE_PREREQUISITES: Record<ApprovalGate, Array<keyof ApprovalSnapshot>> = {
  account: [],
  contact: ['accountApproval'],
  draft: ['accountApproval', 'contactApproval'],
  send: ['accountApproval', 'contactApproval', 'draftApproval'],
};

const GATE_FIELD: Record<ApprovalGate, keyof ApprovalSnapshot> = {
  account: 'accountApproval',
  contact: 'contactApproval',
  draft: 'draftApproval',
  send: 'sendApproval',
};

export class CrmNotFoundError extends Error {
  constructor() {
    super('Account was not found');
    this.name = 'CrmNotFoundError';
  }
}

export class CrmGateViolationError extends Error {
  constructor(message = 'Approval gate progression is not allowed') {
    super(message);
    this.name = 'CrmGateViolationError';
  }
}

function requiredText(value: string | undefined, field: string): string {
  const normalized = value?.trim();
  if (!normalized) throw new CrmGateViolationError(`${field} is required`);
  return normalized;
}

export class GovernedCrmService {
  constructor(private readonly repository: CrmRepository) {}

  async createSourceIntakeAccount(input: CreateSourceIntakeAccountInput): Promise<Account> {
    return this.repository.createSourceIntakeAccount({
      source: requiredText(input.source, 'source'),
      displayName: input.displayName?.trim() || undefined,
      externalReference: input.externalReference?.trim() || undefined,
    });
  }

  async listAccounts(limit = 100): Promise<Account[]> {
    return this.repository.listAccounts(Math.min(Math.max(limit, 1), 100));
  }

  /**
   * Read-only tracking view for Stations 1 and 2. This derives the next review
   * from persisted approvals; it never changes an approval or unlocks send.
   */
  async getStationOverview(queueLimit = 50): Promise<StationOverview> {
    const [metrics, accounts] = await Promise.all([
      this.repository.getStationMetrics(),
      this.listAccounts(Math.min(Math.max(queueLimit, 1), 100)),
    ]);
    return {
      metrics,
      reviewQueue: accounts.map((account) => this.toStationQueueItem(account)),
    };
  }

  private toStationQueueItem(account: Account): StationQueueItem {
    if ([account.accountApproval, account.contactApproval, account.draftApproval, account.sendApproval].includes('rejected')) {
      return { account, queueState: 'blocked_rejected', nextGate: null };
    }
    if (account.accountApproval === 'pending') {
      return { account, queueState: 'station_1_account_review', nextGate: 'account' };
    }
    if (account.contactApproval === 'pending') {
      return { account, queueState: 'station_1_contact_review', nextGate: 'contact' };
    }
    if (account.draftApproval === 'pending') {
      return { account, queueState: 'station_2_draft_review', nextGate: 'draft' };
    }
    if (account.sendApproval === 'pending') {
      return { account, queueState: 'station_2_send_review', nextGate: 'send' };
    }
    return { account, queueState: 'outreach_authorized', nextGate: null };
  }

  async recordApproval(input: RecordApprovalInput): Promise<void> {
    const actor = requiredText(input.actor, 'actor');
    const account = await this.repository.getAccount(input.accountId);
    if (!account) throw new CrmNotFoundError();

    const targetField = GATE_FIELD[input.gate];
    if (account[targetField] !== 'pending') {
      throw new CrmGateViolationError('A gate can only be decided once');
    }
    if (GATE_PREREQUISITES[input.gate].some((field) => account[field] !== 'approved')) {
      throw new CrmGateViolationError('All earlier approval gates must be approved first');
    }

    const expectedApprovals: ApprovalSnapshot = {
      accountApproval: account.accountApproval,
      contactApproval: account.contactApproval,
      draftApproval: account.draftApproval,
      sendApproval: account.sendApproval,
    };
    const recorded = await this.repository.recordApproval({ ...input, actor, expectedApprovals });
    if (!recorded) {
      throw new CrmGateViolationError('Approval state changed; retry after reviewing the current state');
    }
  }

  async health(configured: boolean): Promise<{ configured: boolean; ready: boolean; freshness: { checked_at: string; database_reachable: boolean } }> {
    const ready = configured ? await this.repository.checkReady() : false;
    return {
      configured,
      ready,
      freshness: {
        checked_at: new Date().toISOString(),
        database_reachable: ready,
      },
    };
  }
}

export function isApprovalGate(value: unknown): value is ApprovalGate {
  return typeof value === 'string' && Object.prototype.hasOwnProperty.call(GATE_FIELD, value);
}

export function isApprovalDecision(value: unknown): value is ApprovalDecision {
  return value === 'approved' || value === 'rejected';
}
