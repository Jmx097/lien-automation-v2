import express from 'express';
import { once } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import { isCrmRequestAuthorized } from '../../src/crm/auth';
import { createCrmRouter } from '../../src/crm/router';
import {
  CrmGateViolationError,
  GovernedCrmService,
  type CrmRepository,
  type Account,
  type ApprovalDecision,
  type ApprovalGate,
  type CreateSourceIntakeAccountInput,
} from '../../src/crm';

class FakeCrmRepository implements CrmRepository {
  private readonly accounts = new Map<string, Account>();
  readonly approvals: Array<{ accountId: string; gate: ApprovalGate; decision: ApprovalDecision }> = [];

  async createSourceIntakeAccount(input: CreateSourceIntakeAccountInput): Promise<Account> {
    const account: Account = {
      id: `account-${this.accounts.size + 1}`,
      source: input.source,
      displayName: input.displayName ?? null,
      externalReference: input.externalReference ?? null,
      accountApproval: 'pending',
      contactApproval: 'pending',
      draftApproval: 'pending',
      sendApproval: 'pending',
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      updatedAt: new Date('2026-01-01T00:00:00.000Z'),
    };
    this.accounts.set(account.id, account);
    return account;
  }

  async listAccounts(): Promise<Account[]> {
    return [...this.accounts.values()];
  }

  async getStationMetrics() {
    const accounts = [...this.accounts.values()];
    const count = (predicate: (account: Account) => boolean) => accounts.filter(predicate).length;
    const accountRejected = count((account) => account.accountApproval === 'rejected');
    const contactRejected = count((account) => account.contactApproval === 'rejected');
    const draftRejected = count((account) => account.draftApproval === 'rejected');
    const sendRejected = count((account) => account.sendApproval === 'rejected');
    return {
      totalAccounts: accounts.length,
      station1: {
        accountReviewPending: count((account) => account.accountApproval === 'pending'),
        contactReviewPending: count((account) => account.accountApproval === 'approved' && account.contactApproval === 'pending'),
      },
      station2: {
        draftReviewPending: count((account) => account.accountApproval === 'approved' && account.contactApproval === 'approved' && account.draftApproval === 'pending'),
        sendReviewPending: count((account) => account.accountApproval === 'approved' && account.contactApproval === 'approved' && account.draftApproval === 'approved' && account.sendApproval === 'pending'),
        outreachAuthorized: count((account) => account.sendApproval === 'approved'),
      },
      blocked: {
        totalRejected: accountRejected + contactRejected + draftRejected + sendRejected,
        accountRejected,
        contactRejected,
        draftRejected,
        sendRejected,
      },
    };
  }

  async getAccount(id: string): Promise<Account | null> {
    return this.accounts.get(id) ?? null;
  }

  async recordApproval(input: {
    accountId: string;
    gate: ApprovalGate;
    decision: ApprovalDecision;
    actor: string;
    expectedApprovals: Pick<Account, 'accountApproval' | 'contactApproval' | 'draftApproval' | 'sendApproval'>;
  }): Promise<boolean> {
    const account = this.accounts.get(input.accountId);
    if (!account) return false;
    const expected = input.expectedApprovals;
    if (
      account.accountApproval !== expected.accountApproval ||
      account.contactApproval !== expected.contactApproval ||
      account.draftApproval !== expected.draftApproval ||
      account.sendApproval !== expected.sendApproval
    ) return false;

    const property = `${input.gate}Approval` as const;
    if (account[property] !== 'pending') return false;
    account[property] = input.decision;
    account.updatedAt = new Date('2026-01-01T00:01:00.000Z');
    this.approvals.push({ accountId: input.accountId, gate: input.gate, decision: input.decision });
    return true;
  }

  async checkReady(): Promise<boolean> {
    return true;
  }
}

describe('CRM bearer authorization', () => {
  it('fails closed when the configured token is absent, malformed, or mismatched', () => {
    expect(isCrmRequestAuthorized({ headers: { authorization: 'Bearer expected' } }, undefined)).toBe(false);
    expect(isCrmRequestAuthorized({ headers: { authorization: 'Basic expected' } }, 'expected')).toBe(false);
    expect(isCrmRequestAuthorized({ headers: { authorization: 'Bearer wrong' } }, 'expected')).toBe(false);
  });

  it('authorizes only the exact bearer token', () => {
    expect(isCrmRequestAuthorized({ headers: { authorization: 'Bearer expected' } }, 'expected')).toBe(true);
  });

  it('protects the CRM health endpoint before any database probe', async () => {
    const app = express();
    app.use('/crm', createCrmRouter({
      service: new GovernedCrmService(new FakeCrmRepository()),
      apiToken: 'expected',
      databaseConfigured: true,
    }));
    const server = app.listen(0);
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Expected a TCP listener');
    try {
      const baseUrl = `http://127.0.0.1:${address.port}/crm/health`;
      expect((await fetch(baseUrl)).status).toBe(401);
      expect((await fetch(baseUrl, { headers: { authorization: 'Bearer wrong' } })).status).toBe(401);
      const response = await fetch(baseUrl, { headers: { authorization: 'Bearer expected' } });
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ configured: true, ready: true });
    } finally {
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }
  });
});

describe('Station 1 approval gates', () => {
  it('creates only pending source-intake accounts and permits the linear sequence', async () => {
    const repository = new FakeCrmRepository();
    const service = new GovernedCrmService(repository);
    const account = await service.createSourceIntakeAccount({ source: 'county-records', displayName: 'Example LLC' });

    expect(account).toMatchObject({
      source: 'county-records',
      accountApproval: 'pending',
      contactApproval: 'pending',
      draftApproval: 'pending',
      sendApproval: 'pending',
    });

    await service.recordApproval({ accountId: account.id, gate: 'account', decision: 'approved', actor: 'operator@example.test' });
    await service.recordApproval({ accountId: account.id, gate: 'contact', decision: 'approved', actor: 'operator@example.test' });
    await service.recordApproval({ accountId: account.id, gate: 'draft', decision: 'approved', actor: 'operator@example.test' });
    await service.recordApproval({ accountId: account.id, gate: 'send', decision: 'approved', actor: 'operator@example.test' });

    expect(repository.approvals.map((approval) => approval.gate)).toEqual(['account', 'contact', 'draft', 'send']);
  });

  it('fails closed for skipped, repeated, and rejected gate progressions', async () => {
    const repository = new FakeCrmRepository();
    const service = new GovernedCrmService(repository);
    const account = await service.createSourceIntakeAccount({ source: 'county-records' });

    await expect(service.recordApproval({ accountId: account.id, gate: 'send', decision: 'approved', actor: 'operator@example.test' }))
      .rejects.toBeInstanceOf(CrmGateViolationError);
    await expect(service.recordApproval({ accountId: account.id, gate: 'contact', decision: 'approved', actor: 'operator@example.test' }))
      .rejects.toBeInstanceOf(CrmGateViolationError);

    await service.recordApproval({ accountId: account.id, gate: 'account', decision: 'rejected', actor: 'operator@example.test' });
    await expect(service.recordApproval({ accountId: account.id, gate: 'contact', decision: 'approved', actor: 'operator@example.test' }))
      .rejects.toBeInstanceOf(CrmGateViolationError);
    await expect(service.recordApproval({ accountId: account.id, gate: 'account', decision: 'approved', actor: 'operator@example.test' }))
      .rejects.toBeInstanceOf(CrmGateViolationError);
    expect(repository.approvals).toHaveLength(1);
  });
});

describe('Station 1–2 command center', () => {
  it('classifies the precise next gate without performing any transition', async () => {
    const repository = new FakeCrmRepository();
    const service = new GovernedCrmService(repository);
    const pendingAccount = await service.createSourceIntakeAccount({ source: 'source-a' });
    const contactReview = await service.createSourceIntakeAccount({ source: 'source-b' });
    const draftReview = await service.createSourceIntakeAccount({ source: 'source-c' });
    const sendReview = await service.createSourceIntakeAccount({ source: 'source-d' });
    const authorized = await service.createSourceIntakeAccount({ source: 'source-e' });
    const rejected = await service.createSourceIntakeAccount({ source: 'source-f' });

    for (const account of [contactReview, draftReview, sendReview, authorized]) {
      await service.recordApproval({ accountId: account.id, gate: 'account', decision: 'approved', actor: 'operator@example.test' });
    }
    for (const account of [draftReview, sendReview, authorized]) {
      await service.recordApproval({ accountId: account.id, gate: 'contact', decision: 'approved', actor: 'operator@example.test' });
    }
    for (const account of [sendReview, authorized]) {
      await service.recordApproval({ accountId: account.id, gate: 'draft', decision: 'approved', actor: 'operator@example.test' });
    }
    await service.recordApproval({ accountId: authorized.id, gate: 'send', decision: 'approved', actor: 'operator@example.test' });
    await service.recordApproval({ accountId: rejected.id, gate: 'account', decision: 'rejected', actor: 'operator@example.test' });

    const overview = await service.getStationOverview();
    expect(overview.metrics).toMatchObject({
      totalAccounts: 6,
      station1: { accountReviewPending: 1, contactReviewPending: 1 },
      station2: { draftReviewPending: 1, sendReviewPending: 1, outreachAuthorized: 1 },
      blocked: { totalRejected: 1, accountRejected: 1 },
    });
    expect(overview.reviewQueue.map(({ account, queueState, nextGate }) => [account.id, queueState, nextGate])).toEqual([
      [pendingAccount.id, 'station_1_account_review', 'account'],
      [contactReview.id, 'station_1_contact_review', 'contact'],
      [draftReview.id, 'station_2_draft_review', 'draft'],
      [sendReview.id, 'station_2_send_review', 'send'],
      [authorized.id, 'outreach_authorized', null],
      [rejected.id, 'blocked_rejected', null],
    ]);
  });

  it('protects the tracking endpoint and exposes only a read-only overview', async () => {
    const app = express();
    const repository = new FakeCrmRepository();
    const service = new GovernedCrmService(repository);
    await service.createSourceIntakeAccount({ source: 'county-records', displayName: 'Example LLC' });
    app.use('/crm', createCrmRouter({ service, apiToken: 'expected', databaseConfigured: true }));
    const server = app.listen(0);
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Expected a TCP listener');
    try {
      const baseUrl = `http://127.0.0.1:${address.port}/crm/stations/overview`;
      expect((await fetch(baseUrl)).status).toBe(401);
      const response = await fetch(baseUrl, { headers: { authorization: 'Bearer expected' } });
      expect(response.status).toBe(200);
      await expect(response.json()).resolves.toMatchObject({
        stations: { metrics: { totalAccounts: 1 }, reviewQueue: [{ queueState: 'station_1_account_review', nextGate: 'account' }] },
        governance: { mode: 'read_only' },
        freshness: { checked_at: expect.any(String) },
      });
    } finally {
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }
  });
});

describe('Account workspace contacts and notes', () => {
  it('returns source-intake contacts and appends an attributed account note', async () => {
    const accountWorkspace = vi.fn().mockResolvedValue({
      account: { id: 'account-1', display_name: 'Example LLC' },
      contacts: [{ id: 'contact-1', full_name: 'A. Person', contact_approval: 'pending' }],
      notes: [],
    });
    const addAccountNote = vi.fn().mockResolvedValue({ recorded: true });
    const app = express();
    app.use(express.json());
    app.use('/crm', createCrmRouter({
      service: new GovernedCrmService(new FakeCrmRepository()),
      campaignWorkbench: { accountWorkspace, addAccountNote } as never,
      apiToken: 'expected',
      databaseConfigured: true,
    }));
    const server = app.listen(0);
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Expected a TCP listener');
    const base = `http://127.0.0.1:${address.port}/crm/accounts/account-1`;
    try {
      const headers = { authorization: 'Bearer expected', 'x-crm-actor': 'staff:operator@example.test' };
      const read = await fetch(`${base}/workspace`, { headers });
      expect(read.status).toBe(200);
      await expect(read.json()).resolves.toMatchObject({ contacts: [{ full_name: 'A. Person' }] });
      const write = await fetch(`${base}/notes`, {
        method: 'POST', headers: { ...headers, 'content-type': 'application/json' },
        body: JSON.stringify({ note: 'Reviewed source evidence.' }),
      });
      expect(write.status).toBe(201);
      expect(addAccountNote).toHaveBeenCalledWith('account-1', 'Reviewed source evidence.', 'staff:operator@example.test');
      expect((await fetch(`${base}/notes`, { method: 'POST', headers: { authorization: 'Bearer expected', 'content-type': 'application/json' }, body: JSON.stringify({ note: '' }) })).status).toBe(400);
    } finally {
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    }
  });
});
