import express from 'express';
import { once } from 'node:events';
import { describe, expect, it } from 'vitest';
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
