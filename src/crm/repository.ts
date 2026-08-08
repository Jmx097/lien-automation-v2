import { randomUUID } from 'node:crypto';
import { Pool, type PoolClient } from 'pg';
import type {
  Account,
  ApprovalGate,
  ApprovalSnapshot,
  CreateSourceIntakeAccountInput,
  CrmRepository,
  RecordApprovalInput,
} from './types';

const GATE_COLUMNS: Record<ApprovalGate, string> = {
  account: 'account_approval',
  contact: 'contact_approval',
  draft: 'draft_approval',
  send: 'send_approval',
};

function accountFromRow(row: Record<string, unknown>): Account {
  return {
    id: String(row.id),
    source: String(row.source),
    displayName: row.display_name == null ? null : String(row.display_name),
    externalReference: row.external_reference == null ? null : String(row.external_reference),
    accountApproval: row.account_approval as Account['accountApproval'],
    contactApproval: row.contact_approval as Account['contactApproval'],
    draftApproval: row.draft_approval as Account['draftApproval'],
    sendApproval: row.send_approval as Account['sendApproval'],
    createdAt: new Date(String(row.created_at)),
    updatedAt: new Date(String(row.updated_at)),
  };
}

export class PostgresCrmRepository implements CrmRepository {
  constructor(private readonly pool: Pool) {}

  static fromConnectionString(connectionString: string): PostgresCrmRepository {
    return new PostgresCrmRepository(new Pool({ connectionString }));
  }

  async createSourceIntakeAccount(input: CreateSourceIntakeAccountInput): Promise<Account> {
    const result = await this.pool.query(
      `INSERT INTO crm_accounts (id, source, intake_type, display_name, external_reference)
       VALUES ($1, $2, 'source_intake', $3, $4)
       RETURNING *`,
      [randomUUID(), input.source, input.displayName ?? null, input.externalReference ?? null],
    );
    return accountFromRow(result.rows[0]);
  }

  async listAccounts(limit: number): Promise<Account[]> {
    const result = await this.pool.query(
      'SELECT * FROM crm_accounts ORDER BY created_at DESC LIMIT $1',
      [limit],
    );
    return result.rows.map(accountFromRow);
  }

  async getAccount(id: string): Promise<Account | null> {
    const result = await this.pool.query('SELECT * FROM crm_accounts WHERE id = $1', [id]);
    return result.rowCount ? accountFromRow(result.rows[0]) : null;
  }

  async recordApproval(input: RecordApprovalInput & { expectedApprovals: ApprovalSnapshot }): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const changed = await this.updateGateIfCurrent(client, input);
      if (!changed) {
        await client.query('ROLLBACK');
        return false;
      }
      await client.query(
        `INSERT INTO crm_approval_decisions (id, account_id, gate, decision, actor)
         VALUES ($1, $2, $3, $4, $5)`,
        [randomUUID(), input.accountId, input.gate, input.decision, input.actor],
      );
      await client.query('COMMIT');
      return true;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async updateGateIfCurrent(
    client: PoolClient,
    input: RecordApprovalInput & { expectedApprovals: ApprovalSnapshot },
  ): Promise<boolean> {
    const column = GATE_COLUMNS[input.gate];
    const result = await client.query(
      `UPDATE crm_accounts
       SET ${column} = $2, updated_at = NOW()
       WHERE id = $1
         AND account_approval = $3
         AND contact_approval = $4
         AND draft_approval = $5
         AND send_approval = $6
         AND ${column} = 'pending'`,
      [
        input.accountId,
        input.decision,
        input.expectedApprovals.accountApproval,
        input.expectedApprovals.contactApproval,
        input.expectedApprovals.draftApproval,
        input.expectedApprovals.sendApproval,
      ],
    );
    return result.rowCount === 1;
  }

  async checkReady(): Promise<boolean> {
    try {
      const result = await this.pool.query<{ ready: boolean }>('SELECT to_regclass(\'public.crm_accounts\') IS NOT NULL AS ready');
      return result.rows[0]?.ready === true;
    } catch {
      return false;
    }
  }
}
