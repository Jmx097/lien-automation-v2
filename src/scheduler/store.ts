import Database from 'better-sqlite3';
import { Pool, type QueryResultRow } from 'pg';
import { ensureDatabaseReady, resolveDbPath } from '../db/init';
import type { SiteConnectivityState } from './connectivity';
import type { SupportedSite } from '../sites';

export interface ScheduledRunRecord {
  id: string;
  site: SupportedSite;
  idempotency_key: string;
  slot_time: string;
  trigger_source: 'external' | 'manual';
  started_at: string;
  finished_at?: string;
  status: 'running' | 'success' | 'error' | 'deferred';
  records_scraped: number;
  records_skipped: number;
  rows_uploaded: number;
  amount_found_count: number;
  amount_missing_count: number;
  amount_coverage_pct: number;
  ocr_success_pct: number;
  row_fail_pct: number;
  deadline_hit: number;
  effective_max_records: number;
  partial: number;
  error?: string;
  failure_class?: string;
}

interface MissedAlertRecord {
  site: SupportedSite;
  idempotency_key: string;
  slot: 'morning' | 'afternoon';
  expected_by: string;
}

export interface ScheduleControlState {
  site: SupportedSite;
  effective_max_records: number;
  updated_at: string;
}

type SchedulerStoreBackendKind = 'sqlite' | 'postgres';

export interface SchedulerStoreReadiness {
  backend: SchedulerStoreBackendKind;
  ok: boolean;
  detail?: string;
}

interface SchedulerStoreBackend {
  initialize(): Promise<void>;
  close(): Promise<void>;
  insertRun(run: ScheduledRunRecord): Promise<void>;
  updateRun(run: ScheduledRunRecord): Promise<void>;
  getByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null>;
  getMostRecentRun(site?: SupportedSite): Promise<ScheduledRunRecord | null>;
  getSuccessfulRunByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null>;
  getRunHistory(limit?: number, site?: SupportedSite): Promise<ScheduledRunRecord[]>;
  getRecentSuccessfulRuns(site: SupportedSite, limit?: number): Promise<ScheduledRunRecord[]>;
  upsertControlState(site: SupportedSite, effectiveMaxRecords: number): Promise<void>;
  getControlState(site: SupportedSite): Promise<ScheduleControlState | null>;
  upsertConnectivityState(state: SiteConnectivityState): Promise<void>;
  getConnectivityState(site: SupportedSite): Promise<SiteConnectivityState | null>;
  listConnectivityStates(): Promise<SiteConnectivityState[]>;
  insertMissedAlert(alert: MissedAlertRecord): Promise<void>;
  getMissedAlertByKey(idempotencyKey: string): Promise<MissedAlertRecord | null>;
}

function toIso(value: unknown): string | undefined {
  if (value == null) return undefined;
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function toNumber(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'string' && value.trim() !== '') return Number(value);
  return 0;
}

function normalizeScheduledRunRecord(row: Record<string, unknown> | undefined): ScheduledRunRecord | null {
  if (!row) return null;
  return {
    id: String(row.id),
    site: String(row.site) as SupportedSite,
    idempotency_key: String(row.idempotency_key),
    slot_time: String(row.slot_time),
    trigger_source: String(row.trigger_source) as ScheduledRunRecord['trigger_source'],
    started_at: toIso(row.started_at) ?? '',
    finished_at: toIso(row.finished_at),
    status: String(row.status) as ScheduledRunRecord['status'],
    records_scraped: toNumber(row.records_scraped),
    records_skipped: toNumber(row.records_skipped),
    rows_uploaded: toNumber(row.rows_uploaded),
    amount_found_count: toNumber(row.amount_found_count),
    amount_missing_count: toNumber(row.amount_missing_count),
    amount_coverage_pct: toNumber(row.amount_coverage_pct),
    ocr_success_pct: toNumber(row.ocr_success_pct),
    row_fail_pct: toNumber(row.row_fail_pct),
    deadline_hit: toNumber(row.deadline_hit),
    effective_max_records: toNumber(row.effective_max_records),
    partial: toNumber(row.partial),
    error: row.error == null ? undefined : String(row.error),
    failure_class: row.failure_class == null ? undefined : String(row.failure_class),
  };
}

function normalizeControlState(row: Record<string, unknown> | undefined): ScheduleControlState | null {
  if (!row) return null;
  return {
    site: String(row.site) as SupportedSite,
    effective_max_records: toNumber(row.effective_max_records),
    updated_at: toIso(row.updated_at) ?? '',
  };
}

function normalizeConnectivityState(row: Record<string, unknown> | undefined): SiteConnectivityState | null {
  if (!row) return null;
  return {
    site: String(row.site) as SupportedSite,
    status: String(row.status) as SiteConnectivityState['status'],
    opened_at: toIso(row.opened_at),
    last_success_at: toIso(row.last_success_at),
    last_failure_at: toIso(row.last_failure_at),
    policy_block_count: toNumber(row.policy_block_count),
    timeout_count: toNumber(row.timeout_count),
    empty_result_count: toNumber(row.empty_result_count),
    window_started_at: toIso(row.window_started_at),
    next_probe_at: toIso(row.next_probe_at),
    consecutive_probe_successes: toNumber(row.consecutive_probe_successes),
    last_failure_reason: row.last_failure_reason == null ? undefined : String(row.last_failure_reason),
    last_alerted_at: toIso(row.last_alerted_at),
    last_recovery_alert_at: toIso(row.last_recovery_alert_at),
  };
}

function normalizeMissedAlertRecord(row: Record<string, unknown> | undefined): MissedAlertRecord | null {
  if (!row) return null;
  return {
    site: String(row.site) as SupportedSite,
    idempotency_key: String(row.idempotency_key),
    slot: String(row.slot) as MissedAlertRecord['slot'],
    expected_by: toIso(row.expected_by) ?? '',
  };
}

function hasDatabaseUrl(): boolean {
  return Boolean(process.env.DATABASE_URL?.trim());
}

export function getSchedulerStoreBackendKind(): SchedulerStoreBackendKind {
  return hasDatabaseUrl() ? 'postgres' : 'sqlite';
}

function createCommonSchemaSql(): string[] {
  return [
    `
      CREATE TABLE IF NOT EXISTS scheduled_runs (
        id TEXT PRIMARY KEY,
        site TEXT NOT NULL DEFAULT 'ca_sos',
        idempotency_key TEXT NOT NULL UNIQUE,
        slot_time TEXT NOT NULL,
        trigger_source TEXT NOT NULL CHECK(trigger_source IN ('external', 'manual')),
        started_at TEXT NOT NULL,
        finished_at TEXT,
        status TEXT NOT NULL CHECK(status IN ('running', 'success', 'error', 'deferred')),
        records_scraped INTEGER NOT NULL DEFAULT 0,
        records_skipped INTEGER NOT NULL DEFAULT 0,
        rows_uploaded INTEGER NOT NULL DEFAULT 0,
        amount_found_count INTEGER NOT NULL DEFAULT 0,
        amount_missing_count INTEGER NOT NULL DEFAULT 0,
        amount_coverage_pct REAL NOT NULL DEFAULT 0,
        ocr_success_pct REAL NOT NULL DEFAULT 0,
        row_fail_pct REAL NOT NULL DEFAULT 0,
        deadline_hit INTEGER NOT NULL DEFAULT 0,
        effective_max_records INTEGER NOT NULL DEFAULT 0,
        partial INTEGER NOT NULL DEFAULT 0,
        error TEXT,
        failure_class TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `,
    `CREATE INDEX IF NOT EXISTS idx_scheduled_runs_started_at ON scheduled_runs(started_at)`,
    `CREATE INDEX IF NOT EXISTS idx_scheduled_runs_status ON scheduled_runs(status)`,
    `CREATE INDEX IF NOT EXISTS idx_scheduled_runs_site_started_at ON scheduled_runs(site, started_at)`,
    `
      CREATE TABLE IF NOT EXISTS scheduler_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site TEXT NOT NULL DEFAULT 'ca_sos',
        idempotency_key TEXT NOT NULL,
        slot TEXT NOT NULL CHECK(slot IN ('morning', 'afternoon')),
        alert_type TEXT NOT NULL CHECK(alert_type IN ('missed_run')),
        expected_by TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(idempotency_key, alert_type)
      )
    `,
    `
      CREATE TABLE IF NOT EXISTS scheduler_site_control_state (
        site TEXT PRIMARY KEY,
        effective_max_records INTEGER NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `,
    `
      CREATE TABLE IF NOT EXISTS scheduler_site_connectivity_state (
        site TEXT PRIMARY KEY,
        status TEXT NOT NULL CHECK(status IN ('healthy', 'degraded', 'blocked', 'probing')),
        opened_at TEXT,
        last_success_at TEXT,
        last_failure_at TEXT,
        policy_block_count INTEGER NOT NULL DEFAULT 0,
        timeout_count INTEGER NOT NULL DEFAULT 0,
        empty_result_count INTEGER NOT NULL DEFAULT 0,
        window_started_at TEXT,
        next_probe_at TEXT,
        consecutive_probe_successes INTEGER NOT NULL DEFAULT 0,
        last_failure_reason TEXT,
        last_alerted_at TEXT,
        last_recovery_alert_at TEXT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `,
  ];
}

class SQLiteSchedulerStoreBackend implements SchedulerStoreBackend {
  private db: Database.Database | null = null;

  async initialize(): Promise<void> {
    if (this.db) return;

    ensureDatabaseReady();
    this.db = new Database(resolveDbPath());
    this.ensureSchema();
  }

  async close(): Promise<void> {
    this.db?.close();
    this.db = null;
  }

  private getDb(): Database.Database {
    if (!this.db) {
      throw new Error('SQLite scheduler store is not initialized');
    }
    return this.db;
  }

  private ensureSchema(): void {
    const db = this.getDb();
    db.exec(createCommonSchemaSql().join(';\n'));

    const scheduledRunColumns = db.prepare("PRAGMA table_info('scheduled_runs')").all() as Array<{ name: string }>;
    if (!scheduledRunColumns.some((column) => column.name === 'site')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
    }

    const alertColumns = db.prepare("PRAGMA table_info('scheduler_alerts')").all() as Array<{ name: string }>;
    if (!alertColumns.some((column) => column.name === 'site')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
    }

    if (!scheduledRunColumns.some((column) => column.name === 'failure_class')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN failure_class TEXT").run();
    }
  }

  async insertRun(run: ScheduledRunRecord): Promise<void> {
    this.getDb().prepare(
      `INSERT INTO scheduled_runs (
        id, site, idempotency_key, slot_time, trigger_source, started_at, finished_at, status,
        records_scraped, records_skipped, rows_uploaded,
        amount_found_count, amount_missing_count, amount_coverage_pct, ocr_success_pct, row_fail_pct,
        deadline_hit, effective_max_records, partial, error, failure_class
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      run.id,
      run.site,
      run.idempotency_key,
      run.slot_time,
      run.trigger_source,
      run.started_at,
      run.finished_at ?? null,
      run.status,
      run.records_scraped,
      run.records_skipped,
      run.rows_uploaded,
      run.amount_found_count,
      run.amount_missing_count,
      run.amount_coverage_pct,
      run.ocr_success_pct,
      run.row_fail_pct,
      run.deadline_hit,
      run.effective_max_records,
      run.partial,
      run.error ?? null,
      run.failure_class ?? null
    );
  }

  async updateRun(run: ScheduledRunRecord): Promise<void> {
    this.getDb().prepare(
      `UPDATE scheduled_runs
       SET site = ?, finished_at = ?, status = ?, records_scraped = ?, records_skipped = ?, rows_uploaded = ?,
           amount_found_count = ?, amount_missing_count = ?, amount_coverage_pct = ?, ocr_success_pct = ?, row_fail_pct = ?,
           deadline_hit = ?, effective_max_records = ?, partial = ?, error = ?, failure_class = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`
    ).run(
      run.site,
      run.finished_at ?? null,
      run.status,
      run.records_scraped,
      run.records_skipped,
      run.rows_uploaded,
      run.amount_found_count,
      run.amount_missing_count,
      run.amount_coverage_pct,
      run.ocr_success_pct,
      run.row_fail_pct,
      run.deadline_hit,
      run.effective_max_records,
      run.partial,
      run.error ?? null,
      run.failure_class ?? null,
      run.id
    );
  }

  async getByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null> {
    const row = this.getDb()
      .prepare('SELECT * FROM scheduled_runs WHERE idempotency_key = ? ORDER BY created_at DESC LIMIT 1')
      .get(idempotencyKey) as Record<string, unknown> | undefined;
    return normalizeScheduledRunRecord(row);
  }

  async getMostRecentRun(site?: SupportedSite): Promise<ScheduledRunRecord | null> {
    const statement = site
      ? this.getDb().prepare('SELECT * FROM scheduled_runs WHERE site = ? ORDER BY started_at DESC LIMIT 1')
      : this.getDb().prepare('SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT 1');
    const row = (site ? statement.get(site) : statement.get()) as Record<string, unknown> | undefined;
    return normalizeScheduledRunRecord(row);
  }

  async getSuccessfulRunByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null> {
    const row = this.getDb()
      .prepare("SELECT * FROM scheduled_runs WHERE idempotency_key = ? AND status = 'success' ORDER BY created_at DESC LIMIT 1")
      .get(idempotencyKey) as Record<string, unknown> | undefined;
    return normalizeScheduledRunRecord(row);
  }

  async getRunHistory(limit = 50, site?: SupportedSite): Promise<ScheduledRunRecord[]> {
    const rows = site
      ? this.getDb().prepare('SELECT * FROM scheduled_runs WHERE site = ? ORDER BY started_at DESC LIMIT ?').all(site, limit)
      : this.getDb().prepare('SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT ?').all(limit);
    return (rows as Record<string, unknown>[]).map((row) => normalizeScheduledRunRecord(row)).filter(Boolean) as ScheduledRunRecord[];
  }

  async getRecentSuccessfulRuns(site: SupportedSite, limit = 4): Promise<ScheduledRunRecord[]> {
    const rows = this.getDb()
      .prepare("SELECT * FROM scheduled_runs WHERE site = ? AND status = 'success' ORDER BY started_at DESC LIMIT ?")
      .all(site, limit) as Record<string, unknown>[];
    return rows.map((row) => normalizeScheduledRunRecord(row)).filter(Boolean) as ScheduledRunRecord[];
  }

  async upsertControlState(site: SupportedSite, effectiveMaxRecords: number): Promise<void> {
    this.getDb().prepare(
      `INSERT INTO scheduler_site_control_state (site, effective_max_records, updated_at)
       VALUES (?, ?, CURRENT_TIMESTAMP)
       ON CONFLICT(site) DO UPDATE SET effective_max_records = excluded.effective_max_records, updated_at = CURRENT_TIMESTAMP`
    ).run(site, effectiveMaxRecords);
  }

  async getControlState(site: SupportedSite): Promise<ScheduleControlState | null> {
    const row = this.getDb()
      .prepare('SELECT * FROM scheduler_site_control_state WHERE site = ?')
      .get(site) as Record<string, unknown> | undefined;
    return normalizeControlState(row);
  }

  async upsertConnectivityState(state: SiteConnectivityState): Promise<void> {
    this.getDb().prepare(
      `INSERT INTO scheduler_site_connectivity_state (
        site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
        empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
        last_failure_reason, last_alerted_at, last_recovery_alert_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(site) DO UPDATE SET
        status = excluded.status,
        opened_at = excluded.opened_at,
        last_success_at = excluded.last_success_at,
        last_failure_at = excluded.last_failure_at,
        policy_block_count = excluded.policy_block_count,
        timeout_count = excluded.timeout_count,
        empty_result_count = excluded.empty_result_count,
        window_started_at = excluded.window_started_at,
        next_probe_at = excluded.next_probe_at,
        consecutive_probe_successes = excluded.consecutive_probe_successes,
        last_failure_reason = excluded.last_failure_reason,
        last_alerted_at = excluded.last_alerted_at,
        last_recovery_alert_at = excluded.last_recovery_alert_at,
        updated_at = CURRENT_TIMESTAMP`
    ).run(
      state.site,
      state.status,
      state.opened_at ?? null,
      state.last_success_at ?? null,
      state.last_failure_at ?? null,
      state.policy_block_count,
      state.timeout_count,
      state.empty_result_count,
      state.window_started_at ?? null,
      state.next_probe_at ?? null,
      state.consecutive_probe_successes,
      state.last_failure_reason ?? null,
      state.last_alerted_at ?? null,
      state.last_recovery_alert_at ?? null
    );
  }

  async getConnectivityState(site: SupportedSite): Promise<SiteConnectivityState | null> {
    const row = this.getDb().prepare(
      `SELECT site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
              empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
              last_failure_reason, last_alerted_at, last_recovery_alert_at
       FROM scheduler_site_connectivity_state
       WHERE site = ?`
    ).get(site) as Record<string, unknown> | undefined;
    return normalizeConnectivityState(row);
  }

  async listConnectivityStates(): Promise<SiteConnectivityState[]> {
    const rows = this.getDb().prepare(
      `SELECT site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
              empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
              last_failure_reason, last_alerted_at, last_recovery_alert_at
       FROM scheduler_site_connectivity_state
       ORDER BY site`
    ).all() as Record<string, unknown>[];
    return rows.map((row) => normalizeConnectivityState(row)).filter(Boolean) as SiteConnectivityState[];
  }

  async insertMissedAlert(alert: MissedAlertRecord): Promise<void> {
    this.getDb().prepare(
      `INSERT OR IGNORE INTO scheduler_alerts (site, idempotency_key, slot, expected_by, alert_type)
       VALUES (?, ?, ?, ?, 'missed_run')`
    ).run(alert.site, alert.idempotency_key, alert.slot, alert.expected_by);
  }

  async getMissedAlertByKey(idempotencyKey: string): Promise<MissedAlertRecord | null> {
    const row = this.getDb()
      .prepare("SELECT site, idempotency_key, slot, expected_by FROM scheduler_alerts WHERE idempotency_key = ? AND alert_type = 'missed_run' LIMIT 1")
      .get(idempotencyKey) as Record<string, unknown> | undefined;
    return normalizeMissedAlertRecord(row);
  }
}

class PostgresSchedulerStoreBackend implements SchedulerStoreBackend {
  private pool: Pool;

  constructor() {
    const connectionString = process.env.DATABASE_URL?.trim();
    if (!connectionString) {
      throw new Error('DATABASE_URL must be set when using the Postgres scheduler store');
    }

    this.pool = new Pool({
      connectionString,
      ssl: connectionString.includes('sslmode=disable') ? undefined : { rejectUnauthorized: false },
    });
  }

  async initialize(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`
        CREATE TABLE IF NOT EXISTS scheduled_runs (
          id TEXT PRIMARY KEY,
          site TEXT NOT NULL DEFAULT 'ca_sos',
          idempotency_key TEXT NOT NULL UNIQUE,
          slot_time TEXT NOT NULL,
          trigger_source TEXT NOT NULL CHECK(trigger_source IN ('external', 'manual')),
          started_at TIMESTAMPTZ NOT NULL,
          finished_at TIMESTAMPTZ,
          status TEXT NOT NULL CHECK(status IN ('running', 'success', 'error', 'deferred')),
          records_scraped INTEGER NOT NULL DEFAULT 0,
          records_skipped INTEGER NOT NULL DEFAULT 0,
          rows_uploaded INTEGER NOT NULL DEFAULT 0,
          amount_found_count INTEGER NOT NULL DEFAULT 0,
          amount_missing_count INTEGER NOT NULL DEFAULT 0,
          amount_coverage_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
          ocr_success_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
          row_fail_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
          deadline_hit INTEGER NOT NULL DEFAULT 0,
          effective_max_records INTEGER NOT NULL DEFAULT 0,
          partial INTEGER NOT NULL DEFAULT 0,
          error TEXT,
          failure_class TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await client.query('CREATE INDEX IF NOT EXISTS idx_scheduled_runs_started_at ON scheduled_runs(started_at DESC)');
      await client.query('CREATE INDEX IF NOT EXISTS idx_scheduled_runs_status ON scheduled_runs(status)');
      await client.query('CREATE INDEX IF NOT EXISTS idx_scheduled_runs_site_started_at ON scheduled_runs(site, started_at DESC)');
      await client.query(`
        CREATE TABLE IF NOT EXISTS scheduler_alerts (
          id BIGSERIAL PRIMARY KEY,
          site TEXT NOT NULL DEFAULT 'ca_sos',
          idempotency_key TEXT NOT NULL,
          slot TEXT NOT NULL CHECK(slot IN ('morning', 'afternoon')),
          alert_type TEXT NOT NULL CHECK(alert_type IN ('missed_run')),
          expected_by TIMESTAMPTZ NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(idempotency_key, alert_type)
        )
      `);
      await client.query(`
        CREATE TABLE IF NOT EXISTS scheduler_site_control_state (
          site TEXT PRIMARY KEY,
          effective_max_records INTEGER NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await client.query(`
        CREATE TABLE IF NOT EXISTS scheduler_site_connectivity_state (
          site TEXT PRIMARY KEY,
          status TEXT NOT NULL CHECK(status IN ('healthy', 'degraded', 'blocked', 'probing')),
          opened_at TIMESTAMPTZ,
          last_success_at TIMESTAMPTZ,
          last_failure_at TIMESTAMPTZ,
          policy_block_count INTEGER NOT NULL DEFAULT 0,
          timeout_count INTEGER NOT NULL DEFAULT 0,
          empty_result_count INTEGER NOT NULL DEFAULT 0,
          window_started_at TIMESTAMPTZ,
          next_probe_at TIMESTAMPTZ,
          consecutive_probe_successes INTEGER NOT NULL DEFAULT 0,
          last_failure_reason TEXT,
          last_alerted_at TIMESTAMPTZ,
          last_recovery_alert_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  private async queryRow<T extends QueryResultRow>(sql: string, params: unknown[] = []): Promise<T | undefined> {
    const result = await this.pool.query<T>(sql, params);
    return result.rows[0];
  }

  async insertRun(run: ScheduledRunRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO scheduled_runs (
        id, site, idempotency_key, slot_time, trigger_source, started_at, finished_at, status,
        records_scraped, records_skipped, rows_uploaded,
        amount_found_count, amount_missing_count, amount_coverage_pct, ocr_success_pct, row_fail_pct,
        deadline_hit, effective_max_records, partial, error, failure_class
      ) VALUES (
        $1, $2, $3, $4, $5, $6::timestamptz, $7::timestamptz, $8,
        $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
      )`,
      [
        run.id,
        run.site,
        run.idempotency_key,
        run.slot_time,
        run.trigger_source,
        run.started_at,
        run.finished_at ?? null,
        run.status,
        run.records_scraped,
        run.records_skipped,
        run.rows_uploaded,
        run.amount_found_count,
        run.amount_missing_count,
        run.amount_coverage_pct,
        run.ocr_success_pct,
        run.row_fail_pct,
        run.deadline_hit,
        run.effective_max_records,
        run.partial,
        run.error ?? null,
        run.failure_class ?? null,
      ]
    );
  }

  async updateRun(run: ScheduledRunRecord): Promise<void> {
    await this.pool.query(
      `UPDATE scheduled_runs
       SET site = $1,
           finished_at = $2::timestamptz,
           status = $3,
           records_scraped = $4,
           records_skipped = $5,
           rows_uploaded = $6,
           amount_found_count = $7,
           amount_missing_count = $8,
           amount_coverage_pct = $9,
           ocr_success_pct = $10,
           row_fail_pct = $11,
           deadline_hit = $12,
           effective_max_records = $13,
           partial = $14,
           error = $15,
           failure_class = $16,
           updated_at = NOW()
       WHERE id = $17`,
      [
        run.site,
        run.finished_at ?? null,
        run.status,
        run.records_scraped,
        run.records_skipped,
        run.rows_uploaded,
        run.amount_found_count,
        run.amount_missing_count,
        run.amount_coverage_pct,
        run.ocr_success_pct,
        run.row_fail_pct,
        run.deadline_hit,
        run.effective_max_records,
        run.partial,
        run.error ?? null,
        run.failure_class ?? null,
        run.id,
      ]
    );
  }

  async getByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null> {
    return normalizeScheduledRunRecord(
      await this.queryRow<Record<string, unknown>>(
        'SELECT * FROM scheduled_runs WHERE idempotency_key = $1 ORDER BY created_at DESC LIMIT 1',
        [idempotencyKey]
      )
    );
  }

  async getMostRecentRun(site?: SupportedSite): Promise<ScheduledRunRecord | null> {
    const sql = site
      ? 'SELECT * FROM scheduled_runs WHERE site = $1 ORDER BY started_at DESC LIMIT 1'
      : 'SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT 1';
    return normalizeScheduledRunRecord(await this.queryRow<Record<string, unknown>>(sql, site ? [site] : []));
  }

  async getSuccessfulRunByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null> {
    return normalizeScheduledRunRecord(
      await this.queryRow<Record<string, unknown>>(
        "SELECT * FROM scheduled_runs WHERE idempotency_key = $1 AND status = 'success' ORDER BY created_at DESC LIMIT 1",
        [idempotencyKey]
      )
    );
  }

  async getRunHistory(limit = 50, site?: SupportedSite): Promise<ScheduledRunRecord[]> {
    const result = site
      ? await this.pool.query<Record<string, unknown>>(
        'SELECT * FROM scheduled_runs WHERE site = $1 ORDER BY started_at DESC LIMIT $2',
        [site, limit]
      )
      : await this.pool.query<Record<string, unknown>>(
        'SELECT * FROM scheduled_runs ORDER BY started_at DESC LIMIT $1',
        [limit]
      );
    return result.rows.map((row) => normalizeScheduledRunRecord(row)).filter(Boolean) as ScheduledRunRecord[];
  }

  async getRecentSuccessfulRuns(site: SupportedSite, limit = 4): Promise<ScheduledRunRecord[]> {
    const result = await this.pool.query<Record<string, unknown>>(
      "SELECT * FROM scheduled_runs WHERE site = $1 AND status = 'success' ORDER BY started_at DESC LIMIT $2",
      [site, limit]
    );
    return result.rows.map((row) => normalizeScheduledRunRecord(row)).filter(Boolean) as ScheduledRunRecord[];
  }

  async upsertControlState(site: SupportedSite, effectiveMaxRecords: number): Promise<void> {
    await this.pool.query(
      `INSERT INTO scheduler_site_control_state (site, effective_max_records, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT(site) DO UPDATE SET effective_max_records = EXCLUDED.effective_max_records, updated_at = NOW()`,
      [site, effectiveMaxRecords]
    );
  }

  async getControlState(site: SupportedSite): Promise<ScheduleControlState | null> {
    return normalizeControlState(
      await this.queryRow<Record<string, unknown>>(
        'SELECT site, effective_max_records, updated_at FROM scheduler_site_control_state WHERE site = $1',
        [site]
      )
    );
  }

  async upsertConnectivityState(state: SiteConnectivityState): Promise<void> {
    await this.pool.query(
      `INSERT INTO scheduler_site_connectivity_state (
        site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
        empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
        last_failure_reason, last_alerted_at, last_recovery_alert_at, updated_at
      ) VALUES (
        $1, $2, $3::timestamptz, $4::timestamptz, $5::timestamptz, $6, $7, $8, $9::timestamptz, $10::timestamptz,
        $11, $12, $13::timestamptz, $14::timestamptz, NOW()
      )
      ON CONFLICT(site) DO UPDATE SET
        status = EXCLUDED.status,
        opened_at = EXCLUDED.opened_at,
        last_success_at = EXCLUDED.last_success_at,
        last_failure_at = EXCLUDED.last_failure_at,
        policy_block_count = EXCLUDED.policy_block_count,
        timeout_count = EXCLUDED.timeout_count,
        empty_result_count = EXCLUDED.empty_result_count,
        window_started_at = EXCLUDED.window_started_at,
        next_probe_at = EXCLUDED.next_probe_at,
        consecutive_probe_successes = EXCLUDED.consecutive_probe_successes,
        last_failure_reason = EXCLUDED.last_failure_reason,
        last_alerted_at = EXCLUDED.last_alerted_at,
        last_recovery_alert_at = EXCLUDED.last_recovery_alert_at,
        updated_at = NOW()`,
      [
        state.site,
        state.status,
        state.opened_at ?? null,
        state.last_success_at ?? null,
        state.last_failure_at ?? null,
        state.policy_block_count,
        state.timeout_count,
        state.empty_result_count,
        state.window_started_at ?? null,
        state.next_probe_at ?? null,
        state.consecutive_probe_successes,
        state.last_failure_reason ?? null,
        state.last_alerted_at ?? null,
        state.last_recovery_alert_at ?? null,
      ]
    );
  }

  async getConnectivityState(site: SupportedSite): Promise<SiteConnectivityState | null> {
    return normalizeConnectivityState(
      await this.queryRow<Record<string, unknown>>(
        `SELECT site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
                empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
                last_failure_reason, last_alerted_at, last_recovery_alert_at
         FROM scheduler_site_connectivity_state
         WHERE site = $1`,
        [site]
      )
    );
  }

  async listConnectivityStates(): Promise<SiteConnectivityState[]> {
    const result = await this.pool.query<Record<string, unknown>>(
      `SELECT site, status, opened_at, last_success_at, last_failure_at, policy_block_count, timeout_count,
              empty_result_count, window_started_at, next_probe_at, consecutive_probe_successes,
              last_failure_reason, last_alerted_at, last_recovery_alert_at
       FROM scheduler_site_connectivity_state
       ORDER BY site`
    );
    return result.rows.map((row) => normalizeConnectivityState(row)).filter(Boolean) as SiteConnectivityState[];
  }

  async insertMissedAlert(alert: MissedAlertRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO scheduler_alerts (site, idempotency_key, slot, expected_by, alert_type)
       VALUES ($1, $2, $3, $4::timestamptz, 'missed_run')
       ON CONFLICT(idempotency_key, alert_type) DO NOTHING`,
      [alert.site, alert.idempotency_key, alert.slot, alert.expected_by]
    );
  }

  async getMissedAlertByKey(idempotencyKey: string): Promise<MissedAlertRecord | null> {
    return normalizeMissedAlertRecord(
      await this.queryRow<Record<string, unknown>>(
        "SELECT site, idempotency_key, slot, expected_by FROM scheduler_alerts WHERE idempotency_key = $1 AND alert_type = 'missed_run' LIMIT 1",
        [idempotencyKey]
      )
    );
  }
}

export class ScheduledRunStore {
  private backend: SchedulerStoreBackend;
  private ready: Promise<void>;

  constructor() {
    this.backend = getSchedulerStoreBackendKind() === 'postgres'
      ? new PostgresSchedulerStoreBackend()
      : new SQLiteSchedulerStoreBackend();
    this.ready = this.backend.initialize();
  }

  private async ensureReady(): Promise<void> {
    await this.ready;
  }

  async close(): Promise<void> {
    await this.ensureReady();
    await this.backend.close();
  }

  async insertRun(run: ScheduledRunRecord): Promise<void> {
    await this.ensureReady();
    await this.backend.insertRun(run);
  }

  async updateRun(run: ScheduledRunRecord): Promise<void> {
    await this.ensureReady();
    await this.backend.updateRun(run);
  }

  async getByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null> {
    await this.ensureReady();
    return this.backend.getByIdempotencyKey(idempotencyKey);
  }

  async getMostRecentRun(site?: SupportedSite): Promise<ScheduledRunRecord | null> {
    await this.ensureReady();
    return this.backend.getMostRecentRun(site);
  }

  async getSuccessfulRunByIdempotencyKey(idempotencyKey: string): Promise<ScheduledRunRecord | null> {
    await this.ensureReady();
    return this.backend.getSuccessfulRunByIdempotencyKey(idempotencyKey);
  }

  async getRunHistory(limit = 50, site?: SupportedSite): Promise<ScheduledRunRecord[]> {
    await this.ensureReady();
    return this.backend.getRunHistory(limit, site);
  }

  async getRecentSuccessfulRuns(site: SupportedSite, limit = 4): Promise<ScheduledRunRecord[]> {
    await this.ensureReady();
    return this.backend.getRecentSuccessfulRuns(site, limit);
  }

  async upsertControlState(site: SupportedSite, effectiveMaxRecords: number): Promise<void> {
    await this.ensureReady();
    await this.backend.upsertControlState(site, effectiveMaxRecords);
  }

  async getControlState(site: SupportedSite): Promise<ScheduleControlState | null> {
    await this.ensureReady();
    return this.backend.getControlState(site);
  }

  async upsertConnectivityState(state: SiteConnectivityState): Promise<void> {
    await this.ensureReady();
    await this.backend.upsertConnectivityState(state);
  }

  async getConnectivityState(site: SupportedSite): Promise<SiteConnectivityState | null> {
    await this.ensureReady();
    return this.backend.getConnectivityState(site);
  }

  async listConnectivityStates(): Promise<SiteConnectivityState[]> {
    await this.ensureReady();
    return this.backend.listConnectivityStates();
  }

  async insertMissedAlert(alert: MissedAlertRecord): Promise<void> {
    await this.ensureReady();
    await this.backend.insertMissedAlert(alert);
  }

  async getMissedAlertByKey(idempotencyKey: string): Promise<MissedAlertRecord | null> {
    await this.ensureReady();
    return this.backend.getMissedAlertByKey(idempotencyKey);
  }
}

export async function getSchedulerStoreReadiness(): Promise<SchedulerStoreReadiness> {
  if (getSchedulerStoreBackendKind() === 'sqlite') {
    try {
      const dbPath = resolveDbPath();
      const db = new Database(dbPath, { readonly: true });
      db.prepare('SELECT 1').get();
      const runsTable = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scheduled_runs'").get() as
        | { name: string }
        | undefined;
      db.close();

      if (!runsTable) {
        return {
          backend: 'sqlite',
          ok: false,
          detail: 'SQLite reachable but scheduled_runs table is missing. Run node src/queue/init-db.js.',
        };
      }

      return { backend: 'sqlite', ok: true };
    } catch (err: unknown) {
      return {
        backend: 'sqlite',
        ok: false,
        detail: err instanceof Error ? err.message : String(err),
      };
    }
  }

  try {
    const pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.DATABASE_URL?.includes('sslmode=disable') ? undefined : { rejectUnauthorized: false },
    });
    await pool.query('SELECT 1');
    await pool.end();
    return { backend: 'postgres', ok: true };
  } catch (err: unknown) {
    return {
      backend: 'postgres',
      ok: false,
      detail: err instanceof Error ? err.message : String(err),
    };
  }
}
