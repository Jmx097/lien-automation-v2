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
  attempt_count?: number;
  max_attempts?: number;
  retried?: number;
  retry_exhausted?: number;
  source_tab_title?: string;
  master_tab_title?: string;
  review_tab_title?: string;
  quarantined_row_count?: number;
  current_run_quarantined_row_count?: number;
  current_run_conflict_row_count?: number;
  retained_prior_review_row_count?: number;
  review_reason_counts_json?: string;
  requested_date_start?: string;
  requested_date_end?: string;
  discovered_count?: number;
  returned_count?: number;
  filtered_out_count?: number;
  returned_min_filing_date?: string;
  returned_max_filing_date?: string;
  upstream_min_filing_date?: string;
  upstream_max_filing_date?: string;
  partial_reason?: string;
  artifact_retrieval_enabled?: number;
  artifact_fetch_coverage_pct?: number;
  enrichment_mode?: string;
  artifact_readiness_not_met?: number;
  enriched_record_count?: number;
  partial_record_count?: number;
  new_master_row_count?: number;
  purged_review_row_count?: number;
  lead_alert_attempted?: number;
  lead_alert_delivered?: number;
  master_fallback_used?: number;
  anomaly_detected?: number;
  debug_artifact_json?: string;
  sla_score_pct?: number;
  sla_pass?: number;
  sla_policy_version?: string;
  sla_components_json?: string;
}

export type SchedulerAlertType = 'missed_run' | 'quality_anomaly' | 'sla_breach' | 'cadence_breach';

interface MissedAlertRecord {
  site: SupportedSite;
  idempotency_key: string;
  slot: 'morning' | 'afternoon' | 'evening';
  expected_by: string;
}

export interface SchedulerAlertRecord {
  site: SupportedSite;
  idempotency_key: string;
  slot: 'morning' | 'afternoon' | 'evening';
  alert_type: SchedulerAlertType;
  expected_by: string;
  run_id?: string;
  metrics_triggered?: string[];
  summary?: string;
  baseline_records_scraped?: number;
  baseline_amount_coverage_pct?: number;
  baseline_ocr_success_pct?: number;
  baseline_row_fail_pct?: number;
  records_scraped?: number;
  amount_coverage_pct?: number;
  ocr_success_pct?: number;
  row_fail_pct?: number;
  detected_at?: string;
}

export interface QualityAnomalyAlertRecord {
  site: SupportedSite;
  idempotency_key: string;
  run_id: string;
  slot: 'morning' | 'afternoon' | 'evening';
  metrics_triggered: string[];
  summary: string;
  baseline_records_scraped: number;
  baseline_amount_coverage_pct: number;
  baseline_ocr_success_pct: number;
  baseline_row_fail_pct: number;
  records_scraped: number;
  amount_coverage_pct: number;
  ocr_success_pct: number;
  row_fail_pct: number;
  detected_at: string;
}

export interface ScheduleControlState {
  site: SupportedSite;
  effective_max_records: number;
  updated_at: string;
}

export interface SiteStateArtifactRecord {
  site: SupportedSite;
  artifact_key: string;
  payload_json: string;
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
  upsertSiteStateArtifact(record: SiteStateArtifactRecord): Promise<void>;
  getSiteStateArtifact(site: SupportedSite, artifactKey: string): Promise<SiteStateArtifactRecord | null>;
  insertMissedAlert(alert: MissedAlertRecord): Promise<void>;
  getMissedAlertByKey(idempotencyKey: string): Promise<MissedAlertRecord | null>;
  insertSchedulerAlert(alert: SchedulerAlertRecord): Promise<void>;
  getAlertByKey(idempotencyKey: string, alertType: SchedulerAlertType): Promise<SchedulerAlertRecord | null>;
  insertQualityAnomalyAlert(alert: QualityAnomalyAlertRecord): Promise<void>;
  getLatestQualityAnomalyAlert(site: SupportedSite): Promise<QualityAnomalyAlertRecord | null>;
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
    attempt_count: toNumber(row.attempt_count || 1),
    max_attempts: toNumber(row.max_attempts || 1),
    retried: toNumber(row.retried || 0),
    retry_exhausted: toNumber(row.retry_exhausted || 0),
    source_tab_title: row.source_tab_title == null ? undefined : String(row.source_tab_title),
    master_tab_title: row.master_tab_title == null ? undefined : String(row.master_tab_title),
    review_tab_title: row.review_tab_title == null ? undefined : String(row.review_tab_title),
    quarantined_row_count: toNumber(row.quarantined_row_count || 0),
    current_run_quarantined_row_count: toNumber(row.current_run_quarantined_row_count || 0),
    current_run_conflict_row_count: toNumber(row.current_run_conflict_row_count || 0),
    retained_prior_review_row_count: toNumber(row.retained_prior_review_row_count || 0),
    review_reason_counts_json: row.review_reason_counts_json == null ? undefined : String(row.review_reason_counts_json),
    requested_date_start: row.requested_date_start == null ? undefined : String(row.requested_date_start),
    requested_date_end: row.requested_date_end == null ? undefined : String(row.requested_date_end),
    discovered_count: toNumber(row.discovered_count || 0),
    returned_count: toNumber(row.returned_count || 0),
    filtered_out_count: toNumber(row.filtered_out_count || 0),
    returned_min_filing_date: row.returned_min_filing_date == null ? undefined : String(row.returned_min_filing_date),
    returned_max_filing_date: row.returned_max_filing_date == null ? undefined : String(row.returned_max_filing_date),
    upstream_min_filing_date: row.upstream_min_filing_date == null ? undefined : String(row.upstream_min_filing_date),
    upstream_max_filing_date: row.upstream_max_filing_date == null ? undefined : String(row.upstream_max_filing_date),
    partial_reason: row.partial_reason == null ? undefined : String(row.partial_reason),
    artifact_retrieval_enabled: toNumber(row.artifact_retrieval_enabled || 0),
    artifact_fetch_coverage_pct: toNumber(row.artifact_fetch_coverage_pct || 0),
    enrichment_mode: row.enrichment_mode == null ? undefined : String(row.enrichment_mode),
    artifact_readiness_not_met: toNumber(row.artifact_readiness_not_met || 0),
    enriched_record_count: toNumber(row.enriched_record_count || 0),
    partial_record_count: toNumber(row.partial_record_count || 0),
    new_master_row_count: toNumber(row.new_master_row_count || 0),
    purged_review_row_count: toNumber(row.purged_review_row_count || 0),
    lead_alert_attempted: toNumber(row.lead_alert_attempted || 0),
    lead_alert_delivered: toNumber(row.lead_alert_delivered || 0),
    master_fallback_used: toNumber(row.master_fallback_used || 0),
    anomaly_detected: toNumber(row.anomaly_detected || 0),
    debug_artifact_json: row.debug_artifact_json == null ? undefined : String(row.debug_artifact_json),
    sla_score_pct: toNumber(row.sla_score_pct || 0),
    sla_pass: toNumber(row.sla_pass || 0),
    sla_policy_version: row.sla_policy_version == null ? undefined : String(row.sla_policy_version),
    sla_components_json: row.sla_components_json == null ? undefined : String(row.sla_components_json),
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

function normalizeSchedulerAlertRecord(row: Record<string, unknown> | undefined): SchedulerAlertRecord | null {
  if (!row) return null;
  return {
    site: String(row.site) as SupportedSite,
    idempotency_key: String(row.idempotency_key),
    slot: String(row.slot) as SchedulerAlertRecord['slot'],
    alert_type: String(row.alert_type) as SchedulerAlertType,
    expected_by: toIso(row.expected_by) ?? '',
    run_id: row.run_id == null ? undefined : String(row.run_id),
    metrics_triggered: parseMetricsTriggered(row.metrics_triggered),
    summary: row.summary == null ? undefined : String(row.summary),
    baseline_records_scraped: row.baseline_records_scraped == null ? undefined : toNumber(row.baseline_records_scraped),
    baseline_amount_coverage_pct: row.baseline_amount_coverage_pct == null ? undefined : toNumber(row.baseline_amount_coverage_pct),
    baseline_ocr_success_pct: row.baseline_ocr_success_pct == null ? undefined : toNumber(row.baseline_ocr_success_pct),
    baseline_row_fail_pct: row.baseline_row_fail_pct == null ? undefined : toNumber(row.baseline_row_fail_pct),
    records_scraped: row.records_scraped == null ? undefined : toNumber(row.records_scraped),
    amount_coverage_pct: row.amount_coverage_pct == null ? undefined : toNumber(row.amount_coverage_pct),
    ocr_success_pct: row.ocr_success_pct == null ? undefined : toNumber(row.ocr_success_pct),
    row_fail_pct: row.row_fail_pct == null ? undefined : toNumber(row.row_fail_pct),
    detected_at: toIso(row.detected_at),
  };
}

function normalizeSiteStateArtifactRecord(row: Record<string, unknown> | undefined): SiteStateArtifactRecord | null {
  if (!row) return null;
  return {
    site: String(row.site) as SupportedSite,
    artifact_key: String(row.artifact_key),
    payload_json: String(row.payload_json ?? ''),
    updated_at: toIso(row.updated_at) ?? '',
  };
}

function parseMetricsTriggered(value: unknown): string[] {
  if (typeof value !== 'string' || value.trim() === '') return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((entry) => String(entry)) : [];
  } catch {
    return [];
  }
}

function normalizeQualityAnomalyAlertRecord(row: Record<string, unknown> | undefined): QualityAnomalyAlertRecord | null {
  if (!row) return null;
  return {
    site: String(row.site) as SupportedSite,
    idempotency_key: String(row.idempotency_key),
    run_id: String(row.run_id),
    slot: String(row.slot) as QualityAnomalyAlertRecord['slot'],
    metrics_triggered: parseMetricsTriggered(row.metrics_triggered),
    summary: String(row.summary ?? ''),
    baseline_records_scraped: toNumber(row.baseline_records_scraped),
    baseline_amount_coverage_pct: toNumber(row.baseline_amount_coverage_pct),
    baseline_ocr_success_pct: toNumber(row.baseline_ocr_success_pct),
    baseline_row_fail_pct: toNumber(row.baseline_row_fail_pct),
    records_scraped: toNumber(row.records_scraped),
    amount_coverage_pct: toNumber(row.amount_coverage_pct),
    ocr_success_pct: toNumber(row.ocr_success_pct),
    row_fail_pct: toNumber(row.row_fail_pct),
    detected_at: toIso(row.detected_at) ?? '',
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
        attempt_count INTEGER NOT NULL DEFAULT 1,
        max_attempts INTEGER NOT NULL DEFAULT 1,
        retried INTEGER NOT NULL DEFAULT 0,
        retry_exhausted INTEGER NOT NULL DEFAULT 0,
        source_tab_title TEXT,
        master_tab_title TEXT,
        review_tab_title TEXT,
        quarantined_row_count INTEGER NOT NULL DEFAULT 0,
        current_run_quarantined_row_count INTEGER NOT NULL DEFAULT 0,
        current_run_conflict_row_count INTEGER NOT NULL DEFAULT 0,
        retained_prior_review_row_count INTEGER NOT NULL DEFAULT 0,
        review_reason_counts_json TEXT,
        requested_date_start TEXT,
        requested_date_end TEXT,
        discovered_count INTEGER NOT NULL DEFAULT 0,
        returned_count INTEGER NOT NULL DEFAULT 0,
        filtered_out_count INTEGER NOT NULL DEFAULT 0,
        returned_min_filing_date TEXT,
        returned_max_filing_date TEXT,
        upstream_min_filing_date TEXT,
        upstream_max_filing_date TEXT,
        partial_reason TEXT,
        artifact_retrieval_enabled INTEGER NOT NULL DEFAULT 0,
        artifact_fetch_coverage_pct REAL NOT NULL DEFAULT 0,
        enrichment_mode TEXT,
        artifact_readiness_not_met INTEGER NOT NULL DEFAULT 0,
        enriched_record_count INTEGER NOT NULL DEFAULT 0,
        partial_record_count INTEGER NOT NULL DEFAULT 0,
        new_master_row_count INTEGER NOT NULL DEFAULT 0,
        purged_review_row_count INTEGER NOT NULL DEFAULT 0,
        lead_alert_attempted INTEGER NOT NULL DEFAULT 0,
        lead_alert_delivered INTEGER NOT NULL DEFAULT 0,
        master_fallback_used INTEGER NOT NULL DEFAULT 0,
        anomaly_detected INTEGER NOT NULL DEFAULT 0,
        debug_artifact_json TEXT,
        sla_score_pct REAL NOT NULL DEFAULT 0,
        sla_pass INTEGER NOT NULL DEFAULT 0,
        sla_policy_version TEXT,
        sla_components_json TEXT,
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
        slot TEXT NOT NULL CHECK(slot IN ('morning', 'afternoon', 'evening')),
        alert_type TEXT NOT NULL CHECK(alert_type IN ('missed_run', 'quality_anomaly', 'sla_breach', 'cadence_breach')),
        expected_by TEXT NOT NULL,
        run_id TEXT,
        metrics_triggered TEXT,
        summary TEXT,
        baseline_records_scraped REAL,
        baseline_amount_coverage_pct REAL,
        baseline_ocr_success_pct REAL,
        baseline_row_fail_pct REAL,
        records_scraped REAL,
        amount_coverage_pct REAL,
        ocr_success_pct REAL,
        row_fail_pct REAL,
        detected_at TEXT,
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
    `
      CREATE TABLE IF NOT EXISTS scheduler_site_artifacts (
        site TEXT NOT NULL,
        artifact_key TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (site, artifact_key)
      )
    `,
  ];
}

function recreateSchedulerAlertsTable(db: Database.Database): void {
  const columns = db.prepare("PRAGMA table_info('scheduler_alerts')").all() as Array<{ name: string }>;
  const hasColumn = (name: string) => columns.some((column) => column.name === name);

  db.exec(`
    ALTER TABLE scheduler_alerts RENAME TO scheduler_alerts_legacy;

    CREATE TABLE scheduler_alerts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      site TEXT NOT NULL DEFAULT 'ca_sos',
      idempotency_key TEXT NOT NULL,
      slot TEXT NOT NULL CHECK(slot IN ('morning', 'afternoon', 'evening')),
      alert_type TEXT NOT NULL CHECK(alert_type IN ('missed_run', 'quality_anomaly', 'sla_breach', 'cadence_breach')),
      expected_by TEXT NOT NULL,
      run_id TEXT,
      metrics_triggered TEXT,
      summary TEXT,
      baseline_records_scraped REAL,
      baseline_amount_coverage_pct REAL,
      baseline_ocr_success_pct REAL,
      baseline_row_fail_pct REAL,
      records_scraped REAL,
      amount_coverage_pct REAL,
      ocr_success_pct REAL,
      row_fail_pct REAL,
      detected_at TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(idempotency_key, alert_type)
    );
  `);

  db.prepare(
    `INSERT INTO scheduler_alerts (
      id, site, idempotency_key, slot, alert_type, expected_by, run_id, metrics_triggered, summary,
      baseline_records_scraped, baseline_amount_coverage_pct, baseline_ocr_success_pct, baseline_row_fail_pct,
      records_scraped, amount_coverage_pct, ocr_success_pct, row_fail_pct, detected_at, created_at
    )
    SELECT
      id,
      ${hasColumn('site') ? 'site' : "'ca_sos'"},
      idempotency_key,
      slot,
      alert_type,
      expected_by,
      ${hasColumn('run_id') ? 'run_id' : 'NULL'},
      ${hasColumn('metrics_triggered') ? 'metrics_triggered' : 'NULL'},
      ${hasColumn('summary') ? 'summary' : 'NULL'},
      ${hasColumn('baseline_records_scraped') ? 'baseline_records_scraped' : 'NULL'},
      ${hasColumn('baseline_amount_coverage_pct') ? 'baseline_amount_coverage_pct' : 'NULL'},
      ${hasColumn('baseline_ocr_success_pct') ? 'baseline_ocr_success_pct' : 'NULL'},
      ${hasColumn('baseline_row_fail_pct') ? 'baseline_row_fail_pct' : 'NULL'},
      ${hasColumn('records_scraped') ? 'records_scraped' : 'NULL'},
      ${hasColumn('amount_coverage_pct') ? 'amount_coverage_pct' : 'NULL'},
      ${hasColumn('ocr_success_pct') ? 'ocr_success_pct' : 'NULL'},
      ${hasColumn('row_fail_pct') ? 'row_fail_pct' : 'NULL'},
      ${hasColumn('detected_at') ? 'detected_at' : 'NULL'},
      ${hasColumn('created_at') ? 'created_at' : 'CURRENT_TIMESTAMP'}
    FROM scheduler_alerts_legacy`
  ).run();

  db.exec('DROP TABLE scheduler_alerts_legacy;');
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
    const alertsTable = db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'scheduler_alerts'").get() as
      | { sql: string | null }
      | undefined;
    const alertsNeedRecreate = !alertsTable?.sql ||
      !alertsTable.sql.includes("'quality_anomaly'") ||
      !alertsTable.sql.includes("'sla_breach'") ||
      !alertsTable.sql.includes("'cadence_breach'") ||
      !alertsTable.sql.includes("'evening'") ||
      !alertColumns.some((column) => column.name === 'run_id') ||
      !alertColumns.some((column) => column.name === 'detected_at');
    if (alertsNeedRecreate) {
      recreateSchedulerAlertsTable(db);
    }
    const refreshedAlertColumns = db.prepare("PRAGMA table_info('scheduler_alerts')").all() as Array<{ name: string }>;
    if (!refreshedAlertColumns.some((column) => column.name === 'site')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN site TEXT NOT NULL DEFAULT 'ca_sos'").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'run_id')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN run_id TEXT").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'metrics_triggered')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN metrics_triggered TEXT").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'summary')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN summary TEXT").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'baseline_records_scraped')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN baseline_records_scraped REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'baseline_amount_coverage_pct')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN baseline_amount_coverage_pct REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'baseline_ocr_success_pct')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN baseline_ocr_success_pct REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'baseline_row_fail_pct')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN baseline_row_fail_pct REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'records_scraped')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN records_scraped REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'amount_coverage_pct')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN amount_coverage_pct REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'ocr_success_pct')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN ocr_success_pct REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'row_fail_pct')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN row_fail_pct REAL").run();
    }
    if (!refreshedAlertColumns.some((column) => column.name === 'detected_at')) {
      db.prepare("ALTER TABLE scheduler_alerts ADD COLUMN detected_at TEXT").run();
    }

    if (!scheduledRunColumns.some((column) => column.name === 'failure_class')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN failure_class TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'attempt_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 1").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'max_attempts')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'retried')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN retried INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'retry_exhausted')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN retry_exhausted INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'source_tab_title')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN source_tab_title TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'master_tab_title')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN master_tab_title TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'review_tab_title')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN review_tab_title TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'quarantined_row_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN quarantined_row_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'current_run_quarantined_row_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN current_run_quarantined_row_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'current_run_conflict_row_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN current_run_conflict_row_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'retained_prior_review_row_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN retained_prior_review_row_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'review_reason_counts_json')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN review_reason_counts_json TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'requested_date_start')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN requested_date_start TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'requested_date_end')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN requested_date_end TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'discovered_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN discovered_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'returned_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN returned_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'filtered_out_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN filtered_out_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'returned_min_filing_date')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN returned_min_filing_date TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'returned_max_filing_date')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN returned_max_filing_date TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'upstream_min_filing_date')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN upstream_min_filing_date TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'upstream_max_filing_date')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN upstream_max_filing_date TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'partial_reason')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN partial_reason TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'artifact_retrieval_enabled')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN artifact_retrieval_enabled INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'artifact_fetch_coverage_pct')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN artifact_fetch_coverage_pct REAL NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'enrichment_mode')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN enrichment_mode TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'artifact_readiness_not_met')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN artifact_readiness_not_met INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'enriched_record_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN enriched_record_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'partial_record_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN partial_record_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'new_master_row_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN new_master_row_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'purged_review_row_count')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN purged_review_row_count INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'lead_alert_attempted')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN lead_alert_attempted INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'lead_alert_delivered')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN lead_alert_delivered INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'master_fallback_used')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN master_fallback_used INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'anomaly_detected')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN anomaly_detected INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'debug_artifact_json')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN debug_artifact_json TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'sla_score_pct')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN sla_score_pct REAL NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'sla_pass')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN sla_pass INTEGER NOT NULL DEFAULT 0").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'sla_policy_version')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN sla_policy_version TEXT").run();
    }
    if (!scheduledRunColumns.some((column) => column.name === 'sla_components_json')) {
      db.prepare("ALTER TABLE scheduled_runs ADD COLUMN sla_components_json TEXT").run();
    }
  }

  async insertRun(run: ScheduledRunRecord): Promise<void> {
    const entries: Array<[string, unknown]> = [
      ['id', run.id],
      ['site', run.site],
      ['idempotency_key', run.idempotency_key],
      ['slot_time', run.slot_time],
      ['trigger_source', run.trigger_source],
      ['started_at', run.started_at],
      ['finished_at', run.finished_at ?? null],
      ['status', run.status],
      ['records_scraped', run.records_scraped],
      ['records_skipped', run.records_skipped],
      ['rows_uploaded', run.rows_uploaded],
      ['amount_found_count', run.amount_found_count],
      ['amount_missing_count', run.amount_missing_count],
      ['amount_coverage_pct', run.amount_coverage_pct],
      ['ocr_success_pct', run.ocr_success_pct],
      ['row_fail_pct', run.row_fail_pct],
      ['deadline_hit', run.deadline_hit],
      ['effective_max_records', run.effective_max_records],
      ['partial', run.partial],
      ['error', run.error ?? null],
      ['failure_class', run.failure_class ?? null],
      ['attempt_count', run.attempt_count ?? 1],
      ['max_attempts', run.max_attempts ?? 1],
      ['retried', run.retried ?? 0],
      ['retry_exhausted', run.retry_exhausted ?? 0],
      ['source_tab_title', run.source_tab_title ?? null],
      ['master_tab_title', run.master_tab_title ?? null],
      ['review_tab_title', run.review_tab_title ?? null],
      ['quarantined_row_count', run.quarantined_row_count ?? 0],
      ['current_run_quarantined_row_count', run.current_run_quarantined_row_count ?? 0],
      ['current_run_conflict_row_count', run.current_run_conflict_row_count ?? 0],
      ['retained_prior_review_row_count', run.retained_prior_review_row_count ?? 0],
      ['review_reason_counts_json', run.review_reason_counts_json ?? null],
      ['requested_date_start', run.requested_date_start ?? null],
      ['requested_date_end', run.requested_date_end ?? null],
      ['discovered_count', run.discovered_count ?? 0],
      ['returned_count', run.returned_count ?? 0],
      ['filtered_out_count', run.filtered_out_count ?? 0],
      ['returned_min_filing_date', run.returned_min_filing_date ?? null],
      ['returned_max_filing_date', run.returned_max_filing_date ?? null],
      ['upstream_min_filing_date', run.upstream_min_filing_date ?? null],
      ['upstream_max_filing_date', run.upstream_max_filing_date ?? null],
      ['partial_reason', run.partial_reason ?? null],
      ['artifact_retrieval_enabled', run.artifact_retrieval_enabled ?? 0],
      ['artifact_fetch_coverage_pct', run.artifact_fetch_coverage_pct ?? 0],
      ['enrichment_mode', run.enrichment_mode ?? null],
      ['artifact_readiness_not_met', run.artifact_readiness_not_met ?? 0],
      ['enriched_record_count', run.enriched_record_count ?? 0],
      ['partial_record_count', run.partial_record_count ?? 0],
      ['new_master_row_count', run.new_master_row_count ?? 0],
      ['purged_review_row_count', run.purged_review_row_count ?? 0],
      ['lead_alert_attempted', run.lead_alert_attempted ?? 0],
      ['lead_alert_delivered', run.lead_alert_delivered ?? 0],
      ['master_fallback_used', run.master_fallback_used ?? 0],
      ['anomaly_detected', run.anomaly_detected ?? 0],
      ['debug_artifact_json', run.debug_artifact_json ?? null],
      ['sla_score_pct', run.sla_score_pct ?? 0],
      ['sla_pass', run.sla_pass ?? 0],
      ['sla_policy_version', run.sla_policy_version ?? null],
      ['sla_components_json', run.sla_components_json ?? null],
    ];
    const columns = entries.map(([column]) => column);
    const values = entries.map(([, value]) => value);
    this.getDb()
      .prepare(`INSERT INTO scheduled_runs (${columns.join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`)
      .run(...values);
  }

  async updateRun(run: ScheduledRunRecord): Promise<void> {
    const entries: Array<[string, unknown]> = [
      ['site', run.site],
      ['finished_at', run.finished_at ?? null],
      ['status', run.status],
      ['records_scraped', run.records_scraped],
      ['records_skipped', run.records_skipped],
      ['rows_uploaded', run.rows_uploaded],
      ['amount_found_count', run.amount_found_count],
      ['amount_missing_count', run.amount_missing_count],
      ['amount_coverage_pct', run.amount_coverage_pct],
      ['ocr_success_pct', run.ocr_success_pct],
      ['row_fail_pct', run.row_fail_pct],
      ['deadline_hit', run.deadline_hit],
      ['effective_max_records', run.effective_max_records],
      ['partial', run.partial],
      ['error', run.error ?? null],
      ['failure_class', run.failure_class ?? null],
      ['attempt_count', run.attempt_count ?? 1],
      ['max_attempts', run.max_attempts ?? 1],
      ['retried', run.retried ?? 0],
      ['retry_exhausted', run.retry_exhausted ?? 0],
      ['source_tab_title', run.source_tab_title ?? null],
      ['master_tab_title', run.master_tab_title ?? null],
      ['review_tab_title', run.review_tab_title ?? null],
      ['quarantined_row_count', run.quarantined_row_count ?? 0],
      ['current_run_quarantined_row_count', run.current_run_quarantined_row_count ?? 0],
      ['current_run_conflict_row_count', run.current_run_conflict_row_count ?? 0],
      ['retained_prior_review_row_count', run.retained_prior_review_row_count ?? 0],
      ['review_reason_counts_json', run.review_reason_counts_json ?? null],
      ['requested_date_start', run.requested_date_start ?? null],
      ['requested_date_end', run.requested_date_end ?? null],
      ['discovered_count', run.discovered_count ?? 0],
      ['returned_count', run.returned_count ?? 0],
      ['filtered_out_count', run.filtered_out_count ?? 0],
      ['returned_min_filing_date', run.returned_min_filing_date ?? null],
      ['returned_max_filing_date', run.returned_max_filing_date ?? null],
      ['upstream_min_filing_date', run.upstream_min_filing_date ?? null],
      ['upstream_max_filing_date', run.upstream_max_filing_date ?? null],
      ['partial_reason', run.partial_reason ?? null],
      ['artifact_retrieval_enabled', run.artifact_retrieval_enabled ?? 0],
      ['artifact_fetch_coverage_pct', run.artifact_fetch_coverage_pct ?? 0],
      ['enrichment_mode', run.enrichment_mode ?? null],
      ['artifact_readiness_not_met', run.artifact_readiness_not_met ?? 0],
      ['enriched_record_count', run.enriched_record_count ?? 0],
      ['partial_record_count', run.partial_record_count ?? 0],
      ['new_master_row_count', run.new_master_row_count ?? 0],
      ['purged_review_row_count', run.purged_review_row_count ?? 0],
      ['lead_alert_attempted', run.lead_alert_attempted ?? 0],
      ['lead_alert_delivered', run.lead_alert_delivered ?? 0],
      ['master_fallback_used', run.master_fallback_used ?? 0],
      ['anomaly_detected', run.anomaly_detected ?? 0],
      ['debug_artifact_json', run.debug_artifact_json ?? null],
      ['sla_score_pct', run.sla_score_pct ?? 0],
      ['sla_pass', run.sla_pass ?? 0],
      ['sla_policy_version', run.sla_policy_version ?? null],
      ['sla_components_json', run.sla_components_json ?? null],
    ];
    const assignments = entries.map(([column]) => `${column} = ?`).join(', ');
    const values = entries.map(([, value]) => value);
    this.getDb()
      .prepare(`UPDATE scheduled_runs SET ${assignments}, updated_at = CURRENT_TIMESTAMP WHERE id = ?`)
      .run(...values, run.id);
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

  async upsertSiteStateArtifact(record: SiteStateArtifactRecord): Promise<void> {
    this.getDb().prepare(
      `INSERT INTO scheduler_site_artifacts (site, artifact_key, payload_json, updated_at)
       VALUES (?, ?, ?, CURRENT_TIMESTAMP)
       ON CONFLICT(site, artifact_key) DO UPDATE SET
         payload_json = excluded.payload_json,
         updated_at = CURRENT_TIMESTAMP`
    ).run(record.site, record.artifact_key, record.payload_json);
  }

  async getSiteStateArtifact(site: SupportedSite, artifactKey: string): Promise<SiteStateArtifactRecord | null> {
    const row = this.getDb().prepare(
      `SELECT site, artifact_key, payload_json, updated_at
       FROM scheduler_site_artifacts
       WHERE site = ? AND artifact_key = ?`
    ).get(site, artifactKey) as Record<string, unknown> | undefined;
    return normalizeSiteStateArtifactRecord(row);
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

  async insertSchedulerAlert(alert: SchedulerAlertRecord): Promise<void> {
    this.getDb().prepare(
      `INSERT OR IGNORE INTO scheduler_alerts (
        site, idempotency_key, slot, expected_by, alert_type, run_id, metrics_triggered, summary,
        baseline_records_scraped, baseline_amount_coverage_pct, baseline_ocr_success_pct, baseline_row_fail_pct,
        records_scraped, amount_coverage_pct, ocr_success_pct, row_fail_pct, detected_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(
      alert.site,
      alert.idempotency_key,
      alert.slot,
      alert.detected_at ?? alert.expected_by,
      alert.alert_type,
      alert.run_id ?? null,
      alert.metrics_triggered ? JSON.stringify(alert.metrics_triggered) : null,
      alert.summary ?? null,
      alert.baseline_records_scraped ?? null,
      alert.baseline_amount_coverage_pct ?? null,
      alert.baseline_ocr_success_pct ?? null,
      alert.baseline_row_fail_pct ?? null,
      alert.records_scraped ?? null,
      alert.amount_coverage_pct ?? null,
      alert.ocr_success_pct ?? null,
      alert.row_fail_pct ?? null,
      alert.detected_at ?? null
    );
  }

  async getAlertByKey(idempotencyKey: string, alertType: SchedulerAlertType): Promise<SchedulerAlertRecord | null> {
    const row = this.getDb()
      .prepare(
        `SELECT site, idempotency_key, slot, alert_type, expected_by, run_id, metrics_triggered, summary,
                baseline_records_scraped, baseline_amount_coverage_pct, baseline_ocr_success_pct, baseline_row_fail_pct,
                records_scraped, amount_coverage_pct, ocr_success_pct, row_fail_pct, detected_at
         FROM scheduler_alerts
         WHERE idempotency_key = ? AND alert_type = ?
         LIMIT 1`
      )
      .get(idempotencyKey, alertType) as Record<string, unknown> | undefined;
    return normalizeSchedulerAlertRecord(row);
  }

  async insertQualityAnomalyAlert(alert: QualityAnomalyAlertRecord): Promise<void> {
    await this.insertSchedulerAlert({
      ...alert,
      alert_type: 'quality_anomaly',
      expected_by: alert.detected_at,
    });
  }

  async getLatestQualityAnomalyAlert(site: SupportedSite): Promise<QualityAnomalyAlertRecord | null> {
    const row = this.getDb().prepare(
      `SELECT site, idempotency_key, run_id, slot, metrics_triggered, summary,
              baseline_records_scraped, baseline_amount_coverage_pct, baseline_ocr_success_pct, baseline_row_fail_pct,
              records_scraped, amount_coverage_pct, ocr_success_pct, row_fail_pct, detected_at
       FROM scheduler_alerts
       WHERE site = ? AND alert_type = 'quality_anomaly'
       ORDER BY COALESCE(detected_at, created_at) DESC
       LIMIT 1`
    ).get(site) as Record<string, unknown> | undefined;
    return normalizeQualityAnomalyAlertRecord(row);
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
            attempt_count INTEGER NOT NULL DEFAULT 1,
            max_attempts INTEGER NOT NULL DEFAULT 1,
            retried INTEGER NOT NULL DEFAULT 0,
            retry_exhausted INTEGER NOT NULL DEFAULT 0,
            source_tab_title TEXT,
            master_tab_title TEXT,
            review_tab_title TEXT,
            quarantined_row_count INTEGER NOT NULL DEFAULT 0,
            current_run_quarantined_row_count INTEGER NOT NULL DEFAULT 0,
            current_run_conflict_row_count INTEGER NOT NULL DEFAULT 0,
            retained_prior_review_row_count INTEGER NOT NULL DEFAULT 0,
            review_reason_counts_json TEXT,
            requested_date_start TEXT,
            requested_date_end TEXT,
            discovered_count INTEGER NOT NULL DEFAULT 0,
            returned_count INTEGER NOT NULL DEFAULT 0,
            filtered_out_count INTEGER NOT NULL DEFAULT 0,
            returned_min_filing_date TEXT,
            returned_max_filing_date TEXT,
            upstream_min_filing_date TEXT,
            upstream_max_filing_date TEXT,
            partial_reason TEXT,
            artifact_retrieval_enabled INTEGER NOT NULL DEFAULT 0,
            artifact_fetch_coverage_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
            enrichment_mode TEXT,
            artifact_readiness_not_met INTEGER NOT NULL DEFAULT 0,
            enriched_record_count INTEGER NOT NULL DEFAULT 0,
            partial_record_count INTEGER NOT NULL DEFAULT 0,
            new_master_row_count INTEGER NOT NULL DEFAULT 0,
            purged_review_row_count INTEGER NOT NULL DEFAULT 0,
            lead_alert_attempted INTEGER NOT NULL DEFAULT 0,
            lead_alert_delivered INTEGER NOT NULL DEFAULT 0,
            master_fallback_used INTEGER NOT NULL DEFAULT 0,
            anomaly_detected INTEGER NOT NULL DEFAULT 0,
            debug_artifact_json TEXT,
            sla_score_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
            sla_pass INTEGER NOT NULL DEFAULT 0,
            sla_policy_version TEXT,
            sla_components_json TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          )
        `);
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 1');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS retried INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS retry_exhausted INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS source_tab_title TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS master_tab_title TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS review_tab_title TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS quarantined_row_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS current_run_quarantined_row_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS current_run_conflict_row_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS retained_prior_review_row_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS review_reason_counts_json TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS requested_date_start TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS requested_date_end TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS discovered_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS returned_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS filtered_out_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS returned_min_filing_date TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS returned_max_filing_date TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS upstream_min_filing_date TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS upstream_max_filing_date TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS partial_reason TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS artifact_retrieval_enabled INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS artifact_fetch_coverage_pct DOUBLE PRECISION NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS enrichment_mode TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS artifact_readiness_not_met INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS enriched_record_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS partial_record_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS new_master_row_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS purged_review_row_count INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS lead_alert_attempted INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS lead_alert_delivered INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS master_fallback_used INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS anomaly_detected INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS debug_artifact_json TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS sla_score_pct DOUBLE PRECISION NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS sla_pass INTEGER NOT NULL DEFAULT 0');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS sla_policy_version TEXT');
        await client.query('ALTER TABLE scheduled_runs ADD COLUMN IF NOT EXISTS sla_components_json TEXT');
      await client.query('CREATE INDEX IF NOT EXISTS idx_scheduled_runs_started_at ON scheduled_runs(started_at DESC)');
      await client.query('CREATE INDEX IF NOT EXISTS idx_scheduled_runs_status ON scheduled_runs(status)');
      await client.query('CREATE INDEX IF NOT EXISTS idx_scheduled_runs_site_started_at ON scheduled_runs(site, started_at DESC)');
      await client.query(`
        CREATE TABLE IF NOT EXISTS scheduler_alerts (
          id BIGSERIAL PRIMARY KEY,
          site TEXT NOT NULL DEFAULT 'ca_sos',
          idempotency_key TEXT NOT NULL,
          slot TEXT NOT NULL CHECK(slot IN ('morning', 'afternoon', 'evening')),
          alert_type TEXT NOT NULL CHECK(alert_type IN ('missed_run', 'quality_anomaly', 'sla_breach', 'cadence_breach')),
          expected_by TIMESTAMPTZ NOT NULL,
          run_id TEXT,
          metrics_triggered TEXT,
          summary TEXT,
          baseline_records_scraped DOUBLE PRECISION,
          baseline_amount_coverage_pct DOUBLE PRECISION,
          baseline_ocr_success_pct DOUBLE PRECISION,
          baseline_row_fail_pct DOUBLE PRECISION,
          records_scraped DOUBLE PRECISION,
          amount_coverage_pct DOUBLE PRECISION,
          ocr_success_pct DOUBLE PRECISION,
          row_fail_pct DOUBLE PRECISION,
          detected_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(idempotency_key, alert_type)
        )
      `);
      await client.query('ALTER TABLE scheduler_alerts DROP CONSTRAINT IF EXISTS scheduler_alerts_alert_type_check');
      await client.query(
        "ALTER TABLE scheduler_alerts ADD CONSTRAINT scheduler_alerts_alert_type_check CHECK(alert_type IN ('missed_run', 'quality_anomaly', 'sla_breach', 'cadence_breach'))"
      ).catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        if (!/already exists/i.test(message)) throw err;
      });
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS run_id TEXT');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS metrics_triggered TEXT');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS summary TEXT');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS baseline_records_scraped DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS baseline_amount_coverage_pct DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS baseline_ocr_success_pct DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS baseline_row_fail_pct DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS records_scraped DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS amount_coverage_pct DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS ocr_success_pct DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS row_fail_pct DOUBLE PRECISION');
      await client.query('ALTER TABLE scheduler_alerts ADD COLUMN IF NOT EXISTS detected_at TIMESTAMPTZ');
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
      await client.query(`
        CREATE TABLE IF NOT EXISTS scheduler_site_artifacts (
          site TEXT NOT NULL,
          artifact_key TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (site, artifact_key)
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
    const entries: Array<[string, unknown]> = [
      ['id', run.id],
      ['site', run.site],
      ['idempotency_key', run.idempotency_key],
      ['slot_time', run.slot_time],
      ['trigger_source', run.trigger_source],
      ['started_at', run.started_at],
      ['finished_at', run.finished_at ?? null],
      ['status', run.status],
      ['records_scraped', run.records_scraped],
      ['records_skipped', run.records_skipped],
      ['rows_uploaded', run.rows_uploaded],
      ['amount_found_count', run.amount_found_count],
      ['amount_missing_count', run.amount_missing_count],
      ['amount_coverage_pct', run.amount_coverage_pct],
      ['ocr_success_pct', run.ocr_success_pct],
      ['row_fail_pct', run.row_fail_pct],
      ['deadline_hit', run.deadline_hit],
      ['effective_max_records', run.effective_max_records],
      ['partial', run.partial],
      ['error', run.error ?? null],
      ['failure_class', run.failure_class ?? null],
      ['attempt_count', run.attempt_count ?? 1],
      ['max_attempts', run.max_attempts ?? 1],
      ['retried', run.retried ?? 0],
      ['retry_exhausted', run.retry_exhausted ?? 0],
      ['source_tab_title', run.source_tab_title ?? null],
      ['master_tab_title', run.master_tab_title ?? null],
      ['review_tab_title', run.review_tab_title ?? null],
      ['quarantined_row_count', run.quarantined_row_count ?? 0],
      ['current_run_quarantined_row_count', run.current_run_quarantined_row_count ?? 0],
      ['current_run_conflict_row_count', run.current_run_conflict_row_count ?? 0],
      ['retained_prior_review_row_count', run.retained_prior_review_row_count ?? 0],
      ['review_reason_counts_json', run.review_reason_counts_json ?? null],
      ['requested_date_start', run.requested_date_start ?? null],
      ['requested_date_end', run.requested_date_end ?? null],
      ['discovered_count', run.discovered_count ?? 0],
      ['returned_count', run.returned_count ?? 0],
      ['filtered_out_count', run.filtered_out_count ?? 0],
      ['returned_min_filing_date', run.returned_min_filing_date ?? null],
      ['returned_max_filing_date', run.returned_max_filing_date ?? null],
      ['upstream_min_filing_date', run.upstream_min_filing_date ?? null],
      ['upstream_max_filing_date', run.upstream_max_filing_date ?? null],
      ['partial_reason', run.partial_reason ?? null],
      ['artifact_retrieval_enabled', run.artifact_retrieval_enabled ?? 0],
      ['artifact_fetch_coverage_pct', run.artifact_fetch_coverage_pct ?? 0],
      ['enrichment_mode', run.enrichment_mode ?? null],
      ['artifact_readiness_not_met', run.artifact_readiness_not_met ?? 0],
      ['enriched_record_count', run.enriched_record_count ?? 0],
      ['partial_record_count', run.partial_record_count ?? 0],
      ['new_master_row_count', run.new_master_row_count ?? 0],
      ['purged_review_row_count', run.purged_review_row_count ?? 0],
      ['lead_alert_attempted', run.lead_alert_attempted ?? 0],
      ['lead_alert_delivered', run.lead_alert_delivered ?? 0],
      ['master_fallback_used', run.master_fallback_used ?? 0],
      ['anomaly_detected', run.anomaly_detected ?? 0],
      ['debug_artifact_json', run.debug_artifact_json ?? null],
      ['sla_score_pct', run.sla_score_pct ?? 0],
      ['sla_pass', run.sla_pass ?? 0],
      ['sla_policy_version', run.sla_policy_version ?? null],
      ['sla_components_json', run.sla_components_json ?? null],
    ];
    const columns = entries.map(([column]) => column);
    const values = entries.map(([, value]) => value);
    const placeholders = values.map((_, index) => `$${index + 1}`).join(', ');
    await this.pool.query(
      `INSERT INTO scheduled_runs (${columns.join(', ')}) VALUES (${placeholders})`,
      values,
    );
  }

  async updateRun(run: ScheduledRunRecord): Promise<void> {
    const entries: Array<[string, unknown]> = [
      ['site', run.site],
      ['finished_at', run.finished_at ?? null],
      ['status', run.status],
      ['records_scraped', run.records_scraped],
      ['records_skipped', run.records_skipped],
      ['rows_uploaded', run.rows_uploaded],
      ['amount_found_count', run.amount_found_count],
      ['amount_missing_count', run.amount_missing_count],
      ['amount_coverage_pct', run.amount_coverage_pct],
      ['ocr_success_pct', run.ocr_success_pct],
      ['row_fail_pct', run.row_fail_pct],
      ['deadline_hit', run.deadline_hit],
      ['effective_max_records', run.effective_max_records],
      ['partial', run.partial],
      ['error', run.error ?? null],
      ['failure_class', run.failure_class ?? null],
      ['attempt_count', run.attempt_count ?? 1],
      ['max_attempts', run.max_attempts ?? 1],
      ['retried', run.retried ?? 0],
      ['retry_exhausted', run.retry_exhausted ?? 0],
      ['source_tab_title', run.source_tab_title ?? null],
      ['master_tab_title', run.master_tab_title ?? null],
      ['review_tab_title', run.review_tab_title ?? null],
      ['quarantined_row_count', run.quarantined_row_count ?? 0],
      ['current_run_quarantined_row_count', run.current_run_quarantined_row_count ?? 0],
      ['current_run_conflict_row_count', run.current_run_conflict_row_count ?? 0],
      ['retained_prior_review_row_count', run.retained_prior_review_row_count ?? 0],
      ['review_reason_counts_json', run.review_reason_counts_json ?? null],
      ['requested_date_start', run.requested_date_start ?? null],
      ['requested_date_end', run.requested_date_end ?? null],
      ['discovered_count', run.discovered_count ?? 0],
      ['returned_count', run.returned_count ?? 0],
      ['filtered_out_count', run.filtered_out_count ?? 0],
      ['returned_min_filing_date', run.returned_min_filing_date ?? null],
      ['returned_max_filing_date', run.returned_max_filing_date ?? null],
      ['upstream_min_filing_date', run.upstream_min_filing_date ?? null],
      ['upstream_max_filing_date', run.upstream_max_filing_date ?? null],
      ['partial_reason', run.partial_reason ?? null],
      ['artifact_retrieval_enabled', run.artifact_retrieval_enabled ?? 0],
      ['artifact_fetch_coverage_pct', run.artifact_fetch_coverage_pct ?? 0],
      ['enrichment_mode', run.enrichment_mode ?? null],
      ['artifact_readiness_not_met', run.artifact_readiness_not_met ?? 0],
      ['enriched_record_count', run.enriched_record_count ?? 0],
      ['partial_record_count', run.partial_record_count ?? 0],
      ['new_master_row_count', run.new_master_row_count ?? 0],
      ['purged_review_row_count', run.purged_review_row_count ?? 0],
      ['lead_alert_attempted', run.lead_alert_attempted ?? 0],
      ['lead_alert_delivered', run.lead_alert_delivered ?? 0],
      ['master_fallback_used', run.master_fallback_used ?? 0],
      ['anomaly_detected', run.anomaly_detected ?? 0],
      ['debug_artifact_json', run.debug_artifact_json ?? null],
      ['sla_score_pct', run.sla_score_pct ?? 0],
      ['sla_pass', run.sla_pass ?? 0],
      ['sla_policy_version', run.sla_policy_version ?? null],
      ['sla_components_json', run.sla_components_json ?? null],
    ];
    const assignments = entries.map(([column], index) => `${column} = $${index + 1}`).join(', ');
    const values = entries.map(([, value]) => value);
    values.push(run.id);
    await this.pool.query(
      `UPDATE scheduled_runs SET ${assignments}, updated_at = NOW() WHERE id = $${values.length}`,
      values,
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

  async upsertSiteStateArtifact(record: SiteStateArtifactRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO scheduler_site_artifacts (site, artifact_key, payload_json, updated_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT(site, artifact_key) DO UPDATE SET
         payload_json = EXCLUDED.payload_json,
         updated_at = NOW()`,
      [record.site, record.artifact_key, record.payload_json]
    );
  }

  async getSiteStateArtifact(site: SupportedSite, artifactKey: string): Promise<SiteStateArtifactRecord | null> {
    return normalizeSiteStateArtifactRecord(
      await this.queryRow<Record<string, unknown>>(
        `SELECT site, artifact_key, payload_json, updated_at
         FROM scheduler_site_artifacts
         WHERE site = $1 AND artifact_key = $2`,
        [site, artifactKey]
      )
    );
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

  async insertSchedulerAlert(alert: SchedulerAlertRecord): Promise<void> {
    await this.pool.query(
      `INSERT INTO scheduler_alerts (
         site, idempotency_key, slot, expected_by, alert_type, run_id, metrics_triggered, summary,
         baseline_records_scraped, baseline_amount_coverage_pct, baseline_ocr_success_pct, baseline_row_fail_pct,
         records_scraped, amount_coverage_pct, ocr_success_pct, row_fail_pct, detected_at
       )
       VALUES (
         $1, $2, $3, $4::timestamptz, $5, $6, $7, $8,
         $9, $10, $11, $12, $13, $14, $15, $16, $17::timestamptz
       )
       ON CONFLICT(idempotency_key, alert_type) DO NOTHING`,
      [
        alert.site,
        alert.idempotency_key,
        alert.slot,
        alert.detected_at ?? alert.expected_by,
        alert.alert_type,
        alert.run_id ?? null,
        alert.metrics_triggered ? JSON.stringify(alert.metrics_triggered) : null,
        alert.summary ?? null,
        alert.baseline_records_scraped ?? null,
        alert.baseline_amount_coverage_pct ?? null,
        alert.baseline_ocr_success_pct ?? null,
        alert.baseline_row_fail_pct ?? null,
        alert.records_scraped ?? null,
        alert.amount_coverage_pct ?? null,
        alert.ocr_success_pct ?? null,
        alert.row_fail_pct ?? null,
        alert.detected_at ?? null,
      ]
    );
  }

  async getAlertByKey(idempotencyKey: string, alertType: SchedulerAlertType): Promise<SchedulerAlertRecord | null> {
    return normalizeSchedulerAlertRecord(
      await this.queryRow<Record<string, unknown>>(
        `SELECT site, idempotency_key, slot, alert_type, expected_by, run_id, metrics_triggered, summary,
                baseline_records_scraped, baseline_amount_coverage_pct, baseline_ocr_success_pct, baseline_row_fail_pct,
                records_scraped, amount_coverage_pct, ocr_success_pct, row_fail_pct, detected_at
         FROM scheduler_alerts
         WHERE idempotency_key = $1 AND alert_type = $2
         LIMIT 1`,
        [idempotencyKey, alertType]
      )
    );
  }

  async insertQualityAnomalyAlert(alert: QualityAnomalyAlertRecord): Promise<void> {
    await this.insertSchedulerAlert({
      ...alert,
      alert_type: 'quality_anomaly',
      expected_by: alert.detected_at,
    });
  }

  async getLatestQualityAnomalyAlert(site: SupportedSite): Promise<QualityAnomalyAlertRecord | null> {
    return normalizeQualityAnomalyAlertRecord(
      await this.queryRow<Record<string, unknown>>(
        `SELECT site, idempotency_key, run_id, slot, metrics_triggered, summary,
                baseline_records_scraped, baseline_amount_coverage_pct, baseline_ocr_success_pct, baseline_row_fail_pct,
                records_scraped, amount_coverage_pct, ocr_success_pct, row_fail_pct, detected_at
         FROM scheduler_alerts
         WHERE site = $1 AND alert_type = 'quality_anomaly'
         ORDER BY COALESCE(detected_at, created_at) DESC
         LIMIT 1`,
        [site]
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

  async upsertSiteStateArtifact(record: SiteStateArtifactRecord): Promise<void> {
    await this.ensureReady();
    await this.backend.upsertSiteStateArtifact(record);
  }

  async getSiteStateArtifact(site: SupportedSite, artifactKey: string): Promise<SiteStateArtifactRecord | null> {
    await this.ensureReady();
    return this.backend.getSiteStateArtifact(site, artifactKey);
  }

  async insertMissedAlert(alert: MissedAlertRecord): Promise<void> {
    await this.ensureReady();
    await this.backend.insertMissedAlert(alert);
  }

  async getMissedAlertByKey(idempotencyKey: string): Promise<MissedAlertRecord | null> {
    await this.ensureReady();
    return this.backend.getMissedAlertByKey(idempotencyKey);
  }

  async insertSchedulerAlert(alert: SchedulerAlertRecord): Promise<void> {
    await this.ensureReady();
    await this.backend.insertSchedulerAlert(alert);
  }

  async getAlertByKey(idempotencyKey: string, alertType: SchedulerAlertType): Promise<SchedulerAlertRecord | null> {
    await this.ensureReady();
    return this.backend.getAlertByKey(idempotencyKey, alertType);
  }

  async insertQualityAnomalyAlert(alert: QualityAnomalyAlertRecord): Promise<void> {
    await this.ensureReady();
    await this.backend.insertQualityAnomalyAlert(alert);
  }

  async getLatestQualityAnomalyAlert(site: SupportedSite): Promise<QualityAnomalyAlertRecord | null> {
    await this.ensureReady();
    return this.backend.getLatestQualityAnomalyAlert(site);
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
