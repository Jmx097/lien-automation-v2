import { beforeEach, describe, expect, it, vi } from 'vitest';

const pgBehavior = {
  failSelect1: false,
};

const sheetsBehavior = {
  sourceReachable: true,
  targetReachable: true,
};

const maricopaBehavior = {
  artifactRetrievalEnabled: false,
  sessionPresent: false,
  sessionFresh: false,
  sessionCapturedAt: undefined as string | undefined,
  artifactCandidatesPresent: false,
  artifactCandidateCount: 0,
  refreshRequired: false,
  refreshReason: 'artifact_retrieval_disabled' as string | undefined,
  detail: 'Maricopa artifact retrieval is disabled by configuration.',
};

vi.mock('pg', () => {
  class Pool {
    async connect() {
      return {
        query: this.query.bind(this),
        release() {},
      };
    }

    async end() {}

    async query(sql: string) {
      const normalized = sql.replace(/\s+/g, ' ').trim();
      if (normalized === 'SELECT 1') {
        if (pgBehavior.failSelect1) throw new Error('connect ECONNREFUSED');
        return { rows: [{ '?column?': 1 }] };
      }
      if (
        /^BEGIN$|^COMMIT$|^ROLLBACK$/.test(normalized) ||
        normalized.startsWith('CREATE TABLE') ||
        normalized.startsWith('CREATE INDEX') ||
        normalized.startsWith('ALTER TABLE')
      ) {
        return { rows: [] };
      }
      if (normalized.includes('FROM scheduler_site_connectivity_state WHERE site = $1')) {
        return { rows: [] };
      }
      throw new Error(`Unhandled query in readiness pg mock: ${normalized}`);
    }
  }

  return { Pool };
});

vi.mock('../../src/scraper/ocr-runtime', () => ({
  checkOCRRuntime: () => ({ ok: true, missing: [] }),
}));

vi.mock('../../src/sheets/push', () => ({
  getMergedSheetTargetConfig: () => ({
    source_spreadsheet_id: 'sheet-id',
    target_spreadsheet_id: '1qa32AEUMC4TYHh4G6AV4msRS4GjQQZFTNPpYHMh4n5A',
    fallback_tab_title: 'Master',
    target_tab_title: 'Master',
    default_target_used: true,
  }),
  checkSpreadsheetAccess: async (spreadsheetId: string) => {
    if (spreadsheetId === 'sheet-id') {
      return sheetsBehavior.sourceReachable
        ? { ok: true }
        : { ok: false, detail: 'source spreadsheet access denied' };
    }

    return sheetsBehavior.targetReachable
      ? { ok: true }
      : { ok: false, detail: 'target spreadsheet access denied' };
  },
}));

vi.mock('../../src/scraper/maricopa_artifacts', () => ({
  getMaricopaPersistedStateReadiness: async () => ({ ...maricopaBehavior }),
}));

function setReadyEnv(): void {
  process.env.SHEET_ID = 'sheet-id';
  process.env.SHEETS_KEY = JSON.stringify({
    client_email: 'svc@example.com',
    private_key: '-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n',
  });
  process.env.SCHEDULE_RUN_TOKEN = 'token';
  process.env.SBR_CDP_URL = 'wss://example.invalid';
  process.env.SCHEDULE_CA_SOS_WEEKLY_DAYS = 'MO,TU,WE,TH,FR';
  process.env.SCHEDULE_CA_SOS_RUN_HOUR = '9';
  process.env.SCHEDULE_CA_SOS_RUN_MINUTE = '0';
  process.env.SCHEDULE_MARICOPA_RECORDER_WEEKLY_DAYS = 'MO,TU,WE,TH,FR';
  process.env.SCHEDULE_MARICOPA_RECORDER_RUN_HOUR = '9';
  process.env.SCHEDULE_MARICOPA_RECORDER_RUN_MINUTE = '0';
  process.env.SCHEDULE_NYC_ACRIS_TIMEZONE = 'America/New_York';
  process.env.SCHEDULE_NYC_ACRIS_WEEKLY_DAYS = 'TU,WE,TH,FR';
  process.env.SCHEDULE_NYC_ACRIS_MORNING_RUN_HOUR = '10';
  process.env.SCHEDULE_NYC_ACRIS_MORNING_RUN_MINUTE = '0';
  process.env.SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_HOUR = '14';
  process.env.SCHEDULE_NYC_ACRIS_AFTERNOON_RUN_MINUTE = '0';
  process.env.SCHEDULE_NYC_ACRIS_MAX_RECORDS = '5';
}

describe('schedule readiness with scheduler store backends', () => {
  beforeEach(() => {
    vi.resetModules();
    pgBehavior.failSelect1 = false;
    sheetsBehavior.sourceReachable = true;
    sheetsBehavior.targetReachable = true;
    delete process.env.SQLITE_DB_PATH;
    process.env.DATABASE_URL = 'postgres://postgres:postgres@127.0.0.1:5432/lien';
    setReadyEnv();
    maricopaBehavior.artifactRetrievalEnabled = false;
    maricopaBehavior.sessionPresent = false;
    maricopaBehavior.sessionFresh = false;
    maricopaBehavior.sessionCapturedAt = undefined;
    maricopaBehavior.artifactCandidatesPresent = false;
    maricopaBehavior.artifactCandidateCount = 0;
    maricopaBehavior.refreshRequired = false;
    maricopaBehavior.refreshReason = 'artifact_retrieval_disabled';
    maricopaBehavior.detail = 'Maricopa artifact retrieval is disabled by configuration.';
  });

  it('reports postgres scheduler store readiness when DATABASE_URL is available', async () => {
    const { getScheduleReadinessReport } = await import('../../src/schedule/readiness');
    const report = await getScheduleReadinessReport();

    expect(report.status).toBe('ready');
    expect(report.checks.find((check) => check.name === 'db_reachable')).toEqual(
      expect.objectContaining({ ok: true, detail: 'scheduler_store=postgres' })
    );
    expect(report.checks.find((check) => check.name === 'source_sheet_reachable')).toEqual(
      expect.objectContaining({ ok: true })
    );
    expect(report.merged_output).toEqual(expect.objectContaining({
      target_reachable: true,
      fallback_active: false,
    }));
    expect(report.maricopa).toEqual(expect.objectContaining({
      artifact_retrieval_enabled: false,
      refresh_required: false,
    }));
  });

  it('returns not_ready instead of throwing when postgres connectivity fails', async () => {
    pgBehavior.failSelect1 = true;

    const { getScheduleReadinessReport } = await import('../../src/schedule/readiness');
    const report = await getScheduleReadinessReport();

    expect(report.status).toBe('not_ready');
    expect(report.checks.find((check) => check.name === 'db_reachable')).toEqual(
      expect.objectContaining({ ok: false, detail: expect.stringContaining('ECONNREFUSED') })
    );
    expect(report.site_connectivity.nyc_acris.status).toBe('healthy');
  });

  it('reports merged output fallback mode when the destination sheet is not reachable', async () => {
    sheetsBehavior.targetReachable = false;

    const { getScheduleReadinessReport } = await import('../../src/schedule/readiness');
    const report = await getScheduleReadinessReport();

    expect(report.status).toBe('ready');
    expect(report.merged_output).toEqual(expect.objectContaining({
      target_reachable: false,
      fallback_active: true,
      detail: expect.stringContaining('target spreadsheet access denied'),
    }));
  });

  it('reports not_ready when Maricopa artifact retrieval is enabled but persisted state is stale', async () => {
    maricopaBehavior.artifactRetrievalEnabled = true;
    maricopaBehavior.sessionPresent = true;
    maricopaBehavior.sessionFresh = false;
    maricopaBehavior.sessionCapturedAt = '2026-03-10T12:00:00.000Z';
    maricopaBehavior.refreshRequired = true;
    maricopaBehavior.refreshReason = 'session_missing_or_stale';
    maricopaBehavior.detail = 'Maricopa session is stale (captured_at=2026-03-10T12:00:00.000Z). Run refresh:maricopa-session on the droplet.';

    const { getScheduleReadinessReport } = await import('../../src/schedule/readiness');
    const report = await getScheduleReadinessReport();

    expect(report.status).toBe('not_ready');
    expect(report.checks.find((check) => check.name === 'maricopa_persisted_state_ready')).toEqual(
      expect.objectContaining({ ok: false, detail: expect.stringContaining('refresh:maricopa-session') })
    );
    expect(report.maricopa).toEqual(expect.objectContaining({
      artifact_retrieval_enabled: true,
      refresh_required: true,
      refresh_reason: 'session_missing_or_stale',
    }));
  });
});
