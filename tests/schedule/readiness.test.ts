import { beforeEach, describe, expect, it, vi } from 'vitest';

const pgBehavior = {
  failSelect1: false,
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
      if (/^BEGIN$|^COMMIT$|^ROLLBACK$/.test(normalized) || normalized.startsWith('CREATE TABLE') || normalized.startsWith('CREATE INDEX')) {
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

function setReadyEnv(): void {
  process.env.SHEET_ID = 'sheet-id';
  process.env.SHEETS_KEY = JSON.stringify({
    client_email: 'svc@example.com',
    private_key: '-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n',
  });
  process.env.SCHEDULE_RUN_TOKEN = 'token';
  process.env.SBR_CDP_URL = 'wss://example.invalid';
  process.env.SCHEDULE_NYC_ACRIS_TIMEZONE = 'America/New_York';
  process.env.SCHEDULE_NYC_ACRIS_WEEKLY_DAYS = 'TU,WE,TH,FR';
  process.env.SCHEDULE_NYC_ACRIS_RUN_HOUR = '14';
  process.env.SCHEDULE_NYC_ACRIS_RUN_MINUTE = '0';
  process.env.SCHEDULE_NYC_ACRIS_MAX_RECORDS = '5';
}

describe('schedule readiness with scheduler store backends', () => {
  beforeEach(() => {
    vi.resetModules();
    pgBehavior.failSelect1 = false;
    delete process.env.SQLITE_DB_PATH;
    process.env.DATABASE_URL = 'postgres://postgres:postgres@127.0.0.1:5432/lien';
    setReadyEnv();
  });

  it('reports postgres scheduler store readiness when DATABASE_URL is available', async () => {
    const { getScheduleReadinessReport } = await import('../../src/schedule/readiness');
    const report = await getScheduleReadinessReport();

    expect(report.status).toBe('ready');
    expect(report.checks.find((check) => check.name === 'db_reachable')).toEqual(
      expect.objectContaining({ ok: true, detail: 'scheduler_store=postgres' })
    );
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
});
