import Database from 'better-sqlite3';
import path from 'path';

export interface ReadinessCheck {
  name: string;
  ok: boolean;
  detail?: string;
}

export interface ScheduleReadinessReport {
  status: 'ready' | 'not_ready';
  checks: ReadinessCheck[];
}

const REQUIRED_ENV_VARS = ['SBR_CDP_URL', 'SHEET_ID', 'SHEETS_KEY', 'SCHEDULE_RUN_TOKEN'] as const;

function checkRequiredEnv(): ReadinessCheck {
  const missing = REQUIRED_ENV_VARS.filter((envVar) => !process.env[envVar]);

  if (missing.length > 0) {
    return {
      name: 'required_env_present',
      ok: false,
      detail: `Missing env vars: ${missing.join(', ')}`,
    };
  }

  return {
    name: 'required_env_present',
    ok: true,
  };
}

function checkDbReachable(): ReadinessCheck {
  const dbPath = path.join(process.cwd(), 'data/db/lien-queue.db');

  try {
    const db = new Database(dbPath, { readonly: true });
    db.prepare('SELECT 1').get();

    const runsTable = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scheduled_runs'").get() as
      | { name: string }
      | undefined;

    db.close();

    if (!runsTable) {
      return {
        name: 'db_reachable',
        ok: false,
        detail: 'DB reachable but scheduled_runs table is missing. Run node src/queue/init-db.js.',
      };
    }

    return {
      name: 'db_reachable',
      ok: true,
    };
  } catch (err: any) {
    return {
      name: 'db_reachable',
      ok: false,
      detail: err?.message ?? String(err),
    };
  }
}

function checkDownstreamCredentialsLoaded(): ReadinessCheck {
  const sheetsKeyRaw = process.env.SHEETS_KEY;
  const sheetId = process.env.SHEET_ID;

  if (!sheetsKeyRaw || !sheetId) {
    return {
      name: 'downstream_credentials_loaded',
      ok: false,
      detail: 'SHEETS_KEY and SHEET_ID must both be set.',
    };
  }

  try {
    const parsed = JSON.parse(sheetsKeyRaw.replace(/^'+|'+$/g, '')) as Record<string, unknown>;
    const hasClientEmail = typeof parsed.client_email === 'string' && parsed.client_email.length > 0;
    const hasPrivateKey = typeof parsed.private_key === 'string' && parsed.private_key.length > 0;

    if (!hasClientEmail || !hasPrivateKey) {
      return {
        name: 'downstream_credentials_loaded',
        ok: false,
        detail: 'SHEETS_KEY JSON is missing client_email or private_key.',
      };
    }

    return {
      name: 'downstream_credentials_loaded',
      ok: true,
    };
  } catch (err: any) {
    return {
      name: 'downstream_credentials_loaded',
      ok: false,
      detail: `SHEETS_KEY is not valid JSON: ${err?.message ?? String(err)}`,
    };
  }
}

export function getScheduleReadinessReport(): ScheduleReadinessReport {
  const checks = [checkRequiredEnv(), checkDbReachable(), checkDownstreamCredentialsLoaded()];

  return {
    status: checks.every((check) => check.ok) ? 'ready' : 'not_ready',
    checks,
  };
}
