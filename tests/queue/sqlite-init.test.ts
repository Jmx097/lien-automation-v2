import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';

import { SQLiteQueueStore } from '../../src/queue/sqlite';

const originalCwd = process.cwd();
const originalDbPath = process.env.SQLITE_DB_PATH;

afterEach(() => {
  process.chdir(originalCwd);
  if (originalDbPath === undefined) {
    delete process.env.SQLITE_DB_PATH;
  } else {
    process.env.SQLITE_DB_PATH = originalDbPath;
  }
});

describe('SQLiteQueueStore initialization', () => {
  it('creates the data/db directory and db file in an empty working directory', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'lien-queue-init-'));
    const dbPath = path.join(tempRoot, 'data', 'db', 'lien-queue.db');
    process.chdir(tempRoot);
    process.env.SQLITE_DB_PATH = dbPath;

    const store = new SQLiteQueueStore();

    const dbDir = path.join(tempRoot, 'data/db');

    expect(store).toBeDefined();
    expect(fs.existsSync(dbDir)).toBe(true);
    expect(fs.existsSync(dbPath)).toBe(true);
  });

  it('auto-initializes queue schema before queue operations', async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'lien-queue-schema-'));
    const dbPath = path.join(tempRoot, 'data', 'db', 'lien-queue.db');
    process.chdir(tempRoot);
    process.env.SQLITE_DB_PATH = dbPath;

    const store = new SQLiteQueueStore();

    await expect(store.getPendingCount()).resolves.toBe(0);
  });
});
