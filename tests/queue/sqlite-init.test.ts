import fs from 'fs';
import os from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';

import { SQLiteQueueStore } from '../../src/queue/sqlite';

const originalCwd = process.cwd();

afterEach(() => {
  process.chdir(originalCwd);
});

describe('SQLiteQueueStore initialization', () => {
  it('creates the data/db directory and db file in an empty working directory', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'lien-queue-init-'));
    process.chdir(tempRoot);

    const store = new SQLiteQueueStore();

    const dbDir = path.join(tempRoot, 'data/db');
    const dbPath = path.join(dbDir, 'lien-queue.db');

    expect(store).toBeDefined();
    expect(fs.existsSync(dbDir)).toBe(true);
    expect(fs.existsSync(dbPath)).toBe(true);
  });

  it('still requires init-db schema setup before queue operations', async () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'lien-queue-schema-'));
    process.chdir(tempRoot);

    const store = new SQLiteQueueStore();

    await expect(store.getPendingCount()).rejects.toThrow(/no such table: queue_jobs/i);
  });
});
