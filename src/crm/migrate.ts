import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { Pool } from 'pg';

const migrationDirectory = path.resolve(process.cwd(), 'src/crm/migrations');

async function run(): Promise<void> {
  const connectionString = process.env.CRM_DATABASE_URL;
  if (!connectionString) {
    throw new Error('CRM_DATABASE_URL is required to run CRM migrations');
  }

  const pool = new Pool({ connectionString });
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS crm_schema_migrations (
        name TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    const files = (await readdir(migrationDirectory))
      .filter((file) => /^\d+_.+\.sql$/.test(file))
      .sort();

    for (const file of files) {
      const existing = await client.query('SELECT 1 FROM crm_schema_migrations WHERE name = $1', [file]);
      if (existing.rowCount) continue;

      const sql = await readFile(path.join(migrationDirectory, file), 'utf8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO crm_schema_migrations (name) VALUES ($1)', [file]);
        await client.query('COMMIT');
        console.log(`Applied CRM migration ${file}`);
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
  } finally {
    client.release();
    await pool.end();
  }
}

run().catch((error: unknown) => {
  console.error(`CRM migration failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
