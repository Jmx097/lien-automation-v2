import dotenv from 'dotenv';
import { discoverMaricopaArtifacts } from '../src/maintenance/maricopa';

dotenv.config({ quiet: true });

async function main(): Promise<void> {
  const result = await discoverMaricopaArtifacts();
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exitCode = 1;
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
