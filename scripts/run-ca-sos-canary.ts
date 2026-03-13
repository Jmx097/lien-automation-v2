import dotenv from 'dotenv';
import { runCASOSCanary } from '../src/proof/live-proof';

dotenv.config();

async function main(): Promise<void> {
  const summary = await runCASOSCanary();
  console.log(JSON.stringify(summary, null, 2));
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
