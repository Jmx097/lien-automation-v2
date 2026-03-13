import dotenv from 'dotenv';
import { runAllSitesLiveProof } from '../src/proof/live-proof';

dotenv.config();

async function main(): Promise<void> {
  const summary = await runAllSitesLiveProof();
  console.log(JSON.stringify(summary, null, 2));
  if (summary.error_count > 0) {
    process.exitCode = 1;
  }
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
