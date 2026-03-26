import dotenv from 'dotenv';
import { exportTriSiteProof } from '../src/proof/tri_site';

dotenv.config();

async function main(): Promise<void> {
  const summary = await exportTriSiteProof();
  console.log(JSON.stringify(summary, null, 2));
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exitCode = 1;
});
