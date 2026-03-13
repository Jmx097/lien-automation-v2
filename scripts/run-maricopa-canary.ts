import dotenv from 'dotenv';
import { getMaricopaProofReadiness, runMaricopaCanary } from '../src/proof/live-proof';

dotenv.config();

async function main(): Promise<void> {
  const readiness = await getMaricopaProofReadiness();
  const summary = await runMaricopaCanary();

  console.log(
    JSON.stringify(
      {
        readiness,
        ...summary,
      },
      null,
      2,
    ),
  );
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});
