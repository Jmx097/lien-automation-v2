import dotenv from 'dotenv';
import { validateNYCAcrisSelectors } from '../src/scraper/nyc_acris';
import { resolveTransportMode } from '../src/browser/transport';

dotenv.config();

async function main(): Promise<void> {
  if (resolveTransportMode() !== 'brightdata-browser-api') {
    throw new Error('NYC live selector validation requires BRIGHTDATA_BROWSER_WS Browser API transport');
  }

  const maxDocuments = Number(process.env.ACRIS_VALIDATION_MAX_DOCS ?? '2');
  const manifest = await validateNYCAcrisSelectors({ max_documents: maxDocuments });

  console.log(
    JSON.stringify(
      {
        transportMode: manifest.transportMode,
        resultPagesVisited: manifest.resultPagesVisited,
        docCount: manifest.docIds.length,
        validatedDocs: manifest.documents.length,
        failures: manifest.failures,
        warnings: manifest.warnings,
        validationSteps: manifest.validationSteps,
      },
      null,
      2
    )
  );
}

void main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exitCode = 1;
});

