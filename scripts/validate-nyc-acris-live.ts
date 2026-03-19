import dotenv from 'dotenv';
import { validateNYCAcrisSelectors } from '../src/scraper/nyc_acris';
import { resolveTransportMode } from '../src/browser/transport';

dotenv.config();

function logStageEvent(event: {
  step: string;
  status: 'started' | 'succeeded' | 'failed';
  at: string;
  detail?: string;
  durationMs?: number;
  timeoutMs?: number;
}): void {
  const timeout = event.timeoutMs ? ` timeout_ms=${event.timeoutMs}` : '';
  const duration = typeof event.durationMs === 'number' ? ` duration_ms=${event.durationMs}` : '';
  const detail = event.detail ? ` detail=${event.detail}` : '';
  console.error(`[${event.at}] nyc_validation step=${event.step} status=${event.status}${timeout}${duration}${detail}`);
}

async function main(): Promise<void> {
  const requestedTransportMode = resolveTransportMode({
    site: 'nyc_acris',
    purpose: 'diagnostic',
  });

  if (requestedTransportMode !== 'brightdata-browser-api') {
    throw new Error('NYC live selector validation requires BRIGHTDATA_BROWSER_WS Browser API transport');
  }

  const maxDocuments = Number(process.env.ACRIS_VALIDATION_MAX_DOCS ?? '2');
  console.error(
    `[${new Date().toISOString()}] nyc_validation starting transport=${requestedTransportMode} max_documents=${maxDocuments}`
  );
  const manifest = await validateNYCAcrisSelectors({
    max_documents: maxDocuments,
    onStageEvent: logStageEvent,
    transportPolicyPurpose: 'diagnostic',
  });

  console.log(
    JSON.stringify(
      {
        transportMode: manifest.transportMode,
        resultPagesVisited: manifest.resultPagesVisited,
        docCount: manifest.docIds.length,
        validatedDocs: manifest.documents.length,
        failures: manifest.failures,
        warnings: manifest.warnings,
        bootstrapTrace: manifest.bootstrapTrace,
        validationSteps: manifest.validationSteps,
      },
      null,
      2
    )
  );
}

void main().catch((err) => {
  if (err instanceof Error) {
    console.error(
      JSON.stringify(
        {
          error: err.message,
          name: err.name,
        },
        null,
        2
      )
    );
  } else {
    console.error(String(err));
  }
  process.exitCode = 1;
});
