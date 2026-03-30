import dotenv from 'dotenv';
import { validateNYCAcrisSelectors } from '../src/scraper/nyc_acris';
import { resolveTransportMode, type BrowserTransportMode } from '../src/browser/transport';

dotenv.config();

function readValidationTransportOverride(): BrowserTransportMode | undefined {
  const raw = process.env.NYC_ACRIS_VALIDATION_TRANSPORT_MODE?.trim();
  if (!raw) return undefined;

  if (
    raw === 'brightdata-browser-api' ||
    raw === 'brightdata-proxy' ||
    raw === 'legacy-sbr-cdp' ||
    raw === 'local'
  ) {
    return raw;
  }

  throw new Error(`Invalid NYC_ACRIS_VALIDATION_TRANSPORT_MODE: ${raw}`);
}

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
  const transportModeOverride = readValidationTransportOverride();
  const requestedTransportMode = resolveTransportMode({
    site: 'nyc_acris',
    purpose: 'diagnostic',
    transportModeOverride,
  });

  if (requestedTransportMode === 'local') {
    throw new Error(
      'NYC live selector validation requires a remote browser transport. Configure BRIGHTDATA_BROWSER_WS, SBR_CDP_URL, or set NYC_ACRIS_VALIDATION_TRANSPORT_MODE accordingly.',
    );
  }

  const maxDocuments = Number(process.env.ACRIS_VALIDATION_MAX_DOCS ?? '2');
  console.error(
    `[${new Date().toISOString()}] nyc_validation starting transport=${requestedTransportMode} max_documents=${maxDocuments}`
  );
  const manifest = await validateNYCAcrisSelectors({
    max_documents: maxDocuments,
    onStageEvent: logStageEvent,
    transportPolicyPurpose: 'diagnostic',
    transportModeOverride,
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
          requestedTransportMode: resolveTransportMode({
            site: 'nyc_acris',
            purpose: 'diagnostic',
            transportModeOverride: readValidationTransportOverride(),
          }),
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
