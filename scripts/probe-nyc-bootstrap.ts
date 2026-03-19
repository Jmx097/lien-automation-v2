import dotenv from 'dotenv';
import { resolveTransportMode, type BrowserTransportMode } from '../src/browser/transport';
import { probeNYCAcrisConnectivity } from '../src/scraper/nyc_acris';

// Usage:
//   Browser API: set BRIGHTDATA_BROWSER_WS and run `npm run probe:nyc-bootstrap`
//   Legacy CDP: unset BRIGHTDATA_BROWSER_WS, set SBR_CDP_URL, and run `npm run probe:nyc-bootstrap`
dotenv.config();

function readProbeTransportOverride(): BrowserTransportMode | undefined {
  const raw = process.env.NYC_ACRIS_PROBE_TRANSPORT_MODE?.trim();
  if (!raw) return undefined;

  if (
    raw === 'brightdata-browser-api' ||
    raw === 'brightdata-proxy' ||
    raw === 'legacy-sbr-cdp' ||
    raw === 'local'
  ) {
    return raw;
  }

  throw new Error(`Invalid NYC_ACRIS_PROBE_TRANSPORT_MODE: ${raw}`);
}

export interface BootstrapProbeOutput {
  requestedTransportMode: ReturnType<typeof resolveTransportMode>;
  transportMode: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['transportMode'];
  ok: boolean;
  detail?: string;
  failureClass?: string;
  recoveryAction: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['recoveryAction'];
  bootstrapStrategy: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['bootstrapStrategy'];
  diagnostic?: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['diagnostic'];
  steps?: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['steps'];
  bootstrapTrace?: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['bootstrapTrace'];
  failures?: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['failures'];
  warnings?: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>['warnings'];
}

export function buildBootstrapProbeOutput(
  result: Awaited<ReturnType<typeof probeNYCAcrisConnectivity>>,
): BootstrapProbeOutput {
  const transportModeOverride = readProbeTransportOverride();
  return {
    requestedTransportMode: resolveTransportMode({
      site: 'nyc_acris',
      purpose: 'diagnostic',
      transportModeOverride,
    }),
    transportMode: result.transportMode,
    ok: result.ok,
    detail: result.detail,
    failureClass: result.failureClass,
    recoveryAction: result.recoveryAction,
    bootstrapStrategy: result.bootstrapStrategy,
    diagnostic: result.diagnostic,
    steps: result.steps,
    bootstrapTrace: result.bootstrapTrace,
    failures: result.failures,
    warnings: result.warnings,
  };
}

export async function main(): Promise<void> {
  const transportModeOverride = readProbeTransportOverride();
  const result = await probeNYCAcrisConnectivity({
    transportPolicyPurpose: 'diagnostic',
    transportModeOverride,
  });
  console.log(JSON.stringify(buildBootstrapProbeOutput(result), null, 2));
}

if (require.main === module) {
  void main().catch((err) => {
    console.error(
      JSON.stringify(
        {
          requestedTransportMode: resolveTransportMode({
            site: 'nyc_acris',
            purpose: 'diagnostic',
            transportModeOverride: readProbeTransportOverride(),
          }),
          error: err instanceof Error ? err.message : String(err),
        },
        null,
        2,
      ),
    );
    process.exitCode = 1;
  });
}
