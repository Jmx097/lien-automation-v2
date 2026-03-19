import dotenv from 'dotenv';
import { resolveTransportMode } from '../src/browser/transport';
import { probeNYCAcrisConnectivity } from '../src/scraper/nyc_acris';

// Usage:
//   Browser API: set BRIGHTDATA_BROWSER_WS and run `npm run probe:nyc-bootstrap`
//   Legacy CDP: unset BRIGHTDATA_BROWSER_WS, set SBR_CDP_URL, and run `npm run probe:nyc-bootstrap`
dotenv.config();

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
  return {
    requestedTransportMode: resolveTransportMode(),
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
  const result = await probeNYCAcrisConnectivity();
  console.log(JSON.stringify(buildBootstrapProbeOutput(result), null, 2));
}

if (require.main === module) {
  void main().catch((err) => {
    console.error(
      JSON.stringify(
        {
          requestedTransportMode: resolveTransportMode(),
          error: err instanceof Error ? err.message : String(err),
        },
        null,
        2,
      ),
    );
    process.exitCode = 1;
  });
}
