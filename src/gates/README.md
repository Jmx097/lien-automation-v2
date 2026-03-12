# Pipeline Gates

This directory contains the implementation of the reinforcement gates for the lien automation pipeline as described in the MCP_GATE_PIPELINE_PLAN.md.

## Gate 1: Pre-Run Health Check

Located in `src/gates/pre-run-health.ts`, this gate performs several checks before any scraping begins:

1. Current runtime environment variables are present:
   - `SHEET_ID`
   - `SHEETS_KEY`
   - `SCHEDULE_RUN_TOKEN`
   - one browser transport (`BRIGHTDATA_BROWSER_WS`, `BRIGHTDATA_PROXY_SERVER`, or `SBR_CDP_URL`)
   - per-site schedule env vars for the checked sites
2. `SHEETS_KEY` parses as a valid service-account credential payload
3. OCR runtime is available when `REQUIRE_OCR_TOOLS != 0`
4. `GET /schedule/health` reports the service as ready
5. `GET /version` reports runtime metadata
6. Playwright is available in the local toolchain

### Usage

```typescript
import { preRunHealthCheck } from './src/gates/pre-run-health';

const result = await preRunHealthCheck();
if (!result.success) {
  console.error('Pre-run health check failed:', result.errors);
  process.exit(1);
}
```

Warnings are returned separately for softer conditions such as fallback publishing still being active.

## Retry Policy Utility

Located in `src/utils/retry-policy.ts`, this provides a configurable retry mechanism with exponential backoff and jitter.

### Features

- Configurable timeout per operation
- Exponential backoff with configurable multiplier
- Jitter to prevent thundering herd
- Automatic retry with progressive delays

### Usage

```typescript
import { processRecordWithRetry } from './src/utils/retry-policy';

const result = await processRecordWithRetry(
  'record-id',
  async (id) => {
    // Your operation here
    return await processRecord(id);
  }
);

if (result.success) {
  console.log('Operation succeeded:', result.result);
} else {
  console.error('Operation failed after retries:', result.error);
}
```

## Tests

Tests for the gates are located in `tests/gates/` and can be run with:

```bash
npm run test:gates
```

Tests for the retry policy are located in `tests/utils/retry-policy.test.ts` and can be run with:

```bash
npm run test:retry
```
