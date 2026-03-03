# Pipeline Gates

This directory contains the implementation of the reinforcement gates for the lien automation pipeline as described in the MCP_GATE_PIPELINE_PLAN.md.

## Gate 1: Pre-Run Health Check

Located in `src/gates/pre-run-health.ts`, this gate performs several checks before any scraping begins:

1. Docker container is running and responsive
2. Required environment variables are set:
   - `BRIGHT_DATA_PROXY`
   - `GOOGLE_SHEETS_CREDENTIALS`
   - `DATABASE_URL`
3. Canary request succeeds (fetch simple page, <5s timeout)
4. Playwright browsers installed

### Usage

```typescript
import { preRunHealthCheck } from './src/gates/pre-run-health';

const result = await preRunHealthCheck();
if (!result.success) {
  console.error('Pre-run health check failed:', result.errors);
  process.exit(1);
}
```

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