# Contributing

## Prerequisites

- Node.js 22 (`.nvmrc`)
- `npm install`

## Local development

1. Copy environment defaults:
   ```bash
   cp .env.example .env
   ```
2. Set at least `SBR_CDP_URL` (or another Bright Data transport), `SHEETS_KEY`, `SHEET_ID`, and `SCHEDULE_RUN_TOKEN`.
3. Start the API:
   ```bash
   npm run dev
   ```

## Quality checks

Run the baseline checks before opening a PR:

```bash
npm run test:types
npm run test:selector-smoke
npm run test:scheduler
```

## Commit hygiene

- Do not commit HAR files, temporary shell/cmd scripts, or runtime data artifacts.
- Keep secrets out of the repo (`.env`, keys, tokens, credentials).
