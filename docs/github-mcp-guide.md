# GitHub MCP Integration Guide

This document explains how to use GitHub MCP to propose changes to the lien automation pipeline.

## Overview

GitHub MCP is used as a **gatekeeper** for code changes, not for executing scrapes. It enables safe, reviewable code changes through Pull Requests.

## Workflow

1. **Inspect**: Use `github.read_file` to understand current implementation
2. **Propose**: Use `github.update_file` with gate/chunking logic
3. **Submit**: Use `github.create_pull_request` with clear description
4. **Human review**: Jon reviews and merges manually
5. **Deploy**: Cron jobs pick up changes on next run

## Example Commands

### Reading Files

```bash
# Read a specific file to understand current implementation
github.read_file --repo Jmx097/lien-automation-v2 --path src/scraper/ca_sos_enhanced.ts
```

### Creating a Pull Request

```bash
# After making changes, create a PR
github.create_pull_request \
  --repo Jmx097/lien-automation-v2 \
  --title "Gate 1: Pre-Run Health Check" \
  --body "Implementation of pre-run health check as specified in MCP_GATE_PIPELINE_PLAN.md" \
  --branch feature/gate-1-health-check
```

## Best Practices

1. Keep PRs small and focused (one gate or feature per PR)
2. Include clear descriptions explaining what and why
3. Add tests for new functionality
4. Follow the established code style
5. Reference the pipeline plan in PR descriptions

## PR Structure

Each PR should follow this structure:

- **Title**: Brief description of the change
- **Description**: Detailed explanation of what, why, and testing
- **Files Changed**: Only the necessary files
- **Testing**: Instructions or automated tests

Example:

```markdown
## Gate 1: Pre-Run Health Check

### What
Adds pre-run health check that validates:
- Docker container is responsive
- Required env vars are set
- Canary request succeeds (<5s)

### Why
Fail fast before processing any records. Prevents wasted compute on broken configs.

### Testing
- Run `npm run test:gate1` to validate health check logic
- Manually test with missing env vars (should fail immediately)

### Files Changed
- `src/gates/pre-run-health.ts` (new)
- `src/scraper/index.ts` (integrate gate before processing)
- `tests/gates/pre-run-health.test.ts` (new)
```