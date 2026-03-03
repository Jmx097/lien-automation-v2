# Playwright MCP Debugging Guide

This document explains how to use Playwright MCP for debugging the lien automation pipeline.

## Overview

Playwright MCP is used for **observability and debugging only**, not for executing bulk scraping operations. It provides tools to inspect failing containers, network events, and capture traces.

## Allowed Uses

1. Inspecting console logs from failing containers
2. Inspecting network events (403/429 detection)
3. Capturing traces for debugging
4. Running short JS snippets to validate selectors
5. Reproducing single-record failures interactively

## Forbidden Uses

1. Running bulk scraping operations
2. Replacing Docker cron jobs
3. Processing multiple records in one session
4. Running long-lived sessions (>5 min)

## Example Commands

### Inspecting Console Logs

```bash
# Stream console output for a failing record
playwright-cli console --container lien-scraper --selector "#error-log"
```

### Inspecting Network Events

```bash
# Inspect network events for 403/429s
playwright-cli network --container lien-scraper --filter "status>=400"
```

### Capturing Traces

```bash
# Capture trace for a specific failure
playwright-cli tracing-start --container lien-scraper
# ... reproduce failure ...
playwright-cli tracing-stop --output /logs/traces/failure-<id>.zip
```

### Validating Selectors

```bash
# Validate selector exists
playwright-cli eval --container lien-scraper \
  "document.querySelector('#ucc-filing-table') !== null"
```

## Debugging Workflow

1. **Identify Failure**: Locate the failing record or chunk in logs
2. **Inspect Container**: Use Playwright MCP to examine the container state
3. **Capture Evidence**: Collect console logs, network events, and traces
4. **Reproduce Locally**: Use the captured information to reproduce the issue
5. **Fix and Verify**: Implement fix and verify with Playwright MCP

## Best Practices

1. Always start with console logs to understand the error context
2. Check network events for HTTP errors (403, 429, 5xx)
3. Capture traces for complex UI interactions
4. Validate selectors before assuming they're correct
5. Keep debugging sessions short and focused

## Example Debugging Session

```bash
# Check for errors in console
playwright-cli console --container lien-scraper --contains "error"

# Look for network failures
playwright-cli network --container lien-scraper --filter "status=403"

# Validate a specific selector
playwright-cli eval --container lien-scraper \
  "document.querySelectorAll('.filing-row').length > 0"

# If UI interaction is failing, capture a trace
playwright-cli tracing-start --container lien-scraper
# ... perform the failing action ...
playwright-cli tracing-stop --output /tmp/debug-trace.zip
```

## Common Failure Patterns

1. **Rate Limiting (429)**: Too many requests in a short time
2. **Authentication Issues (403)**: Proxy or credential problems
3. **Selector Failures**: DOM structure changed
4. **Timeouts**: Slow network or overloaded system
5. **Memory Issues**: Container running out of resources

## Troubleshooting Tips

1. Check the proxy connection status
2. Verify environment variables are correctly set
3. Ensure sufficient memory allocation for containers
4. Monitor CPU usage during scraping
5. Review recent changes to the target website