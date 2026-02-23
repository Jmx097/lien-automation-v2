# Lien Automation v2

A web scraper and automation tool for retrieving UCC (Uniform Commercial Code) filings and lien records from the California Secretary of State website, with automatic export to Google Sheets.

## Overview

This project provides an Express.js API server that scrapes lien data from the CA Secretary of State's UCC filing system and automatically pushes the results to Google Sheets for analysis and tracking.

**Current Status**: MVP phase with mock data implementation. The infrastructure validates Cloud Run deployment and Google Sheets integration before full automation is implemented.

## Features

- **Web Scraping**: Uses Playwright with Chromium for headless browser automation
- **Rate Limiting**: Built-in rate limiting to prevent overwhelming target servers
- **Retry Logic**: Automatic retry mechanisms for failed requests
- **Google Sheets Integration**: Direct data export to Google Sheets
- **Structured Logging**: JSON-formatted logs with timestamps

## Architecture

The project is organized into three main modules:

- **Scraper** (`src/scraper/`): Handles web scraping logic for CA SOS website
- **Sheets** (`src/sheets/`): Manages Google Sheets API integration
- **Utils** (`src/utils/`): Utility functions for delays, rate limiting, retries, and logging
- **Server** (`src/server.ts`): Express.js API server with `/scrape` endpoint

## API Usage

### POST /scrape

Initiates a scraping job for the specified date range.

**Request Body**:
```json
{
  "site": "ca_sos",
  "date_start": "01/01/2024",
  "date_end": "01/31/2024",
  "max_records": 100
}
```

**Required Fields**: `site`, `date_start`, `date_end`

**Response**:
```json
{
  "success": true,
  "records_scraped": 5,
  "rows_uploaded": 5,
  "duration_seconds": 2.5
}
```

## Environment Variables

Required environment variables:

- `SHEETS_KEY`: Google service account credentials (JSON string)
- `SHEET_ID`: Target Google Sheets spreadsheet ID
- `SBR_CDP_URL`: Bright Data Scraping Browser Playwright connection string (wss://brd-customer-hl_57a9fdd9-zone-lien_automation_v3:7g1mw53lymza@brd.superproxy.io:9222)

## Data Schema

Scraped records include the following fields:
- UCC Type (e.g., "Federal Tax Lien")
- Debtor Name
- Debtor Address
- File Number
- Secured Party Name
- Secured Party Address
- Status
- Filing Date
- Lapse Date
- Document Type
- PDF Filename

Data is exported to Google Sheets with a state column prepended.

## Technology Stack

- **Runtime**: Node.js with TypeScript
- **Web Framework**: Express.js
- **Scraping**: Playwright (Chromium)
- **API Integration**: Google Sheets API (googleapis)

## Server

The server runs on port 8080 by default.

## Notes

- This is currently an MVP. Full scraping automation is in progress.
- The scraper is configured to run in headless mode with sandbox disabled for container compatibility.
- Human-like delays are implemented between actions to mimic natural browsing patterns.
- Only California SOS (`ca_sos`) is supported in the current version.
- On `TooManyResultsError`, halve the date range and retry.
- The scraper uses Bright Data Scraping Browser via the `SBR_CDP_URL` connection string for remote Playwright sessions.
- `SBR_CDP_URL`: Bright Data Scraping Browser Playwright connection string (wss://brd-customer-hl_57a9fdd9-zone-lien_automation_v3:7g1mw53lymza@brd.superproxy.io:9222)
