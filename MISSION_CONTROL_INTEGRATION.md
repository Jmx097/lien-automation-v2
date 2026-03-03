# Mission Control Integration for lien-automation-v2

> **Status:** Archived reference. Mission Control is disabled in the default workspace/deployment.

This document describes the integration of Mission Control with the lien-automation-v2 repository to enable multi-agent orchestration for automated lien scraping.

## Overview

Mission Control is an AI Agent Orchestration Dashboard that allows you to:
- Create and manage tasks
- Plan with AI
- Dispatch tasks to agents
- Monitor agent activity in real-time

This integration extends the existing lien-automation-v2 system with:
- Multi-agent orchestration capabilities
- Real-time monitoring and task management
- Automated monthly lien scraping
- Qwen cost tracking and monitoring

## Repository Structure

```
lien-automation-v2/
├── mission-control/          # Cloned crshdn/mission-control repository
├── agents/                   # OpenClaw MCPs for chunk_scrape, validate_chunk, upload_chunk
│   ├── chunk_scrape.ts
│   ├── validate_chunk.ts
│   └── upload_chunk.ts
├── docker-compose.yml        # Updated to include mission-control service
├── crontab/                  # Monthly cron for >10-record jobs
│   └── monthly-lien-scrape.sh
├── config/                   # Environment configuration
│   └── .env.mission-control
├── scripts/                  # Utility scripts
│   └── qwen-cost-tracker.js
├── data/                     # Data storage
│   └── qwen-costs.log
└── docs/                     # Documentation
    └── qwen-cost-dashboard.html
```

## Components

### 1. Mission Control Dashboard

The Mission Control dashboard provides a web interface for:
- Creating and managing scraping tasks
- Monitoring agent activity
- Viewing task progress
- Analyzing system performance

Access the dashboard at: `http://localhost:4000`

### 2. OpenClaw MCP Agents

Three custom MCP (Mission Control Protocol) agents have been created:

1. **chunk_scrape.ts** - Handles scraping lien data in chunks
2. **validate_chunk.ts** - Validates scraped data and performs checksums
3. **upload_chunk.ts** - Uploads validated data to Google Sheets

### 3. Docker Configuration

The `docker-compose.yml` file has been updated to include:
- The existing `lien-scraper` service
- A new `mission-control` service
- Proper networking between services

### 4. Qwen Cost Tracking

A cost tracking system has been implemented to monitor Qwen API usage:
- Tracks prompt and completion tokens
- Estimates costs based on token usage
- Logs data to `data/qwen-costs.log`
- Provides a dashboard for visualization

Access the dashboard at: `/root/lien-automation-v2/docs/qwen-cost-dashboard.html`

### 5. Automated Monthly Scraping

A cron job script automates monthly lien scraping:
- Runs on the first day of each month at 2 AM EST
- Processes multiple sites in chunks
- Sends tasks to Mission Control for orchestration

Script location: `/root/lien-automation-v2/crontab/monthly-lien-scrape.sh`

## Deployment

### Prerequisites

- Node.js v18+
- Docker and Docker Compose
- OpenClaw Gateway
- Alibaba Cloud DashScope API key
- BrightData proxy account

### Setup

1. Run the deployment script:
   ```bash
   cd lien-automation-v2
   ./deploy.sh
   ```

2. Add the cron job to your crontab:
   ```bash
   crontab -e
   # Add this line:
   0 2 1 * * /root/lien-automation-v2/crontab/monthly-lien-scrape.sh
   ```

### Environment Configuration

The Mission Control environment is configured in:
`/root/lien-automation-v2/config/.env.mission-control`

Key configuration variables:
- `DASHSCOPE_API_KEY` - Your Alibaba Cloud API key
- `BRIGHTDATA_SESSION` - Your BrightData session identifier
- `OPENCLAW_GATEWAY_TOKEN` - Your OpenClaw gateway token

## Usage

### Accessing Services

- Mission Control Dashboard: `http://localhost:4000`
- lien-automation-v2 API: `http://localhost:8080`
- Qwen Cost Dashboard: `/root/lien-automation-v2/docs/qwen-cost-dashboard.html`

### Creating Tasks

Tasks can be created through:
1. The Mission Control web interface
2. The API: `POST http://localhost:4000/api/tasks`
3. The monthly cron job script

### Monitoring

Monitor system performance through:
1. Mission Control dashboard
2. Qwen cost tracking dashboard
3. Docker logs: `docker-compose logs -f`

## Troubleshooting

### Common Issues

1. **Cannot connect to OpenClaw Gateway**
   - Check that OpenClaw is running: `openclaw gateway status`
   - Verify the gateway token in `.env.mission-control`

2. **Mission Control not starting**
   - Check Docker logs: `docker-compose logs mission-control`
   - Verify environment configuration

3. **Qwen cost tracking not working**
   - Check that `DASHSCOPE_API_KEY` is set correctly
   - Verify the API key has proper permissions

### Logs

- Mission Control logs: `docker-compose logs -f mission-control`
- lien-automation-v2 logs: `docker-compose logs -f lien-scraper`
- Qwen cost logs: `/root/lien-automation-v2/data/qwen-costs.log`

## Security

- API tokens are randomly generated during deployment
- Webhook signatures are used for validation
- All sensitive configuration is stored in environment files
- Docker containers run with minimal privileges

## Maintenance

Regular maintenance tasks:
1. Update Mission Control: `cd mission-control && git pull && docker-compose build && docker-compose up -d`
2. Rotate API tokens periodically
3. Monitor disk space for log files
4. Review cost tracking data for anomalies

## Support

For issues with this integration, please contact the development team.
