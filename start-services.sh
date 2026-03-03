#!/bin/bash
# Startup script for lien-automation-v2 API only (Mission Control is archived/disabled)

echo "Starting lien-automation-v2 API (Mission Control archived/disabled)..."

# Start lien-automation-v2 service
echo "Starting lien-automation-v2 service..."
cd /root/lien-automation-v2
docker compose up -d lien-scraper

echo "Services started:"
echo "- lien-automation-v2 API: http://localhost:8080"
echo ""
echo "Logs are available at:"
echo "- lien-automation-v2 logs: docker compose logs -f lien-scraper"
