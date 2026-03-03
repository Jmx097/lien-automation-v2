#!/bin/bash
# Optimized startup script for lien-automation-v2 API only (Mission Control is archived/disabled)

echo "Starting lien-automation-v2 API only (optimized version; Mission Control archived/disabled)..."

# Create logs directory if it doesn't exist
mkdir -p /root/lien-automation-v2/logs

# Start lien-automation-v2 service with resource limits
echo "Starting lien-automation-v2 service..."
cd /root/lien-automation-v2
docker compose up -d lien-scraper

echo "Service started:"
echo "- lien-automation-v2 API: http://localhost:8080"
echo ""
echo "Logs are available at:"
echo "- lien-automation-v2 logs: docker compose logs -f lien-scraper"
