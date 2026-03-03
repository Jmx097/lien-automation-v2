#!/bin/bash
# Final optimized restart script for lien-automation-v2 API only (Mission Control is archived/disabled)

echo "=== Optimized API Restart ==="
echo "Restarting lien-automation-v2 API with optimized settings..."

# Create logs directory if it doesn't exist
mkdir -p /root/lien-automation-v2/logs

# Start lien-automation-v2 service
echo "Ensuring lien-automation-v2 service is running..."
cd /root/lien-automation-v2
docker compose up -d lien-scraper

# Wait for service to initialize
echo "Waiting for service to initialize..."
sleep 10

# Check if service is running
echo "Checking service status..."
if curl -sS http://localhost:8080/health >/dev/null 2>&1; then
    echo "✓ lien-automation-v2 API is running"
else
    echo "✗ lien-automation-v2 API is not accessible"
fi

echo ""
echo "=== Service Started ==="
echo "Access URL:"
echo "- lien-automation-v2 API: http://localhost:8080"
echo ""
echo "Management Commands:"
echo "- Logs: docker compose logs -f lien-scraper"
echo "- Restart: docker compose up -d lien-scraper"
