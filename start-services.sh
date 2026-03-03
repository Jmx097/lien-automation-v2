#!/bin/bash
# Startup script for lien-automation-v2 with Mission Control integration

echo "Starting lien-automation-v2 with Mission Control integration..."

# Start lien-automation-v2 service
echo "Starting lien-automation-v2 service..."
cd /root/lien-automation-v2
docker-compose up -d lien-scraper

# Start Mission Control
echo "Starting Mission Control..."
cd /root/lien-automation-v2/mission-control
nohup npm run dev > /root/lien-automation-v2/logs/mission-control.log 2>&1 &

echo "Services started:"
echo "- lien-automation-v2 API: http://localhost:8080"
echo "- Mission Control Dashboard: http://localhost:4000"
echo ""
echo "Logs are available at:"
echo "- lien-automation-v2 logs: docker-compose logs -f lien-scraper"
echo "- Mission Control logs: tail -f /root/lien-automation-v2/logs/mission-control.log"