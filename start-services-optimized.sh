#!/bin/bash
# Optimized startup script for lien-automation-v2 with Mission Control integration

echo "Starting lien-automation-v2 with Mission Control integration (optimized version)..."

# Create logs directory if it doesn't exist
mkdir -p /root/lien-automation-v2/logs

# Start lien-automation-v2 service with resource limits
echo "Starting lien-automation-v2 service..."
cd /root/lien-automation-v2
docker-compose up -d lien-scraper

# Start Mission Control with optimized settings
echo "Starting Mission Control (production mode)..."
cd /root/lien-automation-v2/mission-control

# Set Node.js memory limits
export NODE_OPTIONS="--max-old-space-size=1024"

# Start production server in background with resource limits
nohup npm start > /root/lien-automation-v2/logs/mission-control.log 2>&1 &

echo "Services started:"
echo "- lien-automation-v2 API: http://localhost:8080"
echo "- Mission Control Dashboard: http://localhost:4000"
echo ""
echo "Logs are available at:"
echo "- lien-automation-v2 logs: docker-compose logs -f lien-scraper"
echo "- Mission Control logs: tail -f /root/lien-automation-v2/logs/mission-control.log"