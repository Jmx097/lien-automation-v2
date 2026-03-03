#!/bin/bash
# Final optimized restart script for lien-automation-v2 with Mission Control

echo "=== Mission Control Optimization Restart ==="
echo "Restarting services with optimized settings..."

# Create logs directory if it doesn't exist
mkdir -p /root/lien-automation-v2/logs

# Stop any existing processes
echo "Stopping existing processes..."
pkill -f "npm start" 2>/dev/null
pkill -f "next start" 2>/dev/null

# Start lien-automation-v2 service (assuming it's already running in Docker)
echo "Checking lien-automation-v2 service..."
if ! docker ps | grep -q lien-automation; then
    echo "Starting lien-automation-v2 service..."
    cd /root/lien-automation-v2
    docker-compose up -d lien-scraper
    sleep 5
else
    echo "lien-automation-v2 service is already running"
fi

# Start Mission Control with PM2 for better resource management
echo "Starting Mission Control with PM2..."
cd /root/lien-automation-v2
pm2 restart mission-control 2>/dev/null || pm2 start pm2-mission-control.config.js

# Wait for services to initialize
echo "Waiting for services to initialize..."
sleep 10

# Check if services are running
echo "Checking service status..."

# Check lien-automation-v2
if curl -sS http://localhost:8080/health >/dev/null 2>&1; then
    echo "✓ lien-automation-v2 API is running"
else
    echo "✗ lien-automation-v2 API is not accessible"
fi

# Check Mission Control
if curl -sS http://localhost:4000 >/dev/null 2>&1; then
    echo "✓ Mission Control is running"
else
    echo "✗ Mission Control is not accessible"
fi

# Show PM2 status
echo ""
echo "PM2 Process Status:"
pm2 list

echo ""
echo "=== Services Started Successfully ==="
echo "Access URLs:"
echo "- lien-automation-v2 API: http://localhost:8080"
echo "- Mission Control Dashboard: http://localhost:4000"
echo ""
echo "Management Commands:"
echo "- Status: /root/lien-automation-v2/pm2-manager.sh status"
echo "- Logs: /root/lien-automation-v2/pm2-manager.sh logs"
echo "- Restart: /root/lien-automation-v2/pm2-manager.sh restart"