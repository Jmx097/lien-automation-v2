#!/bin/bash
# Clean startup procedure for optimized Mission Control setup

echo "=== Clean Startup Procedure ==="
echo "Starting lien-automation-v2 and Mission Control with optimizations..."

# Create logs directory
mkdir -p /root/lien-automation-v2/logs

# Function to check if service is running
check_service() {
    local url=$1
    local name=$2
    if curl -sS --max-time 5 "$url" >/dev/null 2>&1; then
        echo "✓ $name is accessible"
        return 0
    else
        echo "✗ $name is not accessible"
        return 1
    fi
}

# Check if lien-scraper is already running
echo "Checking lien-automation-v2 service..."
if docker ps | grep -q lien-automation; then
    echo "lien-automation-v2 container is already running"
else
    echo "Starting lien-automation-v2 container..."
    cd /root/lien-automation-v2
    docker start 84c0c206ef22 2>/dev/null || {
        echo "Failed to start existing container, attempting docker-compose..."
        docker-compose up -d lien-scraper
    }
fi

# Wait a moment for lien-scraper to start
sleep 5

# Kill any existing Mission Control processes
echo "Stopping existing Mission Control processes..."
pkill -f "next start" 2>/dev/null
pkill -f "mission-control" 2>/dev/null

# Wait for ports to be released
sleep 3

# Start Mission Control
echo "Starting Mission Control..."
cd /root/lien-automation-v2/mission-control
nohup npm start > /root/lien-automation-v2/logs/mission-control.log 2>&1 &

# Wait for services to initialize
echo "Waiting for services to initialize..."
sleep 10

# Check service status
echo ""
echo "=== Service Status ==="
check_service "http://localhost:8080/health" "lien-automation-v2 API"
check_service "http://localhost:4000" "Mission Control Dashboard"

# Show process information
echo ""
echo "=== Process Information ==="
echo "Mission Control processes:"
ps aux | grep -E "(next|mission-control)" | grep -v grep

echo ""
echo "Docker containers:"
docker ps | grep lien

echo ""
echo "=== Startup Complete ==="
echo "Services should now be running with optimized resource usage."
echo "Check logs at: /root/lien-automation-v2/logs/"