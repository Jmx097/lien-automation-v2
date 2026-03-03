#!/bin/bash
# Clean startup procedure for lien-automation-v2 API only (Mission Control is archived/disabled)

echo "=== Clean Startup Procedure ==="
echo "Starting lien-automation-v2 API only (Mission Control archived/disabled)..."

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

# Ensure lien-scraper is running
echo "Ensuring lien-automation-v2 service is running..."
cd /root/lien-automation-v2
docker compose up -d lien-scraper

# Wait a moment for lien-scraper to start
sleep 5

# Check service status
echo ""
echo "=== Service Status ==="
check_service "http://localhost:8080/health" "lien-automation-v2 API"

echo ""
echo "Docker containers:"
docker ps | grep lien

echo ""
echo "=== Startup Complete ==="
echo "lien-automation-v2 API should now be running."
echo "Check logs with: docker compose logs -f lien-scraper"
