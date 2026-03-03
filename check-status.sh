#!/bin/bash
# Status check script for lien-automation-v2 with Mission Control integration

echo "Checking status of lien-automation-v2 services..."

# Check lien-automation-v2 service
echo "Checking lien-automation-v2 service..."
if curl -sS http://localhost:8080/health >/dev/null 2>&1; then
    echo "✓ lien-automation-v2 API is running"
else
    echo "✗ lien-automation-v2 API is not accessible"
fi

# Check Mission Control service
echo "Checking Mission Control service..."
if curl -sS http://localhost:4000 >/dev/null 2>&1; then
    echo "✓ Mission Control is running"
else
    echo "✗ Mission Control is not accessible"
fi

# Check Docker containers
echo "Checking Docker containers..."
if docker ps | grep -q lien-automation; then
    echo "✓ lien-automation-v2 container is running"
else
    echo "✗ lien-automation-v2 container is not running"
fi

# Check Qwen cost tracking log
echo "Checking Qwen cost tracking..."
if [ -f /root/lien-automation-v2/data/qwen-costs.log ]; then
    echo "✓ Qwen cost tracking log exists"
    echo "  Log size: $(du -h /root/lien-automation-v2/data/qwen-costs.log | cut -f1)"
else
    echo "✗ Qwen cost tracking log not found"
fi

echo ""
echo "Service URLs:"
echo "- lien-automation-v2 API: http://localhost:8080"
echo "- Mission Control Dashboard: http://localhost:4000"