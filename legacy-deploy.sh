#!/bin/bash
# LEGACY: Deployment script for historical Mission Control integration with lien-automation-v2

set -e  # Exit on any error

if [ "${ALLOW_LEGACY_MISSION_CONTROL:-}" != "1" ]; then
  echo "WARNING: legacy-deploy.sh is archived and must not be used in production."
  echo "Set ALLOW_LEGACY_MISSION_CONTROL=1 only for controlled archival/testing use."
  exit 2
fi

echo "Starting LEGACY Mission Control deployment..."

# Navigate to the project directory
cd "$(dirname "$0")"

# Check if we're in the right directory
if [ ! -f "package.json" ]; then
  echo "Error: This script must be run from the lien-automation-v2 directory"
  exit 1
fi

# Clone Mission Control repository if it doesn't exist
if [ ! -d "mission-control" ]; then
  echo "Cloning Mission Control repository..."
  git clone https://github.com/crshdn/mission-control.git
else
  echo "Mission Control repository already exists, pulling latest changes..."
  cd mission-control
  git pull
  cd ..
fi

# Install dependencies for Mission Control
echo "Installing Mission Control dependencies..."
cd mission-control
npm install
cd ..

# Create necessary directories
echo "Creating directories..."
mkdir -p agents config crontab data

# Copy MCP agents to Mission Control directory
echo "Copying MCP agents..."
cp agents/*.ts mission-control/

# Build the project
echo "Building lien-automation-v2..."
npm run build

# Generate secure tokens for Mission Control
echo "Generating secure tokens..."
MC_API_TOKEN=$(openssl rand -hex 32)
WEBHOOK_SECRET=$(openssl rand -hex 32)

# Update the Mission Control environment file with generated tokens
echo "Updating Mission Control environment configuration..."
sed -i "s/generated_api_token_here/$MC_API_TOKEN/g" config/.env.mission-control
sed -i "s/generated_webhook_secret_here/$WEBHOOK_SECRET/g" config/.env.mission-control

# Build Docker images
echo "Building Docker images..."
docker-compose build

# Start services
echo "Starting services..."
docker-compose up -d

# Set up cron job
echo "Setting up cron job..."
# Note: In a production environment, you would add this to the actual crontab
# For now, we'll just show what the cron entry would look like
echo "Add this line to your crontab (crontab -e):"
echo "0 2 1 * * /root/lien-automation-v2/crontab/monthly-lien-scrape.sh"

# Set up Qwen cost tracking
echo "Setting up Qwen cost tracking..."
mkdir -p data
touch data/qwen-costs.log

echo "Deployment completed!"
echo "Access Mission Control at: http://localhost:4000"
echo "Access lien-automation-v2 at: http://localhost:8080"
echo "Qwen cost tracking dashboard: /root/lien-automation-v2/docs/qwen-cost-dashboard.html"
echo "Qwen cost logs: /root/lien-automation-v2/data/qwen-costs.log"
