#!/bin/bash
# Monthly lien scraping script for Mission Control
# Run first of month at 2am EST

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../.env"

# Define sites and chunks
SITES=("ucc-ca" "nyacris" "cook")
CHUNKS=(1 2)  # 2 chunks per site for 20+ records

# Loop through sites and chunks
for site in "${SITES[@]}"; do
  for chunk in "${CHUNKS[@]}"; do
    echo "Triggering scrape for site: $site, chunk: $chunk"
    
    # Send POST request to Mission Control API
    curl -X POST http://localhost:4000/api/tasks \
      -H "Content-Type: application/json" \
      -d "{\"title\":\"Scrape $site chunk $chunk\",\"description\":\"Monthly lien scraping for $site chunk $chunk\",\"agent\":\"chunker\",\"params\":{\"site\":\"$site\",\"chunk_id\":$chunk,\"date_start\":\"$(date -d "first day of last month" +%Y-%m-%d)\",\"date_end\":\"$(date -d "last day of last month" +%Y-%m-%d)\",\"max_records\":10}}"
    
    # Add a small delay between requests
    sleep 5
  done
done

echo "Monthly scraping tasks triggered"