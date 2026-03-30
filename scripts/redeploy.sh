#!/bin/bash
# redeploy.sh — rebuild and restart specified services, then signal deploy complete
# Usage: ./redeploy.sh scraper dbt_runner
# Note: this script is a home-server placeholder; Plan 62 (CI/CD) will supersede it

set -e

OPS_URL="http://localhost:8060"

if [ $# -eq 0 ]; then
  echo "Usage: $0 <service> [service ...]"
  echo "Example: $0 scraper dbt_runner"
  exit 1
fi

echo "Building: $@"
docker compose build "$@"

echo "Restarting containers..."
docker compose up -d "$@"

# TODO Plan 76: replace sleep with health endpoint polling
sleep 10

echo "Signalling deploy complete..."
curl -sf -X POST "$OPS_URL/deploy/complete" || echo "Warning: failed to signal deploy/complete"

echo "Done."
