#!/bin/bash
# deploy.sh — pull latest code, rebuild changed images, and restart services.
# Migrations are applied automatically by the flyway service on startup.
# Run manually: ssh cartracker /opt/cartracker/scripts/deploy.sh
# Or trigger via redeploy.sh for targeted service restarts.

set -e

REPO_DIR="/opt/cartracker"
OPS_URL="http://localhost:8060"

cd "$REPO_DIR"

echo "==> Ensuring external volumes exist..."
docker volume inspect cartracker_pgdata > /dev/null 2>&1 || docker volume create cartracker_pgdata
docker volume inspect cartracker_raw > /dev/null 2>&1 || docker volume create cartracker_raw
docker volume inspect n8n_data > /dev/null 2>&1 || docker volume create n8n_data

echo "==> Pulling latest code..."
git fetch origin
git checkout master
git pull origin master

echo "==> Rebuilding images..."
docker compose build

echo "==> Restarting services..."
docker compose up -d

echo "==> Waiting for services to stabilise..."
sleep 15

echo "==> Service status:"
docker compose ps

echo "==> Signalling deploy complete..."
curl -sf -X POST "$OPS_URL/deploy/complete" || echo "Warning: failed to signal deploy/complete"

echo "==> Done. Check logs with: docker compose logs -f <service>"
