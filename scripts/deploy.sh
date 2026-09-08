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

# Plan 170 Stage B. The build above is one of exactly two things that build on
# this host, and build cache is ~90% of its storage growth (2.04 -> 7.52 GB in
# 8 days, against images that stayed flat). So the reclaim belongs to the
# producer rather than to a scheduled job, and it runs here, after the deploy
# has been verified and signalled. `|| echo` is load-bearing: under `set -e` a
# failing prune would fail a deploy that has already succeeded.
#
# `-a` is what makes the cap bind at all, learned in production 2026-09-08:
# without it BuildKit protects internal, frontend and *shared* records, so the
# first run stopped at 5.72 GB with the 4GB cap never reached. See decision 9
# in redeploy.sh for the measurement.
echo "==> Pruning build cache back to the 4GB cap..."
docker builder prune -a --keep-storage 4GB -f || echo "Warning: build cache prune failed; the deploy itself is unaffected"

echo "==> Done. Check logs with: docker compose logs -f <service>"
