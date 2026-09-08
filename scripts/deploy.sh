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

# Plan 170 Stage B decision 10. A build that cannot fit does not start; see
# the header of scripts/redeploy.sh for the argument. This is a floor check,
# not a build-size predictor, and the number must equal
# HOST_DISK_FLOORS["bytes_available"] in scripts/host_maintenance.py -- a test
# asserts they agree.
DISK_FLOOR_BYTES=10737418240
AVAILABLE_BYTES="$(df -B1 --output=avail / | tail -1 | tr -d ' ')"
if [ "$AVAILABLE_BYTES" -lt "$DISK_FLOOR_BYTES" ]; then
    echo "Refusing to build: / has $(( AVAILABLE_BYTES / 1024 / 1024 )) MiB free," >&2
    echo "  below the ${DISK_FLOOR_BYTES} byte floor this host is held to." >&2
    echo "  Nothing has been changed. Reclaim first, or deploy service by" >&2
    echo "  service with scripts/redeploy.sh:" >&2
    echo "    docker builder prune -a -f" >&2
    echo "    docs/runbooks/runbook_storage_maintenance.md §2" >&2
    exit 1
fi

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
# No size cap: `--keep-storage` was tried twice in production on 2026-09-08 and
# reclaimed nothing it was supposed to. Build cache is a pure speed
# optimisation -- nothing in production reads it -- so it is discarded whole
# and the next build pays for it. See decision 9 in redeploy.sh.
echo "==> Discarding the build cache this deploy produced..."
docker builder prune -a -f || echo "Warning: build cache prune failed; the deploy itself is unaffected"

echo "==> Done. Check logs with: docker compose logs -f <service>"
