#!/bin/bash
# redeploy.sh — rebuild and restart specified services, then signal deploy complete
# Usage: ./redeploy.sh scraper dbt_runner

set -e

OPS_URL="http://localhost:8060"
TELEGRAM_CHAT_ID="774819707"
SERVICES="$*"

_on_exit() {
    local exit_code=$?

    echo "Signalling deploy complete..."
    curl -sf -X POST "$OPS_URL/deploy/complete" \
        || echo "Warning: failed to signal /deploy/complete"

    if [ "$exit_code" -ne 0 ] && [ -n "$TELEGRAM_API" ]; then
        curl -sf -X POST "https://api.telegram.org/bot${TELEGRAM_API}/sendMessage" \
            --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
            --data-urlencode "text=Deploy FAILED [${SERVICES}] — intent released. Exit code: ${exit_code}" \
            || echo "Warning: failed to send Telegram alert"
    fi
}
trap _on_exit EXIT

if [ $# -eq 0 ]; then
  echo "Usage: $0 <service> [service ...]"
  echo "Example: $0 scraper dbt_runner"
  exit 1
fi

echo "Building: $SERVICES"
docker compose build "$@"

echo "Restarting containers..."
docker compose up -d "$@"

# TODO Plan 76: replace sleep with health endpoint polling
sleep 10

echo "Done."
