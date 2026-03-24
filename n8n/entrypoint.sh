#!/bin/sh
# Import all workflow JSON files from /workflows/ into n8n on startup.
# Existing workflows with matching IDs are updated; new ones are created.
# Credentials are NOT overwritten — they stay in the n8n database.

set -e

echo "[entrypoint] Importing workflows from /workflows/ ..."
if [ -d /workflows ] && ls /workflows/*.json 1>/dev/null 2>&1; then
    n8n import:workflow --separate --input=/workflows/ 2>&1 || {
        echo "[entrypoint] WARNING: workflow import failed (may be first run — n8n DB not ready yet)"
    }
    echo "[entrypoint] Workflow import complete."
    echo "[entrypoint] Re-activating all workflows ..."
    n8n update:workflow --all --active=true 2>&1 || {
        echo "[entrypoint] WARNING: workflow activation failed"
    }
else
    echo "[entrypoint] No workflow files found in /workflows/, skipping import."
fi

# Hand off to the default n8n entrypoint
exec n8n "$@"
