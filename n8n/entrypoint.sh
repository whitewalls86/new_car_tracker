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
    echo "[entrypoint] Re-activating workflows in dependency order ..."

    # Cache the workflow list once (ID|Name format)
    WF_LIST=$(n8n list:workflow 2>/dev/null || true)

    publish_workflow() {
        name="$1"
        id=$(echo "$WF_LIST" | grep "|${name}$" | cut -d'|' -f1)
        if [ -n "$id" ]; then
            n8n publish:workflow --id="$id" 2>&1 && echo "[entrypoint] Published: $name" \
                || echo "[entrypoint] WARNING: failed to publish $name ($id)"
        else
            echo "[entrypoint] WARNING: workflow not found in DB: $name"
        fi
    }

    # Publish leaves first, then callers — n8n requires sub-workflows to be
    # published before any workflow that calls them.
    #
    # Tier 1 — no sub-workflow dependencies
    publish_workflow "Check Service Health"
    publish_workflow "Update n8n Runs Table"
    # Tier 2 — depend on Tier 1
    publish_workflow "Containers Up"
    publish_workflow "Check Deploy Intent"
    # Tier 3 — depend on Tier 2
    publish_workflow "Build DBT"
    # Tier 4 — depend on Tier 3
    # Tier 5 — top-level callers
    publish_workflow "Error Handler"
    publish_workflow "Orphan Checker"
    publish_workflow "Cleanup Artifacts"
    publish_workflow "Cleanup Parquet"
    publish_workflow "Job Poller V2"
    publish_workflow "Results Processing"
    publish_workflow "Scrape Detail Pages V2"
    publish_workflow "Scrape Listings"
else
    echo "[entrypoint] No workflow files found in /workflows/, skipping import."
fi

# Hand off to the default n8n entrypoint
exec n8n "$@"
