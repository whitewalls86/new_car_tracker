# Plan 29: Set up n8n API

**Status:** Not started
**Priority:** Medium — foundation for several downstream plans

n8n exposes a REST API that allows programmatic interaction with workflows, executions, and credentials. Currently nothing in the project uses it — all n8n interaction is manual via the UI. Establishing an authenticated API client is the foundation that unlocks a set of downstream improvements.

## Foundation work
- Enable n8n API access (API key, base URL config)
- Build a thin client/wrapper (Python) usable from setup scripts and the scraper admin
- Document the API key as a required env var alongside `POSTGRES_PASSWORD`

## Use cases unlocked (sub-items, implemented separately)

**29.1 — Credential automation (fresh install)**
On fresh install, `setup.ps1` calls the n8n API to create the Postgres credential programmatically instead of requiring manual UI steps. Closes the silent failure gap where workflows import but fail on first run. *(Also tracked as Plan 67)*

**29.2 — Trigger detail scrape from admin UI**
Add a "Trigger Detail Scrape" button to the admin UI that calls the n8n API to fire the Scrape Detail Pages workflow on demand, without opening n8n.

**29.3 — Trigger SRP scrape from admin UI**
Same pattern — trigger a specific search config's SRP scrape on demand from the admin UI. Useful for testing a new config without waiting for the rotation schedule.

**29.4 — Workflow execution status in admin UI**
Surface recent n8n execution history in the admin run history page — show whether the last dbt build, detail scrape, or SRP scrape succeeded or failed, without leaving the admin.

**29.5 — Pause/resume workflows during redeploy**
During a planned redeploy (Plan 60), call the n8n API to deactivate scheduled workflows before bringing containers down, then reactivate them once health checks pass. Eliminates the race condition where n8n fires mid-redeploy.

## Downstream plans that depend on this
- Plan 67 — n8n credential automation
- Plan 60 — Safe redeploy (pause/resume workflows)
