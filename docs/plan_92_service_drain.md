# Plan 92: Service Drain Endpoints

**Status:** Planned
**Priority:** Low — quality-of-life improvement to deployment process; current deploy flow works but is clunky

---

## Overview

Each service currently exposes `/health` (liveness — "am I running?") but has no way to signal "am I idle and safe to shut down?". During deployments we set `deploy_intent` to block new work from starting, but have no clean mechanism to wait for in-flight work to finish before stopping containers.

This plan adds a `/ready` endpoint to each service that answers the drain question. The deployment flow becomes:

1. Set `deploy_intent` → Airflow sensors block new DAG runs from starting
2. Poll each service's `/ready` until all return idle
3. Stop / redeploy containers
4. Set `deploy_intent` back to `'none'` → sensors unblock, work resumes

The `runs` table and orphan checker remain unchanged — they handle history and stuck-run cleanup. `/ready` is purely a live signal for the deployment drain window.

---

## Per-Service Definition of "Idle"

| Service | Idle condition |
|---|---|
| `scraper` | No `runs` rows with `status = 'running'` owned by this instance |
| `processing` | No artifacts currently mid-parse — track with an in-memory counter or check `artifact_processing` rows in `'processing'` state |
| `ops` | Stateless request handler — always ready |
| `archiver` | No active archive jobs in flight — in-memory counter on active requests |

---

## Endpoint Contract

```
GET /ready
→ 200 {"ready": true}                        # idle, safe to stop
→ 200 {"ready": false, "reason": "..."}      # busy, do not stop yet
```

Always returns HTTP 200 — the `ready` field carries the signal. This keeps polling logic simple (no need to handle non-200 as a distinct case from "busy").

---

## Deployment Script Changes

The existing deploy script (or ops admin action) gains a drain step between setting `deploy_intent` and restarting containers:

```python
# pseudocode
set_deploy_intent("deploy")

drain_services = ["scraper", "processing", "archiver"]
poll_until_ready(drain_services, timeout=600)   # 10 min max

docker_compose_up()

set_deploy_intent("none")
```

The polling logic lives in the deploy script, not in Airflow — this is an operational concern, not a scheduling concern.

---

## What This Replaces

The current approach queries the `runs` table externally to guess whether the scraper is busy. That's fragile — a stuck `'running'` row (which the orphan checker cleans up) would block a deployment indefinitely. The `/ready` endpoint gives each service control over its own drain state and can apply smarter logic (e.g., "running but finishing in < 30s, report ready").

---

## Out of Scope

- Forceful drain (sending a signal to abort in-flight work) — not needed yet
- K8s readiness probes — same concept, different wiring; worth noting for future
- `ops` service drain — stateless, no drain needed
