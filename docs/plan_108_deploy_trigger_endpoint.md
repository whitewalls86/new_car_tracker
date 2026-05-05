# Plan 108: Deploy Trigger Endpoint

**Status:** Planned
**Goal:** Add `POST /deploy/trigger` to the ops API so deploys can be initiated from the ops UI without SSH access, while keeping the existing `redeploy.sh` as the fallback for manual runs.

---

## Background

Today the deploy flow is: SSH to server → run `redeploy.sh <services>`. The script sets intent, builds, restarts containers, and releases intent via trap. This works but requires a shell session. An ops API endpoint would allow triggering deploys from the UI or any HTTP client.

---

## Constraint: ops cannot redeploy itself

If `ops` is included in the services list, the container running the subprocess is killed mid-build. The EXIT trap in `redeploy.sh` never fires — intent is left stuck as `pending` for 30 minutes and no Telegram alert is sent. This is strictly worse than the current behaviour.

**Rule:** the endpoint must reject any request that includes `ops` in the services list. The allowlist approach (explicit permitted service names) is safer than a denylist.

---

## Prerequisites

- Docker socket mounted into the ops container: `-v /var/run/docker.sock:/var/run/docker.sock`
- `docker` CLI available inside the ops container (add to Dockerfile)
- `docker-compose.yml` accessible from inside the container (mount the repo root read-only, or copy compose file into image)
- `TELEGRAM_API` env var passed through to the ops container (already set for Grafana; add to ops service in compose)

---

## Implementation

### Allowlist

`ops/routers/deploy.py`:
```python
DEPLOYABLE_SERVICES = {"scraper", "processing", "dbt_runner"}
```

Any name not in this set → `400 Bad Request` immediately, before intent is set.

### Endpoint

```
POST /deploy/trigger
Body: {"services": ["scraper", "dbt_runner"]}
```

1. Validate all service names are in `DEPLOYABLE_SERVICES` → 400 if not
2. Call `_set_intent(...)` → 409 if already locked, 503 on DB error
3. Launch `redeploy.sh <services>` as a detached subprocess (`subprocess.Popen`, not `subprocess.run`)
4. Return `202 Accepted` immediately with `{"services": [...], "pid": <pid>}`

The subprocess inherits `TELEGRAM_API` from the container environment. The EXIT trap in `redeploy.sh` handles intent release and failure alerting — no change needed there.

### Auth

Gate behind the existing admin auth middleware (same as other mutating ops endpoints).

---

## What changes

| File | Change |
|------|--------|
| `ops/routers/deploy.py` | Add `DEPLOYABLE_SERVICES`, `POST /deploy/trigger` endpoint |
| `ops/Dockerfile` | Install `docker` CLI |
| `docker-compose.yml` | Mount `/var/run/docker.sock` + repo root into ops; pass `TELEGRAM_API` |
| `tests/ops/routers/test_deploy.py` | Tests for trigger endpoint: happy path, unknown service, already locked |

---

## Out of scope

- Progress streaming / log tailing (follow-on; would require SSE or websocket)
- Redeploying `ops` itself (must always be done via SSH + `redeploy.sh`)
- Any rollback or image pinning logic
