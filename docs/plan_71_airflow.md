# Plan 71: Airflow Migration

**Status:** In progress — steps 1–13 complete; steps 14–15 (n8n cutover + decommission) pending
**Priority:** Medium — strong portfolio signal; replaces n8n entirely

## Overview

Replace n8n with Apache Airflow. The n8n workflows are opaque JSON blobs — not reviewable in PRs, not unit-testable, not recognizable to hiring managers. Airflow DAGs are Python code: readable, diffable, testable, and on nearly every DE job description.

This migration is also the right moment to move business logic out of n8n Postgres nodes and into Python services. The goal is not just to replicate what n8n does in Airflow — it's to end up with a system where the orchestrator (Airflow) is thin and dumb, and the services are fat and testable.

Estimated effort: **55–70 hours** (increased from original estimate to account for the processing service and scraper slimming).

---

## Design Principles

### Fat services, thin DAGs

Logic lives in service endpoints. DAG tasks call those endpoints via HTTP. The DAG is a dependency graph, not a logic container.

This is what enables two planned future states:
- **Kafka**: when events replace schedules, a Kafka consumer calls the same endpoint the DAG task calls today. The service doesn't change; the trigger does.
- **Multi-VM scrapers**: stateless scrapers on separate VMs call a central coordinator. No shared state, no shared filesystem required on the scraper VM.

If logic is placed in Airflow task functions instead of service endpoints, both of those future states require rewriting the logic. That's the mistake to avoid.

### Thin DAG tasks look like this

```python
def process_artifacts(run_id: str):
    requests.post(f"{PROCESSING_URL}/artifacts/process", json={"run_id": run_id})

def advance_rotation():
    return requests.post(f"{OPS_URL}/scrape/rotation/advance").json()
```

The DAG orchestrates; the services act.

---

## Service Architecture Changes

This migration introduces one new service and redistributes responsibilities across existing services.

### New: `processing` service (main VM)

Extracts all Results Processing logic from n8n into a dedicated Python/FastAPI service. Stays on the main VM because it needs access to the local filesystem where raw artifact files are stored.

**Responsibilities:**
- Claim unprocessed artifacts from `artifacts_queue` (by minio_path — requires Plan 97)
- Read artifact files from MinIO
- Parse HTML/JSON → write to MinIO silver `observations` (primary — see Plan 96)
- Upsert `price_observations` and `vin_to_listing` Postgres HOT tables (see Plan 93)
- Handle unlisted vehicle logic: DELETE from `price_observations`
- Carousel: parse make/model, filter against `search_configs`, upsert matching into `price_observations`
- Manage `artifacts_queue` status (complete / retry / skip)

**Key endpoints:**
```
POST /artifacts/process          { run_id }   → processes all unprocessed artifacts for a run
POST /artifacts/process/{id}     { }          → processes a single artifact by artifact_id
GET  /artifacts/status/{run_id}  { }          → returns processing progress for a run
```

The `artifact_id`-based endpoint is intentional: when Kafka arrives, the consumer receives an event containing an `artifact_id` and calls this endpoint. The service fetches the file itself. The message is a pointer, not a payload.

**File storage:** Raw artifact files are written directly to MinIO by the scraper (Plan 97). The processing service reads them from MinIO via `artifacts_queue.minio_path`. Plan 97 is a prerequisite for this service — it delivers the scraper→MinIO write path and the `artifacts_queue` work queue. Multi-VM scraping (Plan 79) is separately on hold and unblocked once Plan 97 ships.

### Scraper: slimmed to a fetch machine

The scraper loses all logic that isn't directly about fetching pages. It becomes: receive work → fetch URLs → write raw artifacts → report done.

**Moves out of the scraper:**
- `advance_rotation` endpoint → ops service
- Claim management (batch claiming, claim release) → ops service
- Run lifecycle writes → ops service
- All JSON/HTML parsing → processing service
- All observation writing → processing service

**What stays in the scraper:**
- Browser stack (Patchright, FlareSolverr, curl_cffi impersonation)
- The fetch loop
- `raw_artifacts` writes (scraper still writes the file and the metadata row)
- A simple "job done" callback

### Ops service: gains coordination responsibilities

Becomes the central coordinator for scrape work. This is required for multi-VM scrapers — each scraper instance on any VM calls ops to claim work; no scraper owns the rotation or claim logic.

**New endpoints:**
```
POST /scrape/rotation/advance    → claims next due rotation slot; returns {slot, configs}
POST /scrape/claims/claim-batch  → returns a batch of listing_ids for a scraper instance
POST /scrape/claims/release      { run_id, results } → releases claims after completion
```

These wrap the existing DB-level coordination (`FOR UPDATE SKIP LOCKED`, `ON CONFLICT`) that already handles concurrency correctly. The logic doesn't change; the owner does.

---

## Airflow Primitives That Replace n8n Patterns

| n8n Pattern | Airflow Equivalent |
|---|---|
| Poll loop (Job Poller V2, health checks) | `HttpSensor` / custom `BaseSensorOperator` |
| While-loop with counter (Check Deploy Intent) | `PokeIntervalSensor` with `timeout` |
| Retry with wait (Build DBT 409 logic) | `retries=5, retry_delay=timedelta(seconds=60)` |
| Sub-workflow (Update n8n Runs Table) | Native Airflow run history — workflow disappears |
| Error Handler workflow | `on_failure_callback` on each DAG |
| Schedule trigger | `schedule` param on DAG |

---

## DAG Inventory

| DAG | Replaces | Notes |
|---|---|---|
| `scrape_listings` | Scrape Listings | `POST /ops/scrape/rotation/advance` → fan-out scrape tasks → `POST /scraper/run-search` per config |
| `scrape_detail_pages` | Scrape Detail Pages V2 | `POST /ops/scrape/claims/claim-batch` → scraper fetches → `POST /ops/scrape/claims/release` |
| `results_processing` | Results Processing + Job Poller V2 | HttpSensor awaits scrape completion; `POST /processing/artifacts/process` per run; dbt trigger after |
| `dbt_build` | Build DBT | HttpOperator → dbt_runner; retry on 409 lock conflict |
| `cleanup_artifacts` | Cleanup Artifacts | Archive → cleanup → mark-deleted chain; logic stays as SQL in task functions (cleanup-only, no Kafka path needed) |
| `cleanup_parquet` | Cleanup Parquet | Find expired months → delete from MinIO → mark deleted in DB |
| `orphan_checker` | Orphan Checker | Parallel SQL updates across runs/scrape_jobs/claims/artifact_processing; task functions are fine here |
| `delete_stale_emails` | Delete Stale Request Emails | Single `SQLExecuteQueryOperator` |

**Workflows that disappear entirely:**
- **Job Poller V2** → becomes an `HttpSensor` inside `results_processing`
- **Update n8n Runs Table** → Airflow metadata DB handles run history natively
- **Error Handler** → `on_failure_callback` on each DAG
- **Check Service Health / Containers Up / Check Deploy Intent** → become Sensors

**Note on cleanup and orphan DAGs:** These workflows contain SQL logic (retention rules, timeout queries) but have no Kafka future and no multi-service consumers. Putting their SQL directly in Airflow task functions is fine — they don't benefit from the endpoint pattern and don't need to be independently callable.

---

## Staleness Detection and the Kafka Bridge

`ops_vehicle_staleness` and `ops_detail_scrape_queue` are dbt models — the detection logic stays in dbt. What the Airflow DAG adds is an "emit" step after the dbt build:

```
dbt build → read ops_vehicle_staleness → fan out scrape tasks (Airflow)
                                        → publish listing_goes_stale events (Kafka, later)
```

The detection logic doesn't move. Only the output mechanism changes. Design the Airflow task as "read view, emit work items" so the emit target can be swapped.

---

## Architecture

- Airflow runs in Docker Compose alongside the existing stack (`apache/airflow` image)
- Uses the **existing Postgres instance** as the Airflow metadata DB (separate schema)
- DAGs live in `airflow/dags/` — Python files, checked into git, reviewed in PRs
- `processing/` — new service directory alongside `scraper/`, `ops/`, `dbt_runner/`
- `shared/db.py` reused by the processing service; no new DB connection patterns
- n8n stays running until all DAGs are validated, then decommissioned

---

## Rollout Order

1. ~~**Airflow service**~~ — ✓ done
2. ~~**Processing service scaffold**~~ — ✓ done (`processing/app.py` stub with `/health`)
3. ~~**Ops service: coordination endpoints**~~ — ✓ done (`advance_rotation`, `claim-batch`, `release-claims`)
4. ~~**`dbt_build` DAG**~~ — ✓ done
5. ~~**`orphan_checker` + `delete_stale_emails` + `cleanup_parquet`**~~ — ✓ done
6. ~~**`cleanup_artifacts` DAG**~~ — ✓ done
7. ~~**[V018 migration](plan_v018_schema_migration.md)**~~ — ✓ done. Created `ops.price_observations`, `ops.vin_to_listing`, `ops.blocked_cooldown`, `staging.detail_scrape_claim_events`, `staging.blocked_cooldown_events`; dropped dead Plan 89 tables; fixed UUID column types.
8. ~~**`scrape_listings` DAG**~~ — ✓ done. `advance_rotation` gap check migrated from `runs` to `search_configs.last_queued_at`; returns `run_id` UUID without writing `runs`. Fan-out scrape per config×scope; shadow-run alongside n8n.
9. ~~**`scrape_detail_pages` DAG**~~ — ✓ done. `claim-batch` → `scrape_detail/batch` → `release`; shadow-run alongside n8n. **Unblocks Plan 79.**
10. ~~**Processing service: core logic**~~ — ✓ done (Plan 93, 2026-04-21).
11. ~~**`results_processing` DAG**~~ — ✓ done (2026-04-21).
12. ~~**V029 migration (was "V019")**~~ — ✓ done (2026-04-27). `ops_vehicle_staleness` and `ops_detail_scrape_queue` rewritten as plain Postgres views reading HOT tables directly. `customer_id IS NULL` replaces full_details staleness; carousel pool merged into dealer_unenriched pool. dbt ops models deleted.
13. ~~**Scraper: add `/ready` + remove ported logic**~~ — ✓ done. `GET /ready` drain endpoint added; `/search_configs/advance_rotation` removed from scraper (now lives in ops).
14. **Disable n8n schedules** — cutover; n8n container stays up briefly as fallback.
15. **Decommission n8n** — remove from docker-compose, archive workflow JSONs to `docs/n8n_archive/`.

**Not in original rollout — added during execution:**
- ~~**`flush_silver_observations` + `flush_staging_events` DAGs**~~ — ✓ done (PR #86, 2026-04-21). Flush `staging.silver_observations` and staging event tables to MinIO Parquet on schedule. Live in production.
- ~~**`cleanup_queue` DAG**~~ — ✓ done (PR #88, 2026-04-27). Cleans up completed `artifacts_queue` entries.

---

## What Stays the Same

- `detail_scrape_claims` concurrency model (`FOR UPDATE SKIP LOCKED`) — just moves to an ops endpoint
- `blocked_cooldown` / `stg_blocked_cooldown` logic
- Deploy intent flag and ops admin UI
- dbt models and dbt_runner HTTP interface (until Plan 90 decommissions them)

## What Changes

- `advance_rotation` moves from scraper → ops
- Claim management moves from scraper → ops
- Scraper writes artifacts directly to MinIO; `artifacts_queue` replaces `raw_artifacts` + `artifact_processing` (Plan 97)
- Results Processing logic moves from n8n → processing service: reads from MinIO, writes to MinIO silver (primary) and Postgres HOT tables (`price_observations`, `vin_to_listing`)
- Scraper loses everything except the browser stack, fetch loop, and MinIO write
- n8n decommissioned
