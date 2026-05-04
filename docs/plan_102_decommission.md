# Plan 102: Full Decommission — n8n, Legacy Postgres, Dead Service Code

**Status:** Complete (2026-04-29)
**Supersedes:** Plan 90 (dbt decommission / dbt-duckdb migration — absorbed and extended)

---

## Overview

With Airflow running all pipelines, dbt on DuckDB, MinIO as the primary observation store, and the processing service owning all parsing, the system still carries a large body of n8n-era tables, endpoints, and infrastructure. This plan removes all of it.

The work divides into four tracks that can be executed in parallel within a track but must respect cross-track ordering at the migration step.

---

## Track 1 — n8n Infrastructure Removal

**No DB changes. No service logic changes. Safe to do first.**

### Files to delete
- `n8n/` directory (15 workflow JSONs + `entrypoint.sh`)

### `docker-compose.yml`
- Remove `n8n` service block
- Remove `n8n_data` volume declaration
- Remove `n8n` from `caddy` `depends_on`

### `Caddyfile`
- Remove `handle /n8n*` block

### Test naming cleanup (cosmetic — n8n never ships)
- `tests/scraper/processors/test_scrape_detail.py`: rename `N8N_ARTIFACT_KEYS` → `ARTIFACT_KEYS`, `TestV3N8nContract` → `TestArtifactContract`, update assertion messages
- `tests/scraper/processors/test_results_page_cards.py`: rename `N8N_LISTING_FIELDS` → `LISTING_FIELDS`, `TestV3N8nContract` → `TestListingContract`
- `tests/scraper/processors/test_parse_detail_page.py`: rename `N8N_PRIMARY_FIELDS` → `PRIMARY_FIELDS`, `TestN8nContract` → `TestDetailContract`
- Comment updates in `scraper/processors/scrape_results.py`, `scraper/app.py`, `archiver/processors/cleanup_parquet.py`

---

## Track 2 — Scraper Dead Endpoint Removal

These endpoints were called by n8n workflows. No Airflow DAG calls them.

### `scraper/app.py` — remove endpoints
| Endpoint | Was called by |
|---|---|
| `POST /process/results_pages` | n8n Results Processing workflow |
| `POST /process/detail_pages` | n8n Results Processing workflow |
| `POST /scrape_results/retry` | n8n Job Poller retry path |
| `GET /search_configs/{search_key}/known_vins` | n8n Scrape Listings (reads `analytics.int_vehicle_attributes`, being dropped) |

### `scraper/app.py` — remove dead imports/helpers
- Async pool (`get_pool`, `asyncpg`) used only by `known_vins` endpoint
- Any orphan references to `analytics.int_vehicle_attributes`

### Tests to remove
- Unit tests covering the four removed endpoints

---

## Track 3 — ops Service + Orphan Checker Cleanup

### `ops/routers/scrape.py`
Remove `runs` tracking from the claim/release lifecycle. The Airflow DAG run is the batch tracker.
- `claim_batch`: remove `INSERT INTO runs` and `UPDATE runs SET status='skipped'`
- `release_claims`: remove `UPDATE runs SET status=...`

### `ops/routers/admin.py`
- Remove `GET /admin/runs` endpoint and handler
- Remove `GET /admin/runs/{run_id}` endpoint and handler
- Remove corresponding Jinja2 templates: `admin/runs.html`, `admin/run_detail.html` (if present)

### `ops/routers/maintenance.py`
Remove endpoints whose underlying tables are being dropped:
- `POST /maintenance/expire-orphan-runs` (references `public.runs`)
- `POST /maintenance/expire-orphan-processing-runs` (references `public.processing_runs`)
- `POST /maintenance/reset-stale-artifact-processing` (references `public.artifact_processing`)
- `POST /maintenance/expire-orphan-scrape-jobs` (references `public.scrape_jobs`)

### `ops/queries.py`
Remove constants: `EXPIRE_ORPHAN_RUNS`, `EXPIRE_ORPHAN_PROCESSING_RUNS`, `RESET_STALE_ARTIFACT_PROCESSING`, `EXPIRE_ORPHAN_SCRAPE_JOBS`

Remove corresponding SQL files from `ops/sql/`:
- `expire_orphan_runs.sql`
- `expire_orphan_processing_runs.sql`
- `reset_stale_artifact_processing.sql`
- `expire_orphan_scrape_jobs.sql`

### `ops/routers/deploy.py`
Rewrite `_intent_status()` to reflect the current architecture. Replace the three-CTE join (`n8n_executions` + `runs` + `processing_runs`) with:
- `ops.artifacts_queue` WHERE `status IN ('pending', 'processing')` — artifacts in flight
- `ops.detail_scrape_claims` WHERE `status = 'running'` — detail batches in flight

### `airflow/dags/orphan_checker.py`
Remove three dead tasks and their callables:
- `expire_orphan_runs` / `_expire_orphan_runs()`
- `expire_orphan_processing_runs` / `_expire_orphan_processing_runs()`
- `reset_stale_artifact_processing` / `_reset_stale_artifact_processing()`
- `expire_orphan_scrape_jobs` / `_expire_orphan_scrape_jobs()`

Only `expire_orphan_detail_claims` remains — rename DAG tasks for clarity if desired.

### Integration test cleanup
- `tests/integration/sql/test_ops_queries.py`: remove any smoke tests that reference `n8n_executions`, `runs`, `scrape_jobs`, `processing_runs`

---

## Track 4 — Archiver Cleanup + `staging.artifacts_queue_events` Wiring

### Problem
`staging.artifacts_queue_events` was created in V017 per the hot+staging architecture but nothing writes to it. Every status transition on `ops.artifacts_queue` should produce an event row. The archiver's `cleanup_artifacts` pipeline reads `raw_artifacts` + `artifact_processing` to find local HTML files — but the scraper now writes directly to MinIO, so there are no local files. This runs as a permanent no-op.

### Add event writes — scraper (`scraper/app.py`)
On `INSERT INTO ops.artifacts_queue`, also insert into `staging.artifacts_queue_events`:
```sql
INSERT INTO staging.artifacts_queue_events
    (artifact_id, status, event_at, minio_path, artifact_type, fetched_at, listing_id, run_id)
VALUES (%s, 'pending', now(), %s, %s, %s, %s, %s)
```

### Add event writes — processing service
On every `UPDATE ops.artifacts_queue SET status = ...`, also insert the new status into `staging.artifacts_queue_events`. Status transitions: `pending → processing`, `processing → complete`, `processing → retry`, `processing → skip`.

### Archiver — remove dead pipeline

**Delete files:**
- `archiver/processors/archive_artifacts.py`
- `archiver/processors/cleanup_artifacts.py`
- `archiver/sql/get_cleanup_candidates.sql`
- `archiver/sql/mark_artifacts_deleted.sql`

**Update `archiver/app.py`:** Remove endpoints:
- `POST /archive/artifacts` (calls `archive_artifacts`)
- `POST /cleanup/artifacts/run` (calls `run_cleanup_artifacts`)

**Update `airflow/dags/cleanup_artifacts.py`:** Replace call to `/cleanup/artifacts/run` with `/cleanup/parquet/run`. The DAG continues to exist; it now only runs the Parquet retention sweep (already implemented in `archiver/processors/cleanup_parquet.py`).

---

## Flyway Migrations

Order is important — views must be dropped before tables; FKs constrain table drop order.

### V034 — Drop observation tables + analytics schema

```sql
-- Legacy observation tables (migrated to MinIO silver in Plan 100)
DROP TABLE IF EXISTS public.srp_observations CASCADE;
DROP TABLE IF EXISTS public.detail_observations CASCADE;
DROP TABLE IF EXISTS public.detail_carousel_hints CASCADE;
DROP TABLE IF EXISTS public.pipeline_errors;

-- analytics schema: views first (dependency order)
DROP VIEW IF EXISTS analytics.int_carousel_price_events_mapped;
DROP VIEW IF EXISTS analytics.int_carousel_price_events_unmapped;
DROP VIEW IF EXISTS analytics.int_price_history_by_vin;
DROP VIEW IF EXISTS analytics.int_dealer_inventory;
DROP VIEW IF EXISTS analytics.int_scrape_targets;
DROP VIEW IF EXISTS analytics.stg_blocked_cooldown;   -- V029 inlined formula; now unused
DROP VIEW IF EXISTS analytics.stg_dealers;
DROP VIEW IF EXISTS analytics.stg_search_configs;

-- analytics schema: tables (dbt Postgres era — now in DuckDB)
DROP TABLE IF EXISTS analytics.stg_srp_observations;
DROP TABLE IF EXISTS analytics.stg_detail_observations;
DROP TABLE IF EXISTS analytics.stg_detail_carousel_hints;
DROP TABLE IF EXISTS analytics.stg_raw_artifacts;
DROP TABLE IF EXISTS analytics.int_carousel_hints_filtered;
DROP TABLE IF EXISTS analytics.int_listing_to_vin;
DROP TABLE IF EXISTS analytics.int_latest_dealer_name_by_vin;
DROP TABLE IF EXISTS analytics.int_latest_price_by_vin;
DROP TABLE IF EXISTS analytics.int_latest_tier1_observation_by_vin;
DROP TABLE IF EXISTS analytics.int_listing_current_state;
DROP TABLE IF EXISTS analytics.int_listing_days_on_market;
DROP TABLE IF EXISTS analytics.int_model_price_benchmarks;
DROP TABLE IF EXISTS analytics.int_price_events;
DROP TABLE IF EXISTS analytics.int_price_percentiles_by_vin;
DROP TABLE IF EXISTS analytics.int_vehicle_attributes;
DROP TABLE IF EXISTS analytics.int_vin_current_state;
DROP TABLE IF EXISTS analytics.mart_deal_scores;
DROP TABLE IF EXISTS analytics.mart_vehicle_snapshot;
DROP TABLE IF EXISTS analytics.scrape_targets;
```

### V035 — Drop n8n + runs-era tables

```sql
DROP TABLE IF EXISTS public.n8n_executions;
DROP TABLE IF EXISTS public.pipeline_errors;   -- if not already in V034
DROP TABLE IF EXISTS public.runs CASCADE;      -- CASCADE drops FK from scrape_jobs
DROP TABLE IF EXISTS public.scrape_jobs;
DROP TABLE IF EXISTS public.processing_runs;
DROP TABLE IF EXISTS public.dbt_intents;
DROP TABLE IF EXISTS public.dbt_lock;
DROP TABLE IF EXISTS public.dbt_runs;
```

### V036 — Drop raw artifact tables (after archiver cleanup is deployed)

```sql
DROP TABLE IF EXISTS public.artifact_processing;  -- no FK deps remain
DROP TABLE IF EXISTS public.raw_artifacts CASCADE;
```

V036 requires Track 4 (archiver cleanup) to be deployed first — the archiver must stop reading `raw_artifacts` before the table can be dropped.

---

## What Stays

| Table | Reason |
|---|---|
| `public.search_configs` | Core config — scraper + ops |
| `public.dealers` | Reference data |
| `public.deploy_intent` | Active (updated query in Track 3) |
| `ops.artifacts_queue` | Active work queue |
| `ops.price_observations` | HOT table |
| `ops.vin_to_listing` | HOT table |
| `ops.blocked_cooldown` | HOT table |
| `ops.detail_scrape_claims` | Active claim table |
| `ops.tracked_models` | Target filtering |
| `ops.ops_vehicle_staleness` | View |
| `ops.ops_detail_scrape_queue` | View |
| `staging.*` | All event buffers |

---

## Rollout Sequence

1. **Track 1** (n8n infra) — commit, deploy, done
2. **Tracks 2 + 3** — can be done in parallel; one PR each
3. **V034** — safe to run as soon as Tracks 1–3 are deployed (no code depends on analytics schema or observation tables)
4. **V035** — run after Track 3 ops code is deployed
5. **Track 4** (archiver + event writes) — implement + test
6. **V036** — run after Track 4 is deployed and validated
