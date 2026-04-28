# Plan 101: Dashboard Restructure + Analytics Migration

**Status:** Planned — depends on Plan 96 validation, Plan 86 implementation
**Supersedes:** Plan 90 ("dbt Decommission" / "dbt Migration to dbt-duckdb")

---

## Overview

This plan redefines the Streamlit dashboard's identity and cleans up the technical debt accumulated from the n8n era. It also supersedes Plan 90's dashboard section, which assumed dbt would be decommissioned; dbt is staying and migrating to DuckDB/MinIO (Plan 96).

The dashboard becomes a **pure analytics product** — deals, inventory, market trends — with a lightweight ops data health page sourced from dbt. All pipeline/infrastructure observability moves to Grafana (Plan 86). All n8n-era operational queries are deleted.

---

## Why Plan 90 Is Superseded

Plan 90 was originally titled "dbt Decommission" and later renamed "dbt Migration to dbt-duckdb." Plan 96 has already implemented the DuckDB source layer. What remains of Plan 90 (Flyway migration to drop legacy Postgres observation tables, switching the dbt default target) is absorbed into the post-Plan-96-validation work and does not require a separate dashboard restructure plan.

The dashboard section of Plan 90 (switching `dashboard/db.py` from psycopg2 → duckdb) is covered here with a cleaner framing.

---

## Dashboard Identity After This Plan

| Page | Source | Purpose |
|---|---|---|
| Deals | `analytics.mart_deal_scores` | User-facing: find underpriced vehicles |
| Inventory | `analytics.mart_vehicle_snapshot` | User-facing: browse tracked inventory |
| Market Trends | `analytics.mart_model_price_benchmarks` | User-facing: price context by model |
| Data Health (new) | `analytics.mart_*` (new ops models) | Admin: is the data good? coverage, staleness shape, cooldown distribution |

`pipeline_health.py` is deleted entirely. Everything metric/time-series shaped lives in Grafana.

---

## What Gets Deleted

### SQL files — all 21 current files are removed

**n8n legacy (tables being deprecated):**
- `active_runs.sql` — `runs` + `scrape_jobs`
- `runs_over_time.sql` — `runs`
- `terminated_runs.sql` — `runs`
- `recent_detail_runs.sql` — `runs` + `raw_artifacts` + analytics joins

**Complex analytics on HOT/deprecated tables — rebuild as dbt models when needed:**
- `stale_vehicle_backlog.sql` — `ops.ops_detail_scrape_queue` (HOT)
- `cooldown_backlog.sql` — `ops.blocked_cooldown` + queue (HOT)
- `blocked_cooldown_histogram.sql` — `ops.blocked_cooldown` (HOT)
- `price_freshness.sql` — `ops.ops_vehicle_staleness` (HOT view)
- `search_scrape_jobs.sql` — `ops.artifacts_queue` (deprecated as analytics source)

**Ops config — belongs in ops API or Airflow UI:**
- `rotation_schedule.sql` — schedule lives in Airflow; slot config accessible via ops API

**Legacy congestion artifacts:**
- `dbt_lock_status.sql` — lock existed to prevent dbt from competing with Postgres writers; irrelevant once dbt runs on DuckDB
- `dbt_build_history.sql` — dbt build metrics belong in Grafana Pipeline Health dashboard

**Migrates to Grafana (Plan 86) — deleted from dashboard SQL:**
- `airflow_dag_runs.sql`
- `processing_throughput.sql`
- `processor_activity.sql`
- `success_rate.sql`
- `detail_extraction_coverage.sql`
- `artifact_backlog.sql`
- `pipeline_errors.sql`
- `pg_stat_connections.sql`
- `pg_stat_slow_queries.sql`

### Python files deleted:
- `dashboard/pages/pipeline_health.py`

### `queries.py` entries removed:
All constants referencing the deleted SQL files.

---

## New: Data Health Page

A new `dashboard/pages/data_health.py` page answers operational data quality questions — not infrastructure questions. It is admin-facing and sourced entirely from dbt analytics models.

**Questions it answers:**
- What fraction of tracked inventory has been enriched (detail-scraped) vs. SRP-only?
- How many listings are in exponential cooldown, and what does the attempt distribution look like?
- Is price freshness degrading over time by make/model?
- Which detail scrape batches had poor extraction yield?

**These are all dbt models**, sourced from MinIO silver (Plan 96). They are not live operational queries — they are analytics over the permanent record.

**dbt models to build (when needed, not blocking this plan's cleanup phase):**
- `mart_inventory_coverage` — enrichment rate by make/model, trends over time
- `mart_cooldown_cohorts` — attempt distribution, time-to-eligible, historical trend
- `mart_detail_batch_outcomes` — per-batch extraction yield, unlisted counts, newly-mapped VINs (replaces `recent_detail_runs.sql`)
- `mart_price_freshness_trend` — freshness distribution over time (replaces `price_freshness.sql`)

---

## Dashboard Connection Architecture

After Plan 96 promotes DuckDB to the default dbt target and mart models materialize to a persistent DuckDB file (not `:memory:`), `dashboard/db.py` gains a second connection:

```python
import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/cartracker.duckdb")

@st.cache_resource
def get_duckdb_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)
```

Operational queries (anything hitting live Postgres HOT tables) keep the existing psycopg2 connection. Analytics queries (`analytics.*` mart models) move to the DuckDB connection. This is the dashboard section of what Plan 90 described.

**This connection switch is gated on Plan 96 validation completing and dbt running DuckDB as the default target in production.**

---

## Rollout Sequence

1. **Cleanup phase** (can start now, before Plan 86 or Plan 96 complete):
   - Delete all 21 SQL files
   - Delete `pipeline_health.py`
   - Remove deleted query constants from `queries.py`
   - Remove pipeline health nav entry from `app.py`
   - Verify the three analytics pages still render correctly

2. **Grafana handoff** (requires Plan 86):
   - Confirm Grafana Pipeline Health dashboard covers DAG run outcomes, throughput, HTTP rates, Postgres connections, processing backlog
   - Add Grafana link to dashboard sidebar

3. **Data health page** (requires Plan 96 production validation + dbt model builds):
   - Build new dbt models as data quality questions arise in production
   - Add `data_health.py` page as models are ready
   - Switch `dashboard/db.py` to DuckDB connection for analytics queries

---

## What Stays Unchanged

- `dashboard/pages/deals.py`
- `dashboard/pages/inventory.py`
- `dashboard/pages/market_trends.py`
- `dashboard/db.py` (until step 3 above)
- All dbt models (Plan 96 / Plan 90 handle model changes)
- Layer 2 integration tests
