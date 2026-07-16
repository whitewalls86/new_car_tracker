# Plan 125: DuckDB to Iceberg Analytics Migration

## Status

**Draft.** This plan is the next major lakehouse step after the paused Plan
112 foundation work.

Plan 112 proved that Iceberg, Lakekeeper, Spark, local snapshots, and MLflow
provenance can work in this repo. It did **not** make Iceberg the analytical
contract. Today, dbt still builds the important analytics tables into
`analytics.duckdb`, and dashboards/scripts still assume DuckDB is the serving
surface.

Plan 125 moves the project from:

```text
normalized Parquet -> dbt-duckdb -> analytics.duckdb -> dashboards/backtests
```

to:

```text
normalized Parquet/Iceberg -> Spark/dbt-compatible execution -> Iceberg tables
        -> dashboards/backtests/MLflow
```

## Goal

Make Iceberg the canonical analytics table layer and reduce DuckDB to, at most,
a local query/cache tool.

The plan should be complete when the core adaptive-refresh feature/mart chain
can be built, validated, and consumed from Iceberg without relying on
`analytics.duckdb` as the source of truth.

## Non-Goals

- No production adaptive-refresh claim integration. Plan 113 owns that.
- No catalog RBAC/governance expansion. Plan 119 owns that.
- No model training claims. Plan 112 resumes model/backtest work after this
  plan supplies stable Iceberg-native inputs.
- No full dashboard redesign unless required to read the new backend.

## Design Principles

1. **Port by chain, not by everything-at-once.**
   Start with the smallest useful feature chain and validate parity.

2. **Keep Plan 120 local snapshots as the repeatable test substrate.**
   Every migration gate should be runnable locally against the A4-style seeded
   fixture, not only on the VM.

3. **Compare before switching readers.**
   DuckDB and Iceberg outputs should run side by side until grain, row count,
   null, freshness, and sampled entity checks pass.

4. **Do not hand-roll a new modeling framework.**
   Prefer `dbt-spark` or a similarly standard dbt-compatible path if it can
   write Iceberg cleanly. Use custom PySpark only where dbt cannot reasonably
   express the operation.

5. **Treat resource limits as product requirements.**
   Spark jobs need bounded memory, explicit one-shot execution, and clear
   cleanup behavior.

## Gate 0: Portability Audit

Audit the current dbt project for DuckDB-specific assumptions.

Deliverables:

- Script or doc table listing every model with:
  - materialization
  - tags (`hourly_core`, `feature_daily`, `backtest`)
  - source dependencies
  - DuckDB-specific SQL/functions
  - incremental strategy
  - candidate migration difficulty
- Recommendation for first migration chain.

Likely first chain:

- `stg_observations`
- `stg_price_events`
- `int_listing_observation_fingerprints`
- `int_listing_observation_runs`
- `int_listing_state_fingerprints`
- `int_listing_state_runs`
- `int_price_history`
- `int_listing_volatility_features`

Open question: whether staging models should read normalized Parquet directly
through Spark or first register normalized Parquet as external Iceberg/metadata
tables.

## Gate A: Spark/dbt Execution Spike

Add the smallest dbt/Spark execution path that can materialize one model into
Iceberg.

Candidate implementation:

- Add a dedicated Spark/dbt profile target.
- Reuse `lakehouse/Dockerfile` rather than creating another runtime image.
- Point the target at Lakekeeper REST catalog + MinIO.
- Materialize one tiny fixture model into an isolated namespace/table.

Success criteria:

- Runs locally against Plan 120 seeded data.
- Runs in CI if runtime is acceptable; otherwise has unit coverage plus a
  documented VM/local smoke.
- Writes an Iceberg table with deterministic name/prefix.
- Captures table metadata: catalog, namespace, table, snapshot ID, row count,
  schema, source snapshot.

## Gate B: First Real Model Chain

Port the first useful adaptive-refresh feature chain to Iceberg.

Recommended target: `int_listing_volatility_features`, because Plan 112 already
proved it can be exported and validated as one row per `vin17`.

Required parity checks against existing DuckDB output:

- row count
- distinct primary key count
- duplicate key count
- null counts for schema-tested fields
- min/max freshness timestamps
- source distribution where relevant
- sampled VIN/listing histories
- hash or checksum comparison for stable subsets where practical

This gate should not switch dashboards yet. It proves correctness.

## Gate C: Incremental Semantics

Recreate the Plan 123 incremental behavior in the Iceberg path.

Models needing special care:

- `int_listing_observation_fingerprints`
- `int_listing_state_fingerprints`
- `int_price_history`
- `int_listing_state_runs`
- `int_listing_observation_runs`
- `mart_scrape_volume`
- `int_latest_observation`

Required checks:

- bootstrap from empty table
- idempotent rerun
- late-arrival lookback pickup
- correction replacement
- affected-entity full-history reread where required
- full-refresh equivalence

Use the shared seeded fixture phases from Plan 123/120 where possible. Avoid
reintroducing tiny throwaway dbt projects as the main coverage path.

## Gate D: Reader Migration

Move consumers off `analytics.duckdb` one by one.

This gate needs to be treated as an application and observability migration,
not just a SQL-reader swap. The current DuckDB file is read by:

- Streamlit dashboard pages through `dashboard/db.py::run_duckdb_query`.
- Dashboard SQL files under `dashboard/sql/`.
- Public `/info` stats through `ops/routers/info.py`.
- Custom Prometheus gauges through `ops/metrics/duckdb_gauges.py`.
- Grafana panels and alerts that consume those custom gauges.
- Plan 112 backtest scripts and local rehearsal/preflight scripts.

The target is not "DuckDB is forbidden"; the target is "DuckDB is no longer
the authoritative build artifact."

### D1: Reader Inventory

Build a concrete inventory before changing code:

| Consumer | Current dependency | Notes |
|---|---|---|
| `dashboard/db.py` | `DUCKDB_PATH`, read-only DuckDB connection | Central Streamlit reader used by dashboard pages. |
| `dashboard/sql/*.sql` | DuckDB SQL over mart/int tables | Uses marts such as `mart_deal_scores`, `mart_vehicle_snapshot`, `mart_scrape_volume`, `mart_block_rate`, `mart_detail_batch_outcomes`, `mart_price_freshness_trend`, `mart_cooldown_cohorts`, `mart_inventory_coverage`, and `int_latest_observation`. |
| `ops/routers/info.py` | direct DuckDB reads | Public portfolio stats from `mart_vehicle_snapshot` and `mart_scrape_volume`; failures are currently soft. |
| `ops/metrics/duckdb_gauges.py` | direct DuckDB reads | Populates Prometheus gauges from mart tables. |
| `grafana/dashboards/pipeline_health.json` | Prometheus gauge names | Depends on custom metrics such as `cartracker_observation_count_last_hour`, `cartracker_artifact_count_last_hour`, `cartracker_block_events_last_hour`, `cartracker_extraction_yield_last_day`, `cartracker_stale_listings_pct`, `cartracker_cooldown_backlog`, and `cartracker_cooldown_permanent`. |
| `grafana/provisioning/alerting/rules.yml` | Prometheus gauge names | Some alerts depend on the custom DuckDB-derived metrics. |
| Loki/Promtail | logs, not analytics tables | Not a reader to migrate, but mandatory for cutover verification. |

Deliverable: a checked-in reader inventory doc or script output that names each
table/query/metric and its proposed Iceberg-era source.

### D2: Choose Dashboard Serving Pattern

Evaluate these options in this order:

1. **Dashboard-serving extracts from Iceberg.**
   A scheduled job reads Iceberg and publishes small dashboard-serving tables or
   files. This is likely the lowest-risk first cut because Streamlit stays
   fast and simple.

2. **DuckDB as a non-authoritative Iceberg reader/cache.**
   Dashboard still uses DuckDB, but the file is rebuilt from Iceberg snapshots
   and is explicitly a cache, not the canonical build output.

3. **Dashboard queries Iceberg through a live query service.**
   Spark Thrift/Trino/other service. This is closer to a warehouse pattern, but
   adds an always-on query service and more operational surface.

Do not switch the dashboard directly to an expensive per-request Spark job.
Dashboard pages need predictable latency and failure behavior.

### D3: Dashboard and `/info` Migration

Port the user-facing readers first in a compatibility layer:

- Add a reader abstraction around `dashboard/db.py` instead of spreading engine
  selection through every page.
- Keep existing dashboard SQL files stable where possible; if SQL dialect must
  diverge, split by backend with a naming convention rather than inline
  conditionals.
- Migrate `/info` stats in `ops/routers/info.py` to the same serving source or
  an equivalent lightweight reader.
- Keep the current soft-failure posture for `/info`: missing analytics should
  omit stats, not break the public page.

Validation:

- Page-level smoke for Deals, Inventory, Market Trends, and Data Health.
- Query-level row/freshness parity against the DuckDB build during the dual-run
  period.
- Latency check for the dashboard pages that load the largest tables.

### D4: Prometheus/Grafana Observability Migration

The custom Prometheus gauges are part of the reader migration because Grafana
pipeline health panels and alerts depend on them.

Current producer:

- `ops/metrics/duckdb_gauges.py`

Current derived metrics:

- `cartracker_observation_count_last_hour`
- `cartracker_artifact_count_last_hour`
- `cartracker_block_events_last_hour`
- `cartracker_extraction_yield_last_day`
- `cartracker_stale_listings_pct`
- `cartracker_cooldown_backlog`
- `cartracker_cooldown_permanent`

Required work:

1. Rename the module or add a parallel implementation so the code no longer
   describes itself as DuckDB-specific once the source changes.
2. Keep metric names stable for the first migration so Grafana dashboards and
   alert rules do not all churn at once.
3. Add a health metric for the analytics reader itself, for example:
   - last successful refresh timestamp
   - source backend (`duckdb_cache`, `iceberg_extract`, etc.)
   - query/update duration
   - failure count
4. Update Grafana dashboards only if the metric meaning changes. Do not change
   dashboard JSON just because the backend changed.
5. Verify Prometheus scrapes continue and Grafana alerts still evaluate after
   the reader switch.

Acceptance checks:

- `curl /metrics` on ops contains all existing custom metric names.
- Grafana `pipeline_health.json` panels still populate.
- Grafana alerting rules referencing the custom gauges evaluate without
  `NoData`/query errors.
- A forced reader failure produces a visible ops log and increments/sets a
  failure signal without crashing the service.

### D5: Loki/Promtail Cutover Verification

Loki and Promtail do not need an Iceberg migration, but they are how we prove
the migration is operationally boring.

During each reader cutover:

- Check dashboard container logs for backend/query errors.
- Check ops logs for reader update failures from the metrics/info path.
- Check dbt/lakehouse-worker logs for failed Iceberg refreshes.
- Confirm no new recurring `Conflicting lock` style DuckDB messages remain
  once DuckDB stops being canonical.

Deliverable: a short VM verification snippet in this plan or a follow-up
runbook section with the exact Grafana/Loki queries used during cutover.

### D6: Dual-Run and Rollback

Do not remove DuckDB readers in the same PR that introduces Iceberg readers.

Dual-run requirements:

- Keep DuckDB and Iceberg-backed serving outputs available for at least one
  release cycle.
- Add a feature flag/env var for dashboard/ops reader backend selection.
- Record source backend in logs and, ideally, in a Prometheus gauge.
- Document rollback as changing the backend flag and restarting only affected
  services, not rebuilding the whole stack.

## Gate E: Cutover and Retirement

After parity and reader migration:

- Stop treating `/data/analytics/analytics.duckdb` as canonical.
- Update Airflow/dbt cadence to build Iceberg-native tables.
- Keep a rollback path for at least one release cycle:
  - last known good DuckDB build
  - last known good Iceberg snapshots
  - documented switchback command
- Update docs/PLANS.md and architecture docs to reflect the new analytical
  contract.

## Testing Strategy

Unit tests:

- SQL/rendering/config checks.
- Compose/image config checks.
- Metadata/prefix safety checks.

Integration tests:

- Local Plan 120 snapshot -> seeded MinIO -> Spark/dbt -> Iceberg chain.
- Existing seeded fixture phases for incremental scenarios.
- Parity comparison between DuckDB and Iceberg for the migration chain.
- Dashboard reader smoke using the selected Gate D backend.
- Ops metrics/info reader smoke proving the custom Prometheus gauges still
  populate from the selected backend.

VM tests:

- Full production-scale chain build.
- Resource/OOM check.
- Runtime comparison vs DuckDB.
- Reader smoke for dashboards/backtest scripts.
- Grafana pipeline-health panels still populate after the reader switch.
- Grafana alert rules evaluate without query errors.
- Loki/Promtail show no recurring dashboard/ops/lakehouse reader failures
  during the dual-run window.

## Risks

- dbt-spark SQL dialect incompatibilities.
- Spark runtime weight on the VM and in CI.
- Iceberg incremental semantics differing from DuckDB delete+insert behavior.
- Dashboard query latency if a good serving path is not chosen.
- Overbuilding catalog/governance before the analytics migration proves useful.

## Exit Criteria

- At least the adaptive-refresh feature chain is Iceberg-native.
- DuckDB and Iceberg parity passes for that chain.
- Local rehearsal can build/read the Iceberg tables from a Plan 120 snapshot.
- VM run passes without OOM.
- Plan 112 can resume Gate C using Iceberg tables as its replay input.
