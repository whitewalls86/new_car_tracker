# Plan 125: DuckDB to Iceberg Analytics Migration

## Status

**Draft.** This plan is the next major lakehouse step after the paused Plan
112 foundation work.

Plan 112 proved that Iceberg, Lakekeeper, Spark, local snapshots, and MLflow
provenance can work in this repo. It did **not** make Iceberg the analytical
contract. Today, dbt still builds the important analytics tables into
`analytics.duckdb`, and dashboards/scripts still assume DuckDB is the serving
surface.

> **Catalog governance decision (read first):** before implementation, settle
> whether to keep Lakekeeper or spike an alternate governed catalog now. See
> [docs/plan_125_catalog_decision_report.md](plan_125_catalog_decision_report.md)
> and **Gate 0.5** below. The current recommendation is to keep Lakekeeper and
> apply the report's catalog-neutral guardrails (R1-R7).

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

## Gate 0.5: Catalog Governance Decision (before implementation)

Before any migration code is written, ratify the catalog posture so we are not
wiring dbt/Spark writers, readers, ops metrics, and MLflow provenance to a
catalog shape we intend to leave.

See the full analysis in
[docs/plan_125_catalog_decision_report.md](plan_125_catalog_decision_report.md).

Decision:

- **Default: keep Lakekeeper through Plan 125.** Do not stand up Unity Catalog
  OSS, Apache Polaris, or Apache Gravitino as part of this plan unless a spike
  first proves the candidate is better for the exact operations Plan 125 needs.
- Re-open the question when Plan 119 (governance) or Plan 113 (policy
  promotion) presents a concrete RBAC / credential-vending / lineage /
  shared-metastore requirement.
- Any alternate catalog is **gated on a write-compatibility spike**: prove native
  Iceberg REST create, append, replace, and time-travel on MinIO/ARM64 before
  committing.
- If a spike is needed, compare candidates in this order: **Apache Polaris**,
  **Apache Gravitino**, then **Unity Catalog OSS**, with Lakekeeper as the
  control. Polaris and Lakekeeper are the most Iceberg-native governance
  candidates; Gravitino has the broader metadata/lineage surface; UC OSS remains
  strategically interesting but must prove self-hosted native-Iceberg write.
- Design service identities now but enforce later: `lakehouse_writer`,
  `dashboard_reader`, `ops_metrics_reader`, `mlflow_provenance_writer`, and
  `ci_local_lakehouse`.

Deliverable (regardless of catalog): adopt the report's **catalog-neutral
guardrails R1-R7** as Plan 125 implementation rules -- single catalog-config
chokepoint, env-driven catalog identity, consumers read a serving layer (not the
catalog directly), static-key `S3FileIO`, rebuildable Iceberg tables until
cutover, isolated/idempotent provisioning, and catalog-agnostic provenance.

Gate 0.5 implementation work before Gate A:

- [x] Introduce neutral consumer-facing `ICEBERG_CATALOG_*` env/config names, with
  temporary fallback to existing Lakekeeper names where needed.
- [x] Keep Lakekeeper-specific payloads and env names inside provisioning code.
- [x] Add tests proving Spark/dbt scripts use the neutral config path.
- [x] Do not run a Polaris/Gravitino/UC OSS spike unless the team explicitly chooses
  to challenge the default Lakekeeper path. **Not run; default stands.**

**Status: implemented.** The catalog decision is unchanged -- Lakekeeper remains
the default through Plan 125, and no alternate catalog spike has been run.

### Catalog config contract (as implemented)

`shared/iceberg_catalog.py` is the single catalog-config chokepoint (R1). It
splits consumer config from provisioning config:

| Env var | Read by | Role |
|---|---|---|
| `ICEBERG_CATALOG_URI` | `catalog_uri()` -> `spark_conf_for_rest_catalog()` | **Preferred.** Iceberg REST endpoint for all consumer (Spark/dbt) code. |
| `LAKEKEEPER_CATALOG_URI` | `catalog_uri()` fallback; `register_lakehouse_warehouse._management_base_uri()` | **Legacy/compat.** Consumers fall back to it when the neutral var is unset; provisioning prefers it. |
| `ICEBERG_WAREHOUSE_NAME` | `WAREHOUSE_NAME` | Already neutral. Warehouse/namespace name. |

Resolution rules:

- **Consumers prefer neutral.** `catalog_uri()` reads `ICEBERG_CATALOG_URI`
  first, falls back to `LAKEKEEPER_CATALOG_URI`, and raises `CatalogConfigError`
  naming both if neither is set. An empty value is treated as unset, so
  `ICEBERG_CATALOG_URI=` in an env file falls through rather than configuring
  Spark with an empty endpoint.
- **Provisioning prefers Lakekeeper-specific.**
  `register_lakehouse_warehouse._management_base_uri()` reads
  `LAKEKEEPER_CATALOG_URI` first and falls back to the neutral resolver. The
  precedence is deliberately the inverse of the consumer path: if consumers are
  ever pointed at another catalog while a Lakekeeper server is still up,
  provisioning must keep addressing Lakekeeper. The `/catalog` -> `/management`
  suffix strip and `warehouse_storage_payload()`'s storage-profile schema are
  Lakekeeper-specific and stay on this side of the line (R6).
- **The `cartracker` catalog alias is not env-driven.** It is baked into every
  `cartracker.<namespace>.<table>` identifier and into captured MLflow
  provenance, so it stays a stable constant across a catalog swap.

Compatibility: `docker-compose.lakehouse.yml`'s `lakehouse-worker` sets both
vars, defaulting to `http://lakekeeper:8181/catalog`:

```yaml
ICEBERG_CATALOG_URI: ${ICEBERG_CATALOG_URI:-${LAKEKEEPER_CATALOG_URI:-http://lakekeeper:8181/catalog}}
LAKEKEEPER_CATALOG_URI: ${LAKEKEEPER_CATALOG_URI:-http://lakekeeper:8181/catalog}
```

The neutral var's fallback is nested rather than a plain default, and that
nesting is load-bearing: compose always populates the container's
`ICEBERG_CATALOG_URI`, so `catalog_uri()`'s runtime fallback can never fire
inside the worker. With a plain default, a shell exporting only
`LAKEKEEPER_CATALOG_URI` at a non-default endpoint would be silently ignored by
consumers, which would still get the baked-in default. The host-side fallback
has to happen at interpolation time.

Resulting behaviour, per host shell:

| Host exports | Consumers get | Provisioning gets |
|---|---|---|
| neither | default | default |
| `LAKEKEEPER_CATALOG_URI` only | the legacy value | the legacy value |
| `ICEBERG_CATALOG_URI` only | the neutral value | default (stays on Lakekeeper) |
| both | the neutral value | the legacy value |

So existing A2/A3/A4 local, CI, and VM flows that export only
`LAKEKEEPER_CATALOG_URI` keep working unchanged.
`tests/integration/lakehouse/test_compose_catalog_interpolation.py` proves this
matrix against real `docker compose config`. Remove the fallback once no
environment sets the legacy name.

A catalog swap then means: repoint `ICEBERG_CATALOG_URI`, and rewrite the
provisioning module. No consumer script changes.

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
