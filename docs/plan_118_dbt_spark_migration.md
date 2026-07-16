# Plan 118: dbt Migration From DuckDB to Spark-Compatible Execution

## Status

**Superseded / refined by Plan 125.** This document is retained as historical
planning context for the dbt/Spark migration, but the active implementation
track is now [Plan 125: DuckDB to Iceberg Analytics Migration](plan_125_duckdb_to_iceberg_migration.md).

Plan 118 correctly identified the need to move dbt away from DuckDB and toward a
Spark-compatible execution model. Plan 125 narrows and updates that work around
the Plan 112 Iceberg/Lakekeeper proof: the explicit target is now
DuckDB-to-Iceberg migration, with dashboard/ops/Grafana reader cutover and local
Plan 120 snapshot rehearsal as first-class requirements.

## Overlap With Plan 125

Plan 118 and Plan 125 overlap heavily:

| Plan 118 area | Plan 125 replacement |
|---|---|
| Adapter decision | Plan 125 Gate A: Spark/dbt execution spike, using the existing lakehouse runtime and catalog-neutral config. |
| Source redesign | Plan 125 Gate 0/Gate A: move from file-glob sources toward Iceberg/catalog tables. |
| Model compatibility pass | Plan 125 Gates B/C: migrate the adaptive-refresh feature chain and reproduce incremental semantics. |
| Materialization strategy | Plan 125 Gates B/C/E: Iceberg-native feature/mart tables become the analytical contract. |
| dbt runner changes | Plan 125 Gate A/E: runner/runtime work happens only after the smallest Spark/dbt proof. |
| CI strategy | Plan 125 Testing Strategy: local Plan 120 snapshot, CI where feasible, VM proof for heavy Spark paths. |
| Dashboard transition | Plan 125 Gate D: dashboard, `/info`, Prometheus/Grafana, and Loki verification are explicitly covered. |

Use Plan 118 as background when reviewing old dbt/Spark assumptions, but do not
start new implementation from this file. New work should update Plan 125.

## Goal

Move CarTracker's analytics transformation layer away from DuckDB and toward a
Spark-compatible dbt execution model.

This supports the Plan 117 north star:

> Portable open lakehouse now; transferable to Databricks, Snowflake/Iceberg,
> and open-source Spark ecosystems later.

The goal is not to remove dbt. The goal is to keep dbt's modeling, testing,
lineage, and documentation value while changing the execution backend.

---

## Context

Current state:

- dbt runs through `dbt-duckdb`.
- DuckDB reads MinIO Parquet through `httpfs`.
- Mart models materialize into a DuckDB database.
- Dashboard analytics read those DuckDB mart tables.

Future direction:

- Iceberg tables become the analytical table substrate.
- Spark/PySpark becomes the primary write and feature-prep engine.
- dbt should compile/run against a Spark-compatible backend.
- DuckDB should become optional tooling, not the long-term analytics endpoint.

---

## Scope

This plan owns:

- adapter decision
- SQL compatibility audit
- source/model migration strategy
- CI strategy
- dashboard read transition
- dbt-runner changes

This plan does not own:

- standing up MLflow; see Plan 112
- production adaptive refresh; see Plan 113
- broad governance/catalog expansion; see Plan 119
- managed Databricks deployment

---

## Phase 0: Current dbt Inventory

Inventory the existing dbt project before changing adapters.

Deliverables:

- model list by materialization
- source list and physical backing store
- dashboard/API dependencies on each mart
- model runtime and row-count baseline
- dbt tests currently active
- DuckDB-specific SQL usage list

DuckDB-specific patterns to search for:

- `read_parquet`
- `postgres_scan`
- DuckDB JSON functions
- DuckDB regex functions
- `QUALIFY` or window syntax differences
- timestamp/date arithmetic differences
- list/struct syntax
- `generate_series` or sequence helpers
- local file/database assumptions

---

## Phase 1: Adapter Decision

Choose the local dbt execution path.

Candidates:

| Adapter | Why consider it | Risk |
|---------|-----------------|------|
| `dbt-spark` | More directly self-hostable with local Spark/Thrift/JDBC patterns | May diverge from Databricks adapter behavior |
| `dbt-databricks` | Useful if managed Databricks becomes explicit target | May be awkward without managed Databricks |
| Dual-profile approach | Local Spark now, Databricks profile later | More config and CI complexity |

Decision criteria:

- can run locally without managed Databricks
- can read/write Iceberg tables
- compatible with MinIO or local object storage
- can run dbt tests in CI
- minimizes SQL divergence from future Spark/Databricks targets
- keeps model definitions understandable

Deliverable:

- `docs/dbt_spark_adapter_decision.md`

---

## Phase 2: Source Redesign

Move dbt sources away from file-glob thinking and toward table-name thinking.

Current pattern:

```text
dbt model -> DuckDB external source -> MinIO Parquet glob
```

Target pattern:

```text
dbt model -> catalog/schema/table -> Iceberg table storage
```

Initial source candidates:

- `silver_observations`
- `price_observation_events`
- `vin_to_listing_events`
- `blocked_cooldown_events`
- `detail_scrape_claim_events`
- `artifacts_queue_events`

Open question:

Should Postgres HOT tables remain readable through dbt, or should production
operational tables be kept out of the Spark/dbt analytics layer except through
event/table exports?

Default answer:

Keep hot operational claim logic in Postgres. For analytics, prefer exported
history/event tables over live Postgres scans.

---

## Phase 3: Model Compatibility Pass

Port models in layers.

Suggested order:

1. staging sources
2. intermediate state/history models
3. mart models used by dashboard
4. adaptive-refresh feature models
5. any optional/legacy models

For each model:

- compile under the new adapter
- run against a small fixture dataset
- compare row counts to DuckDB baseline
- compare key metrics to DuckDB baseline
- identify semantic differences explicitly

Do not refactor business logic while porting SQL unless required by engine
differences.

---

## Phase 4: Materialization Strategy

Decide how dbt outputs become persisted data.

Candidate materializations:

| Layer | Initial materialization |
|-------|-------------------------|
| staging | view or table depending on Spark cost |
| intermediate | table |
| mart | Iceberg table |
| backtest outputs | Iceberg table or MLflow artifact, depending on size |

Avoid creating a maze of transient views that repeatedly rescan large Iceberg
tables. Spark startup and scan costs are different from DuckDB's local query
costs.

---

## Phase 5: dbt Runner Changes

Update `dbt_runner` to support the selected adapter.

Needed decisions:

- where Spark runs locally
- how dbt connects to Spark
- how MinIO credentials are provided
- how Iceberg dependencies are supplied
- whether dbt_runner remains FastAPI around `dbt build`
- how build locks work with longer Spark jobs
- how docs generation works

The existing lock/intent behavior should be preserved unless there is a clear
reason to replace it.

---

## Phase 6: CI Strategy

CI must prove model correctness without requiring managed Databricks.

Minimum CI path:

1. start Postgres and MinIO test services
2. seed small Iceberg fixtures or create them during setup
3. start local Spark or compatible test backend
4. run `dbt deps`
5. run selected `dbt build`
6. run data tests and model assertions

If full Spark CI is too heavy, define two tiers:

- fast compile/unit checks on every PR
- heavier Spark/dbt integration job on selected branches or manual trigger

---

## Phase 7: Dashboard Transition

Move dashboard analytics off DuckDB materializations.

Options:

| Option | Use when |
|--------|----------|
| dashboard queries Spark/SQL endpoint | local endpoint is stable and responsive |
| dashboard reads exported Postgres serving tables | low-latency dashboard matters more than pure lakehouse reads |
| dashboard reads small materialized extracts | practical bridge during migration |

Default approach:

For large analytical marts, materialize through dbt/Spark and expose a stable
serving surface. Keep operational dashboard widgets on Postgres.

---

## Rollout

1. Complete adapter decision.
2. Build a minimal local Spark/dbt proof using one small Iceberg table.
3. Port staging models.
4. Port one representative mart end to end.
5. Compare row counts and dashboard output to DuckDB baseline.
6. Expand model coverage.
7. Run dual builds for a validation window.
8. Switch dashboard reads.
9. Retire DuckDB dbt target once no production dependency remains.

---

## Testing

- dbt project compiles under the selected adapter.
- source tables resolve by table name, not file glob.
- migrated models match DuckDB baseline on fixture data.
- dbt tests run in CI.
- dbt_runner lock behavior still prevents overlapping builds.
- dashboard queries return expected shapes after read transition.
- rollback to DuckDB target remains possible until final cutover.

---

## Files Changed

| File | Change |
|------|--------|
| `docs/dbt_spark_adapter_decision.md` | New adapter decision record |
| `dbt/profiles.yml` | Add Spark-compatible target |
| `dbt/packages.yml` | Adapter/package updates if needed |
| `dbt/models/sources.yml` | Move sources toward catalog tables |
| `dbt/models/**` | SQL compatibility updates |
| `dbt_runner/` | Runner config/dependency changes |
| `docker-compose.yml` | Local Spark/dbt service wiring if needed |
| `tests/integration/dbt/` | Spark/dbt integration coverage |
| `dashboard/` | Analytics read transition |

---

## Out Of Scope

- Managed Databricks deployment.
- Full governance rollout. See Plan 119.
- Production adaptive-refresh claim filtering. See Plan 113.
- MLflow experiment design. See Plan 112.
- Removing historical DuckDB docs or case-study material.
