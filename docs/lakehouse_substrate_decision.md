# Lakehouse Substrate Decision (Plan 112 Gate 0 / Gate A preflight)

## Status

This is the **first-pass** substrate decision written during Gate 0 preflight
(`docs/plan_112_refresh_policy_backtesting.md`). It documents the intended
toolchain and rationale so Gate A can execute the actual spike without
re-litigating these choices. The Gate A section below (commands, cleanup
proof, real snapshot IDs) is a placeholder until the spike runs.

No Spark, PySpark, Iceberg, MLflow, Unity Catalog OSS, Polaris, or Lakekeeper
has been installed or stood up by this document or this PR.

## Decision summary

- **Table format**: Apache Iceberg, **v2** as the initial compatibility
  target (per `docs/plan_117_storage_and_adaptive_refresh_roadmap.md` — v3
  only if the selected toolchain forces it).
- **Compute**: Spark/PySpark for the first spike's writes/reads.
- **Catalog**: First spike: minimal Lakekeeper REST catalog in standalone 
  docker-compose.lakehouse.yml, with isolated lakekeeper-postgres metadata 
  store, PySpark primary write/read, PyIceberg optional validation.
- **Data scope**: first Iceberg tables are isolated copies/subsets of
  existing feature outputs (dbt/DuckDB `int_*` models or normalized Parquet),
  written to dedicated MinIO prefixes. Production Parquet/dbt outputs are
  read-only inputs and must never be mutated by the spike.

## Why Iceberg over Delta for this project now

Carried forward from `docs/plan_117_storage_and_adaptive_refresh_roadmap.md`:

- Iceberg has become the strongest vendor-neutral open table-format story —
  Snowflake, Databricks (via Iceberg access paths), Trino, Spark, and
  catalog vendors are converging on Iceberg interoperability.
- Databricks remains Delta-native, but a local Delta clone would not fully
  satisfy a "managed Databricks experience" requirement anyway if that ever
  becomes a hard gate — so optimizing for a Delta-only story does not buy
  much that Iceberg doesn't already cover, while Iceberg additionally
  transfers to Snowflake and open-engine (Trino/Spark) contexts Delta does
  not cover as cleanly.
- The professional/systems-knowledge value (table formats, catalogs,
  snapshots, schema evolution, reproducible ML workflows) is the durable
  asset, and Iceberg is the more portable vehicle for that knowledge across
  Databricks, Snowflake, and open-source engines.

## Why Hadoop/file catalog first, despite governance ambitions

Gate A's job is to prove the smallest real thing: write an Iceberg table to
MinIO, read it back, snapshot it, time-travel it, and clean it up without
touching production data. A REST catalog, Unity Catalog OSS, Polaris, or
Lakekeeper each add their own service to stand up, configure, and secure
before any of that proof exists.

Starting with a Hadoop/file catalog (or the default local/Spark-bundled
catalog) keeps Gate A's surface area to "Spark + Iceberg only" so the table
format itself can be validated in isolation. Governance/catalog concepts are
real learning goals (Plan 119), but they are a second, separable proof —
bundling them into the first PR risks a stalled spike where it's unclear
whether Iceberg or the catalog service is the source of friction.

This matches the fallback rule already written into both Plan 112 (Gate A)
and Plan 117 (Stage 1): if Unity Catalog OSS blocks the local workflow,
continue with Spark + Iceberg + MLflow on a simpler catalog and defer deeper
catalog work to Plan 119.

## What would trigger moving to REST catalog / Unity Catalog OSS / Polaris / Lakekeeper

Move beyond the file catalog once:

- The Gate A minimum checks pass cleanly (write, read, two snapshots, time
  travel, cleanup proof, CI/local fixture recreation) with the file catalog.
- A concrete governance behavior needs to be demonstrated (e.g.
  reader/writer separation, table registration rules, ownership metadata) —
  i.e. Plan 119 scope becomes active, not before.
- Multi-engine table discovery becomes necessary (e.g. Trino or a second
  Spark session needing to resolve a table by name without out-of-band path
  knowledge).

Until one of those is true, the file catalog is sufficient and adding a
catalog service would be governance theater (explicitly discouraged in
`docs/plan_117_storage_and_adaptive_refresh_roadmap.md`).

## Candidate first tables for the Iceberg spike

Candidates, in order of preference:

1. **`int_listing_volatility_features`** (recommended) — already a single
   materialized `table`, one row per `vin17`, small enough for a fast
   write/read/time-travel loop, and directly relevant to Plan 112's backtest
   feature row. Isolating a copy of this table is the most direct rehearsal
   for Gate C's actual replay-input snapshotting.
2. **`int_listing_state_runs`** — larger (multiple rows per VIN) and
   exercises Iceberg's handling of a table that isn't already a clean
   VIN-per-row table, useful once the trivial case works.
3. **A small fixture subset from Plan 120's CI lake snapshot** — useful if
   the spike needs to run in CI/local without touching the VM's production
   DuckDB file at all.

**Recommendation**: start with `int_listing_volatility_features`. It is the
smallest, most self-contained proof of write → read → snapshot → time-travel
→ cleanup, and it is a real Plan 112 output rather than a synthetic table.

## Proposed MinIO/local path convention for isolated spike tables

```text
s3a://<bucket>/lakehouse_spike/iceberg/<table_name>/
```

e.g. `s3a://cartracker/lakehouse_spike/iceberg/int_listing_volatility_features/`

Rules:

- `lakehouse_spike/` is a dedicated top-level prefix, disjoint from the
  existing `silver/`, `ops/`, and bronze HTML prefixes documented in
  `docs/implementation_plan_110_storage_layout_hygiene.md`.
- Nothing under `lakehouse_spike/` is read by production dbt, the dashboard,
  or the ops API. It exists solely for Gate A/B experimentation.
- Local/file-catalog metadata (if not colocated with the table data) lives
  under a matching local path, e.g. `./lakehouse_spike/catalog/`, kept out of
  the MinIO bucket and out of git (add to `.gitignore` when the spike script
  is written).

## Cleanup/safety rules

- The spike must only write to `lakehouse_spike/*` prefixes — never to
  `silver/`, `ops/`, bronze HTML prefixes, or any path a production dbt
  model or dashboard query reads from.
- The spike reads production feature tables (e.g.
  `int_listing_volatility_features`) **read-only**, either via the existing
  DuckDB `httpfs` connection (`shared/duckdb_s3.py`) or by exporting a static
  Parquet snapshot first. It must never open a write connection to the
  production DuckDB file or production Parquet paths.
- Gate A's deliverables explicitly include "cleanup proof": a documented step
  (and, once written, `scripts/spike_iceberg_lakehouse.py --cleanup` or
  equivalent) that deletes the isolated `lakehouse_spike/` prefix and table
  metadata, followed by a check that production Parquet row counts/schemas
  are unchanged before and after the spike.
- The same isolated table must be reproducible from a Plan 120 CI/local
  fixture snapshot, not only from the VM's live production data, so the
  spike can be re-run and verified without VM access.

## Known gaps vs managed Databricks and Snowflake-managed Iceberg

- **No managed compute autoscaling.** Local/single-node Spark does not
  reflect Databricks' cluster management, job scheduling, or Photon
  execution engine.
- **No managed Unity Catalog governance.** A Hadoop/file catalog has none of
  Unity Catalog's cross-workspace governance, lineage UI, or fine-grained
  access control; a later Unity Catalog OSS or REST catalog spike narrows
  but does not eliminate this gap (OSS Unity Catalog lags managed Databricks
  Unity Catalog in feature completeness and support).
- **No managed table maintenance.** Databricks and Snowflake-managed Iceberg
  handle compaction, vacuum, and clustering automatically; a local spike must
  invoke Iceberg maintenance operations (or Spark procedures) manually and
  will not benchmark them at production scale.
- **No cross-region/cross-account replication or disaster recovery** — this
  is a single-VM, single-bucket MinIO setup.
- **No production-grade concurrency control validation.** Iceberg's
  optimistic concurrency model can be exercised at a toy scale locally, but
  multi-writer contention at production volume is not something this spike
  can validate.
- **Snowflake-managed Iceberg specifically** also handles catalog sync,
  external volume permissions, and automatic clustering that this local
  setup has no equivalent for; the local spike's value is understanding the
  Iceberg table-format mechanics (snapshots, manifests, schema evolution),
  not replicating Snowflake's managed operational layer.

## Gate A spike results (placeholder)

**Not yet run.** This section must be replaced with real output once the
Gate A Iceberg spike executes: exact commands used, table/catalog names,
snapshot IDs, row counts, schema, time-travel proof, and cleanup
verification (before/after production Parquet row counts and schema hashes).

```text
Spike date: <fill in>
Run by: <fill in>
Spark version: <fill in>
Iceberg version: <fill in>
Catalog type: <fill in>
Table: <fill in>
Snapshot IDs: <fill in>
Time-travel proof: <fill in>
Cleanup proof: <fill in>
```
