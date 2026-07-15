# Lakehouse Substrate Decision (Plan 112 Gate 0 / Gate A preflight)

## Status

This revision (2026-07-14) supersedes the prior Hadoop/file-catalog-first
pass per `docs/plan_112_gate_a_b_implementation_plan.md`'s locked decisions
(D1/D2). Gate A1 (`docker-compose.lakehouse.yml`, isolated
`lakekeeper-postgres`, CI smoke) has been implemented. **Gate A2 is complete
and verified**, both in the dedicated CI `lakehouse` job and on the
production VM (2026-07-15): `lakehouse-worker`, idempotent server
bootstrap + warehouse registration, and the full PySpark write/append/
time-travel/cleanup round-trip against a fixture-derived table all ran
successfully end to end -- see `docs/runbook_lakehouse.md`'s A2 section for
the commands and the results below for the real output. PyIceberg validation
(A2b) and the real A3 spike (real `int_listing_volatility_features`
snapshot, VM-only) remain outstanding.

Iceberg table writes now exist, but only against the isolated
`lakehouse_spike/` MinIO prefix and a small synthetic fixture table -- no
real production feature table, MLflow, Unity Catalog OSS, or Polaris has
been touched by this document or by A1/A2. Lakekeeper's REST catalog server
+ isolated Postgres metadata store are stood up by A1; the
`cartracker_experiments` warehouse and a `spike_fixture` table now exist
against them via A2 (the table itself is dropped by the spike's own cleanup
step, but the warehouse/namespace registration persists in Lakekeeper's
isolated `lakekeeper_pgdata` volume -- expected, see the runbook).

## Decision summary

- **Table format**: Apache Iceberg, **v2** as the initial compatibility
  target (per `docs/plan_117_storage_and_adaptive_refresh_roadmap.md` — v3
  only if the selected toolchain forces it).
- **Compute**: Spark/PySpark is the primary writer/reader for the first
  spike (Gate A2); PyIceberg is kept only as an optional secondary
  validation client against the same REST catalog (A2b).
- **Catalog**: minimal Lakekeeper REST catalog, deployed via a standalone
  Compose file/project (`docker-compose.lakehouse.yml`, project
  `cartracker-lakehouse`) — never a `profiles:` entry inside, or even the
  same Compose project as, the main `docker-compose.yml` — with its own
  isolated `lakekeeper-postgres` metadata store. One REST catalog
  implementation serves both PySpark (`spark.sql.catalog.*.type=rest`) and
  PyIceberg (`RestCatalog`) with zero per-engine adapter code, and the
  standalone-file/project isolation makes the "never touches production
  Postgres, never reachable by a broad teardown command" guarantee airtight
  rather than merely conventional. Real governance (RBAC, multi-tenant
  namespaces) is deferred to Plan 119.
- **Catalog metadata store**: isolated `lakekeeper-postgres` for Gate A while
  the catalog stack is still experimental. If the spike graduates, consolidate
  onto the main Postgres server as a separate database/user, not as tables in
  the operational `cartracker` database and not as a forever-separate Postgres
  container.
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

## Why a minimal Lakekeeper REST catalog first (revised 2026-07-14)

Gate A's job is to prove the smallest real thing: write an Iceberg table to
MinIO, read it back, snapshot it, time-travel it, and clean it up without
touching production data. A prior pass reasoned that a Hadoop/file catalog
would keep this surface smallest, deferring any catalog service to Plan 119.
That reasoning is superseded: a REST catalog is the correct *minimum* path,
not a step to defer, because it is the one catalog interface both PySpark
and PyIceberg speak without any per-engine adapter code, and Lakekeeper is a
small, purpose-built REST catalog server (not a general application requiring
integration work) rather than a heavyweight governance platform.

Concretely, this is kept minimal by:

- a standalone Compose file/project (`docker-compose.lakehouse.yml`,
  `cartracker-lakehouse`) rather than any change to the main
  `docker-compose.yml` or production Postgres;
- Lakekeeper's own isolated `lakekeeper-postgres` metadata store — no schema,
  user, or connection against the production database;
- no RBAC/multi-tenant/governance configuration in Gate A — a single
  unauthenticated (or single-token) REST endpoint is acceptable for an
  isolated spike. That deeper governance work remains Plan 119 scope, as the
  fallback rule in both Plan 112 (Gate A) and Plan 117 (Stage 1) anticipated
  for if Unity Catalog OSS ever blocked the local workflow.

## Catalog metadata store lifecycle

Lakekeeper's Postgres database is a **control-plane metadata store**, not the
lakehouse data store. MinIO remains the storage layer for Parquet data files
and Iceberg metadata/manifest files; Postgres stores Lakekeeper's durable
catalog state such as warehouses, namespaces, table registration metadata,
storage profiles, and commit coordination state.

Gate A intentionally runs that metadata store as `lakekeeper-postgres`, a
standalone container and volume. This is a safety choice for the spike:

- no production Postgres connection strings, users, schemas, or Flyway
  migrations are needed before the catalog proves useful;
- full lakehouse teardown can delete the experimental catalog metadata volume
  without reaching the production Compose project;
- failures in a pre-1.0 catalog service cannot add load or schema churn to
  the operational database.

If Lakekeeper graduates from spike infrastructure to a durable project
component, the preferred steady-state shape is **one managed Postgres
server/container with separate databases and users per service**, for example:

```text
postgres server/container
|-- cartracker         # operational app data
|-- airflow_metadata   # Airflow scheduler/task metadata
|-- lakekeeper         # Iceberg catalog control-plane metadata
`-- mlflow             # MLflow tracking backend metadata
```

That keeps operational ownership boundaries clearer than putting every
service's metadata into one database with separate schemas, while avoiding the
memory, backup, monitoring, and lifecycle overhead of several permanent
Postgres containers on one VM. The consolidation decision belongs after A2/A3
prove the catalog path and before Plan 119 turns governance into real
operational infrastructure.

## What Plan 119 still owns

Even with the REST catalog now the Gate A baseline (not a later upgrade),
the following remain explicitly deferred to Plan 119, not Gate A/A2/A3:

- RBAC and multi-tenant namespace configuration.
- A concrete governance behavior demonstration (e.g. reader/writer
  separation via a scoped token, table registration rules, ownership
  metadata beyond what the spike's metadata-capture JSON already records).
- Multi-engine table discovery beyond Spark + PyIceberg (e.g. Trino
  resolving a table by name).
- Any decision about whether Lakekeeper's metadata store should ever move.

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

## Proposed MinIO path convention for isolated spike tables

The real bucket is **`bronze`** (`MINIO_BUCKET` throughout
`docker-compose.yml`), not a `cartracker` bucket:

```text
s3://bronze/lakehouse_spike/warehouse/<namespace>/<table_name>/    (Lakekeeper / PyIceberg view)
s3a://bronze/lakehouse_spike/warehouse/<namespace>/<table_name>/   (Spark / Hadoop-AWS view of the same objects)
```

e.g.
`s3a://bronze/lakehouse_spike/warehouse/cartracker_experiments/int_listing_volatility_features/`

The `s3://` vs `s3a://` split is expected, not a mistake: Lakekeeper's Rust
S3 client and PyIceberg's `s3fs` both use `s3://` semantics, while Spark's
Hadoop-AWS connector requires `s3a://` for the same physical MinIO objects.

Rules:

- `lakehouse_spike/` is a dedicated top-level prefix, disjoint from the
  existing `silver/`, `ops_normalized/`, and bronze HTML prefixes documented
  in `docs/implementation_plan_110_storage_layout_hygiene.md`.
- Nothing under `lakehouse_spike/` is read by production dbt, the dashboard,
  or the ops API. It exists solely for Gate A/B experimentation.
- The `cartracker_experiments` namespace is adopted from Plan 119 Phase 1 on
  day one, so spike tables never need renaming when governance lands.
- Lakekeeper's own catalog metadata lives in `lakekeeper-postgres` (see
  `docs/runbook_lakehouse.md`), not in a local/file catalog directory — there
  is no `./lakehouse_spike/catalog/` path to gitignore under this revision.

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
- **No managed Unity Catalog governance.** A minimal self-hosted Lakekeeper
  REST catalog has none of managed Unity Catalog's cross-workspace governance,
  lineage UI, or fine-grained access control. Plan 119 can narrow this gap
  with explicit governance behavior, but OSS/self-hosted catalog tooling still
  will not be equivalent to managed Databricks Unity Catalog.
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

## Gate A spike results

**A2 (fixture-derived table): run and verified, both in CI and on the VM
(2026-07-15).** A3's real `int_listing_volatility_features` snapshot with
before/after production-data cleanup proof is still outstanding -- see the
placeholder further below.

```text
Spike date: 2026-07-15
Run by: docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse
  run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse roundtrip
  (dedicated `lakehouse` CI job, and the OCI A1/Ampere ARM64 production VM)
Spark version: pyspark 3.5.3
Iceberg version: iceberg-spark-runtime-3.5_2.12 1.6.1 + iceberg-aws-bundle 1.6.1 (S3FileIO)
Catalog type: Lakekeeper REST catalog, quay.io/lakekeeper/catalog:v0.13.1
Table: cartracker.cartracker_experiments.spike_fixture (5-VIN synthetic fixture, two batches)
Snapshot IDs (VM run): 6545653745595400329 -> 179396139582586898
Time-travel proof: snapshot 1 = 5 rows, current (snapshot 2) = 10 rows
Cleanup proof: table dropped from the catalog; all 14 underlying MinIO objects
  deleted from the table's actual (Lakekeeper-allocated, UUID-based) location
  under lakehouse_spike/warehouse/ -- verified via boto3 listing before/after,
  not assumed
```

**A3 (real `int_listing_volatility_features` snapshot, VM-only): not yet run.**
This sub-section must be replaced with real output once A3 executes: exact
commands used, row counts, schema, time-travel proof, and before/after
production Parquet row-count/schema-hash cleanup verification.

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
