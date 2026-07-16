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

## Relationship To Plan 118

Plan 125 supersedes/refines [Plan 118](plan_118_dbt_spark_migration.md). Plan
118 correctly scoped the move from DuckDB to Spark-compatible dbt execution, but
Plan 125 is the active implementation path because it incorporates the Plan 112
Iceberg/Lakekeeper proof, makes Iceberg the explicit analytical contract, and
adds the dashboard/ops/Grafana reader migration detail needed for cutover.

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
   express the operation. *Settled at Gate A:* `dbt-spark==1.10.3`, `method: session`
   — see [Gate A adapter choice](plan_125_portability_audit.md#gate-a-adapter-choice).
   This principle also rules out forking dbt-spark's incremental internals to
   recreate `delete+insert` unless a measurement proves it necessary.

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

**Status: implemented.** Full findings:
[docs/plan_125_portability_audit.md](plan_125_portability_audit.md) — a
human-readable audit doc, not a script; the checks were one-time reads that
would not pay for a maintained tool.

Deliverables (all covered by the audit doc):

- Doc table listing every model with:
  - materialization
  - tags (`hourly_core`, `feature_daily`, `backtest`)
  - source dependencies
  - DuckDB-specific SQL/functions
  - incremental strategy
  - candidate migration difficulty
- Recommendation for first migration chain.

### Audit outcomes that change the plan

1. **`delete+insert` does not exist on dbt-spark (audit F1) — CONFIRMED at Gate A.**
   Verified against the adapter's own `validate.sql`, which accepts only `append`,
   `merge`, `insert_overwrite`, and `microbatch`. The in-repo comments asserting
   Spark-family support were false and have been corrected
   (`int_listing_state_fingerprints.sql`, `int_price_history.sql`).

   The Gate 0 draft's grouping was also wrong: **two** models are entity-replacement
   (multi-row per key), not four. `int_price_history` and `int_latest_observation`
   emit one row per key (both have `unique` tests), so `merge` is equivalent for
   them. Five of the seven models are a straight `merge` port. Only
   `int_listing_state_runs` and `int_listing_observation_runs` have no equivalent —
   and both are **daily**, feeding a model that already fully rebuilds, so the Gate B
   plan is to materialize them as `table`. Full per-model plan:
   [Incremental strategy decision](plan_125_portability_audit.md#incremental-strategy-decision).
2. **`postgres_scan` against live Postgres (audit F8)** is an architectural
   blocker, not a dialect one. `stg_search_configs` and `int_active_make_models`
   read live HOT tables, and `int_active_make_models` inner-joins into
   `mart_vehicle_snapshot` — so it filters the whole mart layer. Spark needs a
   JDBC read or a snapshot of that reference data in MinIO/Iceberg before the
   serving chain can move. Recommendation: use an hourly snapshot of
   `public.search_configs` and `ops.tracked_models` as the Plan 125 path. They
   are low-change operational reference tables, so a scheduled export to
   MinIO/Iceberg is simpler and safer than making the Spark/dbt build reach back
   into live Postgres. A future streaming pass can improve this by publishing
   change events from the ops routers that mutate these tables and having a
   consumer update the lakehouse copy, with the hourly snapshot retained as a
   reconciliation/repair path.
3. **First chain revised — Gate A should not start on the volatility chain.**
   The chain below remains the right first *useful* chain for **Gate B**, but the
   audit recommends Gate A spike on `stg_blocked_cooldown_events` →
   `mart_block_rate` (two models, one Parquet source, no incrementals, near-zero
   SQL translation), then resolve the incremental-strategy question on
   `mart_scrape_volume` before it entangles with entity-replacement semantics.
   See the audit's "Recommended First Migration Chain".
4. **The reader abstraction is cheaper than feared (Gate D).**
   `dashboard/db.py::run_duckdb_query` is a single chokepoint for all 25 dashboard
   SQL files. But `mart_deal_scores` backs 15 of those 25 files and has no
   `unique` test on `vin`; reader risk concentrates in it and
   `mart_vehicle_snapshot`, while the easiest models to port are also the safest
   to cut over first.

Gate B first chain (unchanged, now with audit difficulty grades — all High except
the two staging views):

- `stg_observations`
- `stg_price_events`
- `int_listing_observation_fingerprints`
- `int_listing_observation_runs`
- `int_listing_state_fingerprints`
- `int_listing_state_runs`
- `int_price_history`
- `int_listing_volatility_features`

Open question (**answered by the audit**): whether staging models should read
normalized Parquet directly through Spark or first register normalized Parquet as
external Iceberg/metadata tables. → **Read Parquet directly through Spark for
Gate A.** External Iceberg registration is a second migration with its own failure
modes, and guardrail R5 (Iceberg tables stay rebuildable, normalized Parquet stays
the recovery point) argues for keeping Parquet a plain input. Revisit at Gate C if
snapshot-consistent reads matter for incremental watermarks.

## Gate A: Spark/dbt Execution Spike

**Status: IMPLEMENTED and PROVEN (2026-07-16).** dbt-spark reads normalized
Parquet from MinIO, computes the `stg_blocked_cooldown_events` →
`mart_block_rate` chain, and materializes it as a real Iceberg table through
Lakekeeper — verified in the catalog, not by dbt's exit code — with **exact**
parity against the DuckDB build of the same seeded snapshot. See
[Gate A results](#gate-a-results-2026-07-16).

Add the smallest dbt/Spark execution path that can materialize one model into
Iceberg.

### Gate A research pass (2026-07-16) — settled before implementation

Three of the blockers are now answered from primary sources; no Gate A model has
been implemented. Full rationale in the audit:
[Gate A adapter choice](plan_125_portability_audit.md#gate-a-adapter-choice),
[Incremental strategy decision](plan_125_portability_audit.md#incremental-strategy-decision),
[Unit-test impact](plan_125_portability_audit.md#unit-test-impact),
[Risks/unknowns remaining](plan_125_portability_audit.md#risksunknowns-remaining).

| Blocker | Status |
|---|---|
| `delete+insert` on dbt-spark (F1) | **Answered.** Does not exist; per-model plan agreed (5× `merge`, 1× `insert_overwrite`, 2× full rebuild). Still **unproven** — Gate A has no incremental model. |
| Concrete adapter choice | **Answered, and now proven.** `dbt-spark==1.10.3` with `method: session`, in the existing `lakehouse-worker` image. Drove a full dbt build against Lakekeeper at Gate A. |
| Fate of the dbt unit tests | **Answered and PROVEN at Gate A.** 64 tests (not 85). dbt-spark unit tests run in session mode — 3/3 `mart_block_rate` tests pass, `+00` literals parse, ~0.3–1.2s each. None exercise the incremental strategy, so the unit-test and F1 workstreams stay independent. |
| `postgres_scan` replacement (F8) | **Still open.** Blocks the serving chain, not Gate A — though it does force a parse-time `POSTGRES_URL` stub even there. Decide before Gate B. |
| Parity tolerance for rounding | **Still open**, but not needed yet: Gate A parity is exact (no rounding in the chain). Needs a stated number before Gate B. |
| Hadoop AWS jars | **Answered at Gate A, opposite to the prediction.** Required for plain Parquet reads (`s3a://`); not involved in Iceberg table I/O. See the correction above. |

**Adapter/runtime pins for Gate A** (do not float; `dbt-spark 1.11.0` shipped
2026-07-16 and has no track record here):

| Component | Pin |
|---|---|
| `dbt-core` | `1.10.20` (matches repo-wide pin) |
| `dbt-spark[session]` | `1.10.3` |
| `pyspark` | `3.5.3` (already pinned) |
| `iceberg-spark-runtime-3.5_2.12` / `iceberg-aws-bundle` | `1.6.1` / `1.6.1` (lockstep, already pinned) |
| `hadoop-aws` / `aws-java-sdk-bundle` | `3.3.4` / `1.12.262` — **required after all; this table's original "none" was wrong.** See the correction below. |

> **Correction (Gate A implementation): Hadoop AWS jars ARE required.** The
> research pass predicted none were needed. That was wrong, and the error was
> conflating two different code paths:
>
> - **Iceberg *table* reads/writes** use Lakekeeper's `s3://` locations, served
>   by Iceberg's native `S3FileIO`. The original claim is correct *for this
>   path*, and it is unchanged — `hadoop-aws` is still not involved.
> - **Plain Parquet reads** of `ops_normalized/` never touch Iceberg at all.
>   Spark resolves the URI through Hadoop's FileSystem API, which has no
>   handler for `s3`/`s3a` unless `hadoop-aws` is on the classpath.
>
> Verified empirically before any model was written: without the jars, reading
> the seeded `blocked_cooldown_events` Parquet fails with
> `UnsupportedFileSystemException: No FileSystem for scheme "s3"` (and
> `ClassNotFoundException: ...s3a.S3AFileSystem` for `s3a://`). No previous
> script caught this because **nothing in this repo had ever asked Spark to read
> Parquet**: Plan 112's A2 wrote synthetic in-memory rows, and A3 read via DuckDB
> and passed a DataFrame to Spark. Gate A is the first Spark-native read.
>
> The two coexist rather than conflict: `hadoop-aws` uses AWS SDK v1
> (`com.amazonaws`), `iceberg-aws-bundle` shades SDK v2
> (`software.amazon.awssdk`). Verified: one session reads `s3a://` Parquet *and*
> resolves the Iceberg REST catalog. `hadoop-aws` **must** match the
> `hadoop-client-*` version pyspark 3.5.3 bundles (3.3.4).
>
> The old rejection of `hadoop-aws` (that it cannot serve Lakekeeper's `s3://`
> table locations) stands and is not being re-litigated. Iceberg keeps
> `S3FileIO`; `hadoop-aws` serves only `s3a://` plain-Parquet input.

Install dbt-spark **only** in the lakehouse image and give CI its own isolated venv
for it — never alongside `dbt-duckdb`/`dbt-postgres` in `dbt_runner`.

Candidate implementation:

- Add a dedicated Spark/dbt profile target (`method: session`, `host: NA`),
  feeding `server_side_parameters` from `shared/iceberg_catalog.py`'s
  `spark_conf_for_rest_catalog()` rather than hand-writing catalog config into
  `profiles.yml` — that keeps the R1/R2 single-chokepoint guarantee intact.
- Also set `spark.sql.defaultCatalog=cartracker` (dbt-spark relations are two-part
  and it has no `catalog:` field — without this, dbt writes to `spark_catalog`,
  i.e. **not Iceberg**, and may still exit 0), `spark.sql.session.timeZone=UTC`
  (Spark has no `TIMESTAMPTZ`), and `file_format: iceberg` (required by `merge`).
- Reuse `lakehouse/Dockerfile` rather than creating another runtime image.
- Point the target at Lakekeeper REST catalog + MinIO.
- Materialize one tiny fixture model into an isolated namespace/table.

Success criteria:

- Runs locally against Plan 120 seeded data.
- Runs in CI if runtime is acceptable; otherwise has unit coverage plus a
  documented VM/local smoke.
- Writes an Iceberg table with deterministic name/prefix — asserted by the table
  appearing in Lakekeeper, **not** by dbt's exit code (see the `defaultCatalog`
  trap above).
- Captures table metadata: catalog, namespace, table, snapshot ID, row count,
  schema, source snapshot.
- **Runs one real dbt unit test** (`mart_block_rate`'s
  `test_block_rate_hourly_grouping`) end-to-end. This is the cheapest available
  proof of session mode, `safe_cast` rendering, `+00` timestamp parsing, session
  timezone, and per-test Spark cost — all in one run. Until this passes, "dbt unit
  tests work on Spark" stays documented-but-unproven.

## Gate A results (2026-07-16)

Every success criterion above is met. What follows is evidence, not intent.

### What was built

| Piece | Where |
|---|---|
| dbt Spark/Iceberg target (`method: session`) | `dbt/profiles.yml` (`spark` output) |
| Spark session config (one chokepoint, R1) | `shared/iceberg_catalog.py::spark_conf_for_dbt_session` |
| Runner + catalog guards | `scripts/run_dbt_spark.py` |
| Adapter-dispatched Parquet source | `dbt/macros/parquet_source.sql` + `meta.spark_external_location` |
| Parity check | `scripts/compare_gate_a_parity.py` |
| Unit tests (no Docker needed) | `tests/lakehouse/test_dbt_spark_session_config.py`, `tests/lakehouse/test_gate_a_parity.py` |

dbt-spark + `hadoop-aws` are installed **only** in `lakehouse/Dockerfile`, never
alongside `dbt-duckdb`/`dbt-postgres` in `dbt_runner`.

### Evidence: it landed in Iceberg, not `spark_catalog`

The headline risk was dbt exiting 0 having written no Iceberg. It didn't:

```text
Spark session ready: defaultCatalog=cartracker timeZone=UTC
1 of 1 OK created sql table model cartracker_experiments.mart_block_rate ... [OK in 2.95s]
Verifying dbt output really landed in the Iceberg catalog:
  verified cartracker.cartracker_experiments.mart_block_rate: rows=23
    provider=iceberg location=s3://bronze/lakehouse_spike/warehouse/019f6c6f-...
```

`spark.sql.defaultCatalog=cartracker` is enforced in three independent places,
because its failure mode is silent:

1. `spark_conf_for_dbt_session()` sets it (the only place it is set).
2. `run_dbt_spark.assert_default_catalog()` refuses to invoke dbt if it is
   anything else — *before* dbt writes anything.
3. After dbt succeeds, `verify_iceberg_tables()` re-reads each table from the
   catalog and asserts `provider=iceberg` and an `s3://` location.

`tests/lakehouse/test_dbt_spark_session_config.py` locks all of this down,
including that `assert_default_catalog` rejects `spark_catalog`.

### Evidence: parity is exact

12/12 checks, 23 rows, zero differences — DuckDB vs Spark/Iceberg from the same
seeded snapshot:

| Check | DuckDB | Spark/Iceberg |
|---|---|---|
| row count | 23 | 23 |
| distinct `hour` / duplicate keys | 23 / 0 | 23 / 0 |
| min `hour` | 2026-06-13 00:00 | 2026-06-13 00:00 |
| max `hour` | 2026-07-12 06:00 | 2026-07-12 06:00 |
| sum(new_blocks) | 26 | 26 |
| sum(block_increments) | 0 | 0 |
| sum(total_block_events) | 26 | 26 |
| sum(unique_listings_blocked) | 26 | 26 |
| sum(max_attempts_seen) | 23 | 23 |
| row-by-row equality | 23 rows identical | |

Matching min/max `hour` is also the practical proof that
`spark.sql.session.timeZone=UTC` is doing its job — an unpinned session zone
would shift every bucket.

**Caveat, stated plainly: `sum(block_increments)` is 0 on both sides**, so the
snapshot does not exercise the `event_type = 'incremented'` branch at all. That
column's parity is currently vacuous. The gap is covered by
`test_block_rate_event_type_split` (a unit test that *does* exercise it, and
passes on Spark), but a snapshot containing `incremented` events would be
strictly better evidence.

### Evidence: dbt unit tests run on dbt-spark

The audit listed this as "documented, **unproven here**". It is now proven —
all 3 `mart_block_rate` unit tests pass in session mode:

```text
1 of 3 PASS mart_block_rate::test_block_rate_event_type_split ... [PASS in 1.23s]
2 of 3 PASS mart_block_rate::test_block_rate_hourly_grouping .... [PASS in 0.39s]
3 of 3 PASS mart_block_rate::test_block_rate_unique_listings .... [PASS in 0.29s]
Done. PASS=4 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=4
```

This single run answers four open questions at once:

- **The `+00` timestamp question is closed.** The audit's biggest single
  unknown — 208 `"YYYY-MM-DD HH:MM:SS+00"` fixture literals, with a silent-NULL
  failure mode — parses correctly on Spark. These fixtures contain exactly that
  form.
- `safe_cast` renders correctly; session mode drives unit tests.
- The session timezone holds through fixture parsing.
- **Per-test cost measured: ~0.3–1.2s** (first test pays warm-up), ~2.9s for 3
  tests plus ~10s JVM startup. Extrapolating, all 64 tests ≈ 30s + startup —
  meaningfully slower than DuckDB's milliseconds, but not the multi-minute job
  the audit feared. The "keep the Spark job narrowly selected" guidance still
  holds, but is less urgent than assumed.

### Deviations from the plan, and why

Three things the research pass got wrong or did not anticipate. All were
verified empirically, not reasoned around:

1. **`hadoop-aws` is required** — see the correction in the pins table above.
2. **`stg_blocked_cooldown_events` must be `ephemeral` on Spark, not `view`.**
   A persisted view stores its body and re-analyzes it against the *view's* own
   catalog on read, which rewrites `parquet.`s3a://...`` into
   `cartracker.parquet.`s3a://...`` — a table lookup that fails with
   `TABLE_OR_VIEW_NOT_FOUND`. Verified the same reference resolves correctly
   inline and in a CTE, which is what `ephemeral` compiles to. Both `view` and
   `ephemeral` mean "no stored data, recomputed on demand", so this matches
   DuckDB's semantics rather than diverging from them. DuckDB keeps `view`.
3. **F8 intrudes on Gate A after all — mildly.** dbt renders *every* source's
   Jinja at parse time regardless of `--select`, so the `postgres_scan` sources
   demand `POSTGRES_URL` even though the Gate A DAG never touches them.
   `run_dbt_spark.stub_parse_only_env()` sets an unroutable dummy rather than
   giving `sources.yml` a default — production DuckDB genuinely requires the var,
   and a default there would turn a loud misconfiguration into a silent one.
   This is a Gate A expedient; the real fix is the F8 decision, due before Gate B.

### Commands to reproduce locally

From a downloaded Plan 120 snapshot. Nothing here needs live production
Postgres or production MinIO.

```bash
# 0. Stack + seeded MinIO from a Plan 120 snapshot. If not already seeded:
python -m scripts.run_local_lakehouse_rehearsal --skip-a2 --skip-a3
#    (or, with an explicit archive:)
#    python -m scripts.run_local_lakehouse_rehearsal \
#      --snapshot-path .cache/lake_snapshots/<id>/snapshot.tar.zst

export MINIO_ROOT_USER=cartracker MINIO_ROOT_PASSWORD=cartracker123
COMPOSE="docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml -p local-lakehouse"

# 1. Build the DuckDB side of the Gate A chain (the parity baseline).
docker build -f dbt/Dockerfile -t cartracker-dbt-local .
docker run --rm --network local-lakehouse_cartracker-net \
  -e DUCKDB_PATH=/out/analytics.duckdb -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ROOT_USER=cartracker -e MINIO_ROOT_PASSWORD=cartracker123 \
  -e MINIO_BUCKET=bronze \
  -e POSTGRES_URL=postgresql://unused:unused@localhost:5432/unused \
  -v "$(pwd)/.cache/analytics:/out" cartracker-dbt-local \
  build --target duckdb --full-refresh \
  --select stg_blocked_cooldown_events mart_block_rate

# 2. Build the same chain into Iceberg via dbt-spark, and verify the catalog.
docker build -f lakehouse/Dockerfile --target lakehouse-worker -t cartracker-lakehouse:latest .
$COMPOSE run --rm lakehouse-worker python -m scripts.run_dbt_spark \
  --verify-table mart_block_rate -- \
  run --select stg_blocked_cooldown_events mart_block_rate

# 3. dbt unit tests on Spark.
$COMPOSE run --rm lakehouse-worker python -m scripts.run_dbt_spark -- \
  test --select "mart_block_rate,test_type:unit"

# 4. Parity: DuckDB vs Spark/Iceberg.
$COMPOSE run --rm lakehouse-worker python -m scripts.compare_gate_a_parity
```

On Windows/Git Bash, prefix the `docker run` in step 1 with `MSYS_NO_PATHCONV=1`
or the `/out` mount path is mangled into a Windows path.

### CI strategy: not yet wired, deliberately

Gate A is **local/VM-verifiable only**; no CI job was added. The honest reason
is that the cost is real and the benefit is currently small: a CI job would need
its own isolated venv (dbt-spark must not share a resolver with dbt-duckdb),
plus JVM startup, plus Lakekeeper + MinIO + a seeded snapshot — several minutes,
to cover two models behind a `--target` nothing in production uses.

What is in CI instead, and runs in the normal fast unit job with neither pyspark
nor a container:

- `tests/lakehouse/test_dbt_spark_session_config.py` — the `defaultCatalog` /
  UTC / s3a config contract, and that the guards actually reject a bad session.
- `tests/lakehouse/test_gate_a_parity.py` — that the comparator genuinely fails
  on row-count, per-hour, duplicate-key, and missing-column differences.

The existing `dbt build + test` job is **untouched and no slower**. Revisit a
narrow, isolated dbt-spark CI job at Gate B, when migrated models start carrying
required unit tests — the measured per-test cost above (~0.4s) makes that look
affordable.

### What Gate A does and does not prove

Proven: dbt-spark session mode drives Lakekeeper + `S3FileIO` through a full dbt
build; Spark reads normalized Parquet directly (`hadoop-aws`, `s3a://`,
Hive-partition discovery); dbt writes a real Iceberg table to the intended
catalog; DuckDB parity is exact; dbt unit tests run on Spark, `+00` literals and
all.

Not proven, and explicitly still open: **any incremental strategy** (Gate A has
none — `merge`/`insert_overwrite` remain unproven, and `mart_scrape_volume` is
still the right canary); the F8 `postgres_scan` replacement; parity at
production scale or on wide/rounding-sensitive models (F5/F10/F12); fingerprint
`md5` parity; and the `session`-mode-is-experimental risk over long runs.

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

Per-model strategy, decided at the Gate A research pass (full rationale:
[Incremental strategy decision](plan_125_portability_audit.md#incremental-strategy-decision)).
None of these are `delete+insert` any more — that strategy does not exist on
dbt-spark:

| Model | Cadence | Iceberg target | Why |
|---|---|---|---|
| `int_listing_state_fingerprints` | daily | `merge` on `artifact_id` | row-unique; in-model dedupe already satisfies Iceberg's MERGE cardinality check |
| `int_listing_observation_fingerprints` | daily | `merge` on `observation_id` | row-unique |
| `int_price_history` | hourly | `merge` on `vin` | one row per vin; merge can't delete a vanished vin, but the event stream is append-only |
| `int_latest_observation` | hourly | `merge` on `vin17` | one row per vin17; same reasoning |
| `mart_scrape_volume` | hourly | `insert_overwrite` by hour/day — **prove first** | window replacement: `merge` would strand a `(hour, source)` row that drops out of the recomputed window |
| `int_listing_state_runs` | **daily** | `table` (full rebuild) | multi-row per `vin17`; no equivalent strategy |
| `int_listing_observation_runs` | **daily** | `table` (full rebuild) | multi-row per `listing_id`; no equivalent strategy |

The two full-rebuild models are daily and already feed a full-rebuild `table`
(`int_listing_volatility_features`), so this loses no freshness — only compute. Do
not fork dbt-spark's incremental materialization to recreate delete+insert until a
VM measurement shows the rebuild is too slow.

**Note a semantic regression that cannot be engineered away:** any Spark
delete+insert equivalent needs two statements, hence two Iceberg commits, where
DuckDB used one transaction. Readers between commits could see an entity
mid-replacement. Guardrails R3/R5 mitigate; Gate D's serving choice must not expose
mid-build state.

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

   **Now demonstrated (2026-07-16), not just documented.** DuckDB can `ATTACH`
   Lakekeeper's REST catalog and read Iceberg tables Spark wrote — no Spark
   session, no Trino, no always-on service. Measured against the real local
   stack on **DuckDB 1.5.4, the version both images already pin** (no upgrade
   needed):

   | Capability | Result |
   |---|---|
   | `ATTACH` Lakekeeper REST catalog | works (`AUTHORIZATION_TYPE 'none'`) |
   | discover/list tables | works |
   | read Iceberg table data | works |
   | **join Iceberg table ⋈ raw Parquet glob in one query** | **works, 0.24s** |
   | warm repeat query | 0.01s |
   | `iceberg_snapshots()` | works |
   | **time travel** (`AT (VERSION => …)`) | **works** |

   Three consequences worth carrying into Gate D:

   - This preserves the ad hoc capability the migration otherwise *loses*:
     querying raw Parquet while joining a dbt-built table, which is exactly
     what `analytics.duckdb` gives today. Option 2 keeps `dashboard/db.py`
     and its embedded-latency profile intact.
   - Time travel through DuckDB is a **net gain** over today — `analytics.duckdb`
     cannot do it at all — and it is the reproducibility primitive the Plan 112
     backtest/MLflow work wants, reachable without a Spark session.
   - **Gotcha, load-bearing:** `CREATE SECRET (TYPE s3, …)` does **not** apply to
     Iceberg data-file fetches — DuckDB fetches Lakekeeper's manifest locations
     unsigned and MinIO returns 403. The legacy `SET s3_access_key_id/...`
     settings work. This is an open upstream bug
     ([duckdb/duckdb#19185](https://github.com/duckdb/duckdb/discussions/19185)):
     the *documented* path is the one that silently fails, so pin this in code
     with a comment or it will be "fixed" back into breakage.

   Unproven: latency at real `mart_deal_scores` scale (tested at 26 rows), and
   `AUTHORIZATION_TYPE 'none'` only holds while Lakekeeper runs `allowall` —
   real OAuth (Plan 119) needs client credentials. DuckDB remains **read-only**
   on Iceberg; it cannot replace Spark as the writer.

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

dbt unit tests — see
[Unit-Test Strategy For Spark/Iceberg Migration](plan_125_portability_audit.md#unit-test-strategy-for-sparkiceberg-migration)
for the full policy. Summary:

- The project has **64** dbt unit tests (2 staging / 31 intermediate / 31 marts)
  covering 17 of 22 models — not the 85 the Gate 0 draft claimed.
- dbt-spark has supported unit tests since 1.8.0 (`spark__safe_cast`). That is
  **documented, not proven here**; do not record unit tests as "covered" on Spark
  until Gate A actually runs one.
- **No unit test exercises an incremental strategy** — every one overrides
  `is_incremental: false`. So the unit-test port and the F1 strategy port are
  independent, and, importantly, **no unit test can catch an incremental
  regression**. That gap must be covered by seeded-fixture and parity tests.
- Migrated models are not required to keep dbt unit tests at Gate A; required at
  Gate B, per chain. DuckDB-side unit tests keep running until that chain's readers
  are cut over at Gate E — during dual-run they are the executable specification
  that adjudicates any Spark/DuckDB disagreement.
- CI shape: a **separate, isolated** dbt-spark job with its own venv, selecting only
  migrated models. Do not add Spark to the existing `dbt build + test` job, and do
  not run all 64 tests through Spark — each pays JVM plus per-test Spark job
  overhead (seconds, vs milliseconds on DuckDB). Existing Layer 1 → Layer 2 ordering
  still applies.

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

- dbt-spark SQL dialect incompatibilities (audit F2–F13).
- Spark runtime weight on the VM and in CI — now also a **test-suite** cost, not
  just a build cost: per-test Spark overhead is why the dbt-spark CI job must stay
  narrowly selected.
- ~~Iceberg incremental semantics differing from DuckDB delete+insert behavior.~~
  **Confirmed and planned** — `delete+insert` does not exist on dbt-spark; see the
  Gate C table above. The residual risk is narrower and concrete: any Spark
  delete+insert equivalent is **two commits, not atomic**.
- `dbt-spark`'s `session` connection method is officially **experimental**, and the
  Gate A path rests on it. Fallback is `thrift` (accepting an always-on service),
  not a different adapter.
- **Silent misconfiguration:** without `spark.sql.defaultCatalog=cartracker`, dbt
  writes to `spark_catalog` and can exit 0 having written no Iceberg. Without
  `spark.sql.session.timeZone=UTC`, every timestamp silently shifts. Verify by
  inspecting Lakekeeper, not exit codes.
- **Fingerprint hash parity** (`md5`/concat/null semantics): low probability, but if
  Spark and DuckDB disagree at all, every run boundary in the volatility chain moves.
  Check on real data at Gate B, not on fixtures.
- Dashboard query latency if a good serving path is not chosen.
- Overbuilding catalog/governance before the analytics migration proves useful.

## Exit Criteria

- At least the adaptive-refresh feature chain is Iceberg-native.
- DuckDB and Iceberg parity passes for that chain.
- Local rehearsal can build/read the Iceberg tables from a Plan 120 snapshot.
- VM run passes without OOM.
- Plan 112 can resume Gate C using Iceberg tables as its replay input.
