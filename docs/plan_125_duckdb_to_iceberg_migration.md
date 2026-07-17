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
   recreate `delete+insert` unless a measurement proves it necessary. *Gate C
   later removed even that exception: a vanilla `pre_hook` + `append` design
   dominates the fork — see
   [Gate C shape decisions](#gate-c-shape-decisions-2026-07-17), decision 3.*

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
snapshot-consistent reads matter for incremental watermarks. → **Revisited and
decided at Gate C (2026-07-17): silver is registered metadata-only via
`add_files`, keeping Parquet the recovery point — see
[Gate C shape decisions](#gate-c-shape-decisions-2026-07-17), decision 1.**

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

### Commands to reproduce Gate B locally

Same stack. **Both builds must use the same `--vars as_of_at`**: without it
`int_listing_volatility_features` falls back to `now()`, the two builds run
minutes apart, and every `days_since_*` feature drifts for reasons that have
nothing to do with the engines.

```bash
AS_OF='{"as_of_at": "2026-06-01T00:00:00+00:00"}'

# 0. Rebuild BOTH images after touching any dbt file -- cached layers won't
#    include them. This costs a debugging pass every time it is skipped.
docker build -f dbt/Dockerfile -t cartracker-dbt-local .
docker build -f lakehouse/Dockerfile --target lakehouse-worker -t cartracker-lakehouse:latest .

# 1. DuckDB baseline (the parity reference). Expect PASS=201; the single
#    stg_search_configs error is the F8 postgres_scan model and is expected
#    locally -- there is no local Postgres.
MSYS_NO_PATHCONV=1 docker run --rm --network local-lakehouse_cartracker-net \
  -e DUCKDB_PATH=/out/analytics.duckdb -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ROOT_USER=cartracker -e MINIO_ROOT_PASSWORD=cartracker123 \
  -e MINIO_BUCKET=bronze \
  -e POSTGRES_URL=postgresql://unused:unused@localhost:5432/unused \
  -v "$(pwd -W)/.cache/analytics:/out" cartracker-dbt-local \
  build --target duckdb --full-refresh --vars "$AS_OF"

# 2. Build all ten models into Iceberg, verifying every table via the catalog.
$COMPOSE run --rm lakehouse-worker python -m scripts.run_dbt_spark \
  --verify-table int_price_history --verify-table int_listing_state_fingerprints \
  --verify-table int_listing_observation_fingerprints --verify-table int_listing_state_runs \
  --verify-table int_listing_observation_runs --verify-table int_latest_observation \
  --verify-table int_benchmarks --verify-table int_listing_volatility_features -- \
  run --select +int_listing_volatility_features --vars "$AS_OF"

# 3. dbt unit tests on Spark (expect 38/38).
$COMPOSE run --rm lakehouse-worker python -m scripts.run_dbt_spark -- \
  test --select "intermediate,test_type:unit" "mart_block_rate,test_type:unit" \
               "mart_scrape_volume,test_type:unit"

# 4. Parity (expect 101/101, exact). Needs the MinIO vars: the tie queries read
#    stg_* which are views over Parquet, not tables in the .duckdb file.
MSYS_NO_PATHCONV=1 $COMPOSE run --rm \
  -e MINIO_ROOT_USER=cartracker -e MINIO_ROOT_PASSWORD=cartracker123 \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -v "$(pwd -W)/.cache/analytics:/data/analytics" \
  lakehouse-worker python -m scripts.compare_gate_b_parity

# 5. Re-verify the datediff macros against the committed 830-case corpus.
$COMPOSE run --rm lakehouse-worker python -m scripts.verify_dialect_datediff --check
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

> **Revisited at Gate B, and the answer flipped to yes** (unit tests only, not
> parity): [CI decision](#ci-decision-add-a-narrow-dbt-spark-unit-test-job--the-gate-a-calculus-has-changed).
> The deciding evidence was not cost but F16 — Gate A's own unit-test claim
> silently stopped being true and no job existed to notice.

### What Gate A does and does not prove

Proven: dbt-spark session mode drives Lakekeeper + `S3FileIO` through a full dbt
build; Spark reads normalized Parquet directly (`hadoop-aws`, `s3a://`,
Hive-partition discovery); dbt writes a real Iceberg table to the intended
catalog; DuckDB parity is exact; ~~dbt unit tests run on Spark, `+00` literals and
all.~~

> **CORRECTED at Gate B: the unit-test claim above was invalidated by Gate A
> itself.** Those 3 `mart_block_rate` tests passed while
> `stg_blocked_cooldown_events` was still a `view`. Gate A then switched staging
> to `ephemeral` — correctly — which removed the relation dbt introspects to
> build a dict fixture, and the tests began to ERROR. Nobody re-ran them, so the
> table above kept saying PROVEN for a claim that had stopped being true. They
> pass again as of Gate B, via `format: sql` fixtures. Full mechanism and fix:
> [F16](plan_125_portability_audit.md#f16-dbt-unit-tests-cannot-mock-an-ephemeral-model--found-at-gate-b-disproves-a-gate-a-claim).
> The `+00` literal half of the claim stands.
>
> This is the clearest evidence in the whole plan that a documented PASS decays.
> It is the main argument behind [the Gate B CI decision](#ci-decision-add-a-narrow-dbt-spark-unit-test-job--the-gate-a-calculus-has-changed).

~~Not proven, and explicitly still open: **any incremental strategy** (Gate A has
none — `merge`/`insert_overwrite` remain unproven, and `mart_scrape_volume` is
still the right canary); the F8 `postgres_scan` replacement; parity at
production scale or on wide/rounding-sensitive models (F5/F10/F12); fingerprint
`md5` parity; and the `session`-mode-is-experimental risk over long runs.~~

**Updated after Gate B.** Of that list: `merge` is proven (and
`insert_overwrite` was measured to be the *wrong* choice, not merely unproven);
parity on wide/rounding-sensitive models is proven exactly (F5/F10/F12, including
the 28-field fingerprint); `md5` parity is proven on 16,847 real rows. Still open
and unchanged: the **F8 `postgres_scan` replacement** (blocks the serving chain
only), the **`session`-mode-is-experimental risk over long runs**, and
`insert_overwrite` (now deliberately unused rather than pending). Late-arrival and
correction behaviour under `merge` was briefly open and is now
[closed](#late-arrival-and-correction-under-merge-closed-2026-07-17).

## Gate B: First Real Model Chain

**Status: COMPLETE (2026-07-17).** All ten models build into Iceberg and hit exact
parity with DuckDB on the real snapshot (101/101 checks, 0 differences); 38/38 dbt
unit tests pass on Spark; the `merge` strategy is demonstrated equivalent to
`delete+insert` under late arrival and correction; and a narrow dbt-spark CI job
now guards the dialect macros and unit tests. The one gap this gate opened was
closed before it shipped. See [Gate B progress](#gate-b-progress-2026-07-16).

Port the first useful adaptive-refresh feature chain to Iceberg.

Recommended target: `int_listing_volatility_features`, because Plan 112 already
proved it can be exported and validated as one row per `vin17`.

### Correction: the chain is TEN models, not eight

The eight-model list under "Audit outcomes that change the plan" above is
**incomplete**, verified against the real graph
(`dbt list --select +int_listing_volatility_features --resource-type model`):

- **`int_benchmarks`** is missing — `int_listing_volatility_features` refs it
  directly for `price_vs_make_model_median`. Difficulty **Medium**.
- **`int_latest_observation`** is missing — pulled in *transitively*, because
  `int_benchmarks` refs it. Difficulty **High**, and it brings F2
  (`select * exclude`) into scope, which the audit had assumed was
  serving-chain-only.

There is no viable eight-model subset: the capstone model cannot build without
`int_benchmarks`, which cannot build without `int_latest_observation`.

Crucially, **this does not drag F8 in**: `int_latest_observation` refs only
`stg_observations`. The `postgres_scan` blocker is confined to
`int_active_make_models` / `stg_search_configs`, which stay out of scope. So the
ten-model chain builds without an F8 decision — F8 blocks the *serving* chain,
as the audit said, just not this one.

## Gate B progress (2026-07-16)

Evidence, not intent. What is not listed here is not done.

### Proven

| Claim | Evidence |
|---|---|
| **`merge` works on Iceberg via dbt-spark** — the central Gate B risk | `mart_scrape_volume` built and then re-ran incrementally; Iceberg snapshot history shows `op=overwrite added=1354 deleted=1354`, i.e. a real MERGE, not a rebuild |
| **The merge is idempotent** | rerun → 1,354 rows, 1,354 distinct keys, **0 duplicates** |
| **Exact DuckDB parity on the canary** | 1,354/1,354 rows, **zero** value differences, on 16,847 real observations |
| **md5/fingerprint parity on real data** | 1,354/1,354 md5 surrogate keys matched (see the audit's [dialect measurements](plan_125_portability_audit.md#gate-b-dialect-measurements)) |
| **`stg_observations` builds on Spark** | `rlike`, `ephemeral`, and `s3a://` Hive-partitioned Parquet all work; feeds the canary |
| **DuckDB production path is unaffected** | full `dbt build --target duckdb --full-refresh` → **201/201 PASS**, including all 64 dbt unit tests and the refactored `valid_vin` tests |

### The canary answered the opposite of what was asked

The audit's step 2 said to prove `insert_overwrite` over `merge`, because
"delete+insert removes a disappeared `(hour, source)`; merge would strand it".
**Measured: it doesn't.** dbt-duckdb's delete+insert deletes only keys present in
the incoming batch, so today's production build strands that row too. `merge` is
therefore *exactly* equivalent to current behaviour and `insert_overwrite` would
be a behaviour change. Full detail and the generated SQL:
[the correction](plan_125_portability_audit.md#correction-the-mart_scrape_volume-canary-premise-was-false).

**`mart_scrape_volume`'s Gate C row above is updated accordingly: `merge`, not
`insert_overwrite`.**

### Correction: the seeded snapshot is NOT too small for Gate B

The Gate B data plan assumed the Plan 120 fixture was "tiny (26 rows)" and that a
larger snapshot was needed for runtime signal and hash confidence. That 26-row
figure is `blocked_cooldown_events` — **the Gate A source only**. The already-seeded
local snapshot carries, for the *Gate B* chain:

| Source | Rows | Spread |
|---|---|---|
| `silver_normalized/observations` | **16,847** | 3 sources, 320 listings, 2 months, 1,354 `(hour, source)` buckets |
| `ops_normalized/price_observation_events` | **16,615** | 232 VINs, 1 month |

That is production-shaped and was enough to match 1,354 md5 keys exactly. No new
snapshot download is needed for hash/rounding confidence. A larger snapshot may
still be worth it for the *runtime* question on the two full-rebuild `_runs`
models, but that is a Gate C measurement, not a Gate B blocker.

### All ten models are ported and at exact parity (2026-07-16)

Every model was built into Iceberg via `scripts/run_dbt_spark` and **verified
through the catalog** (`provider=iceberg`, `s3://` location), not via dbt's exit
code. Parity is `scripts/compare_gate_b_parity.py` against a DuckDB build from
the same snapshot and the same `--vars as_of_at`:

| Model | Spark materialization | Rows | Parity |
|---|---|---|---|
| `int_price_history` | `merge` on `vin` | 206 | exact |
| `int_listing_state_fingerprints` | `merge` on `artifact_id` | 2,486 | exact (18-field md5) |
| `int_listing_observation_fingerprints` | `merge` on `observation_id` | 16,847 | exact (28-field md5) |
| `int_listing_state_runs` | `table` (full rebuild) | 529 | exact |
| `int_listing_observation_runs` | `table` (full rebuild) | 1,135 | exact |
| `int_latest_observation` | `merge` on `vin17` | 239 | exact |
| `int_benchmarks` | `table` | 12 | exact |
| `int_listing_volatility_features` | `table` | 214 | exact |

`stg_observations` / `stg_price_events` are ephemeral on Spark — no stored output
to compare, by construction. **Result: 101/101 parity checks passed, 0 differences.**

Also verified along the way:

- **DuckDB is unaffected**: full `dbt build --target duckdb --full-refresh` →
  **PASS=201**, identical to the pre-Gate-B baseline. (The 1 error is
  `stg_search_configs`, the F8 `postgres_scan` model, which needs a real Postgres
  no local run has. Pre-existing and environmental; it accounts for the 40 skips.)
- **Merge idempotency on Iceberg**: a second `run --select
  +int_listing_volatility_features` held every row count and produced 0 duplicate
  keys on all four merge models.
- **dbt unit tests: 38/38 PASS on Spark** (31 intermediate + `mart_block_rate` 3 +
  `mart_scrape_volume` 3), and all 64 still pass on DuckDB.

Before any model was ported, two cast items the audit had filed as "mechanical"
turned out not to be, and one construct the audit never found at all blocked the
build. Both are written up in the audit:
[F12 corrected](plan_125_portability_audit.md#f12-casts-int-numeric52-timestamp-date),
[F15](plan_125_portability_audit.md#f15-lateral-column-alias-in-a-window-order-by--found-at-gate-b-missed-by-the-audit).

### The datediff verification gap is closed — and the old probe could not have caught it

`spark__datediff_hours` was previously verified on **6 hand-picked cases**, whose
only negative case had both endpoints on exact hour boundaries — so it could not
discriminate the truncate-then-diff form from the naive elapsed-time form at all.

Re-verified on an **830-case generated corpus** (both directions, arbitrary
minute/second offsets, adversarial near-boundary, cross-day/month/year, a DST
date, a leap day, large spans), with DuckDB's real answers as expected values:

- `datediff_hours`: **830/830 exact**
- `datediff_days`: **830/830 exact**
- the naive `(unix_timestamp(b) - unix_timestamp(a)) / 3600` translation would
  **miss 392/830** — i.e. the corpus genuinely discriminates.

This matters because these feed `run_duration_hours` and `hours_until_change`,
real model features, where a miss is silent feature drift rather than an error.
`scripts/verify_dialect_datediff.py --check` re-runs it against the committed
corpus (`tests/fixtures/datediff_cases.json`), and **fails loudly if the corpus
ever stops discriminating** — a corpus both forms pass would prove nothing, which
is exactly how the 6-case probe passed while leaving the bug reachable. It renders
the real macro bodies out of `dialect.sql` rather than a hand-copy, so it tests
the macro the models actually compile with.

### Parity tolerance: exact, with ties enumerated separately

**Decision: exact equality on every field. No numeric tolerance.** Reasoning, from
`scripts/compare_gate_b_parity.py`'s docstring:

- Every measured divergence is either exactly reproducible via
  `dbt/macros/dialect.sql` or a genuine tie nondeterminism. There is no third
  "close enough" category for a tolerance to absorb.
- A ±1 tolerance would specifically **hide** the F12 truncation bug that
  `cast_to_int`/`bround` exists to fix — a one-dollar difference on every
  benchmark row. The bug and the tolerance are the same size. Disqualifying.
- Ties differ in kind, not degree, so they are reported and counted but excluded
  from the verdict. Tie keys are computed from the **source** data, never inferred
  from the observed differences — otherwise a real defect on a tied key could
  excuse itself.

The comparator does normalize three *representation* differences, none of which is
a tolerance: tz-aware DuckDB vs naive Spark datetimes (compared as instants —
sound only because the session is pinned to UTC, which the script now **asserts**
as a first-class check rather than assuming), `Decimal` vs `float`, and `bool` vs
0/1.

**Honest caveat: the real snapshot contains ZERO arg_max ties**, so the tie path
never executed during the parity run. It is proven only by unit test
(`tests/lakehouse/test_gate_b_parity.py`, 25 tests), which covers that a tie is
reported not swallowed, that a tie key does not excuse unrelated columns, and
that a tie column on a non-tie key still fails.

### Fixture phases: the required phases already existed

**Correction to the Gate B plan.** It called for extending
`scripts/seed_lake_snapshot_fixture.py` "with crafted phases per migrated model".
Checked against the file: **all seven already exist**, built by Plan 123 —
`observation_fingerprint`, `detail_fingerprint`, `price_history`,
`listing_state_runs`, `scrape_volume`, `latest_observation`, and
`observation_runs`. `int_benchmarks` and `int_listing_volatility_features` are
`table` models with no incremental logic, so they need none. No new phases were
required, and none were invented.

The audit's proposed negative fixture — proving a disappearing `(hour, source)`
row is *removed* from `mart_scrape_volume` — remains **moot**: that premise was
measured false (delete+insert strands it too), so a fixture asserting it would
encode a behaviour neither engine has.

### Late-arrival and correction under `merge`: CLOSED (2026-07-17)

> **This section previously said "OPEN GAP — could not be verified in this
> environment", on the grounds that the seeder needs `psycopg2` (via the
> archiver's writer schemas) and the lakehouse image does not have it. That
> conclusion was wrong, and the reasoning error is worth recording: the seeder
> was only ever tried *inside the lakehouse image*. It does not need to run
> there. It needs **MinIO access**, nothing more — and the host Python (and the
> CI runner, which already `pip install`s `psycopg2-binary`) has every
> dependency it needs. "I couldn't run it in the container" was generalised to
> "it can't be verified here" without checking the obvious alternative.**

The `merge` strategy is now demonstrated equivalent to `delete+insert` under a
late arrival that **reorders history**, using the existing Plan 123 fixture
phases. Procedure — the ordering is the whole point, since a full-refresh at the
end would prove nothing:

1. Seed the `base` phase into MinIO (host Python, `MINIO_ENDPOINT=http://localhost:19000`).
2. `--full-refresh` **both** targets, establishing a common base.
3. Seed `price_history_incremental` (+ `detail_fingerprint_incremental`,
   `latest_observation_incremental`).
4. Build **incrementally** — no `--full-refresh` — on both targets, so DuckDB
   exercises `delete+insert` and Spark exercises `merge`.
5. Compare.

**Result: 101/101 parity checks, exact**, on the grown dataset (`int_price_history`
212 rows, `int_listing_observation_fingerprints` 16,929, `int_latest_observation`
284, `int_listing_volatility_features` 254).

Parity alone would be satisfied by both engines doing nothing, so the scenario
was checked directly to prove it is **not vacuous**. `VIN_PH_AFFECTED`'s base
history is `40000 → 39000`; the phase adds a late `38000` (landing *before*) and
a new `42000`:

| VIN | `total_price_observations` | `price_drop_count` | `price_increase_count` | `current_price` |
|---|---|---|---|---|
| `VIN_PH_AFFECTED` | 4 | **2** | **1** | 42000 |
| `VIN_PH_STABLE` | 1 | 0 | 0 | 15000 |

**Identical on both engines.** `price_drop_count = 2` is the load-bearing number:
the base run produced 1 drop, and no append-only path can yield 2 drops + 1
increase — both engines rewound and recomputed the VIN's *entire* history, which
is exactly what the affected-entity replacement contract promises and what
`merge` was suspected of not doing. `VIN_PH_STABLE` being untouched proves the
lookback filter is real rather than a full rebuild wearing a disguise.

So all five Gate C checks now hold on Spark: bootstrap from empty, idempotent
rerun, **late-arrival lookback pickup**, **correction replacement**, and
full-refresh equivalence.

**Still not automated.** This was run by hand; nothing re-runs it. That is the
same failure mode as F16 (a PASS nobody re-ran), so it should not be treated as
permanently settled. The CI job below deliberately does **not** cover it — see
the scope note there.

### CI decision: add a narrow dbt-spark unit-test job — the Gate A calculus has changed

**Decision: yes, add one — but scoped to unit tests only, not a parity job.**
Gate A said "revisit at Gate B, when migrated models start carrying required unit
tests; the measured per-test cost (~0.4s) makes that look affordable." Three
things have changed, and two of them only became true today:

1. **There is now something worth running.** Gate A had 2 models and 3 unit tests
   behind a target nothing uses. Gate B has 10 models, real incremental logic, and
   38 passing Spark unit tests.
2. **Unit tests on Spark were silently broken for ~a day and nobody noticed**
   ([F16](plan_125_portability_audit.md#f16-dbt-unit-tests-cannot-mock-an-ephemeral-model--found-at-gate-b-disproves-a-gate-a-claim)).
   Gate A recorded them PASSING; Gate A's own later `view`→`ephemeral` switch broke
   them; the doc still said PROVEN. **That is the strongest possible argument for
   this job**: the regression was invisible precisely because nothing re-ran it.
   A green doc table is not a test.
3. **The cost is now measured, not estimated**: 38 tests in **~11s** of Spark work
   plus ~10s JVM startup. Unit tests mock all inputs, so no seeded snapshot and no
   MinIO *data* are needed.

**Implemented (2026-07-17), then reversed the same day — see the amendment
below the scope list.** The initial write-up proposed a new job with "its own
isolated venv". That was judged unnecessary: the existing **`lakehouse`** job
already brought up Lakekeeper + MinIO, built the `lakehouse-worker` image, and
registered the warehouse, so the Gate B steps were added as two
`docker compose run` lines appended to it — no new job, no new stack, no venv.
Running in that image rather than a bare-runner venv also satisfied the
isolation constraint *more* strongly than a venv would have (dbt-spark never
shares a resolver with dbt-duckdb because it is a different image), and it
exercised the real `lakehouse/Dockerfile`, exactly as the A2 steps did. That
reasoning about isolation stands; what did not survive contact with a routine
PR run was the fixture story underneath it — see the amendment.

Two steps were added:

- **`dbt-spark unit tests`** — `test --select "intermediate,test_type:unit"` plus
  `mart_block_rate` and `mart_scrape_volume`. Lakekeeper must be up (the runner
  asserts the default catalog before running), which it already is.
- **`Verify the datediff dialect macros`** — `scripts/verify_dialect_datediff --check`.
  `dialect.sql` tells the reader in comments not to "simplify" `bround`, the
  `filter` on `max_by`, or truncate-then-diff `datediff`. A comment is not
  enforcement; this step is. It renders the real macro bodies out of the file
  (not a hand-copy), checks them against 830 committed DuckDB answers, and fails
  if the corpus ever stops discriminating against the naive form. Pure SQL over
  literals — no MinIO data.

Scope, deliberately narrow:

- **Out: the parity run and the phased fixtures**, including the late-arrival
  verification above. Those need a seeded snapshot *and* a DuckDB build of the
  same chain with a matching `as_of_at` — minutes of runtime and a lot of moving
  infrastructure to guard a target nothing in production reads. They stay
  local/VM-verified until the Spark path is closer to authoritative (Gate D/E).
  **The honest trade: this job catches dialect and compile regressions cheaply and
  would have caught F16; it does not protect the incremental-strategy equivalence
  proven by hand above.** Revisit at Gate C, when that equivalence starts carrying
  weight.
- The existing `dbt build + test` job stays untouched and no slower.

> **Reversed (2026-07-17): the `lakehouse` CI job has been pulled out of CI
> entirely, including the Gate A/A2 smoke steps that predate Gate B.** It is
> not deleted from the repo's capability — it is deliberately absent from
> `.github/workflows/ci.yml` until a real fixture strategy exists for it.
>
> What happened: the job's unit-test step was found broken on a routine PR run
> — `ERROR=13` of its 38 unit tests, on every run, not intermittent. The 13 are
> the dict-format fixtures that mock **materialized** `int_*` models
> (`int_latest_observation`, both fingerprint models, `int_listing_state_runs`).
> Dict fixtures are built by introspecting the mocked input's relation
> (`get_fixture_sql` → `get_columns_in_relation`) — the same mechanism as
> [F16](plan_125_portability_audit.md#f16-dbt-unit-tests-cannot-mock-an-ephemeral-model--found-at-gate-b-disproves-a-gate-a-claim),
> with a different trigger: F16's relation could *never* exist (ephemeral),
> these relations just don't exist *yet* on the CI job's freshly-registered
> catalog, where no model has ever been built. Error signature:
> `Not able to get columns for unit test '<model>' from relation ... because
> the relation doesn't exist`. It never showed locally because local dev
> catalogs already carried the Gate B tables from prior spike work — this is
> exactly the "PASS nobody re-ran" failure mode F16 itself warns about, one
> layer further out.
>
> The first attempted fix (`dbt run --empty --select
> +int_listing_volatility_features` before the unit-test step) traded that
> failure for a worse one: `--empty` still requires Spark to *resolve* each
> source's Parquet path (list the directory, infer a schema) before it can
> limit rows to zero, and the CI job's MinIO is — by design — completely
> unseeded (the unit-test step's whole premise was "no seeded snapshot and no
> MinIO data are needed", since unit tests mock all inputs). Result:
> `PATH_NOT_FOUND: s3a://bronze/silver_normalized/observations`, failing
> *earlier* than the original bug and skipping the unit-test step's signal
> entirely. This was verified against a long-lived local dev stack that
> already had both the target tables *and* seeded source data — neither of
> which CI's fresh stack has — so the local "fix confirmed" result did not
> transfer. The lesson: verifying a CI fix against warm local state proves
> nothing about a job whose entire point is that it starts cold.
>
> **Decision, given the job doesn't gate anything production-relevant** (dbt-
> duckdb — the actual production build — is the separate `dbt` job below, and
> nothing on this branch deploys until an explicit `git pull` on the VM): stop
> patching the symptom and pull the whole job rather than ship a second
> half-fix. The real problem is that this job invents its own ad hoc fixture
> story every time it needs data (Gate A's tiny synthetic round-trip, the
> `--empty`-without-a-seed attempt above) instead of sharing one with the `dbt`
> job's already-working DuckDB-side fixture handling, which solved this exact
> empty-relation-compilation problem already, by seeding schema-correct empty
> Parquet before the build (see that job's "Seed MinIO with empty Parquet
> schemas" step). **Planned direction, not yet built:** fold the Spark path
> into the same Plan 120 fixture the `dbt` job already seeds, so both
> `dbt-duckdb` and `dbt-spark` build off identical data and a parity check
> becomes possible in CI, not just locally. Until that lands, Gate A/B/C's
> Spark-side proofs remain local/VM-verified only, exactly as Gate A originally
> was before the Gate B CI job existed.

Unchanged from Gate A, and still running in the normal fast unit job with neither
pyspark nor a container:
`tests/lakehouse/test_dbt_spark_session_config.py`,
`tests/lakehouse/test_gate_a_parity.py`, and now
`tests/lakehouse/test_gate_b_parity.py` (25 tests).

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

**Status update (Gate B, 2026-07-17): most of this gate is already done.** Every
strategy in the table below is implemented and building on Spark; all eight models
hit exact parity; and all five "required checks" at the bottom of this section now
hold on Spark, including
[late-arrival pickup and correction replacement under `merge`](#late-arrival-and-correction-under-merge-closed-2026-07-17)
— demonstrated with the Plan 123 fixture phases, not argued.

**Status update (Gate C shape decisions, 2026-07-17):** the three "lakehouse
shape" questions this gate owned — how silver is exposed to Iceberg, what write
mode the tables use, and how the two full-rebuild `_runs` models get an
incremental path — are now **decided, with each mechanism proven by direct
testing against the real local Lakekeeper/MinIO/Spark stack**. See
[Gate C shape decisions](#gate-c-shape-decisions-2026-07-17). Nothing from those
decisions is *built* yet; what was proven is the mechanisms.

What Gate C still genuinely owns, updated for those decisions:

- **The runtime measurement on the two full-rebuild `_runs` models.** Now doing
  double duty: it is the build/don't-build call on the two-model decompose
  (decision 3 below), and it gates the partition-spec/MoR reconfiguration of the
  already-shipped Gate B `merge` models (decision 2 below). The bar is unchanged:
  a measurement showing the rebuild is too slow, not a suspicion that it might
  be. Note the fallback has changed: Option C (the adapter fork) is no longer
  what a bad measurement buys — decision 3's vanilla-dbt design is.
- **The dbt-level wiring of decision 3** — the DELETE+INSERT SQL sequence is
  proven, but `pre_hook` + `append` ordering through actual dbt (including the
  first run, where `{{ this }}` does not exist yet) is not.
- **Building decision 1**: the `add_files` sync job co-mingled with
  `compact_silver.py`, the `fs.s3.*` mirror config (audit
  [F17](plan_125_portability_audit.md#f17-add_files-bypasses-s3fileio-and-lakekeepers-location-check-is-scheme-sensitive--found-at-the-gate-c-spike)),
  and the production warehouse registration with a wide key-prefix.
- **Automating the late-arrival verification.** It was run by hand. F16 is the
  standing lesson that an unautomated PASS decays silently — and the
  `lakehouse` CI job's own removal (see the CI decision below) is now a second
  instance of exactly that lesson, one layer out: the job that was supposed to
  guard against silent regressions decayed itself, because it never shared a
  fixture with anything that gets rebuilt regularly. Automating this needs
  that shared fixture (a seeded snapshot + a matching DuckDB build) to exist
  in CI first, which it does not yet.
- Extending the same treatment to any model not in the Gate B ten.

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
| `mart_scrape_volume` | hourly | **`merge` on `scrape_volume_key` — PROVEN at Gate B** | ~~window replacement~~ **The premise was false and was measured: dbt-duckdb's delete+insert does not remove a disappeared `(hour, source)` either, so merge is equivalent to today. `insert_overwrite` would be a behaviour change.** |
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

## Gate C shape decisions (2026-07-17)

Decided by direct testing against the real local Lakekeeper/MinIO/Spark stack
(the same `cartracker-lakekeeper` + `local-lakehouse-minio-1` stack Gate A/B
used), in throwaway spike tables and a throwaway wide-prefix warehouse. What
follows is evidence, not intent; each decision states plainly what remains
unproven. Nothing here is built into the repo yet — the spike scripts were
deliberately not kept.

### 1. Source exposure: `add_files` over a widened warehouse prefix — mechanism PROVEN

Silver stays exactly as `archiver/processors/compact_silver.py` writes it —
plain Parquet, 2-day watermark, per-source/month, incrementally re-mergeable —
and gets registered into Iceberg tables **metadata-only** via Spark's
`system.add_files` procedure. No data copy, no second write path, **zero
changes to the archiver or compaction**. The plan is to co-mingle the
`add_files` sync with the existing compaction cadence rather than adding a
faster separate sync: Plan 112's backtest reproducibility is keyed by Iceberg
snapshot ID per dbt run, not real-time freshness (confirmed against
[docs/plan_112_refresh_policy_backtesting.md](plan_112_refresh_policy_backtesting.md)),
so the 2-day watermark is granularity enough.

This looked blocked at first, and the failure mode is worth recording because
it will be re-encountered by anyone testing against the spike warehouse:

- `add_files` importing a file from OUTSIDE the spike warehouse's
  `lakehouse_spike/` storage-profile prefix failed **at read time** with
  `BadRequestException: Table does not exist or user does not have permission
  to view it at location <path>`. Lakekeeper's remote S3 request signing
  (`S3V4RestSignerClient`) is scoped to a table's own registered storage
  location and rejects reads outside it even with valid static credentials
  configured client-side.
- The same failure occurs with the external file INSIDE the warehouse's prefix
  but outside the specific table's own directory — the signing scope is
  **per-table-location**, not merely per-warehouse-prefix.
- `CREATE TABLE ... LOCATION 's3://...'` is a hard Lakekeeper invariant: a
  location that is not a sub-path of the warehouse's registered prefix fails
  outright with `not a valid sublocation of the storage profile`.

None of that is an architectural obstacle — it is Lakekeeper correctly
refusing to let a warehouse reach outside its own boundary. The Gate A/B spike
warehouse was deliberately scoped narrow (`lakehouse_spike/` isolation, the
right call for a spike), and the earlier "blocker" reported against it was an
artifact of that narrow scope, not of the mechanism. Proven resolution, on a
throwaway wide-prefix warehouse (`spike_wide_prefix`, key-prefix
`spike_wide_root/`), end to end:

1. `CREATE TABLE ... LOCATION 's3://bronze/spike_wide_root/silver_normalized/observations'`
   — succeeds (now a valid sub-path of the storage profile).
2. A plain, Iceberg-unaware Spark write — mimicking exactly what
   `compact_silver.py` does (`df.write.mode('overwrite').parquet(...)`, no
   Iceberg APIs) — lands a file inside that same path.
3. `CALL <catalog>.system.add_files(table => ..., source_table =>
   `` `parquet`.`<path>` ``)` registers the file into the table's manifest
   with **no data copy** — original filename preserved, `records=5` matching
   the source file, no rewrite.
4. A normal `SELECT` against the table returns the correct data.
5. A `DELETE` against rows in the `add_files`-imported file succeeds and (with
   MoR, decision 2) produces a position-delete file rather than a rewrite.

**The one requirement this imposes:** the real (non-spike) warehouse's
storage-profile `key-prefix` must be set, **at registration time**, to a
common ancestor of both `silver_normalized/` (and `ops_normalized/` for the F8
reference tables) and wherever the Iceberg tables themselves live — e.g. the
bucket root or a shared parent directory.
`shared/iceberg_catalog.py::warehouse_storage_payload()` currently hardcodes
the narrow spike prefix; the production registration is where that changes.

Two non-obvious config gotchas hit along the way — `add_files`'s manifest
writer bypassing Iceberg's own `S3FileIO`, and the scheme-sensitivity of the
`LOCATION` sub-path check — are recorded precisely as audit
[F17](plan_125_portability_audit.md#f17-add_files-bypasses-s3fileio-and-lakekeepers-location-check-is-scheme-sensitive--found-at-the-gate-c-spike).
Both will bite again if forgotten.

This also closes Gate 0's deferred question ("read Parquet directly through
Spark for Gate A … revisit at Gate C if snapshot-consistent reads matter"):
silver becomes readable through Iceberg table metadata without becoming a
second write path, and normalized Parquet remains the recovery point (R5) —
the files `add_files` registers are the same files compaction owns.

Boundary notes, so this decision is not over-read:

- **F8 is unaffected.** The `search_configs`/`tracked_models` reference-table
  plan was always a **native** scheduled write (Postgres JDBC read → Iceberg
  write) — there is no pre-existing Parquet file to import for
  Postgres-sourced tables, so no `add_files` and no signing-scope exposure.
  Nothing in this section blocks F8.
- **Plan 126 Gate D is the documented eventual replacement.** Its stated
  "append-only sink from selected topics to MinIO/Iceberg" supersedes the
  batch-era `add_files` sync when it lands. This decision is right for now,
  and the plan already knows what replaces it — it is not a permanent
  architecture commitment.

### 2. Write mode: merge-on-read — PROVEN, including on `add_files`-imported files

Gate C-era tables are **format-version 2, merge-on-read**, set via table
properties:

```sql
TBLPROPERTIES (
  'format-version'='2',
  'write.delete.mode'='merge-on-read',
  'write.update.mode'='merge-on-read',
  'write.merge.mode'='merge-on-read'
)
```

Verified on a table deliberately coalesced to **one multi-row data file**
before the delete — this matters, because a trivial one-row-per-file delete
would prove nothing (Iceberg can drop a whole file for free in that case, and
the rewrite-vs-delta choice never gets exercised). Deleting 2 of 5 rows left
the original 5-record DATA file byte-for-byte untouched and added a 2-record
POS-DELETE file; reads after the delete were correct. Then re-verified
identically against a file registered via `add_files` rather than natively
written — **no behavioural difference between imported and native data
files**.

Why MoR and not copy-on-write: the Gate B canary already measured CoW's
failure shape. `mart_scrape_volume`'s MERGE reported `deleted=1354 added=1354`
for a 72-hour-window change on an unpartitioned CoW table — a small change
rewrote the entire table (the "Gate C signal" the audit flagged). Tolerable at
1,354 rows; not at production scale. CoW is also incompatible with Plan 126
Gate D's stated direction — frequent small writes from a streaming consumer
into a CoW table would mean a whole-table rewrite per micro-batch. MoR is the
shape that does not need redoing when that lands.

**Open, blocked on the runtime measurement (item 4):** whether the
already-shipped Gate B `merge` models (`mart_scrape_volume`,
`int_price_history`, both fingerprint models, `int_latest_observation`) should
be reconfigured to MoR + a partition spec now that this is confirmed, and what
that partition spec should be.

### 3. `_runs` entity replacement: two-model decompose + pre-hook DELETE + append — SQL PROVEN, dbt wiring not yet

The problem is unchanged from F1: `int_listing_state_runs` and
`int_listing_observation_runs` are one row per **run**, many runs per entity
(`vin17`/`listing_id`), and an incremental run must replace an affected
entity's ENTIRE existing run set with a freshly recomputed set of possibly
**different cardinality** — a late-arriving fingerprint can split one run into
two, or merge two into one. `MERGE`'s 1:1 key-based row matching cannot
express that; dbt-spark has no `delete+insert`. Gate B shipped both as
full-rebuild `table`s.

Confirmed design — and it is **not** Option C:

1. **Model A** (new, small, cheap): the distinct set of "affected" entity keys
   in the lookback window. Trivially row-unique, so an ordinary cheap `merge`
   model with no entity-replacement problem of its own.
2. **Model B** (the existing runs-recompute logic, restructured):
   `incremental` + `incremental_strategy='append'`, with a `pre_hook` running
   `DELETE FROM {{ this }} WHERE <entity_key> IN (SELECT <entity_key> FROM
   {{ ref('Model A') }})` before the model's own SELECT — recomputed only for
   the affected entities' full history — gets appended.

The SQL primitive was verified end-to-end via direct Spark SQL against a MoR
table shaped like the real models, including the case that actually matters —
a cardinality-changing incremental run:

```text
first run (empty target):
  INSERT VIN_A×3 runs, VIN_B×1 run  -> 4 single-row DATA files

second run (VIN_A affected, cardinality changes 3 runs -> 2 runs):
  Model A: affected keys = {VIN_A}
  DELETE VIN_A's 3 current rows + INSERT the fresh 2-row recomputed set

result: VIN_A = exactly the new 2-row set (correct — cardinality changed)
        VIN_B = untouched at 1 row (correct — never touched)
files:  6x DATA (all originals + the 2 new, no rewrites) + 1x POS-DELETE (3 records)
```

`MERGE ... WHEN NOT MATCHED BY SOURCE ... THEN DELETE` with a subquery in its
condition was tried first and rejected by Spark
(`UNSUPPORTED_MERGE_CONDITION.SUBQUERY: Subqueries are not allowed`) — but the
identical subquery is legal in a plain `DELETE FROM ... WHERE key IN
(subquery)`. That is *why* this is a two-statement (two Iceberg commit)
design, not a single MERGE. The existing two-commit caveat — a reader between
commits could see a partially-replaced entity — applies unchanged.

Why this is not Option C: Option C is a custom dbt-spark incremental-strategy
adapter fork, deferred behind a measured-necessity bar because it would need
re-verification on every dbt-spark upgrade. This design uses **only vanilla,
built-in, supported dbt features** — a `pre_hook` and the `append` strategy —
and its SQL primitive (DELETE-with-subquery + INSERT) is the same one that
closed the Gate B late-arrival gap
([above](#late-arrival-and-correction-under-merge-closed-2026-07-17)), proven
exact there on real data. Option C is now dominated: even if the runtime
measurement says the full rebuild is too slow, the answer is this design, not
an adapter fork.

**What is genuinely unverified, stated narrowly:** whether dbt's `pre_hook` +
`incremental`/`append` machinery actually sequences DELETE-then-INSERT
correctly against a Spark/Iceberg target — especially on a first run, where
`{{ this }}` does not exist yet. That is ordinary dbt hook-ordering behaviour,
not an Iceberg/Spark capability question, but it has only been tested as raw
`SparkSession.sql()` calls, never through an actual dbt model.

### 4. Still owed: the runtime measurement

Nothing above answers whether decision 2's partition spec for the shipped
merge models needs to change, or whether decision 3's two-model design is
actually cheaper than the current full rebuild at production scale. The
measurement this gate has owed since Gate B has not been run. It blocks
finalizing decision 2's partition spec and the build/don't-build call on
decision 3 — whose mechanism is proven correct either way.

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
