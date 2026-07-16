# Plan 125 Portability Audit (Gate 0)

## Status

**Implemented.** This document is the Gate 0 deliverable for
[Plan 125: DuckDB to Iceberg Analytics Migration](plan_125_duckdb_to_iceberg_migration.md).

It is a repo-based audit of the current dbt/DuckDB analytics stack, written to
answer three questions before Gate A starts:

1. What exactly does the dbt project build today, and at what grain?
2. Which SQL will not survive a move off DuckDB?
3. Who reads the output, and what breaks when the build artifact changes?

No dbt behavior is changed here. No script was added: the audit is a one-time
read of 22 models plus their readers, and a checked-in table is more useful to
the next reader than a linter that would need maintaining. If Gate B/C wants
recurring dialect enforcement, that is a better-scoped follow-up (see
[Open Questions](#gate-a-blockers-and-open-questions)).

## How The Stack Is Wired Today

- **Execution target is DuckDB, not Postgres.** `dbt_runner/app.py:164` hardcodes
  `dbt build --target duckdb`. The `dev`/`prod`/`ci` targets in
  [dbt/profiles.yml](../dbt/profiles.yml) are Postgres and are not what production
  runs. Anyone reading `target: prod` at the top of that file will draw the wrong
  conclusion — worth noting because the Postgres targets look like a portability
  asset and are not one. CI's dbt materialization path also uses
  `dbt build --profiles-dir . --target duckdb`; Postgres is still present in CI
  because DuckDB's `postgres_scan(...)` models need a live source database, not
  because CI uses the Postgres dbt target.
- **Sources are file globs and live Postgres reads**, declared via
  `meta.external_location` in [dbt/models/sources.yml](../dbt/models/sources.yml)
  and registered by `register_upstream_external_models()` (dbt_project.yml
  `on-run-start`). Silver/ops sources are `read_parquet(...)` over MinIO;
  `public.search_configs` and `ops.tracked_models` are `postgres_scan(...)` against
  the live operational database.
- **The build artifact is a single file**, `/data/analytics/analytics.duckdb`,
  opened read-only and concurrently by the dashboard and ops. The write-lock
  contention this creates is already visible in the code
  (`ops/metrics/duckdb_gauges.py:129` special-cases `Conflicting lock`, and
  `ops/routers/info.py:29` retries connects three times).
- **Cadence is tag-driven.** `hourly_core` (17 models) and `feature_daily`/
  `backtest` (5 models) per [dbt/selectors.yml](../dbt/selectors.yml); Airflow
  passes `tag:<cadence>` tokens directly.

## Model Inventory

22 models. "Consumers" lists the reader that breaks first if the model is wrong.

### Staging (5)

| Model | Mat. | Tags | Upstream | Incremental | Grain / key tests | Consumers | Difficulty |
|---|---|---|---|---|---|---|---|
| [stg_observations](../dbt/models/staging/stg_observations.sql) | view | `hourly_core` | source `silver.observations` | — | passthrough + `vin17`; no unique test | everything downstream | **Medium** |
| [stg_price_events](../dbt/models/staging/stg_price_events.sql) | view | `hourly_core` | source `ops_events.price_observation_events` | — | `event_id` unique/not_null | int_price_history, volatility | **Low** |
| [stg_blocked_cooldown_events](../dbt/models/staging/stg_blocked_cooldown_events.sql) | view | `hourly_core` | source `ops_events.blocked_cooldown_events` | — | `event_id` unique/not_null | mart_block_rate, mart_cooldown_cohorts | **Low** |
| [stg_dealers](../dbt/models/staging/stg_dealers.sql) | table | `hourly_core` | `int_latest_observation` | — | `customer_id` unique/not_null | mart_deal_scores | **Medium** |
| [stg_search_configs](../dbt/models/staging/stg_search_configs.sql) | view | `hourly_core` | source `public.search_configs` (`postgres_scan`) | — | none | int_active_make_models | **High** |

Note the layering wrinkle: `stg_dealers` refs an *intermediate* model
(`int_latest_observation`), so the staging layer is not a clean source-only tier.
It matters for Gate B chain selection — `stg_dealers` cannot be ported with the
rest of staging.

### Intermediate (9)

| Model | Mat. | Tags | Upstream | Incremental | Grain / key tests | Consumers | Difficulty |
|---|---|---|---|---|---|---|---|
| [int_listing_observation_fingerprints](../dbt/models/intermediate/int_listing_observation_fingerprints.sql) | incremental | `feature_daily`,`backtest` | stg_observations | `delete+insert`, key `observation_id`; fetched_at lookback 3d | `observation_id` unique | int_listing_observation_runs | **Medium** |
| [int_listing_state_fingerprints](../dbt/models/intermediate/int_listing_state_fingerprints.sql) | incremental | `feature_daily`,`backtest` | stg_observations | `delete+insert`, key `artifact_id`; fetched_at lookback 3d | `artifact_id` unique | int_listing_state_runs | **Medium** |
| [int_listing_observation_runs](../dbt/models/intermediate/int_listing_observation_runs.sql) | incremental | `feature_daily`,`backtest` | int_listing_observation_fingerprints | `delete+insert`, key `listing_id` **as entity-replacement key** (multi-row per key); lookback 3d | multiple runs per listing_id — deliberately no unique test | int_listing_volatility_features | **High** |
| [int_listing_state_runs](../dbt/models/intermediate/int_listing_state_runs.sql) | incremental | `feature_daily`,`backtest` | int_listing_state_fingerprints | `delete+insert`, key `vin17` **as entity-replacement key**; lookback 3d | multiple runs per vin17 — no unique test | int_listing_volatility_features | **High** |
| [int_price_history](../dbt/models/intermediate/int_price_history.sql) | incremental | `hourly_core` | stg_price_events | `delete+insert`, key `vin` (affected-VIN full reread); lookback 3d | `vin` unique/not_null | mart_vehicle_snapshot, int_benchmarks | **High** |
| [int_latest_observation](../dbt/models/intermediate/int_latest_observation.sql) | incremental | `hourly_core` | stg_observations | `delete+insert`, key `vin17` (affected-VIN full rerank); lookback 3d | `vin17` unique/not_null | mart_vehicle_snapshot, stg_dealers, mart_inventory_coverage, dashboard | **High** |
| [int_listing_volatility_features](../dbt/models/intermediate/int_listing_volatility_features.sql) | table | `feature_daily`,`backtest` | int_listing_state_runs, int_listing_observation_runs, int_price_history, int_benchmarks, stg_observations, stg_price_events | full rebuild | `vin17` unique | Plan 112 backtests / MLflow | **High** |
| [int_benchmarks](../dbt/models/intermediate/int_benchmarks.sql) | table | `hourly_core` | int_latest_observation, int_price_history | full rebuild | make/model grain | mart_deal_scores, volatility | **Medium** |
| [int_active_make_models](../dbt/models/intermediate/int_active_make_models.sql) | table | `hourly_core` | source `ops.tracked_models` (`postgres_scan`), stg_search_configs | full rebuild | make/model not_null | mart_vehicle_snapshot (inner join — acts as a filter) | **High** |

### Marts (8)

| Model | Mat. | Tags | Upstream | Incremental | Grain / key tests | Consumers | Difficulty |
|---|---|---|---|---|---|---|---|
| [mart_vehicle_snapshot](../dbt/models/marts/mart_vehicle_snapshot.sql) | table | `hourly_core` | int_latest_observation, int_price_history, int_active_make_models, stg_observations | full rebuild | `vin` unique | `/info`, dashboard `mart_freshness`, mart_deal_scores, mart_price_freshness_trend | **High** |
| [mart_deal_scores](../dbt/models/marts/mart_deal_scores.sql) | table | `hourly_core` | mart_vehicle_snapshot, int_benchmarks, stg_dealers | full rebuild | `current_price` tests; vin not unique-tested | **15 of 25 dashboard SQL files** | **Medium** |
| [mart_scrape_volume](../dbt/models/marts/mart_scrape_volume.sql) | incremental | `hourly_core` | stg_observations | `delete+insert`, key `scrape_volume_key` (md5 surrogate for (hour, source)); 72h window replacement | `scrape_volume_key` unique | Prometheus gauges, `/info`, dashboard | **Medium** |
| [mart_block_rate](../dbt/models/marts/mart_block_rate.sql) | table | `hourly_core` | stg_blocked_cooldown_events | full rebuild | `hour` unique/not_null | Prometheus gauge, dashboard | **Low** |
| [mart_cooldown_cohorts](../dbt/models/marts/mart_cooldown_cohorts.sql) | table | `hourly_core` | stg_blocked_cooldown_events | full rebuild | `attempt_bucket` unique/not_null | 2 Prometheus gauges, dashboard | **Low** |
| [mart_detail_batch_outcomes](../dbt/models/marts/mart_detail_batch_outcomes.sql) | table | `hourly_core` | stg_observations | full rebuild | `obs_date` unique/not_null | Prometheus gauge, dashboard | **Low** |
| [mart_inventory_coverage](../dbt/models/marts/mart_inventory_coverage.sql) | table | `hourly_core` | int_latest_observation | full rebuild | make/model grain | dashboard | **Low** |
| [mart_price_freshness_trend](../dbt/models/marts/mart_price_freshness_trend.sql) | table | `hourly_core` | mart_vehicle_snapshot | full rebuild | make/model grain | Prometheus gauge, dashboard | **Medium** |

## DuckDB-Specific Findings

Ordered by how much work each will cost. Spark-equivalence claims below are
**hypotheses to verify in Gate A**, not confirmed adapter behavior — see
[Open Questions](#gate-a-blockers-and-open-questions).

### F1. `delete+insert` is not a dbt-spark strategy (highest risk)

Seven models use `incremental_strategy='delete+insert'`. The model comments state
this choice was made *for* portability (Plan 118): "it's also supported by the
Postgres/Spark-family adapters this project may migrate onto later"
(`int_listing_state_fingerprints.sql:19-21`).

That assumption needs verifying before Gate C. dbt-spark's documented strategies
are `append`, `merge`, and `insert_overwrite`; `delete+insert` is a
dbt-duckdb/dbt-postgres strategy. If it is unavailable, every incremental model
needs a strategy decision, and two distinct semantics must be preserved:

- **Row-unique keys** (`observation_id`, `artifact_id`, `scrape_volume_key`) —
  probably `merge` on the key.
- **Entity-replacement keys** (`int_listing_state_runs.vin17`,
  `int_listing_observation_runs.listing_id`, `int_price_history.vin`,
  `int_latest_observation.vin17`) — these delete **all** rows for a key and
  reinsert a recomputed multi-row history. `merge` on that key is *wrong*: it
  would update matching rows rather than replacing the set, silently leaving
  stale runs behind. These need delete-then-insert semantics, which on Iceberg
  likely means an explicit `DELETE FROM ... WHERE key IN (...)` plus append, or
  `insert_overwrite` with the key as partition — both are real design work.

This is the single biggest Gate C item and the reason the audit recommends
starting the port on a chain that exercises it early rather than late.

### F2. `select * exclude (...)` — DuckDB-only syntax

`int_latest_observation.sql:62`. Spark has no `EXCLUDE` in the select list.
Requires enumerating columns explicitly — mechanical, but it makes the model's
output schema explicit for the first time, which is a schema-drift risk worth
noting (today it inherits whatever `stg_observations` passes through).

### F3. `distinct on (...)` — Postgres/DuckDB-only

`mart_vehicle_snapshot.sql:21`. Needs a `row_number()`-based rewrite. Behavior
depends on the `order by vin17, fetched_at desc` tie-break, so the rewrite must
keep a deterministic tiebreaker or `listing_state` can flap between builds.

### F4. `arg_max` / `arg_min` / `median` — DuckDB aggregate names

- `arg_max`: `stg_dealers.sql:10-11`, `int_price_history.sql:74`,
  `mart_cooldown_cohorts.sql:13`, `int_listing_volatility_features.sql:45-47`
- `arg_min`: `int_price_history.sql:77`
- `median`: `int_listing_volatility_features.sql:95,107`

Spark spells these `max_by`/`min_by` and `percentile(x, 0.5)`. Mostly mechanical,
but `arg_max` null-handling and ties should be checked against parity output
rather than assumed identical.

### F5. `datediff('unit', a, b)` — signature mismatch

11 call sites across 5 models. DuckDB takes a unit string first; Spark's
`datediff(end, start)` is days-only with no unit argument (`months_between` and
manual arithmetic cover the rest). Every `datediff('hour', ...)` site
(`int_listing_state_runs.sql:119,121`, `int_listing_observation_runs.sql:173,175`)
needs a real conversion, and these feed `run_duration_hours` — a model feature.
Rounding/truncation differences here will show up as feature drift, not as errors.

### F6. `count(*) filter (where ...)` — 20+ sites

Standard SQL, supported by DuckDB and Spark 3.x. Called out only to note it is
*not* a portability problem despite looking exotic.

### F7. JSON access — `->>` and `json_extract_string`

`stg_search_configs.sql:14-19`. Both are DuckDB/Postgres idioms; Spark uses
`get_json_object`/`from_json`. Compounded by F8 — this model has a bigger problem.

### F8. `postgres_scan` against live Postgres

`sources.yml:201,220` — `public.search_configs` and `ops.tracked_models`. This is
the most structurally awkward finding: it is not a dialect issue but an
architecture one. Spark has no `postgres_scan`; reaching Postgres means JDBC, and
these are *live HOT operational tables* being read mid-build.

`int_active_make_models` (which depends on both) inner-joins into
`mart_vehicle_snapshot`, so it acts as a **filter on the entire mart layer**. Any
Iceberg-era `mart_vehicle_snapshot` needs this reference data in a form Spark can
read — either a JDBC read, or a snapshot of these tables landed into
MinIO/Iceberg by the existing processing/flush path. Recommend the latter, and
recommend deciding it *before* Gate B rather than during it.

### F9. `regexp_matches` and `!~`

`stg_observations.sql:20` uses `regexp_matches(...)`; the custom test
`dbt/macros/tests/valid_vin.sql:7` uses the Postgres regex operator `!~`. Spark
uses `rlike`/`RLIKE`. Small surface, but `valid_vin` is a *test* macro — tests
break at a different time than models, so this one hides until the port looks
finished.

### F10. `percentile_cont(...) within group (order by ...)`

`int_benchmarks.sql:16-20`, plus two dashboard SQL files. Spark 3.3+ does support
`percentile_cont` with `WITHIN GROUP`; verify on the pinned Spark version, and
verify the `::int` cast rounding matches (DuckDB's cast truncates vs. rounds — a
one-dollar difference on every benchmark row will trip a naive parity check).

### F11. `now()` and the `now_ts()` macro

`dbt/macros/now_ts.sql` is already the seam for this (`{% macro now_ts() %}now(){% endmacro %}`),
used by `mart_vehicle_snapshot`. But `mart_price_freshness_trend.sql` calls bare
`now()` at 6 sites and bypasses the macro. Route those through `now_ts()` so the
Spark port is a one-line macro change. Spark's `now()` exists but timezone
semantics differ; `int_listing_volatility_features` also casts
`'{{ var("as_of_at") }}'::timestamptz`, which needs an explicit Spark equivalent
to keep backtests reproducible.

### F12. Casts (`::int`, `::numeric(5,2)`, `::timestamp`, `::date`)

~20 sites. Spark accepts `cast(x as ...)` but not `::`. `::numeric(5,2)`
(`int_benchmarks.sql:23`) maps to `decimal(5,2)`. Mechanical; the parity risk is
rounding, not syntax.

### F13. `join ... using (a, b)`

`int_listing_volatility_features.sql:71,241,242`. Supported in Spark; noted as
non-blocking.

### F14. Path / file assumptions

- `DUCKDB_PATH` default `/data/analytics/analytics.duckdb` is hardcoded as a
  default in three places: `dashboard/db.py:13`, `ops/metrics/duckdb_gauges.py:10`,
  `ops/routers/info.py:24`.
- Readers query the `main.` schema explicitly (`main.mart_scrape_volume`, etc.)
  in ops code; dashboard SQL uses unqualified names.
- The `httpfs`/`postgres_scanner` extensions and `s3_*` settings in
  `profiles.yml:31-45` are the DuckDB-specific MinIO wiring; the Iceberg path
  replaces this with `spark_conf_for_rest_catalog()`.

### Not found (good news)

No `qualify`, no `generate_series`/sequence helpers, no list/struct syntax, and
no DuckDB-specific window frames anywhere in the model layer. The window
functions in use (`row_number`, `lag`, `lead`, `sum() over (rows between ...)`,
`percent_rank`) are all standard and should port as-is.

## Reader / Consumer Dependencies

This confirms and sharpens the D1 inventory already in the Plan 125 doc.

| Consumer | Reads | Notes |
|---|---|---|
| `dashboard/db.py::run_duckdb_query` | opens `DUCKDB_PATH` read-only per query | Single chokepoint for all 25 dashboard SQL files — the Gate D reader abstraction has exactly one function to wrap. Best news in this audit. |
| `dashboard/sql/*.sql` (25 files) | **`mart_deal_scores` in 15 of 25**; `mart_scrape_volume` ×2; `mart_vehicle_snapshot`, `int_latest_observation`, and one each of the health marts | Deal scores is the dashboard's spine — Deals, Inventory, and Market Trends pages all read it. `inventory_unlisted_over_time.sql` is the only page query touching an `int_` model directly. |
| `ops/routers/info.py` | `main.mart_vehicle_snapshot` (×3 queries), `main.mart_scrape_volume` | Soft-fail per query (`logger.warning`, omit stat). Retries connect 3× at 2s for lock contention. |
| `ops/metrics/duckdb_gauges.py` | `main.mart_scrape_volume`, `mart_block_rate`, `mart_detail_batch_outcomes`, `mart_price_freshness_trend`, `mart_cooldown_cohorts` ×2 | 7 gauges, each in its own try/except. Grafana `pipeline_health.json` and alert rules depend on the metric names. |
| Grafana | the 7 gauge names above | No direct DuckDB dependency — insulated by Prometheus. Metric names must stay stable through cutover. |
| Scripts | 9 under `scripts/` reference duckdb, incl. `export_volatility_features_to_iceberg.py`, `run_local_lakehouse_rehearsal.py`, `preflight_local_lakehouse_snapshot.py` | Plan 112 rehearsal path; already Iceberg-adjacent. |

Two observations that change Gate D sequencing:

1. **`mart_deal_scores` and `mart_vehicle_snapshot` carry nearly all reader
   risk.** The five health marts are read only by gauges and one dashboard page
   each, and they are the *easiest* models to port (all Low). Reader risk and
   port difficulty are inversely correlated — the cheap models to port are also
   the safe ones to cut over first.
2. **`mart_deal_scores` has no `unique` test on `vin`** despite being one row per
   VIN by construction and being the most-read table in the project. Worth adding
   before parity testing, or the parity check has no key to compare on.

## Migration Difficulty By Chain

| Chain | Models | Difficulty | Why |
|---|---|---|---|
| **Cooldown/block health** | stg_blocked_cooldown_events → mart_block_rate, mart_cooldown_cohorts | **Low** | One `read_parquet` source, no incrementals, only `arg_max` + `filter` + `date_trunc`. Feeds 3 gauges. |
| **Detail batch health** | stg_observations → mart_detail_batch_outcomes | **Low** | Full rebuild, `date_trunc('day')::date`, `filter`. |
| **Volatility feature chain** | stg_observations, stg_price_events → the 4 fingerprint/run models → int_price_history, int_benchmarks → int_listing_volatility_features | **High** | Every hard problem at once: 5 incrementals incl. both entity-replacement models, `datediff('hour')` feeding features, `arg_max`, `median`, `percentile_cont`, `as_of_at` casting. |
| **Current-state / serving chain** | int_latest_observation → mart_vehicle_snapshot → mart_deal_scores | **High** | `select * exclude`, `distinct on`, `now_ts`, plus the `postgres_scan` filter dependency via int_active_make_models. Also the highest reader risk. |
| **Postgres reference data** | stg_search_configs, int_active_make_models | **High** | Architectural, not dialect: needs a non-`postgres_scan` path (F8). Blocks the serving chain. |

## Recommended First Migration Chain

**Recommendation: do not start with the volatility chain, despite the Plan 125
draft naming it as the likely first chain.**

The Plan 125 Gate 0 draft proposes `stg_observations` → … →
`int_listing_volatility_features` (8 models) as the likely first chain, on the
Gate B logic that Plan 112 already validated its one-row-per-`vin17` output. That
reasoning holds for **Gate B** (it is the right first *useful* chain). It is the
wrong first chain for **Gate A**, which only needs to prove one model can be
materialized into Iceberg at all.

Proposed sequencing instead:

1. **Gate A spike: `stg_blocked_cooldown_events` → `mart_block_rate`.**
   Two models, one MinIO Parquet source, no incremental logic, no Postgres, and
   the only dialect items are `date_trunc(...)::timestamp` and `count(*) filter`.
   It proves the whole Gate A success list — dbt-spark target, Lakekeeper REST
   catalog via `catalog_uri()`, MinIO `S3FileIO`, deterministic table naming,
   metadata capture — while holding SQL translation near zero, so a failure is
   unambiguously an infrastructure failure. It also has a real consumer
   (`cartracker_block_events_last_hour`), so Gate D can rehearse a reader cutover
   on something whose blast radius is one Grafana panel.

2. **Immediately after Gate A, resolve F1 (incremental strategy) on
   `mart_scrape_volume`.** It is the simplest incremental model (single-column
   md5 surrogate key, contiguous-window replacement) and it will answer the
   `delete+insert` question before that question is entangled with the
   entity-replacement semantics of the run models. Do not discover F1 for the
   first time on `int_listing_state_runs`.

3. **Then Gate B on the volatility chain as planned**, with the entity-replacement
   models (`int_listing_state_runs`, `int_listing_observation_runs`,
   `int_price_history`) as the known-hard core.

4. **Serving chain last**, gated on an F8 decision, because it carries both the
   Postgres dependency and nearly all the reader risk.

Answering the Gate 0 open question in the plan ("should staging models read
normalized Parquet directly through Spark or first register normalized Parquet as
external Iceberg tables?"): **read Parquet directly through Spark for Gate A.**
Registering silver as external Iceberg metadata is a second migration with its own
failure modes, and R5 (Iceberg tables stay rebuildable, Parquet stays the recovery
point) argues for keeping normalized Parquet as a plain input for now. Revisit at
Gate C, where snapshot-consistent reads may start to matter for incremental
watermarks.

## Gate A Blockers And Open Questions

Verification items — these are the claims this audit could not settle from the
repo alone, ordered by how much they'd change the plan:

1. **Does dbt-spark support `delete+insert`, and if not, what reproduces
   entity-replacement semantics on Iceberg?** (F1) Blocks Gate C. Verify against
   the pinned dbt-spark/Iceberg versions before committing to a strategy per
   model. The in-repo comments asserting Spark-family support for `delete+insert`
   should be corrected once this is settled — they currently record an untested
   assumption as fact.
2. **How does Spark reach `search_configs` / `tracked_models`?** (F8) Blocks the
   serving chain. Options: JDBC read from Spark, or land a periodic snapshot to
   MinIO via the existing flush path. Recommend deciding at Gate A time even
   though it is not needed until Gate B.
3. **Which dbt adapter, concretely?** Plan 125 says "prefer dbt-spark or a
   similarly standard dbt-compatible path". Gate A must pick one and confirm it
   writes Iceberg through the REST catalog on ARM64 (the prod VM is ARM64; CI is
   x86_64).
4. **What happens to the 85 dbt unit tests?** `unit_tests.yml` files hold 37
   (intermediate), 40 (marts), and 8 (staging) entries. They render model SQL
   against fixture inputs, so every dialect change in F2–F12 lands on them too,
   and dbt unit-test support varies by adapter. This is unscoped work in the
   current plan.
5. **Retire or demote the Postgres dbt targets.** They are not the production
   build path (`--target duckdb` is hardcoded in dbt_runner), and the main CI dbt
   build also passes `--target duckdb`. They may still be useful as legacy/manual
   compile targets, but they should not be treated as migration scaffolding. One
   concrete cleanup: `dbt_runner`'s `/dbt/docs/generate` endpoint currently runs
   `dbt docs generate` without `--target`, so it inherits `target: prod`; Gate A
   should either pass `--target duckdb` there or make the docs target explicit.
6. **Parity tolerance for rounding.** F5, F10, and F12 all imply the Iceberg
   output may differ from DuckDB by ±1 on cast/round boundaries. Gate B's parity
   checks need a stated tolerance, or "row count matches, checksums don't" will
   be the outcome and will read as failure.

Two small cleanups this audit recommends folding into Gate A rather than tracking
separately, since both make the port measurably cheaper:

- Route `mart_price_freshness_trend`'s six bare `now()` calls through the
  existing `now_ts()` macro (F11).
- Add a `unique` test on `mart_deal_scores.vin` so the most-read table in the
  project has a comparable key before parity testing starts.
