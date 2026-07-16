# Plan 125 Portability Audit (Gate 0)

## Status

**Implemented, plus a Gate A research pass and a Gate A implementation pass
(both 2026-07-16).** This document is the Gate 0 deliverable for
[Plan 125: DuckDB to Iceberg Analytics Migration](plan_125_duckdb_to_iceberg_migration.md).

> **The Gate A implementation corrected two of this document's claims.** Read
> these before trusting anything below:
>
> 1. **"Hadoop AWS jars: none — do not add" was wrong.** They are required for
>    plain `s3a://` Parquet reads (a different code path from Iceberg's
>    `S3FileIO`, which is unaffected). See
>    [the corrected section](#gate-a-adapter-choice).
> 2. **"dbt-spark unit tests: documented, unproven here" is now proven.** 3/3
>    run and pass in session mode; the `+00` timestamp risk is closed. See
>    [Does dbt-spark support unit tests?](#does-dbt-spark-support-unit-tests).
>
> Full evidence, deviations, and reproduction commands:
> [Gate A results](plan_125_duckdb_to_iceberg_migration.md#gate-a-results-2026-07-16).

The Gate A pass resolved three of the Gate 0 open questions from primary sources —
the adapter's own macros and the dbt/Iceberg/Spark docs — without implementing any
Gate A model. It added
[Gate A adapter choice](#gate-a-adapter-choice),
[Incremental strategy decision](#incremental-strategy-decision),
[Unit-Test Strategy For Spark/Iceberg Migration](#unit-test-strategy-for-sparkiceberg-migration),
[Unit-test impact](#unit-test-impact), and
[Risks/unknowns remaining](#risksunknowns-remaining); it confirmed F1, corrected
F1's model grouping, and corrected the unit-test count (64, not 85). Findings marked
**CONFIRMED** are verified against source; everything else is still a hypothesis.

It is a repo-based audit of the current dbt/DuckDB analytics stack, written to
answer three questions before Gate A starts:

1. What exactly does the dbt project build today, and at what grain?
2. Which SQL will not survive a move off DuckDB?
3. Who reads the output, and what breaks when the build artifact changes?

No dbt behavior is changed here. No script was added: the audit is a one-time
read of 22 models plus their readers, and a checked-in table is more useful to
the next reader than a linter that would need maintaining. If Gate B/C wants
recurring dialect enforcement, that is a better-scoped follow-up (see
[Risks/unknowns remaining](#risksunknowns-remaining)).

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

Ordered by how much work each will cost. Except where a finding is explicitly
marked **CONFIRMED**, Spark-equivalence claims below remain **hypotheses to verify**,
not confirmed adapter behavior — see
[Risks/unknowns remaining](#risksunknowns-remaining). F1 has since been verified
against the adapter source; the rest have not.

### F1. `delete+insert` is not a dbt-spark strategy — **CONFIRMED** (highest risk)

**Status: verified at Gate A (2026-07-16). The hypothesis was correct.** See
[Incremental strategy decision](#incremental-strategy-decision) for the resulting
per-model plan.

Seven models use `incremental_strategy='delete+insert'`. The model comments stated
this choice was made *for* portability (Plan 118): "it's also supported by the
Postgres/Spark-family adapters this project may migrate onto later"
(`int_listing_state_fingerprints.sql`). **That claim is false and has now been
corrected in-repo** (`int_listing_state_fingerprints.sql`, `int_price_history.sql`).

dbt-spark accepts exactly four strategies. From the adapter's own validation macro
(`dbt-spark/src/dbt/include/spark/macros/materializations/incremental/validate.sql`):

> `Invalid incremental strategy provided: {{ raw_strategy }} Expected one of:
> 'append', 'merge', 'insert_overwrite', 'microbatch'`

Corroborated three independent ways: the validation macro above, the strategy
implementations in `strategies.sql` (which define only `get_insert_overwrite_sql`,
`get_insert_into_sql`, `spark__get_merge_sql`, and the `dbt_spark_get_incremental_sql`
dispatcher — no delete+insert macro exists), and the published
[Spark configs reference](https://docs.getdbt.com/reference/resource-configs/spark-configs),
which documents only those four and never mentions `delete+insert`.

Caution for future readers: the prose table on
[About incremental strategy](https://docs.getdbt.com/docs/build/incremental-strategy)
is easy to misread as granting dbt-spark `delete+insert`. The adapter source is
authoritative and disagrees. Trust `validate.sql`.

#### The audit's original grouping was wrong on two models

The Gate 0 draft grouped four models as "entity-replacement". Re-checking each
model's schema file shows **only two** actually are:

| Model | `unique_key` | Rows per key | `unique` test? | Is `merge` equivalent? |
|---|---|---|---|---|
| int_listing_observation_fingerprints | `observation_id` | 1 | yes | **Yes** |
| int_listing_state_fingerprints | `artifact_id` | 1 | yes | **Yes** |
| int_price_history | `vin` | **1** | **yes** (`int_price_history.schema.yml`) | **Yes** |
| int_latest_observation | `vin17` | **1** | **yes** (`int_latest_observation.schema.yml`) | **Yes** |
| mart_scrape_volume | `scrape_volume_key` | 1 | yes | **Almost** — see below |
| int_listing_state_runs | `vin17` | **many** | no, explicitly forbidden | **No** |
| int_listing_observation_runs | `listing_id` | **many** | no, explicitly forbidden | **No** |

`int_price_history` and `int_latest_observation` re-read an entity's *entire input
history* to recompute its row, but they still **emit one row per key**. "Recomputed
from full history" is a property of the SELECT, not of the write. `merge` replaces
that single row correctly. Five of the seven models are therefore a straight
`merge` port, not a design problem.

#### Why `merge` fails on the two `_runs` models

`int_listing_state_runs` (multiple runs per `vin17`) and
`int_listing_observation_runs` (multiple runs per `listing_id`) delete **all** rows
for a key and reinsert a recomputed multi-row history. Both schema files explicitly
forbid a `unique` test on the key.

The Gate 0 draft predicted merge would "silently leave stale runs behind". The real
failure is **louder and better**: Iceberg enforces a MERGE cardinality check, and
errors when one target row matches multiple source rows —
[Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/): "only
one record in the source data can update any given row of the target table, or else
an error will be thrown." So `merge` on these two models **fails the build** rather
than corrupting data. That is a meaningful de-risking: this class of mistake cannot
ship silently.

#### There is no clean drop-in replacement, and custom strategies are blocked

dbt's documented escape hatch — define a `get_incremental_delete_insert_sql` macro —
**does not work here**. Per the dbt docs: *"Custom strategies are not currently
supported on the BigQuery and Spark adapters."* This is structural, not an
oversight: `incremental.sql` calls `dbt_spark_get_incremental_sql(strategy, ...)`,
a hardcoded if/elif dispatcher, and never resolves `get_incremental_{strategy}_sql`
dynamically.

A second constraint compounds it: **Spark executes one statement per call.**
`DELETE` + `INSERT` cannot be returned as a single string from a strategy macro the
way DuckDB's delete+insert is. Any true delete+insert on Spark must issue two
statements — meaning two Iceberg commits, so it is **not atomic**. A reader between
the commits sees an entity with rows missing. DuckDB's delete+insert is one
transaction; this is a genuine semantic regression, not a syntax port.

Options for the two `_runs` models, with honest costs:

| Option | Mechanism | Cost / risk |
|---|---|---|
| **A. Full rebuild (`table`)** | Drop the incremental config for these two models | Correct by construction; costs compute. **Recommended for Gate B.** |
| **B. `pre_hook` DELETE + `append`** | Hook deletes affected keys; strategy appends | Uses supported strategy, but the affected-entity predicate is duplicated in the hook, and tmp_relation doesn't exist at pre-hook time. Non-atomic. |
| **C. Override adapter macros / materialization** | Override `dbt_spark_validate_get_incremental_strategy` + `dbt_spark_get_incremental_sql`, or the whole `incremental` materialization | Full control; forks adapter internals and must be re-verified on **every** dbt-spark upgrade. Non-atomic. |
| **D. `insert_overwrite` partitioned by the key** | Dynamic partition overwrite | **Not viable.** `vin17`/`listing_id` are high-cardinality; a partition per VIN is a metadata explosion. |

**Recommendation: Option A for Gate B, revisit C at Gate C with measured evidence.**
The decisive fact is cadence: both `_runs` models are tagged `feature_daily`/
`backtest` — **daily, not hourly** — and their own downstream consumer
`int_listing_volatility_features` is *already* a full-rebuild `table` in the same
chain. Making its inputs full-rebuild costs compute but loses no freshness and is
consistent with what the chain already does. Do not pay for Option C's adapter fork
until a VM run proves the rebuild is actually too slow.

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

Specific recommendation: treat `public.search_configs` and `ops.tracked_models`
as low-change operational reference dimensions and snapshot them hourly to
MinIO/Iceberg for the Plan 125 migration. This keeps the lakehouse build from
depending on live Postgres while preserving enough freshness for dashboards and
adaptive-refresh features. Later, Plan 126 can make this one of the first
streaming jobs: the ops routers that mutate those tables can publish durable
change events, and a consumer can update the lakehouse copy, while the hourly
snapshot remains a reconciliation/repair mechanism.

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

1. **Gate A spike: `stg_blocked_cooldown_events` → `mart_block_rate`. — DONE
   (2026-07-16), and the choice was vindicated.** Exact DuckDB parity; the only
   SQL change needed was `::timestamp` → `cast(... as timestamp)` (`count(*) filter`
   ported untouched, as F6 predicted). Because SQL translation was ~zero, both
   surprises that did surface — the missing `hadoop-aws` filesystem driver and the
   persisted-view catalog-qualification failure — were unambiguously
   infrastructure, diagnosable in minutes. That is precisely the property this
   recommendation was chosen for.

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
   md5 surrogate key, contiguous-window replacement). *Updated by the Gate A pass:*
   the `delete+insert` question is now settled from source (it does not exist on
   dbt-spark), so this model's job is narrower — prove **`insert_overwrite` vs
   `merge`** for window replacement, specifically that a `(hour, source)` combo
   dropping out of the recomputed window is actually removed. Still do not discover
   write-strategy problems for the first time on `int_listing_state_runs`. See
   [Incremental strategy decision](#incremental-strategy-decision).

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

## Gate A adapter choice

**Recommendation: `dbt-spark` with `method: session`, in the existing
`lakehouse/Dockerfile` `lakehouse-worker` image.**

### Why not the alternatives

Only options that can actually write Iceberg through a REST catalog on ARM64 were
considered real:

| Candidate | Verdict |
|---|---|
| **dbt-spark (`session`)** | **Chosen.** In-process PySpark; no extra service. The image already has PySpark 3.5.3 + Iceberg runtime jars + Java 17 building on both arches. |
| dbt-spark (`thrift`) | Rejected. Needs an always-on Spark Thrift Server — contradicts Plan 125 design principle 5 ("explicit one-shot execution") and adds VM footprint for no gain. |
| dbt-trino | Rejected. Trino is Iceberg-native and a genuinely good fit, but it is another always-on coordinator service, and nothing in the repo runs Trino today. Revisit only if Gate D picks the "live query service" serving pattern (D2 option 3), where one engine could serve both. |
| dbt-databricks | Rejected. Not self-hostable; the whole stack is on one OCI VM. |
| dbt-duckdb + Iceberg | Rejected. DuckDB's Iceberg support is **read-oriented**; it cannot be the writer this plan needs. This is the thing Plan 125 exists to move off. |
| Hand-rolled PySpark | Rejected by design principle 4, and it would strand all 64 unit tests and every `ref()`/lineage/docs affordance. |

`session` carries one honest caveat: dbt labels it **experimental**. It is the
right call anyway — it is the only option that keeps the one-shot worker shape —
but Gate A should treat "session mode works against Lakekeeper" as a thing to
*prove*, not assume, and the pin should not float.

### Pinned versions

Match the existing `1.10.x` line rather than jumping to `dbt-spark 1.11.0`, which
released 2026-07-16 (the day of this audit) and has no track record here.

| Component | Pin | Rationale |
|---|---|---|
| `dbt-core` | `1.10.20` | Already pinned repo-wide (`dbt/Dockerfile`, `dbt_runner/Dockerfile`, CI). Do not diverge. |
| `dbt-spark[session]` | `1.10.3` | Latest `1.10.x`; requires `dbt-core>=1.8.0rc1,<2.0` → compatible. `[session]` extra pulls the PySpark integration. |
| `pyspark` | `3.5.3` | Already pinned in `lakehouse/requirements.txt`; matches the `3.5_2.12` Iceberg runtime artifact. |
| `iceberg-spark-runtime-3.5_2.12` | `1.6.1` | Already pinned via `ICEBERG_SPARK_RUNTIME_VERSION` in `lakehouse/Dockerfile`. |
| `iceberg-aws-bundle` | `1.6.1` | Already pinned; **keep in lockstep** with the runtime jar. |
| `hadoop-aws` / `aws-java-sdk-bundle` | `3.3.4` / `1.12.262` | **Corrected at Gate A — the original "none — do not add" was wrong.** Required for plain `s3a://` Parquet reads. See below. |
| Java | 17 (`openjdk-17-jre-headless`, `-bookworm`) | Already resolved dynamically per-arch in the Dockerfile. |

**On Hadoop AWS jars — this section was WRONG, and Gate A corrected it.**

The original claim, preserved for the record: *"the answer is that we
deliberately need none… adding `hadoop-aws` + `aws-java-sdk-bundle` would
reintroduce a known dead end and a classpath conflict with the shaded bundle."*

Both halves of that were wrong, and the mistake is instructive: it collapsed two
different code paths into one.

| Code path | Who serves it | Scheme | Correct? |
|---|---|---|---|
| Iceberg **table** read/write (Lakekeeper locations) | Iceberg `S3FileIO` (shaded `iceberg-aws-bundle`, SDK v2) | `s3://` | **Yes — unchanged.** `hadoop-aws` genuinely cannot serve this, and is not used for it. |
| Plain **Parquet** read of `ops_normalized/` | Hadoop FileSystem API (`hadoop-aws`) | `s3a://` | **This is what the audit missed entirely.** |

A plain `spark.read.parquet(...)` never involves Iceberg, so `S3FileIO` is not in
the picture — Spark resolves the scheme through Hadoop's FileSystem, which has no
handler for `s3`/`s3a` without `hadoop-aws`. Verified at Gate A before any model
was written:

```text
s3://…/part-0.parquet   -> UnsupportedFileSystemException: No FileSystem for scheme "s3"
s3a://…/part-0.parquet  -> ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
```

**Why nobody caught this earlier:** nothing in this repo had ever asked Spark to
read a Parquet file. Plan 112's A2 wrote synthetic in-memory rows
(`spark.createDataFrame(_BATCH_1, ...)`); A3 read `analytics.duckdb` via DuckDB
and handed Spark a pandas DataFrame. Both sidestep the filesystem entirely. Gate
A is the first Spark-native Parquet read in the project — which is also why "read
normalized Parquet directly through Spark" was a riskier assumption than it looked.

**And there is no classpath conflict.** `hadoop-aws` uses AWS SDK v1
(`com.amazonaws`); `iceberg-aws-bundle` shades SDK v2 (`software.amazon.awssdk`).
Different packages, no collision. Verified: a single Spark session reads `s3a://`
Parquet *and* resolves the Iceberg REST catalog, in the same query.

Constraint to preserve: `hadoop-aws` **must** match the `hadoop-client-api` /
`hadoop-client-runtime` version pyspark 3.5.3 bundles (**3.3.4**) — a mismatch is
a `NoSuchMethodError` at runtime, not a resolution error. Bump both together, and
only with pyspark.

The part of the original claim that survives: **do not** try to make `hadoop-aws`
serve Lakekeeper's `s3://` table locations. That dead end is real. The two stacks
coexist, each on its own scheme.

### Environment compatibility

- **Iceberg REST / Lakekeeper + MinIO** — reuse `spark_conf_for_rest_catalog()`
  (`shared/iceberg_catalog.py`) verbatim; it is the R1 chokepoint and already
  produces the full `rest` + `S3FileIO` + path-style-access config. Inject it into
  the dbt target via `server_side_parameters`. **Do not** hand-write catalog config
  into `profiles.yml` — that would fork the chokepoint and break the R2 guarantee
  that a catalog swap edits one file.
- **ARM64 VM / x86_64 CI** — the image already builds on both; the arch-specific
  `JAVA_HOME` is resolved via symlink, not hardcoded (per Plan 105). PySpark and
  dbt-spark are pure-Python wheels; the jars are arch-independent. No new arch risk.
- **Dependency isolation** — install dbt-spark **only** in the lakehouse image,
  never alongside `dbt-duckdb`/`dbt-postgres` in `dbt_runner`. Same convention as
  `airflow/requirements.txt`: a JVM-backed engine does not share a resolver with the
  rest of the repo. CI needs its own isolated venv for it for the same reason.

### Two config details that will bite Gate A

Both are cheap now and confusing later:

1. **`spark.sql.defaultCatalog=cartracker`.** dbt-spark relations are two-part
   (`schema.identifier`); it has no `catalog:` profile field (that is a
   dbt-databricks/Unity feature). Without setting the default catalog, dbt writes
   into `spark_catalog` — i.e. **not Iceberg at all** — and the build may appear to
   succeed. `CATALOG_NAME` is already the stable `cartracker` constant.
2. **`spark.sql.session.timeZone=UTC`.** Spark has no `TIMESTAMPTZ`; its `TIMESTAMP`
   is instant-typed (`TIMESTAMP_LTZ`) and resolves offsets against the session zone.
   Unpinned, this silently shifts every timestamp and would surface as parity drift
   (F5/F10/F12 territory) rather than an error.

Also set `file_format: iceberg` in the target's `+models` config — dbt-spark
**requires** it for `merge` (delta/iceberg/hudi only), which the strategy decision
below depends on.

## Incremental strategy decision

Grounded in F1 above (verified, not hypothesised). Five of seven models are a
straight `merge` port; two need a real decision.

| Model | Cadence | Today | **Gate B/C target** | Notes |
|---|---|---|---|---|
| int_listing_state_fingerprints | daily | `delete+insert` / `artifact_id` | **`merge`** | Row-unique + `unique` test. In-model dedupe (`row_number()=1`) already guarantees no duplicate source key, which also satisfies Iceberg's MERGE cardinality check. |
| int_listing_observation_fingerprints | daily | `delete+insert` / `observation_id` | **`merge`** | Same shape. |
| int_price_history | hourly | `delete+insert` / `vin` | **`merge`** | One row per vin. Gap: merge cannot delete a vin whose events all vanish — the event stream is append-only, so unreachable. |
| int_latest_observation | hourly | `delete+insert` / `vin17` | **`merge`** | One row per vin17. Same gap, same reasoning. |
| mart_scrape_volume | hourly | `delete+insert` / `scrape_volume_key` | **`merge`** — *the recommendation below was wrong; see the correction* | ~~The one genuine subtlety.~~ **Measured at Gate B: delete+insert does NOT remove a disappeared `(hour, source)`, so merge is exactly equivalent to today's behaviour and insert_overwrite would be a behaviour change.** See [the correction](#correction-the-mart_scrape_volume-canary-premise-was-false). |
| int_listing_state_runs | **daily** | `delete+insert` / `vin17` (multi-row) | **`table`** (full rebuild) | No equivalent strategy. See F1 Option A. |
| int_listing_observation_runs | **daily** | `delete+insert` / `listing_id` (multi-row) | **`table`** (full rebuild) | Same. |

Sequencing (refines the audit's original step 2): `mart_scrape_volume` remains the
right F1 canary, but the question it now answers is narrower and sharper —
**`merge` vs `insert_overwrite` for window replacement**, not "does delete+insert
exist". Prove specifically that a `(hour, source)` combination which disappears
from the recomputed window is actually *removed* from the target. That is the exact
behaviour `merge` gets wrong, and it is invisible to a row-count check.

### Correction: the `mart_scrape_volume` canary premise was FALSE

**Ran the canary (Gate B, 2026-07-16). The paragraph immediately above is wrong,
and so is the `insert_overwrite` recommendation it justifies.**

The premise was that dbt-duckdb's `delete+insert` *removes* a `(hour, source)`
row that drops out of the recomputed 72h window, and that `merge` would strand
it. Both halves are false. dbt-duckdb's
`duckdb__get_delete_insert_merge_sql` generates, for a single-column key:

```sql
delete from target where (unique_key) in (select (unique_key) from source);
insert into target (...) (select ... from source);
```

It deletes **only keys present in the incoming batch**. A `(hour, source)` that
disappears from the recomputed window is by definition *absent from the source*,
so the DELETE never matches it and it survives. Verified by replaying that exact
SQL against a real DuckDB table with a disappearing key: the row survived.

Consequences, all of which cut *toward* the simpler port:

1. **`merge` on `scrape_volume_key` is exactly equivalent to today's production
   behaviour**, not a regression. Both are key-wise upserts.
2. **`insert_overwrite` would be a behaviour CHANGE** — it would start removing
   rows production currently keeps. Gate B measures fidelity to the DuckDB
   baseline, so adopting it here would have manufactured a parity failure and
   then "fixed" it in the wrong direction.
3. **The "one genuine subtlety" of the whole strategy port does not exist.**
   `delete+insert` on a *surrogate* key was never window replacement; the model's
   comment describing it as "window replacement" describes the SELECT, not the
   write.
4. Today's DuckDB build genuinely strands such rows. That may be a real (if
   low-impact) modelling bug, but it is **not a migration concern** — if we want
   to fix it, fix it on DuckDB first, deliberately, so both engines change
   together.

There is also a `partition_by` trap this correction avoids stumbling into: had we
gone with `insert_overwrite`, the only safe partition granularity would have been
`hour`, because the 72h window starts on an hour boundary, not a day boundary —
dynamic overwrite partitioned by *day* would have silently deleted the
pre-window rows of the boundary day.

**Canary result (real snapshot, 16,847 observations, 1,354 `(hour, source)`
buckets):** `merge` on Iceberg via dbt-spark ran, was idempotent on rerun (1,354
rows, 0 duplicate keys), and hit **exact parity with DuckDB — 1,354/1,354 rows,
zero value differences**. Iceberg's snapshot history confirms the write was a
real MERGE (`op=overwrite`), not a rebuild.

One Gate C signal fell out of it: that MERGE reported `deleted=1354 added=1354`.
Iceberg MERGE is copy-on-write and rewrites whole *data files*, so touching a few
rows in the 72h window rewrote the single file holding the entire table. Correct,
but it means merge cost here scales with file layout, not with window size.

**Do not** implement the Option C adapter-macro fork during Gate A. Two daily models
being full-rebuild is a cheaper problem than a forked incremental materialization
that must be re-verified on every dbt-spark upgrade. Revisit only with a VM
measurement showing the rebuild is too slow.

## Gate B dialect measurements

**Measured 2026-07-16 against both real engines** (DuckDB 1.5.4 in `dbt/Dockerfile`;
Spark 3.5.3 in `lakehouse/Dockerfile`, session timezone pinned UTC), before any
Gate B model was ported. Implemented as adapter-dispatched macros in
[dbt/macros/dialect.sql](../dbt/macros/dialect.sql).

The headline: **three of the "mechanical" translations in F4/F5/F12 are wrong**,
and the one flagged as highest-blast-radius (md5/fingerprint) is **exactly
identical**. The audit's risk ranking was inverted.

| Item | Obvious translation | Measured verdict |
|---|---|---|
| **F5 `datediff('hour', a, b)`** | `(unix_timestamp(b) - unix_timestamp(a))/3600` | **WRONG on 3/6 cases.** DuckDB counts *hour boundaries crossed*, not elapsed time: `01:59→02:01` (2 min) = **1**, `00:30→03:10` (2h40m) = **3**. The naive form gives 0 and 2. Correct: truncate both to `HOUR`, then diff — 6/6 exact. |
| **F5 `datediff('day', a, b)`** | `datediff(b, a)` | **Correct as-is**, 4/4 (note reversed arg order). |
| **F12 `x::int`** | `cast(x as int)` | **WRONG.** DuckDB *rounds*; Spark *truncates*. `1.9` → DuckDB 2, Spark 1. DuckDB rounds a DOUBLE **half-to-even**, so the match is `bround()`, not `round()` (which is half-up). `cast(bround(x) as int)` → 7/7 exact. |
| **F4 `arg_max(v, o)`** | `max_by(v, o)` | **WRONG on nulls.** DuckDB ignores rows whose *value* is null; Spark returns the null. On `((null,2),('b',1))`: DuckDB `'b'`, Spark `NULL`. Fix: `max_by(v,o) filter (where v is not null)` → matches. |
| **F4 ties in `arg_max`** | — | **Genuinely divergent, not fixed.** On a tie in the ordering column DuckDB takes the first row, Spark the last. Neither engine guarantees either. The DuckDB model is *already* non-deterministic here; the port does not worsen it, but a tie can surface as a parity difference. |
| **F10 `percentile_cont(p) within group`** | same syntax | **Supported on Spark 3.5.3**, and the raw values agree exactly (`p10` over 1..10 = 1.9 on both). The divergence is entirely the `::int` cast above — i.e. the audit's predicted "one-dollar difference on every benchmark row" is real, but it is an F12 bug, not an F10 one. |
| **F4 `median(x)`** | `percentile(x, 0.5)` | **Correct.** Both interpolate (2.5 over 1,2,3,4). |
| **F9 `regexp_matches` / `!~`** | `rlike` / `not rlike` | **Correct**, and both return NULL (not false) on null input, so the null-guards behave identically. |
| **`md5` / `concat_ws`** | — | **IDENTICAL on every probe.** Same digest; both skip nulls in `concat_ws`; `timestamp`, `int`, `double`, and `decimal(p,s)` all render to the same string. `md5(concat_ws('|', ts, src))` agreed digit-for-digit. |
| **`decimal(p,s)` casts** | `cast(x as decimal(5,2))` | **Identical**, including half-up at `2.555 → 2.56`. |
| **`trim` as a column name** | needs backticks? | **No change needed** — Spark accepts it bare. |

### The md5/fingerprint risk is closed — on real data

Risk #5 ("`md5`/concat parity… low probability, very high blast radius — check on
real data at Gate B, not on fixtures") is **resolved, on real data as required**.
Beyond the unit probes above, the `mart_scrape_volume` canary compared **1,354
md5 surrogate keys** derived from 16,847 real observations: **1,354/1,354
matched**, zero value differences. Since `scrape_volume_key` is
`md5(concat_ws('|', <timestamp>, <varchar>))`, that jointly exercises md5,
`concat_ws`, and timestamp→string rendering at scale.

This does not yet cover the *wide* fingerprints
(`int_listing_state_fingerprints`' 18-field and
`int_listing_observation_fingerprints`' 28-field hashes), which add nullable
varchar and numeric fields. But the mechanism is the same and the null/format
semantics are now measured, so the residual risk is low rather than unknown.

### Consequence for the models

`dbt/macros/dialect.sql` exists because of these measurements, and each `spark__`
implementation reproduces **what DuckDB actually does**, not what Spark's
similarly-named function does. DuckDB is the incumbent spec for the whole
dual-run period. Do not "simplify" `bround`, the `filter` on `max_by`, or the
truncate-then-diff `datediff_hours` back to the obvious spelling — the obvious
spelling is measurably wrong.

## Unit-Test Strategy For Spark/Iceberg Migration

### What exists today — corrected inventory

The Gate 0 draft said **85** unit tests (37 intermediate / 40 marts / 8 staging).
**That count is wrong.** The actual inventory, counted from the three
`unit_tests.yml` files (the only files in the project defining `unit_tests:`):

| Layer | Tests | Models covered |
|---|---|---|
| Staging | **2** | `stg_dealers` only |
| Intermediate | **31** | 8 models |
| Marts | **31** | 8 models |
| **Total** | **64** | **17 of 22 models** |

Per model: `int_listing_observation_fingerprints` 5, `int_latest_observation` 5,
`int_price_history` 4, `int_listing_state_runs` 4, `int_listing_state_fingerprints` 4,
`int_listing_observation_runs` 4, `int_listing_volatility_features` 3,
`int_benchmarks` 2; `mart_vehicle_snapshot` 5, `mart_price_freshness_trend` 4,
`mart_inventory_coverage` 4, `mart_detail_batch_outcomes` 4, `mart_deal_scores` 4,
`mart_cooldown_cohorts` 4, `mart_scrape_volume` 3, `mart_block_rate` 3;
`stg_dealers` 2.

Five models have **no** unit tests: `stg_observations`, `stg_price_events`,
`stg_blocked_cooldown_events`, `stg_search_configs`, `int_active_make_models`. For
the first three this is deliberate and correct — they are pure pass-through views,
and dbt 1.11's unit-test schema gives nothing to assert on them. The absence of
coverage on `int_active_make_models` is more notable, since it filters the entire
mart layer via its inner join (F8).

The staging layer is therefore **not** a migration concern for unit tests: its only
covered model is `stg_dealers`, which is the layering wrinkle (it refs an
intermediate model) and ports with the serving chain, not with staging.

### Does dbt-spark support unit tests?

**Yes — and this is now PROVEN here, not merely documented (Gate A, 2026-07-16).**
dbt-spark implemented `spark__safe_cast` and added functional unit-testing tests in
**v1.8.0**; `safe_cast` is the adapter capability dbt-core's unit-test feature
requires (unit tests render fixtures as `select … union all` CTEs wrapped in
`safe_cast`). Our pin (1.10.3) is well past that.

All 3 `mart_block_rate` unit tests were executed end-to-end against dbt-spark in
session mode and **passed**:

```text
1 of 3 PASS mart_block_rate::test_block_rate_event_type_split ... [PASS in 1.23s]
2 of 3 PASS mart_block_rate::test_block_rate_hourly_grouping .... [PASS in 0.39s]
3 of 3 PASS mart_block_rate::test_block_rate_unique_listings .... [PASS in 0.29s]
```

What that one run settles:

- **The `+00` timestamp question below is closed.** These fixtures use the exact
  `"YYYY-MM-DD HH:MM:SS+00"` form, and they parse correctly under
  `spark.sql.session.timeZone=UTC`. The silent-NULL failure mode did not occur.
- `safe_cast` renders; session mode drives the unit-test path.
- **Per-test cost measured: ~0.3–1.2s**, the first test paying warm-up, plus ~10s
  JVM startup per invocation. Extrapolated to all 64: ≈30s + startup. Slower than
  DuckDB's milliseconds, but well short of the multi-minute job feared below — the
  "keep the Spark selection narrow" advice stands, but it is a preference, not a
  constraint.

Still unproven, and not weakened by the above: that *all 64* tests pass on Spark.
Only 3 have run. The triage below stands as an estimate.

One structural advantage is already confirmed by reading the fixtures: every
unit test on an incremental model uses

```yaml
overrides:
  macros:
    is_incremental: false
```

That is adapter-agnostic dbt-core machinery. It means **the unit tests never
exercise the incremental strategy at all** — they test the SELECT, not the write.
So the entire F1 `delete+insert` problem, which is the biggest item in this audit,
lands on **zero** unit tests. The unit-test migration and the incremental-strategy
migration are independent workstreams. This is the single most useful finding in
this section: it decouples the two hardest parts of the plan.

The corollary is the risk: **nothing in the unit-test suite will catch an
incremental-strategy regression.** That gap is real today and gets worse on Spark,
where the strategies differ per model. It must be covered by fixture/parity tests
(below), not by unit tests.

### Portability triage of the 64 tests

Unit tests render the *model's* SQL, so every F2–F12 dialect item lands on them —
but only via the model. A test whose model needs no SQL change needs no test change.

| Bucket | Tests | Models | What's needed |
|---|---|---|---|
| **Likely portable unchanged** | ~14 | `mart_block_rate` (3), `mart_cooldown_cohorts` (4, but `arg_max`→`max_by` in the model), `mart_detail_batch_outcomes` (4), `mart_inventory_coverage` (4) | Model-side dialect fixes only; fixtures and expectations untouched. These are the Low-difficulty chains. |
| **Fixture/expectation changes likely** | ~24 | `int_price_history` (4), `int_benchmarks` (2), `int_latest_observation` (5), `mart_vehicle_snapshot` (5), `mart_price_freshness_trend` (4), `mart_scrape_volume` (3) | Driven by `arg_max`/`arg_min`, `percentile_cont` + `::int` rounding, `distinct on`, `select * exclude`, `now_ts()`. Rounding items (F10/F12) may shift *expected values* by ±1, not just syntax. |
| **Highest churn** | ~23 | the 4 fingerprint/run models (17), `int_listing_volatility_features` (3), `stg_dealers` (2) | `datediff('hour', …)` feeds `run_duration_hours` (F5) — a **model feature**, so expectations may legitimately change. Hash/fingerprint tests (`test_fingerprints_identical_inputs_same_hash`) depend on `md5`/concat semantics matching across engines. |
| **Should become parity tests instead** | 3 | `int_listing_volatility_features` | Already the widest-input model (6 refs, ~90-row fixtures). Re-authoring these as Spark unit tests is high cost for low marginal signal when Gate B parity already compares this model's real output VIN-by-VIN. |

**Cross-cutting fixture risk: 208 timestamp literals** in the `"YYYY-MM-DD HH:MM:SS+00"`
form across the three files. Spark's `stringToTimestamp` is documented to accept
`+|-h[h]` offsets (SPARK-31005), so `+00` **should** parse — but this is exactly the
kind of thing that must be proven, not assumed, because the failure mode is a silent
`NULL` (non-ANSI mode) or a whole-suite time shift rather than an error. Pinning
`spark.sql.session.timeZone=UTC` is a prerequisite. **This one check gates roughly the
entire suite**; run it first (see the Gate A proof below).

### Recommended Gate A test policy

1. **Migrated models are NOT required to keep dbt unit tests immediately.**
   Gate A's first model is `stg_blocked_cooldown_events → mart_block_rate`, chosen
   precisely so failures are unambiguously infrastructure failures. Porting 64 unit
   tests concurrently would destroy that property. Unit tests become **required at
   Gate B**, per chain, once the adapter is proven.

2. **Minimum bar if dbt unit tests do not work on Spark:** the `merge`/
   `insert_overwrite`/full-rebuild decisions above are *write-path* semantics that
   unit tests structurally cannot cover, so these are required **regardless**:
   - Extend `scripts/seed_lake_snapshot_fixture.py` phases (never a throwaway
     shadow dbt project) to cover, per migrated incremental model: bootstrap from
     empty, idempotent rerun, late-arrival pickup, correction replacement, and
     full-refresh equivalence.
   - One **negative** fixture proving the disappearing-`(hour, source)` case for
     `mart_scrape_volume` — the exact case `merge` gets wrong.
   - Gate B parity (row count, distinct/duplicate key count, null counts, sampled
     entity histories) with a **stated numeric tolerance** for the ±1 rounding of
     F5/F10/F12. Without a stated tolerance, "rows match, checksums don't" reads as
     failure and stalls the gate.

3. **How long DuckDB unit tests stay useful: keep all 64 running on DuckDB until
   the chain they cover is cut over at Gate E — do not delete them at Gate B.**
   During dual-run they are the *executable specification* of intended behaviour.
   When Spark and DuckDB disagree, the DuckDB unit test is what says which one is
   right; deleting it turns a parity failure into an argument. Retire per chain,
   only after that chain's readers are migrated.

4. **CI job shape.** Do not add Spark to the existing `dbt build + test` job.
   - Keep the DuckDB unit-test path exactly where it is: it is fast, and it is the
     regression net for the 5 models never migrating early.
   - Add a **separate, isolated** `dbt-spark` job with its own venv (per the
     `airflow/requirements.txt` convention — dbt-spark must not share a resolver
     with dbt-duckdb).
   - Keep it **narrow**: select only migrated models (`--select tag:iceberg` or the
     ported chain), not the whole project. Every Spark unit test pays JVM startup
     plus per-test Spark job overhead — seconds each, against milliseconds on
     DuckDB. 64 tests × Spark overhead is a multi-minute job and would be the
     slowest thing in CI for no added signal on unmigrated models.
   - Respect the existing Layer 1 → Layer 2 ordering: SQL smoke before dbt
     integration tests.
   - If JVM startup makes even the narrow job unacceptable, fall back to the pattern
     Gate A already allows: unit coverage in CI plus a documented VM/local smoke.

5. ~~**The cheap Gate A proof that de-risks all of the above.**~~ **DONE
   (2026-07-16) — and it paid off exactly as hoped.** All 3 `mart_block_rate` unit
   tests ran end-to-end on dbt-spark and passed. Session mode works, `safe_cast`
   renders, `+00` parses, the session timezone holds, and the cost is ~0.3–1.2s per
   test (plus ~10s JVM startup per invocation). The measured cost is low enough
   that the CI-shape worry in item 4 above is softer than written: all 64 tests
   would be ≈30s of Spark time, not minutes. Narrow selection remains the
   recommendation, but on cost-efficiency grounds, not feasibility.

## Unit-test impact

Short version for the plan-level reader:

- **64 unit tests, not 85** (the Gate 0 figure was wrong); 17 of 22 models covered.
- **dbt-spark supports unit tests** (since 1.8.0, via `spark__safe_cast`) —
  documented, **unproven in this repo**. Do not record this as "covered".
- **Zero unit tests exercise `delete+insert`**, because every incremental model's
  tests override `is_incremental: false`. The F1 strategy migration and the
  unit-test migration are **independent**. Corollary: no unit test will catch an
  incremental regression — that needs fixture/parity coverage.
- Rough triage: ~14 portable unchanged, ~24 needing fixture/expectation edits, ~23
  high-churn, 3 better re-scoped as parity tests.
- Biggest single unknown: **208 `+00` timestamp literals** — documented as
  parseable by Spark, not yet proven, silent-`NULL` failure mode.
- Unit tests are **not** required at Gate A; required at Gate B per chain; DuckDB
  tests stay until per-chain cutover at Gate E.

## Risks/unknowns remaining

Resolved by the research pass: F1 (`delete+insert` confirmed absent, per-model plan
agreed), the adapter choice, and the unit-test question.

**Resolved by the Gate A implementation pass (2026-07-16)** — items 1–3 below were
the top three risks and are now all closed:

1. ~~**`session` mode is officially experimental.**~~ **Proven to work.** It drove a
   full dbt build (parse → compile → `CREATE TABLE ... USING iceberg` → catalog
   commit) against Lakekeeper + `S3FileIO`, plus 3 unit tests. It remains labeled
   experimental upstream, so the `thrift` fallback stays the contingency and the
   pin stays at `dbt-spark==1.10.3` — but "does it work at all" is answered.
2. ~~**The `+00` timestamp fixture question** (208 sites).~~ **Closed.** The
   `mart_block_rate` fixtures use exactly that form and parse correctly under
   `spark.sql.session.timeZone=UTC`. The silent-NULL mode did not occur. (Proven
   for these fixtures; the other ~205 sites are the same literal form, so the risk
   is now low rather than eliminated.)
3. ~~**`spark.sql.defaultCatalog` behaviour with dbt-spark's two-part relations.**~~
   **Confirmed honoured.** dbt wrote to `cartracker.cartracker_experiments.mart_block_rate`
   with `provider=iceberg` and an `s3://` location. The advice stands and is now
   implemented in code: verify via the catalog, never the exit code
   (`run_dbt_spark.assert_default_catalog` + `verify_iceberg_tables`).

**New, discovered at Gate A** (neither predicted here):

- **`hadoop-aws` is required** for plain Parquet reads — see the corrected
  [adapter choice](#gate-a-adapter-choice) section.
- **Staging cannot be a `view` on Spark**; a persisted view re-qualifies the
  `parquet.`s3a://…`` reference against its own catalog and fails. `ephemeral` is
  the equivalent (both = no stored data).
- **F8 leaks into every target.** dbt renders all sources' Jinja at parse time
  regardless of `--select`, so `POSTGRES_URL` must exist even for a DAG that never
  touches Postgres.

Still open, ordered by how much they'd change the plan:

4. **Non-atomic writes are now unavoidable on Spark**, for any future
   delete+insert-equivalent (two Iceberg commits vs DuckDB's one transaction).
   Guardrails R3 (consumers read a serving layer) and R5 (Iceberg stays rebuildable)
   mitigate this; Gate D's serving choice must not expose mid-build state to readers.
   Not a Gate A blocker, but it constrains Gate C and should not be rediscovered then.
5. ~~**`arg_max`/`md5`/concat parity for the fingerprint hashes.**~~ **Largely
   closed at Gate B (2026-07-16), on real data.** `md5`/`concat_ws`/null/format
   semantics are **identical** across the two engines on every probe, and the
   canary matched **1,354/1,354** real md5 surrogate keys. See
   [Gate B dialect measurements](#gate-b-dialect-measurements). Two residuals,
   both narrower than the original risk: the *wide* (18/28-field) fingerprints
   have not been compared yet, and `arg_max` — which was bundled into this risk —
   turned out to diverge for its own unrelated reasons (null values, and ties),
   now handled by a macro rather than by hash luck.
6. **How does Spark reach `search_configs` / `tracked_models`?** (F8) Unchanged from
   Gate 0 and still open. Blocks the serving chain, not Gate A. Recommendation stands:
   hourly snapshot to MinIO/Iceberg over a live JDBC read. Decide before Gate B.
7. **Parity tolerance for rounding** (F5/F10/F12) — still needs a stated number.
   Unchanged from Gate 0.
8. **Retire or demote the Postgres dbt targets.** Unchanged from Gate 0: they are not
   the production build path (`--target duckdb` is hardcoded in dbt_runner) and are
   not migration scaffolding. Concrete cleanup: `dbt_runner`'s `/dbt/docs/generate`
   runs `dbt docs generate` without `--target`, inheriting `target: prod`; Gate A
   should pass `--target duckdb` or make the docs target explicit.

Two small cleanups this audit recommends folding into Gate A rather than tracking
separately, since both make the port measurably cheaper:

- Route `mart_price_freshness_trend`'s six bare `now()` calls through the
  existing `now_ts()` macro (F11).
- Add a `unique` test on `mart_deal_scores.vin` so the most-read table in the
  project has a comparable key before parity testing starts.

Two small cleanups this audit recommends folding into Gate A rather than tracking
separately, since both make the port measurably cheaper:

- Route `mart_price_freshness_trend`'s six bare `now()` calls through the
  existing `now_ts()` macro (F11).
- Add a `unique` test on `mart_deal_scores.vin` so the most-read table in the
  project has a comparable key before parity testing starts.
