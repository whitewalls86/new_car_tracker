# Plan 123: dbt Incrementalization and Analytics Resource Governance

## Objective

Make the current DuckDB-backed analytics pipeline reliable as the project grows,
without turning short-term DuckDB optimization into a barrier to the planned
Spark/Delta migration.

The immediate driver is the production incident on 2026-07-09:

- the hourly dbt graph grew from roughly 162 to 200 resources;
- two dbt processes were killed by the Linux OOM killer;
- each killed process held roughly 12.9 GB RSS;
- the VM has roughly 23.4 GB total memory and already runs the production
  application stack;
- dbt used four DuckDB threads and had no DuckDB or container memory limit;
- a later build also experienced a transient MinIO read failure while the host
  was under heavy analytics pressure.

This plan begins after Plan 120 Gate D is complete.

## Goals

1. Prevent analytics work from destabilizing production services.
2. Reduce the amount of historical data rebuilt during every hourly dbt run.
3. Separate operational dashboard freshness from feature-store refresh cadence.
4. Preserve correctness with late-arriving and corrected events.
5. Produce resource and runtime evidence for the later dbt/Spark migration.

## Non-Goals

- Do not migrate dbt to Spark in this plan; that remains Plan 118.
- Do not convert every model to incremental materialization.
- Do not make feature tables part of the hot scraper or claim path.
- Do not hide failed builds by retrying indefinitely.
- Do not rely on incremental state without a tested full-refresh recovery path.
- Do not run snapshot-worker and heavy dbt builds concurrently on the VM.

## Current Model Assessment

| Model | Current grain | Recommendation | Reason |
|-------|---------------|----------------|--------|
| `int_listing_state_fingerprints` | One row per valid detail artifact | Keep as a detail-only canonical-state model or replace with a detail subset of the observation fingerprint model | Phase 2 made this incremental, but later review found it is too narrow for cadence learning because SRP and carousel observations also refresh price/visibility and can suppress detail scrapes |
| `int_listing_observation_fingerprints` | One row per observed listing per artifact (`artifact_id` + `listing_id`) across detail, SRP, and carousel | Add as the defensible base feature-store layer, incremental append/update by observation row key | The processing writers already emit one normalized silver row per listing observation; this grain handles many-listing SRP/carousel artifacts without treating bare `artifact_id` as row-unique |
| `int_price_history` | One mutable aggregate row per VIN | Incremental, replace affected VINs | New events only require full history recomputation for VINs touched by the incremental input |
| `int_listing_state_runs` | Multiple ordered runs per VIN | Incremental, replace affected VINs after Phase 2b is resolved | New fingerprints can extend or split the open run; recompute all runs for changed VINs. Phase 4 currently depends on detail-only fingerprints and should be revisited after the all-source observation layer lands |
| `int_latest_observation` | One current row per VIN | Evaluate after upstream conversion | Per-VIN replacement is possible, but source-priority and late-arrival behavior require careful tests |
| `int_benchmarks` | Current aggregate per make/model | Keep table; slower cadence initially | A changed VIN can alter make/model percentiles; targeted group replacement is possible but lower priority |
| `int_listing_volatility_features` | One current feature row per VIN | Keep table; daily/on-demand cadence | Dealer and make/model aggregates can change many rows; time-relative features also age without new events |
| `mart_vehicle_snapshot` | One current row per VIN | Keep table initially | Listing status is time-relative and can change without a new source row |

## Phase 0: Immediate Production Guardrails

Apply safety controls before incremental conversion.

### DuckDB limits

Change the DuckDB dbt target from four threads to two and configure an explicit
memory budget:

```yaml
duckdb:
  threads: 2
  settings:
    memory_limit: "8GB"
```

Validate the exact memory budget on the VM. It must leave sufficient headroom
for Postgres, MinIO, Airflow, scraper, ops, and monitoring.

### Container and orchestration controls

- Add a dbt-runner container memory limit as a final containment boundary.
- Ensure the limit is above DuckDB's configured memory budget but below the
  amount that can trigger host-wide OOM.
- Prevent overlapping dbt builds using the existing active-job guard.
- Prevent snapshot-worker execution from overlapping a heavy dbt build.
- Add one bounded Airflow retry for transient infrastructure failures.
- Do not retry an OOM failure without changing execution conditions.

### Observability

Record for every dbt invocation:

- invocation ID and command;
- start/end time and return code;
- peak RSS if available;
- DuckDB thread and memory settings;
- selected model set;
- whether the run was incremental or full refresh;
- model timing from `run_results.json`;
- OOM/SIGKILL classification when return code is `-9` or `137`.

Acceptance gate:

- a complete production build succeeds without host OOM;
- host memory retains an agreed safety margin;
- production APIs and MinIO remain healthy during the build.

### Phase 0 progress (2026-07-09)

Implemented in `feature/plan-123-dbt-resource-guardrails`:

- [x] `dbt/profiles.yml` duckdb target: `threads: 4` -> `2`, added
      `settings.memory_limit: "8GB"`.
- [x] `docker-compose.yml` `dbt_runner` service: added `mem_limit: 12g` —
      above the 8GB DuckDB budget, below the ~23.4GB host total, leaving
      headroom for Postgres/MinIO/Airflow/scraper/ops/monitoring.
- [x] Active-job guard confirmed: `shared/job_counter.py` +
      `dbt_runner/app.py` `/dbt/build` already returns 409 and refuses
      overlapping builds within the dbt_runner process. No change needed.
- [x] Snapshot-worker overlap: `snapshot-worker` had no way to know a dbt
      build was running (separate container, separate in-process job
      counter). Added `DBT_RUNNER_URL` env var to the `snapshot-worker`
      service and a best-effort `_check_dbt_runner_not_building()` guard in
      `archiver/processors/export_ci_lake_snapshot.py::main()` that checks
      dbt_runner's `/ready` endpoint before a real (non-dry-run) export and
      aborts if a build is in progress. Skips silently if `DBT_RUNNER_URL`
      is unset or dbt_runner is unreachable, so it never blocks on
      unrelated infra issues.
- [x] Airflow retry: `dbt_build` task in both `dbt_build.py` and
      `hourly_analytics_refresh.py` keeps its existing bounded
      `retries=1` / 30s backoff for transient infra failures, but now
      raises `AirflowFailException` (skips the retry) when the response is
      classified as OOM/SIGKILL.
- [x] Observability: `dbt_runner/app.py` `/dbt/build` now returns
      `invocation_id`, `started_at`/`ended_at`/`duration_seconds`,
      `duckdb_threads`/`duckdb_memory_limit`, `full_refresh`,
      `model_timings` (from `target/run_results.json` when present), and
      `likely_oom` (return code `-9` or `137`). All fields are also emitted
      via the existing JSON logger.

Not converted to incremental, not split into cadences, no Spark/Delta work —
out of scope for this commit per plan.

Still needs VM verification per the acceptance gate above:

- [ ] Deploy and run one complete production build under monitoring to
      confirm no host OOM and that production APIs/MinIO stay healthy.
- [ ] Confirm the chosen `8GB` DuckDB budget / `12g` container limit are
      sufficient for the current ~200-resource graph (the incident measured
      ~12.9GB RSS per killed process with no memory_limit configured at
      all, so 8GB may force spilling/slower runs — revisit after a real
      run's `model_timings` and `duration_seconds` are observed).
- [ ] Confirm the `AirflowFailException` OOM short-circuit fires correctly
      against a real SIGKILL, not just the classification unit tests.

## Phase 1: Split Build Cadences

Stop treating all analytical outputs as equally time-sensitive.

Create explicit dbt tags/selectors:

- `hourly_core`: operational dashboards and current-state outputs that genuinely
  require hourly freshness;
- `feature_daily`: feature stores, state-history aggregates, and benchmarks;
- `full_validation`: complete graph plus all tests;
- `backtest`: models required for Plan 112 reproducible feature generation.

Proposed scheduling:

| Workload | Initial cadence |
|----------|-----------------|
| Hourly operational models | Hourly |
| Fingerprint/price/run incremental models | Hourly or every few hours after profiling |
| Benchmarks | Daily |
| Volatility feature store | Daily or on demand |
| Full graph validation | Daily and before deployment |
| Full refresh | Manual maintenance window |

The exact split must be derived from dashboard and API dependencies, not model
names alone.

Acceptance gate:

- hourly DAG no longer executes the complete 200-resource graph;
- every dashboard/API dependency is assigned to a documented cadence;
- daily/full builds cannot overlap the hourly build.

### Phase 1 progress (2026-07-09)

In progress on `feature/plan-123-dbt-cadences`. dbt/DuckDB resources are
tagged, the hourly Airflow default now selects only `hourly_core`, and a
`full_validation` path remains manually available. Not yet done: deploying
to the VM, adding a scheduled `feature_daily` build, and Phase 2+
incrementalization.

Cadence assignment was derived from actual `ref()` dependencies
(`dbt/models/**/*.sql`) and from what dashboard/ops code actually queries
(`dashboard/queries.py`, `dashboard/sql/*.sql`, `ops/metrics/duckdb_gauges.py`,
`ops/routers/info.py`), not from model name alone. Notably
`dashboard/sql/deals_table.sql` selects from `mart_deal_scores`, which joins
`int_benchmarks` — so `int_benchmarks` is `hourly_core` even though it is not
directly queried by any dashboard endpoint.

#### `hourly_core` (config tag on 17 models)

Operational dashboards, freshness metrics, scrape health, and current
production-facing views. Runs on `hourly_analytics_refresh` every hour.

- `stg_observations`, `stg_price_events`, `stg_blocked_cooldown_events`,
  `stg_search_configs` — hourly source staging views.
- `stg_dealers` — depends on `int_latest_observation`; feeds
  `mart_deal_scores`.
- `int_latest_observation`, `int_active_make_models`, `int_price_history`,
  `int_benchmarks` — direct or one-hop dependencies of `mart_vehicle_snapshot`
  / `mart_deal_scores`.
- `mart_vehicle_snapshot`, `mart_deal_scores` — queried by
  `dashboard/sql/mart_freshness.sql`, `dashboard/sql/deals_table.sql`, and
  the other `deals_*`/`inventory_*`/`market_trends_*` dashboard queries, plus
  `ops/routers/info.py`.
- `mart_scrape_volume`, `mart_block_rate`, `mart_detail_batch_outcomes`,
  `mart_inventory_coverage`, `mart_cooldown_cohorts`,
  `mart_price_freshness_trend` — queried by `ops/metrics/duckdb_gauges.py`
  and `dashboard/sql/data_health_*.sql` for operational/data-health gauges.

#### `feature_daily` + `backtest` (config tag on 3 models)

Feature-store/backtesting models with no dashboard or API dependency:
`int_listing_state_fingerprints`, `int_listing_state_runs`,
`int_listing_volatility_features`. Tagged with both `feature_daily` and
`backtest` — they are the Plan 112 reproducible feature-generation chain, and
nothing distinguishes a separate `backtest`-only subset yet. Intended for a
daily or on-demand schedule; not yet wired into an Airflow DAG (no scheduled
daily build exists — a manual `dbt build --selector feature_daily` run is the
only trigger today).

#### `full_validation`

Named selector `full_validation` (`dbt/selectors.yml`) selects the complete
graph (`fqn:*`) plus all tests — equivalent to `dbt build` with no selector.
Used for the manual `dbt_build` Airflow DAG and local pre-deploy validation,
not for the hourly schedule.

#### Manual commands / Airflow trigger examples

```bash
# Local: hourly cadence only
dbt build --selector hourly_core

# Local: feature/backtest models only
dbt build --selector feature_daily
dbt build --selector backtest

# Local: full graph + all tests (same as plain `dbt build`)
dbt build --selector full_validation
```

```bash
# Airflow: manual full-graph build via the always-manual dbt_build DAG
# (no conf needed — it has no default selector)
airflow dags trigger dbt_build

# Airflow: manual feature_daily build via the same DAG
airflow dags trigger dbt_build --conf '{"select": ["tag:feature_daily"]}'

# Airflow: override the hourly DAG's default hourly_core selection for one run,
# building the complete graph instead (dbt_runner's /dbt/build only forwards
# raw --select/--exclude tokens, not --selector, so "tag:full_validation" or
# "fqn:*" do NOT work here — an empty select list omits --select entirely,
# which is what makes dbt build everything; see dbt_runner/app.py).
airflow dags trigger hourly_analytics_refresh --conf '{"select": []}'
```

The hourly `dbt_runner` `/dbt/build` payload now defaults to
`{"select": ["tag:hourly_core"]}` (`airflow/dags/hourly_analytics_refresh.py`)
unless `dag_run.conf["select"]` is explicitly set. The manual `dbt_build` DAG
(`airflow/dags/dbt_build.py`) is unchanged: it has no default selector, so an
unparameterized manual trigger still builds the complete graph — this is the
documented full-refresh path. The `full_validation` named selector
(`dbt/selectors.yml`) is a local/manual `dbt` CLI convenience only — it is
not reachable through the `dbt_runner` HTTP API, which validates each
`--select`/`--exclude` token against `SAFE_TOKEN` and only ever passes them
through as raw `--select`/`--exclude`, never `--selector`.

#### Still needs VM validation

- [ ] Confirm the hourly DAG's actual runtime/resource drop once only
      `hourly_core` runs, using the Phase 0 observability fields
      (`model_timings`, `duration_seconds`) from a real production run.
- [ ] Stand up a scheduled `feature_daily` build (cadence/DAG not decided in
      this phase — Phase 1 only adds the tag and a manual trigger path).
- [ ] Confirm `full_validation` / manual `dbt_build` still completes within
      the Phase 0 DuckDB memory budget now that it's exercised on its own
      schedule rather than every hour.

## Phase 2: Incremental Detail Fingerprints

Convert `int_listing_state_fingerprints` first.

This phase was originally framed as "the" listing-state fingerprint layer. After
reviewing the SRP and detail/carousel write paths on 2026-07-10, that framing is
too narrow: the implemented model is a valid **detail-only canonical state**
fingerprint, but it is not a complete base layer for cadence learning.

Requirements:

- use `artifact_id` as the durable row identity;
- derive a source watermark from the existing target;
- include a configurable late-arrival lookback based on `fetched_at`;
- deduplicate by `artifact_id`;
- update an existing artifact if corrected source data is possible;
- preserve deterministic fingerprint logic exactly;
- make first run and `--full-refresh` behavior explicit.

Tests:

- empty target bootstrap;
- no new artifacts;
- new artifacts append once;
- repeated run is idempotent;
- late artifact inside lookback is included;
- duplicate artifact does not duplicate target;
- incremental output equals full-refresh output on the same fixture.

Acceptance gate:

- steady-state scan volume and peak memory are materially lower;
- row count and checksum/grouped comparison match full refresh.

### Phase 2 progress (2026-07-09)

Implemented in `feature/plan-123-incremental-fingerprints`. This phase
intentionally touches only `int_listing_state_fingerprints` — `int_price_history`
and `int_listing_state_runs` are left as full-table builds for Phases 3 and 4.

- [x] `dbt/models/intermediate/int_listing_state_fingerprints.sql` converted
      to `materialized='incremental'`, `unique_key='artifact_id'`,
      `incremental_strategy='delete+insert'`. `delete+insert` was chosen over
      DuckDB's native `merge` strategy because it's the base strategy dbt-duckdb
      supports on any DuckDB version and it's also available on the
      Postgres/Spark-family adapters this project may migrate onto later (Plan
      118) — it doesn't add DuckDB-only incremental semantics.
- [x] Added `fingerprint_incremental_lookback_days` (default `3`, matching the
      existing `staleness_window_days` convention) to `dbt/dbt_project.yml`
      vars, documented there and in
      `dbt/models/intermediate/int_listing_state_fingerprints.schema.yml`.
- [x] On an incremental run, source rows are rescanned from
      `max(target.fetched_at) - fingerprint_incremental_lookback_days` days;
      first run and `--full-refresh` have no target to watermark from, so they
      scan the full source, matching prior full-table behavior exactly.
- [x] Fingerprint hash logic (the `md5(concat_ws(...))` expression, column
      list, and `source = 'detail' and vin17 is not null` filters) is
      byte-for-byte unchanged — only wrapped in a `source_rows` CTE with the
      added incremental filter. dbt's unit test framework does not evaluate
      `is_incremental()` as true, so the existing pinned-hash unit tests in
      `unit_tests.yml` exercise the same full-scan path as before and are
      unaffected.
- [x] `delete+insert` only dedupes an incoming batch against the *existing
      target* row for a given `unique_key` — it does not collapse multiple
      rows sharing an `artifact_id` within the same incremental batch (e.g. an
      ingestion retry landing two rows for one artifact in the same lookback
      window). Added a `fingerprinted` CTE with
      `row_number() over (partition by artifact_id order by fetched_at desc, parsed_fingerprint) = 1`
      so the model itself guarantees `artifact_id` uniqueness regardless of
      source duplicates, and added a `unique` data test on `artifact_id` in
      `int_listing_state_fingerprints.schema.yml` (previously only `not_null`).
- [x] Model remains tagged `feature_daily` and `backtest`
      (`int_listing_state_fingerprints.schema.yml`); tags were not touched.
- [x] Added `tests/integration/dbt/test_fingerprints_incremental.py`: builds a
      throwaway dbt-duckdb project that seeds a `stg_observations` stand-in and
      runs the real model SQL (read directly from the repo) through real
      `dbt seed`/`dbt run`/`dbt run --full-refresh` invocations, since dbt unit
      tests can't exercise state across multiple invocations. Written as one
      sequential scenario test (not several independently-selectable test
      methods sharing state) since each step's assertions depend on exactly
      the state the previous step left behind. Covers: empty target bootstrap
      excludes non-detail/null-vin17 rows; a second run with unchanged source
      is idempotent; a new artifact appends exactly once; a repeated run does
      not duplicate it; a late artifact inside the lookback window is picked
      up; two source rows sharing one `artifact_id` in the same batch collapse
      to a single row (the latest `fetched_at` wins); a corrected
      `artifact_id` replaces its existing row rather than duplicating it; and
      `--full-refresh` output matches the accumulated incremental output.
      Verified locally against `dbt-core==1.10.20` / `dbt-duckdb==1.10.1`
      (pinned versions used in `dbt_runner/Dockerfile` and CI). Runs in CI's
      existing `pytest tests/integration/dbt/ -v -m integration` step in the
      `dbt` job; no CI workflow changes were needed.

      **Superseded (2026-07-10):** this throwaway-project test was deleted and
      its coverage ported to the shared-fixture pattern — see "Shadow test
      removal" below.

Still needs VM verification before this is considered fully rolled out:

- [ ] Run `feature_daily` once normally
      (`dbt build --selector feature_daily` or dbt_runner with
      `{"select": ["tag:feature_daily"]}`) and confirm success.
- [ ] Run `feature_daily` again immediately after and confirm no new rows /
      no duplicates (idempotency under real production data).
- [ ] Run the full graph or `dbt build --full-refresh` and confirm it still
      succeeds and rebuilds the complete table.
- [ ] Check `artifact_id` uniqueness on the real table:
      `select count(*), count(distinct artifact_id) from int_listing_state_fingerprints;`
- [ ] Compare this model's runtime in `run_results.json` against the previous
      full-table `feature_daily` run to confirm steady-state scan volume is
      actually lower.

### Phase 2 correction (2026-07-10): add all-source listing observation fingerprints

The Phase 2 implementation is technically correct for its actual grain:

```text
source = detail
grain = artifact_id
one detail artifact -> one listing observation
```

But it is not sufficient for the ML cadence feature store. SRP and carousel
observations are also meaningful listing refresh signals:

- SRP rows can refresh price and VIN/listing mappings, and the system may avoid a
  detail scrape when SRP already supplied enough data.
- Carousel rows are "other vehicles at this dealer"; the inherited dealer
  context is valid because the carousel membership is defined by the dealer.
- Both SRP and carousel artifacts can contain many listing observations under one
  `artifact_id`, so a bare `artifact_id` is not a defensible unique key for an
  all-source model.

The processing writers already emit one normalized silver row per observed
listing:

- SRP silver rows include `artifact_id`, `listing_id`, resolved `vin`, canonical
  detail URL, price, make/model/trim/year, mileage, MSRP, stock/fuel/body style,
  financing fields, seller fields, page/position, `trid`, `isa_context`,
  `listing_state='active'`, `source='srp'`, and `fetched_at`.
- Carousel silver rows include `artifact_id`, `listing_id`, resolved `vin` when
  known, canonical detail URL, price, mileage, body, condition, year, inherited
  dealer fields, `listing_state='active'`, `source='carousel'`, and
  `fetched_at`.
- Detail silver rows remain the richest canonical state source and include
  listing state, full vehicle fields, and dealer/customer fields.

Add a new model before treating the feature-store chain as semantically complete:

```text
int_listing_observation_fingerprints
```

Target grain:

```text
artifact_id + listing_id
```

Implementation notes:

- Build from `stg_observations`.
- Include `source in ('detail', 'srp', 'carousel')`.
- Require `listing_id is not null`; keep `vin17` when present/resolved, but do
  not drop rows solely because VIN is null.
- Generate a stable `observation_id`, preferably
  `md5(concat_ws('|', artifact_id, listing_id))`, unless production uniqueness
  checks show `artifact_id + listing_id` is insufficient and requires a source or
  card-position component.
- Use `observation_id` or the composite equivalent as the incremental
  `unique_key`, not bare `artifact_id`.
- Preserve source-aware fields in the fingerprint. Common fields should include
  `listing_id`, `vin17`, `source`, `price`, `mileage`, `year`, `make`, `model`,
  `trim`, and `listing_state`. Detail/SRP fields should include stock, fuel,
  body style, MSRP, and relevant dealer/seller IDs. Carousel fields should
  include body, condition, year, price, mileage, and inherited dealer/customer
  context.
- Add uniqueness tests for the row key and unit/integration tests showing an SRP
  artifact with multiple listings and a carousel artifact with multiple listings
  produce multiple stable fingerprint rows without key collisions.

**Implemented (2026-07-10):** `int_listing_observation_fingerprints` now exists
(`dbt/models/intermediate/int_listing_observation_fingerprints.sql`) exactly as
specified above — `observation_id = md5(concat_ws('|', artifact_id, listing_id))`,
`unique_key='observation_id'`, `incremental_strategy='delete+insert'`, watermark
lookback via `listing_observation_fingerprint_lookback_days` (default 3).
`stg_observations` was extended to surface the carousel-only `body` and
`condition` columns (previously present in the silver source but not selected)
so the fingerprint hash can include them. Coverage: dbt unit tests in
`unit_tests.yml` (all-three-sources inclusion, null-listing_id exclusion,
multi-listing SRP/carousel key collisions) plus
`tests/integration/dbt/test_observation_fingerprints_real_build.py`, which runs
the **real** dbt project against the **real** shared lake-snapshot fixture
(`scripts/seed_lake_snapshot_fixture.py`) rather than a throwaway
dbt-duckdb shadow project.

Testing-approach correction (2026-07-10, same day): an earlier version of this
work added a fourth throwaway per-file dbt-duckdb project test (own CSV seed,
own stubbed `stg_observations`), mirroring `test_fingerprints_incremental.py`.
Review concluded that pattern — while it works — creates a second fake
universe per model (fake source shape, fake project setup, separate fixture
data/expectations) that is a maintenance tax as the model graph grows, and
that this repo already has a stronger mechanism for this: the shared MinIO
fixture (`scripts/seed_lake_snapshot_fixture.py`) that
`test_selector_dbt_equivalence.py` and the archiver selector/cohort tests run
the real dbt project against. That fixture was extended with **phases**:
`seed(phase="base")` (unchanged content, plus new SRP/carousel
multi-listing-per-artifact scenarios) and
`seed(phase="observation_fingerprint_incremental")` (silver-only rows written
under distinct filenames, landing alongside — not over — the base phase's
files). `test_observation_fingerprints_real_build.py` seeds the base phase
(already done by CI before this test runs), asserts on the real materialized
table, then seeds the incremental phase, reruns
`dbt build --select int_listing_observation_fingerprints` against the same
DuckDB file with no `--full-refresh` (exercising late-arrival lookback and
observation_id replace-on-correction for real), asserts again, reruns once
more to confirm idempotency, then does a final `--full-refresh` build and
confirms equivalence. The throwaway per-file CSV pattern used by
`test_fingerprints_incremental.py` / `test_price_history_incremental.py` /
`test_listing_state_runs_incremental.py` was **not** touched or removed in
this pass — it may still be useful while Phase 2-4 incrementalization is
actively being developed, but should be treated as temporary scaffolding: once
the phased shared-fixture mechanism proves itself further, revisit those
three and either demote them to a small smoke set or delete the parts fully
covered by the phased fixture.

**Shadow test removal (2026-07-10, follow-up commit):** the three throwaway
dbt-duckdb shadow-project tests flagged above as temporary scaffolding —
`test_fingerprints_incremental.py`, `test_price_history_incremental.py`,
`test_listing_state_runs_incremental.py` — were deleted and their coverage
ported to the same phased shared-fixture pattern
`test_observation_fingerprints_real_build.py` established. Two changes made
this possible:

- `scripts/seed_lake_snapshot_fixture.py` gained three more phases, each a
  second wave of rows seeded only after the base phase has already been
  built once, written under distinct filenames so they land alongside (not
  over) the base phase's and each other's files:
  - `detail_fingerprint_incremental` — silver rows for
    `int_listing_state_fingerprints`: a late-arriving artifact
    (`ARTIFACT_FP_LATE`), a correction with a later `fetched_at`
    (`ARTIFACT_FP_DUP`), and a same-batch duplicate artifact_id
    (`ARTIFACT_FP_RETRY`).
  - `price_history_incremental` — price-event rows for `int_price_history`:
    a late event and a new event for `VIN_PH_AFFECTED`, both inside the
    lookback window, proving the affected-VIN replacement rereads that VIN's
    complete history; `VIN_PH_STABLE` is a never-touched control.
  - `listing_state_runs_incremental` — silver rows for
    `int_listing_state_runs` (via its upstream
    `int_listing_state_fingerprints`): a late artifact splitting
    `VIN_RUNS_A`'s single run into three, and a correction merging
    `VIN_RUNS_B`'s three runs into one; `VIN_RUNS_STABLE` is a never-touched
    control.
- `tests/integration/dbt/test_incremental_models_real_build.py` added one
  test function per model, following the same shape as
  `test_observation_fingerprints_real_build.py`: assert on the base-phase
  real materialized output, seed the phase-2 wave, rerun
  `dbt build --select <model>` with no `--full-refresh` against the same
  DuckDB file and assert the incremental behavior, rerun once more to confirm
  idempotency, then `--full-refresh` and confirm equivalence. The
  `int_listing_state_runs` test rebuilds both `int_listing_state_fingerprints`
  and `int_listing_state_runs` together (`--select int_listing_state_fingerprints
  int_listing_state_runs`), since runs depends on the fingerprints chain.

The `days_on_market`-tracking coverage from the old `int_price_history` shadow
test (a stub downstream model recomputing it from `first_seen_at` at
query time, checked across separate dbt invocations with later as-of dates)
was not ported: that behavior lives in the real `mart_vehicle_snapshot`, not
in a fixture-only stand-in, so it's out of scope for this fixture. What was
ported is the simpler, still-load-bearing assertion that
`int_price_history` itself never re-exposes `days_on_market` as a column.

Correction (2026-07-10, same day): review found the original phase-2 fixture's
"corrected observation" scenario actually tested a *newer fetch* (a later
`fetched_at` for the same artifact_id/listing_id superseding the old one), not
a true reprocessing correction (same `fetched_at`, re-landed later). Since the
model's dedupe only ordered by `fetched_at desc, parsed_fingerprint`, two rows
sharing an identical `fetched_at` had no defined tiebreak. Fixed by surfacing
`written_at` (already present in the silver source, not previously selected)
through `stg_observations`, adding it as a `fetched_at desc, written_at desc`
tiebreaker in the model's dedupe `row_number()` (excluded from
`parsed_fingerprint` itself — it's processing metadata, not business state),
and adding a dedicated fixture scenario
(`ARTIFACT_OBSFP_CORRECTION`/`LISTING_OBSFP_CORRECTION`) plus a dbt unit test
(`test_observation_fingerprints_written_at_breaks_fetched_at_tie`) that pins
two same-`fetched_at` rows differing only in `written_at` and asserts the
later one wins. Residual limitation, now documented on the model: the
incremental rescan window itself is still `fetched_at`-based, so a correction
whose `fetched_at` already fell outside the lookback window before the
correction landed is invisible to incremental runs until the next
`--full-refresh`. `int_listing_state_fingerprints` has the same
`fetched_at`-only limitation and was not changed in this pass.

VM verification of steady-state scan volume is still pending, same as the
rest of Phase 2/2b.

`int_listing_state_fingerprints` remains unchanged and is still the canonical
detail-only state model — it is not being replaced or removed in this pass.
`int_listing_observation_fingerprints` is additive: it is the new all-source
base layer intended for cadence learning, sitting alongside (not instead of)
the detail-only model. Downstream, `int_listing_state_runs` and
`int_listing_volatility_features` still read from the detail-only fingerprint
model as of this pass; see "Downstream impact" below for the follow-up this
creates.

Downstream impact:

- `int_listing_state_fingerprints` can remain as a detail-only canonical-state
  subset if useful for detail-specific semantics.
- `int_listing_state_runs` should be reviewed after this model lands. It may
  either continue to model detail-only canonical state runs, or be replaced by /
  paired with an all-source `int_listing_observation_runs` model for cadence
  learning.
- `int_listing_volatility_features` should eventually consume the all-source
  observation/cadence layer so SRP and carousel refreshes are visible to the ML
  trainer.

## Phase 3: Incremental Price History

Convert `int_price_history` using affected-VIN replacement.

Algorithm:

1. Identify VINs with new or corrected price events inside the watermark
   lookback.
2. Read complete price history only for those VINs.
3. Recompute all aggregate and `lag()`-derived fields for those VINs.
4. Delete and replace those VIN rows atomically.

Do not increment counters from only the new batch. Consecutive-price logic
depends on the event immediately before the incremental boundary.

Tests:

- new VIN;
- additional event for existing VIN;
- late event inserted between existing events;
- duplicate event;
- price correction;
- drop/increase counts across the watermark boundary;
- incremental/full-refresh equivalence.

### Phase 3 progress (2026-07-09)

Implemented in `feature/plan-123-affected-vin-incrementals` alongside Phase 4.

- [x] `dbt/models/intermediate/int_price_history.sql` converted to
      `materialized='incremental'`, `unique_key='vin'`,
      `incremental_strategy='delete+insert'` — the same base strategy as
      `int_listing_state_fingerprints` (Phase 2), for the same portability
      reasons.
- [x] Affected-VIN replacement: an `affected_vins` CTE selects distinct VINs
      from `stg_price_events` with `event_at >= max(target.last_seen_at) -
      price_history_incremental_lookback_days` (new var, default `3`). A
      `history` CTE then rereads **all** `stg_price_events` rows for those
      VINs (not just the rows inside the lookback window) before the
      existing `LAG()`-based aggregation runs, so drop/increase counts and
      all aggregates are recomputed from complete history — never from only
      the new batch. First run and `--full-refresh` skip both filters and
      scan the full source, matching prior full-table behavior.
- [x] **days_on_market correction (option 1 from the plan)**: the old
      `datediff('day', min(event_at), now())` was time-relative and would go
      stale for VINs the affected-VIN logic stops reprocessing every run.
      `int_price_history` no longer computes or exposes `days_on_market` at
      all — only the stable, event-derived `first_seen_at`/`last_seen_at`
      remain. `days_on_market` is now computed downstream in
      `dbt/models/marts/mart_vehicle_snapshot.sql` as
      `datediff('day', ph.first_seen_at, {{ now_ts() }})`. Since
      `mart_vehicle_snapshot` is a plain `materialized='table'` model that
      fully rebuilds on every `hourly_core` run regardless of which VINs
      `int_price_history` touched, this keeps `days_on_market` fresh for
      every VIN every hour. `int_price_history.schema.yml`'s
      `not_null`/data-test on `days_on_market` was removed along with the
      column. No other downstream model reads `int_price_history.days_on_market`
      directly — `mart_deal_scores` only reads it via `mart_vehicle_snapshot`,
      and `int_listing_volatility_features` already computed its own
      `listing_days_on_market` from `first_seen_at` rather than using
      `int_price_history.days_on_market`, so this correction did not need to
      touch it.
- [x] New var `price_history_incremental_lookback_days` (default `3`) added to
      `dbt/dbt_project.yml`, matching the `fingerprint_incremental_lookback_days`
      convention.
- [x] The four existing `int_price_history` dbt unit tests in
      `dbt/models/intermediate/unit_tests.yml` got an explicit
      `overrides: macros: is_incremental: false`, matching the convention set
      by the Phase 2 fingerprints unit tests (dbt's unit test framework does
      not evaluate `is_incremental()` as true regardless, but the override
      documents that intent explicitly). None of these tests assert on
      `days_on_market`, so removing the column did not require test changes
      beyond the override.
- [x] Added `tests/integration/dbt/test_price_history_incremental.py`: a
      throwaway dbt-duckdb project seeding a `stg_price_events` stand-in and
      running the real model SQL through real `dbt seed`/`dbt run`
      invocations. Covers: bootstrap, idempotent rerun, a new VIN appended
      without disturbing existing rows, an additional event recomputing an
      existing VIN's aggregates, a late event inserted between existing
      events reordering the `LAG()`-derived drop/increase sequence, a
      duplicate event preserving the pre-incremental model's behavior
      (duplicates were never deduplicated), a VIN with an old event outside
      the lookback plus a new event inside it proving drop/increase counts
      are computed from complete history across the watermark boundary, and
      `--full-refresh` equivalence. Also includes a dedicated stub downstream
      model mirroring the real `mart_vehicle_snapshot` days_on_market fix, to
      prove `days_on_market` keeps advancing correctly across separate dbt
      invocations with later as-of dates, independent of whether
      `int_price_history` reprocessed that VIN on a given run. Verified
      locally against `dbt-core==1.10.20`/`dbt-duckdb==1.10.1` (matching CI's
      pinned versions).

      **Superseded (2026-07-10):** this throwaway-project test was deleted and
      its coverage ported to the shared-fixture pattern (except the
      downstream `days_on_market` stub model, which isn't needed against the
      real project — `days_on_market`'s absence from `int_price_history` is
      asserted directly via `information_schema.columns`) — see "Shadow test
      removal" below.

Still needs VM verification — see the combined Phase 3/4 verification list at
the end of the Phase 4 section below.

## Phase 4: Incremental Listing-State Runs

Convert `int_listing_state_runs` only after fingerprints are stable.

Important correction after the 2026-07-10 Phase 2 review: the Phase 4 work
implemented affected-VIN replacement over the current detail-only
`int_listing_state_fingerprints` model. That is still useful for detail-state run
correctness and resource reduction, but it should not be treated as the final
cadence feature-store path until Phase 2b's all-source
`int_listing_observation_fingerprints` model exists and the run/feature models
are pointed at the right base layer.

Algorithm:

1. Identify VINs changed by incremental fingerprint input.
2. Read all fingerprint history for those VINs.
3. Recompute complete gaps-and-islands runs for those VINs.
4. Replace all target runs for the affected VINs.

This avoids trying to mutate only the open run, which is unsafe when late events
can split historical runs or alter `lead()`-derived fields.

Tests:

- append extends open run;
- append opens new run;
- relisting opens new run;
- late event splits an existing run;
- late event merges what were previously separate runs;
- `next_state_started_at`, `hours_until_change`, and `is_open_run` remain correct;
- incremental/full-refresh equivalence.

### Phase 4 progress (2026-07-09)

Implemented in `feature/plan-123-affected-vin-incrementals`, depending on
Phase 2's `int_listing_state_fingerprints` incremental conversion.

- [x] `dbt/models/intermediate/int_listing_state_runs.sql` converted to
      `materialized='incremental'`, `unique_key='vin17'`,
      `incremental_strategy='delete+insert'`. `vin17` here is an **entity
      replacement key**, not a row-unique key — the model's grain is still
      multiple runs per `vin17`. `delete+insert` deletes every existing
      target row for each affected `vin17` and reinserts its complete
      recomputed run history; no `unique` data test was added on `vin17`
      (this is called out explicitly in both the model SQL and
      `int_listing_state_runs.schema.yml`, since multiple runs per VIN are
      expected by design).
- [x] Affected-VIN replacement: an `affected_vins` CTE selects distinct
      `vin17` from `int_listing_state_fingerprints` with `fetched_at >=
      max(target.run_ended_at) - listing_state_runs_incremental_lookback_days`
      (new var, default `3`). The `ordered` CTE (the original gaps-and-islands
      `LAG()` step) then joins against `affected_vins` to pull that VIN's
      **entire** fingerprint history — not just fingerprints inside the
      lookback — so a late or corrected fingerprint anywhere in a VIN's
      history triggers a full gaps-and-islands recompute for that VIN, which
      is required to correctly split or merge runs. First run and
      `--full-refresh` skip the filter and join, scanning the full
      fingerprints table.
- [x] The rest of the gaps-and-islands SQL (`flagged`, `numbered`,
      `collapsed`, `with_lead`, final select) is unchanged from the original
      full-table model — only the `ordered` CTE's source changed.
- [x] New var `listing_state_runs_incremental_lookback_days` (default `3`)
      added to `dbt/dbt_project.yml`.
- [x] The four existing `int_listing_state_runs` dbt unit tests in
      `dbt/models/intermediate/unit_tests.yml` got an explicit
      `overrides: macros: is_incremental: false`, matching the Phase 2/3
      convention.
- [x] `int_listing_state_runs.schema.yml` documents the entity-replacement-key
      semantics of `vin17` in the model description; no schema test changes
      were needed since it already had no `unique` test on `vin17`.
- [x] Added `tests/integration/dbt/test_listing_state_runs_incremental.py`: a
      throwaway dbt-duckdb project seeding a `stg_observations` stand-in and
      running the **real SQL for both** `int_listing_state_fingerprints` and
      `int_listing_state_runs` through real `dbt seed`/`dbt run` invocations,
      since Phase 4 explicitly depends on Phase 2's incremental fingerprints
      chain rather than testing `int_listing_state_runs` in isolation.
      Covers: bootstrap, idempotent rerun, an append with the same
      fingerprint extending the open run, an append with a different
      fingerprint opening a new run, a relisting (new `listing_id`) opening a
      new run, a late fingerprint inside the lookback splitting a single run
      into three, a correction to a different VIN's fingerprint merging three
      runs back into one, `next_state_started_at`/`hours_until_change`/
      `is_open_run` correctness throughout, and `--full-refresh` equivalence.
      Uses generous lookback vars (`60` days) in its fixture project since
      lookback-window edge cases are already covered by the Phase 2 and
      Phase 3 integration tests — this test's focus is the gaps-and-islands
      recompute logic itself. Verified locally against
      `dbt-core==1.10.20`/`dbt-duckdb==1.10.1`.

      **Superseded (2026-07-10):** this throwaway-project test was deleted and
      its coverage ported to the shared-fixture pattern — see "Shadow test
      removal" below.

#### Still needs VM verification (Phases 3 and 4)

**First deploy must start with a one-time full-refresh, not a normal
incremental run.** The existing production `int_price_history` table was
built as a plain `materialized='table'` model with a `days_on_market` column
that this change removes; the existing `int_listing_state_runs` table
likewise predates its new incremental config. If the first post-deploy build
runs a normal (non-full-refresh) `dbt build`, dbt will attempt an incremental
`delete+insert` against tables with the old schema/materialization history
instead of rebuilding them under the new config — this is exactly the
mistake Phase 2's rollout called out and avoided by rebuilding first. Deploy
order:

- [ ] One-time rebuild: run
      `dbt build --select int_price_history int_listing_state_runs --full-refresh`
      (or a full-graph `--full-refresh` if simpler operationally) and confirm
      both models rebuild cleanly under the new incremental config, with
      `int_price_history` no longer having a `days_on_market` column.
- [ ] Only after that rebuild, run `dbt build --select tag:hourly_core`
      (covers `int_price_history` and its `mart_vehicle_snapshot`/
      `mart_deal_scores` dependents) as a normal incremental run and confirm
      success.
- [ ] Run `dbt build --selector feature_daily` (covers
      `int_listing_state_fingerprints`, `int_listing_state_runs`,
      `int_listing_volatility_features`) as a normal incremental run and
      confirm success.
- [ ] Run each selector a second time immediately after and confirm no
      unexpected row-count drift (idempotency under real production data).
- [ ] Run `dbt build --full-refresh` again (or the equivalent full-graph
      rebuild) later and confirm both models still fully rebuild correctly —
      this is the ongoing full-refresh recovery path, not just the one-time
      migration step above.
- [ ] Check grain/uniqueness on the real tables:
      `select count(*), count(distinct vin) from int_price_history;` and
      confirm `int_listing_state_runs` still has multiple rows per `vin17`
      as expected (no accidental collapse to one row per VIN).
- [ ] Spot-check real `days_on_market` values in `mart_vehicle_snapshot` for a
      few known-old listings against their `first_seen_at`, to confirm the
      downstream computation is live in production and not still returning
      stale values from a cached/old build.
- [ ] Compare `int_price_history` and `int_listing_state_runs` runtime in
      `run_results.json` against their previous full-table build times to
      confirm steady-state scan volume is actually lower.

## Phase 5: Reassess Downstream Models

Profile the graph again after Phases 2-4.

### Current deployed status (2026-07-10)

Phases 2b, 3, and 4 are implemented and merged to `master`
(`feature/plan-123-affected-vin-incrementals`, PR #150). Initial VM
verification and Phase 5 profiling were collected on 2026-07-10; longer-term
trend checks remain open. This means:

- `int_listing_observation_fingerprints` exists at the all-source
  `artifact_id + listing_id` grain (~37.8M rows locally verified, 0
  duplicate `observation_id`, `detail`/`srp`/`carousel` all present), but has
  **no downstream consumer yet** — nothing reads it. It sits alongside the
  detail-only `int_listing_state_fingerprints`.
- `int_price_history` is incrementalized by affected VIN.
- `int_listing_state_runs` is incrementalized by affected VIN, but its input
  is still the detail-only `int_listing_state_fingerprints`, not the
  all-source observation fingerprints.
- `int_listing_volatility_features` is unchanged: full-table, and still
  consumes the detail-only state/run chain, so SRP/carousel refresh signals
  are not yet visible to the ML feature trainer.
- `int_latest_observation` is incrementalized by affected VIN (this PR — see
  "Phase 5 hourly_core optimization: int_latest_observation" below).
  `int_benchmarks` and `mart_vehicle_snapshot` are unchanged full-table
  builds.
### Phase 5 measurement update (2026-07-10)

Initial production resource measurements were collected in
`docs/plan_123_dbt_resource_baseline.md`. `hourly_core` ran in roughly 62-70s
and is dominated by `int_latest_observation` and `mart_scrape_volume`;
`feature_daily` ran in roughly 47s and is dominated by
`int_listing_volatility_features` and
`int_listing_observation_fingerprints`. The collection window showed no
dbt-runner OOM, no restart, and a 3.3G DuckDB file.

These numbers are enough to prioritize the next candidate, but not enough to
call a long-term trend. The next Phase 5 step should choose between reducing
hourly operational runtime (`int_latest_observation` / `mart_scrape_volume`)
and improving feature-store correctness (`int_listing_observation_runs` /
`int_listing_volatility_features` consuming all-source observations).

### Do not incrementalize yet without evidence

Do not incrementalize a model merely because it is expensive, and do not
convert a model merely because a lower layer just became incremental. Every
Phase 5 candidate needs both of the following before a conversion commit is
opened, not just one:

1. Runtime/resource evidence from `docs/plan_123_dbt_resource_baseline.md`
   showing the model is actually a meaningful share of `hourly_core` or
   `feature_daily` runtime (via `scripts/report_dbt_run_results.py`) or a
   meaningful share of DuckDB file growth/scan volume.
2. A concrete update key that covers every way the model's output can
   change. A fast model with a clean update key is not worth converting yet
   (low payoff); a slow model with an update key that can't cover all change
   paths is not safe to convert regardless of payoff (see the
   `int_benchmarks` and `int_listing_volatility_features` notes below).

### Questions Phase 5 must answer

- What is `hourly_core`'s and `feature_daily`'s actual production runtime and
  per-model breakdown post-Phase-2b/3/4 (not pre-Phase-0 incident numbers)?
- Which models dominate each selector's runtime and DuckDB scan volume now
  that fingerprints, price history, and state runs are incremental?
- Does `int_listing_observation_fingerprints`' all-source coverage
  (`detail`/`srp`/`carousel` row counts) hold up in production the way it
  does against the local lake-snapshot fixture?
- Is the detail-only `int_listing_state_runs` path materially cheaper than a
  hypothetical all-source `int_listing_observation_runs` would be, or is the
  gap small enough that correctness (capturing SRP/carousel cadence signal)
  should win regardless of the runtime delta?
- For `int_latest_observation`: does "latest per VIN" have a clean
  affected-VIN replacement key, and does source-priority/late-arrival logic
  survive being scoped to only affected VINs, or does it require full
  visibility across VINs to resolve priority correctly?
- For `int_benchmarks`: a single changed VIN can shift a make/model
  percentile for every other VIN in that group — does an affected
  make/model-group replacement actually bound the recompute, or does it
  still require scanning most of the table on any change (in which case
  incrementalizing adds complexity without reducing scan volume)?
- For `int_listing_volatility_features`: how much of its cost is
  event-derived (would benefit from affected-VIN replacement) versus
  time-relative (ages independent of new events, and can't be fixed by an
  update key at all — see the `int_price_history` `days_on_market` fix in
  Phase 3, which moved a time-relative field out of an incremental model
  entirely rather than trying to incrementalize it)?
- Which of the above, if any, is more appropriately deferred to Plan 118's
  Spark/Delta migration rather than solved twice (once here in DuckDB, once
  again in Spark)?

### Candidate decisions

a. **Add `int_listing_observation_runs` over all-source observation
   fingerprints, or point `int_listing_state_runs` at them?** Not yet
   decided. Requires: (1) resource evidence that the all-source grain's
   gaps-and-islands recompute is affordable at `hourly_core`/`feature_daily`
   scale, and (2) a semantic decision on whether SRP/carousel-driven "runs"
   mean the same thing as detail-driven canonical-state runs, or whether they
   need distinct modeling (e.g. a run boundary defined by price/visibility
   change instead of full canonical state change).

b. **Should `int_listing_volatility_features` consume all-source runs?**
   Directionally yes per the Phase 2b "Downstream impact" note (SRP/carousel
   refreshes should be visible to the ML trainer), but blocked on (a) landing
   first, and on splitting out the time-relative feature components per the
   question above so incrementalizing the event-derived parts doesn't produce
   silently-stale time-relative ones.

c. **Are `int_latest_observation` or `int_benchmarks` worth
   incrementalizing?** Likely lower priority than (a)/(b) until resource
   evidence says otherwise — both currently have no confirmed production
   cost problem, and `int_benchmarks`' group-recompute fan-out (one VIN
   changing many percentiles) needs a scoping analysis before assuming an
   affected-group replacement actually reduces scan volume.

**2026-07-10 priority update:** `int_latest_observation` is now a confirmed
`hourly_core` cost center and should be investigated as a near-term candidate
if its affected-VIN update key covers source-priority and late-arrival
behavior. `int_benchmarks` remains lower priority: `mart_deal_scores` needs
it hourly, but the benchmark table itself is small in row count and its
group-recompute fan-out still needs a scoping analysis before assuming
affected-group replacement would reduce scan volume.

d. **What should defer to Plan 118 Spark/Delta?** Any time-relative feature
   computation that can't be fixed by an update key at all (see
   `int_listing_volatility_features` above), and any modeling question whose
   answer would change under a different execution engine's cost model
   (e.g. whether affected-group replacement is worth the complexity is a
   DuckDB-scan-cost question that may simply not apply once Spark/Delta
   handles partition pruning natively). Do not build DuckDB-specific
   incremental machinery for a model that Plan 118 is likely to reimplement
   soon after — check the Plan 118 model list before starting (a)/(b)/(c)
   implementation work.

### Phase 5 hourly_core optimization: mart_scrape_volume (2026-07-10)

First implementation chunk against the two questions above
(`int_latest_observation` vs. `mart_scrape_volume`, both confirmed
`hourly_core` cost centers by the Phase 5 baseline). `mart_scrape_volume` was
chosen first over `int_latest_observation` because it satisfies both Phase 5
conversion criteria cleanly:

1. Resource evidence: `docs/plan_123_dbt_resource_baseline.md` shows it at
   ~27-30s, one of the two dominant costs in a 62-70s `hourly_core` run.
2. Update key: its grain is a clean `(hour, source)` aggregate over
   `stg_observations.fetched_at` — a fixed, non-mutating dimension. Once an
   hour has fully elapsed its aggregate can never change again except from a
   late-arriving row for that same hour, which a `fetched_at`-based lookback
   window catches by construction. There is no cross-row priority or
   late-arrival ambiguity to resolve, unlike `int_latest_observation`.

`int_latest_observation` was **not** converted in the same commit as
`mart_scrape_volume` — its "latest row per VIN" semantics depend on
source-priority (detail beats SRP/carousel) and late-arrival ordering across
potentially any historical VIN, not a fixed time dimension, so it needed its
own analysis before a conversion commit was safe to open. That analysis and
implementation followed in this same PR — see "Phase 5 hourly_core
optimization: int_latest_observation" below.

Implementation:

- `dbt/models/marts/mart_scrape_volume.sql` converted to
  `materialized='incremental'`, `incremental_strategy='delete+insert'`,
  `unique_key='scrape_volume_key'` — a synthetic surrogate
  `md5(concat_ws('|', hour, source))` for the `(hour, source)` composite
  grain, following the same synthetic-key approach used elsewhere in this
  repo where dbt-duckdb's `delete+insert` needs a single-column match target.
- New var `scrape_volume_incremental_lookback_hours` (default `72`,
  `dbt/dbt_project.yml`) — on an incremental run, `stg_observations` is
  filtered to a contiguous recent-hour window:
  `date_trunc('hour', fetched_at) >= max(target.hour) - lookback_hours`
  (a recent-window replacement, not a sparse per-row affected-hour lookup).
  ALL rows in that window are reread and every metric recomputed from
  scratch for each `(hour, source)`, not just rows newer than the previous
  run's watermark, so a late-arriving row landing partway through an
  already-built hour still produces a correct full-hour aggregate. First run
  and `--full-refresh` skip the filter and scan the full source, matching the
  prior full-table behavior exactly.
- Metric expressions (`artifact_count`, `observation_count`,
  `unique_listings`, `valid_vin_count`, `vin_extraction_pct`) are
  byte-for-byte unchanged from the original full-table model.
- `dbt/models/marts/mart_scrape_volume.schema.yml`: added `scrape_volume_key`
  with `not_null`/`unique` tests, documented the incremental/lookback
  behavior. `hourly_core` tag untouched.
- The three existing `mart_scrape_volume` dbt unit tests in
  `dbt/models/marts/unit_tests.yml` got an explicit
  `overrides: macros: is_incremental: false`, matching the Phase 2-4
  convention (dbt's unit test framework doesn't evaluate `is_incremental()`
  as true regardless, but the override documents that intent explicitly).
- `scripts/seed_lake_snapshot_fixture.py` gained a `scrape_volume_incremental`
  phase: a base-phase row in `SV_AFFECTED_HOUR` (inside the default 72-hour
  lookback of the base fixture's global max hour) plus a stable control row
  in `SV_STABLE_HOUR` (outside the lookback), then a phase-2 wave adding a
  second, invalid-vin row to the SAME affected hour (proving the whole hour
  is recomputed, not incremented) and a brand-new `(hour, source)` row in
  `SV_NEW_HOUR`.
- `tests/integration/dbt/test_incremental_models_real_build.py`:
  `test_scrape_volume_incremental_real_build_scenario`, following the same
  base-phase-assert / seed-phase-2 / rebuild / idempotency /
  full-refresh-equivalence shape as the other three tests in that module.

### Phase 5 hourly_core optimization: int_latest_observation (2026-07-10)

Second implementation chunk against the two Phase 5 hourly_core candidates
(`int_latest_observation` and `mart_scrape_volume`, converted in the same PR).
`int_latest_observation` was deferred behind `mart_scrape_volume` because its
update key needed a correctness argument, not just a scan-volume one:

1. Resource evidence: `docs/plan_123_dbt_resource_baseline.md` shows it at
   ~26-31s, the other dominant cost in a 62-70s `hourly_core` run alongside
   `mart_scrape_volume`.
2. Update key: `int_latest_observation` is **not** simply "latest row per
   VIN" — its ranking is source-priority first (`detail` > `srp` > `carousel`),
   recency second. An older `detail` observation can and does beat a newer
   `srp`/`carousel` one. This means a naive "only look at recent rows"
   incremental scan would be wrong: if a VIN's winning row is an old detail
   observation and a new, lower-priority row lands, filtering candidates to
   only the lookback window would incorrectly promote the new row (or drop
   the VIN's true winner) because the old detail row wouldn't be in scope to
   out-rank it. The safe key is **affected-VIN discovery, not affected-row
   filtering**: use the lookback window only to decide *which VINs changed*,
   then reread and rerank each affected VIN's **entire** observation history
   from `stg_observations`, exactly like `int_price_history` (Phase 3) already
   does for its LAG()-derived aggregates. This is the same pattern
   `int_price_history` and `int_listing_state_runs` established, applied here
   because source-priority ranking has the same "recompute needs full
   context" property that LAG() and gaps-and-islands ordering do.

Implementation:

- `dbt/models/intermediate/int_latest_observation.sql` converted to
  `materialized='incremental'`, `incremental_strategy='delete+insert'`,
  `unique_key='vin17'`. An `affected_vins` CTE selects distinct `vin17` from
  `stg_observations` where `fetched_at >= max(target.fetched_at) -
  latest_observation_incremental_lookback_days` (discovery only, no source or
  make filter, since a low-priority or NULL-make row can still be the signal
  that a VIN changed). A `candidates` CTE then inner-joins on affected VINs
  (full-table on first run / `--full-refresh`) before the unchanged
  `row_number()` ranking (source-priority, `fetched_at desc`, `artifact_id
  desc`) and `make is not null` filter run over that VIN's complete history —
  not just the newly-discovered rows. The ranking logic itself is
  byte-for-byte unchanged from the original full-table model.
- New var `latest_observation_incremental_lookback_days` (default `3`,
  `dbt/dbt_project.yml`), matching `int_price_history`'s and
  `int_listing_state_runs`' lookback var naming/default.
- `dbt/models/intermediate/int_latest_observation.schema.yml`: documented the
  affected-VIN replacement and the source-priority caveat inline (existing
  `vin17`/`source`/`make` tests untouched).
- The five existing `int_latest_observation` dbt unit tests in
  `dbt/models/intermediate/unit_tests.yml` got an explicit `overrides: macros:
  is_incremental: false`, matching the Phase 2-4/mart_scrape_volume
  convention.
- `scripts/seed_lake_snapshot_fixture.py` gained a
  `latest_observation_incremental` phase: base rows for `VIN_LO_PRIORITY`
  (older detail winner) and `VIN_LO_DETAIL_UPGRADE` (older detail winner) plus
  a stable control `VIN_LO_STABLE`, then a phase-2 wave adding a newer but
  lower-priority SRP row to `VIN_LO_PRIORITY` (must NOT win), a newer same-tier
  detail row to `VIN_LO_DETAIL_UPGRADE` (must win), and a first-ever row for a
  brand-new `VIN_LO_NEW` (must appear).
- `tests/integration/dbt/test_incremental_models_real_build.py`:
  `test_latest_observation_incremental_real_build_scenario`, covering: (a)
  source priority beats recency after a full-history reread, (b) same-tier
  recency wins, (c) a late-arriving new VIN is picked up, (d) an unaffected
  VIN is unchanged, (e) idempotency on a repeated incremental run, and (f)
  full-refresh equivalence over the same final data.

Still needs VM verification (both `mart_scrape_volume` and
`int_latest_observation`):

- [ ] First deploy must run **both**
      `dbt build --select mart_scrape_volume --full-refresh` and
      `dbt build --select int_latest_observation --full-refresh` once before
      any normal incremental `tag:hourly_core` run — both existing
      production tables predate their incremental config, so a plain
      incremental build against either would attempt `delete+insert` against
      a table dbt has no watermark history for (same rollout hazard already
      documented for Phases 3/4).
- [ ] Run `dbt build --select tag:hourly_core` (normal incremental) after
      both rebuilds and confirm success.
- [ ] Run it again immediately after and confirm no row-count drift for
      either model (idempotency under real production data).
- [ ] Check grain/uniqueness on the real tables:
      `select count(*), count(distinct scrape_volume_key) from mart_scrape_volume;`
      and
      `select count(*), count(distinct vin17) from int_latest_observation;`
- [ ] Compare `mart_scrape_volume`'s runtime against the Phase 5 baseline
      (~27-30s) and `int_latest_observation`'s runtime against its Phase 5
      baseline (~26-31s) in `run_results.json` to confirm steady-state scan
      volume is actually lower for both.

## Phase 6: Recovery and Drift Controls

Incremental pipelines require explicit recovery behavior.

Add:

- scheduled incremental/full-refresh equivalence checks on a bounded fixture;
- row-count and uniqueness assertions after every incremental run;
- watermark and affected-entity metrics;
- a documented full-refresh maintenance procedure;
- target backup/restore or atomic replacement behavior before destructive
  rebuilds;
- alerts for unexpectedly large affected-VIN sets;
- a mechanism to force selected VINs or date ranges through recomputation.

## Resource Baseline Report

Capture before/after measurements for:

- total runtime;
- per-model runtime;
- peak dbt RSS;
- host peak memory;
- MinIO read volume;
- DuckDB file growth;
- rows scanned or affected where available;
- hourly and daily DAG duration;
- behavior under a simultaneous production scrape workload.

Store the report in:

```text
docs/plan_123_dbt_resource_baseline.md
```

## Rollout Order

1. Deploy two threads and DuckDB memory limit.
2. Verify one complete build under monitoring.
3. Split hourly and daily model selections.
4. Convert detail fingerprints as an initial resource-reduction step.
5. Add all-source listing observation fingerprints at `artifact_id + listing_id`
   grain.
6. Convert price history.
7. Convert state runs, or revise the existing state-run work to consume the
   correct detail-only vs all-source base layer.
8. Re-profile before touching downstream feature models.
9. Add periodic full-refresh equivalence validation.

Each incremental model should be its own reviewable commit or PR gate.

## Acceptance Criteria

- No dbt-triggered host OOM during seven consecutive days of scheduled runs.
- Hourly analytics and snapshot-worker cannot overlap.
- Hourly DAG executes only its documented model subset.
- Fingerprints, price history, and state runs pass incremental/full-refresh
  equivalence tests.
- Late-arriving events are covered by tests and documented watermark policy.
- A manual full refresh remains possible and has a safe runbook.
- Feature-store freshness expectations are explicit.
- Resource measurements demonstrate lower steady-state peak memory and source
  scan volume.
- The implementation does not add new DuckDB-specific business semantics that
  would impede Plan 118's Spark/Delta migration.

