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
| `int_listing_state_fingerprints` | One row per valid detail artifact | Incremental, append-oriented | Artifact rows are naturally immutable; use a watermark plus late-arrival lookback |
| `int_price_history` | One mutable aggregate row per VIN | Incremental, replace affected VINs | New events only require full history recomputation for VINs touched by the incremental input |
| `int_listing_state_runs` | Multiple ordered runs per VIN | Incremental, replace affected VINs | New fingerprints can extend or split the open run; recompute all runs for changed VINs |
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

## Phase 2: Incremental Fingerprints

Convert `int_listing_state_fingerprints` first.

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
- [x] Model remains tagged `feature_daily` and `backtest`
      (`int_listing_state_fingerprints.schema.yml`); tags were not touched.
- [x] Added `tests/integration/dbt/test_fingerprints_incremental.py`: builds a
      throwaway dbt-duckdb project that seeds a `stg_observations` stand-in and
      runs the real model SQL (read directly from the repo) through real
      `dbt seed`/`dbt run`/`dbt run --full-refresh` invocations, since dbt unit
      tests can't exercise state across multiple invocations. Covers: empty
      target bootstrap excludes non-detail/null-vin17 rows; a second run with
      unchanged source is idempotent; a new artifact appends exactly once; a
      repeated run does not duplicate it; a late artifact inside the lookback
      window is picked up; a corrected `artifact_id` replaces its existing row
      rather than duplicating it; and `--full-refresh` output matches the
      accumulated incremental output. Verified locally against
      `dbt-core==1.10.20` / `dbt-duckdb==1.10.1` (pinned versions used in
      `dbt_runner/Dockerfile` and CI) — all 7 cases pass. Runs in CI's existing
      `pytest tests/integration/dbt/ -v -m integration` step in the `dbt` job;
      no CI workflow changes were needed.

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

## Phase 4: Incremental Listing-State Runs

Convert `int_listing_state_runs` only after fingerprints are stable.

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

## Phase 5: Reassess Downstream Models

Profile the graph again after Phases 2-4.

Decide with evidence whether to:

- incrementally replace affected VINs in `int_latest_observation`;
- incrementally replace affected make/model groups in `int_benchmarks`;
- retain daily full-table builds for benchmarks and volatility features;
- split time-relative feature computation from event-derived feature state;
- move selected feature preparation directly into the later PySpark/Delta work.

Do not incrementalize a model merely because it is expensive. The update key
must cover every way its output can change.

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
4. Convert fingerprints.
5. Convert price history.
6. Convert state runs.
7. Re-profile before touching downstream feature models.
8. Add periodic full-refresh equivalence validation.

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

