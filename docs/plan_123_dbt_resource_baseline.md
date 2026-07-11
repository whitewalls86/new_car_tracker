# Plan 123 Resource Baseline Report

This is the report artifact referenced by "Resource Baseline Report" in
`docs/plan_123_dbt_incrementalization_and_resource_governance.md`, and the
evidence base Phase 5 must use before deciding whether to incrementalize any
more models. It is a template plus an operator checklist until real numbers
are collected on the VM.

Do not skip straight to converting `int_latest_observation`,
`int_benchmarks`, or `int_listing_volatility_features` because they "seem
expensive" — fill in this report first. Phase 5's explicit rule is: do not
incrementalize a model merely because it is expensive; the update key must
cover every way its output can change. This report supplies the runtime
evidence half of that decision; the "does the update key actually cover every
change" half still requires reading the model, not a metric.

## How to collect each measurement

Run these against the deployed VM (`ssh` per `reference_server_ssh.md`), not
locally — DuckDB file size, MinIO read volume, and host memory only mean
something against production data volume.

### 1. dbt invocation duration

```bash
docker exec -it cartracker-dbt-runner dbt build --selector hourly_core
docker exec -it cartracker-dbt-runner dbt build --selector feature_daily
```

Or via `dbt_runner`'s `/dbt/build` HTTP API, which already returns
`duration_seconds`, `duckdb_threads`, `duckdb_memory_limit`, `full_refresh`,
and `likely_oom` in its JSON response (`dbt_runner/app.py`) — prefer this
path when checking production runs, since Airflow triggers builds this way
and the response is also logged via the JSON logger.

### 2. Per-model timings from run_results.json

```bash
docker exec -it cartracker-dbt-runner python scripts/report_dbt_run_results.py \
    --path target/run_results.json
```

This prints a table of model name / status / execution_time sorted slowest
first, using the same `target/run_results.json` that
`_model_timings_from_run_results()` in `dbt_runner/app.py` already reads.
Pass `--resource-type ""` to include tests and operations, or `--json-out`
to save the table for later comparison. Run it once after a `hourly_core`
build and once after a `feature_daily` build to get the split required
below.

### 3. Row counts / grain checks for feature models

```bash
docker exec -it cartracker-dbt-runner python -c "
import duckdb
con = duckdb.connect('/data/analytics/analytics.duckdb', read_only=True)
for model, key in [
    ('int_listing_observation_fingerprints', 'observation_id'),
    ('int_listing_state_fingerprints', 'artifact_id'),
    ('int_price_history', 'vin'),
    ('int_listing_state_runs', None),
    ('int_latest_observation', 'vin'),
    ('int_benchmarks', None),
    ('int_listing_volatility_features', 'vin17'),
]:
    total = con.execute(f'select count(*) from {model}').fetchone()[0]
    if key:
        distinct = con.execute(f'select count(distinct {key}) from {model}').fetchone()[0]
        print(model, total, distinct)
    else:
        print(model, total)
"
```

`int_listing_state_runs` and `int_benchmarks` intentionally have no
single-column uniqueness check — multiple runs per VIN and multiple
make/model rows are expected grain, not a defect.

### 4. Source coverage for observation fingerprints

```bash
docker exec -it cartracker-dbt-runner python -c "
import duckdb
con = duckdb.connect('/data/analytics/analytics.duckdb', read_only=True)
print(con.execute('''
    select source, count(*) from int_listing_observation_fingerprints
    group by 1 order by 2 desc
''').fetchall())
"
```

Expect all three of `detail`, `srp`, `carousel` present with non-trivial row
counts. If one source is at or near zero, that is a stronger reason to
investigate the writer/staging path than to convert the next model.

### 5. DuckDB file size

```bash
docker exec -it cartracker-dbt-runner ls -lh /data/analytics/analytics.duckdb
```

Track this over time (before/after each incremental conversion and each full
refresh) — steady or shrinking growth after a conversion is one signal that
affected-entity replacement is working as intended, not silently falling
back to full scans.

### 6. OOM / restart status

```bash
docker inspect cartracker-dbt-runner --format '{{.State.OOMKilled}} {{.RestartCount}}'
docker logs cartracker-dbt-runner --since 24h | grep -i -E "oom|killed|137"
```

Cross-check against the `likely_oom` field already returned by `/dbt/build`
and logged per-invocation (Phase 0 observability).

### 7. hourly_core vs feature_daily runtime comparison

Run step 1/2 for both selectors back to back and record both
`duration_seconds` values plus each selector's top 5 slowest models (step 2)
side by side in the "Measurements" table below. The two workloads have
different acceptance bars — `hourly_core` must stay well under the hourly
schedule interval with margin for scrape-time contention;
`feature_daily`/`backtest` only need to complete once a day.

## Measurements

Fill in one row per collection date. Leave cells blank rather than guessing.

| Date | Selector | duration_seconds | top 3 slowest models (name: execution_time) | DuckDB file size | OOMKilled/RestartCount | full_refresh? |
|------|----------|-------------------|-----------------------------------------------|-------------------|--------------------------|----------------|
| 2026-07-10 | hourly_core | 69.632s | `int_latest_observation`: 30.731s; `mart_scrape_volume`: 29.650s; `int_price_history`: 15.390s | 3.3G | false / 0 | false |
| 2026-07-10 | hourly_core repeat | 62.448s | `mart_scrape_volume`: 27.318s; `int_latest_observation`: 26.416s; `int_price_history`: 15.286s | 3.3G | false / 0 | false |
| 2026-07-10 | feature_daily | 46.980s | `int_listing_volatility_features`: 32.174s; `int_listing_observation_fingerprints`: 18.016s; `int_listing_state_runs`: 8.245s | 3.3G | false / 0 | false |
|      | full_validation |  |  |  |  |  |

## Source coverage snapshot

| Date | detail rows | srp rows | carousel rows |
|------|-------------|----------|----------------|
| 2026-07-10 | 5,954,833 | 1,132,697 | 30,719,623 |

## Grain checks

| Date | Model | total rows | distinct key rows (where applicable) |
|------|-------|------------|----------------------------------------|
| 2026-07-10 | `int_listing_observation_fingerprints` | 37,807,153 | 37,807,153 distinct `observation_id`; 0 duplicates |
| 2026-07-10 | `int_listing_state_fingerprints` | 5,851,997 | 5,851,997 distinct `artifact_id`; 0 duplicates |
| 2026-07-10 | `int_price_history` | 249,520 | 249,520 distinct `vin`; 0 duplicates |
| 2026-07-10 | `int_listing_state_runs` | 1,236,013 | N/A; multiple runs per `vin17` expected |
| 2026-07-10 | `int_latest_observation` | 250,224 | 250,224 distinct `vin`; 0 duplicates |
| 2026-07-10 | `int_benchmarks` | 100 | N/A; make/model benchmark grain |
| 2026-07-10 | `int_listing_volatility_features` | 247,122 | 247,122 distinct `vin17`; 0 duplicates |

## Phase 5 evidence checklist

Before deciding on any Phase 5 candidate conversion, confirm:

- [x] At least one `hourly_core` and one `feature_daily` run's
      `model_timings`/`duration_seconds` captured post-Phase-2b/3/4 deploy.
- [x] `int_listing_observation_fingerprints` source coverage confirms
      `detail`/`srp`/`carousel` are all present in production (step 4).
- [x] Candidate model's current execution_time is identified from the
      per-model table (step 2), not assumed from its name.
- [ ] Candidate model's update key has been checked against every way its
      output can change (see the per-model notes in Phase 5 of the main
      plan doc) — a fast runtime alone is not a reason to convert, and a slow
      runtime alone is not a reason either if the update key can't be made
      correct.
- [ ] DuckDB file size trend recorded across at least two measurement dates.
- [x] No open OOM/restart in the collection window.

## 2026-07-10 notes

The first post-Phase-2b/3/4 VM profiling pass shows the pipeline is
operationally stable under the current guardrails: no dbt-runner OOM, no
container restart, and a 3.3G DuckDB file. The remaining risk is not host
stability in this sample; it is deciding which downstream models are worth
further incrementalization without building DuckDB-specific complexity that
Plan 118 may replace.

`hourly_core` is dominated by `int_latest_observation` and
`mart_scrape_volume`, with `int_price_history` still visible but already
incrementalized and stable. `feature_daily` is dominated by
`int_listing_volatility_features`, followed by the new all-source
`int_listing_observation_fingerprints`.

The corrected `int_listing_volatility_features` grain check uses `vin17`
and confirms 0 duplicates.

## 2026-07-10 follow-up: mart_scrape_volume converted

`mart_scrape_volume` was converted to incremental (affected-hour replacement,
`unique_key='scrape_volume_key'`) — see "Phase 5 hourly_core optimization:
mart_scrape_volume" in the main plan doc for the full rationale and rollout
steps. After the required one-time `--full-refresh` deploy, re-run steps 1/2
above for `hourly_core` and record a new row in the Measurements table; the
before/after comparison to validate is `mart_scrape_volume`'s
`execution_time` in `run_results.json` against its 2026-07-10 baseline
(~27-30s) and `hourly_core`'s total `duration_seconds` against the 62-70s
baseline. `int_latest_observation` remains the next `hourly_core` candidate,
pending update-key analysis for its source-priority/late-arrival semantics.
