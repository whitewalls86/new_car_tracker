# Plan 112: Iceberg + MLflow Refresh Policy Backtesting

## Goal

Build a reproducible experiment substrate for adaptive detail refresh, then
simulate candidate refresh policies against historical listing timelines.

This plan combines:

1. Apache Iceberg snapshots over the cleaned/normalized Parquet lake from Plan
   110.
2. MLflow tracking for policy parameters, dataset versions, metrics, and
   artifacts.
3. Backtest replay logic that quantifies the tradeoff between fetch volume
   reduction and detection delay before production scraping changes.

Do not change production scraping in this plan.

---

## Context

Plan 110 normalizes storage layout and prepares silver/ops Parquet for Iceberg.
Plan 111 builds listing state fingerprints, state runs, volatility features, and
rule-based refresh priority outputs.

Before wiring that score into production ops, we need to know:

- Which exact dataset snapshot was used?
- Which policy parameters were evaluated?
- How many detail fetches would each policy skip?
- How much detection delay does throttling introduce?
- Which threshold and interval set gives an acceptable tradeoff?
- Does the policy reduce likely 403/block pressure?
- Can a run be reproduced later from the same data snapshot?

Iceberg answers the dataset-version question. MLflow answers the experiment
tracking question.

---

## Architecture

```text
Plan 110 normalized Parquet
        |
        v
Iceberg tables / snapshots
        |
        v
Plan 111 feature models
        |
        v
Backtest replay script/dbt models
        |
        v
MLflow run:
  params + metrics + artifacts + Iceberg snapshot IDs
```

The first policy remains rule-based and interpretable. ML model training is
still deferred until the target, labels, and quality gates are proven.

---

## Iceberg Substrate

Register or create Iceberg tables for the datasets needed by refresh
experiments:

| Logical table | Source |
|---------------|--------|
| `silver_observations` | normalized `silver/observations` from Plan 110 |
| `price_observation_events` | normalized ops event Parquet |
| `vin_to_listing_events` | normalized ops event Parquet |
| `blocked_cooldown_events` | normalized ops event Parquet |
| `listing_state_fingerprints` | Plan 111 model output |
| `listing_state_runs` | Plan 111 model output |
| `listing_volatility_features` | Plan 111 model output |
| `detail_refresh_priority` | Plan 111 model output |

For each backtest run, record:

- table name
- snapshot ID
- snapshot timestamp
- row count
- input window start/end
- source code commit SHA

### Local Catalog Choice

Use the simplest catalog that works on the current single-server setup.
Candidates:

- DuckDB Iceberg extension if it supports the required read/write pattern.
- PyIceberg with a local/sql catalog and MinIO object storage.
- A minimal file/catalog approach if full service deployment is unnecessary for
  the first experiment substrate.

The implementation should prefer operational simplicity over enterprise
completeness. The goal is reproducible backtests, not a full lakehouse platform.

---

## MLflow Tracking

Add MLflow for backtest experiment tracking.

Backend/artifact defaults:

- Backend store: existing Postgres, if practical.
- Artifact store: MinIO or local mounted volume.

Each run logs:

### Parameters

| Parameter | Description |
|-----------|-------------|
| `hot_threshold` | Min score for `hot` tier |
| `daily_threshold` | Min score for `daily` tier |
| `cool_threshold` | Min score for `cool` tier |
| `hot_interval_hours` | Fetch interval for hot tier |
| `daily_interval_hours` | Fetch interval for daily tier |
| `cool_interval_hours` | Fetch interval for cool tier |
| `cold_interval_hours` | Fetch interval for cold tier |
| `input_window_start` | First fetched_at included |
| `input_window_end` | Last fetched_at included |
| `code_sha` | Git commit used for replay |

### Dataset Tags

| Tag | Description |
|-----|-------------|
| `iceberg.silver_observations.snapshot_id` | Source observation snapshot |
| `iceberg.listing_state_runs.snapshot_id` | State-run snapshot |
| `iceberg.detail_refresh_priority.snapshot_id` | Feature/score snapshot |
| `dataset.row_count` | Input row count used for replay |

### Metrics

| Metric | Description |
|--------|-------------|
| `fetches_total` | Baseline fetch count |
| `fetches_skipped` | Fetches policy would skip |
| `fetches_skipped_pct` | Primary efficiency metric |
| `price_changes_delayed_pct` | Primary quality metric |
| `state_changes_delayed_pct` | Parsed-state changes delayed |
| `median_detection_delay_hours` | Typical detection lag |
| `p95_detection_delay_hours` | Tail detection lag |
| `missed_active_periods_pct` | Inventory tracking gaps |
| `estimated_403_reduction_pct` | Proxy from reduced detail fetches |

### Artifacts

- `policy_config.json`
- `dataset_snapshot.json`
- `mart_backtest_policy_summary.parquet`
- `policy_decisions_sample.parquet`
- optional plots for skip rate vs detection delay

---

## Inputs

- Iceberg snapshot of `int_listing_state_runs` from Plan 111: ground truth
  state-change timeline.
- Iceberg snapshot of `mart_detail_refresh_priority` from Plan 111: policy
  scoring output.
- Candidate policy parameters varied per run.

---

## Replay Algorithm

For each listing:

1. Walk the listing's historical state-run timeline in chronological order.
2. At each actual historical fetch point, apply the candidate policy:
   - If `next_detail_fetch_after <= fetch_time`, policy would fetch.
   - Otherwise, policy would skip.
3. For skipped fetches, record whether a state change occurred between the last
   policy-observed fetch and the next policy-eligible fetch.
4. Compute per-listing detection delay for any changes that were delayed.
5. Aggregate skipped fetches, delayed changes, and missed active windows.

The replay must pin all inputs to Iceberg snapshot IDs and record those IDs in
MLflow. If Iceberg setup is not complete, the runner may support a temporary
fixed-window fallback, but that fallback is not sufficient for approving a
production policy.

---

## Backtest Models

### `int_backtest_policy_decisions`

Row per `(listing_id, fetch_point, policy_run_id)`.

| Column | Description |
|--------|-------------|
| `policy_run_id` | Policy run identifier |
| `mlflow_run_id` | MLflow run identifier |
| `listing_id` | Listing key |
| `fetch_time` | Historical candidate fetch point |
| `would_fetch` | Boolean |
| `would_skip` | Boolean |
| `score_at_time` | Volatility score used |
| `tier_at_time` | Tier assigned |
| `reason` | Dominant decision reason |

### `mart_backtest_policy_summary`

One row per policy run with aggregate outcome metrics.

| Column | Description |
|--------|-------------|
| `policy_run_id` | Policy run identifier |
| `mlflow_run_id` | MLflow run identifier |
| `input_snapshot_id` | Primary Iceberg snapshot ID or manifest reference |
| `fetches_total` | Baseline fetch count |
| `fetches_skipped` | Fetches policy would skip |
| `fetches_skipped_pct` | Skip rate |
| `price_changes_delayed` | Price changes detected late |
| `state_changes_delayed` | Any parsed-state changes detected late |
| `median_detection_delay_hours` | Typical delay |
| `p95_detection_delay_hours` | Tail delay |
| `missed_active_periods` | Short-lived inventory missed |
| `estimated_403_reduction_pct` | Proxy from reduced detail fetches |

---

## Baseline Run

Log one MLflow run with `would_fetch = true` for all points. Every candidate
policy is compared to this baseline.

The baseline run should still log Iceberg snapshot IDs so future candidate runs
can prove they used comparable inputs.

---

## Evaluation Criteria

Before approving any policy for production, the chosen run must satisfy:

| Gate | Provisional Threshold |
|------|-----------------------|
| `fetches_skipped_pct` | >= 50% |
| `p95_detection_delay_hours` | <= 48h |
| `missed_active_periods` | <= 2% of total active periods |

These thresholds are provisional and must be reviewed against actual backtest
results before being treated as binding.

---

## Execution

Initial execution can be a local script or dbt operation. Add an Airflow DAG
only after the replay is stable and needs scheduled repetition.

Eventual `backtest_refresh_policy` DAG:

1. Ensure required Iceberg tables/snapshots exist.
2. Read policy parameter grid from config.
3. For each candidate policy, start an MLflow run.
4. Materialize `mart_backtest_policy_summary`.
5. Log params, metrics, Iceberg snapshot IDs, and artifacts.
6. Log the best run ID by skip rate subject to quality gates.

---

## Testing

### Unit Tests

- Replay algorithm correctly identifies skipped fetches and delayed detections.
- Detection delay is zero when the policy would have fetched on time.
- Baseline run produces `fetches_skipped_pct = 0`.
- Policy params and summary metrics are written with correct values.
- MLflow logging receives params, metrics, tags, and artifacts.
- Snapshot metadata is required for non-fallback approval runs.

### Backtest Correctness Tests

- Synthetic listing with no changes is throttled.
- Synthetic listing with recent price drop remains high priority.
- Newly discovered listing is always eligible.
- Listing with SRP recency is promoted.
- Detection-delay metrics are computed correctly for a known state change.

### Integration Tests

- Iceberg table registration reads a small normalized Parquet fixture.
- Snapshot metadata is captured for a fixed input table.
- Artifacts are written to the configured output path and retrievable.
- Repeated runs against the same snapshot produce stable row counts.
- Baseline and candidate runs are comparable by `policy_run_id`.
- MLflow run can be queried by run ID and contains expected metrics/artifacts.

---

## Files Changed

| File | Change |
|------|--------|
| `scripts/register_iceberg_tables.py` | New Iceberg registration/setup helper |
| `scripts/backtest_refresh_policy.py` | New local/DuckDB/Iceberg runner |
| `dbt/models/intermediate/int_backtest_policy_decisions.sql` | New |
| `dbt/models/marts/mart_backtest_policy_summary.sql` | New |
| `mlflow/` or config file | Optional MLflow server/config wiring |
| `airflow/dags/backtest_refresh_policy.py` | Optional later DAG |
| `tests/test_backtest_replay.py` | Replay algorithm unit tests |
| `tests/integration/test_refresh_backtest_outputs.py` | Output artifact integration tests |
| `tests/integration/test_iceberg_refresh_snapshots.py` | Iceberg snapshot tests |
| `tests/integration/test_mlflow_refresh_runs.py` | MLflow tracking tests |

---

## Out of Scope

- Production ops integration. See Plan 113.
- Online ML model serving.
- MLflow model registry promotion.
- Automatic Iceberg maintenance/compaction beyond what is needed for the first
  backtest substrate.
- Exact raw HTML dedup. See Plan 110 and Plan 114.
