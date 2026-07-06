# Plan 112: Lakehouse + MLflow Adaptive Refresh Backtesting

## Goal

Build the reproducible experiment layer for adaptive detail refresh.

This plan is intentionally about both product outcome and skill development. The
project should get more comfortable with:

- MLflow experiment tracking.
- XGBoost model training and evaluation.
- A snapshot-capable lakehouse substrate, choosing between DuckLake and Apache
  Iceberg.
- Reproducible policy backtests over historical vehicle-listing timelines.

Do not change production scraping in this plan. Production integration belongs
to Plan 113.

---

## Context

Plan 110 normalized the storage layout and prepared the Parquet lake for a
snapshot-capable table layer.

Plan 111 shipped the feature foundation:

- `int_listing_state_fingerprints`
- `int_listing_state_runs`
- `int_listing_volatility_features`

Plan 111 did not ship `mart_detail_refresh_priority`. That policy/scoring layer
is part of this plan. Plan 112 owns turning feature rows into candidate refresh
policies, then proving or rejecting those policies through replay.

The current production evidence is strong enough to justify this work:

- Detail fetches have a high semantic duplicate rate.
- Whole-file HTML hashes do not deduplicate well.
- Recompression helps, but only saves roughly 8-10%.
- Plan 111 now provides VIN-grained volatility features across production data.

Before throttling production detail fetches, we need answers to:

- Which exact dataset snapshot was evaluated?
- Which policy or model version produced the decision?
- How many detail fetches would be skipped?
- How much price-change or state-change detection delay is introduced?
- Which VINs, dealers, makes, or models are harmed by throttling?
- Does the policy reduce likely 403/block pressure?
- Can the run be reproduced later from logged metadata?

The lakehouse layer answers dataset versioning. MLflow answers experiment
tracking. XGBoost gives us a real supervised learning path after the rule-based
baseline is established.

---

## Architecture

```text
Plan 110 normalized Parquet
        |
        v
DuckLake or Iceberg tables
        |
        v
Plan 111 feature + state-run models
        |
        v
Plan 112 policy outputs and training labels
        |
        +--> rule-based replay
        |
        +--> XGBoost training/evaluation
        |
        v
MLflow:
  params + metrics + artifacts + models + lakehouse snapshot/version IDs
```

The hot production claim path must not call MLflow, DuckLake, Iceberg, or a live
model server. Plan 113 will consume pinned outputs/configs produced by this
plan.

---

## Grain

Backtesting and modeling are VIN-grained.

Use `vin17` as the primary entity key. `listing_id` remains important metadata
and relisting signal, but it must not define the policy entity. A VIN can move
through multiple listing IDs, and that relisting behavior is itself a signal of
state change or marketplace churn.

Backtest decision rows should be keyed by:

```text
(policy_run_id, vin17, fetch_time)
```

Recommended supporting fields:

- `listing_id`
- `listing_id_change_count`
- `latest_listing_id`
- `source`
- `dealer/customer identifiers`
- `make/model/trim/year`

---

## Phase 1: Lakehouse Substrate Research Spike

Run this first. DuckLake, Iceberg, and MLflow are core learning goals for this
project, not optional polish.

Evaluate DuckLake and Apache Iceberg against the same small normalized dataset
from Plan 110. Use isolated object-store prefixes and isolated catalog
namespaces. Do not give experimental tables permission to compact, expire, or
delete production prefixes.

### Candidate A: DuckLake

- DuckDB-native lakehouse table format.
- SQL catalog, preferably Postgres for this deployment.
- Natural fit for the current DuckDB/dbt-runner workflow.
- Potentially simpler operational story on one VM.

### Candidate B: Apache Iceberg

- Broader ecosystem recognition.
- Stronger portfolio signal for data platform roles.
- Better fit if future work adds Spark, Trino, Flink, Snowflake, BigQuery,
  Databricks-adjacent tooling, or cross-engine lakehouse access.
- May require more machinery than DuckLake on the current single-server setup.

### Required Checks

For each candidate:

1. Create or register a small table from normalized Plan 110 Parquet.
2. Query it from the dbt-runner/DuckDB environment used by production.
3. Create at least two table versions or snapshots.
4. Capture snapshot/version metadata programmatically.
5. Time-travel or version-read to a prior snapshot.
6. Record the snapshot/version ID in an MLflow test run.
7. Validate repeated reads return stable row counts.
8. Verify MinIO path ownership and cleanup behavior.
9. Prove rollback/drop of the experimental table does not mutate source
   Parquet.
10. Document services, credentials, deployment changes, backup/restore needs,
    failure modes, and operator commands.
11. Score portfolio value separately from operational fit.

### Decision Criteria

| Criterion | DuckLake question | Iceberg question |
|-----------|-------------------|------------------|
| DuckDB/dbt fit | Can current jobs read/write directly? | Is read/write practical without excess machinery? |
| Snapshot fidelity | Are version IDs stable and easy to log? | Are snapshot IDs stable and easy to log? |
| MinIO safety | Can we avoid accidental source-prefix ownership? | Can we avoid accidental source-prefix ownership? |
| Ops burden | Does Postgres catalog setup stay simple? | Is catalog setup reasonable on one VM? |
| MLflow fit | Can runs log and replay recorded versions? | Can runs log and replay recorded snapshots? |
| Portfolio value | Is the story differentiated and legible? | Is the story broadly recognized? |

Expected output:

- `docs/lakehouse_substrate_decision.md`
- selected substrate
- rejected alternative
- exact spike commands
- cleanup proof
- production safety notes

If both candidates pass and Iceberg is only moderately harder, prefer Iceberg
for portfolio value. If Iceberg introduces disproportionate fragility or slows
the adaptive-refresh work too much, prefer DuckLake and keep moving.

---

## Phase 2: MLflow Foundation

Stand up MLflow before serious backtesting.

Initial deployment should be simple, but real:

- Backend store: existing Postgres if practical.
- Artifact store: MinIO or a mounted Docker volume.
- Access: internal first; dashboard/reverse-proxy exposure can follow later.

Create one baseline experiment and log a smoke-test run with:

- code SHA
- lakehouse substrate
- table snapshot/version ID
- input row count
- placeholder metrics
- a small artifact such as `dataset_snapshot.json`

This proves the tracking path before the replay logic adds complexity.

### Required MLflow Run Metadata

Parameters:

| Parameter | Description |
|-----------|-------------|
| `policy_family` | `baseline`, `rule`, or `xgboost` |
| `input_window_start` | First `fetched_at` included |
| `input_window_end` | Last `fetched_at` included |
| `code_sha` | Git commit used for replay |
| `entity_grain` | Must be `vin17` |

Dataset tags:

| Tag | Description |
|-----|-------------|
| `lakehouse.substrate` | `ducklake` or `iceberg` |
| `lakehouse.silver_observations.snapshot_id` | Source observation snapshot/version |
| `lakehouse.listing_state_runs.snapshot_id` | State-run snapshot/version |
| `lakehouse.listing_volatility_features.snapshot_id` | Feature snapshot/version |
| `dataset.row_count` | Input row count |
| `dataset.distinct_vins` | Distinct VIN count |

Artifacts:

- `dataset_snapshot.json`
- `policy_config.json`
- `environment.json`

---

## Phase 3: Lakehouse Table Registration

Register or create lakehouse tables needed for refresh experiments.

| Logical table | Source |
|---------------|--------|
| `silver_observations` | normalized silver observations from Plan 110 |
| `price_observation_events` | normalized ops event Parquet |
| `vin_to_listing_events` | normalized ops event Parquet |
| `blocked_cooldown_events` | normalized ops event Parquet |
| `listing_state_fingerprints` | Plan 111 model output |
| `listing_state_runs` | Plan 111 model output |
| `listing_volatility_features` | Plan 111 model output |

For each table, record:

- table name
- snapshot/version ID
- snapshot timestamp
- row count
- distinct VIN count when applicable
- input window start/end
- source code commit SHA
- source object prefix or catalog location

Do not register old production prefixes in a way that enables destructive table
maintenance over source Parquet.

---

## Phase 4: Policy Output Layer

Create the first candidate policy layer in this plan.

### `mart_detail_refresh_priority`

VIN-grained table used by replay first and production only later through Plan
113.

| Column | Description |
|--------|-------------|
| `vin17` | Primary entity key |
| `latest_listing_id` | Current/latest listing ID observed for the VIN |
| `latest_fetched_at` | Latest detail observation |
| `volatility_score` | Interpretable 0-100 risk score |
| `refresh_tier` | `hot`, `daily`, `cool`, `cold` |
| `recommended_interval_hours` | Candidate interval |
| `next_detail_fetch_after` | Earliest recommended next fetch time |
| `reason` | Dominant reason |
| `policy_version` | Static policy identifier |
| `feature_snapshot_id` | Lakehouse feature snapshot/version used |

Initial scoring should be rule-based and interpretable. Treat it as a baseline,
not the final model.

Example starting signals:

- newly seen VIN
- recent price change
- price change count in 7/30 days
- days since last state change
- unchanged observation streak
- SRP recency
- listing ID changes
- dealer/make/model priors from Plan 111 features
- 403/cooldown pressure

Thresholds are provisional until replay results exist.

---

## Phase 5: Backtest Replay

Build a replay runner over historical VIN timelines.

Implementation can be a Python script backed by DuckDB SQL. Do not run a full
dbt build for each policy candidate. Prepare the historical input once, then
evaluate many policy configs over that fixed input.

Preferred shape:

```text
prepare snapshot-backed input tables once
for policy in parameter_grid:
    compute VIN-grained decisions
    compute delay/skip metrics
    log MLflow run
    write summary + sampled decisions
```

Avoid:

```text
for policy in parameter_grid:
    rebuild the whole dbt project
```

### Replay Algorithm

For each VIN:

1. Walk historical detail fetch points in chronological order.
2. Track the latest state the policy would have observed.
3. At each historical fetch point, apply the candidate policy:
   - if `next_detail_fetch_after <= fetch_time`, policy would fetch
   - otherwise, policy would skip
4. When actual parsed state changes, compute whether the policy would detect it
   immediately or later.
5. Aggregate skip, delay, and missed-window metrics.

### Detection Delay Definition

For each material change:

- `actual_detected_at`: first historical fetch where the new state appeared.
- `policy_detected_at`: first fetch time the policy would have allowed after
  the prior policy-observed state.
- `delay_hours = policy_detected_at - actual_detected_at`.

Track separate delay metrics for:

- price changes
- listing-state changes
- relisting/listing-ID changes
- any parsed-fingerprint change
- active-window misses

---

## Phase 6: Rule-Based Experiments

Run an interpretable policy grid first.

Candidate parameters:

| Parameter | Description |
|-----------|-------------|
| `hot_threshold` | Min score for `hot` tier |
| `daily_threshold` | Min score for `daily` tier |
| `cool_threshold` | Min score for `cool` tier |
| `hot_interval_hours` | Fetch interval for hot tier |
| `daily_interval_hours` | Fetch interval for daily tier |
| `cool_interval_hours` | Fetch interval for cool tier |
| `cold_interval_hours` | Fetch interval for cold tier |
| `srp_recent_hours` | Promotion window for recent SRP appearance |
| `new_vin_hours` | Promotion window for new VINs |

Metrics:

| Metric | Description |
|--------|-------------|
| `fetches_total` | Baseline fetch count |
| `fetches_skipped` | Fetches policy would skip |
| `fetches_skipped_pct` | Primary efficiency metric |
| `price_changes_delayed_pct` | Price changes delayed |
| `state_changes_delayed_pct` | Listing-state changes delayed |
| `relisting_changes_delayed_pct` | Listing-ID changes delayed |
| `median_detection_delay_hours` | Typical delay |
| `p95_detection_delay_hours` | Tail delay |
| `missed_active_periods_pct` | Inventory tracking gaps |
| `estimated_403_reduction_pct` | Proxy from reduced detail fetches |

Artifacts:

- `policy_config.json`
- `dataset_snapshot.json`
- `mart_backtest_policy_summary.parquet`
- `policy_decisions_sample.parquet`
- optional plots for skip rate vs detection delay

---

## Phase 7: XGBoost Experiment

After the rule baseline and replay labels are stable, train an XGBoost model.

This is in scope for Plan 112 because learning XGBoost and MLflow is one of the
explicit project goals. The model does not go to production in this plan.

### Target

Predict whether a VIN will experience a material change before the next
candidate refresh window.

Possible binary labels:

- `changes_within_24h`
- `changes_within_48h`
- `price_changes_within_48h`
- `parsed_state_changes_within_48h`

Start with one clearly defined target, then expand only if needed.

### Features

Use Plan 111 feature outputs and simple derived fields:

- days since last state change
- unchanged observation streak
- price change counts
- listing ID change counts
- SRP recency
- make/model priors
- dealer priors
- current price band
- mileage band
- stock type
- fuel/body style
- cooldown/403 pressure where available

### MLflow Logging

Log:

- train/validation window boundaries
- lakehouse snapshot/version IDs
- XGBoost hyperparameters
- AUC/PR-AUC/log-loss
- calibration summary
- confusion matrix at candidate thresholds
- feature importance
- model artifact
- prediction sample

The output should be a candidate risk score that can be compared against the
rule-based policy in the same replay framework.

---

## Backtest Output Tables

### `int_backtest_policy_decisions`

Row per `(policy_run_id, vin17, fetch_time)`.

| Column | Description |
|--------|-------------|
| `policy_run_id` | Policy run identifier |
| `mlflow_run_id` | MLflow run identifier |
| `vin17` | Primary entity key |
| `listing_id` | Historical listing ID at fetch time |
| `fetch_time` | Historical candidate fetch point |
| `would_fetch` | Boolean |
| `would_skip` | Boolean |
| `score_at_time` | Rule/model score used |
| `tier_at_time` | Tier assigned |
| `reason` | Dominant decision reason |
| `actual_change_at` | Change timestamp if applicable |
| `policy_detected_at` | Policy detection timestamp if delayed |
| `delay_hours` | Detection delay |

### `mart_backtest_policy_summary`

One row per policy run.

| Column | Description |
|--------|-------------|
| `policy_run_id` | Policy run identifier |
| `mlflow_run_id` | MLflow run identifier |
| `input_snapshot_id` | Primary lakehouse snapshot/version ID |
| `policy_family` | `baseline`, `rule`, or `xgboost` |
| `fetches_total` | Baseline fetch count |
| `fetches_skipped` | Fetches policy would skip |
| `fetches_skipped_pct` | Skip rate |
| `price_changes_delayed` | Price changes detected late |
| `state_changes_delayed` | Listing-state changes detected late |
| `relisting_changes_delayed` | Listing-ID changes detected late |
| `median_detection_delay_hours` | Typical delay |
| `p95_detection_delay_hours` | Tail delay |
| `missed_active_periods` | Short-lived inventory missed |
| `estimated_403_reduction_pct` | Proxy from reduced detail fetches |

---

## Baseline Run

Log one MLflow run where `would_fetch = true` for all historical fetch points.
Every candidate policy is compared to this baseline.

The baseline run must log the same dataset snapshot/version IDs as candidate
runs so comparisons are meaningful.

---

## Evaluation Gates

Before approving any policy for Plan 113 production work, the chosen run must
satisfy provisional gates:

| Gate | Provisional Threshold |
|------|-----------------------|
| `fetches_skipped_pct` | >= 50% |
| `p95_detection_delay_hours` | <= 48h |
| `missed_active_periods_pct` | <= 2% |
| `price_changes_delayed_pct` | reviewed against business impact |

These thresholds are starting points. Revise them after actual replay results.

XGBoost does not need to beat the rule policy to be valuable. It should teach
whether the available features support useful predictive signal. Production can
still launch with the rule policy if it is safer and easier to explain.

---

## Execution

Initial execution is a local/operator script, not an Airflow DAG.

Add a DAG only after:

- the substrate is selected
- MLflow run logging works
- replay metrics are stable
- output artifacts are useful
- compute cost is understood

Eventual `backtest_refresh_policy` DAG:

1. Ensure required lakehouse snapshots exist.
2. Read policy parameter grid from config.
3. Run baseline.
4. Run rule-policy grid.
5. Optionally train/evaluate XGBoost.
6. Log all runs to MLflow.
7. Write summary artifacts.
8. Mark the best candidate by skip rate subject to quality gates.

---

## Compute Notes

The production feature table currently has hundreds of thousands of VIN rows
and the historical observation table has millions of detail artifacts. This is
large enough to require care, but small enough for DuckDB on one VM if the
runner avoids wasteful repeated full rebuilds.

Guidelines:

- Materialize replay inputs once per snapshot/window.
- Evaluate many policies against that prepared input.
- Prefer columnar intermediate artifacts over Python row loops for large
  timelines.
- Start with a bounded time window, then scale to all available history.
- Log wall-clock runtime, rows scanned, and output row counts to MLflow.
- Keep detailed decision rows sampled or partitioned if they become too large.

---

## Testing

### Unit Tests

- Replay algorithm correctly identifies skipped fetches.
- Detection delay is zero when policy fetches on time.
- Detection delay is positive when policy skips over a change.
- Baseline run produces `fetches_skipped_pct = 0`.
- VIN grain is enforced; relisting does not split the entity.
- Policy params and summary metrics are written correctly.
- MLflow logger receives params, metrics, tags, artifacts, and model metadata.
- Snapshot metadata is required for approval runs.

### Backtest Correctness Tests

- Synthetic VIN with no changes is throttled.
- Synthetic VIN with recent price drop remains high priority.
- Newly discovered VIN is always eligible.
- VIN with recent SRP appearance is promoted.
- VIN relisting is treated as a change signal.
- Known state change produces expected `delay_hours`.

### Lakehouse Integration Tests

- Selected substrate reads a small normalized Parquet fixture.
- Snapshot/version metadata is captured.
- Time-travel/version read returns expected row counts.
- Dropping the experimental table does not mutate source Parquet.
- Repeated reads against the same snapshot are stable.

### MLflow Integration Tests

- MLflow run can be created and queried by run ID.
- Params, metrics, tags, and artifacts are present.
- Dataset snapshot metadata is attached.
- XGBoost model artifact can be logged and loaded.

---

## Files Changed

| File | Change |
|------|--------|
| `docs/lakehouse_substrate_decision.md` | New DuckLake vs Iceberg spike result |
| `scripts/spike_lakehouse_substrate.py` | New isolated substrate spike helper |
| `scripts/register_lakehouse_tables.py` | New selected-substrate registration/setup helper |
| `scripts/backtest_refresh_policy.py` | New replay runner |
| `scripts/train_refresh_xgboost.py` | New XGBoost experiment runner |
| `dbt/models/marts/mart_detail_refresh_priority.sql` | New rule policy output |
| `dbt/models/intermediate/int_backtest_policy_decisions.sql` | New, if dbt materialization is useful |
| `dbt/models/marts/mart_backtest_policy_summary.sql` | New, if dbt materialization is useful |
| `mlflow/` or config file | MLflow server/config wiring |
| `airflow/dags/backtest_refresh_policy.py` | Optional later DAG |
| `tests/test_backtest_replay.py` | Replay algorithm unit tests |
| `tests/test_refresh_xgboost.py` | Model training/evaluation tests |
| `tests/integration/test_lakehouse_refresh_snapshots.py` | Lakehouse snapshot tests |
| `tests/integration/test_mlflow_refresh_runs.py` | MLflow tracking tests |

---

## Out Of Scope

- Production ops claim-query integration. See Plan 113.
- Online model serving.
- Automatic model promotion.
- Calling MLflow or lakehouse services in the hot production scrape path.
- Automatic lakehouse maintenance/compaction beyond what is needed for the
  first reproducible experiment substrate.
- Raw HTML sectioning/deduplication. See Plan 114.
