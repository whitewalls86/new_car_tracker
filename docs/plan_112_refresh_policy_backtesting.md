# Plan 112: Delta + MLflow Adaptive Refresh Backtesting

## Goal

Build the reproducible experiment layer for adaptive detail refresh on the new
Databricks-style track.

This plan starts from two completed foundations:

- Plan 110 normalized storage layout and tightened the object-store contract.
- Plan 111 built the adaptive-refresh feature tables.
- Plan 120 provides CI/local historical fixture snapshots for reproducible
  development and tests.

Before training or backtesting anything seriously, audit those feature outputs
and make sure they are good enough to become experiment inputs.

This plan does **not** change production scraping. Production integration
belongs to Plan 113.

---

## Context

The previous version of this plan centered on DuckLake vs Apache Iceberg. Plan
117 now resets the roadmap toward:

- Delta Lake tables
- Spark/PySpark
- MLflow
- Unity Catalog OSS or a documented catalog/governance fallback
- eventual dbt migration away from DuckDB

The purpose of Plan 112 is to prove reproducible refresh-policy experiments,
not to complete the entire Databricks-style migration.

Plan 112 should consume fixture snapshots produced by Plan 120 when running in
CI or local development. Full production-corpus validation can still run on the
VM manually.

---

## Architecture

```text
Plan 110 normalized Parquet
        |
        v
Delta table spike / table-version capture
        |
        v
Plan 111 feature outputs audit
        |
        v
snapshot/version-pinned replay inputs
        |
        +--> rule-based policy backtests
        |
        +--> XGBoost experiment
        |
        v
MLflow:
  params + metrics + artifacts + models + Delta table versions
```

The hot production claim path must not call MLflow, Spark, Delta, Unity Catalog,
or a live model server. Plan 113 consumes pinned outputs/configs produced by
this plan.

---

## Phase 0: Feature Store Audit

Audit Plan 111 outputs before treating them as training or backtest inputs.

Tables to audit:

- `int_listing_state_fingerprints`
- `int_listing_state_runs`
- `int_listing_volatility_features`
- `mart_detail_refresh_priority`

Required checks:

1. Row counts by source window.
2. Distinct VIN/listing coverage.
3. Null rates on key fields.
4. Duplicate key checks at expected grain.
5. Freshness and input-window checks.
6. Fingerprint stability on repeated identical parsed states.
7. State-run continuity: no impossible overlaps or negative durations.
8. Volatility feature sanity: no unbounded scores, obvious date inversions, or
   impossible counts.
9. Sampled manual review for several VIN histories.
10. Identify whether the backtest grain should be VIN-only or VIN plus listing
    metadata for specific edge cases.

Deliverable:

- `docs/adaptive_refresh_feature_audit.md`

Plan 112 should not proceed to model/backtest claims until this audit is
complete.

---

## Phase 1: Delta + Unity Catalog OSS Spike

Run this first on isolated data and isolated object-store prefixes.

Minimum Delta checks:

1. Stand up a local Spark/PySpark environment.
2. Write a small Delta table from a Plan 110 normalized Parquet subset.
3. Read it back through Spark.
4. Append a second version.
5. Time travel to a previous version.
6. Capture table name, version, path, row count, and schema programmatically.
7. Prove cleanup does not mutate source Parquet.

Minimum Unity Catalog OSS checks:

1. Stand up Unity Catalog OSS or document why it is deferred.
2. Register or expose a test table if feasible.
3. Test the smallest useful governance behavior, such as reader/writer
   separation or table ownership metadata.
4. Document differences from managed Databricks Unity Catalog.

Decision output:

- `docs/lakehouse_substrate_decision.md`
- selected local table/catalog path
- rejected alternatives
- exact spike commands
- cleanup proof
- known gaps vs managed Databricks

Fallback rule:

If Delta works but Unity Catalog OSS blocks the local workflow, continue with
Delta + MLflow and defer catalog integration to Plan 119. If Delta itself is
disproportionately fragile locally, re-open Iceberg + Polaris.

---

## Phase 2: MLflow Foundation

Stand up MLflow before serious backtesting.

Initial deployment:

- Backend store: existing Postgres if practical.
- Artifact store: MinIO or a mounted Docker volume.
- Access: internal first.

Create one smoke-test experiment and log:

- code SHA
- Delta table name
- Delta table version
- input row count
- placeholder metrics
- `dataset_snapshot.json`

Required MLflow metadata:

| Field | Description |
|-------|-------------|
| `policy_family` | `baseline`, `rule`, or `xgboost` |
| `input_window_start` | First `fetched_at` included |
| `input_window_end` | Last `fetched_at` included |
| `code_sha` | Git commit used for replay |
| `entity_grain` | Expected to be `vin17` unless the audit says otherwise |
| `delta.table` | Source table name |
| `delta.version` | Source table version |
| `dataset.row_count` | Input row count |
| `dataset.distinct_vins` | Distinct VIN count |

Artifacts:

- `dataset_snapshot.json`
- `policy_config.json`
- `environment.json`

---

## Phase 3: Backtest Input Preparation

Prepare stable replay inputs once per dataset/version/window.

Required inputs:

- historical detail fetch points
- parsed state fingerprints
- state runs
- volatility features
- SRP recency signals
- listing/VIN relisting signals
- 403/cooldown signals where relevant

Backtesting and modeling are VIN-grained unless the Phase 0 audit shows that a
specific feature requires a secondary listing-level signal.

Backtest decision rows should be keyed by:

```text
(policy_run_id, vin17, fetch_time)
```

Recommended supporting fields:

- `listing_id`
- `latest_listing_id`
- `listing_id_change_count`
- `source`
- `dealer/customer identifiers`
- `make/model/trim/year`

---

## Phase 4: Rule-Based Replay

Run an interpretable policy grid before ML training.

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

Replay algorithm:

1. Walk historical detail fetch points for each VIN in chronological order.
2. Track the latest state the policy would have observed.
3. At each historical fetch point:
   - fetch if `next_detail_fetch_after <= fetch_time`
   - otherwise skip
4. When actual parsed state changes, compute when the policy would detect it.
5. Aggregate skip, delay, and missed-window metrics.

Primary metrics:

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

---

## Phase 5: XGBoost Experiment

After the rule baseline and labels are stable, train an XGBoost model.

This is a learning goal and an experiment. It does not go to production in this
plan.

Start with one binary target:

- `material_change_within_48h`

Candidate features:

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
- cooldown/403 pressure

Log to MLflow:

- train/validation window boundaries
- Delta table versions
- XGBoost hyperparameters
- AUC/PR-AUC/log-loss
- calibration summary
- confusion matrix at candidate thresholds
- feature importance
- model artifact
- prediction sample

---

## Output Tables / Artifacts

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
| `input_table` | Primary Delta table |
| `input_table_version` | Primary Delta table version |
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

## Evaluation Gates

Before approving any policy for Plan 113, the chosen run must satisfy
provisional gates:

| Gate | Provisional Threshold |
|------|-----------------------|
| `fetches_skipped_pct` | >= 50% |
| `p95_detection_delay_hours` | <= 48h |
| `missed_active_periods_pct` | <= 2% |
| `price_changes_delayed_pct` | reviewed against business impact |

XGBoost does not need to beat the rule policy to be valuable. It should teach
whether the available features support useful predictive signal.

---

## Testing

- Feature audit queries catch duplicate keys, null keys, impossible date spans,
  and unstable grains.
- Delta spike can create, append, read, time travel, and clean up a fixture
  table.
- MLflow run logs params, metrics, tags, and artifacts.
- Replay algorithm correctly identifies skipped fetches and detection delay.
- Baseline run produces `fetches_skipped_pct = 0`.
- Newly discovered VINs remain eligible.
- Recent price drops remain high priority.
- Known state changes produce expected `delay_hours`.

---

## Files Changed

| File | Change |
|------|--------|
| `docs/adaptive_refresh_feature_audit.md` | New feature output audit |
| `docs/lakehouse_substrate_decision.md` | New Delta/Unity Catalog OSS spike result |
| `scripts/spike_delta_lakehouse.py` | New isolated Delta spike helper |
| `scripts/register_delta_tables.py` | New selected-table setup helper |
| `scripts/backtest_refresh_policy.py` | New replay runner |
| `scripts/train_refresh_xgboost.py` | New XGBoost experiment runner |
| `mlflow/` or config file | MLflow server/config wiring |
| `tests/test_backtest_replay.py` | Replay algorithm unit tests |
| `tests/test_refresh_xgboost.py` | Model training/evaluation tests |
| `tests/integration/test_delta_refresh_versions.py` | Delta version tests |
| `tests/integration/test_mlflow_refresh_runs.py` | MLflow tracking tests |

---

## Out Of Scope

- Production ops claim-query integration. See Plan 113.
- CI/local fixture snapshot export and delivery. See Plan 120.
- Full dbt migration away from DuckDB. See Plan 118.
- Governance/catalog expansion beyond the spike. See Plan 119.
- Online model serving.
- Automatic model promotion.
- Calling MLflow, Spark, Delta, or Unity Catalog in the hot production scrape
  path.
- Raw HTML sectioning/deduplication. See Plan 114.
