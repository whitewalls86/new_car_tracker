# Plan 112: Iceberg + MLflow Adaptive Refresh Backtesting

## Goal

Build the reproducible experiment layer for adaptive detail refresh on an open,
portable lakehouse foundation.

This plan is the first real implementation step after the Plan 110/111 storage
and feature foundations and the Plan 123 dbt resource work. The new direction is
not "fake Databricks." It is a Databricks-portable open lakehouse:

- Apache Iceberg tables on object storage
- Spark/PySpark for table writes, feature preparation, and model training
- MLflow for experiment tracking and policy artifacts
- Unity Catalog OSS or a compatible catalog/governance layer where practical
- Postgres remains the hot operational system
- DuckDB remains transitional, not the long-term analytical contract

This plan does **not** change production scraping. Production integration
belongs to Plan 113.

---

## Context

Earlier versions of this roadmap moved from a DuckDB-centered analytics layer
toward Delta Lake because the project was framed as "Databricks without
Databricks." That was defensible, but the industry direction has shifted enough
to revisit the choice.

The current strategic read:

- Iceberg is becoming the strongest vendor-neutral table-format story.
- Snowflake, Databricks, Trino, Spark, and catalog vendors are converging around
  Iceberg interoperability.
- Databricks remains Delta-native, but supports Iceberg access paths and Unity
  Catalog-style governance concepts.
- If a role requires managed Databricks experience as a hard gate, a local Delta
  clone would not fully satisfy that anyway.
- The systems knowledge in Iceberg + Spark + MLflow + catalog governance should
  transfer well to Databricks, Snowflake, and open lakehouse roles.

Therefore Plan 112 should use **Iceberg first**, with MLflow unchanged as the
experiment layer. The professional story becomes:

> Built an open lakehouse architecture using Apache Iceberg, Spark/PySpark,
> MLflow, and catalog/governance patterns aligned with Unity Catalog, designed
> for portability across Databricks, Snowflake, and open-source query engines.

Use Iceberg v2 for the initial implementation unless a selected tool requires
v3. Iceberg v3 support is important strategically, but v2 is the safer first
compatibility target.

---

## Foundations

This plan starts from completed or in-flight foundations:

- Plan 110 normalized storage layout and object-store hygiene.
- Plan 111 adaptive-refresh feature foundation.
- Plan 120 CI/local lake snapshot delivery.
- Plan 123 dbt cadence separation, feature-store correction, and DuckDB resource
  guardrails.
- Plan 124 browser-solver memory containment, removing a production instability
  distraction before lakehouse work begins.

Plan 112 should consume Plan 120 fixture snapshots in CI/local development and
can run fuller production-corpus validation manually on the VM.

---

## Architecture

```text
Plan 110 normalized Parquet / Plan 123 dbt feature outputs
        |
        v
Spark/PySpark Iceberg writers
        |
        v
Iceberg tables on MinIO
        |
        +--> catalog path:
        |      Unity Catalog OSS / REST catalog / documented fallback
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
  params + metrics + artifacts + models + Iceberg table snapshot metadata
```

The hot production claim path must not call MLflow, Spark, Iceberg, Unity
Catalog, or a live model server. Plan 113 consumes pinned outputs/configs
produced by this plan.

---

## Gate 0: Feature and Substrate Preflight

**Status: in progress.** Structural audit and first-pass substrate decision
docs and script exist; the placeholder "VM verification results" / "Gate A
spike results" sections in both docs are not yet filled in with real output.
Gate 0 is not complete until those sections are replaced with actual VM runs
and the audit script has been executed against
`/data/analytics/analytics.duckdb`.

Progress so far:

- [x] `docs/adaptive_refresh_feature_audit.md` — structural audit doc (grain,
  required fields, freshness, duplicate-key expectations, deferred checks).
- [x] `docs/lakehouse_substrate_decision.md` — first-pass Iceberg/catalog
  substrate decision, candidate tables, storage convention, cleanup rules.
- [x] `scripts/audit_adaptive_refresh_features.py` — read-only DuckDB audit
  script (row counts, grain/duplicate checks, null counts, freshness, source
  distribution, VIN/listing coverage, negative-duration checks).
- [ ] Real VM run of `scripts/audit_adaptive_refresh_features.py` against
  `/data/analytics/analytics.duckdb`, with output pasted into
  `docs/adaptive_refresh_feature_audit.md`'s placeholder section.
- [ ] Sampled manual VIN/listing history review (SQL snippets provided in the
  audit doc).
- [ ] Gate A Iceberg spike itself (not started — this PR is preflight only).

Before writing lakehouse code, confirm the current feature outputs and substrate
targets are ready.

Tables to audit:

- `int_listing_state_fingerprints`
- `int_listing_state_runs`
- `int_listing_observation_fingerprints`
- `int_listing_observation_runs`
- `int_listing_volatility_features`
- `mart_detail_refresh_priority` if still present or replaced by later feature
  outputs

Required checks:

1. Row counts by source window.
2. Distinct VIN/listing coverage.
3. Null rates on key fields.
4. Duplicate key checks at expected grain.
5. Freshness and input-window checks.
6. Fingerprint stability on repeated identical parsed states.
7. State-run continuity: no impossible overlaps or negative durations.
8. Observation-run continuity: source switches should not create false business
   state changes.
9. Volatility feature sanity: no unbounded scores, obvious date inversions, or
   impossible counts.
10. Sampled manual review for several VIN/listing histories.
11. Decide which backtest rows are VIN-grained and which supporting signals
    remain listing-grained.

Substrate preflight:

1. Choose first Iceberg catalog path:
   - preferred: REST-compatible catalog that can grow toward Unity Catalog OSS,
     Polaris, or Lakekeeper-style governance;
   - acceptable first spike: Hadoop/file catalog if it keeps the first PR small.
2. Choose physical storage prefixes under MinIO for isolated Iceberg tables.
3. Decide which tables are copied from dbt/DuckDB outputs versus written from
   normalized Parquet sources.
4. Document cleanup rules so spike tables cannot mutate production Parquet.

Deliverables:

- `docs/adaptive_refresh_feature_audit.md`
- first-pass catalog/storage decision section in
  `docs/lakehouse_substrate_decision.md`

Plan 112 should not proceed to model/backtest claims until this audit is
complete.

---

## Gate A: Iceberg + Catalog Foundation

Run this first on isolated data and isolated object-store prefixes.

Minimum Iceberg checks:

1. Stand up a local Spark/PySpark environment.
2. Configure Spark to write Iceberg tables to MinIO or a local object-store
   equivalent.
3. Write a small Iceberg table from a Plan 110/123 fixture subset.
4. Read it back through Spark.
5. Append a second snapshot.
6. Time travel to a previous snapshot.
7. Capture table name, catalog name, snapshot ID, path, row count, and schema
   programmatically.
8. Prove cleanup does not mutate source Parquet or production dbt outputs.
9. Confirm the same table can be recreated from a fixture snapshot in CI or a
   local development environment.

Minimum catalog/governance checks:

1. Stand up Unity Catalog OSS or document why it is deferred.
2. If Unity Catalog OSS is not viable for the first PR, evaluate a simpler REST
   catalog or file catalog and record the tradeoff.
3. Register or expose a test table if feasible.
4. Test the smallest useful governance behavior, such as reader/writer
   separation, table ownership metadata, or catalog/schema/table naming rules.
5. Document differences from managed Databricks Unity Catalog.

Decision output:

- `docs/lakehouse_substrate_decision.md`
- selected local Iceberg catalog path
- selected storage prefix convention
- rejected alternatives
- exact spike commands
- cleanup proof
- known gaps vs managed Databricks and Snowflake-managed Iceberg

Fallback rule:

If Unity Catalog OSS blocks the local workflow, continue with Spark + Iceberg +
MLflow using a simpler catalog and defer deeper catalog work to Plan 119. If
Iceberg itself is disproportionately fragile locally, revisit the table-format
decision before proceeding to backtests.

---

## Gate B: MLflow Foundation

Stand up MLflow before serious backtesting.

Initial deployment:

- Backend store: existing Postgres if practical.
- Artifact store: MinIO or a mounted Docker volume.
- Access: internal first.

Create one smoke-test experiment and log:

- code SHA
- Iceberg catalog/table name
- Iceberg snapshot ID
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
| `entity_grain` | Expected primary grain for decisions, likely `vin17` |
| `iceberg.catalog` | Source catalog |
| `iceberg.table` | Source table |
| `iceberg.snapshot_id` | Source snapshot ID |
| `dataset.row_count` | Input row count |
| `dataset.distinct_vins` | Distinct VIN count |

Artifacts:

- `dataset_snapshot.json`
- `policy_config.json`
- `environment.json`

---

## Gate C: Backtest Input Preparation

Prepare stable replay inputs once per dataset/snapshot/window.

Required inputs:

- historical detail fetch points
- parsed state fingerprints
- detail-only state runs
- all-source observation runs
- volatility features
- SRP/carousel recency signals
- listing/VIN relisting signals
- 403/cooldown signals where relevant

Backtesting and modeling are primarily VIN-grained. Listing-level signals should
remain available for relisting, dealer, and observation-cadence edge cases.

Backtest decision rows should be keyed by:

```text
(policy_run_id, vin17, fetch_time)
```

Recommended supporting fields:

- `listing_id`
- `latest_listing_id`
- `listing_id_change_count`
- `source`
- observation-run source counts
- dealer/customer identifiers
- make/model/trim/year

---

## Gate D: Rule-Based Replay

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
| `carousel_recent_hours` | Promotion window for recent carousel appearance |
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

## Gate E: XGBoost Experiment

After the rule baseline and labels are stable, train an XGBoost model.

This is a learning goal and an experiment. It does not go to production in this
plan.

Start with one binary target:

- `material_change_within_48h`

Candidate features:

- days since last detail-state change
- days since last all-source observation change
- unchanged observation streak
- price change counts
- listing ID change counts
- SRP recency
- carousel recency
- make/model priors
- dealer priors
- current price band
- mileage band
- stock type
- fuel/body style
- cooldown/403 pressure

Log to MLflow:

- train/validation window boundaries
- Iceberg table snapshot IDs
- XGBoost hyperparameters
- AUC/PR-AUC/log-loss
- calibration summary
- confusion matrix at candidate thresholds
- feature importance
- model artifact
- prediction sample

---

## Gate F: Policy Artifact For Plan 113

The output of this plan is a pinned policy candidate, not a production rollout.

Required artifact:

- `policy_config.json`

Required fields:

- policy family
- policy version
- code SHA
- MLflow run ID
- Iceberg table/snapshot metadata
- input window
- selected thresholds or model URI
- escape-hatch rules for:
  - new listings
  - never-scraped listings
  - SRP/carousel-recent listings
  - forced refreshes
  - cooldown/blocked listings

Plan 113 owns production claim-query integration, shadow mode, feature flags,
and rollback.

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
| `input_catalog` | Primary Iceberg catalog |
| `input_table` | Primary Iceberg table |
| `input_snapshot_id` | Primary Iceberg snapshot ID |
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
- Iceberg spike can create, append, read, time travel, and clean up a fixture
  table.
- Catalog test confirms whichever catalog path is selected can resolve a table
  by name.
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
| `docs/lakehouse_substrate_decision.md` | New Iceberg/catalog spike result |
| `scripts/spike_iceberg_lakehouse.py` | New isolated Iceberg spike helper |
| `scripts/register_iceberg_tables.py` | New selected-table setup helper |
| `scripts/backtest_refresh_policy.py` | New replay runner |
| `scripts/train_refresh_xgboost.py` | New XGBoost experiment runner |
| `mlflow/` or config file | MLflow server/config wiring |
| `tests/test_backtest_replay.py` | Replay algorithm unit tests |
| `tests/test_refresh_xgboost.py` | Model training/evaluation tests |
| `tests/integration/test_iceberg_refresh_snapshots.py` | Iceberg snapshot/time-travel tests |
| `tests/integration/test_mlflow_refresh_runs.py` | MLflow tracking tests |

---

## Out Of Scope

- Production ops claim-query integration. See Plan 113.
- CI/local fixture snapshot export and delivery. See Plan 120.
- Full dbt migration away from DuckDB. See Plan 118.
- Governance/catalog expansion beyond the spike. See Plan 119.
- Online model serving.
- Automatic model promotion.
- Calling MLflow, Spark, Iceberg, or Unity Catalog in the hot production scrape
  path.
- Raw HTML sectioning/deduplication. See Plan 114.
