# Plan 112: Refresh Policy Backtesting

## Goal

Simulate candidate refresh policies against the historical listing timeline to
quantify the tradeoff between fetch volume reduction and detection delay before
any production scraping behavior changes.

The first implementation should be runnable with DuckDB/dbt and plain artifacts.
MLflow is optional observability, not a dependency for proving the policy.

---

## Context

Plan 111 builds `mart_detail_refresh_priority` using a hand-tuned,
interpretable scoring formula. Before wiring that score into production ops, we
need to know:

- How many detail fetches would each policy skip?
- How much detection delay does throttling introduce?
- Which threshold and interval set gives an acceptable tradeoff?
- Does the policy reduce likely 403/block pressure?

Each candidate policy is a set of thresholds and intervals. Each backtest run
should produce a comparable metrics row and a policy config artifact.

---

## Experiment Tracking

Start simple:

- Write policy configs to JSON.
- Write summary rows to Parquet or a dbt model.
- Store results under a local or MinIO analysis prefix.

Add MLflow only if comparison across many parameter sweeps becomes painful.

If MLflow is later added, it can use existing Postgres for the backend store and
MinIO for artifacts.

---

## Inputs

- `int_listing_state_runs` from Plan 111: ground truth state-change timeline.
- `mart_detail_refresh_priority` from Plan 111: policy scoring output.
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

This replay should run against existing silver Parquet first. Pin inputs by
artifact/fetched-at windows and record row counts in result metadata.

---

## Backtest Models

### `int_backtest_policy_decisions`

Row per `(listing_id, fetch_point, policy_run_id)`.

| Column | Description |
|--------|-------------|
| `policy_run_id` | Policy run identifier |
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

## Policy Run Structure

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
| `input_row_count` | Row count used for replay |

### Metrics

| Metric | Description |
|--------|-------------|
| `fetches_skipped_pct` | Primary efficiency metric |
| `price_changes_delayed_pct` | Primary quality metric |
| `median_detection_delay_hours` | Typical detection lag |
| `p95_detection_delay_hours` | Tail detection lag |
| `missed_active_periods` | Inventory tracking gaps |

### Artifacts

- `policy_config.json`: parameter snapshot.
- `mart_backtest_policy_summary.parquet`: full summary table.
- `policy_decisions_sample.parquet`: optional debugging sample.

### Baseline Run

Log one run with `would_fetch = true` for all points. Every candidate policy is
compared to this baseline.

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

1. Read policy parameter grid from config.
2. For each candidate policy, materialize `mart_backtest_policy_summary`.
3. Write params, metrics, and artifacts.
4. Log the best run ID by skip rate subject to quality gates.

---

## Testing

### Unit Tests

- Replay algorithm correctly identifies skipped fetches and delayed detections.
- Detection delay is zero when the policy would have fetched on time.
- Baseline run produces `fetches_skipped_pct = 0`.
- Policy params and summary metrics are written with correct values.

### Backtest Correctness Tests

- Synthetic listing with no changes is throttled.
- Synthetic listing with recent price drop remains high priority.
- Newly discovered listing is always eligible.
- Listing with SRP recency is promoted.
- Detection-delay metrics are computed correctly for a known state change.

### Integration Tests

- Artifacts are written to the configured output path and retrievable.
- Repeated runs against the same input window produce stable row counts.
- Baseline and candidate runs are comparable by `policy_run_id`.

---

## Files Changed

| File | Change |
|------|--------|
| `dbt/models/intermediate/int_backtest_policy_decisions.sql` | New |
| `dbt/models/marts/mart_backtest_policy_summary.sql` | New |
| `scripts/backtest_refresh_policy.py` | New local/DuckDB runner |
| `airflow/dags/backtest_refresh_policy.py` | Optional later DAG |
| `tests/test_backtest_replay.py` | Replay algorithm unit tests |
| `tests/integration/test_refresh_backtest_outputs.py` | Output artifact integration tests |

---

## Out of Scope

- ML-based volatility model.
- Production ops integration. See Plan 113.
- Iceberg setup.
- MLflow tracking server setup.
- Exact raw HTML dedup. See Plan 110.
