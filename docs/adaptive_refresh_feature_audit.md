# Adaptive Refresh Feature Audit

## Purpose and scope

Plan 112 Gate 0 preflight. Before any Iceberg/Spark/MLflow code is written,
confirm the dbt intermediate models that will feed adaptive-refresh backtest
replay are structurally sound: correct grain, no unexpected duplicate keys,
acceptable null rates on required fields, plausible freshness, and no
impossible date/duration spans.

This document does not evaluate model quality (feature usefulness, predictive
signal) — only structural integrity of the inputs. Model/backtest quality is
Gate D/E territory (`docs/plan_112_refresh_policy_backtesting.md`).

No Spark, PySpark, Iceberg, MLflow, Unity Catalog, Polaris, or Lakekeeper is
introduced by this document or its companion script. All checks run against
the existing DuckDB analytics database via
`scripts/audit_adaptive_refresh_features.py`.

## Source tables/models audited

| Model | Grain | Source models | Detail-only / all-source |
|-------|-------|----------------|---------------------------|
| `int_listing_state_fingerprints` | one row per `artifact_id` | `stg_observations` (source = `detail`) | detail-only |
| `int_listing_state_runs` | multiple rows per `vin17`; one row per contiguous run | `int_listing_state_fingerprints` | detail-only |
| `int_listing_observation_fingerprints` | one row per `observation_id` (= `artifact_id` + `listing_id`) | `stg_observations` (source in `detail`, `srp`, `carousel`) | all-source |
| `int_listing_observation_runs` | multiple rows per `listing_id`; one row per contiguous run | `int_listing_observation_fingerprints` | all-source |
| `int_listing_volatility_features` | one row per `vin17` | `int_listing_state_runs`, `int_listing_observation_runs`, `stg_observations`, `stg_price_events`, `int_price_history`, `int_benchmarks` | VIN-grain rollup, mixes detail-only and all-source signals |
| `mart_detail_refresh_priority` | **does not exist** | — | see "Absence of `mart_detail_refresh_priority`" below |

## Expected grain for each model

- **`int_listing_state_fingerprints`**: one row per `artifact_id`. Unique key
  is `artifact_id` (dbt `unique` + `not_null` tests already enforce this).
- **`int_listing_state_runs`**: unique key is `vin17` used as an *entity
  replacement key* for incremental delete+insert, not a row-unique key. The
  practical row-unique grain is `(vin17, run_started_at)` — exactly one row
  should exist per VIN per run start.
- **`int_listing_observation_fingerprints`**: one row per `observation_id`
  (`md5(artifact_id || listing_id)`). Unique key is `observation_id` (dbt
  `unique` + `not_null` tests already enforce this).
- **`int_listing_observation_runs`**: unique key is `listing_id` used as an
  entity replacement key, same pattern as `int_listing_state_runs`. Practical
  row-unique grain is `(listing_id, run_started_at)`.
- **`int_listing_volatility_features`**: one row per `vin17`. Unique key is
  `vin17` (dbt `unique` + `not_null` tests already enforce this).

## Key fields and required non-null fields

| Model | Required non-null fields |
|-------|---------------------------|
| `int_listing_state_fingerprints` | `vin17`, `listing_id`, `artifact_id`, `fetched_at`, `parsed_fingerprint` |
| `int_listing_state_runs` | `vin17`, `listing_id`, `parsed_fingerprint`, `run_started_at`, `run_ended_at`, `artifact_count`, `run_duration_hours`, `is_open_run` |
| `int_listing_observation_fingerprints` | `observation_id`, `artifact_id`, `listing_id`, `source`, `fetched_at`, `parsed_fingerprint` (`vin17` is intentionally nullable) |
| `int_listing_observation_runs` | `listing_id`, `observation_state_key`, `run_started_at`, `run_ended_at`, `observation_count`, `detail_observation_count`, `srp_observation_count`, `carousel_observation_count`, `distinct_source_count`, `detail_seen`, `srp_seen`, `carousel_seen`, `run_duration_hours`, `is_open_run` (`vin17` is intentionally nullable) |
| `int_listing_volatility_features` | `vin17`, `listing_id`, `latest_fetched_at`, `first_seen_at`, `total_state_changes`, `listing_id_change_count`, `days_since_last_state_change`, `unchanged_observation_streak`, `listing_state_change_count`, `price_change_count_7d`, `price_change_count_30d`, `all_source_unchanged_observation_streak`, `all_source_detail_observation_count`, `all_source_srp_observation_count`, `all_source_carousel_observation_count`, `all_source_non_detail_refresh_seen` |

These lists match each model's `data_tests: not_null` entries in its
`.schema.yml` — the audit script's `not_null_columns` per table is kept in
sync with this table and with the schema.yml tests, not a narrower subset.

## Freshness expectations

All five models are tagged `feature_daily` and are expected to be no more
stale than the daily dbt run cadence (see `docs/plan_123_dbt_incrementalization_and_resource_governance.md`
for the incremental scheduling). Concretely:

- `max(fetched_at)` in `int_listing_state_fingerprints` and
  `int_listing_observation_fingerprints` should track within ~1 day of the
  most recent scrape activity.
- `max(run_ended_at)` in the two run models should track the same window
  (an open run's `run_ended_at` advances every time a new fingerprint/
  observation with an unchanged state is recorded).
- `max(latest_fetched_at)` in `int_listing_volatility_features` should match
  `max(fetched_at)` in `int_listing_state_fingerprints`, since it is derived
  from the same detail observations.

The audit script reports `min`/`max` per table's primary timestamp column;
it does not currently compare across tables. Cross-table freshness
consistency is one of the deferred checks (see below).

## Duplicate-key expectations

- `int_listing_state_fingerprints`: zero duplicate `artifact_id` groups.
- `int_listing_observation_fingerprints`: zero duplicate `observation_id`
  groups.
- `int_listing_state_runs`: zero duplicate `(vin17, run_started_at)` groups.
  Multiple rows per `vin17` are expected and correct — that is the grain.
- `int_listing_observation_runs`: zero duplicate `(listing_id,
  run_started_at)` groups. Multiple rows per `listing_id` are expected.
- `int_listing_volatility_features`: zero duplicate `vin17` groups (strict
  one row per VIN).

## VIN-grain vs listing-grain vs artifact/observation-grain

| Model | Primary grain |
|-------|----------------|
| `int_listing_state_fingerprints` | artifact-grain (`artifact_id`) |
| `int_listing_state_runs` | VIN-grain (multiple rows per `vin17`) |
| `int_listing_observation_fingerprints` | observation-grain (`observation_id`) |
| `int_listing_observation_runs` | listing-grain (multiple rows per `listing_id`) |
| `int_listing_volatility_features` | VIN-grain (one row per `vin17`) |

Backtesting and modeling are primarily VIN-grained
(`docs/plan_112_refresh_policy_backtesting.md`, Gate C). Listing-grained
signals (`int_listing_observation_runs`, and `listing_id`-keyed columns
carried into `int_listing_volatility_features`) remain necessary for
relisting, dealer, and observation-cadence edge cases where a VIN maps to
more than one `listing_id` over time.

## Detail-only vs all-source models

- **Detail-only**: `int_listing_state_fingerprints`, `int_listing_state_runs`.
  These only see `stg_observations` rows where `source = 'detail'`, so every
  row has a resolved, validated `vin17` by construction.
- **All-source**: `int_listing_observation_fingerprints`,
  `int_listing_observation_runs`. These see `detail`, `srp`, and `carousel`
  rows. `vin17` is frequently null on SRP/carousel rows, so these models are
  keyed on `listing_id`/`observation_id` rather than `vin17`.
- **Mixed**: `int_listing_volatility_features` joins detail-only run history
  (`int_listing_state_runs`) with all-source cadence signals
  (`int_listing_observation_runs`, prefixed `all_source_*`) into one VIN-grain
  row.

## Absence of `mart_detail_refresh_priority`

`docs/plan_111_adaptive_detail_refresh.md` originally specified
`mart_detail_refresh_priority` as the final interpretable output feeding
Plan 112. That table was never built. `int_listing_volatility_features`
(materialized as a `table`, not incremental) is the model that actually
shipped in its place — it already assembles the VIN-grain feature row
(state-run history, price signals, dealer/make-model priors, SRP recency,
all-source cadence) that `mart_detail_refresh_priority` was meant to provide.

Plan 112 Gate C ("Backtest Input Preparation") should treat
`int_listing_volatility_features` as the current source of this feature row.
If a genuinely new `mart_detail_refresh_priority` output (e.g. an explicit
tiering/scoring layer) is introduced later, this document should be updated
and the model added to `scripts/audit_adaptive_refresh_features.py`.

## How each table contributes to adaptive refresh backtesting

- **`int_listing_state_fingerprints`**: raw detail-observation state hash
  history — the substrate for detail-only run construction and the source of
  truth for "did the detail page's business state change."
- **`int_listing_state_runs`**: labels for the primary backtest target —
  `hours_until_change` is the ground-truth detection-delay label the rule and
  XGBoost policies are evaluated against (Gate D/E).
- **`int_listing_observation_fingerprints`**: raw all-source observation state
  hash history — supports learning all-source cadence (SRP/carousel-driven
  refresh signals) independent of detail scrapes.
- **`int_listing_observation_runs`**: all-source cadence features (e.g.
  `all_source_unchanged_observation_streak`, `srp_seen`/`carousel_seen`
  flags) that feed escape-hatch and promotion logic (Gate D `srp_recent_hours`
  / `carousel_recent_hours` parameters, Gate F escape hatches).
- **`int_listing_volatility_features`**: the assembled VIN-grain feature row
  Gate C's replay inputs and Gate E's XGBoost candidate features are drawn
  from.

## VM/manual audit commands

Run inside `dbt_runner` on the VM (or any environment with read access to
`/data/analytics/analytics.duckdb`):

```bash
# JSON output (default)
python scripts/audit_adaptive_refresh_features.py

# Readable markdown summary
python scripts/audit_adaptive_refresh_features.py --markdown

# Persist JSON for later comparison
python scripts/audit_adaptive_refresh_features.py \
  --json-out /tmp/adaptive_refresh_feature_audit_$(date +%Y%m%d).json

# Point at a non-default DuckDB path
python scripts/audit_adaptive_refresh_features.py --db-path /path/to/analytics.duckdb
```

Supplementary manual spot-checks (run directly against DuckDB, e.g. via
`duckdb /data/analytics/analytics.duckdb`):

```sql
-- Fingerprint stability: repeated identical parsed states should not
-- fragment into spurious runs. Sample a VIN with a long detail history.
select vin17, listing_id, run_started_at, run_ended_at, artifact_count
from int_listing_state_runs
where vin17 = '<sample_vin>'
order by run_started_at;

-- State-run continuity: no impossible overlaps or negative durations.
select * from int_listing_state_runs where run_duration_hours < 0;
select * from int_listing_state_runs where hours_until_change < 0;

-- Observation-run continuity: source switches alone should not fragment runs.
select listing_id, run_started_at, run_ended_at, distinct_source_count,
       detail_seen, srp_seen, carousel_seen
from int_listing_observation_runs
where listing_id = '<sample_listing_id>'
order by run_started_at;

-- Volatility feature sanity: no unbounded scores / impossible counts.
select *
from int_listing_volatility_features
where listing_id_change_count < 1
   or total_state_changes < 0
   or price_change_count_7d < 0
   or price_change_count_30d < price_change_count_7d;
```

## Deferred checks (not yet implemented in the script)

The following checks are named in
`docs/plan_112_refresh_policy_backtesting.md` Gate 0 but are not automated in
`scripts/audit_adaptive_refresh_features.py`, to keep the script small for
this preflight PR:

- Cross-table freshness consistency (comparing `max(fetched_at)` across all
  five models against each other, not just per-table min/max).
- Sampled manual review of several VIN/listing histories (the SQL snippets
  above support this but require a human to pick VINs and eyeball output).
- Fingerprint stability under repeated identical parsed states beyond the
  structural run-continuity checks (would require replaying raw
  `stg_observations` rows against the fingerprint hash logic).

These should be added to the script (or a follow-up script) if Gate 0 VM
verification surfaces concrete problems that need repeatable detection.

## VM verification results

The audit below was run against `/data/analytics/analytics.duckdb` on the
production VM. The structural checks passed: all expected feature tables exist,
grain checks show no duplicate groups, required not-null checks are clean, and
run-duration checks show no negative durations.

```text
Run date: 2026-07-13
Run by: Andrew
Command: python scripts/audit_adaptive_refresh_features.py --markdown
Output:
# Adaptive Refresh Feature Audit

## int_listing_state_fingerprints
grain: artifact_id (detail-only)

- row_count: 5949066
- grain_distinct_count: 5949066
- duplicate_group_count: 0
- null_counts: {'vin17': 0, 'listing_id': 0, 'artifact_id': 0, 'fetched_at': 0, 'parsed_fingerprint': 0}
- timestamp_range: {'min': '2026-01-26 21:47:36.498729+00:00', 'max': '2026-07-13 19:45:25.544061+00:00'}
- vin_listing_coverage: {'distinct_vin_count': 250790, 'null_vin_count': 0, 'distinct_listing_count': 265817, 'null_listing_count': 0}

## int_listing_state_runs
grain: one row per (vin17, run_started_at); multiple runs per vin17 (detail-only)

- row_count: 1251754
- grain_distinct_count: 1251754
- duplicate_group_count: 0
- null_counts: {'vin17': 0, 'listing_id': 0, 'parsed_fingerprint': 0, 'run_started_at': 0, 'run_ended_at': 0, 'artifact_count': 0, 'run_duration_hours': 0, 'is_open_run': 0}
- timestamp_range: {'min': '2026-01-26 21:47:37.467820+00:00', 'max': '2026-07-13 19:45:25.544061+00:00'}
- vin_listing_coverage: {'distinct_vin_count': 250790, 'null_vin_count': 0, 'distinct_listing_count': 265817, 'null_listing_count': 0}
- negative_durations: {'run_duration_hours': 0, 'hours_until_change': 0}

## int_listing_observation_fingerprints
grain: observation_id (all-source: detail, srp, carousel)

- row_count: 38637191
- grain_distinct_count: 38637191
- duplicate_group_count: 0
- null_counts: {'observation_id': 0, 'artifact_id': 0, 'listing_id': 0, 'source': 0, 'fetched_at': 0, 'parsed_fingerprint': 0}
- timestamp_range: {'min': '2026-01-22 20:12:37.172000+00:00', 'max': '2026-07-13 19:45:25.544061+00:00'}
- source_distribution: {'carousel': 31433775, 'detail': 6052256, 'srp': 1151160}
- vin_listing_coverage: {'distinct_vin_count': 253890, 'null_vin_count': 5257092, 'distinct_listing_count': 412004, 'null_listing_count': 0}

## int_listing_observation_runs
grain: one row per (listing_id, run_started_at); multiple runs per listing_id (all-source)

- row_count: 2767857
- grain_distinct_count: 2767857
- duplicate_group_count: 0
- null_counts: {'listing_id': 0, 'observation_state_key': 0, 'run_started_at': 0, 'run_ended_at': 0, 'observation_count': 0, 'detail_observation_count': 0, 'srp_observation_count': 0, 'carousel_observation_count': 0, 'distinct_source_count': 0, 'detail_seen': 0, 'srp_seen': 0, 'carousel_seen': 0, 'run_duration_hours': 0, 'is_open_run': 0}
- timestamp_range: {'min': '2026-01-22 20:12:37.172000+00:00', 'max': '2026-07-13 19:45:25.544061+00:00'}
- vin_listing_coverage: {'distinct_vin_count': 253890, 'null_vin_count': 362000, 'distinct_listing_count': 412004, 'null_listing_count': 0}
- negative_durations: {'run_duration_hours': 0, 'hours_until_next_observation': 0}

## int_listing_volatility_features
grain: one row per vin17

- row_count: 250790
- grain_distinct_count: 250790
- duplicate_group_count: 0
- null_counts: {'vin17': 0, 'listing_id': 0, 'latest_fetched_at': 0, 'first_seen_at': 0, 'total_state_changes': 0, 'listing_id_change_count': 0, 'days_since_last_state_change': 0, 'unchanged_observation_streak': 0, 'listing_state_change_count': 0, 'price_change_count_7d': 0, 'price_change_count_30d': 0, 'all_source_unchanged_observation_streak': 0, 'all_source_detail_observation_count': 0, 'all_source_srp_observation_count': 0, 'all_source_carousel_observation_count': 0, 'all_source_non_detail_refresh_seen': 0}
- timestamp_range: {'min': '2026-01-28 21:25:01.314973+00:00', 'max': '2026-07-13 19:45:25.544061+00:00'}
- vin_listing_coverage: {'distinct_vin_count': 250790, 'null_vin_count': 0, 'distinct_listing_count': 250790, 'null_listing_count': 0}

## Not yet built
- mart_detail_refresh_priority — see docs/adaptive_refresh_feature_audit.md

Reviewer notes / anomalies found:
- No duplicate grain groups found in any audited feature table.
- Required not-null checks are clean across all audited tables.
- Negative-duration checks are clean for both run tables.
- All-source observation models intentionally include null VIN coverage because
  SRP/carousel observations can be listing-level before VIN resolution.
- `mart_detail_refresh_priority` remains not yet built; current adaptive-refresh
  inputs are the audited intermediate feature tables, especially
  `int_listing_volatility_features`.
```
