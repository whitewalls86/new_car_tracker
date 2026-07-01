# Plan 111: Adaptive Refresh Feature Foundation

## Goal

Build the dbt models that describe listing state history and compute
interpretable volatility features. This is analysis-only foundation work for
the refresh-policy backtest in Plan 112.

Do not change production scraping in this plan.

---

## Context

Plan 110 showed:

- 5,804,559 detail artifacts in silver.
- 4,999,689 were semantically duplicate by parsed-state fingerprint.
- Semantic duplicate rate: 86.13%.
- Whole-file raw HTML hashes still differed.

The most valuable near-term lever is deciding which listings deserve frequent
detail refresh. This plan creates the feature layer needed to answer that
question from historical data.

---

## Core Question

For each listing:

> How likely is this listing to materially change before the next possible
> detail fetch?

The first model should be interpretable and rule-based. ML is deferred until
the target, labels, and quality gates have been proven by backtesting.

---

## Scope

Build dbt models only. Keep the first implementation on existing silver Parquet
and DuckDB reads. Apache Iceberg and ML are explicitly deferred so they do not
block the first backtest.

---

## Candidate Signals

### Listing-Level Signals

| Signal | Rationale |
|--------|-----------|
| `listing_age_days` | Newly discovered listings are more volatile |
| `days_since_first_seen` | Older unchanged listings likely need less detail frequency |
| `days_since_last_seen` | Avoid starving listings that still appear in SRP |
| `days_since_last_state_change` | Core stability signal |
| `unchanged_observation_streak` | Repeated identical parsed states indicate stability |
| `price_change_count_7d` | Recent price movement increases refresh priority |
| `price_change_count_30d` | Medium-term volatility |
| `days_since_price_drop` | Recent drops deserve follow-up |
| `listing_state_change_count` | Active/unavailable/unknown transitions matter |
| `mileage_change_count` | Mileage movement may indicate active listing updates |
| `current_price` | Price band may correlate with churn |
| `discount_vs_msrp` | Large discounts may move faster |

### Dealer / Market Signals

| Signal | Rationale |
|--------|-----------|
| `dealer_price_change_rate` | Some dealers update prices frequently |
| `dealer_inventory_churn_rate` | Dealer-level volatility |
| `make_model_price_change_rate` | Some models move faster than others |
| `make_model_inventory_churn_rate` | Market-level volatility |

### Pipeline / Reliability Signals

| Signal | Rationale |
|--------|-----------|
| `recent_srp_presence` | Recently seen in SRP means still worth tracking |
| `detail_success_rate_7d` | Avoid aggressive retrying through transient failures |
| `blocked_403_count_7d` | Anti-bot pressure should reduce priority |
| `parse_success_rate` | Low parse confidence may need follow-up or suppression |

---

## Models

### `int_listing_state_fingerprints`

One row per detail artifact with a stable parsed-state fingerprint.

Fingerprint should include business-relevant fields, not request-specific HTML
noise:

- `vin`
- `price`
- `mileage`
- `msrp`
- `make`
- `model`
- `trim`
- `year`
- `stock_type`
- `fuel_type`
- `body_style`
- `dealer_name`
- `dealer_zip`
- `customer_id`
- `seller_id`
- `dealer_city`
- `dealer_state`
- `seller_customer_id`

Output:

| Column | Description |
|--------|-------------|
| `artifact_id` | Source artifact |
| `listing_id` | Listing key |
| `fetched_at` | Observation time |
| `parsed_fingerprint` | Business-state hash |
| `price` | Current parsed price |
| `mileage` | Current parsed mileage |
| `source` | Detail/result source |

### `int_listing_state_runs`

Collapse repeated identical fingerprints into contiguous runs.

Output:

| Column | Description |
|--------|-------------|
| `listing_id` | Listing key |
| `parsed_fingerprint` | State hash |
| `run_started_at` | First time state appeared |
| `run_ended_at` | Last time state appeared before change |
| `artifact_count` | Observations in this run |
| `run_duration_hours` | Stability duration |
| `next_state_started_at` | When a change was next observed |

### `int_listing_volatility_features`

Current feature row per listing.

Output:

| Column | Description |
|--------|-------------|
| `listing_id` | Listing key |
| `latest_fetched_at` | Most recent detail observation |
| `first_seen_at` | First detail observation |
| `days_since_last_state_change` | Stability age |
| `unchanged_observation_streak` | Consecutive same-state count |
| `price_change_count_7d` | Recent price volatility |
| `price_change_count_30d` | Medium volatility |
| `dealer_volatility_score` | Dealer-level prior |
| `make_model_volatility_score` | Market-level prior |
| `recent_srp_seen_at` | Last SRP appearance |

### `mart_detail_refresh_priority`

Interpretable output used by Plan 112 backtesting first, then later by Plan 113
production ops integration.

Output:

| Column | Description |
|--------|-------------|
| `listing_id` | Listing key |
| `volatility_score` | 0-100 risk score |
| `refresh_tier` | `hot`, `daily`, `cool`, `cold` |
| `recommended_interval_hours` | Candidate refresh interval |
| `next_detail_fetch_after` | Earliest recommended next fetch time |
| `reason` | Human-readable dominant reason |

---

## Initial Scoring Approach

Start with an interpretable score:

```text
volatility_score =
    35 if listing first seen in last 48h
  + 25 if price changed in last 7d
  + 15 if price changed in last 30d
  + 15 if dealer volatility high
  + 10 if make/model volatility high
  + 10 if seen in SRP in last 24h
  - 10 if unchanged for 7d
  - 20 if unchanged for 14d
  - 30 if unchanged for 30d
```

Clamp to `0..100`.

Initial tier mapping:

| Tier | Score | Interval |
|------|-------|----------|
| `hot` | `>= 70` | 6-12 hours |
| `daily` | `40-69` | 24 hours |
| `cool` | `15-39` | 72 hours |
| `cold` | `< 15` | 7 days |

Thresholds must be tuned from Plan 112 backtest results.

---

## Deferred Work

### Iceberg

Iceberg may be useful later for snapshot isolation and table maintenance, but it
is not required to build the first historical feature tables. The first backtest
can replay timelines from existing silver Parquet using `fetched_at` ordering.

### ML

The first scoring model should be rule-based and interpretable. ML can follow
after Plan 112 proves the objective, labels, and quality gates.

---

## Testing

### dbt Tests

- Fingerprint is stable for identical business-state rows.
- Fingerprint changes when price, mileage, listing state, or dealer identity
  changes.
- State runs collapse repeated identical fingerprints correctly.
- Volatility scores are clamped to `0..100`.
- Refresh tiers map deterministically from score.

### Integration Tests

- dbt model selection builds the four refresh models from existing silver data.
- Row counts are stable across repeated runs on a fixed input snapshot.

---

## Rollout

1. Build `int_listing_state_fingerprints`.
2. Build `int_listing_state_runs`.
3. Build `int_listing_volatility_features`.
4. Build `mart_detail_refresh_priority`.
5. Run dbt tests and hand off to Plan 112.

---

## Out of Scope

- Backtest policy simulation. See Plan 112.
- Production ops integration. See Plan 113.
- Sectioned/recomposable HTML storage. See Plan 114.
- Replacing Airflow scheduling.
- ML model training in the first implementation.
