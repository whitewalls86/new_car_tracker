# Plan 90: dbt Intermediate Cleanup

**Status:** Planned — blocked on Plan 89 shadow period completing
**Priority:** Medium — follow-on to Plan 89; do not start until app-owned tables are validated

---

## Overview

After Plan 89's shadow period confirms that `listing_to_vin`, `price_observations`, and `vin_state` are correctly populated and agree with the dbt-derived equivalents, the now-redundant dbt intermediate models can be removed and the analytics marts updated to read from the application-owned tables directly.

This is a cleanup plan, not a feature plan. Nothing operationally changes — the ops views were already rewritten as live Postgres views in Plan 89. This plan tidies dbt so it only contains analytics logic.

---

## Prerequisites

- Plan 89 complete: app-owned tables live, ops views rewritten as Postgres views
- Shadow validation passed: `listing_to_vin` matches `int_listing_to_vin`, `price_observations` matches `int_price_events`, for at least one full scrape cycle
- Layer 3 integration tests covering the write path are green in CI

---

## Models to Delete

| Model | Replacement |
|---|---|
| `int_listing_to_vin` | `listing_to_vin` app table |
| `int_price_events` | `price_observations` app table |
| `int_latest_price_by_vin` | Query against `price_observations` |
| `int_latest_tier1_observation_by_vin` | `vin_state` app table |
| `int_carousel_hints_filtered` | Validation moved inline to processing service |
| `int_carousel_price_events_mapped` | Subsumed by `price_observations` write path |
| `int_carousel_price_events_unmapped` | Subsumed by `price_observations` write path |
| `ops_vehicle_staleness` (dbt model) | Already a live Postgres view after Plan 89 |
| `ops_detail_scrape_queue` (dbt model) | Already a live Postgres view after Plan 89 |

---

## Models to Update

### `mart_vehicle_snapshot`
Currently joins `int_latest_tier1_observation_by_vin` and `int_latest_price_by_vin`. Update to join `vin_state` and query `price_observations` directly as dbt sources.

### `mart_deal_scores`
Reads through `mart_vehicle_snapshot`. Update follows automatically once the snapshot mart is updated.

### `int_price_history_by_vin`
Currently reads from `int_price_events`. Update to read from `price_observations` as a source.

### `int_vehicle_attributes`
Reads from `stg_srp_observations` and `stg_detail_observations` — no change needed.

---

## Other Cleanup

- Remove `after_srp`, `after_detail`, `both` entries from `dbt_intents` that reference deleted models, or update their `select_args` to reflect the narrowed dbt DAG
- Remove the shadow comparison tests added during Plan 89's validation period
- Run the full dbt test suite after deletions to confirm the analytics layer is intact
- Update Layer 2 dbt integration tests to remove any tests that covered the deleted models

---

## Rollout Order

1. Update `mart_vehicle_snapshot` and `mart_deal_scores` to read from app tables — verify marts produce correct output against the shadow data
2. Update `int_price_history_by_vin` source
3. Delete the redundant intermediate models one at a time, running `dbt build` after each deletion to catch downstream breakage early
4. Update `dbt_intents`
5. Remove shadow comparison tests from CI
6. Full dbt test suite pass
