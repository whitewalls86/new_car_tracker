# Cartracker — Plans & Roadmap

---

## Completed

| Plan | Description | Date |
|------|-------------|------|
| 0 | **Detail page scraper** — curl_cffi bypasses Cloudflare TLS fingerprinting | 2026-03-16 |
| 1 | **Search sort-order rotation** — 4-day rotation (list_price, listed_at_desc, best_deal, best_match_desc) | 2026-03-16 |
| 2 | **Dealer-grouped detail refresh** — batch query partitions by `seller_customer_id` | 2026-03-16 |
| 3 | **Params schema cleanup** — removed `page_size`/`max_pages`, added `scopes` array | 2026-03-16 |
| 4 | **Artifact cleanup pipeline** — retention rules + daily n8n workflow | 2026-01 |
| 7 | **dbt materialized view migration** — `int_listing_current_state` + `int_vin_current_state` as dbt tables | 2026-03-16 |
| 8 | **403 artifact poisoning fix** — 1M+ Cloudflare block pages re-marked, staleness view unblocked | 2026-03-16 |
| 10 | **Pipeline durability** — stale run termination, stuck artifact reset, `pipeline_errors` table, Error Handler workflow | 2026-03-16 |
| 11 | **Search config admin UI** — FastAPI + Jinja2 at `/admin`, Pydantic models, CRUD for `search_configs` | 2026-03-16 |
| 13 | **dbt incremental optimization** — `int_latest_price_by_vin` + `int_latest_tier1_observation_by_vin` as incremental (merge on VIN). `mart_vehicle_snapshot` build dropped from 80s+ to <1s | 2026-03-16 |
| 14.4 | **Staleness window dbt var** — extracted `now() - interval '3 days'` to `var("staleness_window_days")` across 3 dbt models | 2026-03-17 |
| 14.6 | **Percentile default** — `mart_deal_scores` uses conservative 0.75 default for missing benchmarks | 2026-03-17 |
| 14.8 | **Admin soft-delete** — delete now disables + renames key instead of hard `DELETE` | 2026-03-17 |
| 15.1 | **Telegram pipeline alerts** — native Telegram node in Error Handler, uses n8n "Cartracker Alerts" credential | 2026-03-17 |
| 16.1 | **Twice-daily SRP** — second daily SRP schedule in n8n, burns through sort rotation in 2 days | 2026-03-17 |

---

## Plan 5: n8n Webhook Triggers

**Status:** Not started
**Priority:** Low

Add webhook trigger nodes to Scrape Listings, Scrape Detail Pages, and Cleanup Artifacts for programmatic triggering.

---

## Plan 6: Async Job Polling for Scraper API

**Status:** Not started
**Priority:** High

### Problem
n8n holds an open HTTP connection for entire scrape duration (~10 min per scope). 9 configs × 2 scopes = 18 sequential calls = ~3 hours total.

### Solution
Switch to async job pattern:
1. `POST /scrape_results` → returns `{"job_id": "abc123", "status": "processing"}` immediately
2. n8n polls `GET /scrape_results/status/{job_id}` every 30s
3. Fire all 18 requests simultaneously → total time drops from ~3 hours to ~20 minutes

### Implementation
- FastAPI: background task runner, in-memory job store, status endpoint
- n8n: Fire Request → Wait (30s) → Poll Status → IF done → continue

---

## Plan 9: Analytics Dashboard (Streamlit)

**Status:** ✅ Core implemented — remaining polish in Plan 15
**Port:** 8501

Streamlit app in `dashboard/` with 4 sections:
1. **Pipeline Health** — scrape timestamps, success rates, stale backlog, errors, terminated runs
2. **Inventory Overview** — active counts, new listings over time, unlisted trends, by-dealer table
3. **Deal Finder** — `mart_deal_scores` with filters (make, tier, scope), price drops, DOM distribution
4. **Market Trends** — median price by model over time, inventory levels, supply comparison

### Key Notes
- Uses `autocommit=True` on psycopg2 to prevent idle-in-transaction locks blocking dbt
- Queries `ops.ops_vehicle_staleness` (not `analytics.`), `int_listing_days_on_market` for historical counts, `int_price_events` for price trends

### Remaining (not in Plan 15)
- Tune "Listings Going Unlisted" chart (currently uses first-unlisted CTE, may need refinement)
- Add price history sparklines per VIN

---

## Plan 12: Dealer Table

**Status:** ✅ Phases 1-4 implemented

### What's Done
- **Phase 1** — `dealers` table created (PK: `customer_id`, fields: name, street, city, state, zip, phone, website, cars_com_url, rating)
- **Phase 2** — Backfilled 7,259 dealers from existing data. 463 have full address/phone/website/rating (from detail page HTML scrapes)
- **Phase 3** — `_parse_dealer_card()` in `parse_detail_page.py` extracts dealer info from `.dealer-card` HTML. "Upsert Dealers" node in Parse Detail Pages workflow auto-populates on every detail scrape
- **Phase 4** — `dealers` added as dbt source. `mart_deal_scores` now includes `dealer_name`, `dealer_city`, `dealer_state`, `dealer_phone`, `dealer_rating`. `int_dealer_inventory` includes dealer name/city/state.

### Ongoing
- Dealer address data fills in automatically as detail pages are scraped (~463 of 7,259 have full info so far)
- Could add a dedicated dealer profile scraper (`/dealers/{customer_id}/`) to backfill faster

---

## Plan 14: Codebase Audit Bug Fixes

**Status:** In progress (14.4, 14.6, 14.8 done)
**Priority:** High
**Date identified:** 2026-03-16

### CRITICAL — Data Correctness

**14.1 — VIN case normalization gap in `stg_detail_observations`**
- **Files:** `dbt/models/staging/stg_detail_observations.sql`, `dbt/macros/tests/valid_vin.sql`
- `stg_srp_observations` normalizes VINs via `upper(vin)` → `vin17`, but `stg_detail_observations` passes `d.vin` through raw. Case mismatches cause silent join failures in `int_price_events` and lost price events.
- **Fix:** Add `upper(vin)` normalization + `vin17` column to `stg_detail_observations`. Update downstream refs to use `vin17` consistently.
- **Data check (2026-03-17):** Only 1 lowercase VIN found across all data — defensive fix, not actively breaking.

**14.2 — Duplicate ops models**
- **Files:** `dbt/models/ops/ops_listing_trace.sql`, `dbt/models/ops/ops_vin_latest_artifact.sql`
- Byte-for-byte identical. If one is updated without the other, they silently diverge.
- **Fix:** Delete one and consolidate, or make one a ref to the other.

**14.3 — `stg_detail_observations` uniqueness test commented out**
- **File:** `dbt/models/staging/stg_detail_observations.schema.yml` (lines 28-30)
- `unique_combination_of_columns: ['artifact_id', 'listing_id']` is commented out. Duplicates can propagate silently.
- **Fix:** Uncomment the test. If it fails, investigate root cause.

### HIGH — Reliability & Data Quality

**~~14.4 — Hardcoded 3-day staleness window~~** ✅
- Extracted to `var("staleness_window_days")` in `dbt_project.yml` (default: 3). All 3 files updated.

**14.5 — `int_price_events` can double-count prices**
- **File:** `dbt/models/intermediate/int_price_events.sql`
- `UNION ALL` across SRP, detail, and carousel with no dedup. Same VIN at similar timestamps inflates `price_drop_count`.
- **Fix:** Add deduplication (e.g., `DISTINCT ON (vin, observed_at, price)` or priority-based dedup preferring tier-1).
- **Data check (2026-03-17):** Only 1 actual duplicate found across recent data — defensive fix.

**~~14.6 — National price percentile defaults to 50th when missing~~** ✅
- Already uses `0.75` default in `mart_deal_scores.sql`.

**14.7 — No `source_freshness` in dbt sources**
- **File:** `dbt/models/sources.yml`
- If scraper stops ingesting, dbt builds silently succeed on stale data.
- **Fix:** Add `freshness` with `loaded_at_field: fetched_at`.

**~~14.8 — Admin DELETE is hard-delete~~** ✅
- Now soft-deletes: disables + renames key to `_deleted_{key}_{timestamp}`.

### MEDIUM — Robustness

**14.9 — Browser singleton race condition**
- **File:** `scraper/processors/browser.py`
- `get_browser()` has no `threading.Lock()`. Concurrent calls can spawn multiple instances.
- **Fix:** Add lock around initialization.

**14.10 — `scrape_detail_fetch` has no retry logic**
- **File:** `scraper/processors/scrape_detail.py`
- Single attempt per URL. Transient 429/timeout → permanent data loss.
- **Fix:** Add retry with exponential backoff (2-3 attempts).

**14.11 — Hardcoded `chrome131` browser fingerprint**
- **File:** `scraper/processors/scrape_detail.py`
- Single point of failure if blocked.
- **Fix:** Make configurable via env var or rotate versions.

**14.12 — `max_safety_pages` missing bounds validation**
- **File:** `scraper/models/search_config.py`
- Field exists but has no `@field_validator` with bounds (unlike `radius_miles` and `max_listings`).
- **Fix:** Add validator with bounds (1-2000).

---

## Plan 15: Streamlit Dashboard Polish + Telegram Alerts

**Status:** In progress (15.1 done, 15.2/15.3/15.5 done in dashboard)
**Priority:** High
**Date identified:** 2026-03-16

### ~~15.1 — Telegram Pipeline Alerts~~ ✅
- Native Telegram node in Error Handler, uses n8n "Cartracker Alerts" credential (no token in JSON)
- Fires in parallel with DB insert; `continueOnFail` so Telegram failure doesn't block logging

### ~~15.2 — Active Run Indicator~~ ✅
- Banner at top of Pipeline Health showing active runs with elapsed time

### ~~15.3 — Dealer Names in Tables~~ ✅
- Added `dealer_name` to Deal Finder and Inventory tables

### 15.4 — Fix listing_state Filter
- **Data check (2026-03-17):** Not actually critical — mart already filters to 3-day staleness window, so all rows have explicit listing_state. COALESCE change is defensive only.

### ~~15.5 — Refresh Button + Data Freshness~~ ✅
- Refresh button + data freshness timestamp in sidebar

### 15.6 — Mobile Tab Navigation
- **File:** `dashboard/app.py`
- Replace sidebar radio with `st.tabs` for better mobile UX.

---

## Plan 16: Pipeline Efficiency — SRP Frequency + Staleness View Cleanup

**Status:** In progress (16.1 done)
**Priority:** Medium
**Date identified:** 2026-03-17

### ~~16.1 — Twice-Daily SRP Scrape~~ ✅
- Second daily SRP schedule added in n8n — burns through sort rotation in 2 days instead of 4

### 16.2 — Exclude Unlisted VINs from Staleness View
- **File:** `dbt/models/ops/ops_vehicle_staleness.sql`
- Add `WHERE listing_state IS DISTINCT FROM 'unlisted'`
- 15,656 unlisted VINs are perpetually stale noise; removing them focuses the view and speeds up the batch query

### 16.3 — Monitor Detail Scrape Volume (decision pending ~March 20)
- After the post-fix backlog settles and SRP rotation completes a full cycle, check daily detail volume
- If stabilizes ~6-8K/day: no changes needed
- If still 30K+/day: consider changing batch query to `is_full_details_stale` only

---

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **16.2** — Staleness view exclude unlisted | Quick win — removes 15K noise rows |
| 2 | **15.6** — Mobile tabs | Dashboard UX |
| 3 | **14.7** — Source freshness | Alerting for stale data |
| 4 | **14.10** — Detail fetch retry | Prevents transient data loss |
| 5 | **14.3** — Uncomment uniqueness test | Quick — just uncomment + verify |
| 6 | **14.2** — Duplicate ops models | Code smell cleanup |
| 7 | **14.1** — VIN case normalization | Defensive — only 1 affected VIN |
| 8 | **14.5** — Price events dedup | Defensive — only 1 duplicate found |
| 9 | **14.9** — Browser lock | Low risk in practice |
| 10 | **14.11** — Chrome fingerprint env var | Working fine currently |
| 11 | **14.12** — max_safety_pages validator | Low risk |
| 12 | **15.4** — listing_state filter | Defensive only |
| 13 | **16.3** — Monitor detail volume | Wait until ~March 20 |
| 14 | **6** — Async job polling | Large feature — later sprint |
| 15 | **5** — Webhook triggers | Nice-to-have |

---

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
