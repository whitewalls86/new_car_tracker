# Cartracker — Plans & Roadmap

---

## Completed

| Plan | Description | Date |
|------|-------------|------|
| 0 | **Detail page scraper** — curl_cffi bypasses Cloudflare TLS fingerprinting | 2026-03-16 |
| 1 | **Search sort-order rotation** — 4-day rotation (list_price, listed_at_desc, best_deal, best_match_desc) | 2026-03-16 |
| 3 | **Params schema cleanup** — removed `page_size`/`max_pages`, added `scopes` array | 2026-03-16 |
| 4 | **Artifact cleanup pipeline** — retention rules + daily n8n workflow | 2026-01 |
| 7 | **dbt materialized view migration** — `int_listing_current_state` + `int_vin_current_state` as dbt tables | 2026-03-16 |
| 8 | **403 artifact poisoning fix** — 1M+ Cloudflare block pages re-marked, staleness view unblocked | 2026-03-16 |
| 10 | **Pipeline durability** — stale run termination, stuck artifact reset, `pipeline_errors` table, Error Handler workflow | 2026-03-16 |
| 11 | **Search config admin UI** — FastAPI + Jinja2 at `/admin`, Pydantic models, CRUD for `search_configs` | 2026-03-16 |
| 13 | **dbt incremental optimization** — `int_latest_price_by_vin` + `int_latest_tier1_observation_by_vin` as incremental (merge on VIN). `mart_vehicle_snapshot` build dropped from 80s+ to <1s | 2026-03-16 |

---

## Plan 2: Dealer-Grouped Detail Page Refresh

**Status:** Not started
**Priority:** Medium

Re-order the detail-refresh queue to process stale listings grouped by dealer.

**Phase 1:** Change "Get Batch to Process" query in Scrape Detail Pages to `ORDER BY seller_customer_id, tier1_observed_at ASC`.

**Phase 2:** Score dealers by stale listing count + avg staleness. New "Dealer Sweep" sub-workflow for focused batch refreshes.

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

**Status:** ✅ Core implemented — iterating
**Port:** 8501

Streamlit app in `dashboard/` with 4 sections:
1. **Pipeline Health** — scrape timestamps, success rates, stale backlog, errors, terminated runs
2. **Inventory Overview** — active counts, new listings over time, unlisted trends, by-dealer table
3. **Deal Finder** — `mart_deal_scores` with filters (make, tier, scope), price drops, DOM distribution
4. **Market Trends** — median price by model over time, inventory levels, supply comparison

### Key Notes
- Uses `autocommit=True` on psycopg2 to prevent idle-in-transaction locks blocking dbt
- Queries `ops.ops_vehicle_staleness` (not `analytics.`), `int_listing_days_on_market` for historical counts, `int_price_events` for price trends

### Remaining Polish
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

**Status:** Not started
**Priority:** High
**Date identified:** 2026-03-16

Full codebase audit surfaced 12 issues across the dbt pipeline, scraper, and admin UI. Grouped by severity.

### CRITICAL — Data Correctness

**14.1 — VIN case normalization gap in `stg_detail_observations`**
- **Files:** `dbt/models/staging/stg_detail_observations.sql`, `dbt/macros/tests/valid_vin.sql`
- `stg_srp_observations` normalizes VINs via `upper(vin)` → `vin17`, but `stg_detail_observations` passes `d.vin` through raw. The `valid_vin` test requires uppercase (`^[A-Z0-9]{17}$`), so a lowercase VIN breaks `dbt test`. Worse, `int_price_events` joins SRP (`vin17`) with detail (`vin`) — case mismatches cause silent join failures and lost price events.
- **Fix:** Add `upper(vin)` normalization + `vin17` column to `stg_detail_observations`. Update downstream refs to use `vin17` consistently.

**14.2 — Duplicate ops models**
- **Files:** `dbt/models/ops/ops_listing_trace.sql`, `dbt/models/ops/ops_vin_latest_artifact.sql`
- These two files are byte-for-byte identical. If one is updated without the other, they silently diverge.
- **Fix:** Delete one and consolidate, or make one a ref to the other.

**14.3 — `stg_detail_observations` uniqueness test commented out**
- **File:** `dbt/models/staging/stg_detail_observations.schema.yml` (lines 28-30)
- The `unique_combination_of_columns: ['artifact_id', 'listing_id']` test is commented out. If the DB constraint is ever bypassed, duplicates propagate silently into `int_price_events` and `mart_deal_scores`.
- **Fix:** Uncomment the test. If it fails, investigate and fix the root cause.

### HIGH — Reliability & Data Quality

**14.4 — Hardcoded 3-day staleness window in 6+ places**
- **Files:** `mart_deal_scores.sql` (lines 9, 35, 52), `int_model_price_benchmarks.sql`, `int_dealer_inventory.sql`
- `now() - interval '3 days'` is copy-pasted everywhere. Policy changes require editing many files.
- **Fix:** Extract into a dbt variable (`{{ var('staleness_window_days', 3) }}`).

**14.5 — `int_price_events` can double-count prices**
- **File:** `dbt/models/intermediate/int_price_events.sql`
- `UNION ALL` across SRP, detail, and carousel. Same VIN observed at similar timestamps in SRP + detail produces duplicate price events, inflating `price_drop_count` and `total_price_observations` in deal scores.
- **Fix:** Add deduplication (e.g., `DISTINCT ON (vin, observed_at, price)` or priority-based dedup preferring tier-1).

**14.6 — National price percentile defaults to 50th when missing**
- **File:** `mart_deal_scores.sql` (line 123)
- `coalesce(pctl.national_price_percentile, 0.5)` awards 15/30 points to VINs without benchmarks, inflating scores for rare/new trims.
- **Fix:** Default to a conservative value (e.g., 0.75) or exclude unscored VINs.

**14.7 — No `source_freshness` in dbt sources**
- **File:** `dbt/models/sources.yml`
- If the scraper stops ingesting, dbt builds silently succeed on stale data. No alerting.
- **Fix:** Add `freshness: { warn_after: {count: 6, period: hour}, error_after: {count: 12, period: hour} }` with `loaded_at_field: fetched_at`.

**14.8 — Admin DELETE is hard-delete despite comment saying soft-delete**
- **File:** `scraper/routers/admin.py`
- Comment says "soft — set enabled=false" but code runs `DELETE FROM search_configs`. Data permanently destroyed.
- **Fix:** Implement actual soft-delete or correct the comment and add confirmation.

### MEDIUM — Robustness

**14.9 — Browser singleton race condition**
- **File:** `scraper/processors/browser.py`
- `get_browser()` checks `if _browser is None` without a lock. Concurrent calls can spawn multiple Playwright instances.
- **Fix:** Add `threading.Lock()` around initialization.

**14.10 — `scrape_detail_fetch` has no retry logic**
- **File:** `scraper/processors/scrape_detail.py`
- Single attempt per URL. Transient 429/timeout errors → permanent data loss for that listing.
- **Fix:** Add retry with exponential backoff (2-3 attempts).

**14.11 — Hardcoded `chrome131` browser fingerprint**
- **File:** `scraper/processors/scrape_detail.py`
- `BROWSER_IMPERSONATE = "chrome131"` is a single point of failure if blocked.
- **Fix:** Make configurable via env var or rotate versions.

**14.12 — `max_safety_pages` bypasses Pydantic validation** *(partially addressed — field exists in model but scraper enforcement unclear)*
- **Files:** `scraper/models/search_config.py`, `scraper/routers/admin.py`
- Admin form accepts `max_safety_pages` but `SearchConfigParams` doesn't define it. Field bypasses all validation.
- **Fix:** Add `max_safety_pages: int = 500` with bounds (1-2000) to `SearchConfigParams`. Verify scraper actually respects the limit.

---

## Plan 15: Streamlit Dashboard Polish + Telegram Alerts

**Status:** In progress (15.4 partially done — 1 of 7 queries fixed)
**Priority:** High
**Date identified:** 2026-03-16

Plan 9 polish items: quality-of-life dashboard fixes and error push notifications.

### Prerequisites (manual, one-time) — DO THIS BEFORE IMPLEMENTATION
1. Message `@BotFather` on Telegram → `/newbot` → save the bot token
2. Message the bot, then visit `https://api.telegram.org/bot<TOKEN>/getUpdates` to get your `chat_id`
3. Add Telegram API credential in n8n UI

### 15.1 — Telegram Pipeline Alerts
- **File:** `n8n/workflows/Error Handler.json`
- Add "Send Telegram" node wired in parallel with Postgres insert (so Telegram failure doesn't block logging)
- Message format: workflow name, node, error message, timestamp, execution ID

### 15.2 — Active Run Indicator
- **File:** `dashboard/app.py` (top of Pipeline Health section)
- Banner showing active runs with elapsed time. Green "No active runs" when idle.
- Query: `SELECT trigger, started_at, EXTRACT(EPOCH FROM now() - started_at)/60 AS elapsed_min FROM runs WHERE status = 'running'`

### 15.3 — Dealer Names in Tables
- **File:** `dashboard/app.py` (3 queries)
- Deal Finder "All Active Deals": add `dealer_name` to SELECT
- Deal Finder "Price Drop Events": add `dealer_name` to SELECT
- Inventory "Active by Dealer": replace `seller_customer_id` with `COALESCE(dealer_name, seller_customer_id) AS dealer`

### 15.4 — Fix listing_state Filter (Critical) *(1 of 7 fixed)*
- **File:** `dashboard/app.py` (7 queries across Inventory + Deal Finder + Market Trends)
- `WHERE listing_state = 'active'` excludes NULLs — only ~1,600 of ~19,500 rows have confirmed `'active'` state. SRP-only VINs (never detail-scraped) have NULL.
- **Fix:** Replace with `COALESCE(listing_state, 'active') != 'unlisted'` in all 7 locations.
- **Progress:** Line 130 already uses `listing_state IS DISTINCT FROM 'unlisted'`. Remaining 6 queries (lines ~249, ~287, ~352, ~372, ~604, ~627) still use `listing_state = 'active'`.

### 15.5 — Refresh Button + Data Freshness
- **File:** `dashboard/app.py` (sidebar)
- Refresh button: `st.sidebar.button("Refresh Data")` → `st.rerun()`
- Data freshness: query `MAX(price_observed_at) AT TIME ZONE 'America/Chicago'` from `analytics.mart_vehicle_snapshot`

### 15.6 — Mobile Tab Navigation
- **File:** `dashboard/app.py`
- Replace sidebar radio with `st.tabs` for better mobile UX. Sidebar keeps refresh + data freshness.

---

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
