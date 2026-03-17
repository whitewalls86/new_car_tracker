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

## Plan 6: Async SRP Scraping with DB-Backed Job Tracking

**Status:** Implementation complete — needs import into n8n and end-to-end testing
**Priority:** High

### Architecture
- **Scraper API** — in-memory job store, `ThreadPoolExecutor(max_workers=4)`
  - `POST /scrape_results` → returns `{"job_id", "status": "queued"}` immediately, runs scrape in background
  - `GET /scrape_results/jobs/completed` → returns completed jobs with artifacts
  - `POST /scrape_results/jobs/{job_id}/fetched` → removes job from memory
  - `GET /scrape_results/jobs` → lists all jobs (debug)
- **Workflow A (Scrape Listings)** — simplified: fires all searches, inserts `scrape_jobs` rows, rotates sort, exits
- **Workflow B (Job Poller)** — every 1 min: polls completed jobs, inserts artifacts to `raw_artifacts`, marks fetched, checks run completion
- **`scrape_jobs` table** — tracks job lifecycle: `queued → running → completed → fetched` (or `failed`)
- **Results Processing** — user converting to scheduled (every 5 min), independent of this change

### What's Done
- `scrape_jobs` table created with indexes
- `scraper/app.py` modified with async job pattern (tested: POST returns in <1s, background worker completes, all endpoints work)
- `n8n/workflows/Scrape Listings.json` simplified (removed artifact nodes, added job row inserts, timeout 30s)
- `n8n/workflows/Job Poller.json` created

### Remaining
- Import updated Scrape Listings + new Job Poller into n8n
- End-to-end test with a real scrape
- Activate Job Poller schedule

---

## Plan 9: Analytics Dashboard (Streamlit)

**Status:** ✅ Complete (Plan 15 polish done)
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

**Status:** ✅ Complete
**Date identified:** 2026-03-16

### ~~15.1 — Telegram Pipeline Alerts~~ ✅
- Native Telegram node in Error Handler, uses n8n "Cartracker Alerts" credential (no token in JSON)
- Fires in parallel with DB insert; `continueOnFail` so Telegram failure doesn't block logging

### ~~15.2 — Active Run Indicator~~ ✅
- Banner at top of Pipeline Health showing active runs with elapsed time

### ~~15.3 — Dealer Names in Tables~~ ✅
- Added `dealer_name` to Deal Finder and Inventory tables

### ~~15.4 — Fix listing_state Filter~~ ✅
- Data check confirmed: mart already filters to 3-day window, all rows have explicit listing_state. COALESCE added defensively.

### ~~15.5 — Refresh Button + Data Freshness~~ ✅
- Refresh button + data freshness timestamp in sidebar

### ~~15.6 — Mobile Tab Navigation~~ ✅
- Replaced sidebar radio with `st.tabs`. Sidebar keeps refresh button + data freshness.

---

## Plan 16: Pipeline Efficiency — SRP Frequency + Staleness View Cleanup

**Status:** In progress (16.1 done)
**Priority:** Medium
**Date identified:** 2026-03-17

### ~~16.1 — Twice-Daily SRP Scrape~~ ✅
- Second daily SRP schedule added in n8n — burns through sort rotation in 2 days instead of 4

### ~~16.2 — Exclude Unlisted VINs from Staleness View~~ ✅
- Already implemented — `WHERE listing_state IS DISTINCT FROM 'unlisted'` confirmed in production (47,551 active only, zero unlisted)

### 16.3 — Monitor Detail Scrape Volume (decision pending ~March 20)
- After the post-fix backlog settles and SRP rotation completes a full cycle, check daily detail volume
- If stabilizes ~6-8K/day: no changes needed
- If still 30K+/day: consider changing batch query to `is_full_details_stale` only

## Plan 17: Update README.md

**Status:** Not started
**Priority:** Low

Update the README to accurately reflect current project architecture, services, and setup steps.

---

## Plan 18: Active Scrape Progress in Dashboard

**Status:** Not started
**Priority:** Medium

Show live progress during an active detail scrape — number of vehicles processed since the run started.

- **Short-term (no polling needed):** Add a metric to the active run indicator: `SELECT COUNT(*) FROM detail_observations WHERE fetched_at >= (SELECT started_at FROM runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1)`
- **Long-term:** Tie into Plan 6 async polling for richer progress/ETA estimates

---

## Plan 19: Detail Scrape Waits for Active Search Scrape

**Status:** Not started
**Priority:** Medium

If a search scrape is running when the detail scrape triggers, the search scrape may update prices that the detail scrape would otherwise fetch unnecessarily. Add a pre-check in the n8n Scrape Detail Pages workflow: if `runs WHERE status = 'running' AND trigger = 'search scrape'` exists, wait/skip.

- **Simple implementation:** IF node at the start of Scrape Detail Pages — check for active search scrape, exit early if found
- **Better with Plan 6:** Once async, could actively wait and retry after search completes

---

## Plan 20: dbt + Postgres Health in Pipeline Dashboard

**Status:** Not started
**Priority:** Medium

Add a new section or expand Pipeline Health with:
- **dbt:** Last build time, build duration, last build status (query a `dbt_run_results` log table if available, or parse from file)
- **Postgres:** Active/idle-in-transaction connections, any long-running queries, table bloat indicators
- **Locks:** Any queries blocked on locks (`pg_stat_activity` + `pg_locks`)

---

## Plan 21: Investigate Stale Vehicle Backlog vs Price Freshness Discrepancy

**Status:** Likely a non-issue — confirm after 16.2

**Note:** Almost certainly a filter mismatch, not a bug:
- **Stale Vehicle Backlog** filters to `listing_state IS DISTINCT FROM 'unlisted'` AND only shows *currently stale* VINs
- **Price Freshness chart** includes *all* VINs (including unlisted) across all age buckets
- Once 16.2 lands (filtering unlisted from `ops_vehicle_staleness`), both widgets will draw from the same filtered population and should align

Confirm after 16.2 is deployed before investigating further.

---

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **6** — Async SRP scraping | Import workflows into n8n + end-to-end test |
| 2 | **19** — Detail scrape waits for search scrape | Simple IF node in n8n — reduces redundant detail fetches |
| 3 | **18** — Active scrape progress in dashboard | Query `detail_observations` count since run start |
| 4 | **14.7** — dbt source freshness | Alerting when scraper stops ingesting |
| 5 | **14.10** — Detail fetch retry | Prevents transient data loss |
| 6 | **20** — dbt + Postgres health in dashboard | Operational visibility |
| 7 | **14.3** — Uncomment uniqueness test | Quick — just uncomment + verify |
| 8 | **14.2** — Duplicate ops models | Code smell cleanup |
| 9 | **14.1** — VIN case normalization | Defensive — only 1 affected VIN |
| 10 | **14.5** — Price events dedup | Defensive — only 1 duplicate found |
| 11 | **14.9** — Browser lock | Low risk in practice |
| 12 | **14.11** — Chrome fingerprint env var | Working fine currently |
| 13 | **14.12** — max_safety_pages validator | Low risk |
| 14 | **17** — Update README | Admin |
| 15 | **16.3** — Monitor detail volume | Wait until ~March 20 |
| 16 | **5** — Webhook triggers | Nice-to-have |

---

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
