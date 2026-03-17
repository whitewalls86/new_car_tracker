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
- Add dealer name columns to Deal Finder and Inventory tables (now available via `mart_deal_scores.dealer_name`)
- Tune "Listings Going Unlisted" chart (currently uses first-unlisted CTE, may need refinement)
- Add price history sparklines per VIN
- Mobile-friendly layout tweaks

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

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
