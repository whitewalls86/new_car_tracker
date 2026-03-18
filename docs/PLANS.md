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
| 9 | **Analytics Dashboard** — 4-section Streamlit dashboard, mobile tabs, dealer names, refresh button, pipeline health | 2026-03-17 |
| 10 | **Pipeline durability** — stale run termination, stuck artifact reset, `pipeline_errors` table, Error Handler workflow | 2026-03-16 |
| 11 | **Search config admin UI** — FastAPI + Jinja2 at `/admin`, Pydantic models, CRUD for `search_configs` | 2026-03-16 |
| 12 | **Dealer Table** — `dealers` table, 7,259 backfilled, parsed from detail pages, included in mart models | 2026-03-17 |
| 13 | **dbt incremental optimization** — `int_latest_price_by_vin` + `int_latest_tier1_observation_by_vin` as incremental. `mart_vehicle_snapshot` dropped from 80s+ to <1s | 2026-03-16 |
| 14.4 | **Staleness window dbt var** — extracted `now() - interval '3 days'` to `var("staleness_window_days")` | 2026-03-17 |
| 14.6 | **Percentile default** — `mart_deal_scores` uses 0.75 default for missing benchmarks | 2026-03-17 |
| 14.8 | **Admin soft-delete** — delete now disables + renames key instead of hard `DELETE` | 2026-03-17 |
| 15 | **Dashboard polish + Telegram alerts** — Telegram Error Handler, active run indicator, dealer names, refresh button, mobile tabs | 2026-03-17 |
| 16.1 | **Twice-daily SRP** — second daily schedule, burns through sort rotation in 2 days | 2026-03-17 |
| 16.2 | **Exclude unlisted VINs from staleness view** — confirmed already implemented (`WHERE listing_state IS DISTINCT FROM 'unlisted'`) | 2026-03-17 |
| 21 | **Staleness discrepancy** — resolved; both dashboard widgets draw from same 47,551 active VINs | 2026-03-17 |
| 6 | **Async SRP scraping** — ThreadPoolExecutor(12), Job Poller, `scrape_jobs` table, jitter delays. Scrape time ~3hr → ~20min | 2026-03-17 |
| 6.1 | **Orphan job recovery** — `Expire Orphaned Jobs` node in Job Poller marks stuck jobs failed after 30min | 2026-03-17 |
| 6.2 | **Failed API job propagation** — `/completed` endpoint now includes failed jobs so Poller clears them | 2026-03-17 |
| 19 | **Detail scrape waits for search scrape** — IF node loop at start of Scrape Detail Pages, waits 3 min and retries if any scrape is running | 2026-03-17 |
| 18 | **Active scrape progress in dashboard** — `progress_count`/`total_count` on `runs`, 10% milestone UPDATEs in loop, dashboard shows "X / Y scraped (Z%)" | 2026-03-17 |
| 14.2 | **Duplicate ops models** — deleted both `ops_listing_trace` and `ops_vin_latest_artifact` (identical, unused) | 2026-03-17 |

---

## Plan 5: n8n Webhook Triggers

**Status:** Not started
**Priority:** Low

Add webhook trigger nodes to Scrape Listings, Scrape Detail Pages, and Cleanup Artifacts for programmatic triggering.

---

## Plan 6: Async SRP Scraping with DB-Backed Job Tracking

**Status:** ✅ Core complete — one minor item remaining
**Priority:** Low

### Architecture
- **Scraper API** — in-memory job store, `ThreadPoolExecutor(max_workers=12)`
  - `POST /scrape_results` → returns `{"job_id", "status": "queued"}` immediately
  - `GET /scrape_results/jobs/completed` → returns completed + failed jobs with artifacts
  - `POST /scrape_results/jobs/{job_id}/fetched` → removes job from memory
  - `GET /scrape_results/jobs` → lists all jobs (debug)
- **Scrape Listings** — fires all searches with 1-5s jitter, inserts `scrape_jobs` rows, rotates sort, exits in ~30s
- **Job Poller** — every 1 min: expires orphans, polls completed jobs, inserts artifacts, marks fetched, checks run completion
- **`scrape_jobs` table** — lifecycle: `queued → running → completed → fetched` (or `failed`)

### Remaining
**6.3** — Verify `Check Pending Jobs` SELECT includes `run_id` column on next workflow export from n8n (already fixed in UI, just confirm JSON matches).

---

## Plan 14: Codebase Audit Bug Fixes

**Status:** In progress (14.4, 14.6, 14.8 done)
**Priority:** High

### CRITICAL — Data Correctness

**14.1 — VIN case normalization gap in `stg_detail_observations`**
- `stg_srp_observations` normalizes VINs via `upper(vin)`, but `stg_detail_observations` passes raw. Case mismatches cause silent join failures.
- **Fix:** Add `upper(vin)` → `vin17` to `stg_detail_observations`. Update downstream refs.
- **Data check:** Only 1 lowercase VIN found — defensive fix.

**14.2 — Duplicate ops models**
- `ops_listing_trace.sql` and `ops_vin_latest_artifact.sql` are byte-for-byte identical.
- **Fix:** Delete one, make the other the reference.

**14.3 — `stg_detail_observations` uniqueness test commented out**
- `unique_combination_of_columns: ['artifact_id', 'listing_id']` is commented out in schema.yml.
- **Fix:** Uncomment. Investigate if it fails.

### HIGH — Reliability & Data Quality

**14.5 — `int_price_events` can double-count prices**
- `UNION ALL` across SRP, detail, carousel with no dedup. Same VIN at similar timestamps inflates `price_drop_count`.
- **Fix:** Add deduplication (`DISTINCT ON (vin, observed_at, price)` or priority-based).
- **Data check:** Only 1 actual duplicate found — defensive fix.

**14.7 — No `source_freshness` in dbt sources**
- If scraper stops ingesting, dbt builds succeed silently on stale data.
- **Fix:** Add `freshness` with `loaded_at_field: fetched_at` in `sources.yml`.

### MEDIUM — Robustness

**14.9 — Browser singleton race condition**
- `get_browser()` has no `threading.Lock()`. Concurrent calls can spawn multiple instances.
- **Fix:** Add lock around initialization.

**14.10 — `scrape_detail_fetch` has no retry logic**
- Single attempt per URL. Transient 429/timeout → permanent data loss.
- **Fix:** Exponential backoff, 2-3 attempts.

**14.11 — Hardcoded `chrome131` browser fingerprint**
- Single point of failure if blocked.
- **Fix:** Configurable via env var or rotation.

**14.12 — `max_safety_pages` missing bounds validation**
- No `@field_validator` with bounds (unlike `radius_miles` and `max_listings`).
- **Fix:** Add validator (1–2000).

---

## Plan 16: Pipeline Efficiency

**Status:** 16.1 + 16.2 done. 16.3 monitoring.

**16.3 — Monitor detail scrape volume (~March 20)**
- If daily detail volume stabilizes ~6-8K: no changes needed.
- If still 30K+/day: change batch query to filter on `is_full_details_stale` only.

---

## Plan 17: Update README.md

**Status:** Not started
**Priority:** Low

Update README to reflect current architecture, services, and setup steps.

---

## Plan 20: dbt + Postgres Health in Dashboard

**Status:** Not started
**Priority:** Medium

Expand Pipeline Health section with:
- **dbt:** Last build time, duration, status
- **Postgres:** Active/idle-in-transaction connections, long-running queries
- **Locks:** Blocked queries (`pg_stat_activity` + `pg_locks`)

---

## Plan 22: dbt Model Cleanup & Audit

**Status:** Not started
**Priority:** High

Deep dive of all dbt models to identify unused models, duplicates, overlapping logic, and dead code. Clean up the DAG.

- Enumerate all models, check each for downstream references
- Identify any models that overlap in purpose or output
- Remove orphans, consolidate where appropriate
- Verify the DAG is tight and every model earns its place

---

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **22** — dbt model cleanup & audit | Find unused/duplicate/overlapping models, tighten the DAG |
| 2 | **20** — dbt + Postgres health in dashboard | Operational visibility |
| 3 | **14.1** — VIN case normalization | Defensive — only 1 affected VIN |
| 4 | **14.5** — Price events dedup | Defensive — only 1 duplicate found |
| 5 | **14.9** — Browser lock | Low risk in practice |
| 6 | **14.11** — Chrome fingerprint env var | Working fine currently |
| 7 | **14.12** — max_safety_pages validator | Low risk |
| 8 | **17** — Update README | Admin |
| 9 | **16.3** — Monitor detail volume | Wait until ~March 20 |
| 10 | **5** — Webhook triggers | Nice-to-have |

---

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
