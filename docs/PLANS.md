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
| 22 | **dbt model cleanup** — audited all 19 models; deleted 3 orphans (`int_carousel_hints_unmapped`, `int_listing_current_state`, `int_vin_current_state`) + schemas. 16 active models all have downstream consumers. | 2026-03-17 |
| 17 | **Update README** — full rewrite: architecture diagram, all 6 services, 7 workflows, 16 dbt models, data model, refresh strategy, setup steps, project structure | 2026-03-17 |
| 23 | **Fresh install support** — updated `schema_new.sql` (pg_dump), `.env.example`, `setup.ps1` script, example search config seed, README quick-start guide | 2026-03-17 |
| 26.3 | **Reduce max_workers 12→6** — ThreadPoolExecutor halved for immediate rate-limit relief | 2026-03-19 |
| 20 | **dbt + Postgres health in dashboard** — dbt build time/status, active connections, long-running queries, lock visibility | 2026-03-19 |
| 26 | **Search scrape slot rotation** — 6 slots, each fires once/day via `advance_rotation` (`min_idle_minutes=1439`, `min_gap_minutes=230`); discovery mode with VIN breakpoint and stop-on-error; `rotation_slot` + `last_queued_at` on `search_configs` | 2026-03-20 |
| 16.3 | **Detail scrape volume monitoring** — volume dropped from 48K→12K over 4 days, trending to 6-8K target. No intervention needed. | 2026-03-20 |
| 27.1 | **Detail scrape error rate alert** — Telegram alert when error rate >= 2.5% after each detail scrape run | 2026-03-20 |
| 27.2 | **Search scrape Akamai kill alert** — Job Poller Switch node detects ERR_HTTP2, sends Telegram with search_key/scope/page count | 2026-03-20 |
| 32 | **Force-grab stale vehicles in detail scrape** — added second pool for vehicles > 36h stale bypassing one-per-dealer rule | 2026-03-20 |
| 25.2 | **Store numeric `customer_id` in `detail_observations`** — DB migration, n8n Parse Detail Pages, dbt staging→mart chain | 2026-03-20 |
| 25.3 | **Fix dealer join in `mart_deal_scores`** — changed from UUID to numeric `customer_id` via `mart_vehicle_snapshot` | 2026-03-20 |
| 28 | **Dashboard quicklinks** — sidebar links to n8n, Search Config Admin, pgAdmin | 2026-03-20 |
| 31 | **pgAdmin for SQL access** — pgAdmin 4 container on port 5050, connected to cartracker DB | 2026-03-20 |

---

## Plan 5: n8n Webhook Triggers

**Status:** Not started
**Priority:** Low

Add webhook trigger nodes to Scrape Listings, Scrape Detail Pages, and Cleanup Artifacts for programmatic triggering.

---

## Plan 14: Codebase Audit Bug Fixes

**Status:** Mostly complete (14.2, 14.3, 14.4, 14.6, 14.8 done). Remaining items are low-risk defensive fixes.

**14.1 — VIN case normalization** — `stg_detail_observations` passes raw VIN (only 1 lowercase VIN found)
**14.5 — Price events dedup** — `UNION ALL` with no dedup (only 1 actual duplicate found)
**14.9 — Browser singleton lock** — no `threading.Lock()` on `get_browser()` (low risk in practice)
**14.11 — Chrome fingerprint env var** — hardcoded `chrome131` (working fine currently)
**14.12 — max_safety_pages validator** — no bounds check (low risk)

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **25.4** — Replace correlated subquery in staleness | Wait 24-48h for customer_id to populate |
| 2 | **33** — Add error info to runs table | Better run failure visibility |
| 3 | **34** — Fix artifact_count subquery in Job Poller | Prevents overcounting across runs |
| 4 | **29** — n8n API + trigger button | Programmatic workflow control |
| 5 | **30** — More detailed run info in dashboard | Execution list, duration, processing stats |
| 6 | **14.1** — VIN case normalization | Defensive — only 1 affected VIN |
| 7 | **14.5** — Price events dedup | Defensive — only 1 duplicate found |
| 8 | **14.9 / 14.11 / 14.12** — Minor defensive fixes | Low risk |
| 9 | **5** — Webhook triggers | Folded into Plan 29 |

---

## Plan 25: Bridge Dealer ID Systems

**Status:** 25.1/25.2/25.3 done (2026-03-20). 25.4 waiting for data population (24-48h).
**Priority:** High

cars.com uses two different dealer identifiers:
- **UUID** (`150b427b-c147-5a18-a733-cf5aa95519d0`) — in SRP JSON, stored in `srp_observations.seller_customer_id`
- **Numeric** (`735`) — in detail page HTML, already parsed by `parse_detail_page.py` and written to `dealers.customer_id`, but never stored in `detail_observations`


**25.4 — Replace correlated subquery in `ops_vehicle_staleness.sql`** *(defer 24-48h after 25.2 ships)*

Once `customer_id` has been populating for at least one full scrape cycle, the expensive correlated NOT EXISTS subquery for `dealer_unenriched` can be replaced with a simple null check.

Add `customer_id` to the `base` CTE SELECT from `mart_vehicle_snapshot`, then:
```sql
-- Replace correlated NOT EXISTS with:
(b.customer_id IS NULL) AS dealer_unenriched
```

⚠️ Do NOT deploy this until after 25.2 has been running for 24-48h. If deployed while `customer_id` is still NULL for all historical rows, every VIN will be flagged `dealer_unenriched = true` and the staleness system will schedule a full re-scrape of everything.

---

**Execution order:** DB migration → n8n workflow → dbt models (25.2+25.3 together) → wait 24-48h → 25.4

---

## Plan 29: Set up n8n API

- Set up the n8n API so we can interact with it programatically.
- Fold in Plan 5 to this.
- Add button to trigger detail scrape from from dashboard.

## Plan 30: More detailed Run info in Dashboard

- Increase the quality of information we're getting about running n8n processes in Dashboard.
- List of ongoing executions
- Lengh of time
- Information about results processing and detail parsing

## Plan 33: Add error info to runs table
- runs table has no `error_count` or `last_error` column
- When a run fails, there's no record of why (only in scrape_jobs)
- Add columns and update Job Poller's Mark Run Done to populate them

## Plan 34: Fix artifact_count subquery in Job Poller
- `Update scrape_jobs` counts artifacts matching `search_key + search_scope` but doesn't filter by `run_id`
- Could overcount if same search_key+scope runs overlap
- Add `AND run_id = scrape_jobs.run_id` to the subquery

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
