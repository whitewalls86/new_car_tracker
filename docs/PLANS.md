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
| 14.2 | **Duplicate ops models** — deleted both `ops_listing_trace` and `ops_vin_latest_artifact` (identical, unused) | 2026-03-17 |
| 14.4 | **Staleness window dbt var** — extracted `now() - interval '3 days'` to `var("staleness_window_days")` | 2026-03-17 |
| 14.6 | **Percentile default** — `mart_deal_scores` uses 0.75 default for missing benchmarks | 2026-03-17 |
| 14.8 | **Admin soft-delete** — delete now disables + renames key instead of hard `DELETE` | 2026-03-17 |
| 15 | **Dashboard polish + Telegram alerts** — Telegram Error Handler, active run indicator, dealer names, refresh button, mobile tabs | 2026-03-17 |
| 16.1 | **Twice-daily SRP** — second daily schedule, burns through sort rotation in 2 days | 2026-03-17 |
| 16.2 | **Exclude unlisted VINs from staleness view** — confirmed already implemented | 2026-03-17 |
| 16.3 | **Detail scrape volume monitoring** — volume dropped from 48K→12K over 4 days, trending to 6-8K target | 2026-03-20 |
| 17 | **Update README** — full rewrite: architecture diagram, all 6 services, 7 workflows, 16 dbt models, data model, refresh strategy, setup steps, project structure | 2026-03-17 |
| 18 | **Active scrape progress in dashboard** — `progress_count`/`total_count` on `runs`, 10% milestone UPDATEs in loop, dashboard shows "X / Y scraped (Z%)" | 2026-03-17 |
| 19 | **Detail scrape waits for search scrape** — IF node loop at start of Scrape Detail Pages, waits 3 min and retries if any scrape is running | 2026-03-17 |
| 20 | **dbt + Postgres health in dashboard** — dbt build time/status, active connections, long-running queries, lock visibility | 2026-03-19 |
| 21 | **Staleness discrepancy** — resolved; both dashboard widgets draw from same 47,551 active VINs | 2026-03-17 |
| 22 | **dbt model cleanup** — audited all 19 models; deleted 3 orphans. 16 active models all have downstream consumers. | 2026-03-17 |
| 23 | **Fresh install support** — updated `schema_new.sql` (pg_dump), `.env.example`, `setup.ps1` script, example search config seed, README quick-start guide | 2026-03-17 |
| 25.2 | **Store numeric `customer_id` in `detail_observations`** — DB migration, n8n Parse Detail Pages, dbt staging→mart chain | 2026-03-20 |
| 25.3 | **Fix dealer join in `mart_deal_scores`** — changed from UUID to numeric `customer_id` via `mart_vehicle_snapshot` | 2026-03-20 |
| 25.4 | **Replace correlated subquery in staleness** — `customer_id IS NULL` replaces expensive NOT EXISTS subqueries in `ops_vehicle_staleness` | 2026-03-23 |
| 26 | **Search scrape slot rotation** — 6 slots, each fires once/day via `advance_rotation`; discovery mode with VIN breakpoint and stop-on-error | 2026-03-20 |
| 26.3 | **Reduce max_workers 12→6** — ThreadPoolExecutor halved for immediate rate-limit relief | 2026-03-19 |
| 27.1 | **Detail scrape error rate alert** — Telegram alert when error rate >= 2.5% after each detail scrape run | 2026-03-20 |
| 27.2 | **Search scrape Akamai kill alert** — Job Poller Switch node detects ERR_HTTP2, sends Telegram with search_key/scope/page count | 2026-03-20 |
| 28 | **Dashboard quicklinks** — sidebar links to n8n, Search Config Admin, pgAdmin | 2026-03-20 |
| 30 | **Detailed run info in dashboard** — unified Recent Runs table (all types, 48h), throughput rate, ETA, error counts; enhanced active run indicator | 2026-03-23 |
| 31 | **pgAdmin for SQL access** — pgAdmin 4 container on port 5050, connected to cartracker DB | 2026-03-20 |
| 32 | **Force-grab stale vehicles in detail scrape** — added second pool for vehicles > 36h stale bypassing one-per-dealer rule | 2026-03-20 |
| 33 | **Error info on runs table** — `error_count` + `last_error` columns on `runs`; Job Poller aggregates from `scrape_jobs` on completion; dashboard shows errors | 2026-03-23 |
| 34 | **artifact_count subquery in Job Poller** — verified already filters by `run_id`; no fix needed | 2026-03-23 |
| 37 | **Carousel hint discovery pipeline** — Pool 3 in detail batch, make/model filtering via `int_scrape_targets`, VIN mapping via detail obs, dashboard metrics | 2026-03-20 |
| 38 | **SRP anti-detection** — Patchright (Playwright fork), UA rotation (Chrome v132-135), ZIP code pools (10 metros + 10 local), viewport rotation, human-like pacing (8-20s), random page order, sequential scope execution | 2026-03-21 |
| 39 | **Scrape targets seed + on-target filtering** — `scrape_targets.csv` seed, `int_scrape_targets` dbt model, `mart_vehicle_snapshot` filtered to on-target make/models only; cascades to `ops_vehicle_staleness` and detail scrape queue | 2026-03-22 |
| 40 | **Dashboard target filtering** — all dashboard queries filtered to on-target scrape targets; eliminated slow `int_vehicle_attributes` view joins; queries use `mart_deal_scores` or direct `int_scrape_targets` joins | 2026-03-23 |
| 41 | **dbt performance: staging materialization** — `stg_detail_observations` + `stg_srp_observations` converted from views to incremental tables; eliminates repeated 1.9M + 530k row scans | 2026-03-23 |
| 42 | **mart_deal_scores performance** — `detail_only_vins` CTE queries base table directly instead of staging view; partial index on `detail_observations(make, model, fetched_at)`; 6+ min → 200ms | 2026-03-23 |
| 43 | **Detail batch sizing** — capped at 1500 VINs (~15 min at 100/min); carousel VINs fill remaining capacity after stale VINs; designed for 30-min schedule | 2026-03-23 |
| 45 | **int_vehicle_attributes materialized** — converted from view to incremental table; detail > SRP source priority; added first_seen_at, last_seen_at, is_tracked; upstream VIN identity source of truth; eliminates repeated 2M+ row scans per mart build | 2026-03-23 |
| 46 | **Docker build from committed code** — dbt, dbt_runner, dashboard now COPY code at build time; source volume mounts removed; deploy workflow: git pull + docker compose build + docker compose up -d | 2026-03-23 |
| 47 | **dbt build lock** — `dbt_lock` single-row mutex table; dbt_runner acquires atomically before build, releases in `finally`; returns 409 when locked; 30-min stale timeout; n8n retries every 30s; dashboard shows lock status | 2026-03-23 |
| 48 | **Parallel detail scrapes** — `detail_scrape_claims` table keyed on `listing_id` with `status` column; atomic `INSERT ... ON CONFLICT DO UPDATE WHERE status != 'running'`; claims expire by run status, not TTL; two parallel runs get non-overlapping batches | 2026-03-23 |
| 49 | **ops_detail_scrape_queue dbt view** — moved 80-line batch selection SQL from n8n into dbt ops view; combines stale VINs (priority 1: one per dealer, priority 2: force-stale >36h) + unmapped carousel hints (priority 3); n8n query reduced to simple SELECT + claiming | 2026-03-23 |
| 36 | **Automate n8n workflow import** — entrypoint.sh runs `n8n import:workflow --separate` on container startup + `n8n update:workflow --all --active=true`; workflows volume-mounted from repo; `git pull + docker compose restart n8n` picks up changes | 2026-03-23 |
| 50 | **Dashboard refactor** — split 1108-line app.py into per-tab modules: db.py (shared), pages/pipeline_health.py, pages/inventory.py, pages/deals.py, pages/market_trends.py; app.py reduced to 47 lines (sidebar + routing) | 2026-03-23 |
| 51 | **Docs and setup update** — README architecture diagram updated for parallel scrapes + claiming; n8n section updated for auto-import; setup.ps1 step numbering fixed + post-setup messages updated; seed files added to manual setup instructions | 2026-03-23 |

---

## Plan 14: Codebase Audit Bug Fixes

**Status:** Mostly complete (14.2, 14.4, 14.6, 14.8 done). Remaining items are low-risk defensive fixes.

**14.1 — VIN case normalization** — `stg_detail_observations` passes raw VIN (only 1 lowercase VIN found)
**14.5 — Price events dedup** — `UNION ALL` with no dedup (only 1 actual duplicate found)
**14.9 — Browser singleton lock** — no `threading.Lock()` on `get_browser()` (low risk in practice)
**14.11 — Chrome fingerprint env var** — hardcoded `chrome131` (working fine currently)
**14.12 — max_safety_pages validator** — no bounds check (low risk)

---

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **52** — Carousel hint backlog strategy | 430k+ unmapped hints growing faster than processable; need pruning/prioritization strategy |
| 2 | **29** — n8n API + trigger button | Programmatic workflow control; trigger detail scrape from dashboard |
| 3 | **35** — dbt schema audit | Missing staging layers + ops consolidation |
| 4 | **53** — Dashboard cleanup/optimization | Pipeline Health tab is bloated; consider collapsible sections or sub-tabs |
| 5 | **14.1** — VIN case normalization | Defensive — only 1 affected VIN |
| 6 | **14.5** — Price events dedup | Defensive — only 1 duplicate found |
| 7 | **14.9 / 14.11 / 14.12** — Minor defensive fixes | Low risk |

---

## Plan 29: Set up n8n API

**Status:** Not started
**Priority:** Medium

- Set up the n8n API so we can interact with it programmatically.
- Fold in Plan 5 (webhook triggers) to this.
- Add button to trigger detail scrape from dashboard.

---

## Plan 35: dbt Schema Audit — staging gaps + ops consolidation

**Status:** Not started
**Priority:** Low

### 35.1 — Missing staging layers

4 raw tables are accessed directly by intermediate/mart models without a staging layer:

| Raw Table | Direct Consumers (bypassing staging) |
|---|---|
| `raw_artifacts` | `int_listing_days_on_market`, `int_model_price_benchmarks`, `int_price_percentiles_by_vin`, `mart_deal_scores` |
| `dealers` | `int_dealer_inventory`, `mart_deal_scores` |
| `detail_observations` | `int_latest_dealer_name_by_vin`, `mart_deal_scores` |
| `srp_observations` | dashboard queries |

### 35.2 — Ops schema: deprecate or expand?

`ops/` now contains 2 models: `ops_vehicle_staleness` and `ops_detail_scrape_queue`. Growing naturally as operational needs arise.

---

## Plan 36: Automate n8n workflow setup

**Status:** Complete (2026-03-23)

Implemented via option 3 (startup script). Custom `n8n/entrypoint.sh` runs `n8n import:workflow --separate --input=/workflows/` then `n8n update:workflow --all --active=true` before starting n8n. Workflows are volume-mounted from the repo. Deploy: `git pull + docker compose restart n8n`.

---

## Plan 52: Carousel hint backlog strategy

**Status:** Not started
**Priority:** High

430k+ unmapped carousel hints and growing faster than we can process. Need a strategy to:
- Prune hints for vehicles outside scrape targets
- Prioritize hints by recency or price relevance
- Cap the backlog to a manageable size
- Consider aging out hints older than N days

---

## Plan 53: Dashboard cleanup/optimization

**Status:** In progress
**Priority:** Medium

Pipeline Health tab has 18 sections — too much scrolling. Consider:
- Collapsible sections or st.expander for less-critical sections
- Sub-tabs within Pipeline Health (e.g., "Active Runs", "History", "System Health")
- Move Processor Activity and Postgres Health into a "System" sub-tab

File split complete (Plan 50). Stale backlog query updated to use ops_detail_scrape_queue with claim-aware filtering. Price freshness chart updated with STALE bucket.

---

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
