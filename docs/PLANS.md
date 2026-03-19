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

---

## Plan 16: Pipeline Efficiency

**Status:** 16.1 + 16.2 done. 16.3 monitoring.

**16.3 — Monitor detail scrape volume (~March 20)**
- If daily detail volume stabilizes ~6-8K: no changes needed.
- If still 30K+/day: change batch query to filter on `is_full_details_stale` only.

---

## Plan 20: dbt + Postgres Health in Dashboard

**Status:** Done (2026-03-19)

Expanded Pipeline Health section with dbt build time/status, active Postgres connections, long-running queries, and lock visibility.

---

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **27.2** — Search scrape Akamai alert | Immediate signal when IP gets rate-limited |
| 2 | **27.1** — Detail scrape error rate alert | Alert when >20% of detail pages fail |
| 3 | **25.2/25.3** — Bridge dealer ID systems | Unlocks dealer data in mart |
| 6 | **14.1** — VIN case normalization | Defensive — only 1 affected VIN |
| 7 | **14.5** — Price events dedup | Defensive — only 1 duplicate found |
| 8 | **16.3** — Monitor detail volume | Wait until ~March 20 |
| 9 | **14.9 / 14.11 / 14.12** — Minor defensive fixes | Low risk |
| 10 | **5** — Webhook triggers | Nice-to-have |

---

## Plan 25: Bridge Dealer ID Systems

**Status:** 25.1 done. 25.2/25.3/25.4 ready to implement.
**Priority:** High

cars.com uses two different dealer identifiers:
- **UUID** (`150b427b-c147-5a18-a733-cf5aa95519d0`) — in SRP JSON, stored in `srp_observations.seller_customer_id`
- **Numeric** (`735`) — in detail page HTML, already parsed by `parse_detail_page.py` and written to `dealers.customer_id`, but never stored in `detail_observations`

**25.1 — Preserve UUID when detail becomes T1 (done 2026-03-18)**
`int_latest_tier1_observation_by_vin` was overwriting `seller_customer_id` with `null` when a detail observation became the most recent T1. Fixed.

---

**25.2 — Store numeric `customer_id` in `detail_observations`**

The parser already extracts `customer_id` from the detail page. The `Upsert Dealers` node in Parse Detail Pages already uses it. It just never gets written to `detail_observations`.

*Step 1 — DB migration (run first):*
```sql
ALTER TABLE detail_observations ADD COLUMN IF NOT EXISTS customer_id text;
CREATE INDEX IF NOT EXISTS ix_detail_observations_customer_id
  ON detail_observations (customer_id) WHERE customer_id IS NOT NULL;
```

*Step 2 — n8n "Write Detail Observations" node:*
Add to the `rows` CTE SELECT:
```sql
NULLIF(p."primary"->>'customer_id', '') AS customer_id
```
Add `customer_id` to the INSERT column list and SELECT from rows.

*Step 3 — `stg_detail_observations.sql`:*
Add `d.customer_id` to the projection.

*Step 4 — `int_latest_tier1_observation_by_vin.sql`:*
- In the `detail` CTE: add `d.customer_id` (alongside the existing `null::text as seller_customer_id`)
- In the `srp` CTE: add `null::text as customer_id` placeholder
- In the `ranked` CTE: add window `max(case when source = 'detail' then customer_id end) over (partition by vin) as detail_customer_id`
- In final SELECT: expose as `detail_customer_id as customer_id`
- ⚠️ Requires `dbt run --full-refresh` on this incremental model (the workflow's `full_refresh: true` flag handles this automatically on next run)

*Step 5 — `mart_vehicle_snapshot.sql`:*
Add `t.customer_id` to the projection so downstream marts can access it.

---

**25.3 — Fix the dealer join in `mart_deal_scores.sql`**

The existing join is broken because it matches UUID against numeric ID:
```sql
-- Current (broken — UUID never matches numeric):
left join dealers dlr on dlr.customer_id = a.seller_customer_id

-- Fix (use numeric customer_id from mart_vehicle_snapshot):
left join dealers dlr on dlr.customer_id = v.customer_id
```
Where `v` is the existing `mart_vehicle_snapshot` alias. This immediately surfaces `dlr.name`, `dlr.phone`, `dlr.rating`, `dlr.city`, `dlr.state` for all detail-scraped VINs.

---

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

## Plan 26: Search Scrape Reliability — Stagger + Retry

**Status:** 26.3 done. 26.1 backend done — n8n workflow pending.
**Priority:** High

Currently all 9 search configs fire simultaneously at :00 with 12 async workers. Cars.com's rate limiting is cumulative — early search keys succeed (~30% error rate), later ones hit a throttling threshold (~67% error rate). We're consistently getting only ~60% of requested pages.

**26.1 — Stagger search keys across time (rotation index approach)**

Approach: workflow runs every 40 min, claims one search key per run via `/search_configs/advance_rotation`, fires only that key's scopes, then stops. With 9 configs × 40 min = 6h cycle, each config gets scraped ~twice a day, spaced ~12h apart.

**Backend (done 2026-03-19):**
- Added `rotation_order integer` and `last_queued_at timestamptz` to `search_configs`
- New `POST /search_configs/advance_rotation?min_idle_minutes=719` endpoint — atomically claims next due config (SELECT FOR UPDATE SKIP LOCKED), sets `last_queued_at = now()`, returns `{search_key, params, scopes}` or `{search_key: null}` if nothing due

**DB migration to run:**
```sql
ALTER TABLE search_configs ADD COLUMN IF NOT EXISTS rotation_order integer;
ALTER TABLE search_configs ADD COLUMN IF NOT EXISTS last_queued_at timestamptz;

-- Backfill rotation_order (alphabetical by search_key; adjust order as desired)
UPDATE search_configs SET rotation_order = sub.rn
FROM (
    SELECT search_key, ROW_NUMBER() OVER (ORDER BY search_key) AS rn
    FROM search_configs
) sub
WHERE search_configs.search_key = sub.search_key;
```

**n8n workflow changes needed (TODO):**
1. Replace the "Load Search Configs" Postgres node with `POST /search_configs/advance_rotation`
2. Add an IF node after it: if `search_key` is null → stop (nothing due yet)
3. The existing scrape-jobs loop now only iterates over `scopes` for the one returned config
4. Change the schedule trigger from twice-daily to every 40 minutes

**26.2 — Retry failed SRP pages** *(Cancelled)*
Originally planned to re-queue error artifacts for a second pass. Superseded by discovery mode — errors are now intentional early stops on Akamai rate limits. Retrying a rate-limited IP would worsen reputation decay, not recover it.

**26.3 — Reduce max_workers (done 2026-03-19)**
Dropped `ThreadPoolExecutor` from 12 → 6. Less aggressive on cars.com's rate limiter.

---

## Plan 27: Telegram Alerts — Scrape Health

**Status:** Not started
**Priority:** Medium

Two distinct alert scenarios with different trigger points:

---

**27.1 — Detail scrape error rate alert**

After all detail pages finish scraping (after the Loop Over Items in Scrape Detail Pages), query the run's error rate and fire a Telegram message if it exceeds a threshold.

Implementation:
- After "Mark Run Done", add a Postgres node that queries:
  ```sql
  SELECT
    COUNT(*) FILTER (WHERE http_status IS NULL OR http_status >= 400) AS errors,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE http_status IS NULL OR http_status >= 400) / NULLIF(COUNT(*), 0), 1) AS error_pct
  FROM raw_artifacts
  WHERE run_id = '<run_id>'::uuid
    AND artifact_type = 'detail_page'
  ```
- Add an IF node: if `error_pct >= 20` → send Telegram message
- Message format: `⚠️ Detail scrape error rate: {{error_pct}}% ({{errors}}/{{total}} pages failed) — Run {{run_id}}`
- Threshold: 20% (tunable)

---

**27.2 — Search scrape Akamai kill alert**

With discovery mode, an Akamai rate-limit kill (`ERR_HTTP2_PROTOCOL_ERROR`) causes the scraper to stop immediately and return a failed job. The Job Poller detects these as `status = 'failed'` jobs. We should alert when this happens.

Implementation:
- In the **Job Poller** workflow, after processing completed jobs, add a check: count how many jobs in the current poll batch have `status = 'failed'` AND contain `ERR_HTTP2_PROTOCOL_ERROR` in the error field
- If any are found, send a Telegram alert
- Alternatively: add a Telegram node in **Scrape Listings** at the "Mark Run Failed" path (after the Job Poller marks a run as failed due to all jobs failing)
- Message format: `🚨 Akamai rate limit hit — {{search_key}} ({{scope}}) stopped at page {{page_num}}. IP cool-down in effect. Next slot fires in ~4h.`
- This is a signal to watch IP reputation recovery; not necessarily actionable immediately

---

**27.3 — Telegram credential**

Telegram bot is already configured in n8n from Plan 15 (Error Handler). Reuse the same bot/chat ID credential for consistency.

---

## Future Ideas (Unprioritized)

- **Price alert notifications** — email/SMS when a VIN drops below a threshold
- **Dealer reputation scoring** — aggregate rating, inventory size, price competitiveness
- **Geographic heatmaps** — map view of inventory density and pricing by region
- **VIN decode enrichment** — NHTSA VIN decoder for specs not on Cars.com (engine, transmission, packages)
- **Historical deal analysis** — track which deal-scored VINs actually sold (went unlisted) and at what price
