# Cartracker — Future Plans & Optimizations

---

## Plan 0: Fix Detail Page Scraper (curl_cffi)

**Status:** ✅ Implemented (2026-03-16)

### What Was Done
Replaced Python `requests` with `curl_cffi` in `scrape_detail.py`. Cloudflare was blocking all detail page fetches at the WAF level via TLS fingerprint detection. `curl_cffi` with `impersonate='chrome131'` bypasses this entirely — confirmed 200 responses with full structured data.

### Notes
- Periodically check if `chrome131` fingerprint gets blocked; can rotate to newer versions (`chrome136`, etc.)
- No Playwright or cookie harvesting needed — curl_cffi handles Cloudflare natively

---

## Plan 1: Search Sort-Order Rotation

**Status:** ✅ Implemented (2026-03-16)

### What Was Done
- Added `sort_order` and `sort_rotation` fields to `search_configs.params` for all 9 configs
- Updated `scrape_results.py` URL builder to append `&sort={sort_order}` when set
- Added "Rotate Sort Order" node to Scrape Listings n8n workflow JSON

**Active 4-day rotation:**
1. `list_price` — underpriced deals
2. `listed_at_desc` — fresh inventory
3. `best_deal` — Cars.com scored deals
4. `best_match_desc` — broad sweep

### Notes
- Measure overlap after first 2–3 cycles: count `listing_id` duplicates across runs with different sort orders to confirm incremental value
- `price_asc` + `newest` are the two highest-value sorts for deal detection

---

## Plan 2: Dealer-Grouped Detail Page Refresh

**Status:** Not started
**Priority:** Medium
**Complexity:** Medium (Phase 1 low, Phase 2 medium)

### Problem
Detail page refreshes are currently processed one-by-one or in arrival order. If a dealer reprices 10 cars at once, we'll refresh them scattered across many workflow cycles.

### Solution
**Phase 1 (Low effort):** Re-order the detail-refresh queue to process stale listings grouped by dealer — just a SQL `ORDER BY` change.

**Phase 2 (Medium effort):** Build a targeted sweep query that identifies dealers with the most overdue listings and launches focused refresh batches.

### Phase 1 — Queue Reordering
Change the "Get Batch to Process" query in Scrape Detail Pages to `ORDER BY seller_customer_id, tier1_observed_at ASC`.

### Phase 2 — Targeted Dealer Sweeps
1. Score dealers by `COUNT(*) stale listings`, `AVG(days_since_refresh)`, weighted by listing value
2. New n8n sub-workflow: "Dealer Sweep" — takes a `seller_customer_id`, fetches all stale listings for that dealer in one pass
3. Run dealer sweeps as a second daily job (e.g., 6am)

---

## Plan 3: Search Params Schema Fix (`max_listings` migration)

**Status:** Partially implemented
**Priority:** Medium

### Problem
Two conflicting `page_size` values exist:
- `search_configs.params.page_size = 100` (what we request)
- `result_per_page ≈ 22` (what Cars.com actually returns)

### Current State
- `max_listings` and `max_safety_pages` are now read from params ✅
- `max_pages` (per-scope dict) is now re-wired as the loop bound ✅ (fixed 2026-03-14)
- `page_size` param is still sent to Cars.com URL but has no real effect

### Remaining Work
1. Clarify in `search_configs` whether `page_size` should be removed or kept as a hint
2. Consider renaming to `requested_page_size` to make the semantic clear
3. Long term: remove `max_pages` entirely once `max_listings` cap math is validated

---

## Plan 4: Artifact Cleanup Pipeline

**Status:** ✅ Implemented (2026-01-xx)

Retention rules:
- `skip` artifacts → delete immediately
- `ok` artifacts → delete after 15 days
- `retry` artifacts → delete after 30 days

Implemented via:
- `deleted_at TIMESTAMPTZ` column on `raw_artifacts`
- `/cleanup/artifacts` API endpoint
- n8n "Cleanup Artifacts" daily workflow (2am)

### Backfill Status
- ~1.08M ok > 15 days artifacts at start
- 300k deleted via 3 manual runs at LIMIT 100k
- 57 ERROR.txt skip artifacts patched directly via SQL
- Remaining backfill: continue running workflow manually at LIMIT 100k until clear

---

## Plan 5: n8n Webhook Triggers

**Status:** To do (low priority)

Add webhook trigger nodes to key workflows so they can be triggered programmatically without opening the n8n UI.

Workflows to add webhooks to:
- Scrape Listings
- Scrape Detail Pages
- Cleanup Artifacts

URL format: `http://n8n:5678/webhook/{id}`

---

## Plan 6: Async Job Polling for Scraper API

**Status:** To do (medium priority)

### Problem
n8n holds an open HTTP connection for the entire duration of a scrape (up to ~10 min for 20 pages). Can't parallelize — all 9 models × 2 scopes = 18 calls run sequentially, taking ~3 hours total.

### Solution
Switch scraper API to async job pattern:
1. `POST /scrape_results` → immediately returns `{"job_id": "abc123", "status": "processing"}`
2. n8n polls `GET /scrape_results/status/{job_id}` every 30s
3. When status = `"done"`, response includes full artifacts payload

**Real payoff:** Fire all 18 scrape requests simultaneously. Total scrape time drops from ~3 hours to ~20 minutes.

### Implementation
- FastAPI: add background task runner, in-memory job store (or Redis), status endpoint
- n8n: Fire Request → Wait (30s) → Poll Status → IF done → continue

---

## Plan 7: dbt Materialized View Migration

**Status:** ✅ Implemented (2026-03-16)

`listing_current_state` and `vin_current_state` were raw SQL materialized views outside dbt's control and never refreshed automatically. Converted to dbt `materialized='table'` models:

- `dbt/models/intermediate/int_listing_current_state.sql`
- `dbt/models/intermediate/int_vin_current_state.sql`

Both are now rebuilt as part of the `stg_detail_observations+` DAG on every `after_detail` dbt run.

---

## Plan 8: 403 Artifact Poisoning Fix

**Status:** ✅ Implemented (2026-03-16)

### Problem
1,088,728 detail page artifacts with `http_status = 403` (Cloudflare block pages) were marked `status = 'ok'` in `artifact_processing`. The detail page parser processed the block HTML without erroring, making `ops_vehicle_staleness` believe those vehicles had fresh data. Result: only 49 candidates surfaced per detail scrape batch instead of thousands.

### Fix Applied
- Re-marked all 403 `artifact_processing` records as `ok` with message `'Cloudflare 403 block - no parseable data'`
- These will not be retried or parsed again
- `ops_vehicle_staleness` now correctly surfaces ~15k+ vehicles needing detail refresh

---

## Plan 9: Metabase Analytics Dashboard

**Status:** To do (high priority)
**Complexity:** Medium

### Goal
Build a Metabase dashboard that surfaces actionable intelligence from the data we're collecting — not just pipeline health, but actual deal-finding and market analysis.

### Proposed Dashboard Sections

**Pipeline Health**
- Last Search Scrape / Last Detail Scrape (with timezone correction — US Central)
- Below Last Search Scrape: **new vehicles added** count + **vehicles updated** count since that run
- Below Last Detail Scrape: **price updates** count since that run, broken down by source:
  - Updated via direct detail page load
  - Updated via carousel hint (no full page fetch needed)
- Runs over time by type (Search vs Detail)
- Artifact processing backlog (retry count by processor)
- Detail scrape success rate (200 vs 403 over time)

**Inventory Overview**
- Total active listings by make/model
- New listings added (last 24h, 7d, 30d)
- Listings going unlisted (sold/removed) over time
- Active listings by dealer

**Deal Finder**
- `mart_deal_scores` — listings ranked by deal score
- Price vs market median by model
- Price drop events (listings that dropped in price)
- Days on market distribution

**Market Trends**
- Average price by make/model over time
- Inventory levels by model (are they rising or falling?)
- Median days-on-market trends

### Notes
- Most data is already in `mart_vehicle_snapshot` and `mart_deal_scores`
- Timezone: all timestamps should display in US Central (`AT TIME ZONE 'America/Chicago'`)
- New vehicles added: count of VINs first seen in `srp_observations` within the run's time window
- Vehicles updated: count of VINs with a new `srp_observation` in that run (price or state change)
- Price updates via carousel: `detail_carousel_hints` source; via direct load: `detail_observations` source
- The carousel vs direct breakdown is a useful efficiency metric — carousel hits are cheaper (no full page fetch)

---

## Plan 10: Pipeline Durability

**Status:** ✅ Implemented (2026-03-16)

### What Was Done
1. **Auto-terminate stale runs** — "Pipeline Maintenance" node added to Cleanup Artifacts workflow; runs stuck `running` > 2 hours are set to `terminated` nightly at 2:30am
2. **Reset stuck artifact_processing** — same node resets `processing` → `retry` for records older than 15 minutes, preventing silent queue jams
3. **`pipeline_errors` table** — new DB table captures workflow name, execution ID, node name, error message, and error type
4. **Error Handler workflow** — new n8n workflow (`Error Handler.json`) with an Error Trigger that logs to `pipeline_errors`; import it and set it as the error workflow in Scrape Listings + Scrape Detail Pages settings

### Remaining Manual Step
- Import `Error Handler.json` into n8n
- In Scrape Listings settings → set Error Workflow to "Error Handler"
- In Scrape Detail Pages settings → set Error Workflow to "Error Handler"

### Metabase health queries (to add to dashboard — Plan 9)
```sql
-- Recent pipeline errors
SELECT workflow_name, node_name, error_message, occurred_at AT TIME ZONE 'America/Chicago'
FROM pipeline_errors ORDER BY occurred_at DESC LIMIT 20;

-- Runs terminated in last 7 days
SELECT trigger, COUNT(*), MAX(started_at) FROM runs
WHERE status = 'terminated' AND started_at > now() - interval '7 days'
GROUP BY trigger;
```

---

## Plan 11: Search Configuration Front End (+ Plan 3 Params Cleanup)

**Status:** To do — implementation plan ready
**Priority:** High
**Complexity:** Medium

### Goal
A web-based UI for managing `search_configs` without writing SQL. Add, edit, enable/disable, and remove vehicle searches from a browser. Rolled together with Plan 3 (params schema cleanup) since we'll be touching the params structure throughout.

### Tech Approach
FastAPI + Jinja2 templates served from the **existing scraper container** at a new `/admin` prefix. No new Docker services, no separate frontend build step. Simple HTML forms styled with a minimal CSS framework (e.g., Pico CSS — single `<link>` tag, no build tooling).

Accessible at: `http://localhost:8000/admin`

---

### Phase 1: Params Schema Cleanup (Plan 3)

Before building the UI, clean up the params schema so the front end has a clean, unambiguous contract.

**Changes:**
1. **Remove `page_size`** — has no effect (Cars.com ignores it; actual result count is ~22/page). Remove from params and from `scrape_results.py` URL builder.
2. **Remove `max_pages`** — replaced entirely by `max_listings`. The scraper loops pages until `collected >= max_listings` or `pages_attempted >= max_safety_pages`. No need for a page count cap when a listing count cap exists. `max_safety_pages` remains as a hard runaway guard.
3. **Add `scopes` array** — instead of always scraping both local and national, allow per-config scope control: `"scopes": ["local", "national"]`. This makes it manageable from the UI.

**Resulting clean params contract:**
```json
{
  "makes": ["Honda"],
  "models": ["Accord", "CR-V"],
  "zip": "60601",
  "radius_miles": 150,
  "scopes": ["local", "national"],
  "max_listings": 2000,
  "max_safety_pages": 500,
  "sort_order": "list_price",
  "sort_rotation": ["list_price", "listed_at_desc", "best_deal", "best_match_desc"]
}
```

**Files to update:**
- `scraper/processors/scrape_results.py` — remove `page_size` from URL builder, replace `max_pages` loop bound with `max_listings` + `max_safety_pages`
- DB migration — strip `page_size` and `max_pages` from all existing `search_configs` params, add `scopes`

---

### Phase 2: Backend API Routes

Add a new `scraper/routers/admin.py` module and mount it at `/admin` in `app.py`.

**Pydantic models (`scraper/models/search_config.py`):**
```python
class SearchConfigParams(BaseModel):
    makes: List[str]
    models: List[str]
    zip: str                          # 5-digit zip
    radius_miles: int = 150
    scopes: List[str] = ["local", "national"]
    max_listings: int = 2000
    max_safety_pages: int = 500       # hard runaway guard only
    sort_order: Optional[str] = "best_match_desc"
    sort_rotation: Optional[List[str]] = None

class SearchConfigCreate(BaseModel):
    search_key: str                   # immutable PK, slug format
    enabled: bool = True
    params: SearchConfigParams

class SearchConfigUpdate(BaseModel):
    enabled: Optional[bool] = None
    params: Optional[SearchConfigParams] = None
```

**API endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/admin/` | List view — all configs (HTML page) |
| `GET` | `/admin/new` | New config form (HTML page) |
| `GET` | `/admin/{search_key}/edit` | Edit config form (HTML page) |
| `POST` | `/admin/` | Create new config (form POST → redirect) |
| `POST` | `/admin/{search_key}` | Update config (form POST → redirect) |
| `POST` | `/admin/{search_key}/toggle` | Enable/disable toggle |
| `POST` | `/admin/{search_key}/delete` | Soft delete (set enabled=false + flag) |
| `GET` | `/admin/api/searches` | JSON API for list (future use) |

Note: Using POST for updates/deletes (not PUT/DELETE) since HTML forms only support GET/POST natively — no JavaScript required.

---

### Phase 3: UI Templates

Create `scraper/templates/admin/` with Jinja2 templates.

**`base.html`** — shared layout with Pico CSS, nav bar
**`searches/list.html`** — main dashboard

Layout:
```
┌─────────────────────────────────────────┐
│  Cartracker Admin          [+ New Search]│
├──────────────┬──────────────────────────┤
│ search_key   │ honda_accord             │
│ makes/models │ Honda — Accord, CR-V     │
│ zip / radius │ 60601 · 150mi            │
│ scopes       │ Local + National         │
│ max_listings │ 2,000                    │
│ sort_order   │ list_price (rotating)    │
│ enabled      │ ● Active    [Edit] [off] │
└──────────────┴──────────────────────────┘
```

**`searches/form.html`** — shared add/edit form

Fields:
- `search_key` — text input (readonly on edit)
- `makes` — comma-separated text input (displayed as tags)
- `models` — comma-separated text input (displayed as tags)
- `zip` — text input with 5-digit validation
- `radius_miles` — number input
- `scopes` — checkboxes: Local / National
- `max_listings` — number input (e.g. 2000)
- `max_safety_pages` — number input (e.g. 500, advanced/collapsible)
- `sort_rotation` — checkbox group (list_price, listed_at_desc, best_deal, best_match_desc)
- `enabled` — toggle

---

### Phase 4: Database Connection

Add a simple async DB connection to the scraper app using `asyncpg` (already available in the Python ecosystem, lightweight). Connection string from environment variables already defined in docker-compose.

```python
# scraper/db.py
import asyncpg, os

async def get_db():
    return await asyncpg.connect(os.environ["DATABASE_URL"])
```

Or reuse a connection pool initialized on app startup in `app.py`.

---

### Implementation Order

1. **DB migration** — normalize existing `search_configs` params (remove `page_size` and `max_pages`, add `scopes`)
2. **`scrape_results.py` cleanup** — remove `page_size` from URL builder, replace `max_pages` loop bound with `max_listings` + `max_safety_pages`, respect `scopes` param
3. **`scraper/models/search_config.py`** — Pydantic models with validation
4. **`scraper/routers/admin.py`** — API + HTML endpoints
5. **`scraper/templates/admin/`** — Jinja2 templates (base → list → form)
6. **Mount in `app.py`** — `app.include_router(admin_router, prefix="/admin")`
7. **Install deps** — add `jinja2`, `python-multipart`, `asyncpg` to `requirements.txt`
8. **Test** — create a new search config, edit it, toggle it off, verify n8n still picks it up correctly

---

### Out of Scope (for now)
- Authentication — admin is localhost-only, no auth needed for now
- Live scrape triggering from UI — that stays in n8n (could be added via Plan 5 webhooks later)
- Cars.com make/model autocomplete — plain text input with comma separation is sufficient initially
