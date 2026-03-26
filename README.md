# CarTracker

CarTracker tracks local and national new car inventory on Cars.com, collecting accurate, timely pricing and listing data across thousands of vehicles to surface the best deals on specific makes and models.

## Architecture

Seven containerized services orchestrated via Docker Compose:

```
Schedule Trigger (30 min)           Schedule Trigger (15 min)
        |                                   |
   Scrape Listings              Scrape Detail Pages (parallel OK)
   (n8n workflow)                (n8n workflow)
        |                               |
  /advance_rotation            ops_detail_scrape_queue (dbt view)
  claims next slot              priority-ordered batch
  (6 slots, ~4h gap)           (stale VINs + carousel hints)
        |                               |
  Explode Configs × Scopes     Atomic claim via detail_scrape_claims
  fan-out per search_key        (ON CONFLICT DO UPDATE, no duplicates)
        |                               |
   Scraper API                  Scraper API
   POST /scrape_results         POST /scrape_detail
   (async, returns job_id)              |
        |                        raw_artifacts table
   Job Poller (1 min)                   |
   polls /jobs/completed          Parse Detail Pages
   inserts artifacts              (n8n sub-workflow)
        |                               |
   Results Processing           dbt_runner (locked build)
   (n8n sub-workflow)           detail_observations
        |                       detail_carousel_hints
   srp_observations               dealers
```

**Discovery mode:** SRP pages are sorted `listed_at_desc`. Page 1 is fetched first to learn the total page count, then remaining pages are fetched in **randomized order** to avoid sequential pagination fingerprinting. Pagination stops when ≥80% of VINs on a page are already known (seen in the last 14 days) or a rolling 5-page window averages < 1 new VIN per page. Max 30 pages per job. On Akamai rate-limit (`ERR_HTTP2_PROTOCOL_ERROR`), the scraper stops immediately and the job fails.

### Services

| Service | Port | Description |
|---------|------|-------------|
| **postgres** | 5432 | PostgreSQL 16 — all config, raw data, parsed observations, and analytics |
| **scraper** | 8000 | FastAPI — fetches SRP pages via Patchright (Playwright fork with anti-detection patches) and detail pages via curl_cffi. Anti-fingerprinting: rotates Chrome UA + sec-ch-ua headers, ZIP codes, viewports, and uses human-like pacing (8-20s between pages). Async job queue with ThreadPoolExecutor(4). Startup recovery clears orphaned jobs/runs on container restart. Admin UI at `/admin` for search config CRUD |
| **n8n** | 5678 | Workflow orchestration — 7 workflows handle scraping, polling, parsing, cleanup, and error handling |
| **dbt** | — | dbt-postgres 1.8.2 — 21 models across staging/intermediate/marts/ops schemas. Code is baked into the image at build time. Runs on-demand via dbt_runner |
| **dbt_runner** | 8081 | FastAPI wrapper that lets n8n trigger `dbt build` via HTTP. Includes a database-level mutex (`dbt_lock` table) to prevent concurrent builds — returns 409 if a build is already running. `GET /dbt/lock` endpoint for status checks |
| **pgadmin** | 5050 | pgAdmin 4 — web-based SQL IDE for querying and browsing the database |
| **dashboard** | 8501 | Streamlit — 4-tab analytics dashboard (Pipeline Health, Inventory Overview, Deal Finder, Market Trends). Sidebar quicklinks to n8n, admin UI, pgAdmin |

### n8n Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| **Scrape Listings** | Every 30 min | Polls `POST /advance_rotation` — claims the next due rotation slot (6 slots, ≥1439 min idle per slot, ≥230 min gap between any runs). Explodes each slot's configs × scopes into individual async scrape jobs. |
| **Job Poller** | Every 1 min | Expires orphaned jobs (>30 min), polls `/jobs/completed`, inserts artifacts, marks runs done. Sends Telegram alert on Akamai rate-limit kills (ERR_HTTP2). |
| **Scrape Detail Pages** | Every 15 min | Queries `ops_detail_scrape_queue` dbt view (stale VINs + unmapped carousel hints, priority-ordered). Atomically claims batch via `detail_scrape_claims` table — enables parallel runs without duplicate work. Batch capped at 1,500 listings. Fetches detail pages, batch-inserts artifacts, triggers parse + dbt build. Telegram alert if error rate ≥2.5%. |
| **Results Processing** | Sub-workflow | Parses SRP HTML artifacts into `srp_observations` rows |
| **Parse Detail Pages** | Sub-workflow | Parses detail HTML into `detail_observations`, `detail_carousel_hints`, and `dealers` |
| **Cleanup Artifacts** | Daily | Applies retention rules to old raw HTML files |
| **Error Handler** | On error | Logs to `pipeline_errors` table and sends Telegram alerts when any workflow fails |

## Data Model

### Raw Tables (written by scraper/n8n)

- `raw_artifacts` — every HTTP response (filepath, status, sha256, etc.)
- `srp_observations` — parsed search result page listings (VIN, price, make, model, dealer)
- `detail_observations` — parsed vehicle detail pages (full specs, listing state, numeric `customer_id`)
- `detail_carousel_hints` — similar-vehicle prices from detail page carousels
- `dealers` — dealer names and IDs parsed from detail pages
- `search_configs` — search definitions (zip, radius, make/model, sort rotation, `rotation_slot`, `last_queued_at`, `rotation_order`)
- `runs` — run lifecycle tracking (status, trigger, progress_count, total_count)
- `scrape_jobs` — async job tracking (queued/running/completed/fetched/failed)
- `pipeline_errors` — error log for n8n Error Handler
- `artifact_processing` — tracks which artifacts have been parsed
- `dbt_lock` — single-row mutex preventing concurrent dbt builds (locked/locked_at/locked_by)
- `dbt_runs` — dbt build history (duration, pass/error/skip counts, intent)
- `detail_scrape_claims` — listing_id claims for parallel detail scrapes (keyed on listing_id, linked to run_id)
- `processing_runs` — tracks detail-page parse run lifecycle (status, progress_count, total_count, error_count); written by the Parse Detail Pages workflow

### dbt Models (21 active)

**Staging** — incremental tables over raw data:
- `stg_srp_observations`, `stg_detail_observations`, `stg_detail_carousel_hints`

**Intermediate** — business logic:
- `int_listing_to_vin` — maps listing_id to VIN (incremental)
- `int_vehicle_attributes` — canonical make/model/trim/year per VIN with detail > SRP priority (incremental table, not a view)
- `int_scrape_targets` — distinct make/model pairs from search_configs, used to filter on-target vehicles
- `int_latest_price_by_vin` — most recent price per VIN across all sources (incremental)
- `int_latest_tier1_observation_by_vin` — latest SRP/detail observation per VIN (incremental)
- `int_price_events` — union of all price observations (SRP + detail + carousel)
- `int_price_history_by_vin` — price trajectory per VIN (first price, drops, min/max)
- `int_price_percentiles_by_vin` — price rank within make/model/trim cohort
- `int_carousel_price_events_mapped` — carousel hints mapped to VINs via listing_to_vin
- `int_carousel_price_events_unmapped` — on-target carousel hints not yet mapped to a VIN (table, feeds detail scrape queue)
- `int_listing_days_on_market` — days on market, first/last seen per VIN
- `int_model_price_benchmarks` — national price percentiles by make/model/trim
- `int_dealer_inventory` — active vehicle count per dealer per make/model

**Marts** — final analytics tables:
- `mart_vehicle_snapshot` — current state per VIN (price + observation joined, filtered to scrape targets)
- `mart_deal_scores` — composite deal score (0-100) with tier (excellent/good/fair/weak)

**Ops** — operational views:
- `ops_vehicle_staleness` — identifies stale VINs needing a detail refresh
- `ops_detail_scrape_queue` — priority-ordered queue combining stale VINs + unmapped carousel hints (queried by n8n)

## Refresh Strategy

- **Search scrapes** use slot-based rotation: 6 slots of 1-2 search configs each fire ~once per day. Two guards enforce spacing: `min_idle_minutes=1439` (23h59m per slot) and `min_gap_minutes=230` (~4h between any runs). The 30-min n8n schedule is a dumb clock; the API is authoritative. Sort order is `listed_at_desc` (discovery mode — surfaces newly listed vehicles first).
- **Discovery mode** stops pagination early: if ≥80% of VINs on a page were already seen in the last 14 days, or a rolling 5-page window averages < 1 new VIN per page, the job exits. Pages are fetched in **randomized order** (after page 1) to avoid sequential pagination fingerprinting. Max 30 pages per job. This keeps daily request volume low (~200–500/day) to avoid Akamai rate limiting.
- **Detail scrapes** run every 15 minutes, pulling from the `ops_detail_scrape_queue` dbt view. Priority 1: one stale VIN per dealer. Priority 2: force-grab vehicles >36h stale. Priority 3: unmapped carousel hints fill remaining capacity. Batch capped at 1,500 listings (~15 min at ~100 VINs/min). Atomic `detail_scrape_claims` table prevents duplicate work across parallel runs.
- **Price data** is stale after 24 hours; **full details** after 7 days
- **dbt builds** run incrementally after each detail scrape completes. A `dbt_lock` mutex prevents concurrent builds — if locked, n8n retries every 30 seconds until the lock is released.

## Running Locally

### Prerequisites

- Docker Desktop (Windows) or Docker Engine + Docker Compose (Linux/macOS)
- Git

### Quick start (Windows PowerShell)

```powershell
git clone https://github.com/whitewalls86/new_car_tracker.git
cd new_car_tracker
cp .env.example .env        # Edit .env to set a strong POSTGRES_PASSWORD
.\scripts\setup.ps1          # Creates volumes, starts services, inits DB, runs dbt
```

### Manual setup

```bash
# 1. Create .env from template
cp .env.example .env
# Edit .env to set POSTGRES_PASSWORD

# 2. Create external Docker resources
docker network create cartracker-net
docker volume create cartracker_pgdata
docker volume create cartracker_raw
docker volume create n8n_data

# 3. Start all services
docker compose up -d

# 4. Initialize the database schema
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/schema/schema_new.sql

# 5. Load seed data (search config, dbt lock, scrape claims)
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/example_search_config.sql
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/dbt_lock.sql
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/detail_scrape_claims.sql

# 6. Install dbt packages and run initial build
docker compose run --rm dbt deps
docker compose run --rm dbt build
```

### Configure n8n workflows

Workflows are auto-imported from `n8n/workflows/` and activated on container startup via the custom entrypoint. On first setup you still need to create and wire the database credential:

1. Open n8n at `http://localhost:5678`
2. Create a **Postgres credential** (host: `postgres`, user: `cartracker`, password: from `.env`, database: `cartracker`)
3. Open each workflow and wire the Postgres credential into the Postgres nodes
4. Verify all 7 workflows are active (they should be — the entrypoint activates them automatically)

### Add search configurations

Use the admin UI at `http://localhost:8000/admin` to add make/model searches. The setup script loads one example (Honda CR-V Hybrid). Each config defines a zip code, radius, make/model filters, and a `rotation_slot` (1–6) that controls which daily firing window it belongs to. Configs sharing a slot fire together; slots fire ~4 hours apart.

### Service URLs

| Service | URL |
|---------|-----|
| n8n UI | http://localhost:5678 |
| Scraper API | http://localhost:8000 |
| Scraper Admin UI | http://localhost:8000/admin |
| dbt Runner | http://localhost:8081 |
| pgAdmin | http://localhost:5050 |
| Dashboard | http://localhost:8501 |

## Project Structure

```
cartracker-scraper/
  scraper/
    app.py                  # FastAPI app — routes, async job queue, startup recovery
    routers/admin.py        # /admin CRUD UI for search_configs
    processors/
      scrape_results.py     # SRP page fetcher — discovery mode, VIN breakpoint
      scrape_detail.py      # Detail page fetcher (curl_cffi)
      parse_detail_page.py  # HTML parser for detail pages
      results_page_cards.py # HTML parser for SRP cards
      browser.py            # Browser singleton (Patchright)
      fingerprint.py        # UA/sec-ch-ua/viewport/ZIP rotation profiles
      cleanup_artifacts.py  # Artifact retention logic
    models/search_config.py # Pydantic models for search config
  dashboard/
    app.py                  # Streamlit dashboard (4 tabs)
  dbt/
    models/
      staging/              # 3 staging models (incremental)
      intermediate/         # 14 intermediate models
      marts/                # 2 mart models
      ops/                  # 2 ops models
    dbt_project.yml
    profiles.yml
  dbt_runner/
    app.py                  # FastAPI wrapper for dbt build
  n8n/workflows/            # 7 workflow JSON exports
  db/schema/schema_new.sql  # Full database schema (pg_dump)
  db/seed/                  # Seed data (search config, dbt_lock, detail_scrape_claims)
  scripts/setup.ps1         # Windows first-time setup script
  docs/PLANS.md             # Roadmap and completed work log
  .env.example              # Environment variable template
  docker-compose.yml
```
