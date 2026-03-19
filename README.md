# CarTracker

CarTracker tracks local and national new car inventory on Cars.com, collecting accurate, timely pricing and listing data across thousands of vehicles to surface the best deals on specific makes and models.

## Architecture

Six containerized services orchestrated via Docker Compose:

```
Schedule Trigger (30 min)           Schedule Trigger (1h)
        |                                   |
   Scrape Listings              Scrape Detail Pages
   (n8n workflow)                (n8n workflow)
        |                               |
  /advance_rotation            waits if search scrape
  claims next slot              is running or SRP
  (6 slots, ~4h gap)           artifacts are pending
        |                               |
  Explode Configs × Scopes      Scraper API
  fan-out per search_key         POST /scrape_detail
        |                               |
   Scraper API                  raw_artifacts table
   POST /scrape_results                 |
   (async, returns job_id)        dbt_runner
        |                        (incremental build)
   Job Poller (1 min)                   |
   polls /jobs/completed          Parse Detail Pages
   inserts artifacts              (n8n sub-workflow)
        |                               |
   Results Processing         detail_observations
   (n8n sub-workflow)         detail_carousel_hints
        |                         dealers
   srp_observations
```

**Discovery mode:** SRP pages are sorted `listed_at_desc` and paginated until ≥80% of VINs on a page are already known (seen in the last 14 days) or 3 consecutive pages yield 0 new VINs — whichever comes first. Max 30 pages per job. On Akamai rate-limit (`ERR_HTTP2_PROTOCOL_ERROR`), the scraper stops immediately and the job fails.

### Services

| Service | Port | Description |
|---------|------|-------------|
| **postgres** | 5432 | PostgreSQL 16 — all config, raw data, parsed observations, and analytics |
| **scraper** | 8000 | FastAPI — fetches SRP and detail pages via Playwright (Akamai bypass). Async job queue with ThreadPoolExecutor(6). Startup recovery clears orphaned jobs/runs on container restart. Admin UI at `/admin` for search config CRUD |
| **n8n** | 5678 | Workflow orchestration — 7 workflows handle scraping, polling, parsing, cleanup, and error handling |
| **dbt** | — | dbt-postgres 1.8.2 — 16 models across staging/intermediate/marts/ops schemas. Runs on-demand via dbt_runner |
| **dbt_runner** | 8081 | Lightweight FastAPI wrapper that lets n8n trigger `dbt build` via HTTP |
| **dashboard** | 8501 | Streamlit — 4-tab analytics dashboard (Pipeline Health, Inventory Overview, Deal Finder, Market Trends) |

### n8n Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| **Scrape Listings** | Every 30 min | Polls `POST /advance_rotation` — claims the next due rotation slot (6 slots, ≥239 min idle required). Explodes each slot's configs × scopes into individual async scrape jobs. |
| **Job Poller** | Every 1 min | Expires orphaned jobs (>30 min), polls `/jobs/completed`, inserts artifacts, marks runs done |
| **Scrape Detail Pages** | Every 1h | Waits if a search scrape is running or fresh SRP artifacts are unprocessed. Queries stale VINs (1 per dealer), fetches detail pages, batch-inserts artifacts, triggers parse + dbt build |
| **Results Processing** | Sub-workflow | Parses SRP HTML artifacts into `srp_observations` rows |
| **Parse Detail Pages** | Sub-workflow | Parses detail HTML into `detail_observations`, `detail_carousel_hints`, and `dealers` |
| **Cleanup Artifacts** | Daily | Applies retention rules to old raw HTML files |
| **Error Handler** | On error | Logs to `pipeline_errors` table and sends Telegram alerts when any workflow fails |

## Data Model

### Raw Tables (written by scraper/n8n)

- `raw_artifacts` — every HTTP response (filepath, status, sha256, etc.)
- `srp_observations` — parsed search result page listings (VIN, price, make, model, dealer)
- `detail_observations` — parsed vehicle detail pages (full specs, listing state)
- `detail_carousel_hints` — similar-vehicle prices from detail page carousels
- `dealers` — dealer names and IDs parsed from detail pages
- `search_configs` — search definitions (zip, radius, make/model, sort rotation, rotation_slot, last_queued_at)
- `runs` — run lifecycle tracking (status, trigger, progress_count, total_count)
- `scrape_jobs` — async job tracking (queued/running/completed/fetched/failed)
- `pipeline_errors` — error log for n8n Error Handler
- `artifact_processing` — tracks which artifacts have been parsed

### dbt Models (16 active)

**Staging** — clean projections over raw tables:
- `stg_srp_observations`, `stg_detail_observations`, `stg_detail_carousel_hints`

**Intermediate** — business logic:
- `int_listing_to_vin` — maps listing_id to VIN (incremental)
- `int_latest_price_by_vin` — most recent price per VIN across all sources (incremental)
- `int_latest_tier1_observation_by_vin` — latest SRP/detail observation per VIN (incremental)
- `int_price_events` — union of all price observations (SRP + detail + carousel)
- `int_price_history_by_vin` — price trajectory per VIN (first price, drops, min/max)
- `int_carousel_price_events_mapped` — carousel hints mapped to VINs via listing_to_vin
- `int_listing_days_on_market` — days on market, first/last seen per VIN
- `int_srp_vehicle_attributes` — latest make/model/trim/MSRP per VIN
- `int_model_price_benchmarks` — national price percentiles by make/model/trim
- `int_dealer_inventory` — active vehicle count per dealer per make/model

**Marts** — final analytics tables:
- `mart_vehicle_snapshot` — current state per VIN (price + observation joined)
- `mart_deal_scores` — composite deal score (0-100) with tier (excellent/good/fair/weak)

**Ops** — operational views:
- `ops_vehicle_staleness` — identifies stale VINs for the detail scrape batch query

## Refresh Strategy

- **Search scrapes** use slot-based rotation: 6 slots of 1-2 search configs each fire ~once per day with ≥4h spacing between slots. The 30-min n8n schedule is a dumb clock; the `min_idle_minutes=239` gate in `/advance_rotation` is authoritative. Sort order is `listed_at_desc` (discovery mode — surfaces newly listed vehicles first).
- **Discovery mode** stops pagination early: if ≥80% of VINs on a page were already seen in the last 14 days, or 3 consecutive pages yield 0 new VINs, the job exits. Max 30 pages per job. This keeps daily request volume low (~200–500/day) to avoid Akamai rate limiting.
- **Detail scrapes** run hourly, targeting 1 stale VIN per dealer (leverages carousel hints for neighboring inventory)
- **Price data** is stale after 24 hours; **full details** after 7 days
- **Detail scrapes wait** if a search scrape is running or if SRP artifacts from the last 45 minutes haven't been processed yet
- dbt builds run incrementally after each detail scrape completes

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

# 5. Load example search config (Honda CR-V Hybrid)
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/example_search_config.sql

# 6. Install dbt packages and run initial build
docker compose run --rm dbt deps
docker compose run --rm dbt build
```

### Configure n8n workflows

1. Open n8n at `http://localhost:5678`
2. Create a **Postgres credential** (host: `postgres`, user: `cartracker`, password: from `.env`, database: `cartracker`)
3. Import each JSON file from `n8n/workflows/` via **Settings > Import Workflow**
4. Wire the Postgres credential into each workflow's Postgres nodes
5. Activate the workflows

### Add search configurations

Use the admin UI at `http://localhost:8000/admin` to add make/model searches. The setup script loads one example (Honda CR-V Hybrid). Each config defines a zip code, radius, make/model filters, and a `rotation_slot` (1–6) that controls which daily firing window it belongs to. Configs sharing a slot fire together; slots fire ~4 hours apart.

### Service URLs

| Service | URL |
|---------|-----|
| n8n UI | http://localhost:5678 |
| Scraper API | http://localhost:8000 |
| Scraper Admin UI | http://localhost:8000/admin |
| dbt Runner | http://localhost:8081 |
| Dashboard | http://localhost:8501 |

## Project Structure

```
cartracker-scraper/
  scraper/
    app.py                  # FastAPI app — routes, async job queue, startup recovery
    routers/admin.py        # /admin CRUD UI for search_configs
    processors/
      scrape_results.py     # SRP page fetcher — discovery mode, VIN breakpoint
      scrape_detail.py      # Detail page fetcher (Playwright)
      parse_detail_page.py  # HTML parser for detail pages
      results_page_cards.py # HTML parser for SRP cards
      browser.py            # Browser singleton (Playwright)
      cleanup_artifacts.py  # Artifact retention logic
    models/search_config.py # Pydantic models for search config
  dashboard/
    app.py                  # Streamlit dashboard (4 tabs)
  dbt/
    models/
      staging/              # 3 staging models
      intermediate/         # 10 intermediate models
      marts/                # 2 mart models
      ops/                  # 1 ops model
    dbt_project.yml
    profiles.yml
  dbt_runner/
    app.py                  # FastAPI wrapper for dbt build
  n8n/workflows/            # 7 workflow JSON exports
  db/schema/schema_new.sql  # Full database schema (pg_dump)
  db/seed/                  # Example search config seed data
  scripts/setup.ps1         # Windows first-time setup script
  docs/PLANS.md             # Roadmap and completed work log
  .env.example              # Environment variable template
  docker-compose.yml
```
