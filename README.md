# CarTracker

CarTracker tracks local and national new car inventory on Cars.com, collecting accurate, timely pricing and listing data across thousands of vehicles to surface the best deals on specific makes and models.

## Architecture

Ten containerized services orchestrated via Docker Compose:

```
Schedule Trigger (30 min)            Schedule Trigger (15 min)
        |                                    |
  Scrape Listings                  Scrape Detail Pages V2
  (n8n workflow)                    (n8n workflow)
        |                                    |
  /advance_rotation               ops_detail_scrape_queue (dbt view)
  claims next slot                 priority-ordered batch
  (6 slots, ~4h gap)              (stale VINs + carousel hints)
        |                                    |
  Explode Configs × Scopes        Atomic claim via detail_scrape_claims
  fan-out per search_key           (ON CONFLICT DO UPDATE, no duplicates)
        |                                    |
  Scraper API (internal)          Scraper API (internal)
  POST /scrape_results             POST /scrape_detail_batch
  (async, returns job_id)                   |
        |                           raw_artifacts table
  Job Poller V2 (1 min)                     |
  polls /jobs/completed             Results Processing
  inserts artifacts, syncs          (n8n sub-workflow, unified
  blocked_cooldown                   detail + SRP artifact handling)
        |                                    |
  Results Processing              dbt_runner (locked build)
  (n8n sub-workflow)              detail_observations
        |                         detail_carousel_hints
  srp_observations                  dealers
```

**Discovery mode:** SRP pages are sorted `listed_at_desc`. Page 1 is fetched first to learn the total page count, then remaining pages are fetched in **randomized order** to avoid sequential pagination fingerprinting. Pagination stops when ≥80% of VINs on a page are already known (seen in the last 14 days) or a rolling 5-page window averages < 1 new VIN per page. Max 30 pages per job. On Akamai rate-limit (`ERR_HTTP2_PROTOCOL_ERROR`), the scraper stops immediately and the job fails.

**403 cooldown:** Detail page 403s are tracked in `blocked_cooldown` with exponential backoff (12h base, doubles each attempt, fully blocked at 5 attempts). The `stg_blocked_cooldown` dbt model owns all backoff logic. `ops_detail_scrape_queue` filters out cooling and fully-blocked listings automatically.

**Safe redeploy:** All 7 primary workflows check a `deploy_intent` DB flag before starting. During a planned redeploy, the ops deploy panel sets the intent — in-flight runs finish, new runs are blocked until the flag is cleared post-deploy.

### Services

| Service | Port | Description |
|---------|------|-------------|
| **postgres** | 5432 | PostgreSQL 16 — all config, raw data, parsed observations, and analytics |
| **scraper** | internal | FastAPI — fetches SRP pages via Patchright (Playwright fork) and detail pages via curl_cffi. Anti-fingerprinting: rotates Chrome UA + sec-ch-ua headers, ZIP codes, viewports, human-like pacing (8–20s). Async job queue with ThreadPoolExecutor. `/health` endpoint for service gate checks |
| **ops** | 8060 | FastAPI — admin UI (search config CRUD, run history, dbt action panel, log viewer, deploy coordination). Routes: `/admin/searches/`, `/admin/runs`, `/admin/dbt`, `/admin/logs`, `/admin/deploy` |
| **n8n** | 5678 | Workflow orchestration — 13 workflows handle scraping, polling, parsing, cleanup, deploy gating, and error handling |
| **flaresolverr** | internal | FlareSolverr — Cloudflare challenge solver used as a fallback for SRP pages that return 403s |
| **dbt** | — | dbt-postgres — 25 models across staging/intermediate/mart/ops schemas. Code baked into image at build time. Runs on-demand via dbt_runner |
| **dbt_runner** | internal | FastAPI wrapper that lets n8n trigger `dbt build` via HTTP. Database-level mutex (`dbt_lock` table) prevents concurrent builds — returns 409 if locked. Intent-based partial builds (e.g. `after_403` rebuilds only `stg_blocked_cooldown+`) |
| **dashboard** | 8501 | Streamlit — 4-tab analytics dashboard (Pipeline Health, Inventory Overview, Deal Finder, Market Trends). Sidebar quicklinks to n8n, ops admin, pgAdmin |
| **pgadmin** | 5050 | pgAdmin 4 — web-based SQL IDE for querying and browsing the database |
| **caddy** | 80/443 | Caddy — reverse proxy, TLS termination, OAuth2 gating, and role-based route enforcement |

### n8n Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| **Scrape Listings** | Every 30 min | Polls `POST /advance_rotation` — claims the next due rotation slot (6 slots, ≥1439 min idle, ≥230 min gap). Explodes slot configs × scopes into async scrape jobs. Checks deploy intent gate before starting. |
| **Job Poller V2** | Every 1 min | Expires orphaned jobs (>30 min), polls `/jobs/completed`, inserts artifacts, syncs `blocked_cooldown` (upsert 403s, delete successes), triggers dbt `after_403` intent build. Sends Telegram alert on Akamai kills. |
| **Scrape Detail Pages V2** | Every 15 min | Queries `ops_detail_scrape_queue`, atomically claims batch via `detail_scrape_claims`, fetches detail pages, triggers parse + dbt build. Batch capped at 1,500 listings. Telegram alert if error rate ≥2.5%. Checks deploy intent gate. |
| **Results Processing** | Sub-workflow | Unified artifact handler — parses both SRP HTML (`srp_observations`) and detail HTML (`detail_observations`, `detail_carousel_hints`, `dealers`) |
| **Parse Detail Pages** | Sub-workflow | Legacy detail parse sub-workflow (superseded by Results Processing) |
| **Build DBT** | Sub-workflow | Runs `dbt build` with intent-based model selection. Lock-aware with 30s retry. |
| **Orphan Checker** | Every 5 min | Detects and recovers orphaned runs/jobs left by container restarts or crashes |
| **Check Deploy Intent** | Sub-workflow | Polls deploy_intent flag; blocks callers until redeploy completes |
| **Check Service Health** | Sub-workflow | Hits `/health` on scraper, ops, and dbt_runner; Telegram alert on timeout |
| **Containers Up** | On startup | Fires after container restart to verify services are healthy before workflows activate |
| **Update n8n Runs Table** | Sub-workflow | Syncs n8n execution history to `n8n_executions` Postgres table for dashboard visibility |
| **Cleanup Artifacts** | Daily 2:30 AM | Archives eligible HTML to MinIO Parquet (if archiver running), then deletes from disk per retention rules |
| **Error Handler** | On error | Logs to `pipeline_errors` table and sends Telegram alert when any workflow fails |

## Auth & Access Control

Authentication and authorization are split into two layers:

- **Authentication** — Google OAuth2 via [oauth2-proxy](https://github.com/oauth2-proxy/oauth2-proxy). Every request passes through Caddy → oauth2-proxy before reaching any service. Unauthenticated requests are redirected to `/oauth2/sign_in`.
- **Authorization** — the ops service owns a `GET /auth/check` endpoint. Caddy calls it (internally, not exposed publicly) after authentication passes. It hashes the incoming `X-Auth-Request-Email` header with a fixed app-level salt (`AUTH_EMAIL_SALT`) and looks up the result in `authorized_users`. Returns `200 + X-User-Role` if authorized, `403` if not.

### Roles

| Role | Access |
|---|---|
| `admin` | Everything — deploy, dbt, search config edits, pgAdmin, n8n, MinIO |
| `power_user` | `/admin/searches`, `/admin/runs` — can edit configs and trigger runs |
| `observer` | All `/admin/*` pages read-only — can view but not mutate anything |
| `viewer` | Dashboard only |

`observer` is the portfolio-safe role: shows off the ops UI without giving control over the live pipeline.

### Access Request Flow

Users who authenticate with Google but aren't in `authorized_users` are redirected to `/request-access`. They can submit their name, desired role, and a reason. The request is stored in `access_requests` and an admin is notified via Telegram. Admins approve or deny from `/admin/access-requests`.

### Email Hashing

Emails are stored as `SHA-256(AUTH_EMAIL_SALT + lowercase_email)` — not plaintext. This prevents casual DB read access (compromised credential, SQL injection) from revealing who has privileged access.

## Data Model

### Raw / Operational Tables (written by scraper/n8n)

- `raw_artifacts` — every HTTP response (filepath, status, sha256, sizes, etc.)
- `srp_observations` — parsed SRP listings (VIN, price, make, model, dealer)
- `detail_observations` — parsed vehicle detail pages (full specs, listing state, numeric `customer_id`)
- `detail_carousel_hints` — similar-vehicle prices scraped from detail page carousels
- `dealers` — dealer names and IDs parsed from detail pages
- `search_configs` — search definitions (zip, radius, make/model, sort rotation, `rotation_slot`, `last_queued_at`)
- `runs` — run lifecycle (status, trigger, progress_count, total_count, error_count, last_error)
- `scrape_jobs` — async job tracking (queued/running/completed/fetched/failed)
- `scrape_targets` — seed table of target make/model pairs (loaded from `scrape_targets.csv`)
- `detail_scrape_claims` — listing_id claims for parallel detail scrapes (atomic ON CONFLICT)
- `blocked_cooldown` — raw 403 tracking per listing_id (counts + timestamps; backoff logic lives in dbt)
- `deploy_intent` — single-row deploy coordination flag (checked by all primary workflows)
- `n8n_executions` — n8n execution history synced to Postgres for dashboard visibility
- `dbt_lock` — single-row mutex preventing concurrent dbt builds
- `dbt_runs` — dbt build history (duration, pass/error/skip counts, intent)
- `dbt_intents` — named partial-build intent definitions (e.g. `after_403`, `full`)
- `artifact_processing` — tracks which artifacts have been parsed
- `pipeline_errors` — error log for n8n Error Handler
- `processing_runs` — detail-page parse run lifecycle (status, progress, error_count)
- `authorized_users` — email hashes + roles for DB-backed authorization
- `access_requests` — pending/approved/denied access requests submitted via `/request-access`

### dbt Models (25 active)

**Staging** — incremental tables over raw sources:
- `stg_srp_observations`, `stg_detail_observations`, `stg_detail_carousel_hints`
- `stg_dealers`, `stg_raw_artifacts`, `stg_search_configs`
- `stg_blocked_cooldown` — computes `next_eligible_at` and `fully_blocked` from raw 403 counts

**Intermediate** — business logic:
- `int_listing_to_vin` — maps listing_id → VIN (incremental)
- `int_vehicle_attributes` — canonical make/model/trim/year per VIN, detail > SRP priority (incremental table)
- `int_scrape_targets` — distinct make/model pairs from search_configs, used to filter on-target vehicles
- `int_latest_price_by_vin` — most recent price per VIN across all sources (incremental)
- `int_latest_tier1_observation_by_vin` — latest SRP/detail observation per VIN (incremental)
- `int_price_events` — union of all price observations (SRP + detail + carousel), deduped
- `int_price_history_by_vin` — price trajectory per VIN (first price, drops, min/max)
- `int_price_percentiles_by_vin` — price rank within make/model/trim cohort
- `int_carousel_hints_filtered` — on-target carousel hints filtered to scrape target make/models
- `int_carousel_price_events_mapped` — carousel hints mapped to VINs via listing_to_vin
- `int_carousel_price_events_unmapped` — on-target carousel hints not yet mapped to a VIN (feeds detail scrape queue)
- `int_listing_days_on_market` — days on market, first/last seen per VIN
- `int_model_price_benchmarks` — national price percentiles by make/model/trim (table)
- `int_dealer_inventory` — active vehicle count per dealer per make/model

**Marts** — final analytics tables:
- `mart_vehicle_snapshot` — current state per VIN (price + observation, filtered to scrape targets)
- `mart_deal_scores` — composite deal score (0–100) with tier (excellent/good/fair/weak)

**Ops** — operational views:
- `ops_vehicle_staleness` — identifies stale VINs needing a detail refresh
- `ops_detail_scrape_queue` — priority-ordered queue combining stale VINs + unmapped carousel hints, with 403 cooldown filtering

## Refresh Strategy

- **Search scrapes** use slot-based rotation: 6 slots of 1-2 search configs each fire ~once per day. Two guards enforce spacing: `min_idle_minutes=1439` (23h59m per slot) and `min_gap_minutes=230` (~4h between any runs). The 30-min n8n schedule is a dumb clock; the scraper API is authoritative. Sort order is `listed_at_desc` (discovery mode).
- **Discovery mode** stops pagination early when ≥80% of VINs on a page were seen in the last 14 days, or a rolling 5-page window averages < 1 new VIN per page. Pages are fetched in **randomized order** after page 1. Max 30 pages per job. SRP scraping uses FlareSolverr as a fallback for 403 responses.
- **Detail scrapes** run every 15 minutes from `ops_detail_scrape_queue`. Priority 1: one stale VIN per dealer. Priority 2: force-grab vehicles >36h stale (bypasses one-per-dealer rule). Priority 3: unmapped carousel hints fill remaining capacity. Batch capped at 1,500 listings. Atomic `detail_scrape_claims` prevents duplicate work across parallel runs. 403'd listings enter exponential backoff cooldown.
- **Price data** is stale after 24 hours; **full details** after 7 days.
- **dbt builds** run incrementally after each detail scrape. The `dbt_lock` mutex prevents concurrent builds — if locked, n8n retries every 30 seconds. Intent-based partial builds (e.g. `after_403`) rebuild only the affected model subgraph.

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
# Edit .env — set POSTGRES_PASSWORD at minimum

# 2. Create external Docker resources
docker network create cartracker-net
docker volume create cartracker_pgdata
docker volume create cartracker_raw
docker volume create n8n_data

# 3. Start all services
docker compose up -d

# 4. Initialize the database schema
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/schema/schema_new.sql

# 5. Load seed data
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/example_search_config.sql
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/dbt_lock.sql
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/detail_scrape_claims.sql
docker exec -i cartracker-postgres psql -U cartracker -d cartracker < db/seed/dbt_intents.sql

# 6. Install dbt packages and run initial build
docker compose run --rm dbt deps
docker compose run --rm dbt build
```

### Configure n8n workflows

Workflows are auto-imported from `n8n/workflows/` and activated on container startup via the custom entrypoint. On first setup you still need to create and wire the database credential:

1. Open n8n at `http://localhost:5678`
2. Create a **Postgres credential** (host: `postgres`, user: `cartracker`, password: from `.env`, database: `cartracker`)
3. Open each workflow and wire the Postgres credential into the Postgres nodes
4. Verify all workflows are active (the entrypoint activates them automatically)

### Add search configurations

Use the admin UI at `http://localhost:8060/admin` to add make/model searches. The setup script loads one example (Honda CR-V Hybrid). Each config defines a zip code, radius, make/model filters, and a `rotation_slot` (1–6) that controls which daily firing window it belongs to. Configs sharing a slot fire together; slots fire ~4 hours apart.

### Redeploying

Use `scripts/redeploy.sh` for planned redeployments. It sets the `deploy_intent` flag (blocking new workflow runs), waits for in-flight runs to finish, pulls new code, rebuilds containers, and clears the flag.

```bash
./scripts/redeploy.sh
```

### Service URLs

| Service | URL |
|---------|-----|
| n8n | http://localhost:5678 |
| Ops Admin UI | http://localhost:8060/admin |
| dbt Docs | http://localhost:8060/dbt-docs/ |
| Dashboard | http://localhost:8501 |
| pgAdmin | http://localhost:5050 |
| Project Info | https://cartracker.info/info |

### Environment Variables

Key variables in `.env`:

| Variable | Purpose |
|---|---|
| `POSTGRES_PASSWORD` | Primary DB password |
| `AUTH_EMAIL_SALT` | Salt for SHA-256 email hashing in `authorized_users` |
| `ADMIN_EMAIL` | Bootstrap admin email (used once in Flyway V009 migration) |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | OAuth2 credentials for oauth2-proxy and MinIO OIDC |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Alert channel for pipeline errors and access requests |

## Testing

459 tests across 4 test suites. Run from repo root:

```bash
pytest tests/
```

| Suite | Coverage |
|-------|----------|
| `tests/scraper/` | Rotation guards, discovery mode, VIN breakpoint, processors, browser/fingerprint |
| `tests/dbt_runner/` | Token validation, intent logic, lock behavior |
| `tests/ops/` | Admin form parsing, deploy intent coordination |
| `tests/shared/` | DB connection helpers |

All tests are pure unit tests — no database, no Docker, no network required.

## Project Structure

```
cartracker-scraper/
  scraper/                      # SRP + detail page fetcher
    app.py                      # FastAPI — async job queue, scrape endpoints, /health
    db.py                       # asyncpg connection pool
    processors/
      scrape_results.py         # SRP fetcher — discovery mode, VIN breakpoint
      scrape_detail.py          # Detail fetcher (curl_cffi, FlareSolverr fallback)
      parse_detail_page.py      # Detail HTML parser
      results_page_cards.py     # SRP HTML parser
      browser.py                # Patchright browser (per-thread singleton)
      fingerprint.py            # UA/sec-ch-ua/viewport/ZIP rotation
      cleanup_artifacts.py      # Artifact retention logic
    models/                     # Pydantic models
  ops/                          # Admin UI + deploy coordination
    app.py                      # FastAPI — /admin/* and /deploy/* routes
    routers/
      admin.py                  # Search config CRUD, run history, dbt panel, logs
      deploy.py                 # deploy_intent management, n8n execution sync
    templates/                  # Jinja2 HTML templates
  dashboard/
    app.py                      # Streamlit entry point (sidebar + tab routing)
    db.py                       # Shared DB connection helpers
    pages/
      pipeline_health.py        # Runs, dbt status, errors, queue depth
      inventory.py              # VIN counts, staleness, dealer breakdown
      deals.py                  # Deal scores, price vs benchmark
      market_trends.py          # Price trends over time
  dbt/
    models/
      staging/                  # 7 staging models (incremental)
      intermediate/             # 14 intermediate models
      marts/                    # 2 mart models
      ops/                      # 2 ops models
    dbt_project.yml
    profiles.yml
  dbt_runner/
    app.py                      # FastAPI — /dbt/build, /dbt/lock, /health
  shared/
    db.py                       # Shared psycopg2 DB helpers (used by ops + dbt_runner)
  n8n/workflows/                # 13 workflow JSON exports (auto-imported on startup)
  db/
    schema/schema_new.sql       # Full database schema (pg_dump)
    seed/                       # Seed data (search config, dbt_lock, intents, claims)
  tests/                        # 407 pytest unit tests
  scripts/
    setup.ps1                   # Windows first-time setup
    redeploy.sh                 # Safe redeploy with deploy_intent gating
  docs/
    PLANS.md                    # Roadmap index
    completed_plans.md          # Completed work log
    plan_*.md                   # Per-plan detail files
  docker-compose.yml
  docker-compose.override.yml   # Port overrides for local development
  docker-compose.test.yml       # Test container configuration
  .env.example
```
