# Cartracker — Plans & Roadmap

---

## Completed

| Plan | Description                                                                                                                                                                                                                                                                                                                                                                                                                 | Date |
|------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------|
| 0 | **Detail page scraper** — curl_cffi bypasses Cloudflare TLS fingerprinting                                                                                                                                                                                                                                                                                                                                                  | 2026-03-16 |
| 1 | **Search sort-order rotation** — 4-day rotation (list_price, listed_at_desc, best_deal, best_match_desc)                                                                                                                                                                                                                                                                                                                    | 2026-03-16 |
| 2 | **Dealer-grouped detail refresh** — batch query partitions by `seller_customer_id`                                                                                                                                                                                                                                                                                                                                          | 2026-03-16 |
| 3 | **Params schema cleanup** — removed `page_size`/`max_pages`, added `scopes` array                                                                                                                                                                                                                                                                                                                                           | 2026-03-16 |
| 4 | **Artifact cleanup pipeline** — retention rules + daily n8n workflow                                                                                                                                                                                                                                                                                                                                                        | 2026-01 |
| 7 | **dbt materialized view migration** — `int_listing_current_state` + `int_vin_current_state` as dbt tables                                                                                                                                                                                                                                                                                                                   | 2026-03-16 |
| 8 | **403 artifact poisoning fix** — 1M+ Cloudflare block pages re-marked, staleness view unblocked                                                                                                                                                                                                                                                                                                                             | 2026-03-16 |
| 9 | **Analytics Dashboard** — 4-section Streamlit dashboard, mobile tabs, dealer names, refresh button, pipeline health                                                                                                                                                                                                                                                                                                         | 2026-03-17 |
| 10 | **Pipeline durability** — stale run termination, stuck artifact reset, `pipeline_errors` table, Error Handler workflow                                                                                                                                                                                                                                                                                                      | 2026-03-16 |
| 11 | **Search config admin UI** — FastAPI + Jinja2 at `/admin`, Pydantic models, CRUD for `search_configs`                                                                                                                                                                                                                                                                                                                       | 2026-03-16 |
| 12 | **Dealer Table** — `dealers` table, 7,259 backfilled, parsed from detail pages, included in mart models                                                                                                                                                                                                                                                                                                                     | 2026-03-17 |
| 13 | **dbt incremental optimization** — `int_latest_price_by_vin` + `int_latest_tier1_observation_by_vin` as incremental. `mart_vehicle_snapshot` dropped from 80s+ to <1s                                                                                                                                                                                                                                                       | 2026-03-16 |
| 14.2 | **Duplicate ops models** — deleted both `ops_listing_trace` and `ops_vin_latest_artifact` (identical, unused)                                                                                                                                                                                                                                                                                                               | 2026-03-17 |
| 14.4 | **Staleness window dbt var** — extracted `now() - interval '3 days'` to `var("staleness_window_days")`                                                                                                                                                                                                                                                                                                                      | 2026-03-17 |
| 14.6 | **Percentile default** — `mart_deal_scores` uses 0.75 default for missing benchmarks                                                                                                                                                                                                                                                                                                                                        | 2026-03-17 |
| 14.8 | **Admin soft-delete** — delete now disables + renames key instead of hard `DELETE`                                                                                                                                                                                                                                                                                                                                          | 2026-03-17 |
| 15 | **Dashboard polish + Telegram alerts** — Telegram Error Handler, active run indicator, dealer names, refresh button, mobile tabs                                                                                                                                                                                                                                                                                            | 2026-03-17 |
| 16.1 | **Twice-daily SRP** — second daily schedule, burns through sort rotation in 2 days                                                                                                                                                                                                                                                                                                                                          | 2026-03-17 |
| 16.2 | **Exclude unlisted VINs from staleness view** — confirmed already implemented                                                                                                                                                                                                                                                                                                                                               | 2026-03-17 |
| 16.3 | **Detail scrape volume monitoring** — volume dropped from 48K→12K over 4 days, trending to 6-8K target                                                                                                                                                                                                                                                                                                                      | 2026-03-20 |
| 17 | **Update README** — full rewrite: architecture diagram, all 6 services, 7 workflows, 16 dbt models, data model, refresh strategy, setup steps, project structure                                                                                                                                                                                                                                                            | 2026-03-17 |
| 18 | **Active scrape progress in dashboard** — `progress_count`/`total_count` on `runs`, 10% milestone UPDATEs in loop, dashboard shows "X / Y scraped (Z%)"                                                                                                                                                                                                                                                                     | 2026-03-17 |
| 19 | **Detail scrape waits for search scrape** — IF node loop at start of Scrape Detail Pages, waits 3 min and retries if any scrape is running                                                                                                                                                                                                                                                                                  | 2026-03-17 |
| 20 | **dbt + Postgres health in dashboard** — dbt build time/status, active connections, long-running queries, lock visibility                                                                                                                                                                                                                                                                                                   | 2026-03-19 |
| 21 | **Staleness discrepancy** — resolved; both dashboard widgets draw from same 47,551 active VINs                                                                                                                                                                                                                                                                                                                              | 2026-03-17 |
| 22 | **dbt model cleanup** — audited all 19 models; deleted 3 orphans. 16 active models all have downstream consumers.                                                                                                                                                                                                                                                                                                           | 2026-03-17 |
| 23 | **Fresh install support** — updated `schema_new.sql` (pg_dump), `.env.example`, `setup.ps1` script, example search config seed, README quick-start guide                                                                                                                                                                                                                                                                    | 2026-03-17 |
| 25.2 | **Store numeric `customer_id` in `detail_observations`** — DB migration, n8n Parse Detail Pages, dbt staging→mart chain                                                                                                                                                                                                                                                                                                     | 2026-03-20 |
| 25.3 | **Fix dealer join in `mart_deal_scores`** — changed from UUID to numeric `customer_id` via `mart_vehicle_snapshot`                                                                                                                                                                                                                                                                                                          | 2026-03-20 |
| 25.4 | **Replace correlated subquery in staleness** — `customer_id IS NULL` replaces expensive NOT EXISTS subqueries in `ops_vehicle_staleness`                                                                                                                                                                                                                                                                                    | 2026-03-23 |
| 26 | **Search scrape slot rotation** — 6 slots, each fires once/day via `advance_rotation`; discovery mode with VIN breakpoint and stop-on-error                                                                                                                                                                                                                                                                                 | 2026-03-20 |
| 26.3 | **Reduce max_workers 12→6** — ThreadPoolExecutor halved for immediate rate-limit relief                                                                                                                                                                                                                                                                                                                                     | 2026-03-19 |
| 27.1 | **Detail scrape error rate alert** — Telegram alert when error rate >= 2.5% after each detail scrape run                                                                                                                                                                                                                                                                                                                    | 2026-03-20 |
| 27.2 | **Search scrape Akamai kill alert** — Job Poller Switch node detects ERR_HTTP2, sends Telegram with search_key/scope/page count                                                                                                                                                                                                                                                                                             | 2026-03-20 |
| 28 | **Dashboard quicklinks** — sidebar links to n8n, Search Config Admin, pgAdmin                                                                                                                                                                                                                                                                                                                                               | 2026-03-20 |
| 30 | **Detailed run info in dashboard** — unified Recent Runs table (all types, 48h), throughput rate, ETA, error counts; enhanced active run indicator                                                                                                                                                                                                                                                                          | 2026-03-23 |
| 31 | **pgAdmin for SQL access** — pgAdmin 4 container on port 5050, connected to cartracker DB                                                                                                                                                                                                                                                                                                                                   | 2026-03-20 |
| 32 | **Force-grab stale vehicles in detail scrape** — added second pool for vehicles > 36h stale bypassing one-per-dealer rule                                                                                                                                                                                                                                                                                                   | 2026-03-20 |
| 33 | **Error info on runs table** — `error_count` + `last_error` columns on `runs`; Job Poller aggregates from `scrape_jobs` on completion; dashboard shows errors                                                                                                                                                                                                                                                               | 2026-03-23 |
| 34 | **artifact_count subquery in Job Poller** — verified already filters by `run_id`; no fix needed                                                                                                                                                                                                                                                                                                                             | 2026-03-23 |
| 37 | **Carousel hint discovery pipeline** — Pool 3 in detail batch, make/model filtering via `int_scrape_targets`, VIN mapping via detail obs, dashboard metrics                                                                                                                                                                                                                                                                 | 2026-03-20 |
| 38 | **SRP anti-detection** — Patchright (Playwright fork), UA rotation (Chrome v132-135), ZIP code pools (10 metros + 10 local), viewport rotation, human-like pacing (8-20s), random page order, sequential scope execution                                                                                                                                                                                                    | 2026-03-21 |
| 39 | **Scrape targets seed + on-target filtering** — `scrape_targets.csv` seed, `int_scrape_targets` dbt model, `mart_vehicle_snapshot` filtered to on-target make/models only; cascades to `ops_vehicle_staleness` and detail scrape queue                                                                                                                                                                                      | 2026-03-22 |
| 40 | **Dashboard target filtering** — all dashboard queries filtered to on-target scrape targets; eliminated slow `int_vehicle_attributes` view joins; queries use `mart_deal_scores` or direct `int_scrape_targets` joins                                                                                                                                                                                                       | 2026-03-23 |
| 41 | **dbt performance: staging materialization** — `stg_detail_observations` + `stg_srp_observations` converted from views to incremental tables; eliminates repeated 1.9M + 530k row scans                                                                                                                                                                                                                                     | 2026-03-23 |
| 42 | **mart_deal_scores performance** — `detail_only_vins` CTE queries base table directly instead of staging view; partial index on `detail_observations(make, model, fetched_at)`; 6+ min → 200ms                                                                                                                                                                                                                              | 2026-03-23 |
| 43 | **Detail batch sizing** — capped at 1500 VINs (~15 min at 100/min); carousel VINs fill remaining capacity after stale VINs; designed for 30-min schedule                                                                                                                                                                                                                                                                    | 2026-03-23 |
| 45 | **int_vehicle_attributes materialized** — converted from view to incremental table; detail > SRP source priority; added first_seen_at, last_seen_at, is_tracked; upstream VIN identity source of truth; eliminates repeated 2M+ row scans per mart build                                                                                                                                                                    | 2026-03-23 |
| 46 | **Docker build from committed code** — dbt, dbt_runner, dashboard now COPY code at build time; source volume mounts removed; deploy workflow: git pull + docker compose build + docker compose up -d                                                                                                                                                                                                                        | 2026-03-23 |
| 47 | **dbt build lock** — `dbt_lock` single-row mutex table; dbt_runner acquires atomically before build, releases in `finally`; returns 409 when locked; 30-min stale timeout; n8n retries every 30s; dashboard shows lock status                                                                                                                                                                                               | 2026-03-23 |
| 48 | **Parallel detail scrapes** — `detail_scrape_claims` table keyed on `listing_id` with `status` column; atomic `INSERT ... ON CONFLICT DO UPDATE WHERE status != 'running'`; claims expire by run status, not TTL; two parallel runs get non-overlapping batches                                                                                                                                                             | 2026-03-23 |
| 49 | **ops_detail_scrape_queue dbt view** — moved 80-line batch selection SQL from n8n into dbt ops view; combines stale VINs (priority 1: one per dealer, priority 2: force-stale >36h) + unmapped carousel hints (priority 3); n8n query reduced to simple SELECT + claiming                                                                                                                                                   | 2026-03-23 |
| 36 | **Automate n8n workflow import** — entrypoint.sh runs `n8n import:workflow --separate` on container startup + `n8n update:workflow --all --active=true`; workflows volume-mounted from repo; `git pull + docker compose restart n8n` picks up changes                                                                                                                                                                       | 2026-03-23 |
| 50 | **Dashboard refactor** — split 1108-line app.py into per-tab modules: db.py (shared), pages/pipeline_health.py, pages/inventory.py, pages/deals.py, pages/market_trends.py; app.py reduced to 47 lines (sidebar + routing)                                                                                                                                                                                                  | 2026-03-23 |
| 51 | **Docs and setup update** — README architecture diagram updated for parallel scrapes + claiming; n8n section updated for auto-import; setup.ps1 step numbering fixed + post-setup messages updated; seed files added to manual setup instructions                                                                                                                                                                           | 2026-03-23 |
| 14.1 | **VIN case normalization** — `stg_detail_observations` computes `vin17` via `upper(d.vin)` with length/format validation; all downstream models use `vin17`                                                                                                                                                                                                                                                                 | 2026-03-26 |
| 14.5 | **Price events dedup** — `int_price_events` uses `SELECT DISTINCT ON (vin, observed_at, price)` with source priority (detail > srp > carousel) in full-refresh mode                                                                                                                                                                                                                                                         | 2026-03-26 |
| 14.9 | **Browser singleton lock** — moot; `browser.py` uses `threading.local()` so each worker thread owns its own browser instance, no shared state                                                                                                                                                                                                                                                                               | 2026-03-26 |
| 14.11 | **Chrome fingerprint env var** — `fingerprint.py` rotates through `["132", "133", "134", "135"]`; no longer a hardcoded single value                                                                                                                                                                                                                                                                                        | 2026-03-26 |
| 35 | **dbt schema audit** — all 9 sub-items complete: stg_raw_artifacts, stg_dealers, carousel hints incremental, deleted int_latest_dealer_name_by_vin, benchmarks→table, .yml docs, source descriptions, dashboard unlisted query                                                                                                                                                                                              | 2026-03-27 |
| 54+58 | **Admin UI overhaul + DB responsibility consolidation** — run history (/admin/runs), dbt action panel (/admin/dbt), intent management (DB-backed), dbt docs generate/serve (/dbt-docs/), log viewer (/admin/logs), logger.exception() throughout, orphan recovery removed from lifespan (→ n8n Plan 59), search config routes → /admin/searches/, test containers (docker-compose.test.yml), port separation (override.yml) | 2026-03-27 |
| 74 | **dbt logic flaw — new search configs** — traced full DAG; Kia Sportage PHEV now reaches mart tables                                                                                                                                                                                                                                                                                                                        | 2026-03-30 |
| 60+75 | **Safe redeploy + admin migration to ops** — `deploy_intent` + `n8n_executions` tables, Check Deploy Intent + Update n8n Runs Table sub-workflows, `Deploy Allowed?` gate in all 7 workflows, `ops` container with all admin UI (searches, runs, dbt, logs, deploy panel), `redeploy.sh`, scraper stripped of all UI concerns                                                                                               | 2026-03-30 |
| 76 | Service Health Gate  `/health` endpoints on scraper/ops/dbt_runner, Postgres SELECT 1 check, n8n health gate sub-workflow with Telegram on timeout                                                                                                                                                                                                                                                                          | 2026-03-30 |
---

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **79** — Multi-instance detail scraping + artifact pipeline refactor | Scraper writes artifacts to DB directly, batch detail endpoint, round-robin workers, retire Job Poller |
| 2 | **73** — Scraper code review & refactor | Full quality pass + structural split; includes SRP breakpoint fix |
| 3 | **61** — Python unit tests | pytest for scraper, dbt_runner, and ops admin logic |
| 4 | **62** — CI/CD (GitHub Actions) | Automated lint, type check, pytest, docker build, dbt test on every PR |
| 5 | **63** — Schema migration management (Flyway) | SQL-first versioned migrations; CI spins up test DB from migration sequence |
| 6 | **77** — SQL query tests | Smoke tests for mission-critical queries in scraper/ops/dashboard/dbt_runner; catches schema breakage on merge |
| 7 | **64** — PgBouncer connection pooling | Coordination layer in front of Postgres; fixes connection budget across all services |
| 8 | **65** — Auth stack (Authelia + Google OAuth + Postgres roles) | Required before any public deployment |
| 9 | **66** — SQL injection audit | Verify all queries are parameterized; required before public deployment |
| 10 | **29** — n8n API foundation | Unlocks credential automation, admin triggers, redeploy workflow pause, execution status |
| 11 | **67** — n8n credential automation | Depends on Plan 29; closes silent fresh-install failure |
| 12 | **68** — Cloud deployment (Oracle Free Tier) | Move project to real cloud infrastructure; free forever |
| 13 | **69** — Terraform IaC | Describe cloud infrastructure as code; reproducible deployments |
| 14 | **70** — Type annotations | Add type hints throughout scraper and dbt_runner; enforced by mypy in CI |
| 15 | **71** — Airflow DAG translation | Translate n8n pipeline workflows into Airflow DAGs as parallel implementation |
| 16 | **72** — Data lake / Parquet proof of concept | MinIO + Parquet for artifact storage; demonstrates data lake architecture |
| 17 | **53** — Dashboard cleanup/optimization | Pipeline Health tab is bloated; consider collapsible sections or sub-tabs |
| 18 | **58 (remainder)** — advance_rotation ownership | Pure orchestration logic still lives in scraper; low priority |
| 19 | **55** — Dashboard review | Audit current state, fix issues, improve UX |
| 20 | **56** — Analytics next steps | Identify new insights, models, or views to build |
| 21 | **14.12** — max_safety_pages validator | No bounds check; low risk |

---

## Plan 61: Python Unit Tests

**Status:** Not started
**Priority:** High

Add a `tests/` directory at the repo root with pytest coverage for the scraper, dbt_runner, and admin router. Dashboard excluded — it's a pure display layer with data quality covered by dbt tests.

### Scope

| Service | Priority targets |
|---------|----------------|
| `scraper` | Rotation guard math, discovery mode thresholds (80% known VIN, rolling 5-page window), VIN breakpoint logic, `_slug()` URL sanitization, NULL/edge cases for all of the above |
| `dbt_runner` | `_validate_tokens()` (token safety), intent load/save/delete logic, `_cap()` output truncation |
| `admin` | Form parsing helpers (`_parse_comma_list`, `_stringify_uuids`), redirect behavior on missing records |

### Structure
```
tests/
├── scraper/
│   ├── test_rotation.py
│   └── test_discovery.py
└── dbt_runner/
    └── test_validation.py
```

### Notes
- Tests should be pure unit tests — no database, no Docker, no network
- Functions with DB dependencies will need to be structured so the logic is separable (extract pure logic into helper functions if needed)
- Run with a single `pytest` from repo root

---

## Plan 62: CI/CD — GitHub Actions

**Status:** Not started
**Priority:** High

Add a GitHub Actions workflow that automatically validates every PR before it can merge. CD (automated deployment) is out of scope for now — the deployment target is a single home server and the deploy step (`git pull + docker compose build + up`) requires a remote connection that adds complexity without enough payoff yet.

### Gates (in execution order)

| Step | Tool | What it catches |
|------|------|----------------|
| Lint | `ruff` | Syntax errors, unused imports, formatting drift |
| Type check | `mypy` | Type mismatches between functions |
| Unit tests | `pytest` | Business logic regressions |
| Docker build | `docker compose build` | Broken Dockerfiles, missing dependencies |
| dbt build + test | `dbt build` | Model compilation errors, data quality test failures |

### Workflow trigger
- On every pull request to `master`
- On every push to `master`

### Notes
- Workflow config lives at `.github/workflows/ci.yml` — travels with the code, versioned in git
- GitHub provides 2,000 free minutes/month for private repos; this pipeline will use ~3-5 min per run
- Fast gates (lint, type check) run first so failures surface quickly without waiting for slower steps
- dbt build step needs a test database — either a lightweight Postgres service container spun up in the GitHub Actions VM, or skipped initially and added once unit tests are in place
- Integration tests (if added in Plan 61) run separately from unit tests so the fast suite stays fast

---

## Plan 63: Schema Migration Management — Flyway

**Status:** Not started
**Priority:** Medium

Currently `schema_new.sql` is a pg_dump snapshot of the current state — it answers "what does the schema look like now" but not "how do I get an existing database from an older state to the current one." Every schema change is applied manually to production with no rollback capability and no audit trail.

### The problem this solves
- Schema changes applied by hand with no rollback if something breaks
- No ordered history of what changed and when
- A fresh install runs the full dump fine, but an existing database that's one version behind has to figure out what changed manually
- CI/CD (Plan 62) needs a reliable way to stand up a test database from scratch for SQL query tests (Plan 77)

### Tool: Flyway
SQL-first migration tool — just numbered `.sql` files in `db/migrations/`. Flyway tracks applied migrations in a `flyway_schema_history` table and applies any unapplied ones in order. No Python required, no ORM dependency, no autogeneration complexity. Matches how we already write migrations.

### Implementation
- Add a `flyway` container to `docker-compose.yml` (or run as a one-shot job on deploy)
- Rename existing ad-hoc migration scripts in `db/schema/` to Flyway naming convention: `V001__initial_schema.sql`, `V002__add_customer_id.sql`, etc.
- All future schema changes go in `db/migrations/` as new versioned files — never edit existing ones
- `schema_new.sql` retained as a reference for fresh installs and documentation

### CI/CD integration (Plan 62)
Flyway runs as a step in the GitHub Actions pipeline against the ephemeral test Postgres container, applying all migrations from scratch before SQL tests (Plan 77) and dbt tests run. This validates that migrations are correct and complete before any merge.

### Notes
- Flyway community edition is free and sufficient for this project
- `db/schema/` and `db/seed/` structure maps naturally to the migrations + seed convention with minimal reorganization

---

## Plan 77: SQL Query Tests

**Status:** Not started
**Priority:** Medium

Mission-critical SQL queries live in four places outside dbt — scraper, dashboard, ops, and dbt_runner. These are never tested today. A schema change (column rename, type change, table drop) can silently break them and only surfaces at runtime.

### Problem
- `scraper/routers/admin.py` — queries `search_configs`, `runs`, `scrape_jobs`
- `ops/routers/admin.py` — same queries, now the canonical location post-migration
- `ops/routers/deploy.py` — queries `deploy_intent`, `n8n_executions`
- `dashboard/pages/*.py` — complex analytical queries against mart tables and ops views
- `dbt_runner/app.py` — queries `dbt_lock`, `dbt_build_log`

None of these are covered by dbt tests (which only validate dbt model output) or pytest unit tests (which mock the DB).

### Approach
Integration tests that run against a real Postgres test database (spun up in CI via the Flyway migration sequence from Plan 63):

- **Query smoke tests** — execute each mission-critical query against the test DB and assert it returns without error and with expected columns. No business logic assertions needed — the goal is catching schema breakage, not data correctness.
- **Parameterized** — test queries with representative parameter values (valid run_id, known search_key, etc.) seeded into the test DB
- **Organized by service** — `tests/sql/test_scraper_queries.py`, `tests/sql/test_ops_queries.py`, `tests/sql/test_dashboard_queries.py`, `tests/sql/test_dbt_runner_queries.py`

### CI/CD integration (Plan 62)
SQL tests run after Flyway migrations + seed data applied to the ephemeral test DB, before the dbt build step. A schema change that breaks a live query fails the pipeline before it can merge.

### Notes
- Depends on Plan 63 (Flyway) for the test DB setup pattern
- Does not replace dbt tests — dbt tests validate model logic; SQL tests validate application query compatibility
- Dashboard queries are the highest risk — they are the most complex and span the most tables

---

## Plan 64: Connection Pooling — PgBouncer

**Status:** Not started
**Priority:** Medium

The project has a hard `max_connections=50` limit on Postgres, shared across 8 services. Under concurrent load (dbt build + detail scrape + dashboard + Orphan Checker + n8n firing simultaneously) the project can realistically spike toward that ceiling. When Postgres hits the limit it refuses new connections entirely — everything fails at once.

The deeper problem is that services use the connection layer inconsistently:
- `scraper` — asyncpg with a connection pool (correct)
- `dbt_runner` — psycopg2, new connection per function call, unbounded
- `dashboard` — Streamlit, opens connections per query
- `n8n` — direct Postgres node connections, unmanaged
- `dbt` build process — opens connections per model thread

Fixing `dbt_runner` in isolation only solves one service. The right fix is a coordination layer in front of Postgres that all services talk to.

### Solution: PgBouncer

PgBouncer is a lightweight connection pooler purpose-built for Postgres. It sits as a separate container between all application services and Postgres. Services connect to PgBouncer (which looks exactly like Postgres to them), and PgBouncer maintains a small real pool of Postgres connections, multiplexing them across all callers.

- Written in C, battle-tested at scale, 15+ years of production use
- Transparent to applications — just a connection string change
- Enforces a hard cap on real Postgres connections regardless of how many services connect simultaneously
- Standard pattern on any self-hosted Postgres data platform

### Implementation
- New `pgbouncer` container in `docker-compose.yml`
- All services update `DATABASE_URL` to point at PgBouncer port (5432 on PgBouncer, which forwards to Postgres internally)
- PgBouncer config: transaction pooling mode, ~10-15 real Postgres connections, up to 40 client connections

### Known issue to address: dbt + transaction pooling
dbt uses `SET` statements that don't survive transaction-level pooling (state is lost when the connection is returned to the pool). Two options:
- Run PgBouncer in **session mode** for dbt specifically (one real connection per session, less efficient but compatible)
- Set `SET search_path` in the dbt profile and use the `pgbouncer: true` flag in `profiles.yml` which disables the problematic statements

This is a known, documented issue with a known fix — just needs to be handled during implementation.

### Why not build a custom Python pooling layer?
Application-level pooling (what asyncpg already does inside the scraper) only helps within a single service. It doesn't solve competition between 8 independent services. Building a shared Python layer would be reinventing PgBouncer, worse — missing 15 years of edge case handling around authentication, cancel requests, server-side state, and failover.

### Skills demonstrated
Connection pooling architecture is standard expected knowledge for data engineers operating self-hosted Postgres. Recognizing it as an infrastructure-layer problem (not an application-layer problem) and using the right purpose-built tool is the correct call.

---

## Plan 65: Authentication & Authorization Stack

**Status:** Not started
**Priority:** Medium (low risk while local; required before any public deployment)

Currently all services are protected only by Docker network isolation. Port 8000 (admin), 8501 (dashboard), and 5050 (pgAdmin) are open to anyone who can reach the host. No authentication, no user tiers, no database-level permission separation.

### Target architecture

```
Internet
  → Caddy (HTTPS, Let's Encrypt, reverse proxy)
    → Authelia (authentication + coarse authorization)
      → Google OAuth (identity provider — no passwords to manage)
        → Services (admin, dashboard, pgAdmin)
          → Postgres (role-based permissions per service)
```

### Layer 1 — HTTPS (Caddy)
Caddy sits in front of everything as a reverse proxy and handles TLS automatically via Let's Encrypt. No certificate management required. All traffic encrypted in transit.

### Layer 2 — Authentication (Authelia + Google OAuth)
Authelia is a self-hosted SSO server. It intercepts all requests and redirects unauthenticated users to a login page. Authentication is delegated to Google OAuth — users log in with their Google account, no passwords stored anywhere in the project.

- Single login session works across all services (true SSO)
- No password management, no credential storage
- Google handles identity verification

**Transferable skill:** This is structurally identical to enterprise SSO (Okta, Azure AD) — same OAuth protocol, same redirect flow, same token model. The only difference in a big org is the identity provider.

### Layer 3 — Coarse authorization (Authelia rules)
Authelia enforces URL-level access tiers via group membership:

| Group | Access |
|-------|--------|
| `viewer` | `/dashboard` only |
| `power_user` | `/dashboard`, `/admin/searches/`, `/admin/runs` |
| `admin` | All routes including `/admin/dbt`, `/pgadmin` |

Group membership managed in Authelia config. Consumer Google accounts don't support group claims, so groups are managed locally in Authelia and mapped to Google email addresses.

### Layer 4 — Database permissions (Postgres roles)
Each service connects to Postgres (via PgBouncer — Plan 64) with a scoped role rather than a single superuser:

| Role | Permissions | Used by |
|------|------------|---------|
| `scraper_user` | Write to raw tables, read search_configs | scraper |
| `dbt_user` | Write to analytics schema | dbt, dbt_runner |
| `viewer` | SELECT on analytics schema only | dashboard |
| `cartracker_admin` | Full access | admin UI, migrations |

This limits blast radius if any service is compromised — a vulnerability in the dashboard can't write to raw tables.

### What this doesn't cover
Fine-grained in-app authorization (e.g. "this user can view search configs but not edit them") requires application code changes and is out of scope for now. Authelia URL rules provide sufficient coarse control for a small trusted user group.

### New containers
- `caddy` — reverse proxy + TLS
- `authelia` — SSO server
- `authelia_redis` — session storage (Authelia requires Redis)

### Notes
- Google OAuth requires registering an app in Google Cloud Console (free) to get client ID + secret
- Consumer Google accounts don't support group claims — Authelia manages groups locally, mapped by email
- Postgres role migration needs to be coordinated with Plan 63 (schema migrations) and Plan 64 (PgBouncer)

---

## Plan 66: SQL Injection Audit

**Status:** Not started
**Priority:** Medium (required before any public deployment)

SQL injection is a separate concern from authentication — it's about how queries are constructed, not who's allowed to make them. An authenticated user could still exploit an injection vulnerability.

### Current state
asyncpg and psycopg2 both use parameterized queries by default (`$1` placeholders and `%s` respectively), which are injection-safe. The risk is likely low but unverified — no systematic audit has been done.

### Scope
- Audit every database call in `scraper/`, `dbt_runner/`, and `dashboard/` for string-formatted SQL
- Audit n8n Postgres nodes — SQL entered directly in the n8n UI is a common injection surface
- Verify all form inputs that flow into queries go through parameterization, not f-strings or `.format()`
- Check any dynamic query construction (e.g. ORDER BY clauses, table name interpolation) — these can't be parameterized and need explicit allowlists

### Output
A short audit report noting any findings and the fix applied. If nothing is found, that's the output — documented confidence rather than assumed safety.

---

## Plan 29: Set up n8n API

**Status:** Not started
**Priority:** High — foundation for several downstream plans

n8n exposes a REST API that allows programmatic interaction with workflows, executions, and credentials. Currently nothing in the project uses it — all n8n interaction is manual via the UI. Establishing an authenticated API client is the foundation that unlocks a set of downstream improvements.

### Foundation work
- Enable n8n API access (API key, base URL config)
- Build a thin client/wrapper (Python) usable from setup scripts and the scraper admin
- Document the API key as a required env var alongside `POSTGRES_PASSWORD`

### Use cases unlocked (sub-items, implemented separately)

**29.1 — Credential automation (fresh install)**
On fresh install, `setup.ps1` calls the n8n API to create the Postgres credential programmatically instead of requiring manual UI steps. Closes the silent failure gap where workflows import but fail on first run. *(Also tracked as Plan 67)*

**29.2 — Trigger detail scrape from admin UI**
Add a "Trigger Detail Scrape" button to the admin UI that calls the n8n API to fire the Scrape Detail Pages workflow on demand, without opening n8n.

**29.3 — Trigger SRP scrape from admin UI**
Same pattern — trigger a specific search config's SRP scrape on demand from the admin UI. Useful for testing a new config without waiting for the rotation schedule.

**29.4 — Workflow execution status in admin UI**
Surface recent n8n execution history in the admin run history page — show whether the last dbt build, detail scrape, or SRP scrape succeeded or failed, without leaving the admin.

**29.5 — Pause/resume workflows during redeploy**
During a planned redeploy (Plan 60), call the n8n API to deactivate scheduled workflows before bringing containers down, then reactivate them once health checks pass. Eliminates the race condition where n8n fires mid-redeploy.

### Downstream plans that depend on this
- Plan 67 — n8n credential automation
- Plan 60 — Safe redeploy (pause/resume workflows)

---

## Plan 67: n8n Credential Automation

**Status:** Not started
**Priority:** Low — only matters on fresh install; depends on Plan 29

**Depends on:** Plan 29 (n8n API foundation)

Currently a fresh install requires manually creating the Postgres credential in the n8n UI before any workflow that touches Postgres will run. The workflows import successfully, giving no indication anything is wrong — the failure only surfaces on first execution.

### Fix
Extend `setup.ps1` to call the n8n API (`POST /credentials`) to create the Postgres credential programmatically as part of the setup sequence. The credential values come from the same `.env` file used by everything else.

### Notes
- Blocked until Plan 29 establishes the API client and key management
- Small change once Plan 29 is in place — primarily a setup script addition
- Should include a check: if credential already exists, skip creation rather than error

---

## Plan 68: Cloud Deployment — Oracle Free Tier

**Status:** Not started
**Priority:** Medium — prerequisite for Plans 65 (auth), 69 (Terraform), and public sharing

Move the project from a local home server to Oracle Cloud's Always Free tier. Oracle's free tier is the most generous available — 4 Ampere ARM cores, 24GB RAM, 200GB storage, no time limit. The full stack runs comfortably on it with no cost.

### Why Oracle Free Tier
- Genuinely free forever — not a trial
- Enough resources to run all 8 containers without compromise
- Real cloud infrastructure: VMs, networking, firewall rules, DNS
- Gives portfolio evidence of cloud deployment without a credit card

### Work involved
- Provision VM, networking, and firewall rules (manually first, then Terraform in Plan 69)
- Configure DNS if sharing publicly
- Update `docker-compose.yml` for ARM architecture if needed (most images have ARM builds)
- Move `.env` secrets to cloud VM securely
- Set up SSH access and deploy workflow (`git pull + docker compose build + up`)
- Coordinate with Plan 65 (auth) — don't expose publicly without authentication in place

### Notes
- Plans 65 (auth) and 66 (SQL injection audit) should be completed before exposing any port publicly
- Oracle ARM architecture is broadly compatible with standard Docker images; any exceptions need ARM-specific builds

---

## Plan 69: Terraform — Infrastructure as Code

**Status:** Not started
**Priority:** Medium — depends on Plan 68 (cloud deployment)

**Depends on:** Plan 68

Currently cloud infrastructure (if any) would be provisioned by clicking through the Oracle Cloud console. Terraform describes that same infrastructure in version-controlled `.tf` files — someone cloning the repo runs `terraform apply` and gets the identical environment.

### What gets described in Terraform
- Oracle Cloud VM (shape, size, OS image)
- Virtual network and subnet
- Firewall/security group rules (which ports are open)
- SSH key attachment
- DNS record (if using a custom domain)

### What Terraform does NOT manage
- Docker containers (that's Docker Compose's job)
- Application config (that's `.env`)
- Database schema (that's Plan 63)

The boundary is: Terraform provisions the machine, Docker Compose runs the software on it. Clean separation.

### Portfolio value
Terraform is on most senior data engineering job descriptions. Showing infrastructure defined as code — versioned, reviewable, reproducible — demonstrates platform engineering maturity beyond just writing pipelines.

### Notes
- Terraform is free to use; Oracle Cloud provider is well-supported
- State file needs to be stored somewhere (Terraform Cloud free tier, or Oracle Object Storage)
- Can be developed and tested locally against Oracle Cloud without any cost beyond the free tier VM

---

## Plan 70: Type Annotations

**Status:** Not started
**Priority:** Low — enforced going forward by CI (Plan 62); this plan addresses existing code

Plan 62 (CI/CD) adds `mypy` as a gate on new code, but existing functions throughout `scraper/` and `dbt_runner/` lack type annotations. This plan is a systematic pass to add them.

### Scope
- `scraper/app.py` — all endpoint functions and helpers
- `scraper/processors/` — scraping and parsing functions
- `scraper/routers/admin.py` — route handlers and helpers
- `dbt_runner/app.py` — all endpoint functions and helpers

### Why it matters
- Type annotations make function contracts explicit and machine-checkable
- mypy catches type mismatches before runtime
- Signals code quality and maintainability to anyone reading the codebase
- Required for mypy CI gate to be meaningful on existing code

### Notes
- Don't need to annotate everything perfectly — `Any` is acceptable for complex asyncpg return types initially
- Focus on function signatures (parameters + return types) first; internal variables second
- Dashboard excluded — display layer, lower value for the effort

---

## Plan 71: Airflow DAG Translation

**Status:** Not started
**Priority:** Medium — portfolio value, no production impact

Translate the core n8n pipeline workflows into Apache Airflow DAGs as a parallel implementation. Not a replacement — n8n stays running in production. This is a portfolio demonstration showing understanding of DAG-based orchestration, the standard in enterprise data engineering.

### Why Airflow matters
Airflow is the most common data pipeline orchestrator in job descriptions. n8n and Airflow solve similar problems but differently:

| | n8n | Airflow |
|--|-----|---------|
| Concept | Event-driven automation | DAG-based pipeline orchestration |
| Built for | General service automation | Data pipelines specifically |
| Scheduling | Cron-like triggers | Cron + backfill + catchup |
| Dependencies | Node connections | Task dependencies in Python code |
| History | Execution log | Full DAG run history, queryable by date |
| Backfill | Not supported | Built-in |

### DAGs to implement
- **scrape_listings** — mirrors "Scrape Listings" n8n workflow
- **scrape_detail_pages** — mirrors "Scrape Detail Pages" workflow
- **dbt_build** — mirrors "Build DBT" sub-workflow
- **orphan_checker** — mirrors "Orphan Checker" workflow

### Deployment
Airflow runs locally via Docker (official `apache/airflow` image). Sits alongside the existing stack, reads from the same Postgres database. No production traffic goes through it initially.

### Notes
- Airflow has a free Docker Compose quickstart — low barrier to get running
- DAGs are Python files — directly demonstrates Python skills alongside orchestration skills
- The translation exercise itself is valuable: mapping n8n visual flows to Python code requires understanding both systems deeply

---

## Plan 72: Data Lake / Parquet Proof of Concept

**Status:** Not started
**Priority:** Low — proof of concept, no production dependency

Currently raw HTML artifacts are stored on a Docker volume as files, with metadata in the `raw_artifacts` Postgres table. This plan explores a data lake architecture as a proof of concept: store artifacts in MinIO (self-hosted S3-compatible object storage) with a Parquet-based metadata layer.

### Why this is interesting
- Demonstrates understanding of data lake vs data warehouse architecture
- Parquet is the standard columnar format in modern data engineering (Delta Lake, Iceberg, dbt on Snowflake all use it)
- MinIO is free, self-hosted, S3-compatible — skills transfer directly to AWS S3
- The artifact storage problem is a natural fit: large binary files with structured metadata

### Proposed architecture
- Raw HTML artifacts → MinIO bucket (instead of Docker volume)
- Artifact metadata → Parquet files in MinIO (alongside or instead of `raw_artifacts` Postgres table)
- dbt reads from Postgres as today; Parquet layer is for the artifact/raw storage concern only

### What it demonstrates
- Object storage patterns (bucket organization, path conventions, lifecycle policies)
- Parquet file format (columnar, compression, schema evolution)
- S3-compatible API (transfers directly to AWS S3 knowledge)
- Data lake concepts (raw zone, schema-on-read vs schema-on-write)

### Notes
- MinIO runs as a Docker container, free, S3-compatible API
- This is a proof of concept — production pipeline stays on Postgres
- Could be implemented incrementally: new artifacts go to MinIO, old ones stay on volume
- Pairs well with Plan 68 (cloud deployment) — MinIO on Oracle cloud is a real object storage setup

---

## Plan 73: Scraper Code Review & Refactor

**Status:** In progress
**Priority:** High

`scraper/app.py` has accumulated significant scope: scrape logic, async job management, background threading, rotation guards, processing endpoints, and FastAPI wiring all in one file. This plan is a full code review and refactor — structural split plus logic quality pass.

### Scope
- Full read-through of `scraper/app.py` — identify logic that belongs in separate modules, complex functions that should be simplified, error handling gaps
- Structural split along natural seams (similar to Plan 50's dashboard split):
  - Job management (in-memory store, ThreadPoolExecutor, job lifecycle)
  - Rotation logic (`advance_search_rotation`, slot guards)
  - Processing endpoints (results pages, detail pages, cleanup)
  - FastAPI app wiring and lifespan
- Code quality pass:
  - Identify any functions doing too many things
  - Improve error handling and logging where gaps exist
  - Surface any other logic bugs found during review
- Add type annotations to refactored code (coordinates with Plan 70)

### Notes
- Behavior changes (bug fixes) and structural changes (refactor) should be committed separately so regressions are easy to bisect
- This plan does not touch `dbt_runner/` or `dashboard/` — scraper only
- Goal is to get this code ready to add pytests to.

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

## Plan 55: Dashboard review

**Status:** Not started
**Priority:** High

- Audit current dashboard state across all tabs
- Fix any broken or stale widgets
- Improve UX and information hierarchy

---

## Plan 56: Analytics next steps

**Status:** Not started
**Priority:** High

- Identify new insights, models, or views to build on top of existing data
- Evaluate gaps in current analytics coverage
- Plan next wave of dbt models and dashboard features

---

## Plan 76: Service Health Gate **[DONE]**

**Status:** Not started
**Priority:** High

After a redeploy, containers restart and n8n's schedule may fire before scraper/dbt_runner are fully accepting connections. Without a health check, the first workflow run post-deploy fails immediately. This plan adds `/health` endpoints to scraper and ops, and builds a `Service Health Gate` n8n sub-workflow that all 7 workflows call after the `Check Deploy Intent` gate.

### Part 1: `/health` endpoints

`dbt_runner` already has `GET /health → {"ok": true}`. Copy the identical pattern to scraper and ops.

**`scraper/app.py`** — add at the bottom, same pattern as dbt_runner:
```python
@app.get("/health")
def health():
    return {"ok": True}
```

**`ops/app.py`** — same:
```python
@app.get("/health")
def health():
    return {"ok": True}
```

No DB check in the health endpoint — it is purely a liveness signal. If the process is up and responding, it's healthy. DB connectivity is checked separately in the n8n workflow via a direct Postgres node.

### Part 2: `Service Health Gate` n8n sub-workflow

**Trigger:** Called by all 7 workflows via `Execute Workflow` node, immediately after `Check Deploy Intent` returns `can_deploy = true`.

**Workflow structure:**

```
[Set Attempt Counter = 0]
    ↓
[HTTP Request: GET scraper/health]  ←─────────────────────────────┐
[HTTP Request: GET dbt_runner/health]                              │
[HTTP Request: GET ops/health]                                     │
[Postgres: SELECT 1]                                               │
    ↓                                                              │
[If: all 4 succeeded?]                                             │
    ├─ Yes → return {"healthy": true}                              │
    └─ No  → [Increment attempt counter]                           │
                ↓                                                  │
             [If: attempts < 20?]  (20 × 15s = 5 min max)         │
                ├─ Yes → [Wait 15s] ──────────────────────────────┘
                └─ No  → [Telegram alert: "Service health check timed out after 5 min"]
                              ↓
                         return {"healthy": false}
```

**Return value:** `{"healthy": true}` or `{"healthy": false}`

**Caller pattern:** Each of the 7 workflows adds an `If` node after `Call 'Service Health Gate'`:
- `healthy = true` → proceed to main workflow logic
- `healthy = false` → exit cleanly (Telegram already sent by sub-workflow)

**HTTP timeouts:** Each health request uses a 5s timeout. If a service is down, the attempt fails fast and moves to retry — it does not hang the loop for 5s per service (use parallel HTTP nodes or accept sequential with short timeouts).

**Postgres check:** Use an existing Postgres credential node, `SELECT 1`. If the connection succeeds, Postgres is healthy. This covers the scenario where Postgres itself is slow to restart.

### Part 3: Docker healthcheck on Postgres (optional bonus)

Add to the `postgres` service in `docker-compose.yml`:
```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U cartracker -d cartracker"]
  interval: 10s
  timeout: 5s
  retries: 5
```

This surfaces Postgres health in `docker ps` and `docker compose ps` without changing any application logic.

### Files

| File | Change |
|------|--------|
| `scraper/app.py` | Add `GET /health` → `{"ok": True}` |
| `ops/app.py` | Add `GET /health` → `{"ok": True}` |
| `n8n/workflows/Service Health Gate.json` | **Create** — new sub-workflow |
| `n8n/workflows/Scrape Listings.json` | Add `Call 'Service Health Gate'` + `Healthy?` If node |
| `n8n/workflows/Job Poller.json` | Same |
| `n8n/workflows/Results Processing.json` | Same |
| `n8n/workflows/Build DBT.json` | Same |
| `n8n/workflows/Orphan Checker.json` | Same |
| `n8n/workflows/Parse Detail Pages.json` | Same |
| `n8n/workflows/Scrape Detail Pages.json` | Same |
| `docker-compose.yml` | Add `healthcheck` to `postgres` service (optional) |

### Verification

1. Add `GET /health` to scraper and ops, rebuild both — confirm `curl http://localhost:8000/health` and `curl http://localhost:8060/health` return `{"ok": true}`
2. Import `Service Health Gate` workflow into n8n, manually execute it — should return `{"healthy": true}` immediately when all services are up
3. **Failure test:** Stop the scraper container, manually trigger `Service Health Gate` — should retry and eventually send Telegram alert after 5 min (or test with 2 attempts for speed)
4. Import updated workflow JSONs, trigger any workflow manually — confirm it calls the health gate and proceeds normally
5. Simulate post-deploy: stop and restart scraper, immediately trigger Scrape Listings — should wait for health gate to pass before proceeding

---

## Plan 59: Orphan Checker — Add Stale detail_scrape_claims Cleanup

**Status:** Not started
**Priority:** Medium

The Orphan Checker currently cleans up stale `runs` and `scrape_jobs` but does not touch `detail_scrape_claims`. Claims stuck in `running` accumulate silently, blocking vehicles from the detail scrape queue indefinitely. Root cause identified: the Scrape Detail Pages workflow had a split execution branch (from Error Rate High? IF node) that caused Release Claims to only fire on one branch. Fixed by duplicating the Call 'Parse Detail Pages' → Release Claims path on both branches. However, a safety net in the Orphan Checker is still needed to catch any future orphaned claims.

### Fix
Add a cleanup step to the Orphan Checker n8n workflow:
```sql
DELETE FROM detail_scrape_claims
WHERE status = 'running'
AND claimed_by NOT IN (
    SELECT run_id::text FROM runs WHERE status = 'running'
);
```
This releases any claims whose `run_id` doesn't match an actively running scrape — safe to run even while a scrape is in progress.

---

## Plan 58: Scraper Architecture — DB Responsibility Consolidation

**Status:** Partially complete (2026-03-27)
**Priority:** Low (works, philosophical)

| Location | Operation | Notes |
|----------|-----------|-------|
| `lifespan` startup | Orphan recovery | **Removed** — handled by n8n Orphan Checker (Plan 59) |
| `_fetch_known_vins` | Reads `analytics.int_vehicle_attributes` | **Documented exception** — payload size makes HTTP impractical |
| `POST /search_configs/advance_rotation` | Reads/writes `search_configs` + `runs` | **Remaining** — pure orchestration logic still in scraper |

---

## Plan 79: Multi-Instance Detail Scraping + Artifact Pipeline Refactor

**Status:** Not started
**Priority:** Highest — blocking on 403 rate limiting; prerequisite for Plan 68 (cloud deployment)

Cars.com flagged our IP after sustained 50K+/day scraping over multiple days. Even at low rates the IP gets intermittent 403 blocks. The fix is distributing detail scraping across multiple Oracle Cloud Free Tier instances (each with its own public IP) at ~800/hr per instance (~2,400/hr total, enough for a 24h refresh of ~55K active listings).

This requires a deeper refactor: both SRP and detail scrapes must write artifacts (including raw HTML) directly to Postgres so that n8n no longer needs to carry ~500KB-1.2MB HTML payloads through its JSON pipeline. This eliminates the Job Poller workflow entirely.

### Architecture

```
Central VM
  n8n:
    "Scrape Listings"       -> POST http://scraper:8000/scrape_results (unchanged trigger)
    "Scrape Detail Batch"   -> POST http://{{ round-robin worker }}/scrape_detail_batch
    "Results Processing"    -> finds unprocessed artifacts, parses by type
    (Job Poller removed, Parse Detail Pages folded into Results Processing)

  postgres (central)
  scraper (local — SRP + detail + parsing)

Worker VM A (detail-scraper, slim)  -> own public IP
Worker VM B (detail-scraper, slim)  -> own public IP
```

### Implementation Steps

**Phase 1 — Schema:**
1. Add `raw_html bytea` column to `raw_artifacts`
2. Create `detail_worker_hosts` table (host_url PK, enabled, last_used_at) for round-robin routing

**Phase 2 — Scraper writes artifacts to DB:**
3a. `scrape_detail_fetch()` returns `raw_html` bytes in artifact dict
3b. `scrape_results()` (SRP) returns `raw_html` bytes in artifact dict
3c. New `insert_artifacts_sync()` function — psycopg2 batch INSERT with raw_html
3d. `_run_scrape_job` (SRP) inserts artifacts to DB directly, stores metadata only in job store
3e. Scraper background threads update `runs` table directly (progress, status, errors)
4. New `POST /scrape_detail_batch` endpoint — accepts batch of listings, queues background job, writes artifacts to DB
5. Simplify `/scrape_results/jobs/completed` to return metadata only (no artifacts, no HTML)

**Phase 3 — n8n refactor:**
6. New "Scrape Detail Batch" workflow — round-robin worker selection, batch submit, lightweight polling
7. Update "Scrape Listings" — remove Job Poller dependency, add inline completion polling
8. New unified "Results Processing" workflow — picks up unprocessed artifacts by type, routes to parsers, replaces Parse Detail Pages

**Phase 4 — Parsing fallback:**
9. `/process/detail_pages` falls back to `raw_html` from DB when filepath unavailable

**Phase 5 — Slim image:**
10. `detail_scraper/` service — FastAPI with batch endpoint + job management, curl_cffi + psycopg2 only (~200MB image), no Playwright/SRP/parsing

**Phase 6 — Deploy:**
Oracle Cloud ARM instances, each with own public IP, connect to central Postgres over VCN

### n8n workflows retired
| Workflow | Replaced by |
|----------|-------------|
| Job Poller | Inline polling in each scrape workflow |
| Scrape Detail Pages | Scrape Detail Batch |
| Parse Detail Pages | Results Processing |

### Key design decisions
- **Dual-write for now:** files still written to disk alongside DB storage; cut files later once DB path is proven
- **Round-robin via DB table:** `detail_worker_hosts` — add/remove workers by editing rows, no workflow changes
- **Lightweight polling:** n8n polls `/scrape_results/jobs/completed` for metadata (status, counts, errors) — no HTML in responses
- **Unified parsing:** one "Results Processing" workflow handles both `detail_page` and `results_page` artifacts

### Files to create
- `detail_scraper/app.py`, `detail_scraper/processors/scrape_detail.py`, `detail_scraper/Dockerfile`, `detail_scraper/requirements.txt`
- `docker-compose.worker.yml`
- `db/seed/detail_worker_hosts.sql`
- `n8n/workflows/Scrape Detail Batch.json`, `n8n/workflows/Results Processing.json`

### Files to modify
- `scraper/processors/scrape_detail.py` — add `raw_html`
- `scraper/processors/scrape_results.py` — add `raw_html`
- `scraper/app.py` — add `/scrape_detail_batch`, update `_run_scrape_job`, simplify `/jobs/completed`, DB fallback in `/process/detail_pages`
- `db/schema/schema_new.sql` — `raw_html` column, `detail_worker_hosts` table
- `n8n/workflows/Scrape Listings.json` — inline polling, remove Job Poller dependency
- `docs/PLANS.md`

