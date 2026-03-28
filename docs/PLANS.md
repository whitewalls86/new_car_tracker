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
| 14.1 | **VIN case normalization** — `stg_detail_observations` computes `vin17` via `upper(d.vin)` with length/format validation; all downstream models use `vin17` | 2026-03-26 |
| 14.5 | **Price events dedup** — `int_price_events` uses `SELECT DISTINCT ON (vin, observed_at, price)` with source priority (detail > srp > carousel) in full-refresh mode | 2026-03-26 |
| 14.9 | **Browser singleton lock** — moot; `browser.py` uses `threading.local()` so each worker thread owns its own browser instance, no shared state | 2026-03-26 |
| 14.11 | **Chrome fingerprint env var** — `fingerprint.py` rotates through `["132", "133", "134", "135"]`; no longer a hardcoded single value | 2026-03-26 |
| 35 | **dbt schema audit** — all 9 sub-items complete: stg_raw_artifacts, stg_dealers, carousel hints incremental, deleted int_latest_dealer_name_by_vin, benchmarks→table, .yml docs, source descriptions, dashboard unlisted query | 2026-03-27 |
| 54+58 | **Admin UI overhaul + DB responsibility consolidation** — run history (/admin/runs), dbt action panel (/admin/dbt), intent management (DB-backed), dbt docs generate/serve (/dbt-docs/), log viewer (/admin/logs), logger.exception() throughout, orphan recovery removed from lifespan (→ n8n Plan 59), search config routes → /admin/searches/, test containers (docker-compose.test.yml), port separation (override.yml) | 2026-03-27 |

---

## Remaining Priority Order

| Priority | Item | Notes |
|----------|------|-------|
| 1 | **55** — Dashboard review | Audit current state, fix issues, improve UX |
| 2 | **56** — Analytics next steps | Identify new insights, models, or views to build |
| 3 | **60** — Safe redeploy / health gate | n8n waits for services to come back up after rebuild |
| 4 | **61** — Python unit tests | pytest for scraper, dbt_runner, and admin logic |
| 5 | **62** — CI/CD (GitHub Actions) | Automated lint, type check, pytest, docker build, dbt test on every PR |
| 6 | **63** — Schema migration management | Alembic/Flyway/other — tool TBD, tradeoffs to explore |
| 7 | **64** — PgBouncer connection pooling | Coordination layer in front of Postgres; fixes connection budget across all 8 services |
| 8 | **65** — Auth stack (Authelia + Google OAuth + Postgres roles) | Required before any public deployment |
| 9 | **66** — SQL injection audit | Verify all queries are parameterized; required before public deployment |
| 10 | **29** — n8n API foundation | Unlocks credential automation, admin triggers, redeploy workflow pause, execution status |
| 11 | **67** — n8n credential automation | Depends on Plan 29; closes silent fresh-install failure |
| 12 | **74** — dbt logic flaw: new search configs not reaching mart tables | Active data bug; Kia Sportage PHEV not appearing in dashboard |
| 13 | **73** — Scraper code review & refactor | Full quality pass + structural split; includes SRP breakpoint fix |
| 14 | **68** — Cloud deployment (Oracle Free Tier) | Move project to real cloud infrastructure; free forever |
| 15 | **69** — Terraform IaC | Describe cloud infrastructure as code; reproducible deployments |
| 16 | **70** — Type annotations | Add type hints throughout scraper and dbt_runner; enforced by mypy in CI |
| 17 | **71** — Airflow DAG translation | Translate n8n pipeline workflows into Airflow DAGs as parallel implementation |
| 18 | **72** — Data lake / Parquet proof of concept | MinIO + Parquet for artifact storage; demonstrates data lake architecture |
| 19 | **53** — Dashboard cleanup/optimization | Pipeline Health tab is bloated; consider collapsible sections or sub-tabs |
| 20 | **58 (remainder)** — advance_rotation ownership | Pure orchestration logic still lives in scraper; low priority |
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

## Plan 63: Schema Migration Management

**Status:** Not started
**Priority:** Medium

Currently `schema_new.sql` is a pg_dump snapshot of the current state — it answers "what does the schema look like now" but not "how do I get an existing database from an older state to the current one." Every schema change is applied manually to production with no rollback capability and no audit trail.

### The problem this solves
- Schema changes applied by hand with no rollback if something breaks
- No ordered history of what changed and when
- A fresh install runs the full dump fine, but an existing database that's one version behind has to figure out what changed manually
- CI/CD (Plan 62) needs a reliable way to stand up a test database from scratch

### Current state
The ingredients are mostly there already:
- `db/schema/schema_new.sql` — full schema dump (source of truth for current state)
- `db/schema/plan25_add_customer_id.sql` — example of a one-off migration script
- `db/seed/` — seed data scripts

A migration tool would formalize the ordering and tracking of these changes in a version-controlled sequence.

### Tool options to evaluate

| Tool | Language | Notes |
|------|----------|-------|
| **Alembic** | Python | Most common in FastAPI/SQLAlchemy stacks; can autogenerate migrations from model changes; overkill if not using SQLAlchemy ORM |
| **Flyway** | Java (CLI available) | SQL-first; just numbered `.sql` files; no Python required; simple mental model |
| **sqitch** | Perl (CLI) | SQL-first with deploy/revert/verify per change; more powerful than Flyway but steeper learning curve |
| **Raw numbered SQL files** | SQL | No external tool; just a convention (`001_`, `002_`) + a small tracking table in Postgres; maximum simplicity |

### Open questions
- Do we want a Python-native tool (Alembic) that fits the existing stack, or a SQL-first tool (Flyway, sqitch) that matches how we already write migrations?
- How important is autogeneration vs. hand-written SQL scripts?
- Does CI/CD need to run migrations automatically, or is manual apply on deploy acceptable for now?

### Notes
- Whatever tool is chosen, `schema_new.sql` stays as a reference for fresh installs
- `db/schema/` and `db/seed/` structure maps naturally to a migration + seed convention
- This is an area where tradeoffs should be explored before committing to a tool

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

**Status:** Not started
**Priority:** High

`scraper/app.py` has accumulated significant scope: scrape logic, async job management, background threading, rotation guards, processing endpoints, and FastAPI wiring all in one file. This plan is a full code review and refactor — structural split plus logic quality pass.

### Known issues to address
- **SRP breakpoint logic** — known breakpoint issues in discovery mode; the VIN breakpoint / early-exit conditions are not behaving correctly in all cases. Needs investigation and fix as part of this review.

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
- SRP breakpoint fix is the known priority; other issues may surface during review
- This plan does not touch `dbt_runner/` or `dashboard/` — scraper only

---

## Plan 74: dbt Logic Flaw — New Search Configs Not Reaching Mart Tables

**Status:** Not started
**Priority:** High

**Symptom:** Kia Sportage Plug-In Hybrid added as a new search config is not appearing in the dashboard. Suspected cause: a filtering step in the dbt DAG is excluding new make/model combinations that have no existing observations yet, preventing them from ever being scraped or surfacing in mart tables.

### Working theory
`stg_search_configs` or `int_scrape_targets` may filter based on existing observation data, creating a chicken-and-egg problem: a new search config needs observations to pass the filter, but it can't get observations until it passes the filter.

### Investigation approach
Trace the full DAG from dashboard query back to source:
1. What does `mart_vehicle_snapshot` / `mart_deal_scores` filter on for make/model?
2. Where does `int_scrape_targets` get its make/model values — is it from `search_configs` directly or from observation data?
3. Does `stg_search_configs` introduce any filtering that could exclude configs with no history?
4. Verify Kia Sportage PHEV is actually in `search_configs` and enabled
5. Check `ops_detail_scrape_queue` — is it generating any jobs for this config?

### Fix
TBD pending DAG trace. Likely a filter condition that needs to be loosened or a join that needs to become a LEFT JOIN.

### Notes
- Do not guess and change — trace the full chain first per project convention
- This is a pure dbt issue; no Python changes expected

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

## Plan 60: Safe Redeploy — Service Health Gate for n8n

**Status:** Not started
**Priority:** Medium

When scraper or dbt_runner containers are rebuilt and restarted, there's a window where the service is down. Currently n8n has no awareness of this — scheduled workflows fire regardless and fail mid-flight.

### Goal

Trigger a rebuild from the admin UI (or manually), have n8n workflows automatically wait for services to come back up, then resume normally. No manual pausing or timing required.

### Design

**Health-gate sub-workflow** (`Service Health Gate`):
- Accepts `service_url` as input (e.g. `http://scraper:8000/health` or `http://dbt_runner:8080/health`)
- Polls `/health` every 15s, up to ~5 min (20 retries)
- Returns success once 200 is received, failure if timeout exceeded
- Called by Results Processing and Parse Detail Pages at the top of each workflow before any scraper/dbt call

**Maintenance flag** (optional enhancement):
- `POST /admin/maintenance` sets a flag in DB (or a simple `maintenance_mode` table)
- Scraper's `/health` returns 503 while flag is set — triggers the health gate to wait without needing to actually bring the container down
- Useful for pre-signaling before a rebuild starts, giving in-flight workflows a chance to finish

**Redeploy trigger** (optional — n8n webhook):
- n8n webhook `POST /webhook/redeploy` accepts `service: scraper|dbt_runner`
- Signals maintenance mode, waits for in-flight runs to finish (polls `runs` table for active status), then responds — operator runs `docker compose build + up -d` externally and hits the webhook

### Simplest viable version

Just the health-gate sub-workflow added to the top of each n8n workflow. No maintenance flag, no webhook. When a container restarts, n8n retries `/health` until it's back. Combined with the Orphan Checker (Plan 59) cleaning up any jobs that were mid-flight during the restart, this covers the common case with minimal complexity.

### Flow
```
[Workflow trigger]
  → [Service Health Gate: poll /health until 200 or timeout]
      ├─ Healthy → [proceed with workflow]
      └─ Timeout → [send Telegram alert: "scraper unreachable after redeploy"]
```

### Files
| File | Action |
|------|--------|
| `n8n/workflows/Service Health Gate.json` | Create — new sub-workflow, accepts service_url |
| `n8n/workflows/Results Processing.json` | Add health gate call at start |
| `n8n/workflows/Parse Detail Pages.json` | Add health gate call at start |
| `scraper/routers/admin.py` | (optional) Add maintenance flag endpoints |

---

## Plan 58: Scraper Architecture — DB Responsibility Consolidation

**Status:** Partially complete (2026-03-27)
**Priority:** Low (works, philosophical)

| Location | Operation | Notes |
|----------|-----------|-------|
| `lifespan` startup | Orphan recovery | **Removed** — handled by n8n Orphan Checker (Plan 59) |
| `_fetch_known_vins` | Reads `analytics.int_vehicle_attributes` | **Documented exception** — payload size makes HTTP impractical |
| `POST /search_configs/advance_rotation` | Reads/writes `search_configs` + `runs` | **Remaining** — pure orchestration logic still in scraper |

