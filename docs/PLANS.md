# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only. For system design patterns (schema layout, hot+staging, MinIO tiers, testing strategy, drain endpoints), see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current State (as of 2026-04-20)

Site is live at https://cartracker.info. Auth (Plan 82), data migration (Plan 81), CI/CD (Plans 62+63), integration testing (Plan 84), and MinIO artifact store (Plan 97) are complete. Airflow is running with all maintenance DAGs live. Ops coordination endpoints (`advance_rotation`, `claim-batch`, `release`) are implemented.

Architecture is transitioning from Postgres+dbt as the observation data owner to a MinIO-first design: Postgres holds only hot operational state (current inventory, VIN mappings, work queues); MinIO holds the complete observation record. The transition sequence is: V018 → Plan 93 → Plan 71 completion → V019 → n8n cutover → Plan 96 → Plan 90.

---

## Active

| Plan | Title | Notes |
|------|-------|-------|
| [V018](plan_v018_schema_migration.md) | Schema migration | Drop Plan 89 dead tables; create `ops.price_observations`, `ops.vin_to_listing`, `ops.blocked_cooldown`, `staging.detail_scrape_claim_events`, `staging.blocked_cooldown_events`; fix UUID column types on `detail_scrape_claims` and `blocked_cooldown`. Safe to ship now — nothing reads the tables being dropped. |
| [71](plan_71_airflow.md) | Airflow migration (steps 8–9) | `scrape_listings` and `scrape_detail_pages` DAGs can be written now in parallel with V018. Unblocks Plan 79 the moment `scrape_detail_pages` is live. |

---

## Backlog

| Priority | Plan | Title | Blocked on |
|----------|------|-------|------------|
| 1 | [93](plan_93_processing_service.md) | Processing service core | V018 |
| 2 | [71](plan_71_airflow.md) | Airflow migration (steps 10–15) | Plan 93 (`results_processing` DAG); V019 before n8n cutover |
| 3 | **V019** | View migration | Plan 93 live with production data; rewrites `ops_vehicle_staleness` and `ops_detail_scrape_queue` as plain Postgres views reading HOT tables; inlines `stg_blocked_cooldown` backoff formula; removes dbt dependency from the scrape queue |
| 4 | [96](plan_96_silver_layer.md) | Silver layer validation + DuckDB analytics | Plan 93 in production for 2+ weeks |
| 5 | [90](plan_90_dbt_cleanup.md) | dbt decommission | Plan 96 validation gates |
| — | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | `scrape_detail_pages` Airflow DAG live (Plan 71 step 9); resume when IP flagging requires it |
| — | **86** | Grafana observability stack | Airflow live so real DAG metrics exist to observe |
| — | **87** | Kafka event-driven layer | Airflow DAGs producing events + multiple consumers exist |
| — | **88** | Kubernetes | 5+ services under management |
| — | [69](plan_69_terraform.md) | Terraform IaC | Manual provisioning stable |
| — | [66](plan_66_sql_injection.md) | SQL injection audit | Lower urgency with DB-backed auth and Caddy as sole gatekeeper |
| — | [94](plan_94_api_docs.md) | API documentation hub | Swagger UI for all FastAPI services |
| — | [95](plan_95_portfolio_landing_page.md) | Portfolio landing page | Replace `/info` README dump |

---

## Sequencing Rationale

**V018 first** — creates the HOT tables that Plan 93 writes to, migrates `blocked_cooldown` and `detail_scrape_claims` to ops schema, and drops dead Plan 89 tables. Zero breaking risk: the Plan 89 tables have never been written to; everything in production still reads from dbt models.

**`scrape_listings` + `scrape_detail_pages` DAGs now** — all ops endpoints exist, MinIO write path exists. Shadow-run alongside n8n immediately. `scrape_detail_pages` going live unblocks Plan 79 with no other dependencies.

**Plan 93 before `results_processing` DAG** — the DAG calls `POST /process/batch` which doesn't exist until Plan 93 is built. Everything else in Plan 71 can proceed independently.

**V019 before n8n cutover** — the scrape queue view (`ops_detail_scrape_queue`) currently depends on dbt models. V019 rewrites it as a plain Postgres view reading HOT tables, removing the dbt dependency. Must happen before n8n is decommissioned and before Plan 90 drops dbt, because both paths would break the queue otherwise.

**Plan 96 before Plan 90** — Plan 90 drops dbt and legacy Postgres observation tables. Those drops are irreversible. Plan 96's five validation checks are the explicit go/no-go gate: don't decommission dbt until silver has been running in production for at least 2 weeks and all checks pass.

**Plan 79 whenever needed** — IP flagging not currently active. The technical prerequisites (MinIO write path, ops claim endpoints, `scrape_detail_pages` DAG) either exist or are being built now. When IP flagging returns, provision Oracle Cloud VMs, add Airflow connections, and fan out the DAG. No other plans need to be complete first.

---

## In-Progress / Nearly Complete

| Plan | Title | Status |
|------|-------|--------|
| [71](plan_71_airflow.md) | Airflow migration | Steps 1–6 done; steps 7–15 pending |
| [92](plan_92_service_drain.md) | Service drain `/ready` endpoints | archiver + dbt_runner done; scraper (Plan 71 step 13) + processing (Plan 93) pending |
| [91](plan_91_uuid_type_cleanup.md) | UUID column type fixes | Scope collapsed to 2 columns; absorbed into V018 |

---

## Superseded

| Plan | Title | Reason |
|---|---|---|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy preserved; implementation superseded by Plans 93, 97, 96 |

---

## Completed

See [completed_plans.md](completed_plans.md) for full list. Recent completions:
- **97** — MinIO-first artifact store; `ops.artifacts_queue` live; V017 deployed (2026-04-20)
- **98** — Bronze data architecture; schema complete in V017 (2026-04-20)
- **84** — Integration testing: 71 SQL smoke tests + dbt logic coverage + ops API tests (2026-04-16)
- **82** — DB-backed auth with access requests (PRs #64–#67, 2026-04-14)
- **81** — Data migration local → cloud (2026-04-14)
- **80** — 403 cooldown with exponential backoff
- **78** — FlareSolverr + curl_cffi impersonation
- **62+63** — CI/CD + Flyway schema migrations
