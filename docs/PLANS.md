# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only.

## Current State (as of 2026-04-17)

Site is live at https://cartracker.info. Auth (Plan 82), data migration (Plan 81), CI/CD (Plans 62+63), user management, and integration testing (Plan 84) are all complete. 71 SQL smoke tests (Layer 1), full dbt model logic tests (Layer 2), and ops API integration tests (Layer 3, 5 files, 37 tests) are in CI.

Architecture is transitioning from Postgres+dbt as the observation data owner to a MinIO-first design: Postgres holds only hot operational state (current inventory, VIN mappings, work queue), MinIO holds the complete observation record (silver layer). Plans 97 → 93 → 71 → 96 → 90 implement this transition in sequence.

---

## Active

_No active plans._

---

## Backlog

| Priority | Plan | Title | Notes |
|----------|------|-------|-------|
| 1 | [97](plan_97_minio_artifact_store.md) | MinIO-first artifact store | Scraper writes HTML directly to MinIO; `artifacts_queue` replaces `raw_artifacts` + `artifact_processing`. Core prerequisite for Plans 93 and 71 processing service work. |
| 2 | [93](plan_93_processing_service.md) | Processing service core | Reads from MinIO; writes to MinIO silver (primary) and Postgres HOT tables (`price_observations`, `vin_to_listing`). Unlisted = DELETE. Carousel filtered against `search_configs`. Depends on Plan 97. |
| 3 | [71](plan_71_airflow.md) | Airflow migration | Replaces n8n with Python DAGs. Airflow setup, coordination endpoints, and non-processing DAGs can start before Plan 93; `results_processing` DAG requires Plan 93 complete. |
| 4 | [96](plan_96_silver_layer.md) | Silver layer validation + DuckDB analytics | Validates MinIO silver is complete and correct after Plan 93 ships. Establishes production DuckDB query surface that replaces dbt analytics. Explicit go/no-go gate for Plan 90. |
| 5 | [90](plan_90_dbt_cleanup.md) | dbt decommission | Remove dbt, dbt_runner, and legacy Postgres observation tables once silver is validated. Replace analytics with DuckDB queries. Blocked on Plan 96. |
| — | [83](plan_83_n8n_workflow_viewer.md) | n8n workflow viewer | Read-only portfolio page. Quick win, self-contained, unblocked now. |
| — | **86** | Grafana observability stack | Prometheus + Loki + Tempo + Grafana. Best sequenced after Airflow so real DAG metrics exist to observe. |
| — | **87** | Kafka event-driven layer | Replace schedule-driven scraping with events. Defensible only after Airflow DAGs produce events and multiple consumers exist. Processing service already has emit stubs (Plan 93). |
| — | **88** | Kubernetes | Orchestration upgrade over Docker Compose. Makes sense once Airflow is running and 5+ services need managing. |
| hold | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | On hold — IP flagging not currently a problem. Unblocked once Plan 97 ships (MinIO write path was the main blocker). |
| — | [69](plan_69_terraform.md) | Terraform IaC | Write after manual provisioning is stable; shows cloud maturity. |
| — | [66](plan_66_sql_injection.md) | SQL injection audit | Lower urgency now that auth is DB-backed and Caddy is sole gatekeeper. |
| — | [94](plan_94_api_docs.md) | API documentation hub | Swagger UI for all FastAPI services via Caddy `handle_path`. |
| — | [95](plan_95_portfolio_landing_page.md) | Portfolio landing page | Replace `/info` README dump with purpose-built landing page. |

---

## Sequencing Rationale

**Plan 97 first** — everything in the new architecture depends on the scraper writing to MinIO and `artifacts_queue` existing. Pure infrastructure with no logic changes. Validate before touching the processing service.

**Plan 93 before Plan 71 results_processing DAG** — the `results_processing` Airflow DAG calls `POST /process/batch`. That endpoint can't exist until Plan 93 is built. The rest of Plan 71 (Airflow setup, coordination endpoints, maintenance DAGs) can run in parallel with Plan 93.

**Plan 96 before Plan 90** — Plan 90 removes dbt and drops legacy Postgres observation tables. Those drops are irreversible. Plan 96's validation is the explicit gate: don't decommission dbt until silver has been running in production for at least 2 weeks and all five validation checks pass.

**Plan 79 still on hold** — IP flagging hasn't returned. Plan 97 resolves the main technical blocker (MinIO write path). When resumed, Plan 79 is: provision VMs + update Airflow connections + fan out the DAG. Most of the hard work will already be done.

---

## Superseded Plans

| Plan | Title | Reason |
|---|---|---|
| [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Philosophy correct; implementation superseded. `listing_to_vin`/`vin_state`/append-only `price_observations` in Postgres replaced by `vin_to_listing` + `price_observations`-as-HOT-table + MinIO silver. See Plans 93, 97, 96. |

---

## Completed

See [completed_plans.md](completed_plans.md) for full list. Notable recent completions:
- **84** — Integration testing: 71 SQL smoke tests + full dbt model logic coverage + ops API tests (2026-04-16)
- **82** — DB-backed auth with access requests (PRs #64–#67, 2026-04-14)
- **81** — Data migration local → cloud (2026-04-14)
- **62+63** — CI/CD + Flyway schema migrations
- **80** — 403 cooldown with exponential backoff
- **78** — FlareSolverr + curl_cffi impersonation
