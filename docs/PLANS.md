# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only.

## Current State (as of 2026-04-16)

Site is live at https://cartracker.info. Auth (Plan 82), data migration (Plan 81), CI/CD (Plans 62+63), user management, and integration testing (Plan 84) are all complete. 71 SQL smoke tests (Layer 1), full dbt model logic tests (Layer 2), and ops API integration tests (Layer 3, 5 files, 37 tests) are in CI on `feature/integration-testing`.

---

## Active

_No active plans._

---

## Backlog

| Priority | Plan | Title | Notes |
|----------|------|-------|-------|
| 1 | [83](plan_83_n8n_workflow_viewer.md) | n8n workflow viewer | Read-only portfolio page. Plan 82 (auth) is complete — this is unblocked. Quick win. |
| 2 | [89](plan_89_ops_analytics_split.md) | Operational/analytics dbt split | Pull operational state (VIN mapping, price log, vin_state) out of dbt into app-owned tables. Prerequisite for Kafka and for clean Plan 71 processing service design. Implementation scope overlaps Plan 71. |
| 3 | [71](plan_71_airflow.md) | Airflow migration | Replaces n8n with Python DAGs — code-reviewable, testable, on every DE job description. Prerequisite for orchestration integration tests and Grafana DAG metrics. Plan 89 write path is implemented here. |
| 3.5 | [90](plan_90_dbt_cleanup.md) | dbt intermediate cleanup | Delete redundant dbt models after Plan 89 shadow period validates app-owned tables. Blocked on Plan 89. |
| 4 | **86** | Grafana observability stack | Prometheus + Loki + Tempo + Grafana. Best sequenced after Airflow so there are real DAG metrics to observe. Grafana is the right long-term choice (aligns with K8s + Kafka + enterprise DE stack). |
| 5 | **87** | Kafka event-driven layer | Replace n8n-schedule → scraper-API with `listing_updated` / `price_changed` events. Justified once Airflow DAGs produce events and multiple consumers exist (alerting, enrichment, dbt trigger). Do not add before Airflow — the "why Kafka" story isn't defensible yet. |
| 6 | **88** | Kubernetes | Orchestration upgrade over Docker Compose — scheduling, scaling, self-healing. Portfolio + scalability layer. Makes most sense once Airflow is running and 5+ services need managing. |
| 7 | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | **On hold** — not currently needed. Resume only if IP flagging returns. |
| 8 | [69](plan_69_terraform.md) | Terraform IaC | Write after manual provisioning; shows cloud maturity |
| 9 | [66](plan_66_sql_injection.md) | SQL injection audit | Lower urgency now that auth is DB-backed and Caddy is sole gatekeeper |

---

## Sequencing Rationale

**Why this order:**

1. **Plan 84 first** — closes the largest credibility gap. "518 unit tests with mocked DB + 71 SQL smoke tests + dbt model logic tests + API integration tests in CI" is a stronger story than any infrastructure evolution work.

2. **Plan 83 after** — self-contained, quick, makes the live site more useful for portfolio visitors.

3. **Plan 89 (dbt split) before Plan 71** — the processing service built in Plan 71 needs a clear write path. Plan 89 defines the three application-owned tables (`listing_to_vin`, `price_observations`, `vin_state`) and the SQL file pattern. Implementing Plan 89 as part of Plan 71 avoids retrofitting.

4. **Plan 71 (Airflow) next** — once Airflow runs, orchestration logic moves from n8n JSON blobs into Python DAGs that are reviewable, diffable, and directly testable. The processing service implements the Plan 89 write path from day one. This is the prerequisite that unblocks orchestration integration tests and makes observability meaningful.

4. **Plan 86 (Grafana) after Airflow** — there need to be real DAG-level metrics before observability infrastructure pays off. Grafana + Loki + Prometheus is the right long-term stack (aligns with K8s and Kafka when those come later).

5. **Plan 87 (Kafka) after Airflow** — the defensible "why Kafka" story requires event-producing Airflow DAGs and multiple consumers that justify fan-out. Adding Kafka to a cron-triggered scraper adds a message queue between a scheduler and an API, which doesn't hold up in an interview. After Airflow, the story becomes: "DAG completions emit events, downstream consumers react independently."

6. **Plan 88 (Kubernetes)** — portfolio and scalability upgrade. Makes sense once Airflow is running and there are 5+ services to orchestrate.

**What orchestration testing requires:**
Currently all INSERT/UPDATE/DELETE logic lives inside n8n workflow JSON — not in any code file. This means it cannot be integration-tested in CI in any meaningful way (it would require spinning up n8n, importing JSON blobs, triggering workflows, and asserting on DB state — testing n8n's HTTP executor, not your logic). After Airflow, that logic moves to Python task functions in `airflow/dags/` or `shared/`, which are directly callable in integration tests against a real DB.

---

## Completed

See [completed_plans.md](completed_plans.md) for full list. Notable recent completions:
- **84** — Integration testing: 71 SQL smoke tests + full dbt model logic coverage + ops API tests (2026-04-16)
- **82** — DB-backed auth with access requests (PRs #64–#67, 2026-04-14)
- **81** — Data migration local → cloud (2026-04-14)
- **62+63** — CI/CD + Flyway schema migrations
- **80** — 403 cooldown with exponential backoff
- **78** — FlareSolverr + curl_cffi impersonation
