# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only.

## READ ME FIRST ##

Updates since last work: Plan 65 (auth) complete — Google OAuth via oauth2-proxy, Caddy forward_auth with 3-tier authorization (viewer/power_user/admin), scoped Postgres roles (viewer/scraper_user/dbt_user) via Flyway V003+V004 migrations. Domain migrated from DuckDNS to cartracker.info (Cloudflare DNS). Site is live and locked down at https://cartracker.info. Plan 81 (data migration) is next.

---

## Active

| Plan | Title | Status | Notes |
|------|-------|--------|-------|
| [81](plan_81_data_migration.md) | Data migration — local → cloud | **Top priority** | Auth is done; migrate historical data now |
| [66](plan_66_sql_injection.md) | SQL injection audit | **Planned** | Required before public exposure |

---

## Backlog

| Priority | Plan | Title | Notes |
|----------|------|-------|-------|
| 1 | [81](plan_81_data_migration.md) | Data migration — local → cloud | Postgres + raw artifacts + MinIO parquet |
| 2 | [66](plan_66_sql_injection.md) | SQL injection audit | Required before public exposure |
| 3 | [82](plan_82_user_management.md) | User management — DB-backed auth | Replaces ADMIN_PATTERN env vars; adds observer role + access requests |
| 4 | [83](plan_83_n8n_workflow_viewer.md) | n8n workflow viewer | Read-only portfolio page; depends on Plan 82 |
| 5 | [69](plan_69_terraform.md) | Terraform IaC | Write after manual provisioning; shows cloud maturity |
| 6 | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | Oracle VM is live; shows distributed design |
| 7 | [71](plan_71_airflow.md) | Airflow DAG translation | Portfolio breadth — multi-orchestrator knowledge |
| 8 | [77](plan_77_sql_tests.md) | SQL query tests | Depends on Plan 63; completes testing story |
| 9 | [64](plan_64_pgbouncer.md) | PgBouncer connection pooling | Infrastructure nicety; not differentiating for portfolio |
| 10 | [70](plan_70_type_annotations.md) | Type annotations | Code quality; coordinate with Plan 73 linting pass |
| 11 | [29](plan_29_n8n_api.md) | n8n API foundation | Low portfolio value |
| 12 | **14.12** | `max_safety_pages` validator | No bounds check; low risk |

---

## Completed
See [completed_plans.md](completed_plans.md) for full list.
