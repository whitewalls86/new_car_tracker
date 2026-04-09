# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only.

## READ ME FIRST ##

Updates since last work: Plan 68 (Cloud deployment) phases 1-4 complete — Oracle VM live at https://cartracker-scraper.duckdns.org, Flyway integrated into docker-compose, deploy.sh created. Plan 65 (auth) is next — lock down the live URL before migrating data or running the pipeline on cloud.

---

## Active

| Plan | Title | Status | Notes |
|------|-------|--------|-------|
| [65](plan_65_auth.md) | Auth stack (Authelia + Google OAuth) | **Top priority** | Required before sharing live demo publicly |
| [81](plan_81_data_migration.md) | Data migration — local → cloud | **Next** | Migrate after auth is locked down |
| [66](plan_66_sql_injection.md) | SQL injection audit | **Planned** | Required before public exposure |

---

## Backlog

| Priority | Plan | Title | Notes |
|----------|------|-------|-------|
| 1 | [65](plan_65_auth.md) | Auth stack (Authelia + Google OAuth) | Required before sharing live demo publicly |
| 2 | [81](plan_81_data_migration.md) | Data migration — local → cloud | Postgres + raw artifacts + MinIO parquet |
| 3 | [66](plan_66_sql_injection.md) | SQL injection audit | Required before public exposure; unblocks 65 going live |
| 4 | [69](plan_69_terraform.md) | Terraform IaC | Write after manual provisioning; shows cloud maturity |
| 5 | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | Oracle VM is live; shows distributed design |
| 6 | [71](plan_71_airflow.md) | Airflow DAG translation | Portfolio breadth — multi-orchestrator knowledge |
| 7 | [77](plan_77_sql_tests.md) | SQL query tests | Depends on Plan 63; completes testing story |
| 8 | [64](plan_64_pgbouncer.md) | PgBouncer connection pooling | Infrastructure nicety; not differentiating for portfolio |
| 9 | [70](plan_70_type_annotations.md) | Type annotations | Code quality; coordinate with Plan 73 linting pass |
| 10 | [29](plan_29_n8n_api.md) | n8n API foundation | Low portfolio value |
| 11 | **14.12** | `max_safety_pages` validator | No bounds check; low risk |
| — | [82](plan_82_self_hosted_runner.md) | Self-hosted GitHub Actions runner (ARM64) | Unprioritized; eliminates CI/prod platform divergence |

---

## Completed
See [completed_plans.md](completed_plans.md) for full list.
