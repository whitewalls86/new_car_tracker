# Cartracker — Plans & Roadmap

Each plan has its own file in `docs/`. This file is the index only.

---

## Active

| Plan | Title | Status | Notes |
|------|-------|--------|-------|
| [72](plan_72_parquet_archival.md) | Parquet archival (MinIO) | **Top priority** | Biggest DE portfolio showcase; `archiver/` service not yet built |
| [62](plan_62_cicd.md) + [63](plan_63_flyway.md) | CI/CD + Flyway (bundle) | **Planned** | Flyway first; together they add the CI badge and schema versioning |
| [53](plan_53_dashboard_cleanup.md) | Dashboard cleanup | **In progress** | File split done; Pipeline Health tab layout remaining — close out |
| [73](plan_73_scraper_refactor.md) | Scraper refactor | **Deferred** | Review after Plan 72; linting pass will inform whether full structural split is worth pursuing |

---

## Backlog

| Priority | Plan | Title | Notes |
|----------|------|-------|-------|
| 1 | [68](plan_68_cloud_deployment.md) | Cloud deployment — Oracle Free Tier | Live URL is the #1 portfolio artifact |
| 2 | [65](plan_65_auth.md) | Auth stack (Authelia + Google OAuth) | Required before sharing live demo publicly |
| 3 | [66](plan_66_sql_injection.md) | SQL injection audit | Required before public exposure; unblocks 65/68 going live |
| 4 | [69](plan_69_terraform.md) | Terraform IaC | Write after manual provisioning; shows cloud maturity |
| 5 | [79](plan_79_multi_instance.md) | Multi-instance detail scraping | Implement once Oracle VMs are provisioned; shows distributed design |
| 6 | [71](plan_71_airflow.md) | Airflow DAG translation | Portfolio breadth — multi-orchestrator knowledge |
| 7 | [77](plan_77_sql_tests.md) | SQL query tests | Depends on Plan 63; completes testing story |
| 8 | [64](plan_64_pgbouncer.md) | PgBouncer connection pooling | Infrastructure nicety; not differentiating for portfolio |
| 9 | [70](plan_70_type_annotations.md) | Type annotations | Code quality; coordinate with Plan 73 linting pass |
| 10 | [29](plan_29_n8n_api.md) | n8n API foundation | Unlocks credential automation; low portfolio value |
| 11 | [67](plan_67_n8n_credentials.md) | n8n credential automation | Depends on Plan 29 |
| 12 | **14.12** | `max_safety_pages` validator | No bounds check; low risk |

---

## Completed
See [completed_plans.md](completed_plans.md) for full list.
