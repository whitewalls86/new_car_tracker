# Plan 77: SQL Query Tests

**Status:** Not started
**Priority:** Medium

Mission-critical SQL queries live in four places outside dbt — scraper, dashboard, ops, and dbt_runner. These are never tested today. A schema change (column rename, type change, table drop) can silently break them and only surfaces at runtime.

## Problem
- `scraper/routers/admin.py` — queries `search_configs`, `runs`, `scrape_jobs`
- `ops/routers/admin.py` — same queries, now the canonical location post-migration
- `ops/routers/deploy.py` — queries `deploy_intent`, `n8n_executions`
- `dashboard/pages/*.py` — complex analytical queries against mart tables and ops views
- `dbt_runner/app.py` — queries `dbt_lock`, `dbt_build_log`

None of these are covered by dbt tests (which only validate dbt model output) or pytest unit tests (which mock the DB).

## Approach
Integration tests that run against a real Postgres test database (spun up in CI via the Flyway migration sequence from Plan 63):

- **Query smoke tests** — execute each mission-critical query against the test DB and assert it returns without error and with expected columns. No business logic assertions needed — the goal is catching schema breakage, not data correctness.
- **Parameterized** — test queries with representative parameter values (valid run_id, known search_key, etc.) seeded into the test DB
- **Organized by service** — `tests/sql/test_scraper_queries.py`, `tests/sql/test_ops_queries.py`, `tests/sql/test_dashboard_queries.py`, `tests/sql/test_dbt_runner_queries.py`

## CI/CD integration (Plan 62)
SQL tests run after Flyway migrations + seed data applied to the ephemeral test DB, before the dbt build step. A schema change that breaks a live query fails the pipeline before it can merge.

## Notes
- Depends on Plan 63 (Flyway) for the test DB setup pattern
- Does not replace dbt tests — dbt tests validate model logic; SQL tests validate application query compatibility
- Dashboard queries are the highest risk — they are the most complex and span the most tables
