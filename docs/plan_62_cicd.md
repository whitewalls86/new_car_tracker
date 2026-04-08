# Plan 62 + 63: CI/CD (GitHub Actions) + Flyway Schema Migrations

**Status:** Not started  
**Priority:** Top

These two plans are implemented together. Flyway provides the migration-tracked database that the CI dbt step builds against; CI is where Flyway earns its keep.

---

## What this adds

| Deliverable | Value |
|-------------|-------|
| `.github/workflows/ci.yml` | Green badge on every PR; catches regressions before merge |
| `pyproject.toml` | Ruff linting + pytest config in one canonical place |
| `db/migrations/` | Ordered, versioned SQL history; fresh installs and CI use the same migrations |
| `dbt/profiles.yml` `ci` target | dbt runs against ephemeral test Postgres in GitHub Actions |

---

## CI Pipeline — Jobs and order

```
lint (ruff)  ──┐
               ├──▶ unit-tests (pytest)  ──▶ dbt (postgres service + flyway + dbt build)
docker-build ──┘
```

Fast gates (lint, unit tests) run in parallel with Docker build. The dbt job runs last — it's the slowest and depends on a real Postgres.

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `.github/workflows/ci.yml` | Create — full pipeline |
| `pyproject.toml` | Create — ruff + pytest config |
| `db/migrations/V001__initial_schema.sql` | Create — schema from `schema_new.sql` + seed data from `db/seed/*.sql` |
| `dbt/profiles.yml` | Add `ci` target |

> **Note:** `schema_new.sql` is NOT replaced — it stays as a human-readable reference and fast fresh-install option. Flyway is the authoritative migration source going forward. New schema changes go in `db/migrations/` as `V002__*.sql`, `V003__*.sql`, etc., never editing existing files.

---

## Implementation

### 1. `pyproject.toml` (root)

```toml
[tool.ruff]
target-version = "py313"
line-length = 100
select = ["E", "F", "I"]
exclude = [
    "dbt_packages",
    "dbt/target",
    "target",
    "__pycache__",
    ".venv",
    "tests/__pycache__",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
```

mypy is deferred — the scraper stubs out patchright/curl_cffi which makes type inference noisy. Add it to a later plan once coverage is higher.

---

### 2. `dbt/profiles.yml` — add `ci` target

```yaml
cartracker:
  target: prod
  outputs:
    dev:
      type: postgres
      host: postgres
      port: 5432
      user: cartracker
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: cartracker
      schema: analytics_dev
      threads: 2
    prod:
      type: postgres
      host: postgres
      port: 5432
      user: cartracker
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: cartracker
      schema: analytics
      threads: 2
    ci:
      type: postgres
      host: "{{ env_var('DBT_HOST', 'localhost') }}"
      port: "{{ env_var('DBT_PORT', '5432') | int }}"
      user: "{{ env_var('DBT_USER', 'cartracker') }}"
      password: "{{ env_var('DBT_PASSWORD', 'cartracker') }}"
      dbname: "{{ env_var('DBT_DBNAME', 'cartracker') }}"
      schema: analytics_ci
      threads: 4
```

---

### 3. `db/migrations/` — Flyway naming convention

Each file is `V{NNN}__{description}.sql` — three-digit zero-padded version, double underscore separator, lowercase snake_case description.

**V001__initial_schema.sql** — the full schema from `schema_new.sql` plus initial seed data (dbt_intents inserts, dbt_lock table + initial row, detail_scrape_claims table, example_search_config insert). This is a complete baseline; all future migrations will be incremental deltas starting from V002.

After creating V001, the `db/schema/` ad-hoc files (`plan25_add_customer_id.sql`, `plan72_add_archived_at.sql`, etc.) are no longer needed and can be deleted (they've been absorbed into V001).

---

### 4. `.github/workflows/ci.yml`

```yaml
name: CI

on:
  pull_request:
    branches: [master]
  push:
    branches: [master]

jobs:

  lint:
    name: Lint (ruff)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - run: pip install ruff
      - run: ruff check .

  unit-tests:
    name: Unit tests (pytest)
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - name: Install test dependencies
        run: |
          pip install \
            pytest pytest-mock \
            fastapi httpx \
            psycopg2-binary asyncpg \
            requests pydantic jinja2 python-multipart \
            pyarrow \
            bs4 lxml
      - run: pytest tests/ -v

  docker-build:
    name: Docker build (all services)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker compose build

  dbt:
    name: dbt build + test
    runs-on: ubuntu-latest
    needs: unit-tests
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: cartracker
          POSTGRES_PASSWORD: cartracker
          POSTGRES_DB: cartracker
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10

    steps:
      - uses: actions/checkout@v4

      - name: Apply migrations (Flyway)
        uses: docker://flyway/flyway:10-alpine
        with:
          args: >-
            -url=jdbc:postgresql://localhost:5432/cartracker
            -user=cartracker
            -password=cartracker
            -locations=filesystem:/github/workspace/db/migrations
            migrate
        # Flyway action runs as a Docker container; /github/workspace is the
        # mounted repo checkout path used by GitHub Actions container steps.

      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"

      - name: Install dbt
        run: pip install dbt-postgres

      - name: dbt build
        working-directory: dbt
        run: dbt build --profiles-dir . --target ci
        env:
          DBT_HOST: localhost
          DBT_PORT: "5432"
          DBT_USER: cartracker
          DBT_PASSWORD: cartracker
          DBT_DBNAME: cartracker
```

---

## Plan 63 — Flyway notes

The Flyway migrations solve the same problem in two contexts:

| Context | How Flyway is used |
|---------|--------------------|
| CI (Plan 62) | Docker container step in the `dbt` job — runs migrations against ephemeral Postgres |
| Production | One-shot `docker run flyway/flyway migrate` before `docker compose up` on each deploy (replaces the manual psql step in the deploy doc) |

A Flyway service entry in `docker-compose.yml` is **not** needed — Flyway is a migration runner, not a long-lived service. It runs once and exits.

**Future schema changes:** create `V007__<description>.sql` (and so on), commit alongside the code change. Flyway applies it on next deploy. Never edit an existing migration file — Flyway checksums them and will refuse to run if they change.

---

## Verification

1. Create a branch, open a PR → CI workflow appears in GitHub Actions
2. Introduce a ruff violation (`import os, sys` on one line) → `lint` job fails, PR blocked
3. Break a unit test → `unit-tests` job fails
4. Introduce a bad Dockerfile directive → `docker-build` fails
5. Break a dbt model → `dbt` job fails
6. All green → merge unblocked; green badge appears on README
7. Run `flyway/flyway:10-alpine` locally against a test DB to confirm all V00x migrations apply clean from scratch in order

---

## Constraints / Notes

- GitHub provides 2,000 free CI minutes/month for private repos. This pipeline runs ~4-6 min per PR (lint ~30s, tests ~1m, Docker build ~2-3m, dbt ~1m). At several PRs/day it stays well within the free tier.
- `docker-build` does not push images — it just validates the Dockerfiles are buildable. Image push (for CD) is a later plan.
- Initial seed data (dbt_intents, dbt_lock, detail_scrape_claims, search_configs) is included in V001 migrations via `db/seed/*.sql` files.
- `dbt_lock` table is initialized with a single row by V001; the `dbt build` in CI will not conflict because the CI Postgres has no running dbt_runner.
