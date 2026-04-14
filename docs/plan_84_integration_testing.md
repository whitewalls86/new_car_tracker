# Plan 84: Integration Testing

**Status:** Planned
**Priority:** High — fills the largest credibility gap in the test suite
**Absorbs:** Plan 77 (SQL query smoke tests)

## Problem

518 unit tests exist, all with mocked DB connections. They verify that code calls the right queries with the right parameters — but nothing verifies that those queries actually work against a real schema, or that the dbt models produce correct output from real data. A column rename, a broken incremental merge, or a wrong deal score calculation is invisible until it hits production.

The CI `dbt` job already spins up Postgres 16 + applies all Flyway migrations. The infrastructure is there. It just needs tests.

---

## Three Layers

### Layer 1 — SQL Smoke Tests (Plan 77 scope)

**Goal:** Catch schema breakage. Every mission-critical application query runs against the real DB and returns without error with expected columns.

**What breaks silently today:**
- Dashboard queries against mart tables / ops views
- Ops CRUD queries against `search_configs`, `runs`, `scrape_jobs`
- `dbt_runner` lock queries against `dbt_lock`, `dbt_build_log`
- Deploy intent queries in `ops/routers/deploy.py`

**Approach:** Seed minimal fixture rows, execute each query, assert columns present. No business logic assertions — just "this query runs and returns what the app expects."

**Files:**
```
tests/integration/sql/test_ops_queries.py
tests/integration/sql/test_dashboard_queries.py
tests/integration/sql/test_dbt_runner_queries.py
```

---

### Layer 2 — dbt Model Logic Tests

**Goal:** Assert that the transformation logic is correct. Seed known input data, run `dbt build`, assert specific output rows.

These are the tests that actually validate business logic — currently untested.

**Priority test cases:**

| Model | Input | Assert |
|---|---|---|
| `stg_blocked_cooldown` | 5 failed attempts on a listing | `fully_blocked = true`; `next_eligible_at` = correct exponential value |
| `int_listing_to_vin` | SRP observation + detail observation for same listing_id | Detail VIN wins over SRP VIN |
| `int_price_percentiles_by_vin` | 5 VINs in same make/model/trim cohort at known prices | Percentile ranks match expected order |
| `mart_deal_scores` | VIN priced at 10th percentile in cohort | `deal_tier = 'excellent'`; score > 80 |
| `ops_detail_scrape_queue` | Mix of stale VINs, cooled-down VINs, carousel hints | Cooled-down VINs absent; pool assignments correct |
| `ops_vehicle_staleness` | VIN with price observation > 24h ago | Appears in staleness view |

**Approach:** Fixture SQL seeds controlled input rows into `raw_artifacts`, `srp_observations`, `detail_observations`. `dbt build` runs. Assertions query the output models directly via psycopg2.

**Files:**
```
tests/integration/dbt/conftest.py          # seed helpers, dbt build fixture
tests/integration/dbt/test_cooldown.py
tests/integration/dbt/test_vin_mapping.py
tests/integration/dbt/test_price_percentiles.py
tests/integration/dbt/test_deal_scores.py
tests/integration/dbt/test_scrape_queue.py
```

---

### Layer 3 — Service API Integration Tests

**Goal:** FastAPI endpoints tested against a real Postgres instance — no mocked DB. Verifies that the full request → DB → response cycle works correctly.

**Scope (ops service only — highest value):**

- `POST /admin/searches/` — creates config, assert row in `search_configs`
- `GET /admin/searches/` — returns list, verify shape
- `POST /admin/searches/{key}` — updates config, assert DB updated
- `POST /deploy/intent/set/pending` → `GET /deploy/status` — assert intent persisted
- `GET /auth/check` with known email hash — assert `X-User-Role` header returned
- `POST /request-access` — assert row in `access_requests`
- `POST /admin/access-requests/{id}/approve` — assert `authorized_users` row created

**Approach:** FastAPI `TestClient` with a real DB connection (not mocked). Test DB is reset between test modules via transaction rollback or truncate fixture.

**Files:**
```
tests/integration/ops/conftest.py          # real DB client fixture
tests/integration/ops/test_search_crud.py
tests/integration/ops/test_deploy_intent.py
tests/integration/ops/test_auth.py
tests/integration/ops/test_access_requests.py
```

---

## CI Integration

New `integration-tests` job in `ci.yml`, runs after `unit-tests`, reuses the same Postgres service pattern as the `dbt` job.

```yaml
integration-tests:
  name: Integration tests
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
        --health-cmd pg_isready --health-interval 5s --health-timeout 5s --health-retries 10
  steps:
    - uses: actions/checkout@v4
    - name: Apply migrations (Flyway)
      # same as dbt job
    - name: Install dependencies
      run: pip install pytest psycopg2-binary dbt-postgres fastapi httpx ...
    - name: Run integration tests
      run: pytest tests/integration/ -v -m integration
      env:
        TEST_DB_URL: postgresql://cartracker:cartracker@localhost:5432/cartracker
```

**Test marker:** `@pytest.mark.integration` on all integration tests. Existing `pytest tests/` unit test run is unchanged — integration tests only run in the new job.

---

## Shared Infrastructure

**`tests/integration/conftest.py`** — shared fixtures:
- `db_conn` — real psycopg2 connection to `TEST_DB_URL`
- `db_cursor` — cursor with per-test rollback (no state bleed between tests)
- `seed_srp_observation(cursor, **kwargs)` — inserts a minimal valid SRP row
- `seed_detail_observation(cursor, **kwargs)` — inserts a minimal valid detail row
- `seed_search_config(cursor, **kwargs)` — inserts a search config
- `run_dbt_build(target="ci")` — shells out to `dbt build`, returns exit code

**Strategy for DB isolation:** Each test module uses `BEGIN` at setup and `ROLLBACK` at teardown. dbt model tests are the exception — they need committed data for dbt to read across connections, so they use a dedicated test schema that is truncated between modules.

---

## Rollout Order

1. **Layer 1 first** — SQL smoke tests are lowest effort, highest immediate value. Catches the next schema-breaking migration before it merges.
2. **Layer 2 (cooldown + queue tests)** — these are the most operationally critical models. If `stg_blocked_cooldown` or `ops_detail_scrape_queue` is wrong, scraping throughput degrades silently.
3. **Layer 2 (price/deal score tests)** — business logic validation. Lower operational urgency but high portfolio value.
4. **Layer 3** — ops API integration tests. Builds on the DB fixture infrastructure from Layers 1–2.

---

## What This Does Not Cover

- **Scraper browser behavior** — Patchright requires a real browser + Docker. Not practical in CI without significant infra. Covered by manual smoke testing.
- **n8n workflow correctness** — addressed by Plan 71 (Airflow migration makes this testable).
- **Dashboard rendering** — Streamlit has no test client. Visual correctness is manual.
- **End-to-end scrape cycle** — requires live cars.com access. Out of scope for automated CI.
