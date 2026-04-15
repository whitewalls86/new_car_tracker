# Plan 84: Integration Testing

**Status:** In Progress — Layer 1 complete (71 tests), Layer 2 planned, Layer 3 planned
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

---

#### Test isolation strategy

dbt reads from committed rows. The per-test rollback pattern used in Layer 1 does **not** work here — dbt runs in a separate process and cannot see an open transaction.

Strategy:
1. Each test module seeds source data via a **committed** transaction.
2. dbt is invoked as a subprocess (`dbt build --select ... --target ci`), which writes to the `analytics_ci` schema.
3. Test assertions query `analytics_ci.<model>` via psycopg2.
4. A module-level teardown truncates the source tables that were written, resetting state for the next module.

The `ci` dbt target (from `profiles.yml`) uses `cartracker` credentials and writes to `analytics_ci`. Integration tests query that schema.

#### conftest design

```
tests/integration/dbt/conftest.py
```

Key fixtures:
- `dbt_conn` — psycopg2 connection with `autocommit=True` (writes are immediately visible to dbt subprocess)
- `dbt_cur` — cursor on `dbt_conn`
- `run_dbt(select)` — helper that shells out `dbt build --select {select} --target ci --profiles-dir .` from the `dbt/` working directory; returns `CompletedProcess`; fails the test if exit code is non-zero
- `analytics_ci_cur` — cursor pre-connected to `analytics_ci` schema for reading dbt output

---

#### Test 1 — `stg_blocked_cooldown` (VIEW, no dbt build required)

`stg_blocked_cooldown` is a plain SQL VIEW in the `analytics` schema — it reads directly from `public.blocked_cooldown`. No dbt build needed; Layer 1 rollback fixtures work fine.

**Source table:** `public.blocked_cooldown(listing_id, first_attempt_at, last_attempted_at, num_of_attempts)`

**Scenarios:**

| num_of_attempts | Expected next_eligible_at | Expected fully_blocked |
|---|---|---|
| 1 | `last_attempted_at + 12h` (12 × 2⁰) | false |
| 2 | `last_attempted_at + 24h` (12 × 2¹) | false |
| 3 | `last_attempted_at + 48h` (12 × 2²) | false |
| 4 | `last_attempted_at + 96h` (12 × 2³) | false |
| 5 | NULL | true |
| 6 | NULL | true |

**Assertions:** for each row, assert `fully_blocked` matches expected value; for rows 1–4, assert `next_eligible_at` is within 1 second of `last_attempted_at + interval`.

**File:** `tests/integration/dbt/test_cooldown.py` — uses standard Layer 1 `db_conn` rollback fixture, no dbt invocation.

---

#### Test 2 — `int_listing_to_vin` (incremental dbt model)

**Goal:** Verify that the most recent observation wins and that detail observations take precedence over SRP when they are more recent.

**Source tables:** `public.raw_artifacts`, `public.srp_observations`, `public.detail_observations`

**Seed scenarios:**

| Scenario | What to seed | Expected output |
|---|---|---|
| SRP-only | 1 SRP row: `listing_id='L1'`, `vin='SRP_VIN_00001AAAAAAA'` | `vin = 'SRP_VIN_00001AAAAAAA'` |
| Detail wins (more recent) | SRP: `listing_id='L2'`, `vin='SRPVIN_2AAAAAAAAAAA'`, `fetched_at = now()-2h`; Detail: same listing_id, `vin='DETVIN_2BBBBBBBBBBB'`, `fetched_at = now()-1h` | `vin = 'DETVIN_2BBBBBBBBBBB'` |
| SRP wins (more recent) | SRP: `listing_id='L3'`, `vin='SRPVIN_3AAAAAAAAAAA'`, `fetched_at = now()-1h`; Detail: same listing_id, `vin='DETVIN_3BBBBBBBBBBB'`, `fetched_at = now()-2h` | `vin = 'SRPVIN_3AAAAAAAAAAA'` |

Seed data requirements:
- Each `srp_observations` row requires an `artifact_id` FK to `raw_artifacts` (with `search_scope='national'`, `url='http://test'`, `filepath='test'`)
- Each `detail_observations` row requires its own `artifact_id`

**dbt selector:** `dbt build --select stg_srp_observations stg_detail_observations stg_raw_artifacts int_listing_to_vin`

**Assertions:** query `analytics_ci.int_listing_to_vin` WHERE `listing_id IN ('L1','L2','L3')`, assert each row's `vin` matches expected.

**File:** `tests/integration/dbt/test_vin_mapping.py`

---

#### Test 3 — `int_price_percentiles_by_vin` (table model)

**Goal:** Verify `PERCENT_RANK()` over a known cohort produces correct ordinal ranks.

The model filters to `search_scope = 'national'` and `fetched_at >= now() - staleness_window_days days`. The dbt var `staleness_window_days = 3` (from `dbt_project.yml`).

**Seed:** 5 national SRP observations, same `make='TestMake'` / `model='TestModel'` / `trim='TestTrim'`, prices [10000, 20000, 30000, 40000, 50000], all with `fetched_at = now() - interval '1 day'`. Each needs a matching `raw_artifacts` row with `search_scope='national'`. VINs must be valid 17-char strings.

**dbt selector:** `dbt build --select stg_srp_observations stg_raw_artifacts int_price_percentiles_by_vin`

**Assertions:**

| Price | Expected national_price_percentile |
|---|---|
| 10000 | 0.0 (cheapest in cohort) |
| 30000 | 0.5 (middle) |
| 50000 | 1.0 (most expensive) |

Assert within a small tolerance (±0.01) to handle floating-point representation.

**File:** `tests/integration/dbt/test_price_percentiles.py`

---

#### Test 4 — `ops_vehicle_staleness` (dbt model)

**Goal:** Verify staleness flags and `stale_reason` classification are correct.

**Upstream dependencies:** `mart_vehicle_snapshot` → many ancestors. A practical approach: seed `public.srp_observations` minimally (1 VIN), run `dbt build --select +ops_vehicle_staleness` to build the full ancestor chain, assert on the ops view.

**Seed scenarios (separate seeded VINs):**

| Scenario | What to seed | Expected flags |
|---|---|---|
| Price stale | SRP with `fetched_at = now() - 25h`, no detail observation | `is_price_stale=true`, `stale_reason='price_only'` |
| Full-details stale | SRP with `fetched_at = now() - 200h` | `is_full_details_stale=true`, `stale_reason='full_details'` |
| Not stale | SRP with `fetched_at = now() - 1h`, detail with `fetched_at = now() - 1h` | `is_price_stale=false`, `is_full_details_stale=false` |

**dbt selector:** `dbt build --select +ops_vehicle_staleness` (builds the full ancestor chain — the required investment to test this model)

**Assertions:** query `analytics_ci.ops_vehicle_staleness` WHERE `vin IN (test_vins)`, assert per-row flag values.

**File:** `tests/integration/dbt/test_vehicle_staleness.py`

---

#### Test 5 — `ops_detail_scrape_queue` (dbt model)

**Goal:** Verify eligibility filtering: fully-blocked listings are excluded, cooldown-eligible listings are included, and priority pool assignments are correct.

**Builds on top of:** `ops_vehicle_staleness` ancestor chain + `stg_blocked_cooldown` (view) + `int_carousel_price_events_unmapped`.

**Seed scenarios:**

| VIN / listing_id | Staleness state | Cooldown state | Expected in queue? | Expected priority |
|---|---|---|---|---|
| `QUEUE_VIN_1_STALE_` | price stale (>24h) | no cooldown record | yes | pool 1 (dealer pick) |
| `QUEUE_VIN_2_BLOCK_` | price stale | `num_of_attempts=5` (fully blocked) | no | — |
| `QUEUE_VIN_3_COOL__` | price stale | `num_of_attempts=2`, `next_eligible_at = now() + 5h` (not yet eligible) | no | — |
| `QUEUE_VIN_4_ELIG__` | price stale | `num_of_attempts=2`, `next_eligible_at = now() - 1h` (eligible) | yes | pool 1 or 4 |

**dbt selector:** `dbt build --select +ops_detail_scrape_queue`

**Assertions:**
- VINs 1 and 4 appear in `analytics_ci.ops_detail_scrape_queue`
- VINs 2 and 3 do not appear
- VIN 1's `priority = 1` (pool 1 dealer pick)

**File:** `tests/integration/dbt/test_scrape_queue.py`

---

#### Test 6 — `mart_deal_scores` (table model — deferred to Phase 2)

`mart_deal_scores` has the widest DAG of any model — it depends on `mart_vehicle_snapshot`, `int_vehicle_attributes`, `int_listing_days_on_market`, `int_price_history_by_vin`, `int_model_price_benchmarks`, `int_dealer_inventory`, `int_price_percentiles_by_vin`, and `stg_dealers`. A meaningful test requires seeding all of these source chains.

**Defer until after tests 1–5 are stable.** The deal score formula is fully deterministic once upstream models are correct, so tests on `int_price_percentiles_by_vin` and `ops_vehicle_staleness` provide intermediate coverage.

When implemented:
- Seed: 1 VIN in a 5-listing national cohort at the 10th percentile; 30% MSRP discount; 45 days on market; 2 price drops
- Assert: `deal_score` within ±2 of the manually calculated value; `deal_tier = 'excellent'`

---

#### Files

```
tests/integration/dbt/conftest.py          # autocommit conn, run_dbt(), analytics_ci_cur
tests/integration/dbt/test_cooldown.py     # stg_blocked_cooldown scenarios (no dbt build)
tests/integration/dbt/test_vin_mapping.py  # int_listing_to_vin
tests/integration/dbt/test_price_percentiles.py  # int_price_percentiles_by_vin
tests/integration/dbt/test_vehicle_staleness.py  # ops_vehicle_staleness
tests/integration/dbt/test_scrape_queue.py # ops_detail_scrape_queue
```

#### CI changes needed for Layer 2

The `integration-tests` CI job currently installs only `pytest psycopg2-binary`. Layer 2 adds:
- `dbt-postgres` (dbt build subprocess)
- `dbt deps` step (installs `dbt_utils` package)

The `integration-tests` job will run `pytest tests/integration/` which covers both Layer 1 (SQL smoke) and Layer 2 (dbt logic) tests together. A single Postgres + Flyway setup is shared.

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
      run: pip install pytest psycopg2-binary
      # Layer 2 addition: + dbt-postgres, then: cd dbt && dbt deps
    - name: Run integration tests
      run: pytest tests/integration/ -v -m integration
      env:
        TEST_DATABASE_URL: postgresql://cartracker:cartracker@localhost:5432/cartracker
        DBT_HOST: localhost
        DBT_PORT: "5432"
        DBT_USER: cartracker
        DBT_PASSWORD: cartracker
        DBT_DBNAME: cartracker
```

**Test marker:** `@pytest.mark.integration` on all integration tests. Existing `pytest tests/` unit test run is unchanged — integration tests only run in the new job.

**Layer 1 (current):** Only `pytest psycopg2-binary` needed. Already implemented in CI.

**Layer 2 additions needed:** `dbt-postgres` install + `dbt deps` step. The `DBT_*` env vars above are already set in the `dbt` job and can be reused.

---

## Shared Infrastructure

**`tests/integration/conftest.py`** — Layer 1 fixtures (already implemented):
- `db_conn` — real psycopg2 connection to `TEST_DATABASE_URL`, per-test rollback
- `cur` — `RealDictCursor` on `db_conn`
- `seed_search_config`, `seed_run`, `seed_scrape_job`, `seed_authorized_user`, `seed_access_request` — minimal seed rows

**`tests/integration/dbt/conftest.py`** — Layer 2 fixtures:
- `dbt_conn` — psycopg2 connection with `autocommit=True` (writes immediately visible to dbt subprocess)
- `dbt_cur` — cursor on `dbt_conn`
- `run_dbt(select)` — shells out `dbt build --select {select} --target ci` from the `dbt/` directory; fails test on non-zero exit code
- `analytics_ci_cur` — cursor pre-set to `analytics_ci` schema for reading dbt output
- Module-level teardown truncates the source tables written during the test module

**DB isolation:** Layer 1 uses per-test `ROLLBACK`. Layer 2 uses committed writes + source table truncation between modules. dbt writes to `analytics_ci` (isolated from `analytics` production schema).

---

## Rollout Order

1. **Layer 1** ✅ — 71 SQL smoke tests implemented. Catches the next schema-breaking migration before it merges.
2. **Layer 2a** — `test_cooldown.py` (no dbt build, uses Layer 1 fixtures), then `test_vin_mapping.py` (simplest DAG). Builds Layer 2 conftest infrastructure.
3. **Layer 2b** — `test_price_percentiles.py` + `test_vehicle_staleness.py`. Operationally critical; if `ops_vehicle_staleness` is wrong, the scrape queue misfires.
4. **Layer 2c** — `test_scrape_queue.py`. Depends on 2b being stable.
5. **Layer 2d** — `test_deal_scores.py` (wide DAG, defer until 2a–2c are done).
6. **Layer 3** — ops API integration tests. Builds on DB fixture infrastructure from Layers 1–2.

---

## What This Does Not Cover

- **Scraper browser behavior** — Patchright requires a real browser + Docker. Not practical in CI without significant infra. Covered by manual smoke testing.
- **n8n workflow correctness** — addressed by Plan 71 (Airflow migration makes this testable).
- **Dashboard rendering** — Streamlit has no test client. Visual correctness is manual.
- **End-to-end scrape cycle** — requires live cars.com access. Out of scope for automated CI.
