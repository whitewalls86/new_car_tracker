# Plan 84: Integration Testing

**Status:** In Progress — Layer 1 complete (71 tests), Layer 2 Phase 1–2 complete, Layer 2 Phase 3 planned, Layer 3 planned
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

#### Test 6 — `mart_deal_scores` ✅

Implemented in `test_deal_scores.py`. Seed uses a unified session fixture shared with all other Layer 2 tests (see Infrastructure notes below).

- **Seed:** 1 target VIN in a 5-listing honda/crv/Hybrid national cohort; msrp=50000, price=35000 (30% discount); first SRP obs 45 days ago; price history 40k→38k→35k (2 drops); detail obs active at 1h ago.
- **Assertions:**
  - `test_deal_score_value` — score within ±3 of manually calculated 79.2
  - `test_deal_tier_excellent` — `deal_tier = 'excellent'` (score ≥ 70)
  - `test_deal_score_in_bounds` — all rows in table have `deal_score` between 0 and 100
  - `test_msrp_discount_component` — `msrp_discount_pct` ≈ 30%
  - `test_price_drop_count` — exactly 2 drops
  - `test_days_on_market` — 44–46 days

**Score breakdown:** MSRP (35) + percentile (30) + DOM (7.5) + drops (6.67) + supply (0.05) ≈ 79.2

---

#### Infrastructure change: unified session seed

All Layer 2 dbt tests share a single session-scoped `seed_and_build` fixture in `conftest.py`. One `dbt build` runs at session start; all test modules only contain assertions.

Seed ID scheme prevents primary key conflicts across groups:
- `1xx` — VIN mapping scenarios
- `2xx` — price percentile cohort (Test-Make/Test-Model)
- `3xx` — ops/staleness scenarios (honda/crv)
- `4xx` — deal scores target + cohort (honda/crv/Hybrid)

dbt selector: `+mart_deal_scores +ops_detail_scrape_queue` — covers the full DAG for all implemented tests.

---

#### Files

```
tests/integration/dbt/conftest.py               # session seed + dbt build, analytics_ci_cur
tests/integration/dbt/test_cooldown.py          # stg_blocked_cooldown (Layer 1 fixtures, no dbt build)
tests/integration/dbt/test_vin_mapping.py       # int_listing_to_vin
tests/integration/dbt/test_price_percentiles.py # int_price_percentiles_by_vin
tests/integration/dbt/test_ops.py               # ops_vehicle_staleness + ops_detail_scrape_queue
tests/integration/dbt/test_deal_scores.py       # mart_deal_scores
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
2. **Layer 2a** ✅ — `test_cooldown.py` + `test_vin_mapping.py`. Built Layer 2 conftest infrastructure.
3. **Layer 2b** ✅ — `test_price_percentiles.py` + `test_ops.py` (staleness + queue).
4. **Layer 2c** ✅ — `test_deal_scores.py`. Unified session seed introduced; all modules share one dbt build.
5. **Layer 2d** — Intermediate model coverage (see Phase 3 below).
6. **Layer 3** — ops API integration tests. Builds on DB fixture infrastructure from Layers 1–2.

---

---

## Layer 2 Phase 3 — Intermediate Model Coverage

The tests above exercise the operational outputs (`ops_*`) and the final mart (`mart_deal_scores`). The intermediate models that feed them contain their own logic that is currently invisible to the test suite. Errors in these models corrupt deal scores and ops queues in ways the existing tests would not catch.

All Phase 3 tests extend the existing session seed and `conftest.py` — no new infrastructure required.

---

### High-priority gaps

#### Test 7 — `int_vehicle_attributes` — source priority

**Why it matters:** `int_vehicle_attributes` resolves the authoritative make/model/trim/msrp/dealer per VIN using `detail > SRP` priority. If SRP data overwrites a detail scrape, dealer enrichment breaks and downstream deal scores use wrong attributes.

**Scenarios:**
| Scenario | Seed | Expected |
|---|---|---|
| Detail wins | SRP + detail for same VIN, detail is fresher | `attributes_source = 'detail'` |
| SRP-only | SRP observation, no detail | `attributes_source = 'srp'` |
| Detail older than SRP | Both present, SRP is fresher | `attributes_source = 'detail'` (detail always wins regardless of recency) |

**Assertions:** query `analytics_ci.int_vehicle_attributes` by VIN; assert `attributes_source`, `make`, `model`, `msrp` match the detail observation values in the detail-wins scenarios.

**File:** `tests/integration/dbt/test_vehicle_attributes.py`

---

#### Test 8 — `int_price_history_by_vin` — price trajectory

**Why it matters:** `price_drop_count` feeds 10 pts of the deal score. The `LAG()` ordering uses `(observed_at, artifact_id)` — if ordering is wrong, drops are miscounted. The deal score test seeds a known sequence but doesn't isolate this model.

**Scenario:** Seed 1 VIN with 5 price events in known order: 100 → 120 → 90 → 90 → 110.

**Expected:**
- `price_drop_count = 1` (120→90 is the only drop; 90→90 is flat; 90→110 is an increase)
- `price_increase_count = 2` (100→120, 90→110)
- `first_price = 100`
- `min_price = 90`, `max_price = 120`

**File:** `tests/integration/dbt/test_price_history.py`

---

#### Test 9 — `int_listing_days_on_market` — first-seen correctness

**Why it matters:** `days_on_market` is 15 pts of the deal score. `first_seen_at` is computed as the minimum `fetched_at` across SRP, detail, and carousel observations. If carousel or a later SRP observation backdates it incorrectly, DOM is inflated.

**Scenarios:**
| Scenario | Seed | Expected |
|---|---|---|
| SRP only | 3 SRP obs at t-10d, t-5d, t-1d | `first_seen_at ≈ t-10d`, `days_on_market ≈ 10` |
| Detail extends | Add detail at t-3d | `first_seen_at` unchanged (SRP was earlier) |
| National vs local | Add local SRP at t-2d | `first_seen_local_at ≈ t-2d`, `first_seen_national_at ≈ t-10d` |

**Assertions:** `first_seen_at ≤ all individual observation timestamps`; `first_seen_national_at ≤ first_seen_local_at` when both present.

**File:** `tests/integration/dbt/test_days_on_market.py`

---

#### Test 10 — `int_price_events` — union completeness and dedup

**Why it matters:** `int_price_events` is the canonical price feed unioning SRP, detail, and carousel sources. Dedup uses `DISTINCT ON (vin, observed_at, price)` with detail > SRP > carousel priority. If a price event is dropped or duplicated, `int_price_history_by_vin` and `int_latest_price_by_vin` are silently wrong.

**Scenarios:**
| Scenario | Seed | Expected |
|---|---|---|
| SRP-only price | SRP obs with price | 1 row in output |
| Detail-only price | Detail obs with price | 1 row in output |
| Same VIN, same timestamp, same price from SRP + detail | Both sources | 1 row (deduped), `source = 'detail'` |
| Two distinct prices for same VIN at different times | SRP @t-2d + SRP @t-1d | 2 rows |

**Assertions:** count of output rows matches expected; `source` field reflects priority correctly on deduped rows.

**File:** `tests/integration/dbt/test_price_events.py`

---

#### Test 11 — `mart_vehicle_snapshot` — listing state inference

**Why it matters:** `mart_vehicle_snapshot` infers `listing_state = 'active'` for SRP-only VINs seen within 7 days, and `'unlisted'` for older ones. `ops_vehicle_staleness` filters out `unlisted` VINs, so wrong state = missing scrape targets.

**Scenarios:**
| Scenario | Seed | Expected `listing_state` |
|---|---|---|
| Detail with explicit state | Detail obs, `listing_state='active'` | `'active'` |
| SRP-only, seen 2 days ago | SRP only, `fetched_at = now()-2d` | `'active'` (inferred) |
| SRP-only, seen 10 days ago | SRP only, `fetched_at = now()-10d` | `'unlisted'` (inferred) |

**Assertions:** query `analytics_ci.mart_vehicle_snapshot` by VIN; assert `listing_state` matches expected for each scenario.

**File:** `tests/integration/dbt/test_vehicle_snapshot.py`

---

### Lower-priority gaps (existing test coverage holes)

#### `ops_vehicle_staleness` — stale reason precedence

The existing tests only check single-reason scenarios. The precedence order (`dealer_unenriched` > `full_details` > `price_only`) is untested when multiple conditions are true simultaneously.

**Add to `test_ops.py`:**
- Seed a VIN where both `tier1_age > 168h` AND `price_age > 24h` are true → assert `stale_reason = 'full_details'` (not `price_only`)

#### `int_price_percentiles_by_vin` — edge cases

The existing tests only spot-check 3 ranks in a 5-VIN cohort.

**Add to `test_price_percentiles.py`:**
- Single-VIN cohort → `national_price_percentile = 0.0`
- Two VINs with identical prices → both should have `percentile = 0.0` (PERCENT_RANK behavior for ties)

---

## What This Does Not Cover

- **Scraper browser behavior** — Patchright requires a real browser + Docker. Not practical in CI without significant infra. Covered by manual smoke testing.
- **n8n workflow correctness** — addressed by Plan 71 (Airflow migration makes this testable).
- **Dashboard rendering** — Streamlit has no test client. Visual correctness is manual.
- **End-to-end scrape cycle** — requires live cars.com access. Out of scope for automated CI.
