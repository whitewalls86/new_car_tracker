# Plan 84: Integration Testing

**Status:** Complete (ops) — Layer 1 complete (71 tests), Layer 2 Phase 1–3 complete, Layer 3 ops complete (5 files, 37 tests). dbt_runner and scraper deferred — lower risk, meaningful coverage achieved.
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

**Goal:** FastAPI endpoints tested against a real Postgres instance — no mocked DB. Verifies that the full request → DB → response cycle works correctly, including DB constraints, redirect behaviour, and data written to tables that feed dbt.

**Scope:** ops service only. The routes covered are the ones that write to public-schema tables (`search_configs`, `deploy_intent`, `authorized_users`, `access_requests`). Routes that call out to other Docker services (`/admin/dbt/*`, `/admin/logs`) are out of scope — those external calls cannot be made without the full container stack.

---

#### Why not use the existing unit test mocks

The existing unit tests (e.g. `tests/ops/routers/test_users.py`) mock `db_cursor()` and assert that the right SQL was called with the right parameters. That catches argument errors but not:
- DB constraint violations (unique key on `search_configs`, FK on `access_requests` → nothing)
- JSON/JSONB structure that `stg_search_configs` actually reads downstream
- The redirect chain after a real commit (the mock never commits, so there's nothing to redirect to)
- The `ON CONFLICT DO UPDATE` in `approve_access_request` firing correctly

Layer 3 closes this gap.

---

#### DB connection strategy

`shared/db.py` reads `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `POSTGRES_PASSWORD` at module import time and builds `DB_KWARGS`. The same env vars already used by the `integration-tests` CI job point to the test Postgres instance. When `TestClient(app)` is constructed in Layer 3 tests, `db_cursor()` inside each route will connect to the test DB automatically — no patching needed.

For auth email hashing, `ops.routers.auth._SALT` is read from `AUTH_EMAIL_SALT` at import time. Set `AUTH_EMAIL_SALT=test-salt` in the test environment (CI env block and local `.env`) so tests can compute expected hashes with `hashlib.sha256(("test-salt" + email.lower()).encode()).hexdigest()`.

---

#### Isolation strategy

Each route in Layer 3 commits its own transaction (the app's `db_cursor()` calls `conn.commit()` before closing). Per-test rollback is not possible for rows written through the `TestClient`. Instead:

- All test keys/hashes use a unique prefix: `l3test-{uuid[:8]}-`
- A `autouse` module-level teardown fixture deletes rows matching that prefix from the affected tables after all tests in the module complete
- The `deploy_intent` table has exactly one row (`id=1`) — tests that mutate it restore it to `intent='none'` in teardown

For read assertions, a separate `verify_cur` fixture (regular psycopg2 `RealDictCursor`, `autocommit=True`) connects to the test DB to inspect committed state after each `TestClient` request.

---

#### conftest design

**File:** `tests/integration/ops/conftest.py`

```
tests/integration/ops/conftest.py
```

Key fixtures:
- `api_client` — `TestClient(app, raise_server_exceptions=True)`. Module-scoped where tests share a DB state that is cleaned up once.
- `verify_cur` — psycopg2 `RealDictCursor` on an `autocommit=True` connection, for reading committed state after TestClient requests. Function-scoped.
- `test_key_prefix` — returns a uuid-based prefix string (`l3test-{uuid[:8]}-`) unique per test module invocation.
- `auth_email_hash(email)` — helper that computes `sha256("test-salt" + email.lower())` to produce expected hashes.
- `seed_user_committed(email_hash, role)` — inserts into `authorized_users` with a direct psycopg2 `autocommit=True` connection, yields `(id, email_hash)`, and deletes the row in teardown. Used to seed users visible to the auth check endpoint.

---

#### Test 1 — `test_search_crud.py` — search_configs CRUD

Covers `POST /admin/searches/` (create), `GET /admin/searches/` (list), `POST /admin/searches/{key}` (update), `POST /admin/searches/{key}/toggle`, `POST /admin/searches/{key}/delete`.

**Setup:** Module-level teardown deletes all rows from `search_configs` WHERE `search_key LIKE 'l3test-%'`.

**Test cases:**

| Test | Action | Assertion |
|---|---|---|
| `test_create_search_persists_to_db` | POST with valid form data; `search_key='l3test-honda-crv'` | DB row exists; `params` jsonb has correct `makes`, `models`, `zip`, `scopes`; 303 redirect to `/admin/searches/` |
| `test_create_search_params_shape_for_dbt` | POST a valid config | `params` contains `sort_order`, `sort_rotation`, `radius_miles`, `max_listings`, `max_safety_pages` — all fields read by `stg_search_configs` |
| `test_create_search_duplicate_key_returns_422` | POST same `search_key` twice | Second POST returns 422 with "already exists" error; only 1 DB row |
| `test_create_search_invalid_zip_returns_422` | POST with `zip='not-a-zip'` | 422 response; no DB row inserted |
| `test_list_searches_includes_created_row` | Create a config, then GET `/admin/searches/` | Response 200; `search_key` appears in body |
| `test_update_search_persists_to_db` | Create, then POST update with different `radius_miles=250` | DB row `params->>'radius_miles' = '250'`; `updated_at > created_at` |
| `test_toggle_search_flips_enabled` | Create with `enabled=False`, then POST `/toggle` | DB row `enabled` flips to `True`; second toggle flips back |
| `test_delete_search_renames_key` | Create, then POST `/delete` | DB row `search_key` now starts with `_deleted_`; `enabled=false` |

**DB constraint note:** The `search_configs` PK constraint (`search_key TEXT NOT NULL PRIMARY KEY`) is the key source constraint that feeds dbt's `stg_search_configs` view. `test_create_search_duplicate_key_returns_422` verifies the app handles the DB-level UNIQUE violation gracefully rather than 500ing.

---

#### Test 2 — `test_deploy_intent.py` — deploy_intent state machine

Covers `GET /deploy/status`, `POST /deploy/start`, `POST /deploy/complete`.

**Setup:** Module-level teardown resets `deploy_intent` row to `intent='none', requested_at=NULL, requested_by=NULL` via direct psycopg2 after all tests run.

**Test cases:**

| Test | Action | Assertion |
|---|---|---|
| `test_deploy_status_returns_current_state` | GET `/deploy/status` | 200; response JSON has `intent`, `requested_at`, `requested_by`, `number_running`, `min_started_at` keys |
| `test_deploy_start_sets_intent` | POST `/deploy/start` | 200; verify_cur confirms `deploy_intent WHERE id=1` has `intent='pending'` and `requested_by='Deploy Declared'` |
| `test_deploy_start_idempotent_when_already_pending` | POST `/deploy/start` twice in sequence | Second POST returns 503 (intent already set, `_set_intent` returns False) |
| `test_deploy_complete_releases_intent` | POST `/deploy/start`, then POST `/deploy/complete` | verify_cur confirms `intent='none'`, `requested_at IS NULL` |
| `test_deploy_status_reflects_running_count` | Seed 1 row in `runs` with `status='running'`, GET `/deploy/status` | `number_running >= 1` in response JSON |

**Note:** `test_deploy_start_idempotent_when_already_pending` relies on the stale-lock timeout (`STALE_LOCK_MINUTES = 30`). As long as the pending intent was set within the last 30 minutes (guaranteed in CI), the second `POST /deploy/start` will correctly fail.

---

#### Test 3 — `test_auth.py` — auth check

Covers `GET /auth/check`.

**Setup:** Seed one `authorized_users` row for each role variant via `seed_user_committed`. Teardown deletes those rows.

**Test cases:**

| Test | Seed / Header | Assertion |
|---|---|---|
| `test_auth_check_authorized_user_returns_200` | Seed `admin` user; send `X-Auth-Request-Email: admin@test.local` | 200; `X-User-Role: admin` header |
| `test_auth_check_unknown_email_returns_403` | No seed; send `X-Auth-Request-Email: nobody@test.local` | 403 |
| `test_auth_check_no_email_header_returns_403` | No header | 403 |
| `test_auth_check_require_admin_passes_admin` | Seed `admin` user; `GET /auth/check?require=admin` | 200 |
| `test_auth_check_require_admin_fails_viewer` | Seed `viewer` user; `GET /auth/check?require=admin` | 403 |
| `test_auth_check_require_observer_passes_power_user` | Seed `power_user` user; `GET /auth/check?require=observer` | 200 (power_user tier > observer) |
| `test_auth_check_viewer_role_returned_correctly` | Seed `viewer` user | `X-User-Role: viewer` |

**Implementation note:** The conftest `seed_user_committed` fixture must insert the hash computed with `AUTH_EMAIL_SALT=test-salt` to match what the route computes at request time. The test sends the raw email in the header; the route hashes it and looks it up. This end-to-end flow is what unit tests cannot verify.

**Observer middleware tests** (add to same file):

The `observer_readonly` middleware in `ops/app.py` blocks all mutating methods (`POST`, `PUT`, `PATCH`, `DELETE`) for requests carrying `X-User-Role: observer`, with exemptions for `/auth/check` and `/health`. This middleware fires before any route handler, so it doesn't need a seeded user — the role comes from the header directly.

| Test | Action | Assertion |
|---|---|---|
| `test_observer_cannot_mutate` | `POST /admin/searches/` with `X-User-Role: observer` | 403 with "Observers cannot make changes." body |
| `test_observer_can_read` | `GET /admin/searches/` with `X-User-Role: observer` | 200 (GET is not a mutating method) |
| `test_observer_exempt_auth_check` | `POST` to `/auth/check` (if ever used as POST) or confirm `GET /auth/check` with `X-User-Role: observer` | 200 (path is on exempt list) |
| `test_non_observer_can_mutate` | `POST /admin/searches/` with `X-User-Role: admin` | Not blocked by middleware (proceeds to route handler) |

---

#### Test 4 — `test_access_requests.py` — access request lifecycle

Covers `POST /request-access`, `GET /admin/access-requests`, `POST /admin/access-requests/{id}/approve`, `POST /admin/access-requests/{id}/deny`.

**Setup:** Module-level teardown deletes from `access_requests` and `authorized_users` WHERE `email_hash LIKE 'l3test%'`.

**Test cases:**

| Test | Action | Assertion |
|---|---|---|
| `test_submit_access_request_creates_db_row` | POST `/request-access` with `X-Auth-Request-Email: l3test-requester@test.local` | DB row in `access_requests` with `status='pending'`, `requested_role='viewer'`, `display_name` set |
| `test_submit_access_request_duplicate_shows_pending` | Submit request twice for same email | Second POST 200 with "pending" state shown; only 1 row in `access_requests` |
| `test_submit_access_request_already_authorized_redirects` | Seed user in `authorized_users`, POST `/request-access` | 303 redirect to `/dashboard` (viewer) or `/admin` (admin); no new `access_requests` row |
| `test_submit_access_request_invalid_role_returns_400` | POST with `requested_role='superadmin'` | 400; no DB row |
| `test_approve_access_request_creates_authorized_user` | Submit request, then POST `/admin/access-requests/{id}/approve` | `authorized_users` row created with correct `email_hash` and `requested_role`; `access_requests.status='approved'`; `resolved_at` set |
| `test_approve_access_request_conflict_upserts` | Seed existing `authorized_users` row for same email, approve request | `ON CONFLICT DO UPDATE` fires; `authorized_users` row `role` updated to requested role; no duplicate |
| `test_deny_access_request_updates_status` | Submit request, then POST `/admin/access-requests/{id}/deny` | `access_requests.status='denied'`; no `authorized_users` row created |
| `test_approve_nonexistent_request_redirects` | POST `/admin/access-requests/99999/approve` | 303 redirect to `/admin/access-requests`; no DB side-effects |
| `test_list_access_requests_shows_pending` | Submit request, GET `/admin/access-requests` | 200; requester's `display_name` appears in response body |

**`ON CONFLICT` constraint note:** `approve_access_request` uses `INSERT INTO authorized_users ... ON CONFLICT (email_hash) DO UPDATE`. This is a real DB constraint. `test_approve_access_request_conflict_upserts` is the only test in the suite that verifies this path fires correctly end-to-end — the unit test mocks the cursor and cannot trigger the actual constraint.

---

#### Test 5 — `test_user_management.py` — authorized_users mutations

Covers `POST /admin/users/{id}/role`, `POST /admin/users/{id}/revoke`.

**Setup:** `seed_user_committed` inserts a user for each test that needs one. Teardown deletes `email_hash LIKE 'l3test%'` rows.

**Test cases:**

| Test | Action | Assertion |
|---|---|---|
| `test_change_user_role_updates_db` | Seed `viewer` user, POST `/admin/users/{id}/role` with `role=observer` | DB row `role='observer'` |
| `test_change_user_role_invalid_role_no_change` | Seed user, POST with `role=superadmin` | 303 redirect; DB role unchanged |
| `test_revoke_user_removes_row` | Seed user, POST `/admin/users/{id}/revoke` | DB row deleted |
| `test_revoke_nonexistent_user_no_error` | POST `/admin/users/99999/revoke` | 303 redirect; no error |

---

#### CI notes for Layer 3

Layer 3 runs in the **same `integration-tests` job** as Layers 1 and 2. It shares the same Postgres + Flyway setup. No dbt is needed.

Additional env vars required:
```yaml
AUTH_EMAIL_SALT: test-salt
```

Additional pip installs needed (beyond what Layer 2 added):
```
httpx  # required by FastAPI TestClient
```
(`fastapi` itself is already installed as an app dependency.)

The `pytest tests/integration/` invocation already covers `tests/integration/ops/` — no changes to the pytest command needed.

**Ordering note:** Per the Layer 1/2 ordering memory, SQL smoke tests must run before dbt integration tests. Layer 3 tests are independent of dbt and can run in any order relative to Layer 2. The natural ordering — `tests/integration/sql/` then `tests/integration/dbt/` then `tests/integration/ops/` — is fine.

---

#### Test 6 — `test_dbt_runner_intents.py` — dbt_runner intent CRUD + lock state

The dbt_runner service uses `shared.db` (same `PG*` env vars) and writes to two public-schema tables: `dbt_intents` (intent CRUD) and `dbt_lock` (acquired/released around every build). The `POST /dbt/build` route is out of scope here — it shells out to a real dbt subprocess. The intent and lock endpoints are independently testable.

**File:** `tests/integration/dbt_runner/test_dbt_runner_intents.py`

**conftest:** `tests/integration/dbt_runner/conftest.py` — `api_client` is `TestClient(dbt_runner.app.app)`. `verify_cur` reuses the Layer 1 `db_conn_factory`. Teardown deletes from `dbt_intents` WHERE `intent_name LIKE 'l3test-%'` and restores `dbt_lock` to `locked=false` if any test leaves it set.

**Test cases:**

| Test | Action | Assertion |
|---|---|---|
| `test_get_lock_status_returns_shape` | GET `/dbt/lock` | 200; response JSON has `locked`, `locked_at`, `locked_by` keys; `locked=false` on a fresh DB |
| `test_create_intent_persists_to_db` | POST `/dbt/intents` `{intent_name: "l3test-after-srp", select_args: ["stg_raw_artifacts+"]}` | 200; verify_cur confirms row in `dbt_intents` with correct `intent_name` and `select_args` array |
| `test_get_intents_includes_seeded_row` | Create intent, GET `/dbt/intents` | 200; response JSON `intents` dict contains the created intent name with correct `select` list |
| `test_upsert_intent_updates_select_args` | Create intent with `["stg_raw_artifacts+"]`, then POST again with `["stg_raw_artifacts+", "stg_srp_observations+"]` | verify_cur row has updated `select_args`; `GET /dbt/intents` returns new list |
| `test_delete_intent_removes_row` | Create intent, `DELETE /dbt/intents/l3test-after-srp` | 200 `{"ok": true}`; verify_cur confirms row gone |
| `test_delete_nonexistent_intent_returns_404` | `DELETE /dbt/intents/l3test-no-such-intent` | 404 |
| `test_create_intent_invalid_token_returns_400` | POST with `select_args: ["../../evil"]` | 400; no DB row |

**Lock acquire/release tests** (add to same file):

`POST /dbt/build` is the only API route that touches the lock, and it requires a real dbt subprocess — untestable here. Instead, call the helper functions directly. This is still a genuine integration test (real DB, real `UPDATE ... RETURNING`), just not an API-level test.

| Test | Action | Assertion |
|---|---|---|
| `test_acquire_lock_succeeds_when_free` | Call `_acquire_lock("l3test-caller")` directly | Returns `True`; verify_cur confirms `dbt_lock WHERE id=1` has `locked=true`, `locked_by='l3test-caller'` |
| `test_acquire_lock_fails_when_held` | Acquire lock, then call `_acquire_lock("l3test-other")` | Returns `False`; `locked_by` unchanged |
| `test_release_lock_clears_state` | Acquire lock, then call `_release_lock()` | Returns `True`; verify_cur confirms `locked=false`, `locked_at IS NULL`, `locked_by IS NULL` |
| `test_acquire_lock_steals_stale` | Directly set `locked=true, locked_at=now()-31m` via verify_cur, then call `_acquire_lock("l3test-stealer")` | Returns `True` (stale lock stolen); `locked_by='l3test-stealer'` |

Teardown: `_release_lock()` called unconditionally in module teardown to restore `locked=false`.

**Why this matters:** `dbt_intents` drives which dbt selectors run for each pipeline trigger. If an intent row has a corrupt `select_args` array, the next dbt build uses the wrong selector silently. The `ON CONFLICT DO UPDATE` path ensures stale intents are overwritten rather than duplicated — only an integration test can verify this end-to-end. The lock tests verify the stale-lock steal (`STALE_LOCK_MINUTES=30`) fires correctly — a bug here would cause deadlocked builds after any dbt subprocess crash.

---

#### Test 7 — `test_scraper_advance_rotation.py` — search rotation slot claiming

The scraper's `POST /search_configs/advance_rotation` is the only scraper route that writes to the DB without requiring a browser. It performs an atomic slot-claiming transaction against `search_configs` and reads `runs` for the gap guard. This is non-trivial logic that the unit tests cannot exercise.

**Import note:** `scraper/app.py` does `from db import get_pool` (a module-relative import that expects the `scraper/` directory on the path, with `scraper/db.py` as the `db` module). The test conftest must add `scraper/` to `sys.path` before importing the app, and set `DATABASE_URL` to the test DSN so `asyncpg` connects to the right instance.

**File:** `tests/integration/scraper/test_scraper_advance_rotation.py`

**conftest:** `tests/integration/scraper/conftest.py`
- `sys.path.insert(0, str(Path(__file__).parents[3] / "scraper"))` before app import
- `DATABASE_URL` env var set to `postgresql://cartracker:cartracker@localhost:5432/cartracker`
- `api_client` uses `httpx.AsyncClient(app=scraper_app, base_url="http://test")` (async app requires `AsyncClient`)
- `verify_cur` reuses Layer 1 `db_conn_factory`
- Module-level teardown: reset `last_queued_at = NULL` on all test-created `search_configs` rows; delete test `runs` rows

**Test cases:**

| Test | Action | Assertion |
|---|---|---|
| `test_advance_rotation_claims_due_slot` | Seed a `search_configs` row with `rotation_slot=99`, `last_queued_at=NULL`, `enabled=true`; call `POST /search_configs/advance_rotation` with `min_gap_minutes=0` | Response `slot=99`, `configs` contains the seeded `search_key`; verify_cur confirms `last_queued_at` is now set |
| `test_advance_rotation_nothing_due` | Seed same config with `last_queued_at=now()` (just claimed); call again with `min_idle_minutes=1440` | Response `{"slot": null, "configs": []}` |
| `test_advance_rotation_gap_guard` | Seed a `runs` row with `trigger='search scrape'`, `status='running'`, `started_at=now()-1m`; call with `min_gap_minutes=60` | Response `{"slot": null, "configs": [], "reason": "too_soon"}` |
| `test_advance_rotation_claims_all_configs_in_slot` | Seed 2 `search_configs` rows with same `rotation_slot=99`, both `last_queued_at=NULL`; call with `min_gap_minutes=0` | Response `configs` contains both keys; both rows have `last_queued_at` updated |
| `test_advance_rotation_skips_disabled_config` | Seed one `enabled=true` and one `enabled=false` in same slot; call | Only the enabled config appears in response |

**Why this matters:** The rotation slot claiming uses `FOR UPDATE SKIP LOCKED` and a gap guard to prevent concurrent scrape runs. A bug here — wrong slot returned, `last_queued_at` not committed, disabled configs leaking through — means the scraper fires the wrong searches or fires too frequently. The unit tests mock asyncpg; this is the only place the actual transaction semantics are verified.

**Note on `DATABASE_URL` env var:** The scraper's asyncpg pool reads `DATABASE_URL` (not `PG*` vars). The CI `integration-tests` job needs this env var added alongside the existing `PG*` vars:

```yaml
DATABASE_URL: postgresql://cartracker:cartracker@localhost:5432/cartracker
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
5. **Layer 2d** ✅ — Intermediate model coverage (Phase 3 above).
6. **Layer 3** — ops API integration tests. Builds on DB fixture infrastructure from Layers 1–2. No dbt required. Same CI job.

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

- **Scraper browser behavior** — Patchright requires a real browser + Docker. Not practical in CI without significant infra. Covered by manual smoke testing. The non-browser DB routes (`advance_rotation`, `get_known_vins`) are covered in Layer 3.
- **n8n workflow correctness** — addressed by Plan 71 (Airflow migration makes this testable).
- **Dashboard rendering** — Streamlit has no test client. Visual correctness is manual.
- **End-to-end scrape cycle** — requires live cars.com access. Out of scope for automated CI.
