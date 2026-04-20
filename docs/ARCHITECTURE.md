# Cartracker — Architecture Reference

This document describes the system's structural patterns. It is the authoritative reference for how the system is designed — not a plan, not a status tracker. Plans describe what to build; this document describes how it works.

---

## Schema Layout

Three Postgres schemas, each with a distinct purpose:

| Schema | Purpose | Who reads/writes |
|--------|---------|-----------------|
| `public` | Configuration and user data — slow-changing, owned by ops service | ops service, dbt |
| `ops` | Hot operational state — one row per entity, current state only | all services |
| `staging` | Event buffers — append-only until flushed to MinIO Parquet, then TRUNCATED | processing service, ops service |

The `cartracker` role has `search_path = ops, staging, public` (set in V017), so unqualified table names resolve in that order.

---

## Hot + Staging Pattern

Every table that tracks operational state with transitions follows this pattern without exception:

```
ops.<table>          — HOT table: one row per entity, current state only
staging.<table>_events — EVENT table: one row per transition, flushed to MinIO then TRUNCATED
```

Services write both in the same transaction: UPDATE the hot table, INSERT into the events table. The hot table answers "what is the current state?" in O(1). The events table is the durable, replayable record of every transition — it is flushed to MinIO Parquet by an export DAG and then TRUNCATED, keeping Postgres lean.

### Active instances of this pattern

| Hot table | Events table | Owner |
|---|---|---|
| `ops.artifacts_queue` | `staging.artifacts_queue_events` | scraper (pending), processing service (all other transitions) |
| `ops.detail_scrape_claims` | `staging.detail_scrape_claim_events` | ops service |
| `ops.blocked_cooldown` | `staging.blocked_cooldown_events` | processing service |

### Why not a single Postgres append-only log?

Append-only observation logs belong in MinIO (Parquet), not Postgres. Postgres is fast at point lookups and small hot tables; it is expensive for full-history scans over millions of rows. The staging table is a temporary buffer, not a log — it is always short and always getting TRUNCATED. The permanent record lives in MinIO.

---

## MinIO Data Tiers

Two tiers under the `cartracker` bucket:

```
bronze/html/
    year=.../month=.../artifact_type=.../
        <uuid>.html.gz          ← raw compressed HTML, written by scraper

silver/observations/
    year=.../month=.../
        part-*.parquet          ← parsed observations, written by processing service

ops/
    artifacts_queue_events/...  ← hot+staging flush targets (Parquet)
    detail_scrape_claim_events/...
    blocked_cooldown_events/...
```

**Bronze** is the permanent raw record. Every page the scraper fetches is stored here. If a parser bug is discovered, bronze is the source for reprocessing — read the raw HTML, run the fixed parser, write corrected silver rows.

**Silver** is the permanent parsed record. Every observation the processing service produces is written here as the primary write. Postgres HOT tables are updated as a secondary step and hold only current state. Silver is authoritative for analytics and historical queries.

**Silver is not a backup.** It is the primary observation store. If the Postgres HOT tables were lost, they could be reconstructed from silver. The reverse is not true.

---

## Service Responsibilities

| Service | Owns | Does not own |
|---|---|---|
| `scraper` | Fetch loop, browser stack (Patchright/FlareSolverr/curl_cffi), MinIO bronze writes, `artifacts_queue` INSERT | Parsing, observation writes, rotation logic, claim management |
| `ops` | Rotation scheduling (`advance_rotation`), claim lifecycle (`claim-batch`, `release`), user/auth/config management | Fetching, parsing, writing observations |
| `processing` | Artifact claiming, HTML parsing, silver writes, HOT table upserts, `artifacts_queue` status transitions, `blocked_cooldown` writes | Fetching, scheduling |
| `archiver` | Artifact cleanup, Parquet cleanup, archive-to-MinIO pipeline | Everything else |
| `dbt_runner` | Running dbt builds on demand via HTTP | Everything else |

### Fat services, thin DAGs

Logic lives in service endpoints. DAG tasks call those endpoints via HTTP. A DAG task looks like:

```python
def claim_and_scrape(run_id: str):
    listings = requests.post(f"{OPS_URL}/scrape/claims/claim-batch", json={"run_id": run_id}).json()
    requests.post(f"{SCRAPER_URL}/scrape_detail/batch", json={"run_id": run_id, "listings": listings})
```

The DAG orchestrates; the services act. This is what enables future Kafka consumers to call the same endpoints that DAG tasks call today — the trigger changes, the service does not.

---

## Kafka Readiness Boundary

The system is designed to be Kafka-ready without being Kafka-dependent. The boundary sits after the scraper output:

```
Scraper emits artifact → [today: Airflow polls]  → processing service claims it
                         [future: Kafka consumer] → processing service claims it
```

The processing service's `POST /process/artifact/{artifact_id}` endpoint is designed for this: a Kafka consumer receives an event containing an `artifact_id` and calls that endpoint. The service fetches the artifact itself from MinIO. The message is a pointer, not a payload.

Staleness detection (time-threshold driven: "this VIN hasn't been seen in N hours") remains schedule/cron-driven permanently — it is inherently a polling concern and does not benefit from event-driven triggering.

Emit stubs in `processing/events.py` are log-only today. Plan 87 replaces them with real Kafka producer calls without changing any service logic.

---

## Deployment Drain Pattern

Every long-running service exposes two endpoints:

```
GET /health   → 200 {"status": "ok"}        — liveness: is the service running?
GET /ready    → 200 {"ready": true/false}   — drain: is the service idle and safe to stop?
```

`/ready` always returns HTTP 200. The `ready` boolean carries the signal — polling logic never needs to handle non-200 as a distinct case from "busy."

### Current implementation status

| Service | `/health` | `/ready` | Idle condition |
|---|---|---|---|
| `archiver` | ✓ | ✓ | No archive jobs in flight (`shared.job_counter.is_idle()`) |
| `dbt_runner` | ✓ | ✓ | No dbt build in progress (`shared.job_counter.is_idle()`) |
| `scraper` | ✓ | pending | No `runs` rows with `status='running'` |
| `processing` | ✓ (stub) | pending | No artifacts mid-parse (`shared.job_counter.is_idle()`) |
| `ops` | ✓ | N/A | Stateless — always ready |

### `shared.job_counter`

Thread-safe in-memory counter shared by `archiver` and `dbt_runner` today; `processing` will use it when built. Exposes two primitives:

```python
active_job()   # context manager — increments on enter, decrements on exit (even on exception)
is_idle()      # returns True when counter == 0
```

### Deployment flow

```
1. Set deploy_intent → Airflow sensors block new DAG runs from starting
2. Poll GET /ready on each draining service until all return {"ready": true}
3. docker compose up -d --no-deps <service>
4. Set deploy_intent back to 'none' → sensors unblock, work resumes
```

---

## Testing Strategy

Three layers, each with a distinct purpose. All three run in CI against a real Postgres instance with Flyway migrations applied.

### Layer 1 — SQL Smoke Tests

**Goal:** Catch schema breakage before it hits production.

Every mission-critical query runs against the real schema and returns without error with expected columns. No business logic assertions — just "this query executes and returns what the app expects."

**Pattern:** Per-test rollback fixtures. Each test opens a transaction, seeds minimal fixture rows, runs the query, asserts columns, rolls back. Nothing is ever committed.

```python
@pytest.fixture()
def db_conn(db_conn_factory):
    conn = db_conn_factory()
    conn.autocommit = False
    yield conn
    conn.rollback()   # teardown — no committed state left behind
    conn.close()
```

**Files:** `tests/integration/sql/`

---

### Layer 2 — dbt Model Logic Tests

**Goal:** Assert that transformation logic is correct. Known inputs produce known outputs.

dbt runs in a subprocess and cannot see open transactions. A different isolation strategy is required:

1. Seed source data via **autocommit** connection — committed immediately, visible to dbt subprocess
2. Run `dbt build --select <selector> --target ci` — writes to `analytics_ci` schema
3. Assert against `analytics_ci.<model>` rows
4. Module-level teardown TRUNCATEs source tables

All seed data for the entire test session is committed once in the `seed_and_build` session-scoped autouse fixture. Individual test modules contain only assertions — no per-test seeding or dbt invocations.

**ID scheme** prevents primary key conflicts across test groups (100s = VIN mapping, 200s = price percentiles, 300s = ops/staleness, 400s = deal scores, 500s = vehicle attributes, 600s = price history, 700s = days on market, 800s = price events dedup, 900s = vehicle snapshot).

**Key fixture:** `analytics_ci_cur` — a cursor pre-set to `analytics_ci` schema for reading dbt output.

**Files:** `tests/integration/dbt/`

---

### Layer 3 — Service API Integration Tests

**Goal:** Assert that service endpoints behave correctly against a real database.

FastAPI `TestClient` against a real Postgres instance. No mocked DB connections. The app processes the full request path — router → business logic → SQL → DB.

```python
@pytest.fixture(scope="session")
def api_client():
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client
```

A separate `verify_cur` fixture uses an **autocommit** connection to read committed DB state after a TestClient request, without being inside the same transaction.

Auth is exercised for real: `AUTH_EMAIL_SALT` is set in the test environment, and `auth_email_hash` computes the expected hash so tests can seed `authorized_users` rows that the auth middleware will find.

**Files:** `tests/integration/ops/`, `tests/integration/archiver/`, `tests/integration/airflow/`, `tests/integration/dbt_runner/`

---

### Unit Tests

**Goal:** Fast feedback on logic that doesn't require a real DB.

Mock DB connections (psycopg2 mock pattern via `mock_db_conn` fixture). Verify that code calls the correct queries with the correct parameters. Parser tests use real HTML fixtures — no mocking.

**Files:** `tests/ops/`, `tests/scraper/`, `tests/archiver/`, `tests/dbt_runner/`, `tests/shared/`

---

### CI Ordering

Layer 1 SQL smoke tests run before Layer 2 dbt tests. A broken schema detected in Layer 1 fails fast without wasting time on a full dbt build.

```
unit tests → Layer 1 SQL smoke → Layer 2 dbt logic → Layer 3 API integration
```
