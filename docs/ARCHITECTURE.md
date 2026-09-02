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
4. Poll docker inspect health until every recreated service is healthy
5. Set deploy_intent back to 'none' → sensors unblock, work resumes
```

`scripts/redeploy.sh` runs steps 3-5. Step 4 replaced a `sleep 10` in Plan 144;
its timeout is derived from the slowest healthcheck in `docker-compose.yml` and
checked in CI. Step 5 is conditional: intent is released after a failed *build*,
because nothing was recreated, and **held** after a failed recreation, because a
half-deployed fleet should not have work resuming against it.

**This flow only applies when the image changed.** `up -d` on a service Compose
sees no drift for leaves the container running and exits 0, so steps 3-5 do
nothing at all; the script detects that and says so rather than reporting a
deploy. Two cases need a restart instead of a recreate, and both use
`scripts/redeploy.sh --restart <service>` (spelled `--config` as well):

- **A bind-mounted config file changed.** For the six services that mount a
  single *file* rather than a directory, `git pull` lands the new content on a
  new inode and a `SIGHUP` reload silently reloads the old one. The restart path
  verifies the container is reading the file that is on disk.
- **A peer's address changed under a long-lived process.** Recreating a
  container gives it a new IP, and anything holding a cached resolved address —
  Airflow's StatsD client, over UDP — keeps addressing the dead one, silently.
  `deploy-followers.txt` names these and the deploy warns.

See [Plan 144](plans/plan_144_deploy_script_hardening.md).

---

## Testing Strategy

The testing contract lives in **[`docs/TESTING.md`](TESTING.md)** and is owned
by [Plan 161](plans/plan_161_testing_contract.md). It defines the five layers,
the one convention per concern, what a service owes before it ships, and the
gap list. It is asserted by a test rather than agreed to.

This section deliberately holds no summary of it. The description that stood
here from Plan 84 until 2026-08-31 was accurate when written and false by
August in four checkable ways, and nothing in the repository could tell the
difference — which is the whole reason the contract exists.

The two architectural facts about testing that belong in *this* document,
because they are properties of the system rather than of the suite:

- **CI's database is not production's database.** The suites run against bare
  `postgres:16` and `minio/minio:latest` service containers rather than the
  Compose definitions, so Airflow's schema does not exist there and the
  coordination queries that cross into it cannot be executed by any layer.
  Closing that is [Plan 139 Stage F](plans/plan_139_test_suite_maintenance.md).
- **Airflow is tested in its own interpreter**, mirroring its own container in
  production: `apache-airflow`'s starlette pin conflicts with the FastAPI
  services', so `tests/integration/airflow/` runs from an isolated venv built
  inside the CI job. Anything under `tests/airflow/` runs in the main venv and
  must therefore not import `airflow`.
