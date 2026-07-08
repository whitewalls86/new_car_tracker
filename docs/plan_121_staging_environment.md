# Plan 121: Staging Environment

## Goal

Create a `dev.cartracker.info` staging environment for testing application,
auth, routing, dashboard, dbt/Spark, and deployment changes without touching
production traffic or production data.

This plan is intentionally separate from Plan 120:

- Plan 120 provides reproducible fixture data for CI and local development.
- Plan 121 provides a deployed application environment that can consume fixture
  data and exercise the full web/service stack.

---

## Recommendation

Do not block the initial Delta/PySpark work on staging.

Current recommended order:

```text
120 -> 112 -> 113 -> 118 -> [114 / 121 / 119]
```

Staging becomes a bigger win once one of these is true:

- Spark/Delta/dbt changes need realistic service wiring to validate.
- auth/admin/dashboard changes become risky to test only in production.
- Plan 113 adaptive-refresh rollout needs shadow-mode UX and counters tested
  before production.
- the project starts carrying multiple long-running infrastructure changes at
  once.

Until then, Plan 120 plus local dev gives most of the data/ML testing value at
lower operational cost.

---

## Related: Shared Dependency Pinning (CI/project sanity)

Per-service `requirements.txt` files currently duplicate shared third-party
packages (e.g. `boto3`) with no version coordination — a package can be
present in one service's image and silently missing or drifted in another
(see: archiver's `shared.minio` needing `boto3`, which was only added to
`archiver/requirements.txt` after a runtime `ModuleNotFoundError` in Plan 120
Gate C.75 planning-cache writes).

Candidate fix: a root `constraints.txt` pinning shared packages
(`boto3`, `pyarrow`, `duckdb`, etc.), with each service's Dockerfile/CI install
step passing `pip install -c constraints.txt -r requirements.txt`. Keeps
per-service requirement lists independent while removing version drift for
packages multiple services share. Lower cost than a shared base Docker image;
revisit a shared base image only if constraint-file drift keeps recurring.

Scope for this plan:

- add `constraints.txt` at repo root
- update each service's Dockerfile/CI pip install to use `-c constraints.txt`
- audit current `requirements.txt` files for already-drifted shared packages

---

## Context

CarTracker currently has:

- one production Oracle Cloud VM
- Caddy routing
- production Postgres
- production MinIO
- production Airflow
- production ops/admin UI
- production scraper/processing/archiver/dbt_runner/dashboard services

The system is now complex enough that some changes are awkward to test with
unit tests, CI fixtures, or local development alone:

- auth and role behavior
- Caddy/subdomain routing
- dashboard rendering against realistic data
- dbt_runner and longer analytics jobs
- future Spark/Delta/MLflow services
- deploy intent and service-drain behavior
- production-like environment variables and secrets

---

## Target Shape

Initial target:

```text
https://dev.cartracker.info
        |
        v
staging Caddy route
        |
        v
staging ops/dashboard/dbt_runner/MLflow/etc.
        |
        +--> staging Postgres
        +--> staging MinIO bucket/prefix
        +--> seeded Plan 120 snapshot
```

Default staging posture:

- no live Cars.com scraping
- scraper disabled or dummy-only by default
- seeded from Plan 120 fixture snapshots
- separate database
- separate MinIO bucket or strict prefix isolation
- separate auth/admin seed
- separate deploy intent
- clear visual/environment indicator in the UI

---

## Phase 0: Staging Decision Record

Create:

```text
docs/staging_environment_decision.md
```

Decide:

- same VM vs separate VM
- same Docker Compose project vs separate compose file
- separate Postgres container vs separate database on same Postgres
- separate MinIO bucket vs separate prefix
- which services run in staging initially
- whether Airflow runs in staging initially
- which external routes exist
- how secrets are managed

Default first choice:

```text
same VM, separate compose project, separate Postgres DB/container, separate
MinIO bucket, scraper disabled by default
```

This keeps cost low while avoiding accidental production writes.

---

## Phase 1: Environment Isolation

Create an isolated staging runtime.

Minimum isolation:

- `COMPOSE_PROJECT_NAME=cartracker_staging`
- staging `.env`
- staging Postgres database/container
- staging MinIO bucket, e.g. `bronze-staging`
- staging Caddy route for `dev.cartracker.info`
- staging auth seed
- staging service names/ports

Hard rule:

Staging must never share production write credentials for Postgres or MinIO.

---

## Phase 2: Data Seeding

Seed staging from Plan 120 snapshots.

Staging seed flow:

```text
download or mount snapshot archive
verify manifest/checksum
seed staging MinIO
seed staging Postgres support rows
run dbt/Spark build if needed
start dashboard/ops UI against seeded data
```

The initial staging environment should be realistic enough for web/API testing
without requiring a full production data copy.

---

## Phase 3: Service Set

Initial services:

- ops
- dashboard
- dbt_runner
- Postgres
- MinIO
- Caddy route
- optional MLflow once Plan 112 starts using it

Deferred services:

- scraper live fetches
- processing live artifact processing
- full Airflow scheduler
- Grafana/Prometheus clone

If scraper exists in staging, default it to dummy endpoints or disabled claim
execution. Live external scraping should require an explicit operator action.

---

## Phase 4: Auth and Safety

Staging should exercise auth, but not reuse production user data blindly.

Required:

- separate auth salt
- separate admin seed
- clear staging banner in ops/dashboard
- staging-only secrets
- robots/noindex headers if public
- no production deploy actions from staging
- no production scrape controls from staging

Nice to have:

- basic auth or IP allowlist in front of all staging routes
- seeded test users for admin/viewer roles

---

## Phase 5: Deployment Flow

Define how code reaches staging.

Options:

| Option | Use when |
|--------|----------|
| manual deploy script | first implementation |
| GitHub Actions deploy to staging | after secrets and rollback are stable |
| branch-based staging deploy | if staging becomes part of PR review |

Initial flow:

```text
git pull
docker compose -f docker-compose.yml -f docker-compose.staging.yml build
docker compose -f docker-compose.yml -f docker-compose.staging.yml up -d
seed snapshot
run smoke checks
```

---

## Phase 6: Smoke Checks

Minimum staging smoke checks:

- `/health` on ops/dashboard/dbt_runner
- auth login/admin access
- dashboard loads against seeded data
- dbt build or selected dbt job can run
- snapshot metadata visible if Plan 120 UI/API exists
- no production database or bucket is referenced
- staging banner is visible

---

## Testing

- staging config cannot point at production DB URLs.
- staging config cannot point at production MinIO bucket with write access.
- staging service health checks pass.
- dashboard reads seeded snapshot data.
- auth uses staging users/salt.
- scraper is disabled or dummy-only by default.
- Caddy routes `dev.cartracker.info` to staging services only.
- staging seed can be rerun idempotently.

---

## Files Changed

| File | Change |
|------|--------|
| `docs/staging_environment_decision.md` | New staging architecture decision |
| `docker-compose.staging.yml` | Staging overrides |
| `.env.staging.example` | Staging environment template |
| `Caddyfile` | `dev.cartracker.info` route |
| `scripts/deploy_staging.sh` or `.ps1` | Manual staging deploy |
| `scripts/seed_staging_snapshot.py` | Wrapper around Plan 120 seeder |
| `ops/` and `dashboard/` | Staging banner/config support |
| `tests/config/test_staging_safety.py` | Safety checks |

---

## Out Of Scope

- Replacing CI.
- Replacing local development.
- Full production clone.
- Live scraping by default.
- Separate cloud account or Kubernetes environment.
- Managed Databricks staging workspace.
