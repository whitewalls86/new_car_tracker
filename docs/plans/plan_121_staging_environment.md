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

### Second incident, 2026-08-31: FastAPI 0.128 → 0.141, unpinned

The `boto3` case above was a package **missing** from one service. This one is
the other half of the same defect — a package **present everywhere and pinned
nowhere** — and it is the stronger argument, because it reached CI without
anyone changing a line of code.

**What happened.** [Plan 161](plan_161_testing_contract.md)'s new contract test
walked each FastAPI app's `app.routes` to enumerate its routes. It passed on a
developer machine and failed in CI, reporting eight route waivers as stale.
Neither the tests nor the routes had changed. **FastAPI changed
`include_router`:** up to 0.128 it flattened a router's routes into
`app.routes` with the prefix applied; by 0.141 it appends a single
`_IncludedRouter` wrapper exposing neither `routes` nor `prefix`, resolving its
children at match time.

Reproduced on Linux in `cartracker-ops:latest`: `app.routes` yields 4
non-framework routes and 10 wrappers, where `app.openapi()` yields all 54
operations. The developer machine had resolved **0.128.0**; CI resolved
**0.141.1**. Same commit, same code, different answer.

**Why this belongs here and not in Plan 161.** Plan 161 fixed its own test — it
now enumerates from `app.openapi()`, which is public and stable across both
versions. That repair is complete and is not what this entry records. What it
records is the exposure underneath:

| Service | `fastapi` constraint |
|---|---|
| `ops`, `processing`, `scraper`, `dbt_runner`, `container_health` | **none** |
| `archiver` | `>=0.115` — a floor with no ceiling |

`uvicorn`, `pydantic`, `boto3`, `psycopg2-binary` and `pyarrow` are unpinned
the same way. Across the whole tree only `prometheus-client==0.26.0` is pinned
to an exact version.

**The part that is not about tests.** CI is where this surfaced, but CI is not
the exposure. Every service image is built with `pip install -r
requirements.txt` and no constraint, so **a rebuild on the VM takes whatever
was published that day.** FastAPI is a **pre-1.0 project**; a routing-layer
internals change arriving in a minor bump is exactly what `0.x` reserves the
right to do. A `docker compose build` on a Tuesday can therefore change
production request routing or response serialisation with no commit, no review
and no signal — and the first evidence would be behavioural, in production.

**What this adds to the scope above.** Nothing structural: `constraints.txt`
is still the fix. Two refinements:

- The audit item should cover **web-stack packages, not only shared ones**.
  `fastapi`, `uvicorn`, `pydantic` and `starlette` are the highest-risk entries
  in the tree — pre-1.0 or fast-moving, and on every request path — yet they
  are not "shared packages" in the `boto3` sense that motivated this section.
- Pinning wants a **renewal mechanism**, not just a floor. A constraints file
  that is never raised becomes its own liability; whatever raises it should be
  a deliberate, reviewable change rather than a rebuild.

**Evidence:** [Plan 161's CAR-34 evidence](plan_161_testing_contract.md#evidence--car-34-2026-08-31),
PR #306, commit `e46ce27`.

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
- **Airflow metadata migrations against populated data** (added 2026-08-31 by
  Plan 139 Stage F, CAR-36)

### What local bring-up actually is now (recorded 2026-08-31)

`scripts/setup.ps1` was deleted on 2026-08-31. It was the last artifact in the
repository that assumed a local full-stack install — a fossil of the
local-hosting era, last touched 2026-03-23, predating the Oracle VM. It had
been non-functional since roughly April: it read `db/schema/schema_new.sql` and
three `db/seed/*.sql` files that no longer exist, under
`$ErrorActionPreference = "Stop"`, so it aborted at step 4 of 7. Nothing in CI
ran it, which is why nobody noticed.

That matters here because it changes what this plan is solving. **The data
plane already comes up locally in three commands:**

```bash
docker network create cartracker-net
docker volume create cartracker_pgdata     # declared external:
docker compose up -d
```

Both jobs the old script existed to do are now done by the stack itself —
Flyway applies every migration on first start, and `shared/minio.py` creates
the `bronze` bucket lazily on first use. There is no schema step and no seed
step to automate.

**The access plane is what does not come up.** `Caddyfile` is a single site
block hardcoded to `https://cartracker.info`, and Google OAuth requires a human
registering a client against a domain they control. Neither has a local or dev
variant. So a developer can run the pipeline but cannot exercise auth, role
behavior, or routing — which is precisely the first three bullets above, and
what Phase 1 (Environment Isolation) and Phase 4 (Auth and Safety) exist for.

A replacement bootstrap script was considered and rejected on 2026-08-31. The
scriptable remainder is the three commands above, already in the README quick
start; everything that actually blocks a newcomer — OAuth registration, the
domain and TLS story, choosing the `.env` secrets — is irreducibly manual. A
script automating the easy part while unable to do the hard part is the same
false confidence that let the deleted one rot unnoticed, and it would inherit
the same defect unless CI exercised it, which CI has no reason to do when it
already performs an equivalent bootstrap of its own.

---

## Scope note: rehearsing an Airflow upgrade

Production runs `airflow db migrate` on **every** stack start —
`docker-compose.yml`'s `airflow-init` sets `_AIRFLOW_DB_MIGRATE: 'true'`, gated
on Flyway completing, and the apiserver, scheduler and dag-processor are all
gated on `airflow-init` completing in turn. A failed migration is therefore a
full stop for Airflow, not a degraded mode.

[Plan 139](plan_139_test_suite_maintenance.md) Stage F made CI build that schema
from empty, which catches drift between Airflow's tables and the `ops` queries
that read them. It cannot catch the *upgrade path*: production migrates a
populated schema (`airflow.task_instance` held 438,355 rows on 2026-08-25), and
an Alembic step that only fails on real data — a NOT NULL backfill hitting an
existing null, a unique index colliding at scale, a migration that runs for
twenty minutes — is invisible to a build-from-empty.

Closing that needs a deployed stack and a restored production Postgres dump,
not a CI job: **restore a production dump into staging, run `airflow db
migrate` against it, and confirm it completes before bumping the Airflow pin in
`airflow/Dockerfile`.** Recorded here as a capability staging should have, not
as a decision that it is in this plan's first slice.

Note this is a *Postgres metadata* dump, and is unrelated to
[Plan 120](plan_120_ci_lake_snapshot_delivery.md)'s production-derived
fixtures — those are lake Parquet, closed over VIN/listing identity, with none
of that machinery applicable here.

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

## Follow-On: Testing Overhaul

Use the staging environment as the foundation for a broader testing overhaul
after Plan 121 is operational. Do not block the initial staging build on this
work; staging first gives the project a realistic place to replace brittle
mock-heavy coverage with higher-value workflow tests.

Create a separate implementation plan covering:

- a clear unit, component, integration, staging, and production-smoke test
  taxonomy
- consolidation of repetitive validation, argument-forwarding, and logging
  tests
- removal of low-value tests that primarily mirror implementation details
- staging-backed tests for auth, routing, service wiring, dbt/Spark jobs, and
  Plan 120 snapshot seeding
- CI tiers that keep fast PR checks separate from heavier scheduled or
  pre-deployment validation
- preservation of high-value SQL semantics, entity-closure, cache
  invalidation, and atomic-publication coverage

The goal is not an arbitrary test-count reduction. It is a smaller,
better-layered suite whose failures identify meaningful behavioral regressions
and whose heavier checks run in an environment capable of exercising the real
system.

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
