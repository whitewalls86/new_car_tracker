# Lakehouse Runbook (Plan 112 Gate A)

Operational runbook for the Gate A Lakekeeper REST catalog stack. Read
`docs/plan_112_gate_a_b_implementation_plan.md` and
`docs/lakehouse_substrate_decision.md` first for the design rationale.

**Scope of this document (A1):** bring-up/teardown of the standalone
Lakekeeper catalog + its isolated Postgres metadata store, and the CI
topology. PySpark table writes (A2), PyIceberg validation (A2b), and MLflow
(Gate B) are out of scope here and will extend this doc as they land.

---

## Critical safety invariant

**Never run `docker compose down -v` (or `docker compose --profile <name>
down -v`) against the main `docker-compose.yml` for lakehouse cleanup.**

The main file declares `cartracker_pgdata` (production Postgres, `external:
true` so `down -v` cannot delete the volume itself, but a bare `down`
still stops **every service in the main project** -- `postgres`, `minio`,
`scraper`, `ops`, `dashboard`, `airflow-*`, `grafana`, `caddy`, everything --
plus deletes several non-external volumes belonging to other services). A
`--profile lakehouse` flag on that command does **not** scope `down` to only
the profile's services; it only controls which profile-gated services
additionally participate in `up`/`down`.

This is exactly why Gate A infrastructure lives in a **wholly separate
Compose file and project**, `docker-compose.lakehouse.yml` /
`cartracker-lakehouse`. Every command in this runbook targets that file/
project explicitly. Gate A touches **no production Postgres and no Flyway
migration of any kind** -- Lakekeeper's metadata store is entirely isolated
in `lakekeeper-postgres`, a separate container in a separate project the
production database has no visibility into.

---

## Local / VM bring-up

```bash
# Bring up the catalog + its isolated metadata store (idempotent; safe to
# leave running during a working session).
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  up -d lakekeeper lakekeeper-postgres

# Check status / logs
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse ps
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse logs -f lakekeeper
```

Required env vars (see `.env.example`): `LAKEKEEPER_DB_PASSWORD`,
`LAKEKEEPER_PG_ENCRYPTION_KEY`, plus the existing `MINIO_ROOT_USER` /
`MINIO_ROOT_PASSWORD`. `LAKEKEEPER_IMAGE` may be overridden but defaults to a
pinned tagged release.

## Metadata-store posture

A1 deliberately runs Lakekeeper with its own `lakekeeper-postgres` container
and `lakekeeper_pgdata` volume. This does **not** mean lakehouse data is stored
in Postgres:

- MinIO stores the Iceberg table payload: Parquet files, Iceberg metadata JSON,
  manifests, manifest lists, and snapshots.
- Lakekeeper uses Postgres only for catalog control-plane metadata: warehouse
  definitions, namespaces, table registrations, storage profiles, and
  transactional catalog state.

This isolated Postgres container is a spike safety boundary. It lets us bring
Lakekeeper up, tear it down, and even delete its metadata volume without
touching production Postgres or Flyway.

If the Lakekeeper path graduates after A2/A3, the preferred steady-state shape
is **one Postgres server/container with separate databases and users**, not
three permanent Postgres containers and not one shared database full of
unrelated service schemas:

```text
postgres server/container
|-- cartracker         # operational app data
|-- airflow_metadata   # Airflow metadata
|-- lakekeeper         # catalog metadata
`-- mlflow             # MLflow backend metadata
```

Do not consolidate Lakekeeper metadata into production Postgres during Gate A.
Make that a separate follow-up once the REST catalog, PySpark writes, cleanup,
and VM rehearsal have proven out.

## Safe teardown (standalone lakehouse project only)

```bash
# Stop the catalog containers without deleting the metadata volume -- the
# lighter-weight option mid-session (e.g. to free memory between runs).
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  stop lakekeeper lakekeeper-postgres

# Full teardown, including Lakekeeper's own metadata volume. Safe to run
# with -v HERE specifically because docker-compose.lakehouse.yml is a
# standalone file/project declaring no production service or volume --
# unlike the main docker-compose.yml, which owns cartracker_pgdata and every
# production service. This command can never reach anything outside its own
# isolated resources.
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  down -v
```

New files reach the VM only via **git commit -> push -> pull**, and the
image is rebuilt (once `lakehouse-worker` exists, Gate A2) because new files
are not in cached layers -- per the standing deploy conventions. No `scp`, no
direct copy, no production restart without explicit confirmation.

---

## CI topology

The dedicated `lakehouse` GitHub Actions job is independent of the existing
`dbt` job's `services:` blocks -- it never shares Postgres/MinIO instances
with that job, and mixes no GitHub Actions `services:` containers with
Compose-managed ones. Instead it brings up one self-contained Compose stack:

```bash
docker compose \
  -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.ci.yml \
  -p ci-lakehouse up -d minio lakekeeper-postgres lakekeeper
```

- `docker-compose.lakehouse.ci.yml` adds a job-local, throwaway `minio`
  service and makes `cartracker-net` non-external (CI has no persistent main
  project to join), so this `up` creates the network fresh.
- Host ports are published on non-default values (MinIO `19000`, Lakekeeper
  `18181`) so this job's ports can never collide with the `dbt` job's
  `9000`/`5432`, even though the two already run on separate runner VMs by
  default -- this is defense-in-depth, not a fix for a real collision today.
- A1's readiness check waits for MinIO's live endpoint, then Lakekeeper's
  container healthcheck (`/home/nonroot/lakekeeper healthcheck`), then probes
  Lakekeeper's REST `/v1/config` endpoint over plain HTTP
  (`http://localhost:18181`) -- no JVM, no Spark, no PyIceberg needed for
  this check. Namespace CRUD is not attempted in A1: it requires a registered
  warehouse first, which is deferred to A2 alongside the actual table writes
  that need one.
- Teardown at the end of the job:
  ```bash
  docker compose \
    -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.ci.yml \
    -p ci-lakehouse down -v
  ```
  Safe unconditionally: `ci-lakehouse` is its own project containing only
  job-local, throwaway resources.

**Known limitation (A1):** the smoke check only exercises the `/v1/config`
endpoint the pinned image version exposes for health/config -- it does not
attempt namespace CRUD, since that needs a registered warehouse (A2 scope).
The exact endpoint path was not independently re-verified against every
historical Lakekeeper release; if the pinned version's REST surface differs,
the smoke step's assertion may need adjusting at implementation time -- this
is a documented limitation, not a design gap.

---

## Deferred to A2/A3 (not wired in A1)

- **PySpark table writes** and the `lakehouse-worker` one-shot container are
  added to `docker-compose.lakehouse.yml` in Gate A2.
- **`analytics_db` volume name check:** when `lakehouse-worker` is added, it
  needs to mount the main project's `analytics_db` volume read-only. That
  volume is not declared `external: true` in `docker-compose.yml`, so
  Compose names it with the main project's prefix (e.g.
  `cartracker-scraper_analytics_db`, depending on the checked-out directory
  name / `COMPOSE_PROJECT_NAME`). **Confirm the exact resolved name via
  `docker volume ls` on the VM at A2/A3 implementation time** and record it
  here before wiring an `external: true` reference in
  `docker-compose.lakehouse.yml` -- do not guess/hardcode it ahead of that
  check.
- **PyIceberg validation** (A2b) and **MLflow** (Gate B) are unrelated to
  this stack's bring-up/teardown and are documented separately as they land.
