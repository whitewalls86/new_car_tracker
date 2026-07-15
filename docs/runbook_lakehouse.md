# Lakehouse Runbook (Plan 112 Gate A)

Operational runbook for the Gate A Lakekeeper REST catalog stack. Read
`docs/plan_112_gate_a_b_implementation_plan.md` and
`docs/lakehouse_substrate_decision.md` first for the design rationale.

**Scope of this document (A1 + A2):** bring-up/teardown of the standalone
Lakekeeper catalog + its isolated Postgres metadata store, the CI topology,
and (A2) the profile-gated `lakehouse-worker` PySpark round-trip against a
fixture-derived Iceberg table. PyIceberg validation (A2b) and MLflow (Gate B)
are out of scope here and will extend this doc as they land.

## LAKEKEEPER_DB_PASSWORD / LAKEKEEPER_PG_ENCRYPTION_KEY value format

Learned during VM A1 verification: generate these as URL-safe/hex strings,
not arbitrary text with symbols. `lakekeeper-postgres`'s DSN
(`postgresql://lakekeeper:${LAKEKEEPER_DB_PASSWORD}@...`) is built by
substituting the raw env var into a URL, so a password containing `@`, `:`,
`/`, `#`, or similar breaks DSN parsing. Generate both with:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

`.env.example` already documents this for `LAKEKEEPER_PG_ENCRYPTION_KEY`;
apply the same rule to `LAKEKEEPER_DB_PASSWORD`.

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

# Smoke the management API from the Docker network. The base VM/local stack
# does not publish Lakekeeper's 8181 port to the host; only the CI override
# publishes 18181 for GitHub Actions.
docker run --rm --network cartracker-net curlimages/curl:8.10.1 \
  -fsSL http://lakekeeper:8181/management/v1/info
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

## A2: PySpark Iceberg fixture round-trip (local / VM)

```bash
# Build the lakehouse-worker image (new files are not in cached layers --
# rebuild after every git pull that touches lakehouse/ or scripts/).
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  build lakehouse-worker

# One-time (idempotent) server bootstrap + warehouse registration --
# required before any Iceberg REST /v1/config call or table write; A1's
# known limitation is exactly this. A fresh Lakekeeper server has no default
# project until POST /management/v1/bootstrap runs once (warehouse creation
# otherwise 404s with ProjectNotFound) -- this script does that first, then
# registers the warehouse.
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.register_lakehouse_warehouse

# Full write -> append -> time-travel -> cleanup round-trip against a small
# fixture-derived table (cartracker_experiments.spike_fixture). Prints the
# captured snapshot metadata as JSON.
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse roundtrip

# Individual steps (useful for debugging one stage at a time):
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse write
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse append
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse info
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse cleanup
```

A2's table is a small deterministic synthetic fixture (5 VINs, two batches),
generated in-process by `scripts/spike_iceberg_lakehouse.py` rather than a
Parquet slice read from the Plan 120 seeded fixture -- a deliberate scope
simplification to keep the spike script decoupled from the dbt/silver
schema. The real `int_listing_volatility_features` snapshot is A3 (VM-only).

`lakehouse-worker` is profile-gated (`profiles: ["lakehouse-worker"]`), so it
never starts on a bare `docker compose -f docker-compose.lakehouse.yml up`;
every invocation above is an explicit `run --rm`.

**Every write/cleanup is guarded** (`shared/iceberg_catalog.py`) to the
`cartracker_experiments` namespace and the `lakehouse_spike/warehouse/`
MinIO prefix under the `bronze` bucket -- both scripts refuse to touch
anything else, even given a bad table-name/namespace argument.

**Cleanup does not use Iceberg/Spark's `PURGE`.** `DROP TABLE ... PURGE`
unregisters the table from Lakekeeper first, then tries to delete/verify its
data files via Lakekeeper's REST request-signing endpoint -- which then
rejects those S3 calls because the table it needs to authorize against no
longer exists. `cmd_cleanup` instead does a plain `DROP TABLE IF EXISTS`
(catalog metadata only), then deletes the underlying MinIO objects itself
directly via boto3 with our own static credentials, no Lakekeeper signing
involved. It reads the table's *real* location before dropping it
(`key_prefix_from_location`) rather than reconstructing a
`<namespace>/<table_name>` path -- Lakekeeper allocates its own UUID-based
object paths, so a guessed path would silently match nothing.

**A2 leaves state behind on success, by design:** the warehouse registration
and namespace live on in Lakekeeper's isolated `lakekeeper_pgdata` volume
after a `roundtrip` run (only the *table* is dropped by cleanup, not the
warehouse/namespace registration itself). This is expected and harmless --
re-running `register_lakehouse_warehouse` is a no-op, and a full
`down -v` (see "Safe teardown" above) clears it entirely if ever needed.

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
  Lakekeeper's warehouse-free management info endpoint over plain HTTP
  (`http://localhost:18181/management/v1/info`) -- no JVM, no Spark, no
  PyIceberg needed for this check.
- A2 then builds the `lakehouse-worker` image (`docker compose ... build
  lakehouse-worker`) and runs both the warehouse registration
  (`python -m scripts.register_lakehouse_warehouse`, idempotent) and the
  PySpark write/append/time-travel/cleanup round-trip
  (`python -m scripts.spike_iceberg_lakehouse roundtrip`) **inside that same
  container** via `docker compose run --rm lakehouse-worker`, joined to the
  job's Compose network by container-DNS service names
  (`lakekeeper`/`minio`) -- the same image and networking model the
  VM/runbook path uses, not a separate pip-installed pyspark on the bare
  runner. This is the one component whose CI viability was genuinely
  uncertain at planning time (plan Sec 4.3, Q2) -- it is attempted here first
  per that recommendation; fall back to VM/local-manual only if it proves too
  slow/flaky in practice.
- Teardown at the end of the job:
  ```bash
  docker compose \
    -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.ci.yml \
    -p ci-lakehouse down -v
  ```
  Safe unconditionally: `ci-lakehouse` is its own project containing only
  job-local, throwaway resources.

**Known limitation (A1, still true):** the A1 smoke check only exercises
Lakekeeper's warehouse-free management info endpoint. It does not attempt
Iceberg REST `/v1/config` or namespace CRUD directly -- A2's warehouse
registration + PySpark round-trip exercise that path instead, via the real
client libraries (Spark's `SparkCatalog`) rather than a bare REST call. A
bare `/catalog/v1/config` call returns `400` on the pinned Lakekeeper release
before warehouse registration, so treating that as an A1 smoke endpoint was
intentionally avoided.

---

## Deferred to A3 (not wired in A2)

- **Real `int_listing_volatility_features` snapshot** (250,790 rows, one per
  VIN) is A3, VM-only, real production-derived data, read-only from
  `/data/analytics/analytics.duckdb`.
- **`analytics_db` volume mount:** A2's `lakehouse-worker` does not mount
  `analytics_db` -- the fixture-derived spike doesn't need it. When A3 adds
  the mount, confirm the main project's resolved volume name via
  `docker volume ls` on the VM first (it is not declared `external: true` in
  `docker-compose.yml`, so Compose names it with the main project's prefix --
  expected to be `cartracker-scraper_analytics_db` given this repo's checkout
  directory name, but **do not hardcode that without confirming on the VM**)
  before wiring an `external: true` reference in
  `docker-compose.lakehouse.yml`.
- **PyIceberg validation** (A2b) and **MLflow** (Gate B) are unrelated to
  this stack's bring-up/teardown and are documented separately as they land.
