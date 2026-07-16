# Lakehouse Runbook (Plan 112 Gate A)

Operational runbook for the Gate A Lakekeeper REST catalog stack. Read
`docs/plan_112_gate_a_b_implementation_plan.md` and
`docs/lakehouse_substrate_decision.md` first for the design rationale.

**Scope of this document (A1 + A2 + A3 + A4):** bring-up/teardown of the
standalone Lakekeeper catalog + its isolated Postgres metadata store, the CI
topology, (A2) the profile-gated `lakehouse-worker` PySpark round-trip against
a fixture-derived Iceberg table, (A3) the VM/local-manual rehearsal writing
the real `int_listing_volatility_features` table to Iceberg, and (A4) the
local integration harness that runs the same worker path on a dev box against
a Plan 120 snapshot, and (Gate B, first chunk) the MLflow **experiment
provenance bridge** that records enough metadata to trace an experiment back
to its Plan 120 snapshot and Iceberg table. PyIceberg validation (A2b) and
the rest of Gate B (backtest runs, the production always-on MLflow service)
are out of scope here and will extend this doc as they land.

**The Gate B provenance bridge trains no model and schedules no backtest.**
It logs one MLflow run of input-snapshot provenance -- see the "Gate B
provenance smoke" section below.

**A3 is VM/local-manual only, never CI.** It reads the real analytics DuckDB
file, which only exists on the VM (or a local box with that volume seeded) --
there is no CI job for it, and none should be added.

**A2 status: verified end to end, both in CI and on the production VM
(2026-07-15)** -- write, append, time-travel, and cleanup all succeeded
against real Lakekeeper + MinIO on the VM's OCI A1 (Ampere/ARM64) hardware.
See `docs/lakehouse_substrate_decision.md`'s "Gate A spike results" section
for the real snapshot IDs and cleanup proof.

**A3 status: verified end to end on the production VM (2026-07-15)** --
export, info, and cleanup all succeeded against the real
`int_listing_volatility_features` table (250,790 rows, exact match against
the DuckDB source). Three dependency/API fixes landed getting there (missing
`pytz`; duckdb's `.arrow()` returning a `RecordBatchReader` instead of a
`Table`; Python 3.13 dropping `distutils`, which PySpark 3.5.3 still
imports) -- see `docs/lakehouse_substrate_decision.md`'s "Gate A spike
results" section for the real row counts, snapshot ID, table location, and
cleanup proof.

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

## A3: real `int_listing_volatility_features` rehearsal (VM/local-manual only)

**Confirmed VM analytics volume name:** `cartracker_analytics_db` (via
`docker volume ls | grep analytics` on the VM) -- this is the exact name
declared `external: true` in `docker-compose.lakehouse.a3.yml`. If the
checkout directory or `COMPOSE_PROJECT_NAME` of the *main* project ever
changes, this name would need reconfirming; do not assume it stays
`cartracker_analytics_db` without checking `docker volume ls` again first.

**Every A3 command below adds `docker-compose.lakehouse.a3.yml` as a second
`-f`.** That override, not the base `docker-compose.lakehouse.yml`, is what
adds the read-only analytics mount -- kept out of the base file on purpose
because the CI `lakehouse` job runs A2 against that same base file, and a CI
runner has no `cartracker_analytics_db` volume to mount. Never add `-f
docker-compose.lakehouse.a3.yml` to any CI step.

```bash
# Rebuild the lakehouse-worker image -- picks up the procps fix and the new
# script; new files/Dockerfile changes are never in cached layers.
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.a3.yml \
  -p cartracker-lakehouse build lakehouse-worker

# Full export -> info -> cleanup rehearsal against the real
# int_listing_volatility_features table (250,790 rows, one per VIN per the
# Gate 0 audit). Reads /data/analytics/analytics.duckdb read-only; prints the
# captured metadata (snapshot id, row_count, distinct_vin17,
# max_latest_fetched_at, location) as JSON before cleaning up.
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.a3.yml \
  -p cartracker-lakehouse run --rm lakehouse-worker \
  python -m scripts.export_volatility_features_to_iceberg rehearsal

# Individual steps (useful for debugging one stage at a time; --keep on
# rehearsal skips the final cleanup so `info`/manual inspection has something
# to look at):
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.a3.yml \
  -p cartracker-lakehouse run --rm lakehouse-worker \
  python -m scripts.export_volatility_features_to_iceberg export
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.a3.yml \
  -p cartracker-lakehouse run --rm lakehouse-worker \
  python -m scripts.export_volatility_features_to_iceberg info
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.a3.yml \
  -p cartracker-lakehouse run --rm lakehouse-worker \
  python -m scripts.export_volatility_features_to_iceberg cleanup
```

Table name: `cartracker_experiments.volatility_features_snapshot` (distinct
from A2's `spike_fixture`, same namespace/prefix safety posture --
`cartracker_experiments` namespace, `lakehouse_spike/warehouse/` MinIO
prefix, same non-PURGE real-location cleanup as A2).

**Validation the `export` step enforces before it will write anything, and
that `rehearsal` re-checks after:**

- Source row count, `count(distinct vin17)`, and null-`vin17` count are read
  directly from DuckDB before any Spark session starts.
- `vin17` is `int_listing_volatility_features`'s declared primary key
  (`not_null` + `unique` in
  `dbt/models/intermediate/int_listing_volatility_features.schema.yml`) --
  `export` refuses to write if any row has a null `vin17` or if
  `distinct(vin17) != row_count`.
- After the write, `export` re-reads the Iceberg table's row count and
  refuses to proceed (raises) if it does not exactly equal the DuckDB source
  row count.
- `info` additionally reports `distinct_vin17` and `max_latest_fetched_at`
  read back from the *written* Iceberg table, and the table's real
  location/snapshot id(s) -- the same metadata shape Gate B's MLflow bridge
  will consume.

**Read-only posture:** the analytics DuckDB file is mounted `:ro` in
`docker-compose.lakehouse.a3.yml`; `_read_source_duckdb()` opens it with
`duckdb.connect(..., read_only=True)`. This script never opens a write
connection to the analytics DB and never writes to any MinIO prefix other
than `lakehouse_spike/warehouse/`.

**A3 is not wired into CI** -- it depends on a VM-only, production-derived
DuckDB volume that CI has no access to and should never try to seed. All
unit tests for this script (`tests/lakehouse/test_export_volatility_features_metadata.py`)
mock/avoid live DuckDB, Spark, and MinIO, exactly like A2's spike tests.

---

## A4: local integration harness (Plan 120 snapshot -> local smoke)

The A4 local path makes the VM a production *rehearsal* environment rather
than the first place missing dependencies, stale dbt schemas, bad Compose
mounts, or Spark/Lakekeeper config regressions are found. It consumes the
Plan 120 Gate E snapshot contract (`snapshot.tar.zst` +
`archive_manifest.json` + `latest.json`) -- it does **not** define a second
packaging/download format.

Three Compose overrides now exist; never mix their environments:

| Override | Environment | Analytics source | MinIO |
|----------|-------------|------------------|-------|
| `docker-compose.lakehouse.ci.yml` | CI only | none | throwaway, port 19000 |
| `docker-compose.lakehouse.a3.yml` | VM only | external `cartracker_analytics_db` volume, `:ro` | the real `minio` service |
| `docker-compose.lakehouse.local.yml` | local dev only | local dir bind mount (`LAKEHOUSE_LOCAL_ANALYTICS_DIR`, default `./.cache/analytics`), `:ro` | throwaway, port 19000 |

The local override is fully self-contained: non-external network, throwaway
MinIO, no external volume, no production Docker resource of any kind.
`down -v` against project `local-lakehouse` is safe by construction, exactly
like the base file. The throwaway MinIO's contents are ephemeral -- a `down`
loses the seeded snapshot, and reseeding is cheap and expected.

### Preferred: the one-command runner

`scripts/run_local_lakehouse_rehearsal.py` orchestrates the whole A4 flow
(stack up -> snapshot -> seed -> dbt DuckDB build -> warehouse registration
-> preflight -> A2 roundtrip -> A3 rehearsal), cache-aware and idempotent:

```powershell
# Everyday run: reuses the newest cached snapshot, skips seeding when MinIO
# already holds the fixture prefixes, skips the dbt build when
# .cache/analytics/analytics.duckdb exists.
python -m scripts.run_local_lakehouse_rehearsal

# Pull a fresh snapshot through the Plan 120 Gate F ops API, clear+reseed
# local MinIO, and rebuild the local DuckDB (needs $CARTRACKER_SNAPSHOT_TOKEN
# or --token):
python -m scripts.run_local_lakehouse_rehearsal --refresh-seed-data

# Other useful flags:
#   --reseed-only          clear+reseed MinIO from the cached snapshot
#   --rebuild-duckdb       rebuild analytics.duckdb only
#   --snapshot-id <id>     pin a specific snapshot (download or cached)
#   --snapshot-path <p>    offline: explicit snapshot.tar.zst (+ manifest.json beside it)
#   --skip-a2 / --skip-a3  skip the roundtrip / real-table rehearsal
#   --keep-iceberg-table   pass --keep to A3 for debugging
#   --no-build-images      skip lakehouse-worker/dbt image builds
```

The dbt step builds the `dbt/Dockerfile` image and runs a **targeted**
`dbt build --target duckdb --full-refresh --select
+int_listing_volatility_features` against the seeded local MinIO. That
selection includes no `postgres_scan()` source, so no local Postgres is
needed -- `POSTGRES_URL` is a deliberate dummy. The runner passes the local
override's MinIO credentials (`cartracker`/`cartracker123`) explicitly to
every subprocess, so parent-shell/production env vars can never leak in. It
never runs `down` or `down -v` (the dbt step's `-v` is an ordinary
read/write bind mount for its output, not a teardown flag); Gate F is the
only supported refresh path (no SSH).

The numbered steps below are the manual equivalents -- keep them for
troubleshooting individual stages or running one step at a time.

### 0. Preflight (run any time; read-only)

```bash
python -m scripts.preflight_local_lakehouse_snapshot
```

Checks, with actionable errors: required repo files; a downloaded Plan 120
manifest+archive pair (size-verified; `--verify-checksum` for full sha256);
MinIO reachable at `localhost:19000` (and **refuses** production-like
endpoints/buckets, with no override flag); fixture prefixes seeded;
`analytics.duckdb` present; required dbt feature tables exist (default
`int_listing_volatility_features`; add more via `--required-table`);
Lakekeeper's management endpoint answers at `localhost:18181`; the
`cartracker_experiments` warehouse is registered. Every default path/endpoint
is overridable -- see `--help`. It never writes, deletes, or registers
anything.

### 1. Acquire a Plan 120 snapshot archive (Gate F ops API)

Plan 120's Gate F ops download API is live -- download and
checksum-normalize into the local cache directly (token from
`$CARTRACKER_SNAPSHOT_TOKEN` or `--token`):

```powershell
python -m scripts.download_lake_snapshot --latest --base-url https://cartracker.info
# -> .cache/lake_snapshots/<snapshot_id>/{snapshot.tar.zst,manifest.json}
```

For offline/manual use (e.g. an archive+manifest pair already on disk), the
same script's `--manifest-path`/`--archive-path` local mode normalizes it
into the cache with the same checksum verification. There is no SSH path;
Gate F is the supported refresh mechanism.

### 2. Start the local stack

```bash
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse up -d minio lakekeeper-postgres lakekeeper
```

Same env vars as the base stack (`LAKEKEEPER_DB_PASSWORD`,
`LAKEKEEPER_PG_ENCRYPTION_KEY`; `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`
default to `cartracker`/`cartracker123` in this override, like CI).

### 3. Seed the local MinIO from the snapshot

```bash
python -m scripts.seed_lake_snapshot \
  --snapshot .cache/lake_snapshots/<snapshot_id>/snapshot.tar.zst \
  --minio-endpoint http://localhost:19000
```

Creates the `bronze` bucket if needed and uploads the fixture prefixes
present in the archive: `silver_normalized/` and `ops_normalized/` --
the two the Gate E archiver actually materializes and the preflight's
`snapshot-seeded` check requires. (`expected/` is also recognized if
present, but the real archiver never writes it -- see
`scripts/preflight_local_lakehouse_snapshot.py`'s `REQUIRED_SEED_PREFIXES`
comment.) The script's production-target guard applies here too.

### 4. Build analytics.duckdb from the seeded data (targeted; no Postgres)

A **targeted** build selecting `+int_listing_volatility_features` reads only
the seeded MinIO prefixes -- none of its upstream models touch the two
`postgres_scan()` sources (`stg_search_configs`, `tracked_models`), so
`POSTGRES_URL` can be a dummy. Containerized via the `dbt/Dockerfile` image
per the "never pip-install dbt locally" convention:

```bash
docker build -f dbt/Dockerfile -t cartracker-dbt-local .
docker run --rm --network local-lakehouse_cartracker-net \
  -e DUCKDB_PATH=/out/analytics.duckdb \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ROOT_USER=cartracker -e MINIO_ROOT_PASSWORD=cartracker123 \
  -e MINIO_BUCKET=bronze \
  -e POSTGRES_URL=postgresql://unused:unused@localhost:5432/unused \
  -v "$(pwd)/.cache/analytics:/out" \
  cartracker-dbt-local build --target duckdb --full-refresh \
  --select +int_listing_volatility_features
```

A **full** `dbt build --target duckdb` (no `--select`) still needs a real
Postgres with the migrated schema for those two sources -- that remains
out of scope for the A4 harness; the targeted build is all A2/A3 need. If
you have an `analytics.duckdb` from elsewhere (CI artifact, VM-derived
copy), dropping it into `./.cache/analytics/` also works -- the preflight's
`feature-tables` check will tell you whether it is usable.

### 5. Register the warehouse and run the rehearsal

```bash
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse build lakehouse-worker

docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse run --rm lakehouse-worker \
  python -m scripts.register_lakehouse_warehouse

# A2 synthetic round-trip (no analytics.duckdb needed -- proves Spark/
# Lakekeeper/MinIO wiring alone):
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse run --rm lakehouse-worker \
  python -m scripts.spike_iceberg_lakehouse roundtrip

# A3 rehearsal against the local analytics.duckdb (validates source counts,
# writes the Iceberg table, re-validates row counts, prints metadata JSON,
# cleans up):
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse run --rm lakehouse-worker \
  python -m scripts.export_volatility_features_to_iceberg rehearsal
```

Row-count validation is built into `export`/`rehearsal` (source vs Iceberg
exact match) -- the same guards the VM A3 run uses.

### 6. Clean up (lakehouse-local resources only)

```bash
# Iceberg table + its lakehouse_spike/ objects (if --keep was used):
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse run --rm lakehouse-worker \
  python -m scripts.export_volatility_features_to_iceberg cleanup

# Full local teardown, safe by construction (this project owns only
# throwaway resources; the seeded MinIO data is lost, by design):
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse down -v
```

Never point the local override's commands at the VM, never reference it from
CI, and never mount a production volume into it -- the A3 override exists
for the VM path.

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

## A3/A4 status and what remains deferred

**A3 is implemented and VM-verified** (`scripts/export_volatility_features_to_iceberg.py`,
see the "A3" section above) -- the real `int_listing_volatility_features`
snapshot writes to `cartracker_experiments.volatility_features_snapshot` via
the same `lakehouse-worker`, read-only from
`/data/analytics/analytics.duckdb`. The `cartracker_analytics_db` external
volume name was confirmed on the VM via `docker volume ls | grep analytics`
and is wired into `docker-compose.lakehouse.a3.yml`, a separate VM/local-
manual-only override -- never the base `docker-compose.lakehouse.yml`, which
the CI `lakehouse` job also runs A2 against. Export, info, and cleanup all
ran successfully against the real 250,790-row table on 2026-07-15 -- see
`docs/lakehouse_substrate_decision.md`'s "Gate A spike results" section for
the real output.

**A4 is implemented end to end** (`docker-compose.lakehouse.local.yml`,
`scripts/preflight_local_lakehouse_snapshot.py`,
`scripts/run_local_lakehouse_rehearsal.py`, the "A4" section above) -- the
local flow reuses the A2/A3 scripts unchanged against a self-contained local
stack seeded from a Plan 120 archive, with
`python -m scripts.run_local_lakehouse_rehearsal` as the one-command entry
point. Snapshot acquisition goes through the Plan 120 Gate F ops download
API (`scripts/download_lake_snapshot.py --latest`), and the local DuckDB is
built by the scripted targeted dbt build (A4 step 4) -- no VM copy, no SSH,
no local Postgres.

Still deferred:

- **Full local `dbt build --target duckdb`** (no `--select`) -- still needs
  a local Postgres with the migrated schema for the two `postgres_scan()`
  sources; the A4 harness deliberately uses the targeted
  `+int_listing_volatility_features` build instead.
- **Full local `dbt build --target duckdb`** graduation and **PyIceberg
  validation** (A2b) remain deferred as noted above.
- **The rest of Gate B** -- backtest policy runs and the production always-on
  MLflow service (Postgres backend, Flyway migration; plan Sec 3.1/3.2, "B1")
  -- is deferred. The provenance bridge below is the first Gate B chunk.

---

## Gate B provenance smoke (MLflow experiment provenance bridge)

**This trains no model and schedules no backtest.** It logs one MLflow run
recording where a lakehouse/backtesting input snapshot came from: its Plan 120
snapshot archive (`snapshot_id`, `export_fingerprint`, `archive_sha256`,
`archive_key`, `archive_manifest_key`) and its Gate A Iceberg table
(`iceberg_catalog`, `iceberg_table`, `iceberg_snapshot_id`, `row_count`,
`distinct_vin17`, `max_latest_fetched_at`), with the Plan 120
`archive_manifest.json` attached as an artifact.

Two pieces:

- `shared/mlflow_provenance.py` -- pure, MLflow-free payload
  construction/validation (unit-tested in
  `tests/lakehouse/test_mlflow_provenance.py`).
- `scripts/log_lakehouse_experiment_provenance.py` -- the CLI that assembles
  the fields (from a Plan 120 manifest, an Iceberg `info` JSON, a metadata
  JSON, and/or individual flags) and logs exactly one MLflow run.

### Backend choice (why SQLite here, not production Postgres yet)

This first chunk deliberately does **not** touch production Postgres. The
standalone `docker-compose.mlflow.yml` (`-p cartracker-mlflow`) runs the
tracking server against an **isolated SQLite backend store** on its own
`mlflow_store` volume, with artifacts under an **isolated MinIO prefix**
`s3://${MINIO_BUCKET}/mlflow/artifacts/`. No Flyway migration, no new prod DB
user/schema -- same isolation posture as `docker-compose.lakehouse.yml`, so
`down -v` against this project can only touch its own resources. The
Postgres-backed always-on service (plan Sec 3.1/3.2, "B1") is the graduation
path and is intentionally out of scope for this PR.

### Smoke A -- pure local file store (no server, no Docker)

The zero-infrastructure smoke: needs only `pip install mlflow` in a throwaway
venv (never the shared project venv). Logs to a local file store.

```bash
# Build + print the exact params/tags/artifact WITHOUT logging (no mlflow needed):
python -m scripts.log_lakehouse_experiment_provenance \
    --manifest .cache/lake_snapshots/<snapshot_id>/manifest.json \
    --iceberg-info-json /tmp/iceberg_info.json \
    --feature-table-name int_listing_volatility_features \
    --dry-run

# Actually log a run to a local file store (requires mlflow installed):
python -m scripts.log_lakehouse_experiment_provenance \
    --manifest .cache/lake_snapshots/<snapshot_id>/manifest.json \
    --iceberg-info-json /tmp/iceberg_info.json \
    --feature-table-name int_listing_volatility_features \
    --tracking-uri file:./.cache/mlruns

# Inspect it:
mlflow ui --backend-store-uri file:./.cache/mlruns   # then open http://localhost:5000
```

Where the inputs come from in the A4 local flow:
- `--manifest` is the file `scripts/download_lake_snapshot.py --latest`
  already wrote to `.cache/lake_snapshots/<snapshot_id>/manifest.json`.
- `--iceberg-info-json` is the stdout of the exporter's `info` subcommand.
  **Caveat:** the default A4 runner
  (`python -m scripts.run_local_lakehouse_rehearsal`) runs A3 as `rehearsal`
  **without `--keep`, so it drops `volatility_features_snapshot` immediately
  after** — a bare `info` afterwards fails ("table not found"). Two ways to
  have a live table to read `info` from:
  - Run the A4 runner with `--keep-iceberg-table` (passes `--keep`, skips the
    A3 cleanup), then run `info`; **remember to clean up manually afterwards**
    (`... run --rm lakehouse-worker python -m
    scripts.export_volatility_features_to_iceberg cleanup`).
  - Or create-and-read in one shot with `export` (which leaves the table) then
    `info`, independent of the runner:
    ```bash
    LH="docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml -p local-lakehouse"
    $LH run --rm lakehouse-worker python -m scripts.export_volatility_features_to_iceberg export
    $LH run --rm lakehouse-worker python -m scripts.export_volatility_features_to_iceberg info \
        > /tmp/iceberg_info.json
    # ... log provenance ..., then clean up the table you just left behind:
    $LH run --rm lakehouse-worker python -m scripts.export_volatility_features_to_iceberg cleanup
    ```

### Smoke B -- standalone MLflow server (browsable UI + MinIO artifacts)

**Requires the main stack's `cartracker-net` + `minio` (the VM, or a locally
running main `cartracker` project).** `docker-compose.mlflow.yml` joins the
**external** `cartracker-net` and resolves `minio` by container DNS for its
artifact store. It is **not** reachable from an A4-only local box: the A4
stack (`docker-compose.lakehouse.local.yml`, project `local-lakehouse`) runs
its own **isolated, non-external** network + throwaway MinIO, which this file
does not join. On a pure A4-only setup use **Smoke A** (file store) instead;
Smoke B is for the VM / main-stack environment where `cartracker-net` and
`minio` exist.

```bash
# 1. Start the standalone server (joins the EXTERNAL cartracker-net + minio;
#    needs the main cartracker stack / VM running -- see note above):
docker compose -f docker-compose.mlflow.yml -p cartracker-mlflow up -d --build
#    UI at http://localhost:15000

# 2. Log a provenance run against it:
python -m scripts.log_lakehouse_experiment_provenance \
    --manifest .cache/lake_snapshots/<snapshot_id>/manifest.json \
    --iceberg-info-json /tmp/iceberg_info.json \
    --feature-table-name int_listing_volatility_features \
    --env local \
    --tracking-uri http://localhost:15000

# 3. Inspect: open http://localhost:15000, experiment "adaptive_refresh_provenance".
#    The manifest artifact is served from s3://${MINIO_BUCKET}/mlflow/artifacts/.

# 4. Teardown (safe -- standalone project, no production resource declared):
docker compose -f docker-compose.mlflow.yml -p cartracker-mlflow down       # keep data
docker compose -f docker-compose.mlflow.yml -p cartracker-mlflow down -v    # + drop volume
```

On the VM the same commands apply; the server joins the real `cartracker-net`
and writes artifacts to the real MinIO under the isolated `mlflow/` prefix
only. It is **not** wired into the A4 runner by default -- provenance logging
is an explicit, separate step in this PR.

Still deferred for Gate B: backtest policy runs, the production always-on
MLflow service (Postgres backend + Flyway), and any Caddy `/mlflow` route.
