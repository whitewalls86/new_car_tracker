# Plan 112 Gate A + Gate B Implementation Plan

Deep implementation-planning pass for the next Plan 112 chunk: the **Gate A
Iceberg + catalog foundation** and the **Gate B MLflow foundation**. This is a
PR-prep design document, not implementation. It assumes Gate 0 preflight is
substantially complete (real VM audit output is in
`docs/adaptive_refresh_feature_audit.md`; the first-pass substrate decision is
in `docs/lakehouse_substrate_decision.md`) and that the only remaining Gate 0
item is the sampled manual VIN/listing history review.

Read first: `docs/plan_112_refresh_policy_backtesting.md`,
`docs/lakehouse_substrate_decision.md`,
`docs/adaptive_refresh_feature_audit.md`,
`docs/plan_117_storage_and_adaptive_refresh_roadmap.md`,
`docs/plan_118_dbt_spark_migration.md`,
`docs/plan_119_lakehouse_governance.md`,
`docs/plan_120_ci_lake_snapshot_delivery.md`,
`docs/plan_123_dbt_incrementalization_and_resource_governance.md`.

---

## Locked decisions (revised 2026-07-14)

This revision replaces the PyIceberg-first / Postgres-SQL-catalog direction
from the prior pass. The prior pass optimized for the lightest possible CI/
Windows footprint; this revision optimizes for the stated Plan 112/117 goal —
Spark/PySpark as the primary write/read engine, and a REST catalog (the
strongest portability story toward Unity Catalog/Polaris-style governance) —
while still keeping the first PR small and rollback-safe.

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Gate A engine: PySpark is the primary writer/reader.** PyIceberg is kept only as an **optional secondary validation client** against the same REST catalog — useful for a lightweight sanity check or a future CI-only smoke path, never the primary implementation path. | Matches Plan 112/117's explicit goal ("Spark/PySpark for table writes, feature preparation, and model training") instead of deferring it. A REST catalog + Spark is also the more direct rehearsal of the target architecture (`docs/plan_117_storage_and_adaptive_refresh_roadmap.md`'s revised architecture diagram already shows Spark writers feeding Iceberg behind a catalog layer). |
| D2 | **Catalog: minimal Lakekeeper REST catalog**, with its **own isolated Postgres metadata store, deployed via a standalone Compose file/project entirely separate from the main `docker-compose.yml`** — never a schema in, or even the same Compose project as, the production `postgres` service. | Lakekeeper is a REST-catalog server implementing the Iceberg REST Catalog spec, which is what both PySpark (`org.apache.iceberg.rest.RESTCatalog` / `spark.sql.catalog.<name>.type=rest`) and PyIceberg (`RestCatalog`) speak natively — one catalog implementation serves both engines with zero adapter code. Isolating its metadata store in a **separate Compose file/project** (not just a `profile:` inside the main file) avoids any Flyway migration, any new prod-Postgres user/schema, any coupling to production database load, **and** any risk that a broad teardown command scoped to the main project could ever reach production volumes/services (see §5 for the incident this specifically guards against). Real governance (RBAC, multi-tenant namespaces, whether Lakekeeper's metadata store should ever move) is explicitly deferred to Plan 119. |
| D3 | **MLflow backend store: dedicated Postgres schema + user** (`mlflow` schema, `mlflow_user`), MLflow managing its own tables via `search_path`. Artifact store on MinIO. | Unchanged from the prior pass — consistent with how every other DB user/schema is provisioned here (Flyway placeholders, per-service least-privilege users). This is independent of the Gate A catalog choice: MLflow's backend store has no dependency on how/where Iceberg catalog metadata lives, and MLflow stays in the main `docker-compose.yml` because, unlike Gate A, touching production Postgres is its intended design (D3). |
| D4 | **Postgres lifecycle: isolate Lakekeeper during Gate A; consolidate only after the spike proves useful, preferably as separate databases/users on one Postgres server/container.** | MinIO stores Iceberg table data and metadata files; Postgres stores Lakekeeper's catalog control-plane state. The isolated `lakekeeper-postgres` container is a spike safety boundary, not necessarily the forever shape. If Lakekeeper graduates, prefer one Postgres server with separate databases such as `cartracker`, `airflow_metadata`, `lakekeeper`, and `mlflow`: cleaner service ownership than one shared database with many schemas, less operational overhead than several permanent Postgres containers on one VM. |

The hot production scrape/claim path must not call PySpark, PyIceberg, MLflow,
Lakekeeper, or the catalog. That invariant (Plan 112/117) is preserved:
everything here is either a one-shot worker, an internal-only service, or a
script. **Gate A's first spike does not touch the production Postgres
instance, the main Compose project, or any of the main project's volumes at
all** — a deliberate simplification versus the prior pass, which had proposed
a `iceberg_catalog` schema in prod Postgres.

---

## Compose topology decision (governs §2, §4, §5)

Two Compose-isolation questions came up in review and are resolved once,
here, rather than left ambiguous across sections:

1. **Gate A infrastructure lives in a standalone Compose file,
   `docker-compose.lakehouse.yml`, run under its own project name
   (`-p cartracker-lakehouse`) — never as a `profile:` inside the main
   `docker-compose.yml`.** A `--profile lakehouse down -v` invoked against the
   *main* file would still resolve against the *entire* main project (every
   service and every non-external volume it declares, including
   `cartracker_pgdata`'s sibling volumes), because `--profile` only changes
   which profile-gated services participate in `up`/`down` — it does not
   scope `down`/`down -v` to only that profile's services. A wholly separate
   file/project has no such blast radius: it declares no production service
   and no production volume, so `down -v` against *it* is safe by
   construction. See §5 for the exact commands and the explicit "never do
   this against the main file" callout.
2. **CI runs Gate A entirely through Docker Compose, in one dedicated
   `lakehouse` GitHub Actions job, independent of the existing `dbt` job's
   GitHub Actions `services:` blocks.** Mixing the two networking models
   (GH Actions `services:` containers, reachable only via `localhost` port
   mapping, vs. Compose-managed containers, reachable via container-name DNS
   on a Compose network) inside one job is fragile. Instead: the `lakehouse`
   job brings up `lakekeeper` + `lakekeeper-postgres` + a **job-local,
   throwaway MinIO** (via a CI-only Compose override,
   `docker-compose.lakehouse.ci.yml`) as one self-contained Compose stack, with
   distinct, non-default host ports so it can never collide with anything the
   `dbt` job publishes even if GitHub Actions runners were ever shared (they
   are not, today, but the ports are chosen defensively regardless). See §4.2
   for the concrete job design.

---

## 1. Recommended PR sequence

Six PRs. The first two stand up infrastructure without touching real data;
the third proves the full Iceberg mechanics against a CI-safe fixture; the
fourth is the VM-only realistic rehearsal.

| PR | Title | Gate | Depends on | CI-runnable? | Rollback |
|----|-------|------|-----------|--------------|----------|
| **A1** | Lakekeeper REST catalog: standalone Compose file + isolated metadata store + runbook | A | Gate 0 | **Yes** — dedicated `lakehouse` CI job, management `/management/v1/info` smoke only (Iceberg REST `/v1/config` + namespace CRUD deferred to A2 — see implementation note above) | Additive; `docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse down -v` (safe — see Compose topology decision above), nothing else touched |
| **A2** | PySpark Iceberg fixture spike: write/read/append/time-travel against Lakekeeper + MinIO | A | A1 | Same dedicated `lakehouse` CI job, budget-gated — see §4.3 | Additive; delete `lakehouse_spike/` MinIO prefix + drop the fixture table via the catalog |
| **A2b** *(optional, small)* | PyIceberg REST-catalog validation script | A | A1, A2 | **Yes** (pure Python, talks REST to the same Lakekeeper) | Additive script only |
| **A3** | VM rehearsal: real `int_listing_volatility_features` snapshot + cleanup proof | A | A2 | No (VM/local-manual only, real production-derived data) | Additive; no prod Parquet/DuckDB mutation (read-only source mount) |
| **B1** | MLflow service + Postgres backend + MinIO artifact store | B | none (parallel to A) | Smoke via ephemeral store; server VM/local | Remove service + drop `mlflow` schema |
| **B2** | Iceberg→MLflow metadata bridge + smoke experiment | B | A2 (A2b optional), B1 | **Yes** (fixture table → MLflow file store) | Additive helper + test |
| **B3** *(thin)* | Runbook + substrate/feature-audit doc finalization | A+B | A3, B2 | docs only | n/a |

**What belongs in PR A1 (the first real code):**

> **Implementation note (2026-07-14):** A1 shipped slightly leaner than this
> section originally planned. `lakehouse-worker` was **not** scaffolded in
> A1 — it references a `lakehouse/Dockerfile` build context that doesn't
> exist until A2, so adding it structurally now would have left a Compose
> service pointing at a nonexistent Dockerfile. Likewise, no Lakekeeper
> storage-profile/warehouse registration happened in A1, and the CI smoke
> step only hits Lakekeeper's warehouse-free management `/management/v1/info`
> endpoint — **not** Iceberg REST `/v1/config` or namespace CRUD, since both
> require a registered warehouse first. Both the worker and the warehouse/
> namespace registration are now explicitly A2 scope; see
> `docs/runbook_lakehouse.md`'s "Deferred to A2/A3" section. The
> bullets below are left as originally planned for context, with the actual
> delivered scope marked inline.

- `docker-compose.lakehouse.yml` — a **standalone** Compose file (not part of
  `docker-compose.yml`), containing:
  - `lakekeeper` — the REST catalog server.
  - `lakekeeper-postgres` — Lakekeeper's **own** Postgres instance (separate
    image/container/volume from the production `postgres` service; not a
    schema inside it; not even the same Compose project).
  - `lakehouse-worker` (profile-gated within this file so a bare
    `docker compose -f docker-compose.lakehouse.yml up` still cannot launch
    it) — **deferred to A2 in the actual implementation** (see note above),
    not added structurally in A1.
  - Joins the existing external `cartracker-net` network (declared
    `external: true`, same as the main file) so it can reach the real
    `minio` service on the VM/local — see §2.2 for the one caveat this
    creates around the `analytics_db` volume name.
- `docker-compose.lakehouse.ci.yml` — a CI-only override adding a job-local
  throwaway `minio` service and making the network non-external (CI has no
  persistent main project to join) — §4.2.
- Lakekeeper storage-profile configuration pointing at the `bronze` bucket
  under `lakehouse_spike/warehouse/` (§2.3) — **deferred to A2 in the actual
  implementation** (see note above); not needed until a warehouse/table is
  actually written.
- A CI smoke step in the new dedicated `lakehouse` job: bring the stack up,
  wait for health, and hit Lakekeeper's warehouse-free management
  **`/management/v1/info`** endpoint via plain HTTP (no JVM, no Spark, no
  PyIceberg needed for this check). **Iceberg REST `/v1/config` and namespace
  create/list are deferred to A2** in the actual implementation — they need a
  registered warehouse first, which A1 does not set up.
- `docs/runbook_lakehouse.md` (start now, extend through B3): how to bring the
  stack up/down locally and on the VM, and the explicit "never in the hot
  path" + "never touches production Postgres" + "never run `down`/`down -v`
  against the main `docker-compose.yml` for this" invariants.
- **No Flyway migration, no production Postgres change of any kind, no
  changes to `docker-compose.yml` at all.** This is the biggest
  simplification versus the prior PyIceberg/SQL-catalog pass, and the
  standalone-file choice (vs. the previously-proposed `profile:` inside the
  main file) is what makes the isolation actually airtight rather than just
  conventionally-safe.

**What belongs in PR A2:** the actual PySpark write/read/append/time-travel
loop against a fixture-derived table (§2.4), run through the profile-gated
one-shot `lakehouse-worker`, plus snapshot-metadata capture (§2.5) and
cleanup (§2.6).

> **Implementation note (2026-07-14):** A2 shipped with one deliberate
> simplification versus §2.4's original text. Rather than seeding the Plan
> 120 lake-snapshot fixture into MinIO and having the PySpark job read a
> bounded Parquet slice of it, `scripts/spike_iceberg_lakehouse.py` generates
> a small deterministic synthetic dataset in-process (5 VINs, two batches).
> This keeps the spike script fully decoupled from the dbt/silver schema
> while still exercising the complete write → append → time-travel → cleanup
> mechanics against the real REST catalog + MinIO. The real
> `int_listing_volatility_features` snapshot remains A3 (VM-only) scope,
> unaffected. Also landed: `scripts/register_lakehouse_warehouse.py` (the
> idempotent Lakekeeper warehouse-bootstrap script implied by §2.2 but not
> separately named there, which also has to bootstrap the Lakekeeper server
> itself via `POST /management/v1/bootstrap` before a first warehouse can be
> created against its default project), and the CI `lakehouse` job's A2
> round-trip step was attempted per the Q2 recommendation and **passes**
> (see docs/runbook_lakehouse.md's A2 section).
>
> One correction to §2.5's Spark-conf sample: it configures Hadoop-AWS's
> S3AFileSystem (`spark.hadoop.fs.s3a.*`, `s3a://` scheme). In practice,
> Lakekeeper hands Spark `s3://` table locations (not `s3a://`), which
> Iceberg's native `S3FileIO` serves, not Hadoop's generic FileSystem --
> `shared/iceberg_catalog.py` instead sets
> `spark.sql.catalog.cartracker.io-impl=org.apache.iceberg.aws.s3.S3FileIO`
> plus `s3.access-key-id`/`s3.secret-access-key` catalog properties, and
> `lakehouse/Dockerfile` ships `iceberg-aws-bundle` (AWS SDK v2) instead of
> `hadoop-aws`/`aws-java-sdk-bundle` (AWS SDK v1).
>
> **A2 is now verified end to end, both in CI and on the production VM
> (2026-07-15).** Two more fixes landed getting there, beyond what's
> described above: (1) `DROP TABLE ... PURGE` failed because Lakekeeper
> rejects the S3 request-signing calls PURGE issues for a table it has
> already unregistered -- cleanup now does a plain `DROP TABLE` and deletes
> the MinIO objects itself directly via boto3, reading the table's *actual*
> (UUID-based, not `<namespace>/<table_name>`-shaped) location first; (2)
> `lakehouse/Dockerfile` hardcoded `JAVA_HOME` to the x86_64 JVM path, which
> doesn't exist on the production VM (an OCI A1/Ampere ARM64 shape, per Plan
> 105) -- fixed via a build-time symlink that resolves the actual installed
> JVM path regardless of architecture. See `docs/lakehouse_substrate_decision.md`'s
> "Gate A spike results" section for the real VM run's snapshot IDs and
> cleanup proof.

**What belongs in PR A3:** the VM/local-manual rehearsal writing the real
`int_listing_volatility_features` table to Iceberg (§2.4 point 2).

> **Implementation note (2026-07-15):** A3 shipped as
> `scripts/export_volatility_features_to_iceberg.py`, run through the same
> profile-gated `lakehouse-worker` as A2, against a new table name
> (`cartracker_experiments.volatility_features_snapshot`, distinct from A2's
> `spike_fixture`) under the same `cartracker_experiments`
> namespace/`lakehouse_spike/warehouse/` MinIO-prefix safety posture. The
> confirmed VM analytics volume name is `cartracker_analytics_db` (via
> `docker volume ls | grep analytics`), now declared `external: true` in
> `docker-compose.lakehouse.yml` and mounted `:ro` on `lakehouse-worker` --
> resolving the "volume-name caveat" flagged in §2.2 above. `export` reads
> the DuckDB source read-only, validates it (no null `vin17`, `distinct(vin17)
> == row_count`, matching §2.7's "vin17 is the primary key" invariant) before
> writing, then re-validates the written Iceberg table's row count against
> the source count; `info` additionally reports `distinct_vin17` and
> `max_latest_fetched_at` read back from the table. Cleanup reuses A2's
> non-PURGE, real-location-read pattern unchanged (`cleanup_keys` is imported
> directly from `scripts/spike_iceberg_lakehouse.py`, not duplicated). Also
> landed in this PR: `procps` added to `lakehouse/Dockerfile` to silence the
> `ps: command not found` warning Spark's `load-spark-env.sh` emitted during
> VM A2 verification (harmless, but noisy on every session start). A3 is
> VM/local-manual only, same as planned -- no CI job exists or should exist
> for it, since it depends on a VM-only, production-derived DuckDB volume;
> unit tests for the new script's metadata/validation/cleanup-guard logic run
> in the existing `unit-tests` job with everything live (DuckDB, Spark,
> MinIO) mocked or avoided at import time, exactly like A2's spike tests.

**Explicitly deferred out of A1/A2/A3:** any MLflow (B*), PyIceberg
validation (A2b), and any Lakekeeper RBAC/multi-tenant/governance
configuration (Plan 119).

**Dependency / rollback safety notes:**

- Every PR writes only under new, disjoint prefixes (`lakehouse_spike/`,
  `lakehouse/`, `mlflow/`) and, for Gate A, an **entirely separate Compose
  project** rather than a schema or profile in the production one. Nothing
  overwrites `silver/`, `ops_normalized/`, bronze HTML, the analytics DuckDB,
  or any existing schema/service/volume. Rollback = revert the PR +
  `docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse
  down -v` + delete the new MinIO prefix; no production reader is affected
  because none reads these paths (Plan 112 out-of-scope invariant) and no
  production Compose resource is even addressable by this teardown command.
- Because Lakekeeper's metadata store is isolated, there is no Flyway
  rollback-migration question at all for Gate A — dropping the standalone
  project's containers and volume is the entire rollback.

---

## 2. Gate A: Iceberg + catalog spike plan

### 2.1 Architecture recommendation (local, CI, VM)

```text
   docker-compose.lakehouse.yml, project "cartracker-lakehouse"
   (standalone file; never started by the main project's `docker compose up`;
    see "Compose topology decision" above for why this is a separate file)
   +----------------------------------------------------------------+
   |  lakekeeper-postgres        lakekeeper (REST catalog server)   |
   |  (own container/volume,     - Iceberg REST Catalog API         |
   |   NOT production postgres)  - storage-profile -> MinIO bucket  |
   +---------------+---------------------------+---------------------+
                   | internal-only JDBC                |  REST (HTTP)
                   +------------------+                 |
                                      v                 v
           MinIO bucket = bronze on the real cartracker-net (VM/local)
           OR a job-local throwaway MinIO on an ephemeral network (CI)
                             lakehouse_spike/warehouse/
                               cartracker_experiments/<table>/{data,metadata}/
                                      ^                 ^
                                      | S3A (Hadoop-AWS) | S3 (s3fs, optional validation)
                    +-----------------+                 +------------------+
                    |  PySpark (PR A2, primary)   PyIceberg (PR A2b, optional)  |
                    |  spark.sql.catalog.*=rest    RestCatalog(uri=lakekeeper)  |
                    +----------------------------------------------------------+
```

- **Local (Windows dev box):** `docker compose -f docker-compose.lakehouse.yml
  -p cartracker-lakehouse up -d lakekeeper lakekeeper-postgres` brings up the
  catalog, joined to the same external `cartracker-net` the main project's
  `minio` runs on. PySpark runs inside the one-shot `lakehouse-worker`
  container (recommended, avoids installing a JVM/Spark natively on Windows).
- **CI:** the dedicated `lakehouse` job (§4.2) brings up
  `docker-compose.lakehouse.yml` **plus** `docker-compose.lakehouse.ci.yml`
  (job-local MinIO, non-external network) as one self-contained Compose
  stack. A1's smoke check needs only Lakekeeper's REST API; A2's full PySpark
  round-trip is heavier (JVM + Iceberg-Spark runtime jar download) — see
  §4.3 for the CI-vs-VM tradeoff.
- **VM:** A3's real-table rehearsal goes through the same `lakehouse-worker`,
  joined to the real `cartracker-net` and real `minio`, never an always-on
  service, and never concurrently with a heavy dbt build or `snapshot-worker`
  (reuse the Plan 123 `_check_dbt_runner_not_building` guard).

### 2.2 Catalog: minimal Lakekeeper REST catalog, isolated Postgres (D2)

The prior pass chose a Postgres-backed PyIceberg `SqlCatalog` against the
*production* Postgres to minimize new services. Under the revised D1/D2, a
**REST catalog is the correct minimal path**, not a step to defer: it is the
one catalog interface both PySpark and PyIceberg speak without any
per-engine adapter code, and Lakekeeper is a small, purpose-built REST
catalog server (Rust binary, `quay.io/lakekeeper/catalog` image) rather than a
general application requiring integration work.

Deployment shape — **`docker-compose.lakehouse.yml`, a standalone file, not
an addition to `docker-compose.yml`** (see "Compose topology decision"):

```yaml
# docker-compose.lakehouse.yml
#
# Standalone compose file for Gate A. Deliberately NOT merged into
# docker-compose.yml / the main compose project: `down`/`down -v` against
# this file can only ever affect the resources declared in *this* file, so
# it is safe to use freely, including full teardown. Never add these
# services to the main docker-compose.yml under a `profiles:` key instead —
# `docker compose --profile <name> down -v` against the main file still
# resolves against the ENTIRE main project (see §5).
#
# VM/local usage (joins the real cartracker-net + real minio):
#   docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
#     up -d lakekeeper-postgres lakekeeper
#
# CI usage adds docker-compose.lakehouse.ci.yml as a second -f — see §4.2.

services:
  lakekeeper-postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: lakekeeper
      POSTGRES_USER: lakekeeper
      POSTGRES_PASSWORD: ${LAKEKEEPER_DB_PASSWORD}
    volumes:
      - lakekeeper_pgdata:/var/lib/postgresql/data   # a volume owned by THIS project only
    networks: [cartracker-net]

  lakekeeper-migrate:
    image: ${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}
    command: ["migrate"]
    restart: "no"
    environment:
      LAKEKEEPER__PG_ENCRYPTION_KEY: ${LAKEKEEPER_PG_ENCRYPTION_KEY}
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://lakekeeper:${LAKEKEEPER_DB_PASSWORD}@lakekeeper-postgres:5432/lakekeeper
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://lakekeeper:${LAKEKEEPER_DB_PASSWORD}@lakekeeper-postgres:5432/lakekeeper
      LAKEKEEPER__AUTHZ_BACKEND: allowall
    depends_on:
      lakekeeper-postgres: { condition: service_healthy }
    networks: [cartracker-net]

  lakekeeper:
    image: ${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}
    command: ["serve"]
    environment:
      LAKEKEEPER__PG_ENCRYPTION_KEY: ${LAKEKEEPER_PG_ENCRYPTION_KEY}
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://lakekeeper:${LAKEKEEPER_DB_PASSWORD}@lakekeeper-postgres:5432/lakekeeper
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://lakekeeper:${LAKEKEEPER_DB_PASSWORD}@lakekeeper-postgres:5432/lakekeeper
      LAKEKEEPER__AUTHZ_BACKEND: allowall
      # Storage profile registered at warehouse-create time, not env vars,
      # but the MinIO credentials it needs are supplied the same way every
      # other service here gets them:
      LAKEKEEPER_MINIO_ENDPOINT: http://minio:9000
      LAKEKEEPER_MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      LAKEKEEPER_MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    depends_on:
      lakekeeper-migrate: { condition: service_completed_successfully }
    healthcheck:
      test: ["CMD", "/home/nonroot/lakekeeper", "healthcheck"]
      interval: 5s
      timeout: 10s
      retries: 36
      start_period: 5s
    networks: [cartracker-net]

  lakehouse-worker:
    build: { context: ., dockerfile: lakehouse/Dockerfile }
    image: cartracker-lakehouse
    profiles: ["lakehouse-worker"]   # never starts on a bare `up` even within this file
    mem_limit: 6g
    environment:
      LAKEKEEPER_CATALOG_URI: http://lakekeeper:8181/catalog
      ICEBERG_WAREHOUSE_NAME: cartracker_experiments
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      DBT_RUNNER_URL: http://dbt_runner:8080   # reuse the Plan 123 build-in-progress guard
      DUCKDB_PATH: /data/analytics/analytics.duckdb
    volumes:
      - analytics_db:/data/analytics:ro   # see the volume-name caveat below
    networks: [cartracker-net]
    depends_on:
      lakekeeper: { condition: service_started }

volumes:
  lakekeeper_pgdata:   # owned entirely by this project; never referenced by docker-compose.yml

networks:
  cartracker-net:
    external: true
```

**Volume-name caveat (implementation-time check, not a blocker):** the
`analytics_db` volume `lakehouse-worker` mounts read-only is created by the
*main* project (`docker-compose.yml`), which does not mark it `external`, so
Compose names it with the main project's prefix (e.g.
`cartracker-scraper_analytics_db`, depending on the checked-out directory
name/`COMPOSE_PROJECT_NAME`). For a separate-project file to mount that same
volume, `docker-compose.lakehouse.yml` must declare it `external: true` with
the **exact** resolved name — confirm via `docker volume ls` on the VM at
A2/A3 implementation time and record it in `docs/runbook_lakehouse.md`. This
is the one place the standalone-file design needs a concrete environment
check; it does not change the isolation guarantee (a wrong/missing external
volume name fails the mount loudly, it cannot accidentally attach to the
wrong writable volume).

Exact Lakekeeper env var names should be confirmed against the pinned
version at implementation time (Lakekeeper's config surface is still
evolving); the shape above is the planning-level contract: isolated Postgres
DSN, MinIO endpoint/credentials, nothing from the production Postgres.

A single warehouse (`cartracker_experiments`) is registered against
Lakekeeper's REST admin API once at A1 startup (a small idempotent script or
`curl` step in the runbook), pointing its storage profile at
`s3://bronze/lakehouse_spike/warehouse/` (VM/local) or the CI override's
throwaway bucket (CI).

### 2.3 MinIO path + table naming conventions

**Correction to the substrate doc (unchanged from the prior pass):** it wrote
`s3a://cartracker/lakehouse_spike/iceberg/...`, but the real bucket is
`bronze` (`MINIO_BUCKET` throughout `docker-compose.yml`). Corrected
conventions, with the scheme now genuinely dual-purpose since Spark is the
primary engine:

| Purpose | Warehouse root | Namespace | Example table location |
|---------|----------------|-----------|------------------------|
| Gate A spike (A2/A3, VM/local) | `s3://bronze/lakehouse_spike/warehouse/` (Lakekeeper's own S3 view) / `s3a://bronze/lakehouse_spike/warehouse/` (Spark/Hadoop-AWS view of the same objects) | `cartracker_experiments` | `.../cartracker_experiments/int_listing_volatility_features/` |
| Gate A spike (CI, job-local MinIO) | `s3://bronze/lakehouse_spike/warehouse/` inside the throwaway CI bucket — same relative path, different (ephemeral) bucket instance | `cartracker_experiments` | `.../cartracker_experiments/spike_fixture/` |
| Real analytical tables (later, Plan 118/119) | `s3://bronze/lakehouse/warehouse/` | `cartracker_silver`, `cartracker_features`, `cartracker_marts`, `cartracker_experiments` | — out of scope here |

- The `s3://` vs `s3a://` split is not a mistake to fix — it is expected:
  Lakekeeper's Rust S3 client and PyIceberg's `s3fs` both use `s3://`
  semantics, while Spark's Hadoop-AWS connector requires `s3a://` for the
  same physical MinIO objects. Both resolve to identical object keys.
- Namespace vocabulary is adopted **from Plan 119 Phase 1 on day one**
  (`cartracker_experiments`) so spike tables never need renaming when
  governance lands.
- `lakehouse_spike/` remains a dedicated top-level prefix, disjoint from
  `silver/`, `ops_normalized/`, and bronze `html/`. Nothing production reads
  it.

### 2.4 First table: fixture-derived first, `int_listing_volatility_features` second

Unchanged reasoning from the prior pass, now executed through Spark instead
of PyIceberg:

1. **A2 (fixture, CI weight permitting — see §4.3):** first Iceberg table is
   built from the **Plan 120 lake-snapshot fixture**, seeded into the CI
   job's own throwaway MinIO (§4.2) the same way the `dbt` job seeds its
   MinIO today (`scripts/seed_lake_snapshot_fixture.py`, reserved
   `obs_year=2099` partition). The PySpark job reads a bounded slice and
   exercises the full write/read/append/time-travel/cleanup loop against
   Lakekeeper.
2. **A3 (VM):** snapshot the real `int_listing_volatility_features`
   (250,790 rows per the Gate 0 audit — small; one row per VIN) out of
   `/data/analytics/analytics.duckdb` **read-only**, write it to
   `cartracker_experiments.int_listing_volatility_features` via the same
   PySpark job, and capture real snapshot IDs into the substrate doc's
   placeholder section.

Reading the source is strictly read-only: the worker mounts the analytics
DuckDB file `:ro` (§5) — never a write connection to the analytics DB or to
production Parquet paths.

### 2.5 Write / read / append / time-travel / metadata capture

PySpark + Iceberg-REST-catalog API shape the spike job standardizes on:

```python
spark = (
    SparkSession.builder
    .appName("lakehouse-gate-a-spike")
    .config("spark.sql.catalog.cartracker", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.cartracker.type", "rest")
    .config("spark.sql.catalog.cartracker.uri", os.environ["LAKEKEEPER_CATALOG_URI"])  # http://lakekeeper:8181/catalog
    .config("spark.sql.catalog.cartracker.warehouse", "cartracker_experiments")
    .config("spark.sql.catalog.cartracker.s3.endpoint", os.environ["MINIO_ENDPOINT"])
    .config("spark.sql.catalog.cartracker.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

# write (create)
df.writeTo("cartracker.cartracker_experiments.spike_fixture").create()      # snapshot 1

# read
spark.table("cartracker.cartracker_experiments.spike_fixture").show()

# append second snapshot
second_df.writeTo("cartracker.cartracker_experiments.spike_fixture").append()  # snapshot 2

# time travel
snapshots = spark.sql(
    "SELECT snapshot_id, committed_at FROM cartracker.cartracker_experiments.spike_fixture.snapshots"
).collect()
old = spark.read.option("snapshot-id", snapshots[0]["snapshot_id"]) \
    .table("cartracker.cartracker_experiments.spike_fixture")

# metadata capture (write to JSON, and later to MLflow — see Gate B)
meta = {
    "catalog": "cartracker",
    "table": "cartracker_experiments.spike_fixture",
    "current_snapshot_id": snapshots[-1]["snapshot_id"],
    "snapshots": [row["snapshot_id"] for row in snapshots],
    "location": spark.sql(
        "SELECT * FROM cartracker.cartracker_experiments.spike_fixture.files LIMIT 1"
    ).collect(),  # or DESCRIBE TABLE EXTENDED for the location field
    "row_count": spark.table("cartracker.cartracker_experiments.spike_fixture").count(),
}
```

The metadata-capture function returns a plain dict serialized to
`iceberg_snapshot.json` (Gate A artifact and Gate B `dataset_snapshot.json`
input), the same contract as the prior pass — only the producing engine
changed. This decouples the Gate B MLflow bridge (§3.6) from which engine
wrote the table: it only ever consumes this JSON shape.

### 2.6 Optional PyIceberg validation client (A2b)

Kept only as a **secondary, optional** validation path against the same
Lakekeeper REST catalog — not the primary implementation, and not a
prerequisite for A2/A3:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "cartracker",
    uri=os.environ["LAKEKEEPER_CATALOG_URI"],   # same REST endpoint Spark uses
    warehouse="cartracker_experiments",
    **{
        "s3.endpoint": os.environ["MINIO_ENDPOINT"],
        "s3.access-key-id": os.environ["MINIO_ROOT_USER"],
        "s3.secret-access-key": os.environ["MINIO_ROOT_PASSWORD"],
    },
)
tbl = catalog.load_table("cartracker_experiments.spike_fixture")
assert tbl.scan().to_arrow().num_rows == expected_row_count
```

Value: a pure-Python, no-JVM way to re-read a table Spark wrote and confirm
the REST catalog is genuinely engine-agnostic — the concrete proof of the
"portable across Spark, Databricks-style platforms, ... open-source query
engines" goal in Plan 117. This is a small, independently-reviewable script
(A2b) that can land any time after A2, or be skipped entirely without
blocking B*.

### 2.7 Cleanup rules + proof that production is not mutated

Gate A's "cleanup proof" is a first-class, tested part of A2/A3, not prose:

- The spike writes **only** under `lakehouse_spike/*` in MinIO and to
  Lakekeeper's own isolated Postgres — never the production `postgres`
  service, never `silver/`/`ops_normalized/`/bronze `html/`. A guard in the
  worker script asserts the resolved warehouse path starts with
  `lakehouse_spike/` and refuses to run otherwise.
- A cleanup subcommand drops the namespace/table via Lakekeeper's REST API
  (`DELETE` on the table/namespace endpoints) and deletes the
  `lakehouse_spike/warehouse/<table>/` prefix from MinIO (boto3
  `list_objects_v2` + `delete_objects`, using
  `shared/minio.get_boto3_client`).
- **Cleanup proof for A3 (VM):** capture `(row_count, column set)` of the
  source `int_listing_volatility_features` (and the source Parquet prefix
  object count/total bytes) *before* the spike and *after* cleanup; assert
  unchanged. This before/after pair fills the substrate doc's "Cleanup
  proof:" placeholder.
- **Full-teardown proof:**
  `docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse
  down -v` drops the `lakekeeper_pgdata` volume entirely. This is safe to run
  freely — unlike the main project, this file declares no production
  service or volume, so there is nothing it *could* touch outside its own
  isolated resources (see the Compose topology decision and §5).
- In CI, the smoke test always tears down the dedicated `lakehouse` job's
  stack in a post-step so re-runs are deterministic.

### 2.8 Catalog/governance checks (minimum, deferred deep work to Plan 119)

Per Gate A's "minimum catalog checks," A1/A2 demonstrate the smallest useful
governance behavior available for free with a REST catalog:

- **Naming rules:** tables are only ever created under the
  `cartracker_experiments` warehouse/namespace; the worker script rejects any
  other namespace.
- **Reader/writer separation (documented, enforced later):** Lakekeeper
  supports multi-tenant/RBAC configuration, but none is configured in Gate A
  — a single unauthenticated (or single-token) REST endpoint is acceptable
  for an isolated spike. A future scoped token for a `svc_dashboard`-style
  reader is Plan 119 Phase 5 scope.
- **Table ownership metadata:** the metadata JSON records catalog,
  namespace, and writer identity — the seed of Plan 119's
  table-registration record.
- **Differences vs managed Unity Catalog** are already enumerated in the
  substrate doc's "Known gaps" section; A3 appends the concrete
  Lakekeeper-specifics (self-hosted REST catalog, no managed multi-workspace
  governance, single-node metadata store in this spike).

---

## 3. Gate B: MLflow foundation plan

Unchanged from the prior pass except where noted — MLflow's backend store
(D3) has no dependency on the Gate A catalog choice, so most of this section
carries over directly. The one real adjustment is in §3.6 (the Iceberg
snapshot metadata now typically originates from the PySpark job, not
PyIceberg). MLflow also stays in the **main** `docker-compose.yml`
(unaffected by the Compose topology decision above), since — unlike Gate A —
touching production Postgres via a dedicated schema/user is MLflow's intended
design (D3).

### 3.1 Service shape

New `docker-compose.yml` service `mlflow`, internal-only (no Caddy route in
the first cut — "internal first" per the plan; a `/mlflow` reverse-proxy
entry is a follow-up once auth is decided). Modeled on the lightweight
always-on services already here (e.g. `processing`), with a memory cap given
the constrained host.

```yaml
  mlflow:
    build:
      context: .
      dockerfile: mlflow/Dockerfile        # python:3.13-slim + mlflow + psycopg2-binary + boto3
    image: cartracker-mlflow
    container_name: cartracker-mlflow
    restart: unless-stopped
    mem_limit: 1g                           # tracking server is light; cap it on the 23.4GB host
    environment:
      MLFLOW_BACKEND_STORE_URI: postgresql+psycopg2://mlflow_user:${MLFLOW_DB_PASSWORD}@postgres:5432/cartracker?options=-csearch_path%3Dmlflow
      MLFLOW_ARTIFACTS_DESTINATION: s3://bronze/mlflow/artifacts
      MLFLOW_S3_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
    command: >
      mlflow server --host 0.0.0.0 --port 5000
      --backend-store-uri postgresql+psycopg2://mlflow_user:${MLFLOW_DB_PASSWORD}@postgres:5432/cartracker?options=-csearch_path%3Dmlflow
      --artifacts-destination s3://bronze/mlflow/artifacts
      --serve-artifacts
    depends_on:
      flyway:
        condition: service_completed_successfully
      minio:
        condition: service_started
    networks:
      - cartracker-net
```

`--serve-artifacts` routes artifact I/O through the MLflow server so clients
never need direct MinIO creds — the cleanest fit for "internal first" and for
a future CI client that only holds an MLflow URL.

This is the **only** part of Gate A/B that touches production Postgres, and
it does so exactly as designed (D3, unchanged): a dedicated schema/user, same
as every other service.

### 3.2 Backend store (D3): dedicated Postgres schema + user

New Flyway migration (next `V0NN`), same placeholder pattern as every other
role migration:

```sql
CREATE USER mlflow_user WITH PASSWORD '${mlflowPassword}';
CREATE SCHEMA IF NOT EXISTS mlflow AUTHORIZATION mlflow_user;
-- MLflow runs its own alembic migrations into this schema on first server start.
```

Add `mlflowPassword` to the Flyway `-placeholders.*` block in
`docker-compose.yml` and the `${MLFLOW_DB_PASSWORD}` env, mirroring
`dbtPassword`/`metricsPassword`. CI's Flyway step gets a `ci_mlflow`
placeholder value like the others. MLflow owns and migrates its own tables
inside `mlflow`; no hand-written table DDL.

### 3.3 Artifact store

MinIO under `s3://bronze/mlflow/artifacts/` — a new dedicated prefix,
disjoint from `lakehouse*`, `silver/`, `ops_normalized/`, and bronze `html/`.
Reuses the existing `bronze` bucket (no second bucket to provision). Served
through the MLflow server (`--serve-artifacts`) so experiment code and CI
need only the tracking URI.

### 3.4 Experiment naming / tagging conventions

- **Experiment name:** `adaptive_refresh_backtest` (one experiment for the
  whole Plan 112 arc; runs distinguished by `policy_family` tag). A separate
  `adaptive_refresh_smoke` experiment isolates CI/smoke runs from real ones.
- **Run name:** `{policy_family}-{code_sha_short}-{utc_timestamp}`.
- **Tags** (searchable): `policy_family`, `entity_grain` (`vin17`), `code_sha`,
  `iceberg.catalog`, `iceberg.table`, `iceberg.snapshot_id`, `plan` (`112`),
  `gate` (`B`), `env` (`ci` | `vm` | `local`).

### 3.5 Required params, metrics, tags, artifacts (smoke run)

Directly implements the Plan 112 Gate B metadata table:

| MLflow field | Kind | Source |
|--------------|------|--------|
| `policy_family` | tag/param | `baseline` for smoke |
| `input_window_start` / `input_window_end` | param | min/max `fetched_at` of the input |
| `code_sha` | tag/param | `git rev-parse HEAD` |
| `entity_grain` | param | `vin17` |
| `iceberg.catalog` / `iceberg.table` / `iceberg.snapshot_id` | tag | from Gate A metadata capture (§2.5) |
| `dataset.row_count` | param | scanned Iceberg table row count |
| `dataset.distinct_vins` | param | `count(distinct vin17)` over the scan |
| placeholder metrics (e.g. `fetches_skipped_pct=0`) | metric | proves metric logging |
| `dataset_snapshot.json`, `policy_config.json`, `environment.json` | artifact | written by the run |

### 3.6 Logging Iceberg snapshot metadata to MLflow

The Gate A metadata-capture dict (§2.5) is the single source of the
`iceberg.*` tags and `dataset_snapshot.json`. A small bridge helper
(`shared/mlflow_iceberg.py`, PR B2) takes that plain dict — **not** an
engine-specific table handle — plus an MLflow active run, and sets the
tags/params + logs the artifact. Keeping the bridge's input a plain
JSON-serializable dict (rather than a PySpark `Table` or a PyIceberg `Table`
object) means it does not care whether A2's PySpark job or A2b's optional
PyIceberg script produced the metadata — either one satisfies the same
contract. This is the seam the rest of Plan 112 builds on; keeping it
engine-agnostic avoids each later runner re-implementing provenance logging
per engine.

---

## 4. Testing strategy

Guiding constraints (from repo conventions + memories): CI has Postgres +
MinIO + a real dbt build against the Plan 120 fixture, but **no VM data and
no MLflow server**. Heavy/real-table work is VM-only. Isolated deps get
isolated venvs/images. Never pip-install dbt locally to self-run integration
tests (CI-only).

### 4.1 Unit tests (CI, `not integration`)

| File | Cases |
|------|-------|
| `tests/lakehouse/test_lakekeeper_config.py` | storage-profile/warehouse config builder produces the expected `lakehouse_spike/` prefix; namespace guard rejects a non-`cartracker_experiments` namespace; Spark-conf builder sets the `type=rest` / `uri` / S3A keys correctly from env vars (no live Spark session). |
| `tests/lakehouse/test_iceberg_spike_metadata.py` | metadata-capture dict has all required keys given a fake snapshot-list response; cleanup issues the expected `delete_objects` batch (boto3 mocked) and the expected Lakekeeper `DELETE` calls (HTTP mocked). |
| `tests/lakehouse/test_mlflow_iceberg_bridge.py` | bridge sets every required `iceberg.*` tag and `dataset.*` param from a plain metadata dict + captured `mlflow` client; `dataset_snapshot.json` artifact content matches the dict. |

Unit tests mock boto3/HTTP/mlflow — no live services — so they run in the
existing `unit-tests` job with no new services.

### 4.2 Integration tests: dedicated `lakehouse` CI job

Per the Compose topology decision above, Gate A integration coverage runs in
**one new, independent GitHub Actions job** (`lakehouse`), parallel to and
independent of the existing `dbt` job — not as extra `services:` blocks
bolted onto `dbt`, and not sharing that job's Postgres/MinIO instances.

Job design:

1. Checkout.
2. `docker network create cartracker-net` is unnecessary — the CI override
   file makes the network project-local (`external: false`), so
   `docker compose up` creates it.
3. `docker compose -f docker-compose.lakehouse.yml -f
   docker-compose.lakehouse.ci.yml -p ci-lakehouse up -d minio lakekeeper-postgres lakekeeper`
   — brings up a **job-local, throwaway MinIO** (bundled only by the CI
   override, never by the base file) plus Lakekeeper and its isolated
   Postgres, all on one ephemeral Compose-managed network. Host ports are
   published on **non-default values** (e.g. MinIO `19000`, Lakekeeper
   `18181`) specifically so this job's ports can never collide with the
   `dbt` job's `9000`/`5432`, even though separate GitHub Actions jobs
   already run on separate runner VMs by default — the distinct ports are
   defense-in-depth, not a fix for a real collision today.
4. Create the throwaway `bronze` bucket in the job-local MinIO (same
   one-off `boto3` snippet the `dbt` job already uses, pointed at
   `localhost:19000`).
5. Seed the Plan 120 fixture into the job-local MinIO
   (`scripts/seed_lake_snapshot_fixture.py`, `MINIO_ENDPOINT=
   http://localhost:19000`) — this job cannot reuse the `dbt` job's seeded
   data because GitHub Actions jobs run on independent runners with no
   shared filesystem/network; re-seeding is cheap and intentional
   duplication, not a bug.
6. **A1 smoke step:** plain `requests` calls from the runner to
   `http://localhost:18181` — create/list/delete a namespace. No JVM.
7. **A2 round-trip step (budget-gated, see §4.3):** `pyspark` installed on
   the runner (own isolated `pip install` step, or a dedicated venv —
   mirroring the Airflow-venv isolation pattern), Iceberg-Spark-runtime +
   Hadoop-AWS jars fetched via `actions/cache` keyed on their version pins,
   Spark session configured against `localhost:18181` (catalog) and
   `localhost:19000` (S3A) exactly as a local dev run would be, just via
   host-published ports instead of container-DNS names.
8. **A2b step (optional):** `pyiceberg[s3fs]` read-back validation against
   the same `localhost:18181`/`localhost:19000`.
9. Teardown:
   `docker compose -f docker-compose.lakehouse.yml -f
   docker-compose.lakehouse.ci.yml -p ci-lakehouse down -v` — safe, because
   `ci-lakehouse` is its own project containing only job-local resources.

| File | What it exercises | Data source |
|------|-------------------|-------------|
| `tests/integration/lakehouse/test_lakekeeper_smoke.py` | Step 6 above, **as actually implemented in A1: only Lakekeeper's warehouse-free management `/management/v1/info` endpoint** — Iceberg REST `/v1/config` and namespace CRUD are deferred to A2, since they need a registered warehouse first. **No Spark, no PyIceberg** either way — proves the catalog service itself, independent of any client engine. | none — pure catalog check |
| `tests/integration/lakehouse/test_pyspark_iceberg_roundtrip.py` | Step 7: full PySpark round-trip against Lakekeeper + the job-local MinIO: create → append → time-travel → snapshot-id capture → cleanup; asserts snapshot count = 2, time-travel row counts differ, cleanup empties the prefix. | Plan 120 fixture, seeded into the job-local MinIO in step 5 |
| `tests/integration/lakehouse/test_pyiceberg_validation.py` *(A2b, optional)* | Step 8: PyIceberg `RestCatalog` reads the table the Spark test just wrote and confirms row count/schema match — the cross-engine proof. | table written by the Spark round-trip test above |
| `tests/integration/lakehouse/test_mlflow_smoke_run.py` | MLflow logging against an **ephemeral local store** (file-based backend + tmp artifact dir), not the server — runs in the existing `unit-tests`/`dbt`-style flow, unrelated to the `lakehouse` job's Compose stack. | fixture-derived Iceberg table metadata dict |

### 4.3 CI vs VM split, and the explicit weight tradeoff this revision introduces

This revision is **heavier in CI than the PyIceberg-first plan was**, because
Spark plus a REST catalog server both need to run somewhere. Being explicit
about that tradeoff (raised as Open Question Q2 below) rather than hiding it:

| Runs in CI (`lakehouse` job) | VM/local-manual only |
|------------|----------------------|
| Lakekeeper management `/management/v1/info` smoke test (A1) — lightweight, no JVM | Real `int_listing_volatility_features` snapshot (A3) |
| MLflow field-logging smoke (B2, ephemeral store, separate flow) | MLflow **server** end-to-end against Postgres+MinIO (B1) |
| Unit tests (A1/A2/B2, `unit-tests` job) | Cleanup-proof before/after on real prod row counts (A3) |
| PySpark fixture round-trip (A2) — **attempted, but runtime-budget-gated** (below) | PySpark round-trip, if CI runtime/flakiness proves unacceptable (fallback) |
| PyIceberg validation (A2b) — pure Python, cheap in CI | — |

The PySpark fixture round-trip (A2) is the one component whose CI viability
is genuinely uncertain at planning time: `pyspark` + the
`iceberg-spark-runtime` + `hadoop-aws` jars add real download weight and JVM
startup time to a job that would otherwise be pure Python (unlike the `dbt`
job, this new `lakehouse` job has no other JVM workload to amortize the cost
against). The recommendation is to **attempt it in the dedicated `lakehouse`
job first** (bounded runtime budget, `local[2]` Spark master, jars cached via
`actions/cache`) and **fall back to VM/local-manual only** if it proves too
slow or flaky — a decision that should be made after seeing A1's job runtime
land as a baseline, not assumed up front. This is called out explicitly in
§7 (Q2) for a go/no-go call.

### 4.4 Avoiding huge production data in CI

- CI only ever touches the Plan 120 `edge`-scale fixture (~100 VINs); the
  real 250k-VIN table is never downloaded into CI.
- The Iceberg round-trip test writes a **bounded** slice (cap rows
  explicitly, e.g. `LIMIT 500`) so even if the fixture grows, CI
  runtime/storage stays flat.
- The `lakehouse` job's MinIO/Lakekeeper/Postgres are all throwaway,
  job-local containers torn down at the end of the job — no persistent CI
  state to grow over time.

### 4.5 Isolated dependency image/venv in CI

Add `lakehouse/requirements.txt` (`pyspark`, `mlflow`, and, only if A2b
ships, `pyiceberg[s3fs]` — no `[sql]` extra needed anymore since PyIceberg
only talks REST here) and a dedicated `lakehouse/Dockerfile` with the pinned
JDK + Iceberg-Spark-runtime + Hadoop-AWS jars, used both for the
`lakehouse-worker` container image and for the CI job's runner-side install.
This is never merged into the shared FastAPI-services `pip install`,
mirroring the "isolated prod image ⇒ isolated CI dependency surface"
convention already used for Airflow. For the CI job specifically, use
`actions/cache` on the Spark/Iceberg jar directory to avoid re-downloading
tens/hundreds of MB on every run.

---

## 5. Operational safety

The governing lesson from Plan 120/123: **do not run heavy analytical jobs
inside always-on production services** (the snapshot-worker CPU/OOM incident
and the dbt OOM incident). Gate A/B honor this, and the shift to
Spark-as-primary makes the memory/isolation story *more* important, not less.
This section also documents, explicitly, the Compose-project isolation the
review flagged as a real production risk in the prior draft.

- **Never run `docker compose --profile <anything> down -v` (or plain
  `docker compose down -v`) against the main `docker-compose.yml` on the
  VM.** The main file declares `cartracker_pgdata` (production Postgres,
  `external: true` so `down -v` cannot delete it, but `down` with no service
  arguments still **stops every service in the project** — `postgres`,
  `minio`, `scraper`, `ops`, `dashboard`, `airflow-*`, `grafana`, `caddy`,
  everything) plus several non-external volumes for other services. A
  `--profile lakehouse` flag on that command does **not** scope `down` to
  only the profile's services; it only controls which profile-gated services
  additionally participate. This is why Gate A infrastructure is a
  **standalone Compose file/project** (`docker-compose.lakehouse.yml`,
  project `cartracker-lakehouse`) instead of a profile inside the main file
  — see the Compose topology decision at the top of this document. Every
  command below targets that separate file/project explicitly.

- **Both Lakekeeper and the one-shot Spark worker run only via
  `docker-compose.lakehouse.yml`, under project name `cartracker-lakehouse`,
  entirely separate from the main Compose project.** Lakekeeper + its
  isolated Postgres are lightweight enough to stay up for the duration of a
  Gate A working session; the PySpark job itself runs as a **profile-gated,
  one-shot `lakehouse-worker` container within that same standalone file**, a
  direct copy of the `snapshot-worker` pattern (profile-gated, no ports, no
  restart policy, reuses a build context):

  ```yaml
  # (excerpt of docker-compose.lakehouse.yml — full listing in §2.2)
    lakehouse-worker:
      build: { context: ., dockerfile: lakehouse/Dockerfile }
      image: cartracker-lakehouse
      profiles: ["lakehouse-worker"]        # never starts on a bare `up`, even within this file
      mem_limit: 6g                         # bumped vs. the pure-PyIceberg plan: JVM + Spark driver/executor overhead is real
      environment:
        LAKEKEEPER_CATALOG_URI: http://lakekeeper:8181/catalog
        ICEBERG_WAREHOUSE_NAME: cartracker_experiments
        MINIO_ENDPOINT: http://minio:9000
        MINIO_ROOT_USER: ${MINIO_ROOT_USER}
        MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
        DBT_RUNNER_URL: http://dbt_runner:8080   # reuse the Plan 123 build-in-progress guard
        DUCKDB_PATH: /data/analytics/analytics.duckdb
      volumes:
        - analytics_db:/data/analytics:ro         # READ-ONLY mount — cannot mutate the analytics DB
      networks: [cartracker-net]
      depends_on:
        lakekeeper: { condition: service_started }
  ```

  Note the **read-only** `analytics_db` mount — the container physically
  cannot open a write connection to the production DuckDB file. That is the
  strongest form of the §2.7 cleanup guarantee, unchanged from the prior
  pass.

- **No production Postgres load of any kind from Gate A.** This revision
  removes the prior pass's largest operational-safety caveat (a new schema
  and connections against the production Postgres instance during an active
  spike). Lakekeeper's catalog reads/writes land entirely on
  `lakekeeper-postgres`, a separate container in a separate Compose project
  the production database has no visibility into and that a broad teardown
  of that project can never reach.

- **Mutual exclusion:** before a real (VM) Iceberg write, the worker calls
  `dbt_runner`'s `/ready` (reuse
  `archiver.processors.export_ci_lake_snapshot._check_dbt_runner_not_building`)
  and aborts if a heavy dbt build is in progress — same guard Plan 123
  Phase 0 added for `snapshot-worker`. Do not run `lakehouse-worker`,
  `snapshot-worker`, and a full dbt build concurrently on the VM.

- **MLflow server** is always-on but light: `mem_limit: 1g`, internal-only,
  no Caddy exposure in the first cut. It never appears in the hot
  scrape/claim path. This part lives in the **main** `docker-compose.yml`
  (by design, D3) and is entirely unaffected by the Gate A Compose-isolation
  change.

- **Safe VM commands** — all target the standalone `cartracker-lakehouse`
  project explicitly; none ever omit `-f docker-compose.lakehouse.yml`:
  ```bash
  # Bring up the catalog (idempotent; safe to leave running during a working session)
  docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
    up -d lakekeeper lakekeeper-postgres

  # A3 real-table rehearsal (isolated, read-only source, cleanup at end)
  docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
    run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse \
    write --source int_listing_volatility_features --limit 250000
  docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
    run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse info
  docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
    run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse --cleanup

  # Targeted cleanup: stop the catalog containers without deleting their
  # metadata volume (useful mid-session, e.g. to free memory between runs) —
  # the lighter-weight alternative to a full teardown.
  docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
    stop lakekeeper lakekeeper-postgres

  # Full teardown, including Lakekeeper's own metadata volume. Safe to run
  # with -v HERE specifically because docker-compose.lakehouse.yml is a
  # standalone file/project that declares no production service or volume
  # (unlike the main docker-compose.yml, which owns cartracker_pgdata and
  # every production service). NEVER run `docker compose down -v` (or
  # `--profile <name> down -v`) against the main docker-compose.yml on this
  # VM — see the callout at the top of this section.
  docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
    down -v
  ```
  New files reach the VM only via **git commit → push → pull**, and the
  image is rebuilt (`docker compose -f docker-compose.lakehouse.yml build
  lakehouse-worker`) because new files are not in cached layers — per the
  standing deploy conventions. No `scp`, no direct copy, no production
  restart without explicit confirmation.

---

## 6. Implementation details

### 6.1 Files to add / change

| File | PR | Change |
|------|----|--------|
| `docker-compose.lakehouse.yml` | A1 | new standalone Compose file: `lakekeeper` + `lakekeeper-postgres`; own named volume `lakekeeper_pgdata`; joins the existing external `cartracker-net`. **As actually implemented, `lakehouse-worker` is NOT included in A1** — it references a `lakehouse/Dockerfile` build context that doesn't exist until A2, so it is added to this file in A2 instead (see implementation note above) |
| `docker-compose.lakehouse.ci.yml` | A1 | CI-only override: job-local throwaway `minio`, network made non-external, distinct host ports (`19000`, `18181`) |
| `docs/runbook_lakehouse.md` | A1, extended in B3 | bring-up/teardown commands, isolation invariants, explicit "never `down -v` the main file" warning, "never touches production Postgres" note, `analytics_db` external-volume-name check |
| `.github/workflows/ci.yml` | A1 | new independent `lakehouse` job (§4.2): Lakekeeper management `/management/v1/info` smoke step (Iceberg REST `/v1/config` + namespace CRUD deferred to A2) |
| `lakehouse/requirements.txt` | A2 | `pyspark`, pinned |
| `lakehouse/Dockerfile` | A2 | JDK + pinned `pyspark` + `iceberg-spark-runtime` + `hadoop-aws` jars |
| `scripts/spike_iceberg_lakehouse.py` | A2 | PySpark write/read/append/time-travel/info/cleanup subcommands; namespace guard; metadata capture |
| `shared/iceberg_catalog.py` | A2 | `spark_conf_for_rest_catalog()` helper (env-driven), mirrors `shared/duckdb_s3.py`'s config pattern |
| `tests/lakehouse/test_lakekeeper_config.py` | A1 | unit |
| `tests/lakehouse/test_iceberg_spike_metadata.py` | A2 | unit |
| `tests/integration/lakehouse/test_lakekeeper_smoke.py` | A1 | integration (`lakehouse` CI job) |
| `tests/integration/lakehouse/test_pyspark_iceberg_roundtrip.py` | A2 | integration (`lakehouse` CI job, budget-gated) |
| `.github/workflows/ci.yml` | A2 | extend the `lakehouse` job: Spark/Iceberg jar caching + round-trip step |
| `scripts/validate_iceberg_pyiceberg.py` | A2b (optional) | PyIceberg `RestCatalog` read-back validation |
| `tests/integration/lakehouse/test_pyiceberg_validation.py` | A2b (optional) | integration (`lakehouse` CI job) |
| `scripts/register_iceberg_tables.py` | A3 | real `int_listing_volatility_features` snapshot + before/after cleanup proof |
| `docs/lakehouse_substrate_decision.md` | A3, B3 | fill Gate A results placeholder; correct bucket name + catalog choice (see Gate 0 fix note) |
| `docs/adaptive_refresh_feature_audit.md` | B3 | mark sampled VIN review done (Gate 0 close-out) |
| `db/migrations/V0NN__mlflow_role.sql` | B1 | `mlflow_user` + `mlflow` schema (the **only** Flyway migration in this whole plan; the **only** production-Postgres touch) |
| `docker-compose.yml` | B1 | add `mlflow` service (main file — by design, unaffected by the Gate A isolation change) |
| `mlflow/Dockerfile` | B1 | mlflow server image |
| `mlflow/requirements.txt` | B1 | `mlflow`, `psycopg2-binary`, `boto3` |
| `shared/mlflow_iceberg.py` | B2 | Iceberg→MLflow provenance bridge (engine-agnostic dict input) |
| `scripts/mlflow_smoke_run.py` | B2 | baseline smoke experiment |
| `tests/lakehouse/test_mlflow_iceberg_bridge.py` | B2 | unit |
| `tests/integration/lakehouse/test_mlflow_smoke_run.py` | B2 | integration (ephemeral store, unrelated to the `lakehouse` job) |

### 6.2 Python / package dependencies

- **PySpark (primary, A2):** `pyspark` pinned to a version with a published
  matching `iceberg-spark-runtime` artifact (e.g. Spark 3.5.x ⇄
  `iceberg-spark-runtime-3.5_2.12`), plus `hadoop-aws` for S3A access to
  MinIO. Requires a JDK in the `lakehouse` image — the first JVM dependency
  this repo has needed outside the existing Airflow/Flyway images.
- **Lakekeeper (A1):** no Python dependency at all — it is a standalone
  server image; only `requests`/`httpx` (already available) is needed for
  the management info smoke test (A1) and the later warehouse-registration
  script + Iceberg REST config/namespace CRUD checks (A2).
- **PyIceberg (optional, A2b only):** `pyiceberg[s3fs]` — no `[sql]` extra
  needed anymore, since PyIceberg only ever talks the REST catalog protocol
  here, not a SQLAlchemy-backed catalog.
- **MLflow:** `mlflow` + `psycopg2-binary` (backend) + `boto3` (already a
  repo dep) for the artifact store.
- All pinned in per-image requirements files; installed in an isolated
  `lakehouse` image, never merged into the shared `pip install`.

### 6.3 Environment variables (new)

| Var | Consumed by | Example |
|-----|-------------|---------|
| `LAKEKEEPER_DB_PASSWORD` | `lakekeeper-postgres` + `lakekeeper`, in `docker-compose.lakehouse.yml` only | secret, isolated to the standalone lakehouse project |
| `LAKEKEEPER_PG_ENCRYPTION_KEY` | `lakekeeper` service | secret |
| `LAKEKEEPER_CATALOG_URI` | Spark conf, PyIceberg validation script | `http://lakekeeper:8181/catalog` (VM/local, container-DNS) or `http://localhost:18181/catalog` (CI, host-published port) |
| `ICEBERG_WAREHOUSE_NAME` | spike script, worker | `cartracker_experiments` |
| `MLFLOW_DB_PASSWORD` | mlflow service + Flyway placeholder | secret |
| `MLFLOW_TRACKING_URI` | experiment scripts / CI | `http://mlflow:5000` (VM) / file store (CI) |

Existing `MINIO_ENDPOINT`/`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`/
`DUCKDB_PATH` are reused unchanged on the VM/local. Notably, **no new
production Postgres env var** is introduced for Gate A —
`LAKEKEEPER_DB_PASSWORD` only ever reaches `lakekeeper-postgres`, a container
in a separate Compose project with no relationship to the production
`postgres` service or its `${POSTGRES_PASSWORD}`.

### 6.4 Docker Compose changes

- **A1:** new standalone file `docker-compose.lakehouse.yml`
  (`lakekeeper` + `lakekeeper-postgres`, isolated named volume
  `lakekeeper_pgdata`, joins the existing external `cartracker-net`) and
  `docker-compose.lakehouse.ci.yml` (CI-only override). **No change to the
  main `docker-compose.yml`.**
- **A2:** add `lakehouse-worker` to `docker-compose.lakehouse.yml`
  (profile-gated within that file, read-only `analytics_db` mount,
  `mem_limit: 6g`, `DBT_RUNNER_URL` guard) — §5. Still no change to the main
  file.
- **B1:** add `mlflow` service (§3.1) to the **main** `docker-compose.yml`
  and one new Flyway placeholder (`mlflowPassword`) in the `flyway` service
  command block there — this is the one Gate A/B change that does touch the
  main file, by design.
- No changes to any existing service's runtime config anywhere. New files
  require `docker compose -f docker-compose.lakehouse.yml build
  lakehouse-worker` (Gate A) or `docker compose build mlflow` (Gate B) on the
  VM before they take effect.

### 6.5 Scripts / modules to scaffold

- `scripts/spike_iceberg_lakehouse.py` (A2): the Gate A workhorse, now a
  PySpark job.
- `shared/iceberg_catalog.py` (A2): Spark-conf builder for the REST catalog,
  the Iceberg analogue of `shared/duckdb_s3.py`.
- `scripts/validate_iceberg_pyiceberg.py` (A2b, optional): PyIceberg
  cross-engine read-back check.
- `scripts/register_iceberg_tables.py` (A3): real-table snapshot + cleanup
  proof.
- `shared/mlflow_iceberg.py` + `scripts/mlflow_smoke_run.py` (B2).

### 6.6 Docs / runbooks

- `docs/lakehouse_substrate_decision.md`: replace the Gate A placeholder with
  real spike output (A3) and correct the bucket/catalog specifics (see Gate 0
  fix note).
- `docs/adaptive_refresh_feature_audit.md`: check off the sampled VIN review
  (Gate 0 close-out) once done.
- `docs/runbook_lakehouse.md` (new, started in A1, extended through B3): VM
  invocation, mutual-exclusion rules, `docker-compose.lakehouse.yml`
  bring-up/teardown, rollback/drop procedure, the `analytics_db`
  external-volume-name check, "never in the hot path" and "never run
  `down -v` against the main `docker-compose.yml`" reminders.

---

## 7. Open questions and decisions

D1–D3 are settled (top of doc), as is the Compose topology (its own section
above). The following sub-decisions still want a call before or during
implementation; each has a recommendation.

| # | Question | Recommendation |
|---|----------|----------------|
| Q1 | Iceberg **format version v2 vs v3**? | **v2** — matches the substrate doc and Plan 117; both Spark's Iceberg runtime and PyIceberg default to/fully support v2, and every engine reads it. Revisit only if a later tool forces v3. |
| Q2 | Does the **PySpark fixture round-trip (A2) run in the dedicated `lakehouse` CI job**, or is it VM/local-manual only from the start? | **Attempt in the `lakehouse` job first** with a bounded runtime budget and jar caching (§4.3); fall back to VM/local-manual only if it proves too slow/flaky in practice. This is the single biggest open call this revision introduces versus the prior (all-CI) pass, since Spark is real new CI weight. The job topology itself (dedicated job, self-contained Compose stack) is now settled — only the go/no-go on running the heavy Spark step there remains open. **Wants Andrew's call**, ideally after seeing A1's job runtime land as a baseline. |
| Q3 | Should MLflow be **exposed behind Caddy** (`/mlflow`) now, or stay internal-only? | **Internal-only first** (plan says so); add a Caddy route + auth in a Gate B follow-up once we decide between oauth2-proxy (like pgadmin) and MLflow basic auth. Unaffected by the Gate A change. |
| Q4 | Does the **MLflow server run in CI**, or only field-logging against an ephemeral store? | **Ephemeral store in CI** (§4.2); server validated on VM + by `docker-compose build`. Unaffected by the Gate A change. |
| Q5 | First real table in A3: **full 250k-row `int_listing_volatility_features`** or a bounded slice? | **Full table** — it is small (one row per VIN, ~250k rows) and is the genuine Gate C rehearsal; the worker's `mem_limit` and read-only mount bound the risk. Unaffected by the Gate A change. |
| Q6 | Which **Lakekeeper image/version** to pin, and does it need an auth token even for an isolated single-tenant spike? | **Recommend pinning the latest tagged release** (not `:latest-main`) once implementation starts, and using Lakekeeper's simplest unauthenticated/single-token mode for Gate A — full auth/RBAC is explicitly Plan 119 scope. Needs a quick version check against Lakekeeper's release notes at implementation time, since the project is still evolving quickly. |
| Q7 | Password/secret sourcing for `LAKEKEEPER_DB_PASSWORD` / `LAKEKEEPER_PG_ENCRYPTION_KEY` / `MLFLOW_DB_PASSWORD`? | **Mint dedicated secrets** per the least-privilege convention (every service already has its own credentials). |
| Q8 | What is the exact resolved name of the `analytics_db` volume the main project creates (needed for `docker-compose.lakehouse.yml`'s `external: true` reference, §2.2)? | **Confirm via `docker volume ls` on the VM at A2/A3 implementation time** and record the exact name in `docs/runbook_lakehouse.md`; do not guess/hardcode a project-prefix assumption into the compose file ahead of that check. |

---

## Separate: tiny Gate 0 / current-PR fixes (do NOT fold into Gate A/B)

Called out separately per the task instruction. These are corrections to the
already-written Gate 0 docs, not Gate A/B work:

1. **Bucket-name error in `docs/lakehouse_substrate_decision.md`.** The MinIO
   path convention section writes `s3a://cartracker/lakehouse_spike/iceberg/...`,
   but the real bucket is **`bronze`** (`MINIO_BUCKET` throughout
   `docker-compose.yml`). Correct to
   `s3://bronze/lakehouse_spike/warehouse/...` (Lakekeeper/PyIceberg view) /
   `s3a://bronze/lakehouse_spike/warehouse/...` (Spark view of the same
   objects). One-line doc fix; belongs in the current Gate 0 PR.
2. **Catalog choice wording.** The substrate doc says "Hadoop/file catalog
   first." Under the now-approved D1/D2, that should read **"minimal
   Lakekeeper REST catalog first, deployed via a standalone Compose
   file/project (never a profile inside the main `docker-compose.yml`,
   and never the production Postgres instance), with PySpark as the
   primary write/read engine and PyIceberg as an optional secondary
   validation client against the same catalog"**, with a one-line rationale
   (one REST catalog implementation serves both engines with zero per-engine
   adapter code; a fully separate Compose project makes the isolation
   airtight against broad teardown commands, not just conventional). This
   can be a small amendment in the current Gate 0 PR or the first line of
   the A1 doc update — Andrew's preference.
3. **Gate 0 checklist close-out.** Once the sampled manual VIN/listing review
   is done, tick it in both `docs/plan_112_refresh_policy_backtesting.md`
   (Gate 0 list) and `docs/adaptive_refresh_feature_audit.md` (deferred-checks
   section). No code.

These three are intentionally kept out of the Gate A/B PRs so the substrate
decision is internally consistent *before* the first spike PR references it.
