# Plan 112: Iceberg + MLflow Adaptive Refresh Backtesting

## Status

**Paused after foundation/proof work.**

Plan 112 proved the open-lakehouse substrate we wanted to learn from:

- Lakekeeper REST catalog can run in an isolated Compose project.
- Spark/PySpark can write, append, read, time-travel, and clean up Iceberg
  tables on MinIO.
- A real dbt/DuckDB feature table (`int_listing_volatility_features`) can be
  exported to Iceberg with row-count/grain validation.
- A local rehearsal can download a Plan 120 snapshot through the ops API, seed
  local MinIO, build a targeted local DuckDB, publish to Iceberg, and clean up.
- MLflow can record dataset/table provenance for the Iceberg snapshot.

That is enough for Plan 112's original foundation goal. The plan is paused
before deeper MLflow/backtesting/model work because the better next move is to
make Iceberg a real analytics contract instead of a sidecar export. That work
is split into **Plan 125: DuckDB to Iceberg Analytics Migration**.

Plan 112 should resume only after Plan 125 establishes how dbt/Spark/Iceberg
own analytical tables.

## Goal

Build the reproducible experiment layer for adaptive detail refresh on an open,
portable lakehouse foundation:

- Apache Iceberg tables on object storage.
- Spark/PySpark for writes and future feature/model preparation.
- MLflow for experiment tracking and dataset/model provenance.
- Lakekeeper REST catalog as the local/open catalog proof, with deeper
  governance deferred to Plan 119.
- Plan 120 snapshots as the reproducible local/CI substrate.

Plan 112 does **not** change production scraping or claim logic. Production
integration belongs to Plan 113.

## Current Architecture

```text
Plan 120 snapshot archive
        |
        v
local or VM MinIO seed
        |
        v
targeted dbt/DuckDB build
        |
        v
lakehouse-worker / PySpark
        |
        v
Iceberg table in Lakekeeper + MinIO
        |
        v
MLflow provenance run
```

This proved the mechanics, but DuckDB/dbt still owns the real analytics
contract. Plan 125 exists to close that gap.

## Consolidated Decisions

| Decision | Current call |
|---|---|
| Table format | Iceberg v2 for the first implementation. Revisit v3 only when a tool or platform requires it. |
| Primary engine | Spark/PySpark for Iceberg writes and validation. |
| Optional engine | PyIceberg remains optional validation, not the primary writer. |
| Catalog | Lakekeeper REST catalog for local/VM proof. |
| Catalog metadata store | Isolated `lakekeeper-postgres` during Plan 112; no production Postgres coupling. |
| Object store | MinIO bucket `bronze`, isolated `lakehouse_spike/warehouse/` prefix. |
| Governance | RBAC/multi-tenant catalog policy deferred to Plan 119. |
| MLflow service | Standalone experimental service for provenance smoke; production-ish always-on service deferred. |
| Runtime images | Plan 112 runtimes are consolidated into `lakehouse/Dockerfile` with separate targets. |

## Completed Work

### Gate 0: Feature and Substrate Preflight

Completed:

- Read-only audit script: `scripts/audit_adaptive_refresh_features.py`.
- VM audit of current adaptive-refresh feature outputs.
- Documented feature grains, required fields, freshness, duplicates, source
  coverage, and known caveats.
- Confirmed `int_listing_volatility_features` as the first real Iceberg export
  candidate.

Outstanding but non-blocking:

- Sampled manual VIN/listing review remains useful before serious policy
  backtesting, but it no longer blocks the infrastructure proof.

### Gate A: Iceberg + Catalog Foundation

Completed:

- `docker-compose.lakehouse.yml`: isolated Lakekeeper + Lakekeeper Postgres.
- `docker-compose.lakehouse.ci.yml`: CI-only throwaway MinIO/network override.
- `lakehouse/Dockerfile` target `lakehouse-worker`: PySpark/Iceberg runtime.
- `scripts/register_lakehouse_warehouse.py`: idempotent warehouse registration.
- `scripts/spike_iceberg_lakehouse.py`: synthetic fixture write/read/append
  and cleanup proof.
- `scripts/export_volatility_features_to_iceberg.py`: VM/local real-table
  export/info/cleanup for `int_listing_volatility_features`.
- `docker-compose.lakehouse.a3.yml`: VM/local read-only analytics DuckDB mount.
- `docker-compose.lakehouse.local.yml`: isolated local Lakekeeper + MinIO flow.
- `scripts/preflight_local_lakehouse_snapshot.py`: local readiness checks.
- `scripts/run_local_lakehouse_rehearsal.py`: one-command local A4 rehearsal.

Verified:

- A2 synthetic Iceberg round-trip in CI and on the VM.
- A3 real `int_listing_volatility_features` export on the VM: 250,790 rows,
  exact source/Iceberg row-count match, cleanup removed table/data.
- A4 local rehearsal against a Plan 120 snapshot.

### Gate B: MLflow Provenance Smoke

Completed:

- `shared/mlflow_provenance.py`: pure payload construction/validation.
- `scripts/log_lakehouse_experiment_provenance.py`: CLI to log one dataset
  provenance run.
- `docker-compose.mlflow.yml`: standalone experimental MLflow service.
- `lakehouse/Dockerfile` target `mlflow`: MLflow server/provenance client image.
- VM smoke logged an `adaptive_refresh_provenance` run with Plan 120 archive
  metadata and Iceberg table metadata.

Deferred:

- Production-style MLflow backend in Postgres.
- Flyway migration/user/schema for MLflow.
- Caddy `/mlflow` route.
- Real backtest/model runs.

## Operational Guide

### Required Environment

Lakekeeper:

- `LAKEKEEPER_DB_PASSWORD`
- `LAKEKEEPER_PG_ENCRYPTION_KEY`

Local MinIO defaults in the local/CI overrides:

- `MINIO_ROOT_USER=cartracker`
- `MINIO_ROOT_PASSWORD=cartracker123`

Snapshot download:

- `CARTRACKER_SNAPSHOT_TOKEN` or `--token`, sourced from
  `SNAPSHOT_DOWNLOAD_TOKEN`.

Use URL-safe values for Lakekeeper secrets. Avoid raw `@`, `/`, `:`, `?`, `#`,
and whitespace because the password is interpolated into a Postgres URL.

### Lakekeeper Stack

Bring up the VM/local catalog:

```bash
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  up -d lakekeeper-postgres lakekeeper
```

Check status:

```bash
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse ps
docker run --rm --network cartracker-net curlimages/curl:8.10.1 \
  -fsSL http://lakekeeper:8181/management/v1/info
```

Safe teardown for the lakehouse project only:

```bash
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse down
```

Use `down -v` only against standalone lakehouse projects, never against the
main `docker-compose.yml`.

### Synthetic Iceberg Round Trip

```bash
docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  build lakehouse-worker

docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.register_lakehouse_warehouse

docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \
  run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse roundtrip
```

### VM Real-Table Export

Uses the VM-only read-only analytics volume override:

```bash
LH="docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.a3.yml -p cartracker-lakehouse"

$LH run --rm lakehouse-worker python -m scripts.export_volatility_features_to_iceberg export
$LH run --rm lakehouse-worker python -m scripts.export_volatility_features_to_iceberg info
$LH run --rm lakehouse-worker python -m scripts.export_volatility_features_to_iceberg cleanup
```

### Local One-Command Rehearsal

From a local checkout with Docker running and `CARTRACKER_SNAPSHOT_TOKEN` set:

```bash
python -m scripts.run_local_lakehouse_rehearsal --refresh-seed-data
```

Useful flags:

- `--keep-iceberg-table`: leave the Iceberg table behind for inspection.
- `--force-dbt`: rebuild `.cache/analytics/analytics.duckdb`.
- `--reseed-only`: refresh the local MinIO seed without running Spark.

The runner intentionally does not tear down the local stack. Clean up manually:

```bash
docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
  -p local-lakehouse down
```

### MLflow Provenance Smoke

File-store smoke:

```bash
python -m scripts.log_lakehouse_experiment_provenance \
  --manifest .cache/lake_snapshots/<snapshot_id>/manifest.json \
  --iceberg-info-json /tmp/iceberg_info.json \
  --feature-table-name int_listing_volatility_features \
  --tracking-uri file:./.cache/mlruns
```

Standalone server smoke on VM/main-stack environment:

```bash
docker compose -f docker-compose.mlflow.yml -p cartracker-mlflow build mlflow
docker compose -f docker-compose.mlflow.yml -p cartracker-mlflow up -d

docker cp /tmp/archive_manifest.json cartracker-mlflow:/tmp/archive_manifest.json
docker cp /tmp/iceberg_info.json cartracker-mlflow:/tmp/iceberg_info.json

docker exec -i cartracker-mlflow python -m scripts.log_lakehouse_experiment_provenance \
  --manifest /tmp/archive_manifest.json \
  --iceberg-info-json /tmp/iceberg_info.json \
  --feature-table-name int_listing_volatility_features \
  --env vm \
  --tracking-uri http://localhost:5000
```

UI is exposed locally as `http://localhost:15000` for the standalone project.
Caddy `/mlflow` exposure is deferred.

## Deferred Original Gates

The following remain valid, but are intentionally paused until after Plan 125:

### Gate C: Backtest Input Preparation

Prepare stable replay inputs keyed by:

```text
(policy_run_id, vin17, fetch_time)
```

Inputs should include detail fetch points, detail state runs, all-source
observation runs, volatility features, SRP/carousel recency, relisting signals,
and cooldown/403 signals.

### Gate D: Rule-Based Replay

Run an interpretable grid of refresh policies and measure fetch reduction,
change-detection delay, missed active periods, and estimated 403 reduction.

### Gate E: XGBoost Experiment

Train an experimental model such as `material_change_within_48h`, log metrics
and model artifacts to MLflow, and compare against the rule baseline.

### Gate F: Policy Artifact for Plan 113

Emit a pinned `policy_config.json` containing policy family/version, code SHA,
MLflow run ID, Iceberg snapshot metadata, selected thresholds or model URI, and
escape-hatch rules. Plan 113 owns production integration.

## Handoff to Plan 125

Plan 112 showed Iceberg works, but it did not make Iceberg the project’s
analytics contract. Before building more backtesting/modeling on top, Plan 125
should answer:

- Which dbt execution path replaces DuckDB?
- Which existing models can materialize directly to Iceberg?
- How do dashboards and scripts read Iceberg-backed analytics?
- What local/CI fixture path proves parity without depending on the VM?

Once Plan 125 has a stable Iceberg-native feature layer, resume Plan 112 at
Gate C with those tables as the replay input.
