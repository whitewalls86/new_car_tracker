# Plan 120: CI + Local Lake Snapshot Delivery

## Goal

Create a reproducible production-derived fixture snapshot system that CI and
local development can use for dbt, PySpark, Delta, MLflow, and adaptive-refresh
testing.

This plan breaks fixture export and delivery out of Plan 112. Plan 112 should
consume stable snapshots; it should not own the infrastructure for producing
and distributing them.

---

## Context

The current CI pipeline already starts Postgres and MinIO, applies migrations,
creates the `bronze` bucket, and seeds a minimal set of Parquet files so dbt can
compile. That proves the basic pattern:

```text
CI service MinIO + seeded data -> dbt/tests
```

Plan 112, Plan 118, and the upcoming Spark/Delta work need a richer version:

```text
production-derived coherent fixture snapshot
    -> downloadable archive
    -> seed CI MinIO/Postgres
    -> seed local dev MinIO/Postgres
    -> run dbt/Spark/Delta/ML tests
```

---

## Core Design

Use the API as a **file delivery/control plane**, not as a live export engine.

Good:

```text
GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download
    -> streams an existing snapshot archive
```

Avoid:

```text
GET /admin/snapshots/adaptive-refresh/export
    -> scans production data, builds an archive, and streams it synchronously
```

Snapshot creation should happen through an operator command, scheduled job, or
background task. CI and local dev should only download already-built immutable
artifacts.

---

## Snapshot Shape

Initial archive:

```text
adaptive-refresh-YYYY-MM-DD.tar.zst
```

Archive contents:

```text
manifest.json
silver_normalized/observations/...
ops_normalized/price_observation_events/...
ops_normalized/vin_to_listing_events/...
ops_normalized/blocked_cooldown_events/...
expected/
  feature_audit_summary.json
  known_vin_timelines.parquet
```

The snapshot should sample **coherent entity histories**, not random rows.

Recommended sampling:

- choose a bounded cohort of VINs/listings
- include all relevant rows for those entities
- include enough history to test state runs, price changes, relisting, and
  cooldown behavior
- include a mix of stable, volatile, new, stale, relisted, and blocked examples

---

## Manifest

Each snapshot must include `manifest.json`.

Required fields:

```json
{
  "snapshot_id": "adaptive-refresh-2026-07-07",
  "created_at": "2026-07-07T00:00:00Z",
  "source_window_start": "2026-01-01T00:00:00Z",
  "source_window_end": "2026-07-01T00:00:00Z",
  "vin_count": 500,
  "listing_count": 650,
  "archive_sha256": "...",
  "tables": {
    "silver_observations": {
      "rows": 123456,
      "path": "silver_normalized/observations",
      "sha256": "..."
    }
  }
}
```

The manifest is the contract. CI and local seed scripts should refuse to load a
snapshot when required checks fail.

---

## Phase 1: Exporter Script

Add:

```text
scripts/export_ci_lake_snapshot.py
```

Responsibilities:

1. Select a coherent VIN/listing cohort.
2. Read only required production-derived Parquet tables.
3. Filter rows for the cohort and source window.
4. Remove raw HTML and any unnecessary sensitive/internal fields.
5. Write the fixture directory.
6. Generate row counts and checksums.
7. Package `snapshot.tar.zst`.
8. Write `manifest.json`.
9. Upload to MinIO under a versioned prefix.
10. Update `latest.json`.

Suggested MinIO layout:

```text
ci_snapshots/adaptive_refresh/latest.json
ci_snapshots/adaptive_refresh/adaptive-refresh-YYYY-MM-DD/
  manifest.json
  snapshot.tar.zst
```

---

## Phase 2: API Delivery

Add a protected download surface, either in `ops` or a dedicated lightweight
FastAPI service.

Initial preference:

```text
ops container
```

Reason: existing admin auth, routing, and operational controls already live
there.

Routes:

```http
GET /admin/snapshots/adaptive-refresh/latest
GET /admin/snapshots/adaptive-refresh/{snapshot_id}
GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download
POST /admin/snapshots/adaptive-refresh
```

Route behavior:

- `GET latest`: returns metadata from `latest.json`
- `GET {snapshot_id}`: returns manifest metadata
- `GET {snapshot_id}/download`: streams existing `snapshot.tar.zst`
- `POST`: triggers or records an export request; it should not synchronously
  build and return the archive

Auth:

- admin auth for human use
- CI token or bearer token for GitHub Actions
- audit every download with snapshot ID, caller, timestamp, and size

---

## Phase 3: CI Seeder

Add:

```text
scripts/download_lake_snapshot.py
scripts/seed_lake_snapshot.py
```

CI flow:

```text
download snapshot archive
verify archive checksum
unpack snapshot
upload Parquet files to CI MinIO
optionally seed supporting Postgres rows
run dbt/Spark/Delta/ML tests
```

GitHub Actions should store the download token as a secret.

Do not make every PR depend on a large snapshot. Use tiers:

| Tier | Use |
|------|-----|
| tiny committed fixture | fast PR checks |
| medium downloaded snapshot | scheduled/manual or selected integration checks |
| full production corpus | VM/manual validation |

---

## Phase 4: Local Dev Seeder

Use the same downloader and seeder locally.

Target workflow:

```powershell
python scripts/download_lake_snapshot.py --latest
python scripts/seed_lake_snapshot.py --snapshot adaptive-refresh-2026-07-07
```

The local seeded environment should support:

- PySpark experiments
- Delta table creation
- MLflow smoke tests
- dbt migration tests
- adaptive-refresh replay tests

---

## Phase 5: Refresh Cadence

Initial cadence:

- manual export while Plan 112 is under active development
- then weekly or monthly scheduled export if useful

Snapshots are immutable. New snapshots create new IDs. `latest.json` can move,
but CI should be able to pin a specific snapshot ID for reproducibility.

---

## Testing

- Exporter produces coherent VIN histories across all included tables.
- Exporter excludes raw HTML and unwanted sensitive/internal fields.
- Manifest row counts match archive contents.
- Checksums are stable and verified before loading.
- API lists latest and named snapshots.
- API download streams an existing file without triggering export work.
- Unauthorized callers cannot download snapshots.
- CI seeder uploads expected files to MinIO.
- Local seeder can load the same snapshot.
- Plan 112 tests can run against the seeded fixture.

---

## Files Changed

| File | Change |
|------|--------|
| `scripts/export_ci_lake_snapshot.py` | New VM/export script |
| `scripts/download_lake_snapshot.py` | New downloader |
| `scripts/seed_lake_snapshot.py` | New CI/local seeder |
| `ops/routers/snapshots.py` | Snapshot metadata/download routes |
| `ops/app.py` | Include snapshot router |
| `.github/workflows/ci.yml` | Optional medium-snapshot integration job |
| `tests/ops/test_snapshot_downloads.py` | API auth/download tests |
| `tests/scripts/test_lake_snapshot_export.py` | Export/manifest tests |
| `tests/integration/test_lake_snapshot_seed.py` | Seeder integration test |

---

## Out Of Scope

- Live production export during CI.
- Full staging environment. This plan provides fixture data for CI/local dev;
  staging environment design should be a separate plan if pursued.
- Delta table creation itself. See Plan 112.
- dbt migration. See Plan 118.
- Governance expansion beyond download auth/audit. See Plan 119.
