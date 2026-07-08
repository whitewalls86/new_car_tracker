# Plan 120: CI + Local Lake Snapshot Delivery

## Goal

Create a reproducible production-derived fixture snapshot system that CI and
local development can use for dbt, PySpark, Delta, MLflow, and adaptive-refresh
testing.

Detailed implementation design:
[implementation_plan_120_ci_lake_snapshot_delivery.md](implementation_plan_120_ci_lake_snapshot_delivery.md)

This plan breaks fixture export and delivery out of Plan 112. Plan 112 should
consume stable snapshots; it should not own the infrastructure for producing
and distributing them.

---

## Current Status

Status as of 2026-07-07: **Gate B (selector readiness)** is complete. The
implementation is between **Implementation Step 2** and **Implementation
Step 4** in the detailed guide.

The phase labels below describe product areas, not commit gates. For execution
tracking, use the Step 1-11 checklist in
[implementation_plan_120_ci_lake_snapshot_delivery.md](implementation_plan_120_ci_lake_snapshot_delivery.md#implementation-sequence).

| Area | Status | Notes |
|------|--------|-------|
| Exporter skeleton | Mostly done | Request/result models, validation, tier defaults, CLI, dry-run, source audit, and selector diagnostics exist. Non-dry-run export still returns `not_implemented`. |
| Selector registry | Done (Gate B) | All 22 registered selectors are executable, derived entirely from the four supported source tables. `stable_state_run`/`state_change_run` reproduce the dbt fingerprint fields from `int_listing_state_fingerprints.sql` exactly; `detail_beats_srp`/`srp_fallback` mirror `int_latest_observation.sql`'s source-priority ranking. No selector remains a TODO placeholder. Coupling to dbt is guarded by a single CI test (`tests/integration/dbt/test_selector_dbt_equivalence.py`) that seeds fixture data (`scripts/seed_lake_snapshot_fixture.py`), runs the real dbt models, and diffs selector output against dbt's actual materialized tables — so drift in either the dbt SQL or the selector SQL fails CI. |
| DuckDB source reads | Done for audit/selector reads | Shared DuckDB/MinIO helper exists, source audit reads four lake tables, and local fixture mode exists for tests. |
| Cohort allocation and closure | Not started | No `SnapshotCohort`, deterministic fill, VIN/listing/artifact closure, or closure diagnostics yet. |
| Filtered Parquet writer | Not started | No fixture table materialization yet. |
| Manifest/package/upload | Not started | No `.tar.zst` generation, MinIO promotion, or `latest.json` update yet. |
| Archiver endpoint | Partial | Internal endpoint is wired and wrapped with `active_job()`, but it can only audit, dry-run, or return `not_implemented`. |
| Airflow DAG | Structurally done | Manual DAG exists and passes params/defaults. It cannot create a real snapshot until exporter Steps 4-6 exist. |
| Ops download API | Not started | Latest/manifest/download routes and CI token auth still need implementation. |
| Download/seed scripts | Mostly done | Offline/local mode exists and verifies checksums. API mode is scaffolded but depends on the ops download API. |
| CI pilot | Not started | Needs a real archive and ops download route first. |

Next gate: **Gate C - cohort allocation and closure** (Implementation Step 4).
All planned selectors are now executable, so the next step is turning selector
candidates into a coherent `SnapshotCohort` with seed/closed VINs, listing
IDs, artifact IDs, coverage diagnostics, and deterministic fill behavior.

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
Snapshot generation should run in the `archiver` container because it already
owns batch work over Postgres + MinIO and is already triggered by Airflow or
manual HTTP endpoints.

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

Recommended ownership:

| Layer | Responsibility |
|-------|----------------|
| `archiver` | Generate snapshots, run selectors, read production Parquet, write snapshot archives to MinIO. |
| Airflow | Schedule or manually trigger archiver snapshot generation. |
| `ops` API | Authenticated admin/CI facade for listing, requesting, auditing, and downloading snapshots. |
| CI/local scripts | Download, verify, unpack, and seed snapshots. |

This keeps the heavy data export job close to the existing Parquet maintenance
code while keeping public download/auth behavior in the service that already
owns admin access.

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

- choose a bounded cohort of VINs/listings using branch-derived selectors
- include all relevant rows for those entities
- include enough history to test state runs, price changes, relisting, and
  cooldown behavior
- include a mix of stable, volatile, new, stale, relisted, and blocked examples

The dataset is small enough that the first meaningful target should be larger
than a tiny hand-picked fixture. The constraint should be CI/runtime cost, not
fear of storage volume.

Recommended tiers:

| Tier | Target |
|------|--------|
| edge | 100 VINs or <= 50 MB compressed |
| ci | 2,500-5,000 VINs or <= 250 MB compressed |
| dev | 10,000-25,000 VINs or <= 1 GB compressed |
| full | all available history, VM/manual only |

The exporter should keep selecting VINs until it hits `target_vins`,
`max_archive_mb`, or `max_rows`, whichever comes first. Start with the `ci`
tier; if GitHub Actions runtime is acceptable, make that the default
downloaded snapshot for integration checks.

---

## Coverage Strategy

Use code-derived coverage selectors rather than manual data spelunking.

The key idea:

```text
dbt/PySpark branches and guards
    -> named coverage selectors
    -> SQL queries that find production VINs/listings matching each behavior
    -> manifest coverage assertions
```

The exporter should maintain a selector registry. Each selector names the
behavior it protects, the query used to find candidate entities, and the
minimum representation required in the snapshot.

Initial selectors:

| Selector | Why it matters |
|----------|----------------|
| `stable_state_run` | Exercises gaps-and-islands collapse where repeated identical fingerprints become one run. |
| `state_change_run` | Exercises fingerprint changes from price, mileage, dealer, or listing_state changes. |
| `relisted_vin` | Same VIN appears under multiple listing IDs; validates VIN/listing closure and listing_id-change features. |
| `active_to_unlisted` | Validates unlisted/delete handling and state transition features. |
| `price_drop` | Validates `lag(price)` price-drop behavior. |
| `price_increase` | Validates `lag(price)` price-increase behavior. |
| `price_changed_7d` | Validates adaptive-refresh short-window price-change features. |
| `price_changed_30d_only` | Validates 30-day window without also satisfying the 7-day case. |
| `no_price_history` | Validates left joins and null-safe feature/mart behavior. |
| `detail_beats_srp` | Validates latest-observation source priority: detail > srp > carousel. |
| `srp_fallback` | Validates usable vehicle attributes when detail rows are absent or incomplete. |
| `carousel_only_or_low_priority` | Validates carousel is present but loses priority to richer sources. |
| `invalid_or_null_vin` | Validates `stg_observations.vin17` filtering. |
| `benchmark_dense_make_model` | Ensures percentile/median benchmark groups have enough rows. |
| `benchmark_sparse_make_model` | Ensures sparse make/model groups do not disappear silently. |
| `cooldown_blocked` | Validates first 403 block events. |
| `cooldown_incremented` | Validates repeated 403 attempt events. |
| `cooldown_bucket_3_4` | Validates cooldown bucket boundaries. |
| `cooldown_bucket_5_10` | Validates cooldown bucket boundaries. |
| `cooldown_bucket_11_plus` | Validates high-attempt cooldown behavior. |
| `fresh_recent_listing` | Validates young/current active listings. |
| `stale_listing` | Validates old listings and stale recency features. |

This does not replace dbt unit tests. Unit tests protect exact branch behavior
with tiny synthetic examples. The snapshot protects real-world composition:
joins, nulls, duplicates, table closure, ordering, partition layout, and engine
differences across DuckDB/Spark/Delta.

---

## Cohort Closure

Selector queries produce seed entities. The exporter must then expand them into
a coherent entity closure before writing fixture Parquet.

Closure rules:

```text
seed VINs
  -> all listing_ids ever associated with those VINs
  -> previous_listing_ids from remap events
  -> artifact_ids tied to selected listing_ids
  -> price events tied to selected VINs/listing_ids
  -> blocked cooldown events tied to selected listing_ids
  -> observation rows tied to selected VINs/listing_ids/artifact_ids
```

Do not try to identify "the original production Parquet files for a VIN" and
copy them whole. The production files are partitioned for ingestion/storage, not
for entity-level fixture extraction. The exporter should read production
Parquet, filter logical rows, and write a new fixture layout.

The manifest should include both seed counts and closure counts:

```json
{
  "seed_vin_count": 5000,
  "closed_vin_count": 5062,
  "listing_count": 6425,
  "artifact_count": 182311
}
```

Closed VIN count may exceed seed VIN count because carousel rows, remaps, or
associated listing histories can reveal additional VINs needed for consistency.

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
  "tier": "ci",
  "seed_vin_count": 5000,
  "closed_vin_count": 5062,
  "listing_count": 6425,
  "archive_sha256": "...",
  "coverage": {
    "stable_state_run": {"entities": 500, "required": 25},
    "relisted_vin": {"entities": 50, "required": 10},
    "cooldown_bucket_11_plus": {"entities": 3, "required": 1}
  },
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

## Phase 1: Archiver Exporter

This is the broad exporter product area. In the implementation guide it is
split into Steps 1-7. As of 2026-07-07, only the skeleton, source audit,
initial selector diagnostics, and internal endpoint portions are implemented.
The real archive-producing path still depends on cohort closure, filtered
Parquet writing, manifest finalization, packaging, and upload.

Add:

```text
archiver/processors/export_ci_lake_snapshot.py
```

Responsibilities:

1. Parse tier and size limits.
2. Discover candidate entities for each selector.
3. Allocate the cohort across required edge cases and representative random
   coverage.
4. Expand candidate VINs/listings into a coherent closure.
5. Read only required production-derived Parquet tables.
6. Filter rows for the cohort and source window.
7. Remove raw HTML and any unnecessary sensitive/internal fields.
8. Write the fixture directory.
9. Generate row counts and checksums.
10. Generate coverage assertions.
11. Package `snapshot.tar.zst`.
12. Write `manifest.json`.
13. Upload to MinIO under a versioned prefix.
14. Update `latest.json`.

Suggested CLI:

```powershell
python -m archiver.processors.export_ci_lake_snapshot `
  --tier ci `
  --target-vins 5000 `
  --max-archive-mb 250 `
  --source-window-months 12 `
  --snapshot-id adaptive-refresh-2026-07-07
```

Suggested internal shape:

```text
Selector
  name
  min_entities
  max_entities
  query
  entity_key

CandidateSet
  selector_name
  vins
  listing_ids
  artifact_ids
  diagnostics

SnapshotCohort
  seed_vins
  closed_vins
  listing_ids
  artifact_ids
  coverage
```

First-pass implementation can live in one script with clear functions:

```text
load_sources()
build_selector_registry()
run_selectors()
allocate_cohort()
expand_entity_closure()
write_filtered_tables()
write_expected_outputs()
write_manifest()
package_archive()
upload_snapshot()
```

Later, if the script grows, move selector definitions into
`archiver/processors/lake_snapshot_selectors.py`.

Expose an internal archiver route:

```http
POST /snapshots/adaptive-refresh/run
```

The route should accept the same tier/limit inputs as the CLI and return a job
summary:

```json
{
  "snapshot_id": "adaptive-refresh-2026-07-07",
  "tier": "ci",
  "status": "created",
  "seed_vin_count": 5000,
  "closed_vin_count": 5062,
  "archive_bytes": 183447221,
  "manifest_key": "ci_snapshots/adaptive_refresh/adaptive-refresh-2026-07-07/manifest.json",
  "archive_key": "ci_snapshots/adaptive_refresh/adaptive-refresh-2026-07-07/snapshot.tar.zst"
}
```

The route is for internal use by Airflow and ops. It should not be exposed
directly through Caddy.

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
ops container for external/admin API
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
- `POST`: triggers or records an export request by calling the internal
  archiver route; it should not synchronously stream the archive

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
| edge committed or generated fixture | fast PR smoke checks |
| ci downloaded snapshot | default meaningful PR/integration checks if runtime allows |
| dev downloaded snapshot | local Spark/dbt/ML development |
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

## Phase 4.5: Airflow DAG

Add a paused DAG for snapshot generation:

```text
airflow/dags/export_ci_lake_snapshot.py
```

Initial behavior:

- manual trigger by default
- optional weekly/monthly schedule later
- POST to `http://archiver:8001/snapshots/adaptive-refresh/run`
- pass tier and size limits through DAG params
- log snapshot ID, archive key, manifest key, and coverage summary
- fail the DAG if required selector coverage is missing

Recommended defaults:

```json
{
  "tier": "ci",
  "target_vins": 5000,
  "max_archive_mb": 250,
  "source_window_months": 12
}
```

Airflow should create snapshots. CI should download existing snapshots.

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
| `archiver/processors/export_ci_lake_snapshot.py` | New snapshot export processor |
| `archiver/processors/lake_snapshot_selectors.py` | Optional selector registry if the processor grows |
| `archiver/app.py` | Internal snapshot generation route |
| `airflow/dags/export_ci_lake_snapshot.py` | Paused/manual snapshot generation DAG |
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
