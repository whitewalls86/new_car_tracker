# Implementation Plan 120: Archiver-Generated CI Lake Snapshots

## Objective

Implement Plan 120 as an archiver-owned snapshot generation system.

The system should create production-derived, coherent, sanitized fixture
archives that can be downloaded by CI and local development, then loaded into a
test MinIO/Postgres environment for dbt, Spark, Delta, MLflow, and
adaptive-refresh validation.

The important architectural decision is that this is no longer just a helper
script. Snapshot generation is a batch data product owned by `archiver`.

```text
Airflow/manual trigger
  -> archiver endpoint generates immutable snapshot archive
  -> MinIO stores manifest + archive
  -> ops/admin API lists and downloads snapshots
  -> CI/local scripts verify and seed fixture data
```

---

## Non-Goals

- Do not generate snapshots during CI.
- Do not expose the archiver generation endpoint through Caddy.
- Do not stream a live export from a public/admin download request.
- Do not copy raw production Parquet files wholesale.
- Do not include raw HTML, cookies, request headers, or other unnecessary
  scrape internals.
- Do not make Delta table creation part of this plan. Plan 112 consumes the
  seeded data.
- Do not solve staging environment deployment here. Plan 121 owns that.

---

## Current System Fit

`archiver` is the natural owner because it already:

- runs FastAPI internal maintenance endpoints
- is reachable from Airflow at `http://archiver:8001`
- has Postgres credentials
- has MinIO credentials
- has `pyarrow` and `s3fs`
- writes/compacts Parquet datasets
- uses the service drain/job-counter pattern through `active_job()`

Existing endpoint pattern:

```http
POST /flush/silver/run
POST /flush/staging/run
POST /compact/silver/run
POST /cleanup/parquet/run
```

New endpoint:

```http
POST /snapshots/adaptive-refresh/run
```

---

## High-Level Flow

```text
1. Caller triggers snapshot generation.
2. Archiver validates tier, limits, and requested snapshot ID.
3. Archiver builds selector candidates from production lake data.
4. Archiver allocates a seed cohort across required edge cases.
5. Archiver expands seed cohort into closed VIN/listing/artifact/event sets.
6. Archiver filters production Parquet into a temporary fixture directory.
7. Archiver writes expected/audit outputs.
8. Archiver writes manifest.json.
9. Archiver packages the fixture directory as snapshot.tar.zst.
10. Archiver uploads manifest and archive to MinIO.
11. Archiver updates latest.json only after successful upload and validation.
12. CI/local download the existing archive through the ops API.
```

---

## Storage Layout

Production snapshots should live in the existing `bronze` bucket under a
dedicated prefix:

```text
ci_snapshots/adaptive_refresh/latest.json
ci_snapshots/adaptive_refresh/adaptive-refresh-YYYY-MM-DD-HHMMSS/
  manifest.json
  snapshot.tar.zst
```

Inside the archive:

```text
manifest.json
silver_normalized/
  observations/
    source=detail/obs_year=2026/obs_month=7/part-000.parquet
    source=srp/obs_year=2026/obs_month=7/part-000.parquet
    source=carousel/obs_year=2026/obs_month=7/part-000.parquet
ops_normalized/
  price_observation_events/
    year=2026/month=7/part-000.parquet
  vin_to_listing_events/
    year=2026/month=7/part-000.parquet
  blocked_cooldown_events/
    year=2026/month=7/part-000.parquet
expected/
  feature_audit_summary.json
  known_vin_timelines.parquet
```

Use the same logical prefixes that dbt already reads so the seeder can upload
archive contents directly to a test `bronze` bucket without rewriting paths.

---

## Dependencies

Update `archiver/requirements.txt`:

```text
duckdb>=1.0
zstandard>=0.22
```

`duckdb` is recommended for the selector layer because the selectors are SQL
queries over Parquet and should look like the dbt logic they protect.

`zstandard` is needed to write `.tar.zst` archives from Python without relying
on host/container CLI tools.

Existing dependencies retained:

```text
pyarrow
s3fs
psycopg2-binary
fastapi
uvicorn
```

---

## Files

| File | Purpose |
|------|---------|
| `archiver/processors/export_ci_lake_snapshot.py` | Main generation processor and CLI entrypoint. |
| `archiver/processors/lake_snapshot_selectors.py` | Selector registry and SQL snippets. |
| `archiver/app.py` | Adds internal generation endpoint. |
| `airflow/dags/export_ci_lake_snapshot.py` | Manual/paused DAG to trigger archiver generation. |
| `ops/routers/snapshots.py` | Admin/CI list, manifest, trigger, and download routes. |
| `ops/app.py` | Includes snapshot router. |
| `scripts/download_lake_snapshot.py` | Downloads and verifies an archive. |
| `scripts/seed_lake_snapshot.py` | Uploads archive contents to local/CI MinIO. |
| `.github/workflows/ci.yml` | Adds optional downloaded-snapshot integration path. |
| `tests/archiver/test_export_ci_lake_snapshot.py` | Unit tests for selectors, manifest, packaging, limits. |
| `tests/archiver/test_app.py` | Endpoint smoke tests. |
| `tests/integration/airflow/test_dag_integrity.py` | DAG import/task coverage. |
| `tests/ops/test_snapshot_downloads.py` | Auth/download tests. |
| `tests/integration/test_lake_snapshot_seed.py` | Seeder integration coverage. |

---

## Archiver Endpoint

Add to `archiver/app.py`:

```http
POST /snapshots/adaptive-refresh/run
```

This endpoint is internal-only. It should be called by Airflow, docker exec, or
the ops service over the Docker network.

### Request

```json
{
  "tier": "ci",
  "snapshot_id": null,
  "target_vins": 5000,
  "max_archive_mb": 250,
  "max_rows": null,
  "source_window_months": 12,
  "source_window_start": null,
  "source_window_end": null,
  "min_selector_coverage": true,
  "dry_run": false
}
```

Validation:

- `tier` must be one of `edge`, `ci`, `dev`, `full`.
- `snapshot_id`, when provided, must match
  `adaptive-refresh-[A-Za-z0-9._-]+`.
- `target_vins`, `max_archive_mb`, and `max_rows` must be positive when set.
- `source_window_start` and `source_window_end` must either both be null or
  define a valid half-open window.
- `full` tier should require explicit `target_vins: null` or an explicit
  `allow_full: true` flag if implemented later.

Default tier limits:

| Tier | Default Target |
|------|----------------|
| `edge` | 100 VINs, 50 MB compressed |
| `ci` | 5,000 VINs, 250 MB compressed |
| `dev` | 25,000 VINs, 1 GB compressed |
| `full` | all history, no CI use |

### Response

```json
{
  "snapshot_id": "adaptive-refresh-2026-07-07-174500",
  "tier": "ci",
  "status": "created",
  "source_window_start": "2025-07-07T00:00:00Z",
  "source_window_end": "2026-07-07T00:00:00Z",
  "seed_vin_count": 5000,
  "closed_vin_count": 5062,
  "listing_count": 6425,
  "artifact_count": 182311,
  "archive_bytes": 183447221,
  "manifest_key": "ci_snapshots/adaptive_refresh/adaptive-refresh-2026-07-07-174500/manifest.json",
  "archive_key": "ci_snapshots/adaptive_refresh/adaptive-refresh-2026-07-07-174500/snapshot.tar.zst",
  "coverage_failures": []
}
```

### Runtime Mode

First implementation can be synchronous, matching existing archiver endpoints.
Airflow should call it with a large timeout, such as 30-60 minutes.

If generation grows slow enough to make synchronous HTTP brittle, add a second
phase with durable job state:

```text
POST /snapshots/adaptive-refresh/jobs
GET  /snapshots/adaptive-refresh/jobs/{job_id}
```

Do not start with an in-memory FastAPI background task. It is convenient but not
durable across container restarts.

---

## Processor Design

Module:

```text
archiver/processors/export_ci_lake_snapshot.py
```

Public entrypoints:

```python
def export_ci_lake_snapshot(request: SnapshotRequest) -> SnapshotResult:
    ...

def main() -> None:
    ...
```

Suggested data structures:

```python
@dataclass(frozen=True)
class SnapshotRequest:
    tier: str
    snapshot_id: str | None
    target_vins: int | None
    max_archive_mb: int | None
    max_rows: int | None
    source_window_start: datetime | None
    source_window_end: datetime | None
    source_window_months: int | None
    min_selector_coverage: bool
    dry_run: bool


@dataclass(frozen=True)
class Selector:
    name: str
    min_entities: int
    max_entities: int
    entity_key: str
    sql: str


@dataclass
class CandidateSet:
    selector_name: str
    vins: set[str]
    listing_ids: set[str]
    artifact_ids: set[int]
    diagnostics: dict[str, Any]


@dataclass
class SnapshotCohort:
    seed_vins: set[str]
    closed_vins: set[str]
    listing_ids: set[str]
    artifact_ids: set[int]
    coverage: dict[str, dict[str, int]]


@dataclass(frozen=True)
class SnapshotResult:
    snapshot_id: str
    tier: str
    status: str
    manifest_key: str | None
    archive_key: str | None
    archive_bytes: int | None
    coverage_failures: list[str]
```

Suggested function boundaries:

```text
resolve_request_defaults()
connect_duckdb()
configure_duckdb_s3()
build_selector_registry()
run_selectors()
allocate_cohort()
expand_entity_closure()
materialize_filtered_tables()
write_expected_outputs()
build_manifest()
validate_manifest()
package_tar_zst()
upload_snapshot()
update_latest_pointer()
cleanup_temp_dir()
```

---

## DuckDB Connection

Use DuckDB inside archiver for selection and filtering. Configure it to read
MinIO with credentials from existing env vars:

```text
MINIO_ENDPOINT=http://minio:9000
MINIO_ROOT_USER=...
MINIO_ROOT_PASSWORD=...
MINIO_BUCKET=bronze
```

DuckDB setup should:

- install/load `httpfs` if needed by the image
- set S3 endpoint to `minio:9000`
- set access key and secret key
- set `s3_use_ssl=false`
- set `s3_url_style='path'`

Example logical paths:

```text
s3://bronze/silver_normalized/observations/**/*.parquet
s3://bronze/ops_normalized/price_observation_events/**/*.parquet
s3://bronze/ops_normalized/vin_to_listing_events/**/*.parquet
s3://bronze/ops_normalized/blocked_cooldown_events/**/*.parquet
```

Use `hive_partitioning=true` and `union_by_name=true` where needed.

---

## Selector Registry

Module:

```text
archiver/processors/lake_snapshot_selectors.py
```

Selectors should find real production entities that exercise known branches.
They should return at least:

```text
selector_name
vin
listing_id
artifact_id
score
diagnostic fields
```

The first implementation can compute candidates from source Parquet directly.
Later, it can optionally use dbt-built intermediate outputs if those are
available in the lake.

Initial selectors:

| Selector | Source Logic |
|----------|--------------|
| `stable_state_run` | VINs with multiple detail observations where business-state fingerprint is unchanged. |
| `state_change_run` | VINs with multiple distinct business-state fingerprints. |
| `relisted_vin` | VINs with more than one listing ID or remap events with `previous_listing_id`. |
| `active_to_unlisted` | VIN/listing with active detail row and later unlisted/delete event. |
| `price_drop` | Consecutive price event where `price < prev_price`. |
| `price_increase` | Consecutive price event where `price > prev_price`. |
| `price_changed_7d` | Price change within seven days of source window end. |
| `price_changed_30d_only` | Price change within thirty days but outside seven days. |
| `no_price_history` | Observation VIN lacking matching positive price events. |
| `detail_beats_srp` | VIN with detail and SRP observations where detail should win latest-observation priority. |
| `srp_fallback` | VIN with usable SRP attributes and missing/incomplete detail attributes. |
| `carousel_only_or_low_priority` | VIN/listing represented by carousel observations. |
| `invalid_or_null_vin` | Rows with null or invalid VINs that should not become `vin17`. |
| `benchmark_dense_make_model` | Make/model groups with enough rows for percentile stability. |
| `benchmark_sparse_make_model` | Make/model groups with only a few rows. |
| `cooldown_blocked` | First blocked event. |
| `cooldown_incremented` | Repeated blocked attempt event. |
| `cooldown_bucket_3_4` | Latest attempts between 3 and 4. |
| `cooldown_bucket_5_10` | Latest attempts between 5 and 10. |
| `cooldown_bucket_11_plus` | Latest attempts >= 11. |
| `fresh_recent_listing` | Recent active listing. |
| `stale_listing` | Older listing or listing with stale SRP/detail recency. |

Minimum coverage defaults:

| Selector Family | Minimum |
|-----------------|---------|
| common source-priority/price/state selectors | 25 entities |
| rare relisting/unlisted/cooldown high-bucket selectors | 1-10 entities |
| invalid/null VIN row coverage | 25 rows |
| benchmark dense group | 3 make/model groups |
| benchmark sparse group | 3 make/model groups |

The manifest should record actual counts and required counts. Snapshot creation
should fail when `min_selector_coverage=true` and a required selector is short.

---

## Cohort Allocation

Allocation should be deterministic for reproducibility.

Inputs:

- selector candidate sets
- target VIN count
- max entities per selector
- source window
- random seed derived from snapshot ID

Process:

```text
1. Add required minimum entities for each selector.
2. Deduplicate by VIN/listing/artifact.
3. Fill remaining target with stratified random coverage.
4. Prefer VINs with richer histories when multiple candidates are equivalent.
5. Stop before max_vins/max_rows/max_archive_mb caps are exceeded.
```

Representative random fill should stratify by:

- source month
- make/model
- listing_state
- source type: detail, SRP, carousel
- active vs historical
- dealer/customer_id when available

The exporter should log both:

- candidate counts before allocation
- selected counts after allocation and closure

---

## Entity Closure

After seed VINs are selected, expand to a coherent dataset:

```text
seed VINs
  -> listing IDs from silver observations
  -> listing IDs from price events
  -> listing IDs from vin_to_listing_events
  -> previous_listing_id values from remap events
  -> artifact IDs tied to selected VINs/listings
  -> additional VINs tied to selected listing IDs
  -> blocked cooldown events for selected listing IDs
```

Closure should be iterative but bounded:

```text
repeat up to 3 passes:
  expand listings from VINs
  expand VINs from listings
  expand artifacts from VINs/listings
  expand previous listing IDs from remaps
stop when no sets change
```

The closure step should emit diagnostics:

```json
{
  "closure_passes": 2,
  "seed_vins": 5000,
  "closed_vins": 5062,
  "listing_ids": 6425,
  "artifact_ids": 182311,
  "previous_listing_ids_added": 74
}
```

---

## Materializing Tables

Write filtered Parquet tables into a temporary local directory first:

```text
/tmp/cartracker-snapshots/{snapshot_id}/work/
```

Then archive and upload.

Tables:

| Logical Table | Source | Filter |
|---------------|--------|--------|
| `silver_observations` | `silver_normalized/observations/**/*.parquet` | selected VINs OR selected listing IDs OR selected artifact IDs, plus source window |
| `price_observation_events` | `ops_normalized/price_observation_events/**/*.parquet` | selected VINs OR selected listing IDs OR selected artifact IDs, plus source window |
| `vin_to_listing_events` | `ops_normalized/vin_to_listing_events/**/*.parquet` | selected VINs OR selected listing IDs OR previous listing IDs, plus source window |
| `blocked_cooldown_events` | `ops_normalized/blocked_cooldown_events/**/*.parquet` | selected listing IDs, plus source window |

Partition output to match current dbt source expectations:

```text
silver_normalized/observations/source={source}/obs_year={year}/obs_month={month}/part-000.parquet
ops_normalized/{table}/year={year}/month={month}/part-000.parquet
```

Do not preserve production object names. The snapshot is a derived fixture with
its own file layout and checksums.

Suggested write options:

- Parquet compression: `zstd`
- stable ordering before write
- row groups sized for small fixture scans, not production throughput
- one or a few files per partition, depending on row count

Stable sort keys:

| Table | Sort |
|-------|------|
| `silver_observations` | `fetched_at`, `listing_id`, `artifact_id` |
| `price_observation_events` | `event_at`, `listing_id`, `artifact_id`, `event_id` |
| `vin_to_listing_events` | `event_at`, `listing_id`, `artifact_id`, `event_id` |
| `blocked_cooldown_events` | `event_at`, `listing_id`, `event_id` |

---

## Expected Outputs

The snapshot should include lightweight expected artifacts. These are not meant
to replace tests, but they help CI/local smoke checks verify that the fixture
loaded correctly.

Write:

```text
expected/feature_audit_summary.json
expected/known_vin_timelines.parquet
```

`feature_audit_summary.json`:

```json
{
  "selector_coverage": {
    "relisted_vin": {"required": 10, "entities": 42},
    "cooldown_bucket_11_plus": {"required": 1, "entities": 3}
  },
  "table_rows": {
    "silver_observations": 123456,
    "price_observation_events": 45678
  },
  "known_cases": {
    "relisted_vins": ["..."],
    "price_drop_vins": ["..."],
    "cooldown_listing_ids": ["..."]
  }
}
```

`known_vin_timelines.parquet`:

```text
vin
listing_id
case_label
first_seen_at
last_seen_at
price_event_count
state_run_count
listing_id_count
cooldown_event_count
```

---

## Manifest Contract

The manifest is the loading contract for CI/local seeders.

Required fields:

```json
{
  "snapshot_id": "adaptive-refresh-2026-07-07-174500",
  "created_at": "2026-07-07T17:45:00Z",
  "created_by": "archiver",
  "source": {
    "bucket": "bronze",
    "window_start": "2025-07-07T00:00:00Z",
    "window_end": "2026-07-07T00:00:00Z"
  },
  "tier": "ci",
  "limits": {
    "target_vins": 5000,
    "max_archive_mb": 250,
    "max_rows": null
  },
  "counts": {
    "seed_vins": 5000,
    "closed_vins": 5062,
    "listing_ids": 6425,
    "artifact_ids": 182311
  },
  "coverage": {
    "relisted_vin": {
      "required": 10,
      "entities": 42,
      "status": "pass"
    }
  },
  "tables": {
    "silver_observations": {
      "path": "silver_normalized/observations",
      "rows": 123456,
      "files": 12,
      "sha256": "..."
    }
  },
  "archive": {
    "path": "snapshot.tar.zst",
    "bytes": 183447221,
    "sha256": "..."
  },
  "generator": {
    "service": "archiver",
    "version": "git-sha-or-image-tag",
    "selector_version": 1
  }
}
```

Validation:

- table files exist
- table row counts are nonzero unless explicitly allowed
- table checksums match
- archive checksum matches
- coverage requirements pass
- archive size does not exceed tier limit unless `full`
- `latest.json` only points to a snapshot with a valid manifest and archive

---

## Packaging

Use Python `tarfile` plus `zstandard`.

Packaging steps:

```text
1. Write fixture directory.
2. Write manifest into fixture directory.
3. Compute table checksums.
4. Package fixture directory into snapshot.tar.zst.
5. Compute archive sha256 and bytes.
6. Update manifest with archive metadata.
7. Repackage or write manifest outside archive before final upload.
```

Recommended approach:

- write initial manifest
- package archive
- compute archive checksum
- write final manifest beside the archive in MinIO
- include initial manifest inside archive for local loading

It is acceptable for the in-archive manifest to omit the final archive checksum,
as long as the external MinIO `manifest.json` has the final archive checksum.
The downloader should verify against external manifest metadata.

---

## Upload and Atomicity

Upload to a temporary prefix first:

```text
ci_snapshots/adaptive_refresh/_tmp/{snapshot_id}/manifest.json
ci_snapshots/adaptive_refresh/_tmp/{snapshot_id}/snapshot.tar.zst
```

Then copy or move to final prefix:

```text
ci_snapshots/adaptive_refresh/{snapshot_id}/manifest.json
ci_snapshots/adaptive_refresh/{snapshot_id}/snapshot.tar.zst
```

Finally update:

```text
ci_snapshots/adaptive_refresh/latest.json
```

`latest.json` should be last. If generation fails, callers should still see the
previous good latest snapshot.

`latest.json`:

```json
{
  "snapshot_id": "adaptive-refresh-2026-07-07-174500",
  "manifest_key": "ci_snapshots/adaptive_refresh/adaptive-refresh-2026-07-07-174500/manifest.json",
  "archive_key": "ci_snapshots/adaptive_refresh/adaptive-refresh-2026-07-07-174500/snapshot.tar.zst",
  "created_at": "2026-07-07T17:45:00Z",
  "tier": "ci",
  "archive_sha256": "..."
}
```

---

## Ops API

Module:

```text
ops/routers/snapshots.py
```

Routes:

```http
GET  /admin/snapshots/adaptive-refresh/latest
GET  /admin/snapshots/adaptive-refresh/{snapshot_id}
GET  /admin/snapshots/adaptive-refresh/{snapshot_id}/download
POST /admin/snapshots/adaptive-refresh
```

Recommended behavior:

- `GET latest`: read and return `latest.json`.
- `GET {snapshot_id}`: read and return external `manifest.json`.
- `GET {snapshot_id}/download`: stream existing `snapshot.tar.zst`.
- `POST`: trigger generation.

For first implementation, prefer `POST` to trigger the Airflow DAG rather than
directly blocking on the archiver generation endpoint. That gives the user an
API button while keeping long-running execution in the orchestrator.

Fallback if Airflow trigger API is inconvenient:

- `POST` calls archiver endpoint directly with a high timeout.
- response may block until generation completes.
- this is acceptable for internal/admin use, but should be treated as temporary.

Auth:

- human admin session for UI/manual use
- bearer token for GitHub Actions downloads
- deny anonymous downloads

Audit log fields:

```text
event_type
snapshot_id
caller
route
archive_bytes
created_at
```

The audit table can be added later if needed; first pass can log structured
events to the ops logger.

---

## Airflow DAG

File:

```text
airflow/dags/export_ci_lake_snapshot.py
```

Pattern should match existing archiver DAGs:

```text
ready sensor
  -> archiver health sensor
  -> trigger export task
```

Defaults:

```python
ARCHIVER_URL = "http://archiver:8001"
```

Initial DAG:

- `dag_id="export_ci_lake_snapshot"`
- `schedule=None`
- paused/manual by default
- params for tier, target VINs, max archive MB, source window months
- POST to `/snapshots/adaptive-refresh/run`
- timeout 30-60 minutes

Later schedule:

- weekly or monthly, depending on usefulness
- avoid overlap with heavy dbt/Spark jobs

The task should fail if archiver returns:

- non-2xx status
- `coverage_failures` not empty
- missing archive key
- missing manifest key

---

## Downloader

File:

```text
scripts/download_lake_snapshot.py
```

Inputs:

```text
--latest
--snapshot-id adaptive-refresh-...
--base-url https://cartracker.info
--token env:CARTRACKER_SNAPSHOT_TOKEN
--out .cache/lake_snapshots
```

Behavior:

```text
1. Fetch latest or named manifest from ops API.
2. Download snapshot archive.
3. Verify archive sha256.
4. Write manifest beside archive.
5. Print local archive path.
```

Never silently accept checksum mismatch.

---

## Seeder

File:

```text
scripts/seed_lake_snapshot.py
```

Inputs:

```text
--snapshot .cache/lake_snapshots/adaptive-refresh-.../snapshot.tar.zst
--minio-endpoint http://localhost:9000
--bucket bronze
--clear-prefixes
```

Behavior:

```text
1. Verify archive checksum against manifest.
2. Unpack into temporary directory.
3. Optionally clear known fixture prefixes from target MinIO.
4. Upload Parquet files preserving archive-relative paths.
5. Upload expected artifacts if desired.
6. Print table row/file counts.
```

Default should be safe for local/CI fixture buckets. Do not point this at
production without explicit safeguards.

Production safety guard:

- refuse to run when target endpoint looks like production unless
  `--allow-production-target` is provided
- never default to production credentials

---

## CI Integration

Keep the existing tiny schema seed for fast compile/unit behavior.

Add a separate integration path:

```text
download snapshot
seed CI MinIO
run selected dbt/Spark/Delta tests
```

Recommended first wiring:

- manual workflow dispatch or scheduled job
- then PR path if runtime is acceptable

CI secrets:

```text
CARTRACKER_SNAPSHOT_TOKEN
CARTRACKER_SNAPSHOT_BASE_URL
```

Pinning:

- default to `latest` for scheduled validation
- pin `snapshot_id` when debugging regressions
- allow manual workflow input for `snapshot_id`

---

## Testing Plan

### Unit Tests

`tests/archiver/test_export_ci_lake_snapshot.py`

- request defaults by tier
- invalid tier rejected
- invalid snapshot ID rejected
- selector registry has unique names
- coverage failure produced when required selector is short
- allocation deduplicates VINs across selectors
- closure adds previous listing IDs from remap events
- manifest includes required fields
- manifest validation fails on missing table
- package checksum changes when archive changes
- `latest.json` is updated only after successful archive/manifest upload

`tests/archiver/test_app.py`

- `POST /snapshots/adaptive-refresh/run` calls processor
- endpoint wraps processor in `active_job()`
- processor error returns non-2xx or error payload consistent with existing
  archiver patterns

### Integration Tests

`tests/integration/archiver/test_lake_snapshot_export_integration.py`

- seed mini Parquet datasets into test MinIO
- run export processor
- verify archive exists
- verify manifest exists
- unpack archive
- verify row counts
- verify selector coverage
- verify fixture paths match dbt source layout

`tests/integration/test_lake_snapshot_seed.py`

- download/use local test archive
- seed MinIO
- verify required Parquet prefixes exist
- run a small DuckDB/dbt smoke query if practical

`tests/integration/airflow/test_dag_integrity.py`

- DAG imports
- DAG ID is `export_ci_lake_snapshot`
- schedule is `None`
- expected task IDs exist

`tests/ops/test_snapshot_downloads.py`

- unauthenticated latest denied if route is protected
- authenticated latest returns JSON
- named manifest returns JSON
- download streams archive bytes
- checksum header or manifest checksum is available

---

## Implementation Sequence

Use this section as the source of truth for commit gates. The shorter
`docs/plan_120_ci_lake_snapshot_delivery.md` uses phase labels for product
areas, but implementation tracking should use these numbered steps.

Current status as of 2026-07-07 (Gate B complete):

| Step | Gate | Status | Current branch state | Remaining work |
|------|------|--------|----------------------|----------------|
| 1 | Processor skeleton | Mostly done | `SnapshotRequest`, `SnapshotResult`, validation, tier defaults, CLI, dry-run, audit mode, and basic manifest skeleton exist. | Replace `not_implemented` non-dry-run path once Steps 4-6 exist. Extend manifest helper when real counts/checksums are available. |
| 2 | Selector registry | Done (Gate B) | Registry exists. All 22 selectors are executable, each derived from one or more of the four supported source tables. `stable_state_run`/`state_change_run` reproduce the dbt fingerprint fields exactly; `detail_beats_srp`/`srp_fallback` mirror `int_latest_observation.sql`'s source-priority ranking. Coupling is guarded by one CI test (`tests/integration/dbt/test_selector_dbt_equivalence.py`) that runs the actual dbt models against seeded fixture data and diffs selector output against dbt's real materialized tables. | None — proceed to Step 4 (cohort allocation and closure). |
| 3 | DuckDB source reads | Done for current scope | Shared DuckDB/MinIO helper exists. Source audit reads the four included tables. Local fixture mode exists for tests. | Reuse these reads for allocation/closure and table materialization. Add source views only if they reduce duplication in the next steps. |
| 4 | Cohort allocation and closure | Not started | Selector diagnostics currently count/sample candidate entities only. | Build candidate sets, allocate required examples, fill deterministically, expand VIN/listing/artifact/event closure, and emit closure diagnostics. |
| 5 | Write filtered Parquet | Not started | No fixture output is written. | Filter production Parquet by closed cohort, preserve dbt-compatible prefixes, and compute row counts/table checksums. |
| 6 | Package and upload | Not started | No archive or MinIO snapshot prefix is written. | Package `.tar.zst`, upload to temp prefix, validate manifest/archive, promote to final prefix, update `latest.json`. |
| 7 | Archiver endpoint | Partial | `POST /snapshots/adaptive-refresh/run` is wired, guarded by `active_job()`, and tested for dry-run/audit request handling. | Return `created` with archive/manifest keys after Steps 4-6. Keep non-dry-run failure explicit until then. |
| 8 | Airflow DAG | Structurally done | Manual DAG exists, passes params/defaults, logs result fields, and fails on unsupported statuses. | Trigger on VM after Steps 4-6 can create a real snapshot. Add cadence only after manual runs are stable. |
| 9 | Ops download API | Not started | No `ops` routes exist yet. | Add latest/manifest/download routes, CI token auth, and download tests. |
| 10 | Download and seed scripts | Mostly done | Downloader and seeder exist. Offline/local mode works against manifest/archive paths. API mode is scaffolded. Seeder guards production-like targets and verifies checksums. | Test against the real ops API once Step 9 exists. Run local end-to-end seed with a generated archive. |
| 11 | CI pilot | Not started | No workflow consumes snapshots yet. | Add manual/scheduled GitHub Actions path after a real archive and API route exist. Measure runtime before enabling PR checks. |

Gate names for the next commits:

1. **Gate A - docs/status reset:** complete when both Plan 120 docs clearly map
   committed work to these steps and identify the next unfinished gate.
2. **Gate B - selector readiness:** complete (2026-07-07). All 22 registered
   selectors are executable; none remain TODO placeholders. Fingerprint and
   source-priority selectors are covered by a CI equivalence test that runs
   the actual dbt models
   (`tests/integration/dbt/test_selector_dbt_equivalence.py`).
3. **Gate C - cohort allocation and closure:** complete when selector candidates
   become a coherent `SnapshotCohort` with seed/closed VINs, listing IDs,
   artifact IDs, coverage diagnostics, and deterministic fill behavior.
4. **Gate D - filtered Parquet writer:** complete when a closed cohort can be
   materialized into dbt-compatible fixture prefixes with row counts and table
   checksums.
5. **Gate E - manifest/package/upload:** complete when an `edge` snapshot can be
   written to MinIO under a versioned prefix, validated, promoted, and exposed
   through `latest.json`.
6. **Gate F - ops download API:** complete when authenticated latest/manifest/
   download routes can serve an existing snapshot archive.
7. **Gate G - end-to-end smoke:** complete when the VM can generate an `edge`
   snapshot and a local/CI-like environment can download, verify, seed, and run
   a small dbt/Spark smoke test.

### Step 1: Add Processor Skeleton

- Add `SnapshotRequest`, `SnapshotResult`, manifest helpers.
- Add CLI entrypoint.
- Add dry-run mode that returns a planned snapshot ID and limits.
- Add unit tests for request validation.

### Step 2: Add Selector Registry (complete — Gate B)

- Add selector objects with placeholder SQL. (done)
- Implement the first selectors: `relisted_vin`, `price_drop`,
  `price_increase`, `cooldown_incremented`, `stable_state_run`. (done)
- Implement the remaining selectors (Gate B): `state_change_run`,
  `active_to_unlisted`, `price_changed_7d`, `price_changed_30d_only`,
  `no_price_history`, `detail_beats_srp`, `srp_fallback`,
  `carousel_only_or_low_priority`, `invalid_or_null_vin`,
  `benchmark_dense_make_model`, `benchmark_sparse_make_model`,
  `cooldown_blocked`, `cooldown_bucket_3_4`, `cooldown_bucket_5_10`,
  `cooldown_bucket_11_plus`, `fresh_recent_listing`, `stale_listing`. All are
  derived from the four supported source tables; none required a fifth
  source table, so none are marked non-runnable.
- Add unit tests for registry shape and per-selector fixture coverage
  (`tests/archiver/test_export_ci_lake_snapshot.py`) — fast, local, no
  services required. These test the selector SQL on its own merits (does it
  find the intended candidate); they do not assert anything about dbt. (done)
- Add the dbt coupling guard in the CI `dbt` job:
  `scripts/seed_lake_snapshot_fixture.py` seeds known
  business-state scenarios into MinIO, the existing `dbt build` step
  materializes `int_listing_state_runs`/`int_latest_observation` against
  that data for real, and `tests/integration/dbt/test_selector_dbt_equivalence.py`
  diffs selector output against dbt's actual materialized tables. This is
  the single place that fails CI if the selector SQL and the dbt SQL drift
  apart — deliberately not duplicated as a second hand-copied comparison in
  the unit-test file, to avoid a second copy of dbt's logic that could
  itself drift unnoticed. (done)

### Step 3: Add DuckDB Source Reads

- Add DuckDB connection and MinIO configuration.
- Add source views for the four included tables.
- Add a local fixture mode for tests using local Parquet paths.

### Step 4: Implement Cohort Allocation and Closure

- Run selectors.
- Allocate required examples.
- Fill with deterministic random VINs.
- Expand entity closure.
- Emit diagnostics.

### Step 5: Write Filtered Parquet

- Write fixture tables into temp directory.
- Preserve dbt-compatible prefixes.
- Add row counts and table checksums.

### Step 6: Package and Upload

- Add `.tar.zst` packaging.
- Upload to temporary MinIO prefix.
- Validate uploaded manifest/archive.
- Promote to final prefix.
- Update `latest.json`.

### Step 7: Add Archiver Endpoint

- Wire `POST /snapshots/adaptive-refresh/run`.
- Wrap in `active_job()`.
- Add endpoint tests.

### Step 8: Add Airflow DAG

- Add manual DAG.
- Add DAG integrity tests.
- Trigger manually on VM.

### Step 9: Add Ops Download API

- Add latest/manifest/download routes.
- Add CI token auth.
- Add tests.

### Step 10: Add Download and Seed Scripts

- Implement downloader.
- Implement seeder.
- Run local end-to-end seed.

### Step 11: CI Pilot

- Add manual/scheduled GitHub Actions path.
- Measure runtime.
- Decide whether `ci` tier can run on PRs or should remain scheduled/manual.

---

## Operational Runbook

Manual generation through Airflow:

```text
Open Airflow
Trigger DAG: export_ci_lake_snapshot
Params:
  tier=ci
  target_vins=5000
  max_archive_mb=250
```

Manual generation through archiver from VM:

```powershell
docker compose exec archiver python -m archiver.processors.export_ci_lake_snapshot `
  --tier ci `
  --target-vins 5000 `
  --max-archive-mb 250
```

Download locally:

```powershell
python scripts/download_lake_snapshot.py --latest
python scripts/seed_lake_snapshot.py --snapshot .cache/lake_snapshots/adaptive-refresh-.../snapshot.tar.zst
```

Rollback:

- Do not delete the previous snapshot.
- Restore `latest.json` to point at the previous known-good snapshot.
- Because snapshots are immutable, existing pinned CI runs continue to work.

---

## Open Questions

- Should ops `POST /admin/snapshots/adaptive-refresh` trigger Airflow or call
  archiver directly in the first implementation?
- Should `edge` fixtures be committed to the repo or generated/downloaded like
  other tiers?
- Should snapshot archives live in the `bronze` bucket or a separate
  `ci-snapshots` bucket once governance work begins?
- Should selector SQL be based purely on source Parquet, or should it use
  dbt-built intermediate tables after Plan 118?
- How much historical window is enough for default `ci`: 6 months, 12 months,
  or all history?
