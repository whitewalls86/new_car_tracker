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

Status as of 2026-07-08: **Gate C.5 (worker isolation and observability)** is
implemented, and **Gate C.75 (persisted planning cache)** has been added on
top of it. VM validation had exposed an operational issue: production-sized
cohort planning is too heavy to run inside the always-on production archiver
service. Source audit and selector diagnostics passed against the VM lake, but
a six-month `build_cohort` dry run consumed sustained CPU/network for hours
and likely contributed to missed production archiver/Airflow health checks.

Production-sized `build_cohort` work has moved to an isolated `snapshot-worker`
one-shot Docker Compose service (profile-gated, no ports, reuses the archiver
image/build context). The production archiver's
`POST /snapshots/adaptive-refresh/run` endpoint now rejects `build_cohort=True`
with a `409` unless `ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT=true` is set (off by
default; intended for tests/manual override, not production). Cheap
`audit_sources`/`dry_run` (without `build_cohort`) calls remain allowed on the
production archiver. Phase timing logs (`time.monotonic()`-based, `elapsed_s`
fields) were added around source audit, selector candidate collection, cohort
allocation, and each entity-closure pass. When both `run_selectors=True` and
`build_cohort=True` are requested, the exporter now collects selector
candidates once and reuses them for both selector diagnostics and cohort
allocation, instead of scanning the lake twice.

Gate C.75 adds a persisted planning cache
(`archiver/processors/lake_snapshot_planning_cache.py`) on top of that shared
scan, so repeated equivalent heavy planning requests (e.g. re-running the same
`snapshot-worker` invocation) can skip selector/cohort computation entirely
instead of rescanning the lake again. See
[Planning cache (Gate C.75)](#planning-cache-gate-c75) below for the
fingerprint/flag contract. Reuse is opt-in only — no request implicitly reuses
a cache.

The next gate is **Gate D - fingerprint-addressed filtered Parquet writer**.

The phase labels below describe product areas, not commit gates. For execution
tracking, use the Step 1-11 checklist in
[implementation_plan_120_ci_lake_snapshot_delivery.md](implementation_plan_120_ci_lake_snapshot_delivery.md#implementation-sequence).

| Area | Status | Notes |
|------|--------|-------|
| Exporter skeleton | Mostly done | Request/result models, validation, tier defaults, CLI, dry-run, source audit, selector diagnostics, and cohort dry-run diagnostics exist. Non-dry-run export still returns `not_implemented`. |
| Selector registry | Done (Gate B) | All 22 registered selectors are executable, derived entirely from the four supported source tables. `stable_state_run`/`state_change_run` reproduce the dbt fingerprint fields from `int_listing_state_fingerprints.sql` exactly; `detail_beats_srp`/`srp_fallback` mirror `int_latest_observation.sql`'s source-priority ranking. No selector remains a TODO placeholder. Coupling to dbt is guarded by a single CI test (`tests/integration/dbt/test_selector_dbt_equivalence.py`) that seeds fixture data (`scripts/seed_lake_snapshot_fixture.py`), runs the real dbt models, and diffs selector output against dbt's actual materialized tables — so drift in either the dbt SQL or the selector SQL fails CI. |
| DuckDB source reads | Done for audit/selector reads | Shared DuckDB/MinIO helper exists, source audit reads four lake tables, and local fixture mode exists for tests. |
| Cohort allocation and closure | Implemented, VM-hardened (Gate C.5) | `SnapshotCohort`, deterministic allocation/fill, VIN/listing/artifact closure, artifact-only seed closure, selector coverage diagnostics, and target-overrun diagnostics exist. Local and integration tests pass. VM source audit and selector diagnostics pass. Heavy `build_cohort` work now runs only in the isolated `snapshot-worker` container. |
| Worker isolation and observability | Done (Gate C.5) | `snapshot-worker` is a profile-gated, port-free, one-shot Docker Compose service that reuses the archiver image/build context. The production archiver rejects `build_cohort=True` with `409` unless `ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT=true`. Phase timing logs (`elapsed_s`) cover source audit, selector candidate collection, cohort allocation, and each closure pass. Selector diagnostics and cohort allocation share one candidate scan when both are requested. |
| Persisted planning cache | Done (Gate C.75) | `lake_snapshot_planning_cache.py` fingerprints the heavy planning path (tier, selector/cohort toggles, normalized source window, target_vins, min_selector_coverage, source_base_path, selector config/SQL hashes, cohort algorithm version) and stores/loads a JSON planning artifact in MinIO, keyed by that fingerprint. `dry_run`, `audit_sources`, and `snapshot_id` never affect the fingerprint. Reuse/refresh are both opt-in flags; the default computes and persists but never reuses. |
| Filtered Parquet writer | Not started | No fixture table materialization yet. Gate D should derive an export fingerprint from the planning fingerprint plus writer/export semantics, materialize dbt-compatible fixture prefixes under that fingerprint, and explicitly support reusing an existing materialized snapshot. |
| Manifest/package/upload | Not started | No `.tar.zst` generation, MinIO promotion, or `latest.json` update yet. Gate E should package from the materialized export fingerprint and reuse an existing archive when its checksum already matches. |
| Archiver endpoint | Control-plane only (Gate C.5) | Internal endpoint is wired and wrapped with `active_job()`. Cheap `audit_sources`/`dry_run` (without `build_cohort`) calls remain allowed. `build_cohort=True` is rejected with `409` unless `ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT=true`; production-sized work must go through `snapshot-worker`. |
| Airflow DAG | Structurally done, worker target TBD | Manual DAG exists and passes params/defaults. It should trigger the isolated snapshot worker once Steps 4-6 are worker-safe — the DAG currently still posts to the archiver control-plane route, which will 409 if it requests `build_cohort=True` without the override flag. |
| Ops download API | Not started | Latest/manifest/download routes and CI token auth still need implementation. |
| Download/seed scripts | Mostly done | Offline/local mode exists and verifies checksums. API mode is scaffolded but depends on the ops download API. |
| CI pilot | Not started | Needs a real archive and ops download route first. |

Next gate: **Gate D - fingerprint-addressed filtered Parquet writer**. Gate C.5 isolated
production-sized cohort/export work into `snapshot-worker` and added phase
timing logs, so the next step is materializing the closed cohort into
dbt-compatible fixture Parquet. Gate D should preserve the Gate C.75 cache
boundary: a planning cache hit should be able to flow directly to an existing
materialized snapshot when the export fingerprint also matches.

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
Snapshot generation should run in an isolated snapshot worker, not inside the
always-on production archiver service. The worker can reuse archiver code and
the archiver image, but it should be a separate service/process/container so
production flush, cleanup, compact, and health-check responsibilities are not
competing with long DuckDB lake scans.

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

Snapshot creation should happen through an operator command, scheduled job,
one-shot worker container, or background worker. CI and local dev should only
download already-built immutable artifacts.

Recommended ownership:

| Layer | Responsibility |
|-------|----------------|
| production `archiver` | Continue production duties: flush silver, flush staging, cleanup, compact, and cheap snapshot audit/control-plane calls. |
| `snapshot-worker` | Generate snapshots, run selectors, read production Parquet, write snapshot archives to MinIO. This may reuse archiver modules/image but must be isolated from production archiver runtime health. |
| Airflow | Schedule or manually trigger snapshot-worker generation. |
| `ops` API | Authenticated admin/CI facade for listing, requesting, auditing, and downloading snapshots. |
| CI/local scripts | Download, verify, unpack, and seed snapshots. |

This keeps the heavy data export job close to the existing Parquet maintenance
code while avoiding production-service starvation, and keeps public
download/auth behavior in the service that already owns admin access.

Operational lesson from VM validation:

```text
Do not run production-sized build_cohort/export jobs synchronously inside
the production archiver API process.
```

Safe near-term execution model:

```bash
docker compose run --rm snapshot-worker python -m archiver.processors.export_ci_lake_snapshot \
  --tier edge \
  --run-selectors \
  --build-cohort \
  --source-window-months 1 \
  --target-vins 100
```

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
split into Steps 1-7. As of 2026-07-08, the skeleton, source audit, selector
diagnostics, cohort allocation/closure, internal endpoint, local fixture seeder,
download/seed scripts, and manual Airflow DAG structure are implemented. The
real archive-producing path still depends on worker isolation, filtered Parquet
writing, manifest finalization, packaging, and upload.

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
5. Compute or reuse the planning fingerprint for the closed cohort.
6. Derive an export fingerprint from the planning fingerprint plus export
   semantics: included table list, writer version, output schema/version,
   partition layout, compression, and sanitization rules.
7. Reuse a complete materialized snapshot when the caller explicitly allows
   reuse and the export fingerprint already exists.
8. Read only required production-derived Parquet tables.
9. Filter rows for the cohort and source window.
10. Remove raw HTML and any unnecessary sensitive/internal fields.
11. Write the fixture directory under a fingerprint-addressed staging prefix.
12. Generate row counts, file counts, and checksums.
13. Generate coverage assertions.
14. Promote/write the materialized snapshot manifest.
15. Package `snapshot.tar.zst`.
16. Write archive metadata.
17. Upload to MinIO under a fingerprint-addressed archive prefix.
18. Update `latest.json` or another friendly alias only after validation.

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

Expose a cheap internal archiver/control-plane route:

```http
POST /snapshots/adaptive-refresh/run
```

The route should accept the same tier/limit inputs as the CLI and return a job
summary:

```json
{
  "snapshot_id": "adaptive-refresh-2026-07-07",
  "planning_fingerprint": "...",
  "export_fingerprint": "...",
  "tier": "ci",
  "status": "created",
  "seed_vin_count": 5000,
  "closed_vin_count": 5062,
  "archive_bytes": 183447221,
  "manifest_key": "snapshot_exports/fingerprints/{export_fingerprint}/manifest.json",
  "archive_key": "snapshot_archives/fingerprints/{export_fingerprint}/snapshot.tar.zst"
}
```

The route is for internal use by Airflow and ops. It should not be exposed
directly through Caddy. It must not run production-sized cohort/export work
synchronously in the production archiver process; production-sized generation
should be delegated to the isolated snapshot worker.

Suggested MinIO layout:

```text
snapshot_exports/
  fingerprints/{export_fingerprint}/
    manifest.json
    data/
      silver_normalized/observations/...
      ops_normalized/price_observation_events/...
      ops_normalized/vin_to_listing_events/...
      ops_normalized/blocked_cooldown_events/...
snapshot_archives/
  fingerprints/{export_fingerprint}/
    archive_manifest.json
    snapshot.tar.zst
ci_snapshots/adaptive_refresh/
  latest.json
  aliases/{snapshot_id}.json
```

`snapshot_id` is a friendly run label/alias, not the reusable storage identity.
The true identity for generated data is the `export_fingerprint`, which is
derived from the planning fingerprint and export semantics.

---

## Planning cache (Gate C.75)

The heavy dry-run planning path (`dry_run=True` + `run_selectors=True` +
`build_cohort=True`) is expensive: selector candidate collection and cohort
allocation/closure each scan the lake. Gate C.75 adds an explicit, persisted
cache for that path's output so an equivalent planning request can skip
straight to a prior result instead of rescanning.

**Flags** (`SnapshotRequest` fields, CLI flags, and API payload keys):

- `reuse_planning_cache` (default `false`) — if a matching cache entry exists,
  load it and skip selector/cohort computation; on a miss, compute and persist
  as usual.
- `refresh_planning_cache` (default `false`) — ignore any existing cache entry,
  compute fresh, and overwrite it. Mutually exclusive with
  `reuse_planning_cache` (both set is a validation error).
- `planning_cache_bucket_grain` (default `"week"`) — one of `week`, `day`,
  `none`. Controls how a *relative* `source_window_months` window is bucketed
  for fingerprinting (see below).
- `planning_cache_prefix` (default `"snapshot_planning_cache"`) — the MinIO
  key prefix planning artifacts are stored under.

With no flags set, the default behavior is to compute fresh and persist the
result — it never implicitly reuses a cache. Reuse is always something the
caller asks for.

**Fingerprint.** The cache key is a SHA-256 hash over the request's *semantic
planning identity*: `tier`, `run_selectors`, `build_cohort`, the normalized
source window (see below), `target_vins`, `min_selector_coverage`,
`source_base_path`, a hash of the resolved Parquet path for every supported
source table (`source_table_paths_hash` — catches a lake layout/bucket change
even when `source_base_path` itself is unchanged), a hash of the loaded
selector config, a hash of the resolved selector SQL templates, and a cohort
algorithm version constant. `dry_run`, `audit_sources`, and `snapshot_id` are
deliberately excluded — none of them change what selectors/cohort would
compute, so including them would fragment the cache without adding safety.
`snapshot_id` in particular is often unique per invocation (e.g.
Airflow-generated), so including it would make the cache unreusable across
otherwise-identical runs.

**Window normalization.** An explicit `source_window_start`/`source_window_end`
always fingerprints on those exact values. A *relative*
`source_window_months` window is bucketed by `planning_cache_bucket_grain`:
`week` re-anchors "now" to the most recent Monday 00:00 UTC, `day` snaps to
midnight UTC, and `none` uses the exact resolved timestamps for that call
(which will essentially never repeat, so `none` opts out of caching for
relative windows).

Critically, `resolve_planning_window()` re-anchors the window *before* both
the fingerprint is computed and selectors/cohort are actually queried — the
bucketed timestamp isn't just a hashing convenience, it becomes the real
`window_start`/`window_end` used for that call. `export_ci_lake_snapshot()`
samples `datetime.now(timezone.utc)` exactly once per call and threads that
same value into `resolve_source_window()` and `resolve_planning_window()`, so
the two never disagree about "now" across a UTC day/week boundary. Without this, a Monday
03:00 UTC run and a Thursday 21:00 UTC run in the same ISO week would share a
weekly fingerprint while each querying its own exact (different) window; a
cache hit would then silently serve a cohort computed over the wrong window
while the response reported the *current* call's exact timestamps. Because
the actual query is bucketed too, every call in the same bucket computes
(and can safely share) the literal same window. The normalized
`fingerprint_window` and the `resolved_window` persisted in the cache artifact
are therefore always identical for a relative window — both fields are kept
for observability, not because they can diverge.

**Storage.** Cache artifacts are plain JSON, written via `shared.minio.write_json`
/ `read_json` at a deterministic path:
`{planning_cache_prefix}/fingerprints/{fingerprint}/planning.json`. A cache
entry is written only after a successful selector/cohort computation — a
failed compute never persists a partial artifact. Reads/writes failing (e.g.
transient MinIO error) are logged and treated as a miss/no-write; they don't
fail the request.

**Response observability.** `SnapshotResult` exposes `planning_cache_key`
(the fingerprint), `planning_cache_path`, `planning_cache_hit`, and
`planning_cache_action` (one of `"computed"`, `"reused"`, or `"refreshed"`).

Example commands (run inside `snapshot-worker`, same as the Gate C.5 heavy
path):

```bash
# Compute fresh and persist a planning cache entry (default: no flags).
docker compose run --rm snapshot-worker python -m archiver.processors.export_ci_lake_snapshot \
  --tier ci --dry-run --run-selectors --build-cohort \
  --source-window-months 1 --target-vins 5000

# Reuse a previously persisted entry if the fingerprint matches (same week by default).
docker compose run --rm snapshot-worker python -m archiver.processors.export_ci_lake_snapshot \
  --tier ci --dry-run --run-selectors --build-cohort \
  --source-window-months 1 --target-vins 5000 --reuse-planning-cache

# Force a fresh recompute and overwrite the cached entry.
docker compose run --rm snapshot-worker python -m archiver.processors.export_ci_lake_snapshot \
  --tier ci --dry-run --run-selectors --build-cohort \
  --source-window-months 1 --target-vins 5000 --refresh-planning-cache
```

---

## Export materialization cache (Gate D/E contract)

Gate C.75 caches the expensive answer to "which cohort should this snapshot
contain?" Gate D must preserve that win by making the next expensive answer
cacheable too: "which Parquet fixture files represent this planned cohort under
the current export rules?"

Gate D should introduce:

- `planning_fingerprint`: the existing Gate C.75 cache key.
- `export_fingerprint`: a new SHA-256 key derived from the
  `planning_fingerprint` plus export semantics.
- `materialized_snapshot_path`: the final MinIO prefix for filtered Parquet.
- `manifest_path`: the manifest for the materialized Parquet output.
- `archive_path`: the eventual Gate E `.tar.zst` path, once packaging exists.
- `export_cache_hit` and `export_cache_action`: response fields mirroring the
  planning-cache observability pattern.

The export fingerprint should include only fields that change the bytes or
interpretation of the materialized output:

- `planning_fingerprint`
- included logical table list and source table mapping
- output schema/manifest version
- writer algorithm/version
- partition layout
- Parquet compression/settings
- sanitization/drop-column rules
- expected-output generation version, once expected artifacts are written

It should exclude `snapshot_id`, `dry_run`, `audit_sources`, and other run
labels that do not change output bytes.

Expected Gate D flow:

```text
resolve planning artifact
derive export fingerprint
if reuse_export_cache and complete materialized output exists:
  return manifest/archive metadata without scanning source Parquet
else:
  write filtered Parquet to temporary fingerprint staging prefix
  compute row counts, file counts, and checksums
  validate expected coverage and table invariants
  promote/write manifest under snapshot_exports/fingerprints/{export_fingerprint}/
```

Gate E should package from the materialized snapshot path and write the archive
under `snapshot_archives/fingerprints/{export_fingerprint}/`. If the archive
already exists and its checksum matches the manifest, Gate E should be able to
reuse it without repackaging.

This gives us three reusable layers:

```text
planning fingerprint -> cohort plan JSON
export fingerprint   -> filtered Parquet fixture directory + manifest
export fingerprint   -> packaged archive + archive manifest
```

The friendly `snapshot_id` can still be stored in manifests and alias files,
but it should never force recomputation by itself.

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
- trigger the isolated snapshot worker, or POST only to a cheap archiver/ops
  control-plane route that starts the worker
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

## Next VM Validation

Before Gate D, validate that the expensive first phase can be generated and
reused from the isolated worker:

```bash
# Compute the expensive planning phase and write planning.json.
docker compose run --rm snapshot-worker python -m archiver.processors.export_ci_lake_snapshot \
  --tier edge \
  --dry-run \
  --run-selectors \
  --build-cohort \
  --source-window-months 1 \
  --target-vins 100 \
  --refresh-planning-cache

# Re-run the same request and prove it hits the persisted planning cache.
docker compose run --rm snapshot-worker python -m archiver.processors.export_ci_lake_snapshot \
  --tier edge \
  --dry-run \
  --run-selectors \
  --build-cohort \
  --source-window-months 1 \
  --target-vins 100 \
  --reuse-planning-cache
```

Expected result: both responses report the same `planning_cache_key`; the
second response reports `planning_cache_hit=true` and
`planning_cache_action=reused`.

---

## Testing

- Exporter produces coherent VIN histories across all included tables.
- Production-sized export/cohort work runs in an isolated worker, not in the
  production archiver API process.
- Worker logs include phase timing for selector collection, allocation,
  closure passes, table writing, packaging, and upload.
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
