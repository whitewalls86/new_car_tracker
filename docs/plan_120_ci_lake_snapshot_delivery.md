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

Status as of 2026-07-15: **Gate E (manifest/package/upload)** is implemented
and VM-verified. An `edge` snapshot was generated through the isolated
`snapshot-worker`, packaged into `snapshot.tar.zst`, published with
`archive_manifest.json`, promoted to
`ci_snapshots/adaptive_refresh/latest.json`, and then rerun with
`--reuse-archive-cache`, confirming `archive_cache_hit=true` /
`archive_cache_action=reused`.

Earlier VM validation had exposed an operational issue: production-sized
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

VM validation of Gate C.75 also exposed a closure-correctness bug: recursive
artifact-based expansion (an artifact_id resolving back to the VINs/listings
of every row it appeared on) treated SRP/carousel co-occurrence as a
vehicle-identity edge, causing runaway growth (143 seed VINs exploded to
22,556 closed VINs across two passes). This was fixed by constraining core
closure to only VIN<->listing_id and remap edges; artifact_ids are attached to
the frozen core afterward, purely as diagnostics/provenance, never used to
discover more VINs/listings. `COHORT_ALGORITHM_VERSION` was bumped so stale
planning cache entries computed under the old (buggy) semantics are treated
as fingerprint misses.

**Gate D (fingerprint-addressed filtered Parquet writer) is now implemented.**
`archiver/processors/lake_snapshot_export.py` filters the four source tables
by the closed cohort and writes dbt-compatible Parquet under
`{export_prefix}/fingerprints/{export_fingerprint}/data/`.
`archiver/processors/lake_snapshot_export_cache.py` derives the export
fingerprint from the planning fingerprint plus writer/schema/partition/
compression semantics, and persists/loads the resulting manifest, mirroring
the Gate C.75 cache pattern. Critically, the export writer inherits the
closure fix's invariant: `artifact_id` is never used as a blanket
`artifact_id IN (...)` filter against `silver_observations` (which would
reintroduce the same SRP/carousel pollution at materialization time instead
of at closure time). Artifact-only-seeded rows (currently just
`invalid_or_null_vin`) are matched by an exact `(artifact_id, vin,
listing_id)` row identity captured during selector candidate collection
(`CandidateSet.selected_row_keys`), not by artifact_id membership alone. See
[Filter semantics (Gate D)](#filter-semantics-gate-d) below.

**Gate E (manifest/package/upload) is now implemented.**
`archiver/processors/lake_snapshot_archive.py` packages a materialized Gate D
export (the fingerprint-addressed filtered Parquet directory plus its
`manifest.json`) into a deterministic `snapshot.tar.zst`, uploads it to
`snapshot_archives/fingerprints/{export_fingerprint}/snapshot.tar.zst`, and
publishes an `archive_manifest.json` beside it — the export manifest plus an
`archive: {path, bytes, sha256, file_count}` block, which is exactly the
shape `scripts/lake_snapshot_common.py`'s `get_archive_meta` already expects.
`latest.json`/`aliases/{snapshot_id}.json` under
`ci_snapshots/adaptive_refresh/` are promoted only after a successful
archive+manifest publish, and the alias file is always written before
`latest.json` so a failure partway through can never leave `latest.json`
pointing at a snapshot with no alias. See
[Packaging and upload (Gate E)](#packaging-and-upload-gate-e) below.

**Gate F (ops download API) is now implemented** (not yet VM-verified).
`ops/routers/snapshots.py` adds three read-only routes under
`/admin/snapshots/adaptive-refresh/` — see
[Ops download API (Gate F)](#ops-download-api-gate-f) below for the exact
route contract. In parallel, Plan 112 Gate A4 should now consume the
VM-verified Gate E archive contract through the existing download/seed
scripts.

The phase labels below describe product areas, not commit gates. For execution
tracking, use the Step 1-11 checklist in
[implementation_plan_120_ci_lake_snapshot_delivery.md](implementation_plan_120_ci_lake_snapshot_delivery.md#implementation-sequence).

| Area | Status | Notes |
|------|--------|-------|
| Exporter skeleton | Done through Gate D | Request/result models, validation, tier defaults, CLI, dry-run, source audit, selector diagnostics, cohort diagnostics, and a real non-dry-run export path (Gate D) exist. Non-dry-run requests always run full selector/cohort planning regardless of `run_selectors`/`build_cohort` (those flags now only scope dry-run diagnostics). |
| Selector registry | Done (Gate B) | All 22 registered selectors are executable, derived entirely from the four supported source tables. `stable_state_run`/`state_change_run` reproduce the dbt fingerprint fields from `int_listing_state_fingerprints.sql` exactly; `detail_beats_srp`/`srp_fallback` mirror `int_latest_observation.sql`'s source-priority ranking. No selector remains a TODO placeholder. Coupling to dbt is guarded by a single CI test (`tests/integration/dbt/test_selector_dbt_equivalence.py`) that seeds fixture data (`scripts/seed_lake_snapshot_fixture.py`), runs the real dbt models, and diffs selector output against dbt's actual materialized tables — so drift in either the dbt SQL or the selector SQL fails CI. |
| DuckDB source reads | Done for audit/selector/export reads | Shared DuckDB/MinIO helper exists, source audit reads four lake tables, the Gate D writer filters and reads them for materialization, and local fixture mode exists for tests. |
| Cohort allocation and closure | Implemented, VM-hardened, closure-corrected (Gate C.5) | `SnapshotCohort`, deterministic allocation/fill, VIN/listing closure (remap-aware), post-closure artifact attachment, artifact-only selector row-key capture, selector coverage diagnostics, and target-overrun diagnostics exist. Local and integration tests pass, including a regression test proving artifact co-occurrence does not expand the vehicle closure. VM source audit and selector diagnostics pass. Heavy `build_cohort` work now runs only in the isolated `snapshot-worker` container. |
| Worker isolation and observability | Done (Gate C.5) | `snapshot-worker` is a profile-gated, port-free, one-shot Docker Compose service that reuses the archiver image/build context. The production archiver rejects `build_cohort=True` **or non-dry-run** with `409` unless `ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT=true` (a real export always runs the same heavy planning, so it's guarded identically). `audit_sources` is exempt either way. Phase timing logs (`elapsed_s`) cover source audit, selector candidate collection, cohort allocation, each closure pass, and Gate D table materialization. |
| Persisted planning cache | Done (Gate C.75) | `lake_snapshot_planning_cache.py` fingerprints the heavy planning path (tier, selector/cohort toggles, normalized source window, target_vins, source_base_path, selector config/SQL hashes, cohort algorithm version) and stores/loads a JSON planning artifact in MinIO, keyed by that fingerprint. The artifact now persists actual cohort membership (VINs/listing_ids/artifact_ids/artifact_row_keys), not just counts, so a cache hit can feed Gate D materialization directly. `dry_run`, `audit_sources`, `snapshot_id`, and `require_selector_coverage` never affect the fingerprint (the latter is validation policy only — see "Selector coverage policy" below). Reuse/refresh are both opt-in flags; the default computes and persists but never reuses. |
| Filtered Parquet writer | Done (Gate D) | `lake_snapshot_export.py` filters the four source tables by the closed cohort (VIN/listing membership, plus exact artifact row keys for artifact-only-seeded rows) and writes each table into a fresh, immutable, uniquely-named generation directory (`fingerprints/{export_fingerprint}/generations/{generation_id}/data/`) — never overwriting or deleting any previously published generation. A table read error removes that (still-unpublished) generation directory and reports failure; a fully-succeeded generation gets a `_SUCCESS` marker for future GC/audit tooling. The caller (`export_ci_lake_snapshot`) only "publishes" a generation by writing `manifest.json` to point at it, and refuses to do so if any table errored *or* the manifest write itself failed — both cases return `status="export_failed"`, never a false `"exported"`. This sidesteps the atomicity hazard of promoting in place (delete-then-copy could leave an already-published manifest pointing at missing/partial data mid-copy). Rows are written in a stable per-table sort order for reproducible file bytes. `lake_snapshot_export_cache.py` derives the export fingerprint, persists/loads the materialized manifest, and a cache-hit reload re-validates the fingerprint/`data_path`/required tables/table errors before trusting it (not just the schema version) — cheaply, from the manifest JSON alone, without re-listing or re-hashing every object. `reuse_export_cache`/`refresh_export_cache` mirror the planning-cache flag contract. |
| Manifest/package/upload | Done (Gate E) | `lake_snapshot_archive.py` packages the materialized Gate D export into `snapshot.tar.zst`, uploads it and a full `archive_manifest.json` to `snapshot_archives/fingerprints/{export_fingerprint}/`, and promotes `latest.json`/`aliases/{snapshot_id}.json` only after a successful publish. A non-dry-run `export_ci_lake_snapshot()` call now always runs packaging as the last step (for both a freshly materialized export and an export-cache hit), keyed by the same `export_fingerprint`. `reuse_archive_cache`/`refresh_archive_cache` mirror the Gate D flag contract; a checksum conflict at the same fingerprint (which should not normally happen, since packaging is a pure function of the materialized data) is refused rather than silently overwritten unless `refresh_archive_cache` is set. |
| Archiver endpoint | Control-plane only (Gate C.5) | Internal endpoint is wired and wrapped with `active_job()`. Cheap `audit_sources` calls remain allowed regardless of dry_run. `build_cohort=True` or any non-dry-run request is rejected with `409` unless `ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT=true`; production-sized work (which now includes Gate E packaging, since it runs as part of the same non-dry-run flow) must go through `snapshot-worker`. |
| Airflow DAG | Structurally done, worker target TBD | Manual DAG exists and passes params/defaults. It should trigger the isolated snapshot worker once Steps 4-6 are worker-safe — the DAG currently still posts to the archiver control-plane route, which will 409 if it requests `build_cohort=True` (or non-dry-run) without the override flag. |
| Ops download API | Done (Gate F, VM verification pending) | `ops/routers/snapshots.py` implements latest/manifest/download routes with bearer-token auth; see [Ops download API (Gate F)](#ops-download-api-gate-f). |
| Download/seed scripts | Mostly done | Offline/local mode exists and verifies checksums. API mode is exercised against the real ops router in `tests/scripts/test_download_lake_snapshot.py`; still needs a VM pilot run. |
| CI pilot | Not started | Needs a VM-verified live archive + ops download route round trip first. |

Current cross-plan handoff: Plan 112 Gate A4 (`Local Integration Harness`)
should consume the VM-verified Gate E output rather than inventing its own
snapshot packaging or download contract. A4 now has a stable archive +
manifest + checksum path so local MinIO/dbt/DuckDB/Lakekeeper/PySpark tests
can all run from the same production-shaped fixture that CI can also consume.

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
| `stale_listing` | Validates old listings and stale recency features (as-of `window_end`, bounded lookback — see "Selector coverage policy" below). |

This does not replace dbt unit tests. Unit tests protect exact branch behavior
with tiny synthetic examples. The snapshot protects real-world composition:
joins, nulls, duplicates, table closure, ordering, partition layout, and engine
differences across DuckDB/Spark/Delta.

---

## Cohort Closure

Selector queries produce seed entities. The exporter must then expand them into
a coherent entity closure before writing fixture Parquet.

**Closure rule (corrected after VM validation — see Current Status):** core
closure follows only vehicle-identity edges, repeated until stable:

```text
seed VINs
  -> all listing_ids ever associated with those VINs
  -> VINs for those listing_ids
  -> previous_listing_ids from remap events
  (repeat until no set grows)
```

Artifact IDs are **not** part of this loop. Once the core VIN/listing set is
frozen, artifact_ids are attached for diagnostics/provenance only:

```text
frozen core VINs/listing_ids
  -> artifact_ids observed on those VINs/listing_ids
  + explicit artifact selector roots (e.g. invalid_or_null_vin)
```

The reason: an artifact_id (e.g. an SRP/carousel page fetch) can legitimately
appear on many rows belonging to different, unrelated VINs/listings. Using
that co-occurrence to pull those VINs into the closure caused runaway growth
in VM validation (143 seed VINs -> 22,556 closed VINs across two passes).
Gate D's export writer inherits this same invariant: it never filters
`silver_observations` by a blanket `artifact_id IN (...)`, since that would
reintroduce the same pollution at materialization time. See
[Filter semantics (Gate D)](#filter-semantics-gate-d).

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

Closed VIN count may exceed seed VIN count because remaps or associated
listing histories can reveal additional VINs needed for consistency.
`artifact_count` is diagnostic/provenance context for the closed set, not a
driver of VIN/listing membership.

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
source window (see below), `target_vins`,
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

## Selector coverage policy (corrected)

A production VM run once had its Gate D export blocked because a handful of
rare selectors (e.g. `cooldown_bucket_11_plus`) didn't meet their configured
`min_entities` over a narrow historical window. A selector returning fewer
entities than desired is normal — especially for narrow windows or
inherently rare event selectors — so coverage shortfalls are now a
**warning by default**, not a blocking condition.

- Every real (non-dry-run) export, and any dry-run with
  `run_selectors=True`, always computes `coverage_failures` — the list of
  selectors below their configured minimum, with actual vs. required counts.
  This detail is preserved on `SnapshotResult.coverage_failures` and in the
  exported manifest regardless of whether coverage is enforced.
- `require_selector_coverage` (`SnapshotRequest` field, `--require-selector-coverage`
  CLI flag, API payload key; default `false`) is an explicit **opt-in strict/audit
  mode**. When set, a non-empty `coverage_failures` list short-circuits a
  real export with `status="coverage_failed"` *before* materialization —
  the same behavior the old `min_selector_coverage` flag had, just off by
  default and renamed to make the opt-in nature explicit.
- `require_selector_coverage` is validation policy only: it never changes
  cohort membership, selector SQL, or candidate collection, so it is
  deliberately excluded from the planning/export fingerprint (see "Planning
  cache" above) — toggling it does not invalidate or fragment the cache.
- This is orthogonal to real failures, which always block regardless of
  `require_selector_coverage`: a selector query erroring or its source table
  being missing/unreadable (`selector_diagnostics["errors"]` non-empty)
  returns `status="export_failed"` before materialization even runs, exactly
  like a materialization or manifest-publish failure does.

### `stale_listing` as-of semantics

`stale_listing` answers: "which listings' most recent observation at or
before the requested `window_end` is at least 30 days older than
`window_end`, and at most `lookback_days` (currently `60`) older." The
practical range this selector covers is **30 to 60 days stale**, not
open-ended staleness — see the lookback discussion below for why. Two things
this definition is *not*: it is not "stale as of right now" (no wall-clock
`now()` involved anywhere in the query), and it is not "stale relative to
whichever row happens to be newest in the filtered window" (the prior bug —
in a narrow one-month export window, a listing's last observation is almost
always inside that same window, so it can essentially never be 30 days
behind the newest row in it).

The fix reads a **bounded lookback** ending at `window_end` —
`[window_end - lookback_days, window_end]`, inclusive of `window_end` —
instead of the normal `[window_start, window_end)` filter every other
selector uses. `lookback_days` (currently `60`, see
`archiver/config/lake_snapshot_selectors.yml`) is configured explicitly and
kept comfortably larger than the 30-day stale threshold, so a listing's last
observation anywhere in the preceding two months is still found without an
unbounded full-table scan. This deliberately reads history from *before*
`window_start` — the exact rows a plain window filter would have excluded.
A listing whose last-ever observation predates even the lookback window
(i.e. staler than ~60 days) is not found by this selector at all — that's a
deliberate scope boundary of a bounded scan (avoiding an unbounded
full-table scan per the design constraint on this fix), not an
implementation bug; if "any staleness, however old" needs coverage later,
that requires a different retrieval strategy (e.g. a materialized
last-seen-per-listing index) and is out of scope here.

Because `selector_sql_hash`/`selector_config_hash` are part of the planning
fingerprint (see "Planning cache" above), this SQL/config change correctly
produced a new planning fingerprint — any previously persisted planning
cache entry is simply never matched by a request made after this fix, and
the next request for that window recomputes and persists a fresh entry. No
migration or manual cache invalidation is needed.

**Interaction with the snapshot export window.** A selected stale listing's
boundary (last-observation) row may predate `window_start`, so a blanket
`[window_start, window_end)` export filter would otherwise export that
listing with zero supporting `silver_observations` rows — selected into the
cohort but with no evidence for closure/materialization. `stale_listing` is
configured with `capture_boundary_row_key: true`, so
`lake_snapshot_cohort.collect_selector_candidates` captures the exact
`(artifact_id, vin, listing_id)` identity of each selected listing's
boundary row (the same row whose `fetched_at` established `last_seen_at`),
reusing the row-key mechanism `invalid_or_null_vin` already uses for
artifact-only-seeded rows. Gate D's writer
(`lake_snapshot_export._build_table_query`) matches these exact row keys
with a predicate that bypasses the time window entirely — unlike plain
`vin`/`listing_id` membership, which remains time-bound as before. This
guarantees the selected listing's boundary row is present in the export
regardless of how far before `window_start` it falls, while every other
row for that listing/vin (if any) stays subject to the normal window.

The captured row key's non-null `vin` also seeds `vin_seeds` directly in
`allocate_cohort` (`lake_snapshot_cohort.py`), not just `artifact_row_keys`.
This matters because `expand_entity_closure`'s vin<->listing_id lookups
(`_listing_ids_for_vins`/`_vins_for_listing_ids`) are themselves
window-bounded — without an explicit vin seed, a listing whose only
evidence predates `window_start` would never surface its vin through those
lookups, so the vehicle's other listings, price events, and relist/remap
history would be silently excluded from the export even though the stale
listing itself was selected. This is deliberately gated on
`capture_boundary_row_key` (not on having *any* row key at all):
`invalid_or_null_vin`'s artifact-only row keys must keep their existing
non-expanding behavior — a null/malformed vin, or artifact co-occurrence in
general, must never seed vin/listing closure (see `expand_entity_closure`'s
docstring on the artifact-fan-out regression this guards against). The two
kinds of row keys share the same `(artifact_id, vin, listing_id)` tuple
shape and the same `artifact_row_keys` pool for Gate D's export-match
purposes, but only a `capture_boundary_row_key` selector's keys additionally
seed closure.

Because this changes what `allocate_cohort` computes for the exact same
selector inputs, `COHORT_ALGORITHM_VERSION` was bumped `2` -> `3` (mirroring
the earlier artifact-fan-out bump described above) — a planning cache entry
persisted before this fix is a fingerprint miss, so it's never reused; the
next request for that window recomputes and persists a fresh entry with the
boundary vin correctly seeded.

---

## Export materialization cache (Gate D — implemented)

Gate C.75 caches the expensive answer to "which cohort should this snapshot
contain?" Gate D preserves that win by making the next expensive answer
cacheable too: "which Parquet fixture files represent this planned cohort under
the current export rules?"

`archiver/processors/lake_snapshot_export_cache.py` implements:

- `planning_fingerprint`: the existing Gate C.75 cache key (passed through).
- `export_fingerprint`: a SHA-256 key derived from `planning_fingerprint` plus
  writer semantics (`compute_export_fingerprint`).
- `materialized_snapshot_path` (`export_data_prefix`): the MinIO prefix for
  filtered Parquet, `{export_prefix}/fingerprints/{export_fingerprint}/data`.
- `manifest_path` (`export_manifest_path`): the manifest for the materialized
  output, `{export_prefix}/fingerprints/{export_fingerprint}/manifest.json`.
- `export_cache_hit`/`export_cache_action` on `SnapshotResult`, mirroring the
  planning-cache observability pattern (`"computed"`/`"reused"`/`"refreshed"`).
- `archive_path`: still Gate E — not yet implemented.

The export fingerprint hashes `EXPORT_ALGORITHM_VERSION`,
`OUTPUT_SCHEMA_VERSION`, `PARTITION_LAYOUT_VERSION`, `PARQUET_COMPRESSION`, and
the included table list, alongside `planning_fingerprint`. These are
code-level constants (this first-pass writer has no user-configurable output
format), so `SnapshotRequest` doesn't carry separate export-semantic fields —
only `reuse_export_cache`/`refresh_export_cache`/`export_cache_prefix`, which
control cache behavior, not output bytes. It excludes `snapshot_id`,
`dry_run`, `audit_sources`, and other run labels, mirroring the Gate C.75
exclusion rationale.

Implemented flow (`export_ci_lake_snapshot()`, non-dry-run branch):

```text
resolve or reuse planning artifact (same heavy path as dry_run+build_cohort)
if selector_diagnostics has errors: return status="export_failed"  # hard failure, always
if require_selector_coverage and coverage_failures: return status="coverage_failed"  # opt-in
derive export fingerprint from planning_fingerprint
if reuse_export_cache and a matching manifest exists:
  return manifest metadata (export_cache_action="reused") without scanning source Parquet
else:
  materialize_filtered_tables(...) writes filtered Parquet under the fingerprint prefix
  build + write the export manifest (row counts, file counts, checksums per table)
  return status="exported"
```

### Filter semantics (Gate D)

`materialize_filtered_tables` (`lake_snapshot_export.py`) filters each source
table by the closed cohort:

| Table | Predicate |
|-------|-----------|
| `silver_observations` | `vin IN closed_vins` OR `listing_id IN listing_ids` OR exact `(artifact_id, vin, listing_id) IN artifact_row_keys` |
| `price_observation_events` | `vin IN closed_vins` OR `listing_id IN listing_ids` |
| `vin_to_listing_events` | `vin IN closed_vins` OR `listing_id IN listing_ids` |
| `blocked_cooldown_events` | `listing_id IN listing_ids` |

`artifact_row_keys` is a set of exact `(artifact_id, vin, listing_id)` tuples
captured during selector candidate collection for artifact_id-keyed selectors
(`CandidateSet.selected_row_keys`, currently populated only for
`invalid_or_null_vin`) — never a bare `artifact_id IN (...)` filter. The
distinction matters: a bare artifact_id filter would match every row sharing
that artifact_id, including unrelated VINs on the same SRP/carousel page —
exactly the pollution the cohort-closure fix removed, just reintroduced at
materialization time instead. The exact-tuple filter matches only the
specific flagged row, e.g. an `invalid_or_null_vin` row with a NULL vin (using
`IS NOT DISTINCT FROM` for NULL-safe equality).

`cohort.artifact_ids` (the diagnostic/provenance set) is deliberately **not**
used as a table filter — only `cohort.artifact_row_keys` is. This is tested
directly in both the unit suite (`tests/archiver/test_lake_snapshot_export.py`)
with small local Parquet fixtures, and the integration suite
(`tests/integration/archiver/test_lake_snapshot_export.py`) against the real
MinIO fixture's `ARTIFACT_SRP_SHARED` co-occurrence scenario.

Gate E packages from the materialized snapshot path and writes the archive
under `snapshot_archives/fingerprints/{export_fingerprint}/`; see
[Packaging and upload (Gate E)](#packaging-and-upload-gate-e) below.

This gives us three reusable layers:

```text
planning fingerprint -> cohort plan JSON
export fingerprint   -> filtered Parquet fixture directory + manifest
export fingerprint   -> packaged archive + archive manifest (Gate E)
```

The friendly `snapshot_id` can still be stored in manifests and alias files,
but it should never force recomputation by itself.

---

## Packaging and upload (Gate E)

`archiver/processors/lake_snapshot_archive.py` implements Gate E: packaging a
materialized Gate D export into an archive and publishing it, plus the
friendly `latest.json`/alias pointers. It mirrors the dual-mode pattern
`lake_snapshot_export.py` already uses — a local `base_path` reads/writes
plain files (fast, MinIO-free unit tests), and `base_path=None` reads
Parquet via `shared.minio.get_s3fs()` and writes objects via new
`shared.minio` helpers (`write_bytes`, `read_bytes`, `object_size`).

**Archive contents.** `snapshot.tar.zst` contains the Gate D export manifest
as `manifest.json` at the archive root, plus every file under the
materialized `data_path`, added in a stable sorted order with fixed
mtime/uid/gid/mode on every member — so identical materialized data always
produces byte-identical archives. The in-archive `manifest.json`
deliberately omits archive checksum/size (those can't be known until the
archive containing them is itself finished), matching the "Packaging"
section above.

**Archive identity.** The archive is keyed by `export_fingerprint` directly
— packaging is a pure function of the materialized export, so no separate
archive fingerprint is needed:

```text
snapshot_archives/
  fingerprints/{export_fingerprint}/
    snapshot.tar.zst
    archive_manifest.json
```

`archive_manifest.json` is the Gate D export manifest plus an appended
`archive: {path, bytes, sha256, file_count}` block — this is the manifest
`scripts/lake_snapshot_common.py`'s `get_archive_meta` already knows how to
read (it tolerates both this nested shape and a flatter
`archive_sha256`-at-top-level shape), so `scripts/download_lake_snapshot.py`
and `scripts/seed_lake_snapshot.py` needed no changes for Gate E.

**Reuse/refresh.** `reuse_archive_cache` (skip repackaging entirely when a
valid, size-verified existing archive is found) and `refresh_archive_cache`
(force overwrite) mirror the Gate D flag contract. Unlike Gate D's uniquely
named generation directories, there is exactly one canonical archive object
per `export_fingerprint`, so this module never blindly overwrites an
existing valid archive: if a freshly built archive's sha256 differs from one
already published at the same fingerprint (which should not normally
happen, since packaging is a pure function of the materialized data, but is
defended against regardless), packaging fails with an explicit conflict
error rather than silently clobbering it — unless the caller passed
`refresh_archive_cache=True`. An identical rebuild (same bytes) is a
harmless no-op, reported as `cache_action="reused"`.

**Validation on reuse.** `load_archive_manifest` checks the manifest JSON's
own schema version, fingerprint, and field completeness cheaply, plus one
size check of the actual archive object (via `object_size`/`os.path.getsize`
— no HEAD-equivalent-free full download) against the manifest's recorded
`archive.bytes`. Any mismatch is treated as a miss, never silently trusted.

**Safety.** File listing under a materialized `data_path`
(`list_data_files`) never follows symlinks and rejects any relative path
that would traverse outside `data_path`, mirroring the safe-extraction guard
`scripts/lake_snapshot_common.py:safe_extract_tar_zst` already applies on
unpack.

**Wiring.** `export_ci_lake_snapshot()`'s non-dry-run path now always calls
`package_snapshot_archive()` as its last step (after a successful
materialize-or-reuse and manifest publish), then
`promote_snapshot_pointers()` only if packaging succeeded — writing the
per-snapshot alias file before `latest.json`, so a mid-failure never leaves
`latest.json` pointing at a snapshot with no alias. Both steps run inside
the same non-dry-run flow that Gate C.5 already restricts to
`snapshot-worker` (the production archiver rejects any non-dry-run request
with `409` unless `ARCHIVER_ALLOW_SYNC_SNAPSHOT_COHORT=true`), so no
additional isolation gate was needed for packaging specifically.
`SnapshotResult` exposes `archive_key`, `archive_bytes`, `archive_sha256`,
`archive_manifest_key`, `archive_cache_hit`, and `archive_cache_action`.

**Downstream consumer.** Plan 112 Gate A4 (local integration harness) should
consume this archive + `archive_manifest.json` contract directly — download
it (or point at a local copy) via `scripts/download_lake_snapshot.py`/
`scripts/seed_lake_snapshot.py`, exactly as CI would, rather than inventing
a second snapshot packaging or download format.

---

## Ops download API (Gate F)

`ops/routers/snapshots.py` implements Gate F: a small, read-only facade over
the Gate E `ci_snapshots/adaptive_refresh/` pointers and
`snapshot_archives/fingerprints/{export_fingerprint}/` objects. It never
generates, mutates, or promotes a snapshot — every route is a pure read of
objects Gate E already published; snapshot generation still only happens
through `snapshot-worker`/the archiver control-plane route.

**Routes** (mounted at `/admin/snapshots/adaptive-refresh` in the `ops`
container, matching `scripts/download_lake_snapshot.py`'s hardcoded
`_SNAPSHOTS_PATH`):

```http
GET /admin/snapshots/adaptive-refresh/latest
GET /admin/snapshots/adaptive-refresh/{snapshot_id}
GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download
```

| Route | Behavior |
|-------|----------|
| `GET latest` | Reads `ci_snapshots/adaptive_refresh/latest.json` and returns it verbatim (the pointer dict `promote_snapshot_pointers` writes: `snapshot_id`, `export_fingerprint`, `archive_key`, `archive_manifest_key`, `archive_bytes`, `archive_sha256`, `created_at`). 404 if no snapshot has ever been published. |
| `GET {snapshot_id}` | Reads `ci_snapshots/adaptive_refresh/aliases/{snapshot_id}.json` for the pointer, follows its `archive_manifest_key`, and returns that `archive_manifest.json` (the Gate D export manifest plus the `archive: {path, bytes, sha256, file_count}` block). 404 if the alias or manifest is missing. |
| `GET {snapshot_id}/download` | Resolves the same alias, then streams the object at `archive_key` (`snapshot.tar.zst`) via `shared.minio.open_stream` (chunked, not loaded fully into memory). 404 if the alias or the archive object is missing. |

**Auth.** A standalone bearer token (`SNAPSHOT_DOWNLOAD_TOKEN` env var),
independent of the cookie/session admin auth in `ops/routers/auth.py` — CI
callers and `scripts/download_lake_snapshot.py` have no browser session to
present. Missing/malformed `Authorization` header is `401`; a
present-but-wrong token is `403`; an unconfigured (empty) token disables the
routes entirely with `503`. The token is never logged. This matches
`scripts/download_lake_snapshot.py`'s existing `--token`/`$CARTRACKER_SNAPSHOT_TOKEN`
contract, so no downloader changes were needed.

**Proxy routing.** Because these endpoints live under `/admin/...` but are
called by scripts, `Caddyfile` has a dedicated
`handle /admin/snapshots/adaptive-refresh*` block before the generic
OAuth-protected `/admin*` block. It reverse-proxies directly to `ops:8060`
so the route's bearer-token auth runs in FastAPI instead of Caddy redirecting
script callers to `/oauth2/sign_in`.

**Path safety.** `snapshot_id` is validated against
`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` (no `/`, no `..`) before it is used to
build the `aliases/{snapshot_id}.json` key — an invalid id is rejected with
`400` before any MinIO read is attempted. The alias pointer's own
`archive_manifest_key`/`archive_key` fields are *also* re-validated (against
`^snapshot_archives/fingerprints/[A-Za-z0-9]{1,128}/archive_manifest\.json$`
and the equivalent `snapshot.tar.zst` pattern) before either is passed to
`read_json`/`object_size`/`open_stream` — the alias object is stored data,
not caller input, but a corrupted or tampered alias must not be able to
redirect an authenticated request to read or stream an arbitrary MinIO key
(e.g. an `s3://` URI, an absolute path, a `..`-traversal, or an
out-of-prefix object). A key that fails this check is treated the same as
"not found" (`404`). The alias and manifest's own `snapshot_id` fields are
also cross-checked against the requested `snapshot_id` (`alias.snapshot_id`
in `_resolve_alias`, `manifest.snapshot_id` in `get_snapshot_manifest`) — a
corrupted or mismatched alias/manifest object must never silently serve a
different snapshot's manifest or archive under this snapshot_id's URL.

**Download headers.** `Content-Type: application/zstd`,
`Content-Disposition: attachment; filename="{snapshot_id}.tar.zst"`,
`Content-Length` (from `shared.minio.object_size`, checked before opening
the stream so a missing archive is a clean `404` rather than a
mid-stream error), and `X-Archive-SHA256` (from the alias pointer's
`archive_sha256`) for client-side verification alongside the manifest
checksum `scripts/lake_snapshot_common.py` already checks.

**Testing.** `tests/ops/routers/test_snapshots.py` covers auth (missing/wrong
token, unconfigured token), latest success/missing, manifest resolution
through the alias, missing alias/manifest, download streaming/missing
archive, and invalid/path-traversal `snapshot_id` rejection — all against
mocked `shared.minio` calls, no real MinIO required.
`tests/scripts/test_download_lake_snapshot.py::TestDownloadApiAgainstOpsRouter`
additionally runs `scripts/download_lake_snapshot.py`'s `download_api()`
against the real `ops` FastAPI app (via `fastapi.testclient.TestClient`,
MinIO calls mocked) to prove the route shapes match the downloader's
expectations on the wire, not just in a hand-rolled mock transport.

**VM verification (not yet run).** Once a real Gate E archive exists on the
VM:

```bash
# 1. Latest pointer
curl -sS -H "Authorization: Bearer $SNAPSHOT_DOWNLOAD_TOKEN" \
  https://cartracker.info/admin/snapshots/adaptive-refresh/latest

# 2. Manifest for that snapshot id (use snapshot_id from step 1's response)
curl -sS -H "Authorization: Bearer $SNAPSHOT_DOWNLOAD_TOKEN" \
  https://cartracker.info/admin/snapshots/adaptive-refresh/<snapshot_id>

# 3. Download the archive
curl -sS -H "Authorization: Bearer $SNAPSHOT_DOWNLOAD_TOKEN" \
  https://cartracker.info/admin/snapshots/adaptive-refresh/<snapshot_id>/download \
  -o /tmp/snapshot.tar.zst

# 4. Full round trip through the downloader script
python scripts/download_lake_snapshot.py --latest \
  --base-url https://cartracker.info --token "$SNAPSHOT_DOWNLOAD_TOKEN"

# 5. Optional: seed local MinIO from the downloaded archive
python scripts/seed_lake_snapshot.py \
  --snapshot-id <snapshot_id> \
  --manifest-path .cache/lake_snapshots/<snapshot_id>/manifest.json
```

Expected result: step 1's `snapshot_id` matches step 4's resolved snapshot;
step 3's downloaded bytes sha256-match step 2's manifest `archive.sha256`;
step 4 exits 0 and prints the destination archive path.

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
| `archiver/processors/lake_snapshot_archive.py` | Gate E: packages a materialized export into `snapshot.tar.zst`, uploads it plus `archive_manifest.json`, and promotes `latest.json`/alias pointers |
| `archiver/processors/lake_snapshot_selectors.py` | Optional selector registry if the processor grows |
| `archiver/app.py` | Internal snapshot generation route |
| `airflow/dags/export_ci_lake_snapshot.py` | Paused/manual snapshot generation DAG |
| `scripts/download_lake_snapshot.py` | New downloader |
| `scripts/seed_lake_snapshot.py` | New CI/local seeder |
| `ops/routers/snapshots.py` | Snapshot metadata/download routes |
| `ops/app.py` | Include snapshot router |
| `.github/workflows/ci.yml` | Optional medium-snapshot integration job |
| `tests/ops/routers/test_snapshots.py` | API auth/download tests |
| `shared/minio.py` | Adds `open_stream()` for chunked archive download without full in-memory buffering |
| `tests/scripts/test_download_lake_snapshot.py` | Adds a real-app (`TestClient`) round trip proving downloader/ops route wire compatibility |
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
