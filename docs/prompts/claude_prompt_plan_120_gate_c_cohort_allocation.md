# Claude Prompt: Plan 120 Gate C Cohort Allocation And Closure

You are working in the `cartracker-scraper` repo on branch `feature/plan-112-refresh-backtesting`.

Plan 120 Gate B is complete: selector readiness is implemented, selector SQL lives in `archiver/sql/lake_snapshot_selectors/*.sql`, selector metadata lives in `archiver/config/lake_snapshot_selectors.yml`, and selector config loading/validation is separated into `archiver/processors/lake_snapshot_selector_config.py`.

Runtime VM smoke has also passed:

- source audit works against MinIO from the archiver container
- selector diagnostics execute against production MinIO with `run_selectors=true`
- `selector_diagnostics.errors == []`
- one selector, `cooldown_bucket_11_plus`, currently has zero entities in the 6-month prod window, so keep support for `min_selector_coverage=false` while testing

This prompt is for **Gate C only**.

## Gate C Goal

Turn selector candidate output into a coherent `SnapshotCohort` with:

- seed VINs
- closed VINs
- listing IDs
- artifact IDs
- selected rows/events diagnostics
- selector coverage diagnostics
- deterministic fill behavior

Do **not** write filtered Parquet yet. Do **not** package or upload archives yet. Those are Gate D/E.

Gate C is complete when selector candidates can be allocated and expanded into a logically closed cohort that later gates can materialize.

## Existing Files To Build On

Relevant existing files:

- `archiver/processors/export_ci_lake_snapshot.py`
- `archiver/processors/lake_snapshot_selectors.py`
- `archiver/processors/lake_snapshot_selector_config.py`
- `archiver/processors/lake_source_audit.py`
- `archiver/config/lake_snapshot_selectors.yml`
- `archiver/sql/lake_snapshot_selectors/*.sql`
- `tests/archiver/test_export_ci_lake_snapshot.py`

Use existing patterns:

- DuckDB + MinIO reads via `shared.duckdb_s3.get_duckdb_s3_connection`
- local fixture mode via `base_path` / `source_base_path`
- selector execution via `run_lake_selectors` / `build_selector_query`
- request/result contract in `SnapshotRequest` / `SnapshotResult`

## Required Data Shapes

Add typed dataclasses or equivalent simple structures. Suggested shape:

```python
@dataclass(frozen=True)
class CandidateSet:
    selector_name: str
    entity_key: str
    required: int
    entities: tuple[Any, ...]
    candidate_rows: int
    selected_entities: tuple[Any, ...]
    status: str
    error: str | None = None


@dataclass(frozen=True)
class SnapshotCohort:
    seed_vins: frozenset[str]
    closed_vins: frozenset[str]
    listing_ids: frozenset[str]
    artifact_ids: frozenset[int]
    selector_coverage: dict[str, dict[str, Any]]
    diagnostics: dict[str, Any]
```

You can adjust names/types if the existing codebase style suggests something better, but keep the concepts clear.

## Required Functions

Implement clear functions, probably in `archiver/processors/export_ci_lake_snapshot.py` unless the file becomes too large.

Suggested functions:

```python
collect_selector_candidates(...)
allocate_cohort(...)
expand_entity_closure(...)
build_snapshot_cohort(...)
```

If needed, add a small helper module such as:

```text
archiver/processors/lake_snapshot_cohort.py
```

Prefer a small helper module if the exporter starts mixing request handling with heavy cohort SQL.

## Selector Candidate Collection

The current `run_lake_selectors()` returns counts and a 5-entity sample only. Gate C needs the actual candidate entities, not just samples.

Add a way to collect bounded candidate entities for each selector.

Requirements:

- use the existing selector SQL from `build_selector_query`
- preserve source window filtering
- support local fixture mode via `base_path`
- support MinIO mode via the existing DuckDB helper
- cap per-selector candidates so a common selector cannot dominate memory
- deterministic ordering, ideally by entity key / stable SQL order
- retain diagnostics: candidate rows, total entities, selected entity count, required count, status, error

Do not pull millions of candidate rows into Python when only selected entities are needed. Use DuckDB aggregation/limit queries where practical.

## Cohort Allocation

Allocation rules:

1. Start by selecting up to each selector's required minimum entities.
2. Deduplicate across selectors.
3. Track which selectors each selected entity satisfied.
4. Respect `target_vins` as the primary size goal.
5. Fill remaining capacity with deterministic representative VINs if selector seeds do not reach `target_vins`.
6. Keep deterministic behavior across repeated runs against the same source data.

For deterministic fill, use a stable SQL ordering such as `ORDER BY md5(vin)` or another deterministic hash/order available in DuckDB. Avoid Python random unless it is seeded and stable.

Important: selectors have different `entity_key`s:

- some produce `vin`
- some produce `listing_id`
- some produce `artifact_id`
- benchmark selectors produce `make_model`

Gate C must normalize allocated selector entities into a cohort. For non-VIN selectors, use source data to resolve related VINs/listing IDs/artifact IDs during closure.

It is acceptable in the first pass for benchmark `make_model` selectors to seed representative VINs/listings from rows matching the selected make/model groups, as long as this is deterministic and tested.

## Entity Closure Rules

Implement the closure described in Plan 120:

```text
seed VINs
  -> all listing_ids ever associated with those VINs
  -> previous_listing_ids from remap events
  -> artifact_ids tied to selected listing_ids
  -> price events tied to selected VINs/listing_ids
  -> blocked cooldown events tied to selected listing_ids
  -> observation rows tied to selected VINs/listing_ids/artifact_ids
```

Practically, use the four currently supported source tables:

- `silver_observations`
- `price_observation_events`
- `vin_to_listing_events`
- `blocked_cooldown_events`

Closure should be iterative enough to handle relisted VIN/listing relationships:

- selected VINs can reveal listing IDs
- selected listing IDs can reveal VINs
- remap events can reveal previous listing IDs
- newly found listing IDs can reveal more observations/artifacts

Add a bounded closure loop with diagnostics such as:

```json
{
  "closure_passes": 2,
  "seed_vins": 500,
  "closed_vins": 542,
  "listing_ids": 812,
  "artifact_ids": 12345,
  "previous_listing_ids_added": 74
}
```

Avoid unbounded loops. Stop when no set grows or after a small max pass count with a clear diagnostic/error.

## Exporter Integration

Wire Gate C into `export_ci_lake_snapshot()` only for dry-run planning.

For now:

- `dry_run=True` and `run_selectors=True` should continue returning selector diagnostics
- add cohort diagnostics when cohort allocation is requested or when `run_selectors=True`
- non-dry-run should still return `not_implemented` until Gate D/E exist
- do not write files yet

If adding a request flag is useful, prefer a conservative name like:

```python
build_cohort: bool = False
```

But do not make existing cheap dry-runs expensive by default.

A reasonable behavior:

- `dry_run=True`, `run_selectors=False`, `build_cohort=False`: cheap planned result
- `dry_run=True`, `run_selectors=True`, `build_cohort=False`: selector diagnostics only
- `dry_run=True`, `run_selectors=True`, `build_cohort=True`: selector diagnostics + cohort diagnostics

Update the FastAPI archiver route if a new flag is added.

## SnapshotResult / Manifest Skeleton

Extend `SnapshotResult` only as needed for Gate C.

Suggested additions:

- `seed_vin_count`
- `closed_vin_count`
- `listing_count`
- `artifact_count`
- `cohort_diagnostics`

The existing fields already include count slots; populate them for dry-run cohort builds.

Do not finalize archive fields yet:

- `archive_bytes`
- `manifest_key`
- `archive_key`

Those remain `None` until later gates.

## Tests

Add focused local tests in `tests/archiver/test_export_ci_lake_snapshot.py` or a new test file if cleaner.

Required test coverage:

1. Candidate collection returns real selected entities, not just 5 samples.
2. Allocation deduplicates entities across selectors.
3. Allocation is deterministic across repeated runs.
4. Allocation respects `target_vins`.
5. Closure adds listing IDs for selected VINs.
6. Closure adds previous listing IDs from `vin_to_listing_events.previous_listing_id`.
7. Closure adds artifact IDs from selected observations/events.
8. Closure can resolve non-VIN selector seeds such as `listing_id` and `artifact_id`.
9. Empty/short selector candidates do not crash cohort building.
10. `cooldown_bucket_11_plus` with zero candidates does not break cohort building when `min_selector_coverage=False`.
11. Dry-run without `build_cohort` remains cheap and does not run closure.
12. Dry-run with `build_cohort=True` returns cohort counts/diagnostics.

Use existing local Parquet fixture patterns from `tests/archiver/test_export_ci_lake_snapshot.py`.

## Existing Observations To Preserve/Handle

From VM selector smoke:

- `cooldown_bucket_11_plus` currently has zero entities for a 6-month production window.
  - This should be a coverage failure only when `min_selector_coverage=True`.
  - It should not be a runtime error.
- `benchmark_dense_make_model` sampled `" "` from production, meaning blank make/model values exist.
  - Do not make this a Gate C blocker.
  - If tightening benchmark SQL is trivial and covered, it is okay, but avoid scope creep.

## Out Of Scope

Do not:

- write filtered Parquet output
- write expected outputs
- compute table checksums
- create `.tar.zst`
- upload to MinIO
- update `latest.json`
- add ops download routes
- change dbt models
- change selector SQL semantics unless absolutely required for cohort closure
- remove selectors
- change existing selector thresholds without explicit reason

## Validation Commands

Run:

```powershell
ruff check archiver\processors\export_ci_lake_snapshot.py archiver\processors\lake_snapshot_selectors.py archiver\processors\lake_snapshot_selector_config.py tests\archiver\test_export_ci_lake_snapshot.py tests\integration\dbt\test_selector_dbt_equivalence.py
.\.venv\Scripts\python.exe -m pytest tests\archiver\test_export_ci_lake_snapshot.py -q
.\.venv\Scripts\python.exe -m pytest tests\integration\dbt\test_selector_dbt_equivalence.py -q
```

The dbt integration test may skip locally if `MINIO_ENDPOINT` and `DUCKDB_PATH` are not set. That is expected.

## Completion Criteria

Gate C is complete when:

- selector candidates can be collected as bounded entity sets
- a deterministic seed cohort can be allocated
- entity closure expands selected seeds into VIN/listing/artifact sets
- dry-run can return cohort counts/diagnostics
- non-dry-run still clearly returns `not_implemented`
- focused tests pass
- no archive/parquet writing is implemented yet
