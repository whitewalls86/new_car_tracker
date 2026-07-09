"""
Cohort allocation and entity closure for CI lake snapshot exports
(Plan 120 Gate C).

Turns bounded selector candidate pools into a deterministic seed cohort, then
expands (closes) that cohort across the four supported source tables so a
later gate can materialize filtered Parquet from a logically complete set of
VINs/listing_ids/artifact_ids. No filtered Parquet is written here.
"""
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from archiver.processors.lake_snapshot_selectors import (
    _SELECTOR_CONFIGS,
    RUNNABLE_SELECTORS,
    Selector,
    build_selector_query,
    build_selector_registry,
)
from archiver.processors.lake_snapshot_sql import in_clause, table_time_where
from archiver.processors.lake_source_audit import resolve_table_path
from shared.duckdb_s3 import get_duckdb_s3_connection

logger = logging.getLogger("archiver")

DEFAULT_CANDIDATE_CAP = 2000
MAX_CLOSURE_PASSES = 5
MAKE_MODEL_REPRESENTATIVE_LIMIT = 50

_VIN_LISTING_TABLES: Tuple[Tuple[str, str], ...] = (
    ("silver_observations", "fetched_at"),
    ("price_observation_events", "event_at"),
    ("vin_to_listing_events", "event_at"),
)


@dataclass(frozen=True)
class CandidateSet:
    selector_name: str
    entity_key: str
    required: int
    entities: Tuple[Any, ...]
    candidate_rows: int
    selected_entities: Tuple[Any, ...]
    status: str
    error: Optional[str] = None
    # True distinct entity count (may exceed len(entities) when the pool is
    # larger than candidate_cap). Defaults to len(entities) for callers/tests
    # that build a CandidateSet directly from an already-bounded list.
    entity_count: int = -1
    # Exact (artifact_id, vin, listing_id) row identity for each selected
    # entity, populated only when entity_key == "artifact_id" (e.g.
    # invalid_or_null_vin). Gate D's export writer uses this to include the
    # exact flagged row without a blanket `artifact_id IN (...)` table
    # filter, which would also pull in unrelated rows that happen to share
    # the same artifact_id (e.g. an SRP/carousel page's other listings).
    selected_row_keys: Tuple[Tuple[Any, Any, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.entity_count == -1:
            object.__setattr__(self, "entity_count", len(self.entities))


@dataclass(frozen=True)
class CohortAllocation:
    vin_seeds: FrozenSet[str]
    listing_seeds: FrozenSet[str]
    artifact_seeds: FrozenSet[Any]
    make_model_seeds: FrozenSet[str]
    selector_coverage: Dict[str, Dict[str, Any]]
    fill_vins_added: int
    pre_fill_vin_count: int
    required_vin_seeds: FrozenSet[str]
    # Exact (artifact_id, vin, listing_id) row identities for artifact_id-keyed
    # selectors (e.g. invalid_or_null_vin). Used by the Gate D export writer
    # to include the exact flagged row without a blanket artifact_id table
    # filter. See CandidateSet.selected_row_keys.
    artifact_row_keys: FrozenSet[Tuple[Any, Any, Any]] = frozenset()


@dataclass(frozen=True)
class SnapshotCohort:
    seed_vins: FrozenSet[str]
    closed_vins: FrozenSet[str]
    listing_ids: FrozenSet[str]
    artifact_ids: FrozenSet[Any]
    selector_coverage: Dict[str, Dict[str, Any]]
    diagnostics: Dict[str, Any]
    artifact_row_keys: FrozenSet[Tuple[Any, Any, Any]] = frozenset()


def open_duckdb_connection(base_path: Optional[str]):
    """Open a DuckDB connection: local for fixture mode, MinIO/S3 otherwise."""
    if base_path:
        import duckdb
        return duckdb.connect()
    return get_duckdb_s3_connection()


# ---------------------------------------------------------------------------
# Candidate collection
# ---------------------------------------------------------------------------

def _wrap_candidate_query(candidate_sql: str, entity_key: str, cap: int) -> str:
    cap = int(cap)
    return f"""
WITH selector_candidates AS (
{candidate_sql}
),
distinct_entities AS (
    SELECT DISTINCT {entity_key} AS entity_value
    FROM selector_candidates
    WHERE {entity_key} IS NOT NULL
)
SELECT
    (SELECT count(*) FROM selector_candidates) AS candidate_rows,
    (SELECT count(*) FROM distinct_entities) AS entities,
    (
        SELECT list(entity_value)
        FROM (
            SELECT entity_value
            FROM distinct_entities
            ORDER BY entity_value
            LIMIT {cap}
        ) AS bounded
    ) AS bounded_entities
"""


def _selected_row_keys_for_candidates(
    con, candidate_sql: str, params: List[Any], key_column: str, selected: Tuple[Any, ...],
) -> Tuple[Tuple[Any, Any, Any], ...]:
    """Resolve exact (artifact_id, vin, listing_id) row identity for each
    selected entity, matched by *key_column* (the selector's own entity_key
    column — `artifact_id` or `listing_id`).

    For an artifact_id-keyed selector, this lets the Gate D export writer
    match the precise flagged row instead of filtering by a bare artifact_id
    (which could also match unrelated rows sharing that artifact_id, e.g. an
    SRP/carousel page). For a `capture_boundary_row_key` listing_id-keyed
    selector (e.g. stale_listing), this instead captures the exact boundary
    row (e.g. the last-observation row establishing staleness), which may
    predate window_start — an exact row-key match lets the Gate D writer
    export that row despite the snapshot window filter (see
    `lake_snapshot_export._build_table_query`), so a selected listing isn't
    left with zero supporting rows.

    Assumes the selector's candidate SQL exposes vin/artifact_id/listing_id
    columns, which holds for every selector using this mechanism."""
    if not selected:
        return ()
    clause, clause_params = in_clause(key_column, selected)
    query = (
        f"SELECT DISTINCT artifact_id, vin, listing_id FROM ({candidate_sql}) AS c "
        f"WHERE {clause}"
    )
    try:
        rows = con.execute(query, params + clause_params).fetchall()
        return tuple((r[0], r[1], r[2]) for r in rows)
    except Exception as e:
        logger.warning(
            "lake_snapshot_cohort: selected_row_keys_for_candidates key_column=%s error=%s",
            key_column, e,
        )
        return ()


def collect_selector_candidates(
    con,
    name: str,
    base_path: Optional[str],
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
    registry: Optional[Dict[str, Selector]] = None,
    candidate_cap: int = DEFAULT_CANDIDATE_CAP,
) -> CandidateSet:
    """Collect a bounded, deterministically-ordered pool of candidate
    entities for one selector, and pre-select up to its required minimum.

    Never raises for a missing/unreadable source table or bad query — the
    error is captured on the returned CandidateSet, mirroring
    lake_snapshot_selectors.run_selector.
    """
    registry = registry or build_selector_registry()
    selector = registry[name]
    config = _SELECTOR_CONFIGS[name]
    path = resolve_table_path(config.source_table, base_path)

    t0 = time.monotonic()
    logger.info(
        "lake_snapshot_cohort: selector=%s start entity_key=%s required=%s",
        name, selector.entity_key, selector.min_entities,
    )
    try:
        extra_paths = None
        if config.extra_source_tables:
            extra_paths = {
                f"{extra_table}_path": resolve_table_path(extra_table, base_path)
                for extra_table in config.extra_source_tables
            }
        candidate_sql, params = build_selector_query(
            name, path, window_start, window_end, extra_paths=extra_paths
        )
        query = _wrap_candidate_query(candidate_sql, selector.entity_key, candidate_cap)
        candidate_rows, entity_count, entities = con.execute(query, params).fetchone()
        entities = tuple(entities) if entities else ()
        selected = entities[: selector.min_entities]
        status = "pass" if entity_count >= selector.min_entities else "fail"
        selected_row_keys: Tuple[Tuple[Any, Any, Any], ...] = ()
        if selector.entity_key == "artifact_id" and selected:
            selected_row_keys = _selected_row_keys_for_candidates(
                con, candidate_sql, params, "artifact_id", selected,
            )
        elif config.capture_boundary_row_key and selected:
            selected_row_keys = _selected_row_keys_for_candidates(
                con, candidate_sql, params, "listing_id", selected,
            )
        logger.info(
            "lake_snapshot_cohort: selector=%s end elapsed_s=%.2f entities=%s "
            "candidate_rows=%s status=%s",
            name, time.monotonic() - t0, entity_count, candidate_rows, status,
        )
        return CandidateSet(
            selector_name=name,
            entity_key=selector.entity_key,
            required=selector.min_entities,
            entities=entities,
            candidate_rows=candidate_rows,
            selected_entities=selected,
            status=status,
            entity_count=entity_count,
            selected_row_keys=selected_row_keys,
        )
    except Exception as e:
        logger.warning(
            "lake_snapshot_cohort: selector=%s error elapsed_s=%.2f path=%s error=%s",
            name, time.monotonic() - t0, path, e,
        )
        return CandidateSet(
            selector_name=name,
            entity_key=selector.entity_key,
            required=selector.min_entities,
            entities=(),
            candidate_rows=0,
            selected_entities=(),
            status="fail",
            error=str(e),
        )


def collect_all_selector_candidates(
    con,
    names: Optional[List[str]] = None,
    base_path: Optional[str] = None,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
    candidate_cap: int = DEFAULT_CANDIDATE_CAP,
) -> Dict[str, CandidateSet]:
    """Collect bounded candidate sets for every requested selector."""
    names = list(names) if names is not None else list(RUNNABLE_SELECTORS)
    registry = build_selector_registry()
    t0 = time.monotonic()
    logger.info(
        "lake_snapshot_cohort: collect_all_selector_candidates start selectors=%d "
        "window_start=%s window_end=%s",
        len(names), window_start.isoformat() if window_start else None,
        window_end.isoformat() if window_end else None,
    )
    result = {
        name: collect_selector_candidates(
            con, name, base_path, window_start, window_end,
            registry=registry, candidate_cap=candidate_cap,
        )
        for name in names
    }
    logger.info(
        "lake_snapshot_cohort: collect_all_selector_candidates end elapsed_s=%.2f "
        "selectors=%d",
        time.monotonic() - t0, len(names),
    )
    return result


def candidate_sets_to_selector_diagnostics(
    candidate_sets: Dict[str, "CandidateSet"], base_path: Optional[str]
) -> Dict[str, Any]:
    """Convert already-collected CandidateSets into the same diagnostics shape
    returned by `lake_snapshot_selectors.run_lake_selectors`, so cohort
    building and selector diagnostics can share one scan instead of two."""
    selectors: Dict[str, Any] = {}
    errors: List[str] = []
    for name, candidate in candidate_sets.items():
        config = _SELECTOR_CONFIGS[name]
        path = resolve_table_path(config.source_table, base_path)
        selectors[name] = {
            "selector": name,
            "entity_key": candidate.entity_key,
            "required": candidate.required,
            "path": path,
            "candidate_rows": candidate.candidate_rows,
            "entities": candidate.entity_count,
            "sample_entities": list(candidate.entities[:5]),
            "status": candidate.status,
            "error": candidate.error,
        }
        if candidate.error is not None:
            errors.append(f"{name}: {candidate.error}")
    return {"selectors": selectors, "errors": errors, "ok": len(errors) == 0}


# ---------------------------------------------------------------------------
# Cohort allocation
# ---------------------------------------------------------------------------

def _selector_coverage_entry(candidate: CandidateSet) -> Dict[str, Any]:
    return {
        "entity_key": candidate.entity_key,
        "required": candidate.required,
        "found": len(candidate.entities),
        "selected": len(candidate.selected_entities),
        "status": candidate.status,
        "error": candidate.error,
    }


def _fill_representative_vins(
    con,
    base_path: Optional[str],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    exclude: set,
    limit: int,
) -> List[str]:
    """Deterministically pick representative VINs (stable md5(vin) order) to
    fill remaining target_vins capacity when selector seeds fall short."""
    if limit <= 0:
        return []
    path = resolve_table_path("silver_observations", base_path)
    where_clauses = ["vin IS NOT NULL"]
    params: List[Any] = []
    if window_start is not None:
        where_clauses.append("fetched_at >= ?")
        params.append(window_start)
    if window_end is not None:
        where_clauses.append("fetched_at < ?")
        params.append(window_end)
    where_sql = " AND ".join(where_clauses)
    # Overfetch to allow for excluded vins already in the seed set, then trim
    # in Python — avoids relying on duckdb list-parameter binding semantics.
    fetch_limit = limit + len(exclude)
    query = f"""
        SELECT DISTINCT vin
        FROM read_parquet('{path}', union_by_name=true)
        WHERE {where_sql}
        ORDER BY md5(vin)
        LIMIT {int(fetch_limit)}
    """
    try:
        rows = con.execute(query, params).fetchall()
    except Exception as e:
        logger.warning("lake_snapshot_cohort: fill_representative_vins error=%s", e)
        return []
    result: List[str] = []
    for (vin,) in rows:
        if vin in exclude:
            continue
        result.append(vin)
        if len(result) >= limit:
            break
    return result


def allocate_cohort(
    candidate_sets: Dict[str, CandidateSet],
    target_vins: Optional[int],
    con,
    base_path: Optional[str],
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> CohortAllocation:
    """Bucket each selector's pre-selected entities by entity_key type,
    deduplicating across selectors, then deterministically fill remaining
    vin capacity toward target_vins from source data."""
    t0 = time.monotonic()
    logger.info(
        "lake_snapshot_cohort: allocate_cohort start selectors=%d target_vins=%s",
        len(candidate_sets), target_vins,
    )
    vin_seeds: set = set()
    listing_seeds: set = set()
    artifact_seeds: set = set()
    make_model_seeds: set = set()
    artifact_row_keys: set = set()
    coverage: Dict[str, Dict[str, Any]] = {}

    buckets = {
        "vin": vin_seeds,
        "listing_id": listing_seeds,
        "artifact_id": artifact_seeds,
        "make_model": make_model_seeds,
    }

    for name, candidate in candidate_sets.items():
        coverage[name] = _selector_coverage_entry(candidate)
        config = _SELECTOR_CONFIGS.get(name)
        if config is not None and config.capture_boundary_row_key:
            # A capture_boundary_row_key selector's row key establishes real
            # vehicle identity (e.g. stale_listing's last-observation row) —
            # unlike an artifact-only malformed-row key (invalid_or_null_vin),
            # which must never seed closure (see expand_entity_closure's
            # docstring on why artifact co-occurrence must not expand
            # vins/listing_ids). Seeding the vin directly here matters
            # because the listing's only evidence may predate window_start,
            # so the normal vin<->listing_id closure lookups (which are
            # window-bounded) would never discover it on their own — leaving
            # this vin's other listings, price events, and remaps out of the
            # export even though the listing itself was selected.
            for _artifact_id, vin, listing_id in candidate.selected_row_keys:
                if vin is not None:
                    vin_seeds.add(vin)
                if listing_id is not None:
                    listing_seeds.add(listing_id)
        bucket = buckets.get(candidate.entity_key)
        if bucket is None:
            continue
        bucket.update(candidate.selected_entities)
        artifact_row_keys.update(candidate.selected_row_keys)

    required_vin_seeds = frozenset(vin_seeds)
    pre_fill_vin_count = len(vin_seeds)
    logger.info(
        "lake_snapshot_cohort: allocate_cohort required_allocation_done vin_seeds=%d "
        "listing_seeds=%d artifact_seeds=%d make_model_seeds=%d",
        pre_fill_vin_count, len(listing_seeds), len(artifact_seeds), len(make_model_seeds),
    )
    fill_vins_added = 0
    if target_vins is not None and len(vin_seeds) < target_vins:
        needed = target_vins - len(vin_seeds)
        logger.info(
            "lake_snapshot_cohort: allocate_cohort deterministic_fill start "
            "pre_fill_vin_count=%d needed=%d",
            pre_fill_vin_count, needed,
        )
        fill_vins = _fill_representative_vins(
            con, base_path, window_start, window_end, exclude=vin_seeds, limit=needed,
        )
        vin_seeds.update(fill_vins)
        fill_vins_added = len(fill_vins)
        logger.info(
            "lake_snapshot_cohort: allocate_cohort deterministic_fill end "
            "pre_fill_vin_count=%d fill_vins_added=%d seed_vin_count=%d",
            pre_fill_vin_count, fill_vins_added, len(vin_seeds),
        )

    logger.info(
        "lake_snapshot_cohort: allocate_cohort end elapsed_s=%.2f vin_seeds=%d "
        "listing_seeds=%d artifact_seeds=%d fill_vins_added=%d",
        time.monotonic() - t0, len(vin_seeds), len(listing_seeds), len(artifact_seeds),
        fill_vins_added,
    )
    return CohortAllocation(
        vin_seeds=frozenset(vin_seeds),
        listing_seeds=frozenset(listing_seeds),
        artifact_seeds=frozenset(artifact_seeds),
        make_model_seeds=frozenset(make_model_seeds),
        selector_coverage=coverage,
        fill_vins_added=fill_vins_added,
        pre_fill_vin_count=pre_fill_vin_count,
        required_vin_seeds=required_vin_seeds,
        artifact_row_keys=frozenset(artifact_row_keys),
    )


# ---------------------------------------------------------------------------
# Entity closure
# ---------------------------------------------------------------------------

def _resolve_make_model_seeds(
    con,
    base_path: Optional[str],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    make_models: FrozenSet[str],
    limit_per_group: int = MAKE_MODEL_REPRESENTATIVE_LIMIT,
) -> Tuple[set, set]:
    """Resolve benchmark make_model seeds into representative vins/listing_ids."""
    if not make_models:
        return set(), set()
    path = resolve_table_path("silver_observations", base_path)
    make_model_clause, make_model_params = in_clause(
        "concat_ws(' ', make, model)", make_models
    )
    where_clauses = ["vin IS NOT NULL", make_model_clause]
    params: List[Any] = list(make_model_params)
    time_clauses, time_params = table_time_where(window_start, window_end, "fetched_at")
    where_clauses += time_clauses
    params += time_params
    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT vin, listing_id
        FROM (
            SELECT vin, listing_id,
                   row_number() OVER (
                       PARTITION BY concat_ws(' ', make, model) ORDER BY vin
                   ) AS rn
            FROM read_parquet('{path}', union_by_name=true)
            WHERE {where_sql}
        ) AS ranked
        WHERE rn <= {int(limit_per_group)}
    """
    try:
        rows = con.execute(query, params).fetchall()
    except Exception as e:
        logger.warning("lake_snapshot_cohort: resolve_make_model_seeds error=%s", e)
        return set(), set()
    vins = {r[0] for r in rows if r[0] is not None}
    listing_ids = {r[1] for r in rows if r[1] is not None}
    return vins, listing_ids


def _listing_ids_for_vins(
    con, base_path: Optional[str], window_start, window_end, vins: set
) -> set:
    if not vins:
        return set()
    listing_ids: set = set()
    vin_clause, vin_params = in_clause("vin", vins)
    for table, ts_col in _VIN_LISTING_TABLES:
        path = resolve_table_path(table, base_path)
        time_clauses, time_params = table_time_where(window_start, window_end, ts_col)
        clauses = [vin_clause, "listing_id IS NOT NULL"] + time_clauses
        query = (
            f"SELECT DISTINCT listing_id FROM read_parquet('{path}', union_by_name=true) "
            f"WHERE {' AND '.join(clauses)}"
        )
        try:
            rows = con.execute(query, vin_params + time_params).fetchall()
            listing_ids.update(r[0] for r in rows)
        except Exception as e:
            logger.warning(
                "lake_snapshot_cohort: listing_ids_for_vins table=%s error=%s", table, e
            )
    return listing_ids


def _vins_for_listing_ids(
    con, base_path: Optional[str], window_start, window_end, listing_ids: set
) -> set:
    if not listing_ids:
        return set()
    vins: set = set()
    listing_clause, listing_params = in_clause("listing_id", listing_ids)
    for table, ts_col in _VIN_LISTING_TABLES:
        path = resolve_table_path(table, base_path)
        time_clauses, time_params = table_time_where(window_start, window_end, ts_col)
        clauses = [listing_clause, "vin IS NOT NULL"] + time_clauses
        query = (
            f"SELECT DISTINCT vin FROM read_parquet('{path}', union_by_name=true) "
            f"WHERE {' AND '.join(clauses)}"
        )
        try:
            rows = con.execute(query, listing_params + time_params).fetchall()
            vins.update(r[0] for r in rows)
        except Exception as e:
            logger.warning(
                "lake_snapshot_cohort: vins_for_listing_ids table=%s error=%s", table, e
            )
    return vins


def _previous_listing_ids_for(
    con, base_path: Optional[str], window_start, window_end, vins: set, listing_ids: set
) -> set:
    path = resolve_table_path("vin_to_listing_events", base_path)
    or_parts: List[str] = []
    params: List[Any] = []
    if vins:
        clause, clause_params = in_clause("vin", vins)
        or_parts.append(clause)
        params += clause_params
    if listing_ids:
        clause, clause_params = in_clause("listing_id", listing_ids)
        or_parts.append(clause)
        params += clause_params
    if not or_parts:
        return set()
    clauses = [f"({' OR '.join(or_parts)})", "previous_listing_id IS NOT NULL"]
    time_clauses, time_params = table_time_where(window_start, window_end, "event_at")
    clauses += time_clauses
    params += time_params
    query = (
        f"SELECT DISTINCT previous_listing_id FROM read_parquet('{path}', union_by_name=true) "
        f"WHERE {' AND '.join(clauses)}"
    )
    try:
        rows = con.execute(query, params).fetchall()
        return {r[0] for r in rows}
    except Exception as e:
        logger.warning("lake_snapshot_cohort: previous_listing_ids error=%s", e)
        return set()


def _artifact_ids_for(
    con, base_path: Optional[str], window_start, window_end, vins: set, listing_ids: set
) -> set:
    artifact_ids: set = set()
    for table, ts_col in _VIN_LISTING_TABLES:
        path = resolve_table_path(table, base_path)
        or_parts: List[str] = []
        params: List[Any] = []
        if vins:
            clause, clause_params = in_clause("vin", vins)
            or_parts.append(clause)
            params += clause_params
        if listing_ids:
            clause, clause_params = in_clause("listing_id", listing_ids)
            or_parts.append(clause)
            params += clause_params
        if not or_parts:
            continue
        time_clauses, time_params = table_time_where(window_start, window_end, ts_col)
        clauses = [f"({' OR '.join(or_parts)})", "artifact_id IS NOT NULL"] + time_clauses
        params += time_params
        query = (
            f"SELECT DISTINCT artifact_id FROM read_parquet('{path}', union_by_name=true) "
            f"WHERE {' AND '.join(clauses)}"
        )
        try:
            rows = con.execute(query, params).fetchall()
            artifact_ids.update(r[0] for r in rows)
        except Exception as e:
            logger.warning("lake_snapshot_cohort: artifact_ids table=%s error=%s", table, e)
    return artifact_ids


def expand_entity_closure(
    con,
    base_path: Optional[str],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    allocation: CohortAllocation,
    max_passes: int = MAX_CLOSURE_PASSES,
) -> Dict[str, Any]:
    """Expand allocated seeds into a logically closed VIN/listing set, then
    attach artifact context for that frozen core.

    Core closure follows only vehicle-identity edges: seed VINs ->
    listing_ids -> back to VINs -> previous_listing_ids (remap events),
    repeated until no set grows or max_passes is hit. Artifact IDs are never
    used to discover more VINs/listing_ids — an artifact (e.g. an SRP/carousel
    row) can legitimately co-occur with many unrelated listings, and using
    that co-occurrence to expand the vehicle closure is what caused runaway
    growth. Once the core is frozen, artifact_ids are attached for reporting
    (observations/events matching the core VINs/listing_ids, plus any
    explicit artifact selector roots) without feeding back into vins/listing_ids.
    """
    t0 = time.monotonic()
    logger.info("lake_snapshot_cohort: expand_entity_closure start max_passes=%d", max_passes)
    make_model_vins, make_model_listings = _resolve_make_model_seeds(
        con, base_path, window_start, window_end, allocation.make_model_seeds,
    )
    # Selector-required vin pressure, independent of the deterministic
    # target_vins fill — used to report when required coverage (including
    # benchmark make_model groups resolved above) alone exceeds target_vins.
    required_vin_seed_count = len(set(allocation.required_vin_seeds) | make_model_vins)

    seed_vins = set(allocation.vin_seeds) | make_model_vins
    vins = set(seed_vins)
    listing_ids = set(allocation.listing_seeds) | make_model_listings
    previous_listing_ids_added = 0

    logger.info(
        "lake_snapshot_cohort: expand_entity_closure initial vins=%d listing_ids=%d "
        "artifact_roots=%d",
        len(vins), len(listing_ids), len(allocation.artifact_seeds),
    )

    passes = 0
    for passes in range(1, max_passes + 1):
        pass_t0 = time.monotonic()
        before = (len(vins), len(listing_ids))
        logger.info(
            "lake_snapshot_cohort: core_closure pass=%d start vins=%d listing_ids=%d",
            passes, *before,
        )

        listing_ids |= _listing_ids_for_vins(con, base_path, window_start, window_end, vins)
        vins |= _vins_for_listing_ids(con, base_path, window_start, window_end, listing_ids)

        prev_listing_ids = _previous_listing_ids_for(
            con, base_path, window_start, window_end, vins, listing_ids,
        )
        previous_listing_ids_added += len(prev_listing_ids - listing_ids)
        listing_ids |= prev_listing_ids

        after = (len(vins), len(listing_ids))
        logger.info(
            "lake_snapshot_cohort: core_closure pass=%d end elapsed_s=%.2f vins=%d "
            "listing_ids=%d",
            passes, time.monotonic() - pass_t0, *after,
        )
        if after == before:
            logger.info(
                "lake_snapshot_cohort: core_closure pass=%d no_change stopping", passes,
            )
            break

    attach_t0 = time.monotonic()
    logger.info(
        "lake_snapshot_cohort: artifact_attachment start core_vins=%d core_listing_ids=%d "
        "explicit_artifact_roots=%d",
        len(vins), len(listing_ids), len(allocation.artifact_seeds),
    )
    artifact_ids = set(allocation.artifact_seeds)
    artifact_ids |= _artifact_ids_for(con, base_path, window_start, window_end, vins, listing_ids)
    logger.info(
        "lake_snapshot_cohort: artifact_attachment end artifact_ids=%d elapsed_s=%.2f",
        len(artifact_ids), time.monotonic() - attach_t0,
    )

    logger.info(
        "lake_snapshot_cohort: expand_entity_closure end elapsed_s=%.2f passes=%d "
        "closed_vins=%d listing_ids=%d artifact_ids=%d",
        time.monotonic() - t0, passes, len(vins), len(listing_ids), len(artifact_ids),
    )
    return {
        "closed_vins": vins,
        "listing_ids": listing_ids,
        "artifact_ids": artifact_ids,
        "seed_vins": seed_vins,
        "closure_passes": passes,
        "previous_listing_ids_added": previous_listing_ids_added,
        "required_vin_seed_count": required_vin_seed_count,
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def build_snapshot_cohort(
    con,
    base_path: Optional[str],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    target_vins: Optional[int],
    names: Optional[List[str]] = None,
    candidate_cap: int = DEFAULT_CANDIDATE_CAP,
    max_closure_passes: int = MAX_CLOSURE_PASSES,
    candidate_sets: Optional[Dict[str, CandidateSet]] = None,
) -> SnapshotCohort:
    """Collect selector candidates, allocate a seed cohort, and close it into
    a logically complete VIN/listing/artifact set.

    *candidate_sets* may be passed in already collected (e.g. by a caller
    that also needs selector diagnostics) to avoid scanning selector
    candidates twice.
    """
    t0 = time.monotonic()
    logger.info("lake_snapshot_cohort: build_snapshot_cohort start target_vins=%s", target_vins)
    if candidate_sets is None:
        candidate_sets = collect_all_selector_candidates(
            con, names=names, base_path=base_path,
            window_start=window_start, window_end=window_end, candidate_cap=candidate_cap,
        )
    allocation = allocate_cohort(
        candidate_sets, target_vins, con, base_path, window_start, window_end,
    )
    closure = expand_entity_closure(
        con, base_path, window_start, window_end, allocation, max_passes=max_closure_passes,
    )

    diagnostics = {
        "closure_passes": closure["closure_passes"],
        "seed_vins": len(closure["seed_vins"]),
        "closed_vins": len(closure["closed_vins"]),
        "listing_ids": len(closure["listing_ids"]),
        "artifact_ids": len(closure["artifact_ids"]),
        "previous_listing_ids_added": closure["previous_listing_ids_added"],
        "fill_vins_added": allocation.fill_vins_added,
        "pre_fill_vin_count": allocation.pre_fill_vin_count,
        "required_vin_seed_count": closure["required_vin_seed_count"],
        "target_vins": target_vins,
        "target_vins_exceeded_by_required_selectors": (
            target_vins is not None and closure["required_vin_seed_count"] > target_vins
        ),
        "selector_coverage": allocation.selector_coverage,
    }

    logger.info(
        "lake_snapshot_cohort: build_snapshot_cohort end elapsed_s=%.2f closed_vins=%d "
        "listing_ids=%d artifact_ids=%d",
        time.monotonic() - t0, len(closure["closed_vins"]), len(closure["listing_ids"]),
        len(closure["artifact_ids"]),
    )
    return SnapshotCohort(
        seed_vins=frozenset(closure["seed_vins"]),
        closed_vins=frozenset(closure["closed_vins"]),
        listing_ids=frozenset(closure["listing_ids"]),
        artifact_ids=frozenset(closure["artifact_ids"]),
        selector_coverage=allocation.selector_coverage,
        diagnostics=diagnostics,
        artifact_row_keys=allocation.artifact_row_keys,
    )
