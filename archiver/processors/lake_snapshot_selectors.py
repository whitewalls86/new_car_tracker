"""
Selector registry for CI lake snapshot exports (Plan 120).

Each Selector names a dbt/PySpark branch or guard the snapshot must exercise,
the SQL used to find candidate entities in production, and the minimum
representation required in the snapshot before it can be published.

Phase 2 implements real DuckDB SQL (and an execution path, see
`run_lake_selectors`) for five selectors:

    relisted_vin, price_drop, price_increase, cooldown_incremented,
    stable_state_run

All other selectors remain placeholder/TODO SQL — the registry shape and
uniqueness constraint are still the load-bearing parts for those.
"""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from archiver.processors.lake_source_audit import resolve_table_path
from shared.duckdb_s3 import get_duckdb_s3_connection

logger = logging.getLogger("archiver")


@dataclass(frozen=True)
class Selector:
    name: str
    min_entities: int
    entity_key: str
    sql: str
    description: str


# name -> (min_entities, entity_key, description)
_SELECTOR_SPECS: List[tuple] = [
    ("stable_state_run", 25, "vin",
     "VINs with multiple detail observations where the business-state "
     "fingerprint is unchanged (gaps-and-islands collapse)."),
    ("state_change_run", 25, "vin",
     "VINs with multiple distinct business-state fingerprints (price, "
     "mileage, dealer, or listing_state changes)."),
    ("relisted_vin", 10, "vin",
     "VINs with more than one listing_id or remap events with "
     "previous_listing_id."),
    ("active_to_unlisted", 10, "listing_id",
     "VIN/listing with an active detail row followed by an unlisted/delete "
     "event."),
    ("price_drop", 25, "listing_id",
     "Consecutive price event where price < prev_price."),
    ("price_increase", 25, "listing_id",
     "Consecutive price event where price > prev_price."),
    ("price_changed_7d", 25, "listing_id",
     "Price change within seven days of the source window end."),
    ("price_changed_30d_only", 25, "listing_id",
     "Price change within thirty days but outside the seven-day window."),
    ("no_price_history", 25, "vin",
     "Observation VIN lacking any matching positive price events."),
    ("detail_beats_srp", 25, "vin",
     "VIN with both detail and SRP observations where detail should win "
     "latest-observation source priority."),
    ("srp_fallback", 25, "vin",
     "VIN with usable SRP attributes and missing/incomplete detail "
     "attributes."),
    ("carousel_only_or_low_priority", 25, "vin",
     "VIN/listing represented only by carousel observations, or where "
     "carousel loses priority to richer sources."),
    ("invalid_or_null_vin", 25, "artifact_id",
     "Rows with null or invalid VINs that must not become vin17."),
    ("benchmark_dense_make_model", 3, "make_model",
     "Make/model groups with enough rows for stable percentile/median "
     "benchmarks."),
    ("benchmark_sparse_make_model", 3, "make_model",
     "Make/model groups with only a few rows, which must not disappear "
     "silently."),
    ("cooldown_blocked", 10, "listing_id",
     "First 403 blocked cooldown event."),
    ("cooldown_incremented", 10, "listing_id",
     "Repeated 403 blocked attempt event."),
    ("cooldown_bucket_3_4", 1, "listing_id",
     "Cooldown attempts between 3 and 4 (bucket boundary)."),
    ("cooldown_bucket_5_10", 1, "listing_id",
     "Cooldown attempts between 5 and 10 (bucket boundary)."),
    ("cooldown_bucket_11_plus", 1, "listing_id",
     "Cooldown attempts >= 11 (high-attempt bucket)."),
    ("fresh_recent_listing", 25, "listing_id",
     "Young/current active listing."),
    ("stale_listing", 25, "listing_id",
     "Old listing, or listing with stale SRP/detail recency."),
]


def _placeholder_sql(name: str) -> str:
    return f"-- TODO: implement selector SQL for '{name}'\nSELECT NULL WHERE FALSE"


# ---------------------------------------------------------------------------
# Phase 2 real selector SQL
#
# Each template reads a single source Parquet table (see _SELECTOR_TABLE) and
# is filled in with the resolved table path and a WHERE clause built by
# `_build_where`. Table/column names come from the actual writers:
#   archiver/processors/flush_silver_observations.py
#   archiver/processors/flush_staging_events.py
# ---------------------------------------------------------------------------

_RELISTED_VIN_SQL = """
WITH events AS (
    SELECT vin, listing_id, artifact_id, event_type, previous_listing_id, event_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
relisted AS (
    SELECT vin FROM events GROUP BY vin HAVING count(DISTINCT listing_id) > 1
)
SELECT e.vin, e.listing_id, e.artifact_id, e.event_type, e.previous_listing_id, e.event_at
FROM events e
JOIN relisted r USING (vin)
ORDER BY e.vin, e.event_at
"""

_PRICE_DROP_SQL = """
WITH events AS (
    SELECT event_id, listing_id, vin, artifact_id, price, event_type, event_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
diffed AS (
    SELECT *, LAG(price) OVER (PARTITION BY listing_id ORDER BY event_at, event_id) AS prev_price
    FROM events
)
SELECT event_id, listing_id, vin, artifact_id, price, prev_price, event_at
FROM diffed
WHERE prev_price IS NOT NULL AND price < prev_price
ORDER BY listing_id, event_at
"""

_PRICE_INCREASE_SQL = """
WITH events AS (
    SELECT event_id, listing_id, vin, artifact_id, price, event_type, event_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
diffed AS (
    SELECT *, LAG(price) OVER (PARTITION BY listing_id ORDER BY event_at, event_id) AS prev_price
    FROM events
)
SELECT event_id, listing_id, vin, artifact_id, price, prev_price, event_at
FROM diffed
WHERE prev_price IS NOT NULL AND price > prev_price
ORDER BY listing_id, event_at
"""

_COOLDOWN_INCREMENTED_SQL = """
SELECT event_id, listing_id, event_type, num_of_attempts, event_at
FROM read_parquet('{path}', union_by_name=true)
{where}
ORDER BY listing_id, event_at
"""

_STABLE_STATE_RUN_SQL = """
WITH obs AS (
    SELECT vin, listing_id, artifact_id, fetched_at, price, mileage, listing_state,
           CONCAT_WS('|', CAST(price AS VARCHAR), CAST(mileage AS VARCHAR),
                     COALESCE(listing_state, '')) AS fingerprint
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
fp AS (
    SELECT *, LAG(fingerprint) OVER (PARTITION BY vin ORDER BY fetched_at) AS prev_fingerprint
    FROM obs
)
SELECT vin, listing_id, artifact_id, fetched_at, fingerprint
FROM fp
WHERE prev_fingerprint IS NOT NULL AND fingerprint = prev_fingerprint
ORDER BY vin, fetched_at
"""

_SELECTOR_SQL_TEMPLATES: Dict[str, str] = {
    "relisted_vin": _RELISTED_VIN_SQL,
    "price_drop": _PRICE_DROP_SQL,
    "price_increase": _PRICE_INCREASE_SQL,
    "cooldown_incremented": _COOLDOWN_INCREMENTED_SQL,
    "stable_state_run": _STABLE_STATE_RUN_SQL,
}

# selector name -> logical source table name (matches lake_source_audit.SOURCE_TABLE_SPECS)
_SELECTOR_TABLE: Dict[str, str] = {
    "relisted_vin": "vin_to_listing_events",
    "price_drop": "price_observation_events",
    "price_increase": "price_observation_events",
    "cooldown_incremented": "blocked_cooldown_events",
    "stable_state_run": "silver_observations",
}

# selector name -> hardcoded (non-window) filter clauses
_SELECTOR_BASE_FILTERS: Dict[str, List[str]] = {
    "relisted_vin": [],
    "price_drop": ["event_type = 'upserted'", "price IS NOT NULL"],
    "price_increase": ["event_type = 'upserted'", "price IS NOT NULL"],
    "cooldown_incremented": ["num_of_attempts > 1"],
    "stable_state_run": ["source = 'detail'"],
}

# selector name -> timestamp column used for window filtering
_SELECTOR_TS_COL: Dict[str, str] = {
    "relisted_vin": "event_at",
    "price_drop": "event_at",
    "price_increase": "event_at",
    "cooldown_incremented": "event_at",
    "stable_state_run": "fetched_at",
}

RUNNABLE_SELECTORS: Tuple[str, ...] = tuple(_SELECTOR_TABLE.keys())


def build_selector_registry() -> Dict[str, Selector]:
    """Build the selector registry, keyed by selector name.

    Raises ValueError if any selector name is duplicated in _SELECTOR_SPECS.
    """
    registry: Dict[str, Selector] = {}
    for name, min_entities, entity_key, description in _SELECTOR_SPECS:
        if name in registry:
            raise ValueError(f"Duplicate selector name: {name}")
        registry[name] = Selector(
            name=name,
            min_entities=min_entities,
            entity_key=entity_key,
            sql=_SELECTOR_SQL_TEMPLATES.get(name) or _placeholder_sql(name),
            description=description,
        )
    return registry


def _build_where(
    name: str, window_start: Optional[datetime], window_end: Optional[datetime]
) -> Tuple[str, List[Any]]:
    clauses = list(_SELECTOR_BASE_FILTERS[name])
    params: List[Any] = []
    ts_col = _SELECTOR_TS_COL[name]
    if window_start is not None:
        clauses.append(f"{ts_col} >= ?")
        params.append(window_start)
    if window_end is not None:
        clauses.append(f"{ts_col} < ?")
        params.append(window_end)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def build_selector_query(
    name: str,
    path: str,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Tuple[str, List[Any]]:
    """Build the executable SQL and bound params for a Phase 2 selector."""
    if name not in RUNNABLE_SELECTORS:
        raise ValueError(f"No executable query implemented for selector '{name}'")
    template = _SELECTOR_SQL_TEMPLATES[name]
    where_sql, params = _build_where(name, window_start, window_end)
    sql = template.format(path=path, where=where_sql)
    return sql, params


def _wrap_aggregate_query(candidate_sql: str, entity_key: str) -> str:
    """Wrap a selector's candidate-row query so counting/sampling happens in
    DuckDB rather than pulling every candidate row into archiver memory."""
    return f"""
WITH selector_candidates AS (
{candidate_sql}
)
SELECT
    count(*) AS candidate_rows,
    count(DISTINCT {entity_key}) AS entities,
    (
        SELECT list(entity_value)
        FROM (
            SELECT DISTINCT {entity_key} AS entity_value
            FROM selector_candidates
            WHERE {entity_key} IS NOT NULL
            ORDER BY entity_value
            LIMIT 5
        ) AS sample
    ) AS sample_entities
FROM selector_candidates
"""


def run_selector(
    con,
    name: str,
    base_path: Optional[str],
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
    registry: Optional[Dict[str, Selector]] = None,
) -> Dict[str, Any]:
    """Run one Phase 2 selector and return coverage/candidate diagnostics.

    Never raises for a missing/unreadable source table — the error is
    captured in the returned dict, mirroring `lake_source_audit._audit_table`.
    """
    registry = registry or build_selector_registry()
    selector = registry[name]
    table_name = _SELECTOR_TABLE[name]
    path = resolve_table_path(table_name, base_path)

    result: Dict[str, Any] = {
        "selector": name,
        "entity_key": selector.entity_key,
        "required": selector.min_entities,
        "path": path,
        "candidate_rows": 0,
        "entities": 0,
        "sample_entities": [],
        "status": "fail",
        "error": None,
    }

    try:
        candidate_sql, params = build_selector_query(name, path, window_start, window_end)
        agg_sql = _wrap_aggregate_query(candidate_sql, selector.entity_key)
        candidate_rows, entities, sample_entities = con.execute(agg_sql, params).fetchone()
        result["candidate_rows"] = candidate_rows
        result["entities"] = entities
        result["sample_entities"] = list(sample_entities) if sample_entities else []
        result["status"] = "pass" if entities >= selector.min_entities else "fail"
    except Exception as e:
        result["error"] = str(e)
        logger.warning("lake_snapshot_selectors: selector=%s path=%s error=%s", name, path, e)

    return result


def run_lake_selectors(
    names: Optional[List[str]] = None,
    base_path: Optional[str] = None,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Run the Phase 2 selectors and return coverage/candidate diagnostics.

    Returns a dict shaped like:
        {"selectors": {...}, "errors": [...], "ok": bool}
    """
    names = list(names) if names is not None else list(RUNNABLE_SELECTORS)
    registry = build_selector_registry()

    if base_path:
        import duckdb
        con = duckdb.connect()
    else:
        con = get_duckdb_s3_connection()

    try:
        selectors: Dict[str, Any] = {}
        errors: List[str] = []
        for name in names:
            selector_result = run_selector(
                con, name, base_path, window_start, window_end, registry=registry
            )
            selectors[name] = selector_result
            if selector_result["error"] is not None:
                errors.append(f"{name}: {selector_result['error']}")

        return {
            "selectors": selectors,
            "errors": errors,
            "ok": len(errors) == 0,
        }
    finally:
        con.close()
