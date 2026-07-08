"""
Selector registry for CI lake snapshot exports (Plan 120).

Each Selector names a dbt/PySpark branch or guard the snapshot must exercise,
the SQL used to find candidate entities in production, and the minimum
representation required in the snapshot before it can be published.

Gate B implements real DuckDB SQL (and an execution path, see
`run_lake_selectors`) for every selector in the registry. All 22 selectors
are derivable from the four supported source tables
(`archiver/processors/lake_source_audit.py`), so none remain placeholder/TODO
SQL. `state_change_run` and `stable_state_run` reproduce the dbt
fingerprint fields from `int_listing_state_fingerprints.sql` exactly (see the
dbt-equivalence tests in `tests/archiver/test_export_ci_lake_snapshot.py`).
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
# Selector SQL
#
# Each template reads source Parquet table(s) (see _SELECTOR_TABLE and
# _SELECTOR_EXTRA_TABLES) and is filled in with the resolved table path(s)
# and a WHERE clause built by `_build_where`. Table/column names come from
# the actual writers:
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

# Generic "select event rows matching a base filter" shape. Reused for every
# cooldown selector — the bucket boundaries live entirely in
# _SELECTOR_BASE_FILTERS, so the query body does not change.
_COOLDOWN_EVENTS_SQL = """
SELECT event_id, listing_id, event_type, num_of_attempts, event_at
FROM read_parquet('{path}', union_by_name=true)
{where}
ORDER BY listing_id, event_at
"""

# Shared fingerprint CTEs for stable_state_run/state_change_run. Mirrors the
# dbt field list in int_listing_state_fingerprints.sql exactly, deriving
# vin17 from the raw `vin` column the same way stg_observations.sql does,
# since silver_normalized/observations stores only the raw `vin` column.
_FINGERPRINT_CTE = """
WITH obs AS (
    SELECT
        artifact_id, listing_id, fetched_at, vin,
        price, mileage, msrp, make, model, trim AS vehicle_trim, year AS model_year,
        stock_type, fuel_type, body_style, listing_state,
        dealer_name, dealer_zip, dealer_city, dealer_state, customer_id
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
vinned AS (
    SELECT *,
        CASE WHEN vin IS NOT NULL AND length(vin) = 17
                  AND regexp_matches(upper(vin), '^[A-Z0-9]{{17}}$')
             THEN upper(vin) ELSE NULL END AS vin17
    FROM obs
),
filtered AS (
    SELECT *,
        md5(concat_ws('|',
            coalesce(listing_id,                   ''),
            coalesce(vin17,                        ''),
            coalesce(CAST(price       AS VARCHAR), ''),
            coalesce(CAST(mileage     AS VARCHAR), ''),
            coalesce(CAST(msrp        AS VARCHAR), ''),
            coalesce(make,                         ''),
            coalesce(model,                        ''),
            coalesce(vehicle_trim,                 ''),
            coalesce(CAST(model_year  AS VARCHAR), ''),
            coalesce(stock_type,                   ''),
            coalesce(fuel_type,                    ''),
            coalesce(body_style,                   ''),
            coalesce(listing_state,                ''),
            coalesce(dealer_name,                  ''),
            coalesce(dealer_zip,                   ''),
            coalesce(dealer_city,                  ''),
            coalesce(dealer_state,                 ''),
            coalesce(customer_id,                  '')
        )) AS fingerprint
    FROM vinned
    WHERE vin17 IS NOT NULL
),
fp AS (
    SELECT *,
        LAG(fingerprint) OVER (
            PARTITION BY vin17 ORDER BY fetched_at, artifact_id
        ) AS prev_fingerprint
    FROM filtered
)
"""

_STABLE_STATE_RUN_SQL = _FINGERPRINT_CTE + """
SELECT vin17 AS vin, listing_id, artifact_id, fetched_at, fingerprint
FROM fp
WHERE prev_fingerprint IS NOT NULL AND fingerprint = prev_fingerprint
ORDER BY vin17, fetched_at
"""

_STATE_CHANGE_RUN_SQL = _FINGERPRINT_CTE + """
SELECT vin17 AS vin, listing_id, artifact_id, fetched_at, fingerprint, prev_fingerprint
FROM fp
WHERE prev_fingerprint IS NOT NULL AND fingerprint != prev_fingerprint
ORDER BY vin17, fetched_at
"""

_ACTIVE_TO_UNLISTED_SQL = """
WITH obs AS (
    SELECT vin, listing_id, artifact_id, fetched_at, listing_state
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
active AS (
    SELECT listing_id, min(fetched_at) AS first_active_at
    FROM obs
    WHERE listing_state = 'active'
    GROUP BY listing_id
),
unlisted AS (
    SELECT listing_id, min(fetched_at) AS first_unlisted_at
    FROM obs
    WHERE listing_state = 'unlisted'
    GROUP BY listing_id
)
SELECT a.listing_id, a.first_active_at, u.first_unlisted_at
FROM active a
JOIN unlisted u USING (listing_id)
WHERE u.first_unlisted_at > a.first_active_at
ORDER BY a.listing_id
"""

# Anchor "source window end" as MAX(event_at) over the (already window-filtered)
# read: when a window_end is supplied, _build_where already bounds event_at
# below it, so the in-query max is the same recency anchor. When no window is
# supplied, the anchor degrades gracefully to "most recent event in the table".
_PRICE_CHANGED_7D_SQL = """
WITH events AS (
    SELECT event_id, listing_id, vin, artifact_id, price, event_type, event_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
diffed AS (
    SELECT *,
        LAG(price) OVER (PARTITION BY listing_id ORDER BY event_at, event_id) AS prev_price,
        MAX(event_at) OVER () AS window_anchor
    FROM events
)
SELECT event_id, listing_id, vin, artifact_id, price, prev_price, event_at
FROM diffed
WHERE prev_price IS NOT NULL AND price != prev_price
  AND event_at >= window_anchor - INTERVAL 7 DAY
ORDER BY listing_id, event_at
"""

_PRICE_CHANGED_30D_ONLY_SQL = """
WITH events AS (
    SELECT event_id, listing_id, vin, artifact_id, price, event_type, event_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
diffed AS (
    SELECT *,
        LAG(price) OVER (PARTITION BY listing_id ORDER BY event_at, event_id) AS prev_price,
        MAX(event_at) OVER () AS window_anchor
    FROM events
)
SELECT event_id, listing_id, vin, artifact_id, price, prev_price, event_at
FROM diffed
WHERE prev_price IS NOT NULL AND price != prev_price
  AND event_at >= window_anchor - INTERVAL 30 DAY
  AND event_at <  window_anchor - INTERVAL 7 DAY
ORDER BY listing_id, event_at
"""

# Two-table selector: candidate VINs come from silver_observations ({path}),
# "priced" VINs come from price_observation_events
# ({price_observation_events_path}), injected via _SELECTOR_EXTRA_TABLES.
_NO_PRICE_HISTORY_SQL = """
WITH obs AS (
    SELECT DISTINCT vin
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
priced AS (
    SELECT DISTINCT vin
    FROM read_parquet('{price_observation_events_path}', union_by_name=true)
    WHERE price IS NOT NULL AND price > 0
)
SELECT o.vin
FROM obs o
LEFT JOIN priced p USING (vin)
WHERE p.vin IS NULL
ORDER BY o.vin
"""

# Mirrors int_latest_observation.sql's source-priority ranking:
# detail (1) > srp (2) > carousel (3), then fetched_at desc, artifact_id desc.
_DETAIL_BEATS_SRP_SQL = """
WITH obs AS (
    SELECT artifact_id, listing_id, vin, source, fetched_at, make
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
usable AS (
    SELECT * FROM obs WHERE make IS NOT NULL
),
vin_sources AS (
    SELECT vin, bool_or(source = 'detail') AS has_detail, bool_or(source = 'srp') AS has_srp
    FROM usable
    GROUP BY vin
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY vin
            ORDER BY CASE source WHEN 'detail' THEN 1 WHEN 'srp' THEN 2 ELSE 3 END,
                     fetched_at DESC, artifact_id DESC
        ) AS rn
    FROM usable
)
SELECT r.vin, r.listing_id, r.artifact_id, r.source, r.fetched_at
FROM ranked r
JOIN vin_sources v USING (vin)
WHERE r.rn = 1 AND r.source = 'detail' AND v.has_detail AND v.has_srp
ORDER BY r.vin
"""

_SRP_FALLBACK_SQL = """
WITH obs AS (
    SELECT artifact_id, listing_id, vin, source, fetched_at, make
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
usable AS (
    SELECT * FROM obs WHERE make IS NOT NULL
),
vin_sources AS (
    SELECT vin, bool_or(source = 'detail') AS has_detail
    FROM usable
    GROUP BY vin
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY vin
            ORDER BY CASE source WHEN 'detail' THEN 1 WHEN 'srp' THEN 2 ELSE 3 END,
                     fetched_at DESC, artifact_id DESC
        ) AS rn
    FROM usable
)
SELECT r.vin, r.listing_id, r.artifact_id, r.source, r.fetched_at
FROM ranked r
JOIN vin_sources v USING (vin)
WHERE r.rn = 1 AND r.source = 'srp' AND NOT v.has_detail
ORDER BY r.vin
"""

_CAROUSEL_ONLY_OR_LOW_PRIORITY_SQL = """
WITH obs AS (
    SELECT vin, source
    FROM read_parquet('{path}', union_by_name=true)
    {where}
)
SELECT DISTINCT vin
FROM obs
WHERE source = 'carousel'
ORDER BY vin
"""

_INVALID_OR_NULL_VIN_SQL = """
SELECT artifact_id, listing_id, vin, fetched_at
FROM read_parquet('{path}', union_by_name=true)
{where}
ORDER BY fetched_at
"""

_BENCHMARK_DENSE_MAKE_MODEL_SQL = """
WITH obs AS (
    SELECT make, model
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
grouped AS (
    SELECT make, model, count(*) AS row_count
    FROM obs
    WHERE make IS NOT NULL AND model IS NOT NULL
    GROUP BY make, model
)
SELECT make || ' ' || model AS make_model, make, model, row_count
FROM grouped
WHERE row_count >= 20
ORDER BY row_count DESC
"""

_BENCHMARK_SPARSE_MAKE_MODEL_SQL = """
WITH obs AS (
    SELECT make, model
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
grouped AS (
    SELECT make, model, count(*) AS row_count
    FROM obs
    WHERE make IS NOT NULL AND model IS NOT NULL
    GROUP BY make, model
)
SELECT make || ' ' || model AS make_model, make, model, row_count
FROM grouped
WHERE row_count > 0 AND row_count < 20
ORDER BY row_count
"""

_FRESH_RECENT_LISTING_SQL = """
WITH obs AS (
    SELECT listing_id, fetched_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
agg AS (
    SELECT listing_id,
           min(fetched_at) AS first_seen_at,
           max(fetched_at) AS last_seen_at
    FROM obs
    GROUP BY listing_id
),
anchored AS (
    SELECT *, max(last_seen_at) OVER () AS window_anchor
    FROM agg
)
SELECT listing_id, first_seen_at, last_seen_at, window_anchor
FROM anchored
WHERE window_anchor - first_seen_at <= INTERVAL 14 DAY
  AND window_anchor - last_seen_at  <= INTERVAL 2 DAY
ORDER BY listing_id
"""

_STALE_LISTING_SQL = """
WITH obs AS (
    SELECT listing_id, fetched_at
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
agg AS (
    SELECT listing_id,
           min(fetched_at) AS first_seen_at,
           max(fetched_at) AS last_seen_at
    FROM obs
    GROUP BY listing_id
),
anchored AS (
    SELECT *, max(last_seen_at) OVER () AS window_anchor
    FROM agg
)
SELECT listing_id, first_seen_at, last_seen_at, window_anchor
FROM anchored
WHERE window_anchor - last_seen_at >= INTERVAL 30 DAY
ORDER BY listing_id
"""

_SELECTOR_SQL_TEMPLATES: Dict[str, str] = {
    "relisted_vin": _RELISTED_VIN_SQL,
    "price_drop": _PRICE_DROP_SQL,
    "price_increase": _PRICE_INCREASE_SQL,
    "cooldown_incremented": _COOLDOWN_EVENTS_SQL,
    "stable_state_run": _STABLE_STATE_RUN_SQL,
    "state_change_run": _STATE_CHANGE_RUN_SQL,
    "active_to_unlisted": _ACTIVE_TO_UNLISTED_SQL,
    "price_changed_7d": _PRICE_CHANGED_7D_SQL,
    "price_changed_30d_only": _PRICE_CHANGED_30D_ONLY_SQL,
    "no_price_history": _NO_PRICE_HISTORY_SQL,
    "detail_beats_srp": _DETAIL_BEATS_SRP_SQL,
    "srp_fallback": _SRP_FALLBACK_SQL,
    "carousel_only_or_low_priority": _CAROUSEL_ONLY_OR_LOW_PRIORITY_SQL,
    "invalid_or_null_vin": _INVALID_OR_NULL_VIN_SQL,
    "benchmark_dense_make_model": _BENCHMARK_DENSE_MAKE_MODEL_SQL,
    "benchmark_sparse_make_model": _BENCHMARK_SPARSE_MAKE_MODEL_SQL,
    "cooldown_blocked": _COOLDOWN_EVENTS_SQL,
    "cooldown_bucket_3_4": _COOLDOWN_EVENTS_SQL,
    "cooldown_bucket_5_10": _COOLDOWN_EVENTS_SQL,
    "cooldown_bucket_11_plus": _COOLDOWN_EVENTS_SQL,
    "fresh_recent_listing": _FRESH_RECENT_LISTING_SQL,
    "stale_listing": _STALE_LISTING_SQL,
}

# selector name -> logical source table name (matches lake_source_audit.SOURCE_TABLE_SPECS)
_SELECTOR_TABLE: Dict[str, str] = {
    "relisted_vin": "vin_to_listing_events",
    "price_drop": "price_observation_events",
    "price_increase": "price_observation_events",
    "cooldown_incremented": "blocked_cooldown_events",
    "stable_state_run": "silver_observations",
    "state_change_run": "silver_observations",
    "active_to_unlisted": "silver_observations",
    "price_changed_7d": "price_observation_events",
    "price_changed_30d_only": "price_observation_events",
    "no_price_history": "silver_observations",
    "detail_beats_srp": "silver_observations",
    "srp_fallback": "silver_observations",
    "carousel_only_or_low_priority": "silver_observations",
    "invalid_or_null_vin": "silver_observations",
    "benchmark_dense_make_model": "silver_observations",
    "benchmark_sparse_make_model": "silver_observations",
    "cooldown_blocked": "blocked_cooldown_events",
    "cooldown_bucket_3_4": "blocked_cooldown_events",
    "cooldown_bucket_5_10": "blocked_cooldown_events",
    "cooldown_bucket_11_plus": "blocked_cooldown_events",
    "fresh_recent_listing": "silver_observations",
    "stale_listing": "silver_observations",
}

# selector name -> extra logical source tables, resolved and passed to the SQL
# template as `{<table_name>_path}` in addition to the primary `{path}`.
_SELECTOR_EXTRA_TABLES: Dict[str, List[str]] = {
    "no_price_history": ["price_observation_events"],
}

# selector name -> hardcoded (non-window) filter clauses
_SELECTOR_BASE_FILTERS: Dict[str, List[str]] = {
    "relisted_vin": [],
    "price_drop": ["event_type = 'upserted'", "price IS NOT NULL"],
    "price_increase": ["event_type = 'upserted'", "price IS NOT NULL"],
    "cooldown_incremented": ["num_of_attempts > 1"],
    "stable_state_run": ["source = 'detail'"],
    "state_change_run": ["source = 'detail'"],
    "active_to_unlisted": ["source = 'detail'"],
    "price_changed_7d": ["event_type = 'upserted'", "price IS NOT NULL"],
    "price_changed_30d_only": ["event_type = 'upserted'", "price IS NOT NULL"],
    "no_price_history": ["vin IS NOT NULL"],
    "detail_beats_srp": ["source IN ('detail', 'srp')", "vin IS NOT NULL"],
    "srp_fallback": ["source IN ('detail', 'srp')", "vin IS NOT NULL"],
    "carousel_only_or_low_priority": ["vin IS NOT NULL"],
    "invalid_or_null_vin": [
        "(vin IS NULL OR length(vin) != 17 OR NOT regexp_matches(upper(vin), '^[A-Z0-9]{17}$'))"
    ],
    "benchmark_dense_make_model": ["source = 'detail'"],
    "benchmark_sparse_make_model": ["source = 'detail'"],
    "cooldown_blocked": ["num_of_attempts = 1"],
    "cooldown_bucket_3_4": ["num_of_attempts BETWEEN 3 AND 4"],
    "cooldown_bucket_5_10": ["num_of_attempts BETWEEN 5 AND 10"],
    "cooldown_bucket_11_plus": ["num_of_attempts >= 11"],
    "fresh_recent_listing": [],
    "stale_listing": [],
}

# selector name -> timestamp column used for window filtering
_SELECTOR_TS_COL: Dict[str, str] = {
    "relisted_vin": "event_at",
    "price_drop": "event_at",
    "price_increase": "event_at",
    "cooldown_incremented": "event_at",
    "stable_state_run": "fetched_at",
    "state_change_run": "fetched_at",
    "active_to_unlisted": "fetched_at",
    "price_changed_7d": "event_at",
    "price_changed_30d_only": "event_at",
    "no_price_history": "fetched_at",
    "detail_beats_srp": "fetched_at",
    "srp_fallback": "fetched_at",
    "carousel_only_or_low_priority": "fetched_at",
    "invalid_or_null_vin": "fetched_at",
    "benchmark_dense_make_model": "fetched_at",
    "benchmark_sparse_make_model": "fetched_at",
    "cooldown_blocked": "event_at",
    "cooldown_bucket_3_4": "event_at",
    "cooldown_bucket_5_10": "event_at",
    "cooldown_bucket_11_plus": "event_at",
    "fresh_recent_listing": "fetched_at",
    "stale_listing": "fetched_at",
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
    extra_paths: Optional[Dict[str, str]] = None,
) -> Tuple[str, List[Any]]:
    """Build the executable SQL and bound params for a selector."""
    if name not in RUNNABLE_SELECTORS:
        raise ValueError(f"No executable query implemented for selector '{name}'")
    template = _SELECTOR_SQL_TEMPLATES[name]
    where_sql, params = _build_where(name, window_start, window_end)
    format_kwargs: Dict[str, str] = {"path": path, "where": where_sql}
    if extra_paths:
        format_kwargs.update(extra_paths)
    sql = template.format(**format_kwargs)
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
    """Run one selector and return coverage/candidate diagnostics.

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
        extra_paths = None
        extra_tables = _SELECTOR_EXTRA_TABLES.get(name)
        if extra_tables:
            extra_paths = {
                f"{extra_table}_path": resolve_table_path(extra_table, base_path)
                for extra_table in extra_tables
            }
        candidate_sql, params = build_selector_query(
            name, path, window_start, window_end, extra_paths=extra_paths
        )
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
    """Run the selectors and return coverage/candidate diagnostics.

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
