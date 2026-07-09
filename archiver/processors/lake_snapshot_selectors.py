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
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from archiver.processors.lake_snapshot_selector_config import (
    DEFAULT_SQL_DIR,
    SelectorConfig,
    load_selector_configs,
)
from archiver.processors.lake_source_audit import resolve_table_path
from shared.duckdb_s3 import get_duckdb_s3_connection
from shared.query_loader import load_query

logger = logging.getLogger("archiver")

_SQL_DIR = DEFAULT_SQL_DIR


@lru_cache(maxsize=None)
def _q(name: str) -> str:
    return load_query(_SQL_DIR, name)


@dataclass(frozen=True)
class Selector:
    name: str
    min_entities: int
    entity_key: str
    sql: str
    description: str


_SELECTOR_CONFIGS: Dict[str, SelectorConfig] = load_selector_configs()

RUNNABLE_SELECTORS: Tuple[str, ...] = tuple(_SELECTOR_CONFIGS.keys())


def build_selector_registry() -> Dict[str, Selector]:
    """Build the selector registry, keyed by selector name."""
    return {
        name: Selector(
            name=config.name,
            min_entities=config.min_entities,
            entity_key=config.entity_key,
            sql=_q(config.sql_template),
            description=config.description,
        )
        for name, config in _SELECTOR_CONFIGS.items()
    }


def _build_where(
    name: str, window_start: Optional[datetime], window_end: Optional[datetime]
) -> Tuple[str, List[Any]]:
    config = _SELECTOR_CONFIGS[name]
    clauses = list(config.base_filters)
    params: List[Any] = []
    ts_col = config.timestamp_column
    if config.lookback_days is not None:
        # As-of selectors (e.g. stale_listing) need history from *before*
        # window_start to determine the latest observation as of window_end
        # — the normal [window_start, window_end) filter would exclude
        # exactly the rows that answer the question. A bounded lookback
        # avoids an unbounded full-table scan while still comfortably
        # covering the selector's staleness threshold (see selector config
        # comments for the specific margin).
        if window_end is not None:
            clauses.append(f"{ts_col} >= ?")
            params.append(window_end - timedelta(days=config.lookback_days))
            clauses.append(f"{ts_col} <= ?")
            params.append(window_end)
    else:
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
    config = _SELECTOR_CONFIGS[name]
    template = _q(config.sql_template)
    where_sql, params = _build_where(name, window_start, window_end)
    if config.window_anchor == "window_end":
        # Appended last: the `?` this binds sits in the diffed CTE, which
        # follows the {where} clause in the compiled SQL text, so DuckDB's
        # positional binding order matches.
        params = params + [window_end]
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
    config = _SELECTOR_CONFIGS[name]
    path = resolve_table_path(config.source_table, base_path)

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

    t0 = time.monotonic()
    logger.info(
        "lake_snapshot_selectors: selector=%s start entity_key=%s required=%s",
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
        agg_sql = _wrap_aggregate_query(candidate_sql, selector.entity_key)
        candidate_rows, entities, sample_entities = con.execute(agg_sql, params).fetchone()
        result["candidate_rows"] = candidate_rows
        result["entities"] = entities
        result["sample_entities"] = list(sample_entities) if sample_entities else []
        result["status"] = "pass" if entities >= selector.min_entities else "fail"
        logger.info(
            "lake_snapshot_selectors: selector=%s end elapsed_s=%.2f entities=%s "
            "candidate_rows=%s status=%s",
            name, time.monotonic() - t0, entities, candidate_rows, result["status"],
        )
    except Exception as e:
        result["error"] = str(e)
        logger.warning(
            "lake_snapshot_selectors: selector=%s error elapsed_s=%.2f path=%s error=%s",
            name, time.monotonic() - t0, path, e,
        )

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

    t0 = time.monotonic()
    logger.info(
        "lake_snapshot_selectors: run_lake_selectors start selectors=%d window_start=%s "
        "window_end=%s",
        len(names), window_start.isoformat() if window_start else None,
        window_end.isoformat() if window_end else None,
    )
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

        result = {
            "selectors": selectors,
            "errors": errors,
            "ok": len(errors) == 0,
        }
        logger.info(
            "lake_snapshot_selectors: run_lake_selectors end elapsed_s=%.2f ok=%s errors=%d",
            time.monotonic() - t0, result["ok"], len(errors),
        )
        return result
    finally:
        con.close()
