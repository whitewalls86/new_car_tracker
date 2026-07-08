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
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from archiver.processors.lake_source_audit import SOURCE_TABLE_SPECS, resolve_table_path
from shared.duckdb_s3 import get_duckdb_s3_connection
from shared.query_loader import load_query

logger = logging.getLogger("archiver")

_SQL_DIR = Path(__file__).parents[1] / "sql" / "lake_snapshot_selectors"
_CONFIG_PATH = Path(__file__).parents[1] / "config" / "lake_snapshot_selectors.yml"

_VALID_WINDOW_ANCHORS = ("window_end",)


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


@dataclass(frozen=True)
class SelectorConfig:
    name: str
    min_entities: int
    entity_key: str
    source_table: str
    sql_template: str
    timestamp_column: str
    description: str
    base_filters: Tuple[str, ...] = field(default_factory=tuple)
    extra_source_tables: Tuple[str, ...] = field(default_factory=tuple)
    window_anchor: Optional[str] = None


def _require(spec: Dict[str, Any], name: str, key: str) -> Any:
    if key not in spec:
        raise ValueError(
            f"Selector config error: selector '{name}' is missing required key '{key}'"
        )
    return spec[key]


def _as_str_list(spec: Dict[str, Any], name: str, key: str) -> Tuple[str, ...]:
    value = spec.get(key, [])
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError(
            f"Selector config error: selector '{name}' key '{key}' must be a list of strings"
        )
    return tuple(value)


def _load_selector_configs(config_path: Path = _CONFIG_PATH) -> Dict[str, SelectorConfig]:
    """Load and validate archiver/config/lake_snapshot_selectors.yml.

    Raises ValueError with a descriptive message on any malformed entry, so
    a bad config fails loudly at import time rather than surfacing as a
    confusing runtime KeyError.
    """
    raw = yaml.safe_load(config_path.read_text())
    if (
        not isinstance(raw, dict)
        or "selectors" not in raw
        or not isinstance(raw["selectors"], dict)
    ):
        raise ValueError(
            f"Selector config error: {config_path} must have a top-level 'selectors' mapping"
        )

    configs: Dict[str, SelectorConfig] = {}
    for name, spec in raw["selectors"].items():
        if not isinstance(spec, dict):
            raise ValueError(f"Selector config error: selector '{name}' must be a mapping")

        min_entities = _require(spec, name, "min_entities")
        is_valid_int = isinstance(min_entities, int) and not isinstance(min_entities, bool)
        if not is_valid_int or min_entities < 0:
            raise ValueError(
                f"Selector config error: selector '{name}' min_entities must be a "
                f"non-negative integer"
            )

        source_table = _require(spec, name, "source_table")
        extra_source_tables = _as_str_list(spec, name, "extra_source_tables")
        for table_name in (source_table, *extra_source_tables):
            if table_name not in SOURCE_TABLE_SPECS:
                raise ValueError(
                    f"Selector config error: selector '{name}' references unknown "
                    f"source table '{table_name}'"
                )

        sql_template = _require(spec, name, "sql_template")
        if not (_SQL_DIR / f"{sql_template}.sql").is_file():
            raise ValueError(
                f"Selector config error: selector '{name}' sql_template "
                f"'{sql_template}' has no matching .sql file in {_SQL_DIR}"
            )

        window_anchor = spec.get("window_anchor")
        if window_anchor is not None and window_anchor not in _VALID_WINDOW_ANCHORS:
            raise ValueError(
                f"Selector config error: selector '{name}' window_anchor must be one of "
                f"{_VALID_WINDOW_ANCHORS}, got {window_anchor!r}"
            )

        configs[name] = SelectorConfig(
            name=name,
            min_entities=min_entities,
            entity_key=_require(spec, name, "entity_key"),
            source_table=source_table,
            sql_template=sql_template,
            timestamp_column=_require(spec, name, "timestamp_column"),
            description=_require(spec, name, "description"),
            base_filters=_as_str_list(spec, name, "base_filters"),
            extra_source_tables=extra_source_tables,
            window_anchor=window_anchor,
        )
    return configs


_SELECTOR_CONFIGS: Dict[str, SelectorConfig] = _load_selector_configs()

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
