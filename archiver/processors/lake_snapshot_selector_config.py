"""
Selector config loading/validation for CI lake snapshot exports (Plan 120).

Loads and validates archiver/config/lake_snapshot_selectors.yml into typed
SelectorConfig objects consumed by archiver/processors/lake_snapshot_selectors.py.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from archiver.processors.lake_source_audit import SOURCE_TABLE_SPECS

DEFAULT_CONFIG_PATH = Path(__file__).parents[1] / "config" / "lake_snapshot_selectors.yml"
DEFAULT_SQL_DIR = Path(__file__).parents[1] / "sql" / "lake_snapshot_selectors"

VALID_WINDOW_ANCHORS = ("window_end",)


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
    lookback_days: Optional[int] = None


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


def _parse_selector_config(name: str, spec: Dict[str, Any], sql_dir: Path) -> SelectorConfig:
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
    if not (sql_dir / f"{sql_template}.sql").is_file():
        raise ValueError(
            f"Selector config error: selector '{name}' sql_template "
            f"'{sql_template}' has no matching .sql file in {sql_dir}"
        )

    window_anchor = spec.get("window_anchor")
    if window_anchor is not None and window_anchor not in VALID_WINDOW_ANCHORS:
        raise ValueError(
            f"Selector config error: selector '{name}' window_anchor must be one of "
            f"{VALID_WINDOW_ANCHORS}, got {window_anchor!r}"
        )

    lookback_days = spec.get("lookback_days")
    if lookback_days is not None:
        is_valid_lookback = isinstance(lookback_days, int) and not isinstance(lookback_days, bool)
        if not is_valid_lookback or lookback_days <= 0:
            raise ValueError(
                f"Selector config error: selector '{name}' lookback_days must be a "
                f"positive integer"
            )

    return SelectorConfig(
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
        lookback_days=lookback_days,
    )


def load_selector_configs(
    config_path: Path = DEFAULT_CONFIG_PATH,
    sql_dir: Path = DEFAULT_SQL_DIR,
) -> Dict[str, SelectorConfig]:
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

    return {
        name: _parse_selector_config(name, spec, sql_dir)
        for name, spec in raw["selectors"].items()
    }
