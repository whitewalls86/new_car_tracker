"""
Shared SQL predicate-building helpers for CI lake snapshot exports (Plan 120).

Used by both cohort closure (`lake_snapshot_cohort.py`) and the Gate D
materialization writer (`lake_snapshot_export.py`) to build identical
IN-clause and time-window predicates against the four supported source
tables, so filtering logic does not drift between the two phases.
"""
from datetime import datetime
from typing import Any, List, Optional, Tuple


def table_time_where(
    window_start: Optional[datetime], window_end: Optional[datetime], ts_col: str
) -> Tuple[List[str], List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    if window_start is not None:
        clauses.append(f"{ts_col} >= ?")
        params.append(window_start)
    if window_end is not None:
        clauses.append(f"{ts_col} < ?")
        params.append(window_end)
    return clauses, params


def in_clause(column: str, values) -> Tuple[str, List[Any]]:
    values = list(values)
    if not values:
        return "FALSE", []
    placeholders = ", ".join(["?"] * len(values))
    return f"{column} IN ({placeholders})", values
