"""
Read-only DuckDB/MinIO source table audit for CI lake snapshot exports
(Plan 120, Phase 2).

Proves the archiver can configure DuckDB against MinIO (or a local fixture
directory in tests), read the expected lake source tables, and return useful
diagnostics (row counts, timestamp bounds, distinct VIN/listing counts)
without generating a snapshot archive. No writes are performed.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from shared.duckdb_s3 import get_duckdb_s3_connection
from shared.minio import BUCKET

logger = logging.getLogger("archiver")

# table_name -> relative Parquet glob + columns used for diagnostics
SOURCE_TABLE_SPECS: Dict[str, Dict[str, Optional[str]]] = {
    "silver_observations": {
        "relative_path": "silver_normalized/observations/**/*.parquet",
        "timestamp_col": "fetched_at",
        "vin_col": "vin",
        "listing_col": "listing_id",
    },
    "price_observation_events": {
        "relative_path": "ops_normalized/price_observation_events/**/*.parquet",
        "timestamp_col": "event_at",
        "vin_col": "vin",
        "listing_col": "listing_id",
    },
    "vin_to_listing_events": {
        "relative_path": "ops_normalized/vin_to_listing_events/**/*.parquet",
        "timestamp_col": "event_at",
        "vin_col": "vin",
        "listing_col": "listing_id",
    },
    "blocked_cooldown_events": {
        "relative_path": "ops_normalized/blocked_cooldown_events/**/*.parquet",
        "timestamp_col": "event_at",
        "vin_col": None,
        "listing_col": "listing_id",
    },
}


def resolve_table_path(table_name: str, base_path: Optional[str]) -> str:
    """Resolve the Parquet glob for a logical source table.

    When *base_path* is set (test fixture mode), the table is read from a
    local directory instead of s3://{MINIO_BUCKET}.
    """
    spec = SOURCE_TABLE_SPECS[table_name]
    if base_path:
        return f"{base_path.rstrip('/')}/{spec['relative_path']}"
    return f"s3://{BUCKET}/{spec['relative_path']}"


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    return str(value)


def _audit_table(
    con,
    table_name: str,
    base_path: Optional[str],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
) -> Dict[str, Any]:
    spec = SOURCE_TABLE_SPECS[table_name]
    path = resolve_table_path(table_name, base_path)
    result: Dict[str, Any] = {
        "path": path,
        "exists": False,
        "rows": None,
        "min_timestamp": None,
        "max_timestamp": None,
        "distinct_vins": None,
        "distinct_listing_ids": None,
        "error": None,
    }

    select_parts = [
        "count(*) AS row_count",
        f"min({spec['timestamp_col']}) AS min_ts",
        f"max({spec['timestamp_col']}) AS max_ts",
    ]
    if spec["vin_col"]:
        select_parts.append(f"count(distinct {spec['vin_col']}) AS distinct_vins")
    if spec["listing_col"]:
        select_parts.append(f"count(distinct {spec['listing_col']}) AS distinct_listing_ids")

    where_clauses = []
    params: list = []
    if window_start is not None:
        where_clauses.append(f"{spec['timestamp_col']} >= ?")
        params.append(window_start)
    if window_end is not None:
        where_clauses.append(f"{spec['timestamp_col']} < ?")
        params.append(window_end)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM read_parquet('{path}', union_by_name=true) "
        f"{where_sql}"
    )

    try:
        row = iter(con.execute(query, params).fetchone())
        result["rows"] = next(row)
        result["min_timestamp"] = _iso(next(row))
        result["max_timestamp"] = _iso(next(row))
        if spec["vin_col"]:
            result["distinct_vins"] = next(row)
        if spec["listing_col"]:
            result["distinct_listing_ids"] = next(row)
        result["exists"] = True
    except Exception as e:
        result["error"] = str(e)
        logger.warning("lake_source_audit: table=%s path=%s error=%s", table_name, path, e)

    return result


def audit_source_tables(
    base_path: Optional[str] = None,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Read-only audit of the four expected lake source tables.

    Returns a dict shaped like:
        {"tables": {...}, "window": {...}, "errors": [...], "ok": bool}

    Never raises for a missing/unreadable table — the error is captured at
    the table level and `ok` is set to False.
    """
    if base_path:
        import duckdb
        con = duckdb.connect()
    else:
        con = get_duckdb_s3_connection()

    t0 = time.monotonic()
    logger.info("lake_source_audit: audit_source_tables start tables=%d", len(SOURCE_TABLE_SPECS))
    try:
        tables: Dict[str, Any] = {}
        errors: list = []
        for table_name in SOURCE_TABLE_SPECS:
            table_result = _audit_table(con, table_name, base_path, window_start, window_end)
            tables[table_name] = table_result
            if table_result["error"] is not None:
                errors.append(f"{table_name}: {table_result['error']}")

        result = {
            "tables": tables,
            "window": {
                "start": _iso(window_start),
                "end": _iso(window_end),
            },
            "errors": errors,
            "ok": len(errors) == 0,
        }
        logger.info(
            "lake_source_audit: audit_source_tables end elapsed_s=%.2f ok=%s errors=%d",
            time.monotonic() - t0, result["ok"], len(errors),
        )
        return result
    finally:
        con.close()
