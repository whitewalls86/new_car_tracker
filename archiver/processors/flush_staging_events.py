"""
Flush staging event tables to MinIO Parquet, then DELETE flushed rows.

Each staging.*_events table is a temporary buffer. This processor reads all
rows up to a snapshot max primary key, writes them as hive-partitioned Parquet
to MinIO, then deletes the flushed rows.

Pattern per table:
  1. SELECT MAX(pk) to establish a flush boundary
  2. SELECT all rows WHERE pk <= max_pk
  3. Write partitioned Parquet to MinIO
  4. DELETE WHERE pk <= max_pk (only on write success)

MinIO layout:
  s3://bronze/ops/<table_name>/year=YYYY/month=MM/part-<uuid>-0.parquet

Flushing is idempotent: if a flush is interrupted after the Parquet write but
before the DELETE, re-running will overwrite the existing file (same partition,
new filename) and then delete. Duplicate rows in Parquet are acceptable for
append-only event logs.
"""
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from shared.db import get_conn
from shared.minio import BUCKET, get_s3fs

logger = logging.getLogger("archiver")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_str(v) -> Optional[str]:
    """Coerce UUID objects and other non-None values to str."""
    return str(v) if v is not None else None


def _ensure_utc(v) -> Optional[datetime]:
    """Ensure a datetime is UTC-aware; pass None through."""
    if v is None:
        return None
    if isinstance(v, datetime) and v.tzinfo is None:
        return v.replace(tzinfo=timezone.utc)
    return v


# ---------------------------------------------------------------------------
# PyArrow schemas (include year/month partition columns)
# ---------------------------------------------------------------------------

_ARTIFACTS_QUEUE_EVENTS_SCHEMA = pa.schema([
    pa.field("event_id",      pa.int64()),
    pa.field("artifact_id",   pa.int64()),
    pa.field("status",        pa.string()),
    pa.field("event_at",      pa.timestamp("us", tz="UTC")),
    pa.field("minio_path",    pa.string()),
    pa.field("artifact_type", pa.string()),
    pa.field("fetched_at",    pa.timestamp("us", tz="UTC")),
    pa.field("listing_id",    pa.string()),
    pa.field("run_id",        pa.string()),
    pa.field("year",          pa.int32()),
    pa.field("month",         pa.int32()),
])

_DETAIL_SCRAPE_CLAIM_EVENTS_SCHEMA = pa.schema([
    pa.field("event_id",     pa.int64()),
    pa.field("listing_id",   pa.string()),
    pa.field("run_id",       pa.string()),
    pa.field("status",       pa.string()),
    pa.field("stale_reason", pa.string()),
    pa.field("vin",          pa.string()),
    pa.field("event_at",     pa.timestamp("us", tz="UTC")),
    pa.field("year",         pa.int32()),
    pa.field("month",        pa.int32()),
])

_BLOCKED_COOLDOWN_EVENTS_SCHEMA = pa.schema([
    pa.field("event_id",        pa.int64()),
    pa.field("listing_id",      pa.string()),
    pa.field("event_type",      pa.string()),
    pa.field("num_of_attempts", pa.int32()),
    pa.field("event_at",        pa.timestamp("us", tz="UTC")),
    pa.field("year",            pa.int32()),
    pa.field("month",           pa.int32()),
])

_PRICE_OBSERVATION_EVENTS_SCHEMA = pa.schema([
    pa.field("event_id",    pa.int64()),
    pa.field("listing_id",  pa.string()),
    pa.field("vin",         pa.string()),
    pa.field("price",       pa.int32()),
    pa.field("make",        pa.string()),
    pa.field("model",       pa.string()),
    pa.field("artifact_id", pa.int64()),
    pa.field("event_type",  pa.string()),
    pa.field("source",      pa.string()),
    pa.field("event_at",    pa.timestamp("us", tz="UTC")),
    pa.field("year",        pa.int32()),
    pa.field("month",       pa.int32()),
])

_VIN_TO_LISTING_EVENTS_SCHEMA = pa.schema([
    pa.field("event_id",            pa.int64()),
    pa.field("vin",                 pa.string()),
    pa.field("listing_id",          pa.string()),
    pa.field("artifact_id",         pa.int64()),
    pa.field("event_type",          pa.string()),
    pa.field("previous_listing_id", pa.string()),
    pa.field("event_at",            pa.timestamp("us", tz="UTC")),
    pa.field("year",                pa.int32()),
    pa.field("month",               pa.int32()),
])


# ---------------------------------------------------------------------------
# Table configurations
# ---------------------------------------------------------------------------
#
# Each entry:
#   table          — fully qualified Postgres table (schema.table)
#   pk             — primary key column; used for snapshot boundary + DELETE
#   ts_col         — timestamp column to derive year/month partition values
#   db_columns     — ordered SELECT column list (must match schema minus year/month)
#   schema         — pyarrow schema (includes year/month partition fields)
#   minio_prefix   — path under BUCKET (no trailing slash)
#   uuid_cols      — set of column names that psycopg2 returns as uuid.UUID objects

_TABLE_CONFIGS = [
    {
        "table":        "staging.artifacts_queue_events",
        "pk":           "event_id",
        "ts_col":       "event_at",
        "db_columns":   [
            "event_id", "artifact_id", "status", "event_at",
            "minio_path", "artifact_type", "fetched_at", "listing_id", "run_id",
        ],
        "schema":       _ARTIFACTS_QUEUE_EVENTS_SCHEMA,
        "minio_prefix": "ops/artifacts_queue_events",
        "uuid_cols":    set(),
    },
    {
        "table":        "staging.detail_scrape_claim_events",
        "pk":           "event_id",
        "ts_col":       "event_at",
        "db_columns":   [
            "event_id", "listing_id", "run_id", "status",
            "stale_reason", "vin", "event_at",
        ],
        "schema":       _DETAIL_SCRAPE_CLAIM_EVENTS_SCHEMA,
        "minio_prefix": "ops/detail_scrape_claim_events",
        "uuid_cols":    {"listing_id", "run_id", "vin"},
    },
    {
        "table":        "staging.blocked_cooldown_events",
        "pk":           "event_id",
        "ts_col":       "event_at",
        "db_columns":   [
            "event_id", "listing_id", "event_type", "num_of_attempts", "event_at",
        ],
        "schema":       _BLOCKED_COOLDOWN_EVENTS_SCHEMA,
        "minio_prefix": "ops/blocked_cooldown_events",
        "uuid_cols":    {"listing_id"},
    },
    {
        "table":        "staging.price_observation_events",
        "pk":           "event_id",
        "ts_col":       "event_at",
        "db_columns":   [
            "event_id", "listing_id", "vin", "price", "make",
            "model", "artifact_id", "event_type", "source", "event_at",
        ],
        "schema":       _PRICE_OBSERVATION_EVENTS_SCHEMA,
        "minio_prefix": "ops/price_observation_events",
        "uuid_cols":    {"listing_id"},
    },
    {
        "table":        "staging.vin_to_listing_events",
        "pk":           "event_id",
        "ts_col":       "event_at",
        "db_columns":   [
            "event_id", "vin", "listing_id", "artifact_id",
            "event_type", "previous_listing_id", "event_at",
        ],
        "schema":       _VIN_TO_LISTING_EVENTS_SCHEMA,
        "minio_prefix": "ops/vin_to_listing_events",
        "uuid_cols":    {"listing_id", "previous_listing_id"},
    },
]


# ---------------------------------------------------------------------------
# Core flush logic
# ---------------------------------------------------------------------------

def _flush_one(config: dict, conn, fs) -> Dict[str, Any]:
    """
    Flush a single staging table to MinIO and DELETE the flushed rows.

    Returns {"table": str, "flushed": int, "error": str|None}.
    """
    table        = config["table"]
    pk           = config["pk"]
    ts_col       = config["ts_col"]
    db_columns   = config["db_columns"]
    schema       = config["schema"]
    minio_prefix = config["minio_prefix"]
    uuid_cols    = config["uuid_cols"]

    try:
        # 1. Establish snapshot boundary
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX({pk}) FROM {table}")  # noqa: S608
            max_pk = cur.fetchone()[0]

        if max_pk is None:
            logger.debug("flush_staging: %s is empty, skipping", table)
            return {"table": table, "flushed": 0, "error": None}

        # 2. Fetch rows up to snapshot boundary
        cols_sql = ", ".join(db_columns)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {cols_sql} FROM {table} WHERE {pk} <= %s ORDER BY {pk}",  # noqa: S608
                (max_pk,),
            )
            raw_rows = cur.fetchall()

        if not raw_rows:
            return {"table": table, "flushed": 0, "error": None}

        # 3. Build row dicts, coerce types, add partition columns
        rows: List[Dict[str, Any]] = []
        for raw in raw_rows:
            row = dict(zip(db_columns, raw))

            for col in uuid_cols:
                row[col] = _to_str(row.get(col))

            for col, val in row.items():
                if isinstance(val, datetime):
                    row[col] = _ensure_utc(val)

            ts = row.get(ts_col)
            if ts is not None:
                row["year"]  = ts.year
                row["month"] = ts.month
            else:
                row["year"]  = 0
                row["month"] = 0

            rows.append(row)

        # 4. Write Parquet to MinIO
        arrow_table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_to_dataset(
            arrow_table,
            root_path=f"s3://{BUCKET}/{minio_prefix}",
            partition_cols=["year", "month"],
            filesystem=fs,
            compression="zstd",
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{uuid.uuid4()}-{{i}}.parquet",
        )
        logger.info("flush_staging: wrote %d rows → %s/%s", len(rows), BUCKET, minio_prefix)

        # 5. Delete flushed rows
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE {pk} <= %s",  # noqa: S608
                (max_pk,),
            )
            deleted = cur.rowcount
        conn.commit()

        logger.info("flush_staging: deleted %d rows from %s", deleted, table)
        return {"table": table, "flushed": deleted, "error": None}

    except Exception as e:
        logger.error("flush_staging: failed for %s: %s", table, e, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"table": table, "flushed": 0, "error": str(e)}


def flush_staging_events() -> Dict[str, Any]:
    """
    Flush all staging event tables to MinIO.

    Each table is flushed independently — a failure on one table does not
    abort the others. Returns a summary with per-table results.

    Called by POST /flush/staging/run (Airflow DAG trigger or manual).
    """
    try:
        conn = get_conn()
    except Exception as e:
        logger.error("flush_staging: DB connection failed: %s", e)
        return {"total_flushed": 0, "tables": [], "error": str(e)}

    try:
        fs = get_s3fs()
    except Exception as e:
        conn.close()
        logger.error("flush_staging: MinIO connection failed: %s", e)
        return {"total_flushed": 0, "tables": [], "error": str(e)}

    results = []
    try:
        for config in _TABLE_CONFIGS:
            result = _flush_one(config, conn, fs)
            results.append(result)
    finally:
        conn.close()

    total_flushed = sum(r["flushed"] for r in results)
    had_errors    = any(r["error"] for r in results)

    logger.info(
        "flush_staging: complete — total_flushed=%d tables=%d errors=%d",
        total_flushed,
        len(results),
        sum(1 for r in results if r["error"]),
    )

    return {
        "total_flushed": total_flushed,
        "tables":        results,
        "error":         "one or more tables failed" if had_errors else None,
    }
