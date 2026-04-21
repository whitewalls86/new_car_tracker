"""
Flush staging.silver_observations to MinIO silver layer, then DELETE flushed rows.

This is the primary path for landing parsed observations in MinIO. The
processing service writes to staging.silver_observations in Postgres; this
processor reads those rows, writes them as hive-partitioned Parquet, and
deletes the source rows.

MinIO layout:
  s3://bronze/silver/observations/source=.../year=YYYY/month=MM/day=DD/part-<uuid>-0.parquet

Schema mirrors processing/writers/silver_writer.py with two differences:
  - written_at is set to now() at flush time (not stored in Postgres)
  - source/year/month/day partition columns are derived from source + fetched_at
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

_MINIO_PREFIX = "silver/observations"

_DB_COLUMNS = [
    "id",
    # identifiers & metadata
    "artifact_id", "listing_id", "vin", "canonical_detail_url",
    "source", "listing_state", "fetched_at",
    # core vehicle fields
    "price", "make", "model", "trim", "year", "mileage", "msrp",
    "stock_type", "fuel_type", "body_style",
    # dealer fields
    "dealer_name", "dealer_zip", "customer_id", "seller_id",
    "dealer_street", "dealer_city", "dealer_state", "dealer_phone",
    "dealer_website", "dealer_cars_com_url", "dealer_rating",
    # srp-specific fields
    "financing_type", "seller_zip", "seller_customer_id",
    "page_number", "position_on_page", "trid", "isa_context",
    # carousel-specific fields
    "body", "condition",
]

_SCHEMA = pa.schema([
    # identifiers & metadata
    pa.field("artifact_id",          pa.int64()),
    pa.field("listing_id",           pa.string()),
    pa.field("vin",                  pa.string()),
    pa.field("canonical_detail_url", pa.string()),
    pa.field("source",               pa.string()),
    pa.field("listing_state",        pa.string()),
    pa.field("fetched_at",           pa.timestamp("us", tz="UTC")),
    pa.field("written_at",           pa.timestamp("us", tz="UTC")),
    # core vehicle fields
    pa.field("price",                pa.int32()),
    pa.field("make",                 pa.string()),
    pa.field("model",                pa.string()),
    pa.field("trim",                 pa.string()),
    pa.field("year",                 pa.int16()),
    pa.field("mileage",              pa.int32()),
    pa.field("msrp",                 pa.int32()),
    pa.field("stock_type",           pa.string()),
    pa.field("fuel_type",            pa.string()),
    pa.field("body_style",           pa.string()),
    # dealer fields
    pa.field("dealer_name",          pa.string()),
    pa.field("dealer_zip",           pa.string()),
    pa.field("customer_id",          pa.string()),
    pa.field("seller_id",            pa.string()),
    pa.field("dealer_street",        pa.string()),
    pa.field("dealer_city",          pa.string()),
    pa.field("dealer_state",         pa.string()),
    pa.field("dealer_phone",         pa.string()),
    pa.field("dealer_website",       pa.string()),
    pa.field("dealer_cars_com_url",  pa.string()),
    pa.field("dealer_rating",        pa.float32()),
    # srp-specific fields
    pa.field("financing_type",       pa.string()),
    pa.field("seller_zip",           pa.string()),
    pa.field("seller_customer_id",   pa.string()),
    pa.field("page_number",          pa.int16()),
    pa.field("position_on_page",     pa.int16()),
    pa.field("trid",                 pa.string()),
    pa.field("isa_context",          pa.string()),
    # carousel-specific fields
    pa.field("body",                 pa.string()),
    pa.field("condition",            pa.string()),
    # hive partition columns (derived at flush time, not stored in Postgres)
    pa.field("obs_year",             pa.int32()),
    pa.field("obs_month",            pa.int32()),
    pa.field("obs_day",              pa.int32()),
])

_INT16_COLS = {"year", "mileage", "msrp", "price", "page_number", "position_on_page"}


def _ensure_utc(v) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime) and v.tzinfo is None:
        return v.replace(tzinfo=timezone.utc)
    return v


def flush_silver_observations() -> Dict[str, Any]:
    """
    Flush staging.silver_observations to MinIO silver layer.

    Reads all rows up to a snapshot max id, writes Parquet partitioned by
    source/obs_year/obs_month/obs_day (derived from source + fetched_at),
    then deletes the flushed rows.

    Called by POST /flush/silver/run (Airflow DAG or manual trigger).
    """
    try:
        conn = get_conn()
    except Exception as e:
        logger.error("flush_silver: DB connection failed: %s", e)
        return {"flushed": 0, "error": str(e)}

    try:
        fs = get_s3fs()
    except Exception as e:
        conn.close()
        logger.error("flush_silver: MinIO connection failed: %s", e)
        return {"flushed": 0, "error": str(e)}

    try:
        # 1. Snapshot boundary
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(id) FROM staging.silver_observations")
            max_id = cur.fetchone()[0]

        if max_id is None:
            logger.debug("flush_silver: staging.silver_observations is empty, skipping")
            return {"flushed": 0, "error": None}

        # 2. Fetch rows
        cols_sql = ", ".join(_DB_COLUMNS)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {cols_sql} FROM staging.silver_observations"  # noqa: S608
                f" WHERE id <= %s ORDER BY id",
                (max_id,),
            )
            raw_rows = cur.fetchall()

        if not raw_rows:
            return {"flushed": 0, "error": None}

        # 3. Build row dicts
        written_at = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for raw in raw_rows:
            row = dict(zip(_DB_COLUMNS, raw))
            row.pop("id")  # not in Parquet schema

            fetched_at = _ensure_utc(row.get("fetched_at"))
            row["fetched_at"] = fetched_at
            row["written_at"] = written_at

            # Coerce empty strings for numeric columns pyarrow won't cast
            for col in _INT16_COLS:
                if row.get(col) == "":
                    row[col] = None

            ts = fetched_at or written_at
            row["obs_year"]  = ts.year
            row["obs_month"] = ts.month
            row["obs_day"]   = ts.day

            rows.append(row)

        # 4. Write Parquet
        arrow_table = pa.Table.from_pylist(rows, schema=_SCHEMA)
        pq.write_to_dataset(
            arrow_table,
            root_path=f"s3://{BUCKET}/{_MINIO_PREFIX}",
            partition_cols=["source", "obs_year", "obs_month", "obs_day"],
            filesystem=fs,
            compression="zstd",
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{uuid.uuid4()}-{{i}}.parquet",
        )
        logger.info(
            "flush_silver: wrote %d rows → %s/%s", len(rows), BUCKET, _MINIO_PREFIX
        )

        # 5. Delete flushed rows
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM staging.silver_observations WHERE id <= %s", (max_id,)
            )
            deleted = cur.rowcount
        conn.commit()

        logger.info("flush_silver: deleted %d rows from staging.silver_observations", deleted)
        return {"flushed": deleted, "error": None}

    except Exception as e:
        logger.error("flush_silver: failed: %s", e, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"flushed": 0, "error": str(e)}
    finally:
        conn.close()
