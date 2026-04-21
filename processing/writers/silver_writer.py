"""
Silver observation writer — primary permanent record in MinIO.

Writes unified Parquet observations to:
    silver/observations/year=.../month=.../part-*.parquet

Uses s3fs + pyarrow (same pattern as archiver/processors/archive_artifacts.py).
Non-fatal: failures are logged and counted but never roll back Postgres writes.
"""
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from shared.db import db_cursor

logger = logging.getLogger(__name__)

SILVER_BUCKET = os.environ.get("MINIO_BUCKET", "bronze")

_SILVER_SCHEMA = None  # Lazy-loaded to avoid import cost when MinIO is disabled


def _get_schema():
    global _SILVER_SCHEMA
    if _SILVER_SCHEMA is None:
        import pyarrow as pa
        _SILVER_SCHEMA = pa.schema([
            # --- identifiers & metadata ---
            pa.field("artifact_id", pa.int64()),
            pa.field("listing_id", pa.string()),
            pa.field("vin", pa.string()),
            pa.field("canonical_detail_url", pa.string()),
            pa.field("source", pa.string()),
            pa.field("listing_state", pa.string()),
            pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
            pa.field("written_at", pa.timestamp("us", tz="UTC")),
            # --- core vehicle fields (detail + srp) ---
            pa.field("price", pa.int32()),
            pa.field("make", pa.string()),
            pa.field("model", pa.string()),
            pa.field("trim", pa.string()),
            pa.field("year", pa.int16()),
            pa.field("mileage", pa.int32()),
            pa.field("msrp", pa.int32()),
            pa.field("stock_type", pa.string()),
            pa.field("fuel_type", pa.string()),
            pa.field("body_style", pa.string()),
            # --- srp-specific fields ---
            pa.field("financing_type", pa.string()),
            pa.field("seller_zip", pa.string()),
            pa.field("seller_customer_id", pa.string()),
            pa.field("page_number", pa.int16()),
            pa.field("position_on_page", pa.int16()),
            pa.field("trid", pa.string()),
            pa.field("isa_context", pa.string()),
            # --- dealer fields (detail + carousel) ---
            pa.field("dealer_name", pa.string()),
            pa.field("dealer_zip", pa.string()),
            pa.field("customer_id", pa.string()),
            pa.field("seller_id", pa.string()),
            pa.field("dealer_street", pa.string()),
            pa.field("dealer_city", pa.string()),
            pa.field("dealer_state", pa.string()),
            pa.field("dealer_phone", pa.string()),
            pa.field("dealer_website", pa.string()),
            pa.field("dealer_cars_com_url", pa.string()),
            pa.field("dealer_rating", pa.float32()),
            # --- carousel-specific fields ---
            pa.field("body", pa.string()),
            pa.field("condition", pa.string()),
        ])
    return _SILVER_SCHEMA


def _minio_enabled() -> bool:
    return bool(os.environ.get("MINIO_ENDPOINT"))


def write_silver_observations_minio(
    observations: List[Dict[str, Any]],
) -> int:
    """
    Write a batch of observations to MinIO silver layer.

    Returns the number of rows successfully written (0 on failure or disabled).
    Never raises — failures are logged as warnings.
    """
    if not _minio_enabled():
        logger.debug("silver_writer: MINIO_ENDPOINT not set, skipping")
        return 0

    if not observations:
        return 0

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        from shared.minio import BUCKET, get_s3fs

        now = datetime.now(timezone.utc)

        schema = _get_schema()
        field_names = [f.name for f in schema if f.name != "written_at"]
        numeric_fields = {
            f.name for f in schema
            if pa.types.is_integer(f.type) or pa.types.is_floating(f.type)
        }

        rows = []
        for obs in observations:
            fetched_at = obs.get("fetched_at")
            if isinstance(fetched_at, str):
                fetched_at = datetime.fromisoformat(fetched_at)
            if fetched_at and fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)

            row = {name: obs.get(name) for name in field_names}
            # Normalize types
            if row.get("listing_id") is not None:
                row["listing_id"] = str(row["listing_id"])
            if row.get("listing_state") is None:
                row["listing_state"] = "active"
            # Empty strings can't be coerced to numeric types by pyarrow
            for field in numeric_fields:
                if row.get(field) == "":
                    row[field] = None
            row["fetched_at"] = fetched_at
            row["written_at"] = now
            rows.append(row)

        table = pa.Table.from_pylist(rows, schema=_get_schema())

        fs = get_s3fs()
        pq.write_to_dataset(
            table,
            root_path=f"s3://{BUCKET}/silver/observations",
            partition_cols=["source"],
            filesystem=fs,
            compression="zstd",
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{uuid.uuid4()}-{{i}}.parquet",
        )

        logger.info("silver_writer: wrote %d observations", len(rows))
        return len(rows)

    except Exception as e:
        logger.warning("silver_writer: write failed: %s", e, exc_info=True)
        return 0


_POSTGRES_INT_COLS = {
    "artifact_id", "price", "year", "mileage", "msrp",
    "page_number", "position_on_page",
}

_POSTGRES_COLS = [
    # identifiers & metadata
    "artifact_id", "listing_id", "vin", "canonical_detail_url",
    "source", "listing_state", "fetched_at",
    # core vehicle fields
    "price", "make", "model", "trim", "year", "mileage", "msrp",
    "stock_type", "fuel_type", "body_style",
    # dealer fields (detail + carousel)
    "dealer_name", "dealer_zip", "customer_id", "seller_id",
    "dealer_street", "dealer_city", "dealer_state", "dealer_phone",
    "dealer_website", "dealer_cars_com_url", "dealer_rating",
    # srp-specific fields
    "financing_type", "seller_zip", "seller_customer_id",
    "page_number", "position_on_page", "trid", "isa_context",
    # carousel-specific fields
    "body", "condition",
]

_INSERT_SQL = """
    INSERT INTO staging.silver_observations (
        artifact_id, listing_id, vin, canonical_detail_url,
        source, listing_state, fetched_at,
        price, make, model, trim, year, mileage, msrp,
        stock_type, fuel_type, body_style,
        dealer_name, dealer_zip, customer_id, seller_id,
        dealer_street, dealer_city, dealer_state, dealer_phone,
        dealer_website, dealer_cars_com_url, dealer_rating,
        financing_type, seller_zip, seller_customer_id,
        page_number, position_on_page, trid, isa_context,
        body, condition
    )
    VALUES %s
"""


def write_silver_observations_postgres(
    observations: List[Dict[str, Any]],
) -> int:
    """
    Write a batch of observations to the postgres staging table.

    Returns the number of rows successfully written (0 on failure).
    Never raises — failures are logged as warnings.
    """
    if not observations:
        return 0

    try:
        from psycopg2.extras import execute_values

        rows = []
        for obs in observations:
            fetched_at = obs.get("fetched_at")
            if isinstance(fetched_at, str):
                fetched_at = datetime.fromisoformat(fetched_at)
            if fetched_at and fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)

            row = {name: obs.get(name) for name in _POSTGRES_COLS}
            if row.get("listing_id") is not None:
                row["listing_id"] = str(row["listing_id"])
            if row.get("listing_state") is None:
                row["listing_state"] = "active"
            for field in _POSTGRES_INT_COLS:
                if row.get(field) == "":
                    row[field] = None
            row["fetched_at"] = fetched_at
            rows.append(tuple(row[col] for col in _POSTGRES_COLS))

        with db_cursor(error_context="silver_write") as cur:
            execute_values(cur, _INSERT_SQL, rows)
            return cur.rowcount

    except Exception as e:
        logger.warning("silver_writer: postgres write failed: %s", e, exc_info=True)
        return 0