"""
Silver observation writer — buffers parsed observations to staging.silver_observations.

The archiver service flushes staging.silver_observations to MinIO Parquet on a
schedule via POST /flush/silver/run. See archiver/processors/flush_silver_observations.py.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from shared.db import db_cursor

logger = logging.getLogger(__name__)

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
