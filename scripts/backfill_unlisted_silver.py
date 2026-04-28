"""
Backfill silver observations for unlisted listings that were written without a VIN.

When a detail page is an "unlisted" page, Cars.com doesn't serve the activity JSON,
so primary.get("vin") is None. The silver row is written with vin=NULL, which means
int_latest_observation (WHERE vin17 IS NOT NULL) never picks it up, and the vehicle
is invisible to mart_vehicle_snapshot as 'unlisted'.

This script:
  1. Reads price_observation_events (event_type='deleted', source='detail') from MinIO
  2. Joins to vin_to_listing_events to resolve VINs (covers ~98% of cases)
  3. Takes the most recent deleted event per listing
  4. Writes corrected silver rows directly to MinIO with listing_state='unlisted'

Safe to run once. Running twice will write duplicate Parquet files — dbt will still
produce correct results (row_number() deduplication in int_latest_observation picks
the most recent row per vin17), but it wastes storage.
"""
import logging
import os
import uuid
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"].replace("http://", "")
MINIO_USER     = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
BUCKET         = os.environ.get("MINIO_BUCKET", "bronze")
MINIO_PREFIX   = "silver/observations"

# ---------------------------------------------------------------------------
# Schema (mirrors flush_silver_observations.py)
# ---------------------------------------------------------------------------

_SCHEMA = pa.schema([
    pa.field("artifact_id",          pa.int64()),
    pa.field("listing_id",           pa.string()),
    pa.field("vin",                  pa.string()),
    pa.field("canonical_detail_url", pa.string()),
    pa.field("source",               pa.string()),
    pa.field("listing_state",        pa.string()),
    pa.field("fetched_at",           pa.timestamp("us", tz="UTC")),
    pa.field("written_at",           pa.timestamp("us", tz="UTC")),
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
    pa.field("financing_type",       pa.string()),
    pa.field("seller_zip",           pa.string()),
    pa.field("seller_customer_id",   pa.string()),
    pa.field("page_number",          pa.int16()),
    pa.field("position_on_page",     pa.int16()),
    pa.field("trid",                 pa.string()),
    pa.field("isa_context",          pa.string()),
    pa.field("body",                 pa.string()),
    pa.field("condition",            pa.string()),
    pa.field("obs_year",             pa.int32()),
    pa.field("obs_month",            pa.int32()),
    pa.field("obs_day",              pa.int32()),
])


def main():
    # --- Step 1: Query MinIO for unlisted events with resolved VINs -----------
    logger.info("Connecting to DuckDB and configuring MinIO credentials...")
    con = duckdb.connect()
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_USER}';
        SET s3_secret_access_key='{MINIO_PASSWORD}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    logger.info("Querying unlisted events with resolvable VINs and make/model...")
    rows = con.execute("""
        WITH deleted AS (
            -- Most recent deleted event per listing
            SELECT
                listing_id,
                arg_max(artifact_id, event_at) AS artifact_id,
                max(event_at)                  AS fetched_at
            FROM read_parquet('s3://bronze/ops/price_observation_events/**/*.parquet')
            WHERE event_type = 'deleted'
              AND source = 'detail'
            GROUP BY listing_id
        ),
        vin_map AS (
            -- Most recent VIN mapping per listing
            SELECT DISTINCT ON (listing_id)
                listing_id,
                vin
            FROM read_parquet('s3://bronze/ops/vin_to_listing_events/**/*.parquet')
            ORDER BY listing_id, event_at DESC
        ),
        last_make_model AS (
            -- Most recent make/model seen for each listing (from any upserted event)
            SELECT
                listing_id,
                arg_max(make,  event_at) AS make,
                arg_max(model, event_at) AS model
            FROM read_parquet('s3://bronze/ops/price_observation_events/**/*.parquet')
            WHERE event_type = 'upserted'
              AND make IS NOT NULL
              AND model IS NOT NULL
            GROUP BY listing_id
        )
        SELECT
            d.listing_id,
            d.artifact_id,
            v.vin,
            d.fetched_at,
            mm.make,
            mm.model
        FROM deleted d
        JOIN vin_map    v  ON v.listing_id  = d.listing_id
        JOIN last_make_model mm ON mm.listing_id = d.listing_id
    """).fetchall()

    logger.info("Found %d unlisted listings to backfill", len(rows))
    if not rows:
        logger.info("Nothing to backfill.")
        return

    # --- Step 2: Build silver rows --------------------------------------------
    written_at = datetime.now(timezone.utc)
    silver_rows = []

    for listing_id, artifact_id, vin, fetched_at, make, model in rows:
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)

        silver_rows.append({
            "artifact_id":          artifact_id,
            "listing_id":           str(listing_id),
            "vin":                  str(vin),
            "canonical_detail_url": f"https://www.cars.com/vehicledetail/{listing_id}/",
            "source":               "detail",
            "listing_state":        "unlisted",
            "fetched_at":           fetched_at,
            "written_at":           written_at,
            # Vehicle/dealer fields — make/model from last known upserted event
            "price":                None,
            "make":                 make,
            "model":                model,
            "trim":                 None,
            "year":                 None,
            "mileage":              None,
            "msrp":                 None,
            "stock_type":           None,
            "fuel_type":            None,
            "body_style":           None,
            "dealer_name":          None,
            "dealer_zip":           None,
            "customer_id":          None,
            "seller_id":            None,
            "dealer_street":        None,
            "dealer_city":          None,
            "dealer_state":         None,
            "dealer_phone":         None,
            "dealer_website":       None,
            "dealer_cars_com_url":  None,
            "dealer_rating":        None,
            "financing_type":       None,
            "seller_zip":           None,
            "seller_customer_id":   None,
            "page_number":          None,
            "position_on_page":     None,
            "trid":                 None,
            "isa_context":          None,
            "body":                 None,
            "condition":            None,
            "obs_year":             fetched_at.year,
            "obs_month":            fetched_at.month,
            "obs_day":              fetched_at.day,
        })

    # --- Step 3: Write to MinIO ----------------------------------------------
    logger.info("Writing %d rows to MinIO silver...", len(silver_rows))

    import s3fs
    fs = s3fs.S3FileSystem(
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        use_ssl=False,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}"},
    )

    arrow_table = pa.Table.from_pylist(silver_rows, schema=_SCHEMA)
    pq.write_to_dataset(
        arrow_table,
        root_path=f"s3://{BUCKET}/{MINIO_PREFIX}",
        partition_cols=["source", "obs_year", "obs_month", "obs_day"],
        filesystem=fs,
        compression="zstd",
        existing_data_behavior="overwrite_or_ignore",
        basename_template=f"backfill-unlisted-{uuid.uuid4()}-{{i}}.parquet",
    )

    logger.info("Backfill complete. %d rows written.", len(silver_rows))
    logger.info("Run 'dbt build' to rebuild mart_vehicle_snapshot with corrected unlisted state.")


if __name__ == "__main__":
    main()
