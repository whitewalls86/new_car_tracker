"""Seed MinIO with the empty Parquet schemas dbt model compilation needs.

Lifted verbatim out of a 115-line inline heredoc in `ci.yml` by Plan 162
Stage E, which gave two jobs their own `dbt build` and so would otherwise have
duplicated all 115 lines. The schemas here are the *shape* of the external
sources, not data: `dbt build --target duckdb` reads them so every model
compiles against the real column set even before the Plan 120 lake-snapshot
fixture is written alongside them.

CI infrastructure, deliberately under `.github/` rather than `scripts/`:
nothing ships it, no service imports it, and it is not part of the coverage
denominator `scripts/` is.
"""

import io
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

BUCKET = os.environ.get("MINIO_BUCKET", "bronze")

SILVER_OBSERVATIONS = pa.schema([
    pa.field("artifact_id", pa.int64()),
    pa.field("listing_id", pa.string()),
    pa.field("vin", pa.string()),
    pa.field("canonical_detail_url", pa.string()),
    pa.field("source", pa.string()),
    pa.field("listing_state", pa.string()),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("written_at", pa.timestamp("us", tz="UTC")),
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
    pa.field("financing_type", pa.string()),
    pa.field("seller_zip", pa.string()),
    pa.field("seller_customer_id", pa.string()),
    pa.field("page_number", pa.int16()),
    pa.field("position_on_page", pa.int16()),
    pa.field("trid", pa.string()),
    pa.field("isa_context", pa.string()),
    pa.field("body", pa.string()),
    pa.field("condition", pa.string()),
    pa.field("obs_year", pa.int32()),
    pa.field("obs_month", pa.int32()),
    pa.field("obs_day", pa.int32()),
])

PRICE_OBSERVATION_EVENTS = pa.schema([
    pa.field("event_id", pa.int64()),
    pa.field("listing_id", pa.string()),
    pa.field("vin", pa.string()),
    pa.field("price", pa.int32()),
    pa.field("make", pa.string()),
    pa.field("model", pa.string()),
    pa.field("artifact_id", pa.int64()),
    pa.field("event_type", pa.string()),
    pa.field("source", pa.string()),
    pa.field("event_at", pa.timestamp("us", tz="UTC")),
    pa.field("year", pa.int32()),
    pa.field("month", pa.int32()),
])

BLOCKED_COOLDOWN_EVENTS = pa.schema([
    pa.field("event_id", pa.int64()),
    pa.field("listing_id", pa.string()),
    pa.field("event_type", pa.string()),
    pa.field("num_of_attempts", pa.int32()),
    pa.field("event_at", pa.timestamp("us", tz="UTC")),
    pa.field("year", pa.int32()),
    pa.field("month", pa.int32()),
])

# event_id=0: outside the 1-8 range used by the Plan 120 lake-snapshot fixture's
# price_observation_events rows (scripts/seed_lake_snapshot_fixture.py), so
# unique_stg_price_events_event_id doesn't collide across the two seeds.
PRICE_EVENT_SEED_ROW = {
    "event_id": pa.array([0], type=pa.int64()),
    "listing_id": pa.array(["CI_SEED_L1"], type=pa.string()),
    "vin": pa.array(["1HGBH41JXMN109186"], type=pa.string()),
    "price": pa.array([25000], type=pa.int32()),
    "make": pa.array(["Honda"], type=pa.string()),
    "model": pa.array(["Civic"], type=pa.string()),
    "artifact_id": pa.array([1], type=pa.int64()),
    "event_type": pa.array(["upserted"], type=pa.string()),
    "source": pa.array(["srp"], type=pa.string()),
    "event_at": pa.array(
        [datetime(2026, 1, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")
    ),
    "year": pa.array([2026], type=pa.int32()),
    "month": pa.array([1], type=pa.int32()),
}


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "cartracker"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "cartracker123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _upload(s3, key: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"Uploaded {key}")


def _empty(schema: pa.Schema) -> pa.Table:
    return pa.table(
        {field.name: pa.array([], type=field.type) for field in schema}, schema=schema
    )


def main() -> None:
    s3 = _client()
    _upload(
        s3,
        "silver_normalized/observations/source=srp/obs_year=2026/obs_month=1/seed.parquet",
        _empty(SILVER_OBSERVATIONS),
    )
    _upload(
        s3,
        "ops_normalized/price_observation_events/year=2026/month=1/seed.parquet",
        pa.table(PRICE_EVENT_SEED_ROW, schema=PRICE_OBSERVATION_EVENTS),
    )
    _upload(
        s3,
        "ops_normalized/blocked_cooldown_events/year=2026/month=1/seed.parquet",
        _empty(BLOCKED_COOLDOWN_EVENTS),
    )


if __name__ == "__main__":
    main()
