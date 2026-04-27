#!/usr/bin/env python3
"""
Migrate legacy Postgres observation tables to MinIO Parquet.

Reads from:
    raw_artifacts, artifact_processing,
    srp_observations, detail_observations, detail_carousel_hints,
    analytics.int_listing_to_vin (dbt materialized table, for carousel VIN enrichment)

Writes to (matching flush DAG schemas exactly):
    silver/observations          — all observations unified by source column
    ops/artifacts_queue_events   — artifact processing status events
    ops/price_observation_events — one price event per observation row
    ops/vin_to_listing_events    — first VIN mapping per (listing_id, vin)

Usage:
    python scripts/migrate_legacy_to_minio.py [--dry-run]

Environment variables:
    PGHOST, PGPORT, PGDATABASE, PGUSER, POSTGRES_PASSWORD
    MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_BUCKET
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CUTOFF = datetime(2026, 4, 21, tzinfo=timezone.utc)
BUCKET = os.environ.get("MINIO_BUCKET", "bronze")
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "cartracker")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "")

# Negative IDs mark migration-origin rows; never collide with bigserial production IDs.
_LEGACY_EVENT_ID_OFFSET = -1_000_000_000_000

_VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# PyArrow schemas — must match flush_silver_observations.py / flush_staging_events.py exactly
# ---------------------------------------------------------------------------

_SILVER_SCHEMA = pa.schema([
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

_ARTIFACT_EVENTS_SCHEMA = pa.schema([
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

_PRICE_EVENTS_SCHEMA = pa.schema([
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

_VIN_EVENTS_SCHEMA = pa.schema([
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
# Helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "cartracker"),
        user=os.environ.get("PGUSER", "cartracker"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        endpoint_url=ENDPOINT,
        key=MINIO_USER,
        secret=MINIO_PASSWORD,
        use_ssl=False,
    )


def normalize_vin(vin: Optional[str]) -> Optional[str]:
    if not vin:
        return None
    v = vin.strip().upper()
    return v if _VIN_RE.match(v) else None


def ensure_utc(v) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime) and v.tzinfo is None:
        return v.replace(tzinfo=timezone.utc)
    return v


def to_int16(v) -> Optional[int]:
    """Clamp to int16 range; return None for empty/out-of-range values."""
    if v is None or v == "":
        return None
    try:
        i = int(v)
        return i if -32768 <= i <= 32767 else None
    except (TypeError, ValueError):
        return None


def migration_file_exists(fs: s3fs.S3FileSystem, pattern: str) -> bool:
    """Return True if any file matching the glob pattern exists in MinIO."""
    return len(fs.glob(pattern)) > 0


def write_parquet(
    rows: List[dict],
    schema: pa.Schema,
    prefix: str,
    partition_cols: List[str],
    basename_template: str,
    fs: s3fs.S3FileSystem,
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    if dry_run:
        logger.info("  [DRY-RUN] would write %d rows → %s/%s", len(rows), BUCKET, prefix)
        return len(rows)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_to_dataset(
        table,
        root_path=f"s3://{BUCKET}/{prefix}",
        partition_cols=partition_cols,
        filesystem=fs,
        compression="zstd",
        existing_data_behavior="overwrite_or_ignore",
        basename_template=basename_template,
    )
    return len(rows)


def get_months(conn, table: str, ts_col: str) -> List[Tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT date_part('year', {ts_col})::int,"  # noqa: S608
            f"       date_part('month', {ts_col})::int"
            f"  FROM public.{table}"
            f" WHERE {ts_col} < %s"
            f" ORDER BY 1, 2",
            (CUTOFF,),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def load_vin_map(conn) -> Dict[str, Optional[str]]:
    """
    Load listing_id → normalized VIN from analytics.int_listing_to_vin.

    Used to enrich carousel rows, which have no VIN in the raw table.
    The dbt table uses vin17 (already normalized), keyed by text listing_id.
    """
    logger.info("Loading VIN map from analytics.int_listing_to_vin...")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT listing_id, vin FROM analytics.int_listing_to_vin WHERE vin IS NOT NULL"
        )
        rows = cur.fetchall()
    vin_map = {str(r[0]): normalize_vin(r[1]) for r in rows}
    # Drop entries where normalization returned None
    vin_map = {k: v for k, v in vin_map.items() if v}
    logger.info("  Loaded %d VIN mappings", len(vin_map))
    return vin_map


# ---------------------------------------------------------------------------
# Step 1: Build artifact ID remap
# ---------------------------------------------------------------------------

def build_remap(conn) -> Tuple[Dict[int, int], int]:
    """
    Assign each legacy raw_artifacts ID a new non-overlapping ID above the
    current ops.artifacts_queue high-water mark, preserving chronological order.
    Returns (remap dict, max_new_id).
    """
    logger.info("Building artifact ID remap...")
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(artifact_id), 0) FROM ops.artifacts_queue")
        base = cur.fetchone()[0]
        logger.info("  ops.artifacts_queue max artifact_id = %d", base)

        cur.execute(
            "SELECT artifact_id FROM public.raw_artifacts"
            " WHERE fetched_at < %s ORDER BY artifact_id",
            (CUTOFF,),
        )
        old_ids = [row[0] for row in cur.fetchall()]

    remap = {old: base + i for i, old in enumerate(old_ids, start=1)}
    max_new = base + len(old_ids)
    logger.info(
        "  Remapping %d legacy artifact IDs → [%d, %d]",
        len(old_ids), base + 1, max_new,
    )
    return remap, max_new


# ---------------------------------------------------------------------------
# Step 2: silver/observations + ops/price_observation_events
# ---------------------------------------------------------------------------

def _silver_skip_check(fs: s3fs.S3FileSystem, source: str, year: int, month: int) -> bool:
    pattern = (
        f"{BUCKET}/silver/observations/source={source}"
        f"/obs_year={year}/obs_month={month}"
        f"/obs_day=*/legacy-{source}-{year}-{month:02d}-*.parquet"
    )
    return migration_file_exists(fs, pattern)


def _price_skip_check(fs: s3fs.S3FileSystem, source: str, year: int, month: int) -> bool:
    pattern = (
        f"{BUCKET}/ops/price_observation_events"
        f"/year={year}/month={month}"
        f"/legacy-{source}-{year}-{month:02d}-*.parquet"
    )
    return migration_file_exists(fs, pattern)


def _migrate_srp_month(
    conn,
    fs: s3fs.S3FileSystem,
    remap: Dict[int, int],
    year: int,
    month: int,
    written_at: datetime,
    dry_run: bool,
    event_counter: List[int],
) -> Tuple[int, int]:
    source = "srp"
    silver_done = _silver_skip_check(fs, source, year, month)
    price_done = _price_skip_check(fs, source, year, month)

    if silver_done and price_done:
        return 0, 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT artifact_id, listing_id, vin, fetched_at,
                   price, msrp, mileage, year AS vehicle_year,
                   make, model, trim, stock_type, fuel_type, body_style,
                   financing_type, seller_zip, seller_customer_id,
                   page_number, position_on_page, trid, isa_context,
                   canonical_detail_url
            FROM public.srp_observations
            WHERE date_part('year', fetched_at)::int = %s
              AND date_part('month', fetched_at)::int = %s
              AND fetched_at < %s
            """,
            (year, month, CUTOFF),
        )
        rows_raw = cur.fetchall()

    cols = [
        "artifact_id", "listing_id", "vin", "fetched_at",
        "price", "msrp", "mileage", "vehicle_year",
        "make", "model", "trim", "stock_type", "fuel_type", "body_style",
        "financing_type", "seller_zip", "seller_customer_id",
        "page_number", "position_on_page", "trid", "isa_context",
        "canonical_detail_url",
    ]
    silver_rows: List[dict] = []
    price_rows: List[dict] = []

    for raw in rows_raw:
        d = dict(zip(cols, raw))
        aid = remap.get(d["artifact_id"], d["artifact_id"])
        fa = ensure_utc(d["fetched_at"])
        vin = normalize_vin(d["vin"])
        obs_y = fa.year if fa else year
        obs_m = fa.month if fa else month
        obs_d = fa.day if fa else 1

        if not silver_done:
            silver_rows.append({
                "artifact_id":          aid,
                "listing_id":           str(d["listing_id"]) if d["listing_id"] else None,
                "vin":                  vin,
                "canonical_detail_url": d["canonical_detail_url"],
                "source":               "srp",
                "listing_state":        "active",
                "fetched_at":           fa,
                "written_at":           written_at,
                "price":                d["price"],
                "make":                 d["make"],
                "model":                d["model"],
                "trim":                 d["trim"],
                "year":                 to_int16(d["vehicle_year"]),
                "mileage":              d["mileage"],
                "msrp":                 d["msrp"],
                "stock_type":           d["stock_type"],
                "fuel_type":            d["fuel_type"],
                "body_style":           d["body_style"],
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
                "financing_type":       d["financing_type"],
                "seller_zip":           d["seller_zip"],
                "seller_customer_id":   d["seller_customer_id"],
                "page_number":          to_int16(d["page_number"]),
                "position_on_page":     to_int16(d["position_on_page"]),
                "trid":                 d["trid"],
                "isa_context":          d["isa_context"],
                "body":                 None,
                "condition":            None,
                "obs_year":             obs_y,
                "obs_month":            obs_m,
                "obs_day":              obs_d,
            })

        if not price_done:
            event_counter[0] += 1
            price_rows.append({
                "event_id":   _LEGACY_EVENT_ID_OFFSET - event_counter[0],
                "listing_id": str(d["listing_id"]) if d["listing_id"] else None,
                "vin":        vin,
                "price":      d["price"],
                "make":       d["make"],
                "model":      d["model"],
                "artifact_id": aid,
                "event_type": "upserted",
                "source":     "srp",
                "event_at":   fa,
                "year":       obs_y,
                "month":      obs_m,
            })

    s = write_parquet(
        silver_rows, _SILVER_SCHEMA, "silver/observations",
        ["source", "obs_year", "obs_month", "obs_day"],
        f"legacy-srp-{year}-{month:02d}-{{i}}.parquet", fs, dry_run,
    ) if not silver_done else 0

    p = write_parquet(
        price_rows, _PRICE_EVENTS_SCHEMA, "ops/price_observation_events",
        ["year", "month"],
        f"legacy-srp-{year}-{month:02d}-{{i}}.parquet", fs, dry_run,
    ) if not price_done else 0

    return s, p


def _migrate_detail_month(
    conn,
    fs: s3fs.S3FileSystem,
    remap: Dict[int, int],
    year: int,
    month: int,
    written_at: datetime,
    dry_run: bool,
    event_counter: List[int],
) -> Tuple[int, int]:
    source = "detail"
    silver_done = _silver_skip_check(fs, source, year, month)
    price_done = _price_skip_check(fs, source, year, month)

    if silver_done and price_done:
        return 0, 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.artifact_id, d.listing_id, d.vin, d.fetched_at,
                   d.price, d.msrp, d.mileage, d.year AS vehicle_year,
                   d.make, d.model, d.trim, d.stock_type, d.fuel_type, d.body_style,
                   d.dealer_name, d.dealer_zip, d.customer_id, d.listing_state,
                   r.url AS canonical_detail_url
            FROM public.detail_observations d
            LEFT JOIN public.raw_artifacts r USING (artifact_id)
            WHERE date_part('year', d.fetched_at)::int = %s
              AND date_part('month', d.fetched_at)::int = %s
              AND d.fetched_at < %s
            """,
            (year, month, CUTOFF),
        )
        rows_raw = cur.fetchall()

    cols = [
        "artifact_id", "listing_id", "vin", "fetched_at",
        "price", "msrp", "mileage", "vehicle_year",
        "make", "model", "trim", "stock_type", "fuel_type", "body_style",
        "dealer_name", "dealer_zip", "customer_id", "listing_state",
        "canonical_detail_url",
    ]
    silver_rows: List[dict] = []
    price_rows: List[dict] = []

    for raw in rows_raw:
        d = dict(zip(cols, raw))
        aid = remap.get(d["artifact_id"], d["artifact_id"])
        fa = ensure_utc(d["fetched_at"])
        vin = normalize_vin(d["vin"])
        listing_state = d["listing_state"] or "active"
        obs_y = fa.year if fa else year
        obs_m = fa.month if fa else month
        obs_d = fa.day if fa else 1

        if not silver_done:
            silver_rows.append({
                "artifact_id":          aid,
                "listing_id":           str(d["listing_id"]) if d["listing_id"] else None,
                "vin":                  vin,
                "canonical_detail_url": d["canonical_detail_url"],
                "source":               "detail",
                "listing_state":        listing_state,
                "fetched_at":           fa,
                "written_at":           written_at,
                "price":                d["price"],
                "make":                 d["make"],
                "model":                d["model"],
                "trim":                 d["trim"],
                "year":                 to_int16(d["vehicle_year"]),
                "mileage":              d["mileage"],
                "msrp":                 d["msrp"],
                "stock_type":           d["stock_type"],
                "fuel_type":            d["fuel_type"],
                "body_style":           d["body_style"],
                "dealer_name":          d["dealer_name"],
                "dealer_zip":           d["dealer_zip"],
                "customer_id":          d["customer_id"],
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
                "obs_year":             obs_y,
                "obs_month":            obs_m,
                "obs_day":              obs_d,
            })

        if not price_done:
            event_counter[0] += 1
            price_rows.append({
                "event_id":   _LEGACY_EVENT_ID_OFFSET - event_counter[0],
                "listing_id": str(d["listing_id"]) if d["listing_id"] else None,
                "vin":        vin,
                "price":      d["price"],
                "make":       d["make"],
                "model":      d["model"],
                "artifact_id": aid,
                "event_type": "deleted" if listing_state == "unlisted" else "upserted",
                "source":     "detail",
                "event_at":   fa,
                "year":       obs_y,
                "month":      obs_m,
            })

    s = write_parquet(
        silver_rows, _SILVER_SCHEMA, "silver/observations",
        ["source", "obs_year", "obs_month", "obs_day"],
        f"legacy-detail-{year}-{month:02d}-{{i}}.parquet", fs, dry_run,
    ) if not silver_done else 0

    p = write_parquet(
        price_rows, _PRICE_EVENTS_SCHEMA, "ops/price_observation_events",
        ["year", "month"],
        f"legacy-detail-{year}-{month:02d}-{{i}}.parquet", fs, dry_run,
    ) if not price_done else 0

    return s, p


def _migrate_carousel_month(
    conn,
    fs: s3fs.S3FileSystem,
    remap: Dict[int, int],
    year: int,
    month: int,
    written_at: datetime,
    dry_run: bool,
    event_counter: List[int],
    vin_map: Dict[str, Optional[str]],
) -> Tuple[int, int]:
    source = "carousel"
    silver_done = _silver_skip_check(fs, source, year, month)
    price_done = _price_skip_check(fs, source, year, month)

    if silver_done and price_done:
        return 0, 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT artifact_id, listing_id, fetched_at,
                   price, mileage, year AS vehicle_year, body, condition
            FROM public.detail_carousel_hints
            WHERE date_part('year', fetched_at)::int = %s
              AND date_part('month', fetched_at)::int = %s
              AND fetched_at < %s
            """,
            (year, month, CUTOFF),
        )
        rows_raw = cur.fetchall()

    cols = ["artifact_id", "listing_id", "fetched_at",
            "price", "mileage", "vehicle_year", "body", "condition"]
    silver_rows: List[dict] = []
    price_rows: List[dict] = []

    for raw in rows_raw:
        d = dict(zip(cols, raw))
        aid = remap.get(d["artifact_id"], d["artifact_id"])
        fa = ensure_utc(d["fetched_at"])
        listing_id = str(d["listing_id"]) if d["listing_id"] else None
        canonical = f"https://www.cars.com/vehicledetail/{listing_id}/" if listing_id else None
        obs_y = fa.year if fa else year
        obs_m = fa.month if fa else month
        obs_d = fa.day if fa else 1

        # Enrich VIN from int_listing_to_vin where available
        vin = vin_map.get(listing_id) if listing_id else None

        # Parse make/model from body: "Condition Year Make Model..."
        # e.g. "Certified 2021 Ford Escape SE" → make="Ford", model="Escape"
        body_parts = (d.get("body") or "").split()
        make = body_parts[2] if len(body_parts) > 2 else None
        model = body_parts[3] if len(body_parts) > 3 else None

        if not silver_done:
            silver_rows.append({
                "artifact_id":          aid,
                "listing_id":           listing_id,
                "vin":                  vin,
                "canonical_detail_url": canonical,
                "source":               "carousel",
                "listing_state":        "active",
                "fetched_at":           fa,
                "written_at":           written_at,
                "price":                d["price"],
                "make":                 make,
                "model":                model,
                "trim":                 None,
                "year":                 to_int16(d["vehicle_year"]),
                "mileage":              d["mileage"],
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
                "body":                 d["body"],
                "condition":            d["condition"],
                "obs_year":             obs_y,
                "obs_month":            obs_m,
                "obs_day":              obs_d,
            })

        if not price_done:
            event_counter[0] += 1
            price_rows.append({
                "event_id":   _LEGACY_EVENT_ID_OFFSET - event_counter[0],
                "listing_id": listing_id,
                "vin":        vin,
                "price":      d["price"],
                "make":       make,
                "model":      model,
                "artifact_id": aid,
                "event_type": "upserted",
                "source":     "carousel",
                "event_at":   fa,
                "year":       obs_y,
                "month":      obs_m,
            })

    s = write_parquet(
        silver_rows, _SILVER_SCHEMA, "silver/observations",
        ["source", "obs_year", "obs_month", "obs_day"],
        f"legacy-carousel-{year}-{month:02d}-{{i}}.parquet", fs, dry_run,
    ) if not silver_done else 0

    p = write_parquet(
        price_rows, _PRICE_EVENTS_SCHEMA, "ops/price_observation_events",
        ["year", "month"],
        f"legacy-carousel-{year}-{month:02d}-{{i}}.parquet", fs, dry_run,
    ) if not price_done else 0

    return s, p


# ---------------------------------------------------------------------------
# Step 3: ops/artifacts_queue_events
# ---------------------------------------------------------------------------

def migrate_artifact_events(
    conn, fs: s3fs.S3FileSystem, remap: Dict[int, int], dry_run: bool,
) -> int:
    logger.info("Migrating artifact_processing → ops/artifacts_queue_events...")
    total = 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT date_part('year', fetched_at)::int,
                            date_part('month', fetched_at)::int
            FROM public.raw_artifacts
            WHERE fetched_at < %s
            ORDER BY 1, 2
            """,
            (CUTOFF,),
        )
        months = [(r[0], r[1]) for r in cur.fetchall()]

    for year, month in months:
        pattern = (
            f"{BUCKET}/ops/artifacts_queue_events"
            f"/year={year}/month={month}"
            f"/legacy-artifacts-{year}-{month:02d}-*.parquet"
        )
        if migration_file_exists(fs, pattern):
            logger.info("  artifact_events %d-%02d already done, skipping", year, month)
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ap.artifact_id, ap.status, ap.processed_at,
                       r.artifact_type, r.fetched_at, r.listing_id, r.run_id, r.minio_path
                FROM public.artifact_processing ap
                JOIN public.raw_artifacts r USING (artifact_id)
                WHERE date_part('year', r.fetched_at)::int = %s
                  AND date_part('month', r.fetched_at)::int = %s
                  AND r.fetched_at < %s
                """,
                (year, month, CUTOFF),
            )
            rows_raw = cur.fetchall()

        cols = ["artifact_id", "status", "processed_at",
                "artifact_type", "fetched_at", "listing_id", "run_id", "minio_path"]
        rows = []
        for i, raw in enumerate(rows_raw):
            d = dict(zip(cols, raw))
            aid = remap.get(d["artifact_id"], d["artifact_id"])
            event_at = ensure_utc(d["processed_at"])
            fetched_at = ensure_utc(d["fetched_at"])
            rows.append({
                "event_id":      _LEGACY_EVENT_ID_OFFSET - (total + i + 1),
                "artifact_id":   aid,
                "status":        d["status"],
                "event_at":      event_at,
                "minio_path":    d["minio_path"],  # NULL for pre-MinIO artifacts
                "artifact_type": d["artifact_type"],
                "fetched_at":    fetched_at,
                "listing_id":    str(d["listing_id"]) if d["listing_id"] else None,
                "run_id":        str(d["run_id"]) if d["run_id"] else None,
                "year":          event_at.year if event_at else year,
                "month":         event_at.month if event_at else month,
            })

        n = write_parquet(
            rows, _ARTIFACT_EVENTS_SCHEMA, "ops/artifacts_queue_events",
            ["year", "month"],
            f"legacy-artifacts-{year}-{month:02d}-{{i}}.parquet", fs, dry_run,
        )
        logger.info("  artifact_events %d-%02d: %d rows", year, month, n)
        total += n

    return total


# ---------------------------------------------------------------------------
# Step 4: ops/vin_to_listing_events
# ---------------------------------------------------------------------------

def migrate_vin_events(
    conn, fs: s3fs.S3FileSystem, remap: Dict[int, int], dry_run: bool,
) -> int:
    logger.info("Migrating vin_events → ops/vin_to_listing_events...")

    existing = fs.glob(f"{BUCKET}/ops/vin_to_listing_events/year=*/month=*/legacy-vin-*.parquet")
    if existing:
        logger.info("  vin_events already migrated (%d files found), skipping", len(existing))
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT listing_id, vin, artifact_id, fetched_at
            FROM (
                SELECT listing_id, vin, artifact_id, fetched_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY listing_id, vin
                           ORDER BY fetched_at ASC, artifact_id ASC
                       ) AS rn
                FROM (
                    SELECT listing_id, vin, artifact_id, fetched_at
                    FROM public.srp_observations
                    WHERE vin IS NOT NULL AND fetched_at < %s
                    UNION ALL
                    SELECT listing_id, vin, artifact_id, fetched_at
                    FROM public.detail_observations
                    WHERE vin IS NOT NULL AND fetched_at < %s
                ) combined
            ) ranked
            WHERE rn = 1
            ORDER BY fetched_at ASC
            """,
            (CUTOFF, CUTOFF),
        )
        rows_raw = cur.fetchall()

    rows = []
    for i, (listing_id, vin, artifact_id, fetched_at) in enumerate(rows_raw):
        norm_vin = normalize_vin(vin)
        if not norm_vin:
            continue
        aid = remap.get(artifact_id, artifact_id)
        fa = ensure_utc(fetched_at)
        rows.append({
            "event_id":            _LEGACY_EVENT_ID_OFFSET - (i + 1),
            "vin":                 norm_vin,
            "listing_id":          str(listing_id) if listing_id else None,
            "artifact_id":         aid,
            "event_type":          "mapped",
            "previous_listing_id": None,
            "event_at":            fa,
            "year":                fa.year if fa else 2024,
            "month":               fa.month if fa else 1,
        })

    if not rows:
        logger.info("  No valid vin_events to migrate")
        return 0

    n = write_parquet(
        rows, _VIN_EVENTS_SCHEMA, "ops/vin_to_listing_events",
        ["year", "month"],
        "legacy-vin-{i}.parquet", fs, dry_run,
    )
    logger.info("  Wrote %d vin_events", n)
    return n


# ---------------------------------------------------------------------------
# Step 5: Advance sequence
# ---------------------------------------------------------------------------

def advance_sequence(conn, max_new_id: int, dry_run: bool) -> None:
    logger.info("Advancing ops.artifacts_queue_artifact_id_seq to %d...", max_new_id)
    if dry_run:
        logger.info("  [DRY-RUN] skipping setval")
        return
    with conn.cursor() as cur:
        cur.execute(
            "SELECT setval('ops.artifacts_queue_artifact_id_seq', %s)",
            (max_new_id,),
        )
    conn.commit()
    logger.info("  Sequence advanced.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Migrate legacy Postgres observation tables to MinIO Parquet"
    )
    parser.add_argument("--dry-run", action="store_true", help="Read and map without writing")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("=== DRY RUN — no writes will occur ===")

    t0 = time.time()
    conn = get_conn()
    fs = get_fs()

    remap, max_new_id = build_remap(conn)
    vin_map = load_vin_map(conn)
    written_at = datetime.now(timezone.utc)
    event_counter = [0]  # mutable int shared across month functions

    # Silver observations + price events
    logger.info("--- silver/observations ---")
    total_silver = total_price = 0

    srp_months = get_months(conn, "srp_observations", "fetched_at")
    logger.info("srp: %d months", len(srp_months))
    for year, month in srp_months:
        s, p = _migrate_srp_month(
            conn, fs, remap, year, month, written_at, args.dry_run, event_counter
        )
        total_silver += s
        total_price += p
        if s or p:
            logger.info("  srp %d-%02d: %d silver, %d price_events", year, month, s, p)

    detail_months = get_months(conn, "detail_observations", "fetched_at")
    logger.info("detail: %d months", len(detail_months))
    for year, month in detail_months:
        s, p = _migrate_detail_month(
            conn, fs, remap, year, month, written_at, args.dry_run, event_counter
        )
        total_silver += s
        total_price += p
        if s or p:
            logger.info("  detail %d-%02d: %d silver, %d price_events", year, month, s, p)

    carousel_months = get_months(conn, "detail_carousel_hints", "fetched_at")
    logger.info("carousel: %d months", len(carousel_months))
    for year, month in carousel_months:
        s, p = _migrate_carousel_month(
            conn, fs, remap, year, month, written_at, args.dry_run, event_counter,
            vin_map=vin_map,
        )
        total_silver += s
        total_price += p
        if s or p:
            logger.info("  carousel %d-%02d: %d silver, %d price_events", year, month, s, p)

    logger.info("silver/observations total: %d rows", total_silver)
    logger.info("ops/price_observation_events total: %d rows", total_price)

    # Artifact events
    logger.info("--- ops/artifacts_queue_events ---")
    total_artifact = migrate_artifact_events(conn, fs, remap, args.dry_run)
    logger.info("ops/artifacts_queue_events total: %d rows", total_artifact)

    # VIN events
    logger.info("--- ops/vin_to_listing_events ---")
    total_vin = migrate_vin_events(conn, fs, remap, args.dry_run)
    logger.info("ops/vin_to_listing_events total: %d rows", total_vin)

    # Advance sequence
    advance_sequence(conn, max_new_id, args.dry_run)

    conn.close()
    elapsed = time.time() - t0
    logger.info(
        "Done in %.1fs — silver=%d price_events=%d artifact_events=%d vin_events=%d",
        elapsed, total_silver, total_price, total_artifact, total_vin,
    )


if __name__ == "__main__":
    main()
