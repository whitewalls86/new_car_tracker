"""
SRP (Search Results Page) write path.

Flow per plan:
  1. Batch-lookup vin_to_listing for all listing_ids in the artifact
  2. For each listing:
     a. Resolve vin = listing.vin OR lookup result
     b. Upsert price_observations + write event
     c. If vin present: upsert vin_to_listing (recency guard) + write event
  3. Write all observations to MinIO silver (source='srp')
  4. Emit stubs for any listing with vin + price
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from processing.events import emit_price_updated, emit_vin_mapped
from processing.queries import (
    BATCH_LOOKUP_VIN_TO_LISTING,
    INSERT_PRICE_OBSERVATION_EVENT,
    INSERT_TRACKED_MODEL_EVENT,
    INSERT_VIN_TO_LISTING_EVENT,
    UPSERT_PRICE_OBSERVATION,
    UPSERT_TRACKED_MODEL,
    UPSERT_VIN_TO_LISTING,
)
from processing.writers.silver_writer import write_silver_observations
from shared.db import db_cursor

logger = logging.getLogger(__name__)


def write_srp_observations(
    listings: List[Dict[str, Any]],
    artifact_id: int,
    fetched_at: datetime,
    search_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Write parsed SRP listings to Postgres HOT tables and MinIO silver.

    Returns a summary dict with counts for the batch response.
    """
    if not listings:
        return {"upserted": 0, "vin_mapped": 0, "silver_written": 0}

    # --- Step 1: Batch VIN lookup ---
    listing_ids = [
        item["listing_id"] for item in listings
        if item.get("listing_id")
    ]

    vin_by_listing: Dict[str, str] = {}
    if listing_ids:
        with db_cursor(error_context="srp: batch_lookup_vin", dict_cursor=True) as cur:
            cur.execute(BATCH_LOOKUP_VIN_TO_LISTING, {"listing_ids": listing_ids})
            for row in cur.fetchall():
                vin_by_listing[str(row["listing_id"])] = row["vin"]

    # --- Step 2: Upsert price_observations + vin_to_listing ---
    upserted = 0
    vin_mapped = 0
    events_to_emit: List[Tuple[str, ...]] = []

    with db_cursor(error_context=f"srp: upserts artifact_id={artifact_id}") as cur:
        for listing in listings:
            listing_id = listing.get("listing_id")
            if not listing_id:
                continue

            # Resolve VIN: prefer parsed VIN, fall back to lookup
            vin = listing.get("vin") or vin_by_listing.get(listing_id)

            cur.execute(UPSERT_PRICE_OBSERVATION, {
                "listing_id": listing_id,
                "vin": vin,
                "price": listing.get("price"),
                "make": listing.get("make"),
                "model": listing.get("model"),
                "last_seen_at": fetched_at,
                "last_artifact_id": artifact_id,
            })
            upserted += 1

            # Event: price_observation upserted
            cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
                "listing_id": listing_id,
                "vin": vin,
                "price": listing.get("price"),
                "make": listing.get("make"),
                "model": listing.get("model"),
                "artifact_id": artifact_id,
                "event_type": "upserted",
                "source": "srp",
            })

            # Upsert vin_to_listing with recency guard
            if vin:
                cur.execute(UPSERT_VIN_TO_LISTING, {
                    "vin": vin,
                    "listing_id": listing_id,
                    "mapped_at": fetched_at,
                    "artifact_id": artifact_id,
                })
                if cur.rowcount > 0:
                    vin_mapped += 1
                    # Event: vin_to_listing mapped
                    cur.execute(INSERT_VIN_TO_LISTING_EVENT, {
                        "vin": vin,
                        "listing_id": listing_id,
                        "artifact_id": artifact_id,
                        "event_type": "mapped",
                        "previous_listing_id": None,
                    })
                    events_to_emit.append(("vin_mapped", listing_id, vin))

            if vin and listing.get("price"):
                events_to_emit.append((
                    "price_updated", vin, str(listing["price"]), listing_id,
                ))

    # --- Step 2b: Upsert tracked_models ---
    if search_key:
        seen_models: set[Tuple[str, str]] = set()
        for listing in listings:
            make = listing.get("make")
            model = listing.get("model")
            if make and model:
                seen_models.add((make.lower(), model.lower()))

        if seen_models:
            with db_cursor(
                error_context="srp: upsert_tracked_models",
            ) as cur:
                for make, model in seen_models:
                    cur.execute(UPSERT_TRACKED_MODEL, {
                        "search_key": search_key,
                        "make": make,
                        "model": model,
                    })
                    if cur.rowcount > 0:
                        cur.execute(INSERT_TRACKED_MODEL_EVENT, {
                            "search_key": search_key,
                            "make": make,
                            "model": model,
                            "event_type": "added",
                        })

    # --- Step 3: Silver write (non-fatal) ---
    silver_rows = [
        {
            "artifact_id": artifact_id,
            "listing_id": item.get("listing_id"),
            "vin": item.get("vin") or vin_by_listing.get(
                item.get("listing_id", ""),
            ),
            "canonical_detail_url": item.get("canonical_detail_url"),
            "price": item.get("price"),
            "make": item.get("make"),
            "model": item.get("model"),
            "trim": item.get("trim"),
            "year": item.get("year"),
            "mileage": item.get("mileage"),
            "msrp": item.get("msrp"),
            "stock_type": item.get("stockType"),
            "fuel_type": item.get("fuelType"),
            "body_style": item.get("bodyStyle"),
            "financing_type": item.get("financingType"),
            "seller_zip": item.get("seller_zip"),
            "seller_customer_id": item.get("seller_customerId"),
            "page_number": item.get("page_number"),
            "position_on_page": item.get("position_on_page"),
            "trid": item.get("trid"),
            "isa_context": item.get("isaContext"),
            "listing_state": "active",
            "source": "srp",
            "fetched_at": fetched_at,
        }
        for item in listings
        if item.get("listing_id")
    ]
    silver_written = write_silver_observations(silver_rows)

    # --- Step 4: Emit stubs (after commit) ---
    for event in events_to_emit:
        if event[0] == "price_updated":
            emit_price_updated(vin=event[1], price=int(event[2]),
                               listing_id=event[3], source="srp")
        elif event[0] == "vin_mapped":
            emit_vin_mapped(listing_id=event[1], vin=event[2])

    return {"upserted": upserted, "vin_mapped": vin_mapped, "silver_written": silver_written}
