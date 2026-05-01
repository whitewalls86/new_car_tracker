"""
Detail page write path — handles active and unlisted cases.

Active path:
  1. Batch VIN lookup for primary + carousel listing_ids
  2. VIN collision check (relisting detection)
  3. Upsert price_observations for primary
  4. Upsert vin_to_listing
  5. Carousel: filter by search_configs, upsert matching to price_observations
  6. Silver write (primary + all carousel, regardless of filter)
  7. Clear blocked_cooldown
  8. Release detail_scrape_claims + event

Unlisted path:
  1. DELETE from price_observations
  2. Silver write (listing_state='unlisted', price=NULL)
  3. Clear blocked_cooldown
  4. Release detail_scrape_claims + event

403 path: handled at scrape time in scraper/processors/scrape_detail.py.
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from processing.events import emit_listing_removed, emit_price_updated, emit_vin_mapped
from processing.queries import (
    BATCH_LOOKUP_VIN_TO_LISTING,
    CLEAR_BLOCKED_COOLDOWN,
    DELETE_PRICE_OBSERVATION,
    DELETE_PRICE_OBSERVATION_BY_VIN,
    GET_TRACKED_MODELS,
    INSERT_DETAIL_CLAIM_EVENT,
    INSERT_PRICE_OBSERVATION_EVENT,
    INSERT_VIN_TO_LISTING_EVENT,
    LOOKUP_VIN_COLLISION,
    RELEASE_DETAIL_CLAIMS,
    UPSERT_PRICE_OBSERVATION,
    UPSERT_VIN_TO_LISTING,
)
from processing.writers.silver_writer import write_silver_observations_postgres
from shared.db import db_cursor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Carousel search_config filtering
# ---------------------------------------------------------------------------

_TRACKED_MODELS_CACHE: Optional[Tuple[Set[Tuple[str, str]], float]] = None
_CACHE_TTL_SECONDS = 300


def _get_tracked_models() -> Set[Tuple[str, str]]:
    """
    Load (make, model) pairs from ops.tracked_models joined to enabled
    search_configs. Cached for 5 minutes.
    """
    global _TRACKED_MODELS_CACHE
    import time

    now = time.time()
    if _TRACKED_MODELS_CACHE and (now - _TRACKED_MODELS_CACHE[1]) < _CACHE_TTL_SECONDS:
        return _TRACKED_MODELS_CACHE[0]

    allowed: Set[Tuple[str, str]] = set()
    with db_cursor(
        error_context="detail: get_tracked_models", dict_cursor=True,
    ) as cur:
        cur.execute(GET_TRACKED_MODELS)
        for row in cur.fetchall():
            allowed.add((row["make"], row["model"]))

    _TRACKED_MODELS_CACHE = (allowed, now)
    return allowed


def _carousel_matches_search_config(hint: Dict[str, Any]) -> bool:
    """Check if a carousel hint's make/model matches a tracked model."""
    body = hint.get("body") or ""
    # Parse make + model from body:
    #   "New 2026 Honda CR-V Hybrid Sport Touring AWD"
    #   → make="honda", rest="cr-v hybrid sport touring awd"
    m = re.match(
        r"^(?:New|Used|Certified|CPO)\s+\d{4}\s+(\S+)\s+(.+)",
        body, re.IGNORECASE,
    )
    if not m:
        return False
    make = m.group(1).lower()
    rest = m.group(2).lower()

    tracked = _get_tracked_models()
    for t_make, t_model in tracked:
        if t_make != make:
            continue
        # Check if the body's model text starts with the tracked model.
        # e.g. rest="cr-v hybrid sport touring awd" starts with
        # t_model="cr-v hybrid" → match.
        if rest == t_model or rest.startswith(t_model + " "):
            return True
    return False


# ---------------------------------------------------------------------------
# Main write paths
# ---------------------------------------------------------------------------

def write_detail_active(
    primary: Dict[str, Any],
    carousel: List[Dict[str, Any]],
    artifact_id: int,
    fetched_at: datetime,
    listing_id: str,
    run_id: Optional[str],
) -> Dict[str, Any]:
    """
    Detail write path for active listings.
    Returns a summary dict.
    """
    vin = primary.get("vin")
    events_to_emit: List[Tuple[str, ...]] = []

    # --- Step 1: Batch VIN lookup ---
    all_listing_ids = [listing_id]
    for hint in carousel:
        if hint.get("listing_id"):
            all_listing_ids.append(hint["listing_id"])

    vin_by_listing: Dict[str, str] = {}
    with db_cursor(error_context="detail_active: batch_lookup_vin", dict_cursor=True) as cur:
        cur.execute(BATCH_LOOKUP_VIN_TO_LISTING, {"listing_ids": all_listing_ids})
        for row in cur.fetchall():
            vin_by_listing[str(row["listing_id"])] = row["vin"]

    # Resolve primary VIN: prefer parsed, fall back to lookup
    if not vin:
        vin = vin_by_listing.get(listing_id)

    # --- Steps 2-4: Primary observation writes (single transaction) ---
    vin_collision_deleted = False
    previous_listing_id = None
    with db_cursor(error_context=f"detail_active: writes artifact_id={artifact_id}") as cur:
        # Step 2: VIN collision check
        if vin:
            cur.execute(LOOKUP_VIN_COLLISION, {"vin": vin, "listing_id": listing_id})
            collision = cur.fetchone()
            if collision:
                old_listing_id = collision[0]
                cur.execute(DELETE_PRICE_OBSERVATION_BY_VIN, {"old_listing_id": old_listing_id})
                vin_collision_deleted = True
                previous_listing_id = str(old_listing_id)
                # Event: old row deleted due to relisting
                cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
                    "listing_id": old_listing_id,
                    "vin": vin,
                    "price": None,
                    "make": None,
                    "model": None,
                    "artifact_id": artifact_id,
                    "event_type": "deleted",
                    "source": "detail",
                })
                logger.info(
                    "detail: VIN relisting detected vin=%s old=%s new=%s",
                    vin, old_listing_id, listing_id,
                )

        # Step 3: Upsert primary price_observation
        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id,
            "vin": vin,
            "price": primary.get("price"),
            "make": primary.get("make"),
            "model": primary.get("model"),
            "customer_id": primary.get("customer_id"),
            "last_seen_at": fetched_at,
            "last_artifact_id": artifact_id,
        })
        # Event: primary price_observation upserted
        cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
            "listing_id": listing_id,
            "vin": vin,
            "price": primary.get("price"),
            "make": primary.get("make"),
            "model": primary.get("model"),
            "artifact_id": artifact_id,
            "event_type": "upserted",
            "source": "detail",
        })

        # Step 4: Upsert vin_to_listing
        if vin:
            cur.execute(UPSERT_VIN_TO_LISTING, {
                "vin": vin,
                "listing_id": listing_id,
                "mapped_at": fetched_at,
                "artifact_id": artifact_id,
            })
            if cur.rowcount > 0:
                event_type = "remapped" if previous_listing_id else "mapped"
                cur.execute(INSERT_VIN_TO_LISTING_EVENT, {
                    "vin": vin,
                    "listing_id": listing_id,
                    "artifact_id": artifact_id,
                    "event_type": event_type,
                    "previous_listing_id": previous_listing_id,
                })
                events_to_emit.append(("vin_mapped", listing_id, vin))

        # Step 5: Carousel filtering and upsert
        carousel_upserted = 0
        carousel_filtered = 0
        for hint in carousel:
            hint_listing_id = hint.get("listing_id")
            if not hint_listing_id:
                continue
            # Sanity filter: drop hints with null price or body
            if hint.get("price") is None or not hint.get("body"):
                continue

            if _carousel_matches_search_config(hint):
                hint_vin = vin_by_listing.get(hint_listing_id)
                if hint_vin:
                    cur.execute(
                        LOOKUP_VIN_COLLISION, 
                        {"vin": hint_vin, "listing_id": hint_listing_id}
                    )
                    collision = cur.fetchone()
                    if collision:
                        cur.execute(
                            DELETE_PRICE_OBSERVATION_BY_VIN, 
                            {"old_listing_id": collision[0]}
                        )
                cur.execute(UPSERT_PRICE_OBSERVATION, {
                    "listing_id": hint_listing_id,
                    "vin": hint_vin,
                    "price": hint.get("price"),
                    "make": None,  # carousel doesn't have structured make/model
                    "model": None,
                    "customer_id": None,  # carousel never enriches dealer info
                    "last_seen_at": fetched_at,
                    "last_artifact_id": artifact_id,
                })
                # Event: carousel price_observation upserted
                cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
                    "listing_id": hint_listing_id,
                    "vin": hint_vin,
                    "price": hint.get("price"),
                    "make": None,
                    "model": None,
                    "artifact_id": artifact_id,
                    "event_type": "upserted",
                    "source": "carousel",
                })
                carousel_upserted += 1

                if hint_vin and hint.get("price"):
                    events_to_emit.append((
                        "price_updated", hint_vin, str(hint["price"]),
                        hint_listing_id,
                    ))
            else:
                carousel_filtered += 1

        # Step 7: Clear blocked_cooldown
        cur.execute(CLEAR_BLOCKED_COOLDOWN, {"listing_id": listing_id})

        # Step 8: Release detail_scrape_claims
        cur.execute(RELEASE_DETAIL_CLAIMS, {"listing_id": listing_id})
        cur.execute(INSERT_DETAIL_CLAIM_EVENT, {
            "listing_id": listing_id,
            "run_id": run_id,
            "status": "processed",
        })

    # --- Step 6: Silver write (non-fatal) ---
    _URL_PREFIX = "https://www.cars.com/vehicledetail/"
    dealer_fields = {
        "dealer_name": primary.get("dealer_name"),
        "dealer_zip": primary.get("dealer_zip"),
        "customer_id": primary.get("customer_id"),
        "seller_id": primary.get("seller_id"),
        "dealer_street": primary.get("dealer_street"),
        "dealer_city": primary.get("dealer_city"),
        "dealer_state": primary.get("dealer_state"),
        "dealer_phone": primary.get("dealer_phone"),
        "dealer_website": primary.get("dealer_website"),
        "dealer_cars_com_url": primary.get("dealer_cars_com_url"),
        "dealer_rating": primary.get("dealer_rating"),
    }
    silver_rows = [{
        "artifact_id": artifact_id,
        "listing_id": listing_id,
        "vin": vin,
        "canonical_detail_url": f"{_URL_PREFIX}{listing_id}/",
        "price": primary.get("price"),
        "make": primary.get("make"),
        "model": primary.get("model"),
        "trim": primary.get("trim"),
        "year": primary.get("year"),
        "mileage": primary.get("mileage"),
        "msrp": primary.get("msrp"),
        "stock_type": primary.get("stock_type"),
        "fuel_type": primary.get("fuel_type"),
        "body_style": primary.get("body_style"),
        "listing_state": "active",
        "source": "detail",
        "fetched_at": fetched_at,
        **dealer_fields,
    }]
    # All carousel hints go to silver regardless of search_config match
    for hint in carousel:
        if not hint.get("listing_id"):
            continue
        if hint.get("price") is None or not hint.get("body"):
            continue
        hint_lid = hint["listing_id"]
        silver_rows.append({
            "artifact_id": artifact_id,
            "listing_id": hint_lid,
            "vin": vin_by_listing.get(hint_lid),
            "canonical_detail_url": hint.get("canonical_detail_url")
            or f"{_URL_PREFIX}{hint_lid}/",
            "price": hint.get("price"),
            "mileage": hint.get("mileage"),
            "body": hint.get("body"),
            "condition": hint.get("condition"),
            "year": hint.get("year"),
            "listing_state": "active",
            "source": "carousel",
            "fetched_at": fetched_at,
            **dealer_fields,
        })
    silver_written = write_silver_observations_postgres(silver_rows)

    # --- Emit events (after commit) ---
    if vin and primary.get("price"):
        emit_price_updated(vin=vin, price=primary["price"],
                           listing_id=listing_id, source="detail")
    for event in events_to_emit:
        if event[0] == "vin_mapped":
            emit_vin_mapped(listing_id=event[1], vin=event[2])
        elif event[0] == "price_updated":
            emit_price_updated(vin=event[1], price=int(event[2]),
                               listing_id=event[3], source="detail")

    return {
        "upserted": 1,
        "vin": vin,
        "vin_collision_deleted": vin_collision_deleted,
        "carousel_upserted": carousel_upserted,
        "carousel_filtered": carousel_filtered,
        "silver_written": silver_written,
    }


def write_detail_unlisted(
    primary: Dict[str, Any],
    artifact_id: int,
    fetched_at: datetime,
    listing_id: str,
    run_id: Optional[str],
) -> Dict[str, Any]:
    """Detail write path for unlisted listings."""
    vin = primary.get("vin")

    # Unlisted pages rarely carry a VIN in the activity JSON — look it up so the
    # silver row has a vin17 and flows through int_latest_observation correctly.
    if not vin:
        with db_cursor(
            error_context="detail_unlisted: vin_lookup", dict_cursor=True,
        ) as cur:
            cur.execute(BATCH_LOOKUP_VIN_TO_LISTING, {"listing_ids": [listing_id]})
            row = cur.fetchone()
            if row:
                vin = str(row["vin"])

    with db_cursor(error_context=f"detail_unlisted: writes artifact_id={artifact_id}") as cur:
        cur.execute(DELETE_PRICE_OBSERVATION, {"listing_id": listing_id})
        # Event: price_observation deleted (unlisted)
        cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
            "listing_id": listing_id,
            "vin": vin,
            "price": None,
            "make": primary.get("make"),
            "model": primary.get("model"),
            "artifact_id": artifact_id,
            "event_type": "deleted",
            "source": "detail",
        })
        cur.execute(CLEAR_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(RELEASE_DETAIL_CLAIMS, {"listing_id": listing_id})
        cur.execute(INSERT_DETAIL_CLAIM_EVENT, {
            "listing_id": listing_id,
            "run_id": run_id,
            "status": "processed",
        })

    # Silver write (non-fatal)
    _URL_PREFIX = "https://www.cars.com/vehicledetail/"
    silver_written = write_silver_observations_postgres([{
        "artifact_id": artifact_id,
        "listing_id": listing_id,
        "vin": vin,
        "canonical_detail_url": f"{_URL_PREFIX}{listing_id}/",
        "price": None,
        "make": primary.get("make"),
        "model": primary.get("model"),
        "trim": primary.get("trim"),
        "year": primary.get("year"),
        "mileage": None,
        "stock_type": primary.get("stock_type"),
        "fuel_type": primary.get("fuel_type"),
        "body_style": primary.get("body_style"),
        "dealer_name": primary.get("dealer_name"),
        "dealer_zip": primary.get("dealer_zip"),
        "customer_id": primary.get("customer_id"),
        "seller_id": primary.get("seller_id"),
        "listing_state": "unlisted",
        "source": "detail",
        "fetched_at": fetched_at,
    }])

    emit_listing_removed(vin=vin, listing_id=listing_id)

    return {"deleted": True, "vin": vin, "silver_written": silver_written}


