from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup


def _digits_to_int(val: Any) -> Optional[int]:
    """Convert various types to int, stripping non-digit chars from strings."""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        digits = re.sub(r"\D", "", val)
        return int(digits) if digits else None
    return None


def parse_cars_results_page_html_v3(html: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Cars.com SRP parser v3 (srp2025 spark-card / fuse-card format).

    Cars.com migrated to a new frontend around Jan 31 2026. Each listing is now
    a <spark-card> or <fuse-card> element with data-vehicle-details='{...}'
    instead of a single data-site-activity blob. The fuse-card rename was
    observed around Mar 19 2026 — data structure is identical.

    Field mapping from data-vehicle-details JSON:
      listingId             -> listing_id
      vin, make, model, trim, year, mileage, msrp, price -> direct
      stockType             -> stockType
      fuelType              -> fuelType
      bodyStyle             -> bodyStyle
      financingType         -> financingType
      seller.zip            -> seller_zip
      seller.customerId     -> seller_customerId
      trid                  -> trid
      isaContext            -> isaContext
      metadata.page_number  -> page_number
      metadata.position_on_page -> position_on_page
    """
    soup = BeautifulSoup(html, "lxml")

    # Support both old (spark-card) and new (fuse-card) component names
    cards = soup.select("fuse-card[data-vehicle-details], spark-card[data-vehicle-details]")

    listings: List[Dict[str, Any]] = []
    json_failures = 0

    for card in cards:
        raw = card.get("data-vehicle-details") or ""
        try:
            v = json.loads(raw)
        except Exception:
            json_failures += 1
            continue

        listing_id = (v.get("listingId") or "").strip()
        if not listing_id:
            continue

        seller = v.get("seller") or {}
        metadata = v.get("metadata") or {}

        isa_raw = v.get("isaContext")
        isa_context = isa_raw.upper() if isinstance(isa_raw, str) and isa_raw else None

        out = {
            "listing_id": listing_id,
            "canonical_detail_url": f"https://www.cars.com/vehicledetail/{listing_id}/",

            "year": _digits_to_int(v.get("year")),
            "make": v.get("make"),
            "model": v.get("model"),
            "trim": v.get("trim"),

            "stockType": v.get("stockType"),

            "price": _digits_to_int(v.get("price")),
            "msrp": _digits_to_int(v.get("msrp")),
            "mileage": _digits_to_int(v.get("mileage")),

            "vin": v.get("vin"),

            "fuelType": v.get("fuelType"),
            "bodyStyle": v.get("bodyStyle"),
            "financingType": v.get("financingType"),

            "seller_zip": seller.get("zip"),
            "seller_customerId": seller.get("customerId"),

            "page_number": _digits_to_int(metadata.get("page_number")),
            "position_on_page": _digits_to_int(metadata.get("position_on_page")),

            "trid": v.get("trid"),
            "isaContext": isa_context,

            "last_seen_price": _digits_to_int(v.get("price")),
        }

        listings.append(out)

    return listings, {
        "parser": "cars_results_page__listings_v3_spark_card",
        "cards_found": len(cards),
        "listing_ids_extracted": len(listings),
        "json_failures": json_failures,
    }
