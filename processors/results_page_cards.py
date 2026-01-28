from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
import json
import html as html_lib

# Matches "(1,498 mi.)" (with optional decimals)
_MILES_RE = re.compile(r"\(([\d,]+(?:\.\d+)?)\s*mi\.\)", re.IGNORECASE)


def _price_to_int(text: Optional[str]) -> Optional[int]:
    """
    Convert strings like '\\n  $40,658\\n' to 40658.
    Ignores whitespace and non-digits.
    """
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else None


def _miles_to_float(text: Optional[str]) -> Optional[float]:
    """
    Convert strings like '\\n\\n  Wallingford, CT (1,498 mi.)\\n' to 1498.0.
    """
    if not text:
        return None
    m = _MILES_RE.search(text)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def parse_cars_results_page_html(html: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Minimal Cars.com SRP parser (strict selectors, no fallbacks).

    Returns:
      listings: List[dict] matching your n8n upsert schema:
        - listing_id (text)
        - canonical_detail_url (text)
        - last_seen_price (int)
        - last_seen_dealer (text)
        - last_seen_distance_miles (numeric)
      meta: diagnostics
    """
    soup = BeautifulSoup(html, "lxml")

    cards = soup.select("div.vehicle-card[data-listing-id]")
    listings: List[Dict[str, Any]] = []

    missing_url = 0
    missing_price = 0
    missing_dealer = 0
    missing_distance = 0

    for card in cards:
        listing_id = (card.get("data-listing-id") or "").strip()
        if not listing_id:
            # Skip malformed cards only if they don't have the primary key
            continue

        # Detail URL (strict)
        a = card.select_one('a[href^="/vehicledetail/"]')
        href = a.get("href") if a else None
        canonical_detail_url = f"https://www.cars.com{href}" if href else None
        if not canonical_detail_url:
            missing_url += 1

        # Price (strict)
        price_el = card.select_one('[data-qa="primary-price"]')
        price_text = price_el.get_text() if price_el else None
        last_seen_price = _price_to_int(price_text)
        if last_seen_price is None:
            missing_price += 1

        # Dealer (strict)
        dealer_el = card.select_one(".vehicle-dealer .dealer-name strong")
        last_seen_dealer = dealer_el.get_text().strip() if dealer_el else None
        if not last_seen_dealer:
            missing_dealer += 1

        # Distance (strict)
        miles_el = card.select_one('[data-qa="miles-from-user"]')
        miles_text = miles_el.get_text() if miles_el else None
        last_seen_distance_miles = _miles_to_float(miles_text)
        if last_seen_distance_miles is None:
            missing_distance += 1

        listings.append(
            {
                "listing_id": listing_id,
                "canonical_detail_url": canonical_detail_url,
                "last_seen_price": last_seen_price,
                "last_seen_dealer": last_seen_dealer,
                "last_seen_distance_miles": last_seen_distance_miles,
            }
        )

    meta = {
        "parser": "cars_results_page_v1_minimal_strict",
        "cards_found": len(cards),
        "listing_ids_extracted": len(listings),
        "missing_url": missing_url,
        "missing_price": missing_price,
        "missing_dealer": missing_dealer,
        "missing_distance": missing_distance,
    }

    return listings, meta


def parse_cars_results_page_html_v2(html: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Cars.com SRP parser v2 (structured-data-first).

    Primary source:
      - div[data-site-activity] JSON -> vehicleArray[]

    Secondary enrichment:
      - div.vehicle-card[data-listing-id] attributes -> trid (and optional tracking attrs)

    Output keys align to your sample:
      listing_id, canonical_detail_url, year, make, model, trim, stockType,
      price, msrp, mileage, vin, fuelType, bodyStyle, financingType,
      seller_zip, seller_customerId, page_number, position_on_page, trid, isaContext

    Notes:
      - financingType is not present in this payload; we set it to "unavailable".
      - seller_customerId in this payload is a numeric string (customer_id) on this page variant.
      - We include last_seen_price as an alias for compatibility with existing n8n mappings.
    """
    soup = BeautifulSoup(html, "lxml")

    def _to_int(val: Any) -> Optional[int]:
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

    # 1) Card map: listing_id -> trid (and other useful tracking attrs)
    card_tags = soup.select("div.vehicle-card[data-listing-id]")
    card_meta_by_listing_id: Dict[str, Dict[str, Any]] = {}
    for dom_pos, card in enumerate(card_tags, start=1):
        lid = (card.get("data-listing-id") or "").strip()
        if not lid:
            continue
        card_meta_by_listing_id[lid] = {
            "trid": card.get("trid"),
            "trv": card.get("trv"),
            "trsi": card.get("trsi"),
            "trc": card.get("trc"),
            "position_on_page_dom": dom_pos,
            "data_tracking_id": card.get("data-tracking-id"),
        }

    # 2) Structured payload: data-site-activity -> vehicleArray
    site_activity_el = soup.select_one("[data-site-activity]")
    if not site_activity_el:
        return [], {
            "parser": "cars_results_page__listings_v2_site_activity_vehicle_array",
            "cards_found": len(card_tags),
            "listing_ids_extracted": 0,
            "site_activity_found": 0,
            "vehicle_array_len": 0,
            "json_failures": 0,
            "listing_id_mismatches": 0,
        }

    site_activity_raw = site_activity_el.get("data-site-activity") or ""
    try:
        site_activity = json.loads(site_activity_raw)
    except Exception:
        return [], {
            "parser": "cars_results_page__listings_v2_site_activity_vehicle_array",
            "cards_found": len(card_tags),
            "listing_ids_extracted": 0,
            "site_activity_found": 1,
            "vehicle_array_len": 0,
            "json_failures": 1,
            "listing_id_mismatches": 0,
        }

    vehicle_array = site_activity.get("vehicleArray") or []
    page_number = site_activity.get("result_page_number")

    listings: List[Dict[str, Any]] = []
    listing_id_mismatches = 0

    for v in vehicle_array:
        if not isinstance(v, dict):
            continue

        listing_id = (v.get("listing_id") or "").strip()
        if not listing_id:
            continue

        cm = card_meta_by_listing_id.get(listing_id) or {}

        # If you want to detect mismatches between DOM cards and vehicleArray:
        # (Not required; just a useful diagnostic.)
        if card_meta_by_listing_id and listing_id not in card_meta_by_listing_id:
            listing_id_mismatches += 1

        canonical_detail_url = f"https://www.cars.com/vehicledetail/{listing_id}/"

        sponsored_type = v.get("sponsored_type")
        isa_context = sponsored_type.upper() if isinstance(sponsored_type, str) and sponsored_type else None

        out = {
            "listing_id": listing_id,
            "canonical_detail_url": canonical_detail_url,

            "year": _to_int(v.get("year")),
            "make": v.get("make"),
            "model": v.get("model"),
            "trim": v.get("trim"),

            "stockType": v.get("stock_type"),

            "price": _to_int(v.get("price")),
            "msrp": _to_int(v.get("msrp")),
            "mileage": _to_int(v.get("mileage")),

            "vin": v.get("vin"),

            "fuelType": v.get("fuel_type"),
            "bodyStyle": v.get("bodystyle"),
            "financingType": "unavailable",

            "seller_zip": v.get("dealer_zip"),
            "seller_customerId": v.get("customer_id"),

            "page_number": _to_int(page_number),
            "position_on_page": _to_int(v.get("vertical_position")),

            "trid": cm.get("trid"),
            "isaContext": isa_context,

            # Compatibility alias for existing n8n mapping
            "last_seen_price": _to_int(v.get("price")),
        }

        listings.append(out)

    meta_out = {
        "parser": "cars_results_page__listings_v2_site_activity_vehicle_array",
        "cards_found": len(card_tags),
        "site_activity_found": 1,
        "vehicle_array_len": len(vehicle_array),
        "listing_ids_extracted": len(listings),
        "json_failures": 0,
        "listing_id_mismatches": listing_id_mismatches,
    }

    return listings, meta_out
