from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup


_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)

_UNLISTED_TEXT_RE = re.compile(r"\bno longer available\b|\bno longer listed\b", re.IGNORECASE)


def _digits_to_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else None


def _extract_script_json_by_id(soup: BeautifulSoup, script_id: str) -> Optional[Dict[str, Any]]:
    el = soup.select_one(f"script#{script_id}")
    if not el:
        return None
    raw = (el.string or el.get_text() or "").strip()
    if not raw:
        return None
    return json.loads(raw)


def _detect_unlisted(soup: BeautifulSoup, html: str) -> Optional[Dict[str, Any]]:
    """
    Cars.com "no longer listed" pages include a spark-notification like:
      <spark-notification class="unlisted-notification" title="No longer listed"> ... </spark-notification>

    Return a dict describing the unlisted state if found, else None.
    """
    note = soup.select_one("spark-notification.unlisted-notification")
    if note:
        title = (note.get("title") or "").strip() or None
        text = note.get_text(" ", strip=True) or None
        return {
            "listing_state": "unlisted",
            "unlisted_title": title,
            "unlisted_message": text,
        }

    # Fallback: search for common text markers
    if _UNLISTED_TEXT_RE.search(html):
        return {
            "listing_state": "unlisted",
            "unlisted_title": None,
            "unlisted_message": "page contains 'no longer available/listed' marker text",
        }

    return None


def _extract_listing_id_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = _UUID_RE.search(url)
    return m.group(0) if m else None


def _parse_carousel_cards(soup: BeautifulSoup) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Parses:
      <div class="listings-carousel">
        <spark-card-carousel>
          <spark-card> ... </spark-card>
        </spark-card-carousel>
      </div>

    Expected stable bits in samples:
      - spark-save[data-listing-id]
      - a[href^="/vehicledetail/"]
      - span.price, span.body
      - span[slot="footer"] containing something like "10 mi"
    """
    meta: Dict[str, Any] = {
        "carousel_found": False,
        "cards_found": 0,
        "cards_parsed": 0,
        "missing_listing_id": 0,
        "missing_price": 0,
        "missing_body": 0,
        "missing_mileage": 0,
    }

    container = soup.select_one("div.listings-carousel")
    if not container:
        return [], meta

    meta["carousel_found"] = True

    cards_out: List[Dict[str, Any]] = []
    cards = container.select("spark-card")
    meta["cards_found"] = len(cards)

    for card in cards:
        # listing_id
        listing_id = None
        save = card.select_one("spark-save[data-listing-id]")
        if save and save.has_attr("data-listing-id"):
            listing_id = (save.get("data-listing-id") or "").strip()

        if not listing_id:
            a = card.select_one('a[href^="/vehicledetail/"]')
            href = a.get("href") if a else None
            if href:
                m = _UUID_RE.search(href)
                if m:
                    listing_id = m.group(0)

        if not listing_id:
            meta["missing_listing_id"] += 1
            # can’t upsert without PK — skip
            continue

        # href
        a = card.select_one('a[href^="/vehicledetail/"]')
        href = a.get("href") if a else None
        canonical_detail_url = f"https://www.cars.com{href}" if href else None

        # price/body
        price_text = None
        price_el = card.select_one("span.price")
        if price_el:
            price_text = price_el.get_text(" ", strip=True)
        price = _digits_to_int(price_text)
        if price is None:
            meta["missing_price"] += 1

        body_el = card.select_one("span.body")
        body = body_el.get_text(" ", strip=True) if body_el else None
        if not body:
            meta["missing_body"] += 1

        # mileage (footer)
        footer = card.select_one('span[slot="footer"]')
        footer_text = footer.get_text(" ", strip=True) if footer else None
        mileage = _digits_to_int(footer_text)
        if mileage is None:
            meta["missing_mileage"] += 1

        # lightweight derived fields (optional)
        condition = None
        year = None
        if body:
            # e.g. "New 2025 Toyota RAV4 XLE"
            m = re.match(r"^(New|Used|Certified|CPO)\s+(\d{4})\s+(.*)$", body, re.IGNORECASE)
            if m:
                condition = m.group(1)
                year = int(m.group(2))

        cards_out.append(
            {
                "listing_id": listing_id,
                "canonical_detail_url": canonical_detail_url,
                "price": price,
                "mileage": mileage,
                "body": body,
                "condition": condition,
                "year": year,
            }
        )

    meta["cards_parsed"] = len(cards_out)
    return cards_out, meta


def parse_cars_detail_page_html_v1(
        html: str,
        url: Optional[str] = None
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Cars.com detail page parser v1.

    Primary listing:
      - script#initial-activity-data (JSON, top-level keys include listing_id, vin, price, mileage, etc.)

    Carousel:
      - div.listings-carousel spark-card (HTML)

    Returns:
      primary: dict
      carousel: list[dict]
      meta: diagnostics
    """
    soup = BeautifulSoup(html, "lxml")

    unlisted = _detect_unlisted(soup, html)
    listing_state = (unlisted or {}).get("listing_state") or "active"

    # --- Primary JSON blob ---
    activity = _extract_script_json_by_id(soup, "initial-activity-data") or {}

    listing_id_from_activity = activity.get("listing_id")
    listing_id_from_url = _extract_listing_id_from_url(url)
    listing_id = listing_id_from_activity or listing_id_from_url
    listing_id_source = "initial-activity-data" if listing_id_from_activity else (
        "url" if listing_id_from_url else None)

    primary = {
        "listing_state": listing_state,
        "unlisted_title": (unlisted or {}).get("unlisted_title"),
        "unlisted_message": (unlisted or {}).get("unlisted_message"),

        "listing_id": listing_id,
        "listing_id_source": listing_id_source,
        "vin": activity.get("vin"),
        "price": activity.get("price"),
        "mileage": activity.get("mileage"),
        "msrp": activity.get("msrp"),
        "stock_type": activity.get("stock_type"),
        "dealer_name": activity.get("dealer_name"),
        "dealer_zip": activity.get("dealer_zip"),
        "seller_id": activity.get("seller_id"),
        "customer_id": activity.get("customer_id"),
    }

    # --- Carousel ---
    carousel, carousel_meta = _parse_carousel_cards(soup)

    meta: Dict[str, Any] = {
        "parser": "cars_detail_page__v1",
        "html_len": len(html),
        "primary_json_present": bool(activity),
        "listing_id_source": listing_id_source,
        "primary_keys_present": sorted([k for k, v in primary.items() if v is not None]),
        **carousel_meta,
    }

    return primary, carousel, meta
