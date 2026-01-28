from __future__ import annotations
from datetime import datetime, UTC
from typing import Any, Dict, Optional
import hashlib
import os
import requests


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _default_headers() -> Dict[str, str]:
    # Match your SRP scraper "mobile chrome" UA style for consistency.
    return {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.1047.1013 "
            "Mobile Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def scrape_detail_fetch(*, run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Production detail-page scraper processor.

    Inputs (payload):
      listing_id: required
      url: optional (defaults to https://www.cars.com/vehicledetail/<listing_id>/)
      vin: optional (only used for search_key convenience)
      timeout_s: optional
      headers: optional override headers dict

    Returns:
      { "artifacts": [...], "meta": {...}, "error": None|str }
    """
    listing_id = (payload or {}).get("listing_id")
    vin = (payload or {}).get("vin")
    url = (payload or {}).get("url") or (f"https://www.cars.com/vehicledetail/{listing_id}/" if listing_id else None)

    if not listing_id:
        return {"error": "payload.listing_id is required", "artifacts": [], "meta": {"mode": "fetch"}}
    if not url:
        return {"error": "payload.url could not be derived", "artifacts": [], "meta": {"mode": "fetch", "listing_id": listing_id}}

    raw_base = os.environ.get("RAW_BASE", "/data/raw")
    run_dir = os.path.join(raw_base, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    fetched_at = datetime.now(UTC).isoformat()

    timeout_s = int((payload or {}).get("timeout_s") or 30)
    headers = _default_headers()
    hdr_override = (payload or {}).get("headers")
    if isinstance(hdr_override, dict):
        headers.update({str(k): str(v) for k, v in hdr_override.items()})

    session = requests.Session()

    # We always write *something* for auditability.
    # Non-200 responses get written too (useful for debugging blocks/interstitials).
    try:
        resp = session.get(url, headers=headers, timeout=timeout_s, allow_redirects=True)
        status = resp.status_code
        content_type = resp.headers.get("content-type")
        content = resp.content or b""
        size = len(content)

        filename = f"detail_{listing_id}__{status}.html"
        filepath = os.path.join(run_dir, filename)
        with open(filepath, "wb") as f:
            f.write(content)

        artifact = {
            "source": "cars.com",
            "artifact_type": "detail_page",
            "search_key": vin or listing_id,     # convenience; not a DB key
            "search_scope": "detail",
            "page_num": None,
            "url": str(resp.url),                # final URL after redirects
            "fetched_at": fetched_at,
            "http_status": status,
            "content_type": content_type,
            "content_bytes": size,
            "sha256": _sha256_bytes(content) if content else None,
            "filepath": filepath,
            "error": None if status == 200 else f"HTTP {status}",
        }

        return {
            "error": None if status == 200 else f"HTTP {status}",
            "artifacts": [artifact],
            "meta": {
                "mode": "fetch",
                "listing_id": listing_id,
                "vin": vin,
                "final_url": str(resp.url),
            },
        }

    except Exception as e:
        # Write an error marker file so every attempt leaves a disk trace.
        filename = f"detail_{listing_id}__ERROR.txt"
        filepath = os.path.join(run_dir, filename)
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"{type(e).__name__}: {e}\n")
                f.write(f"url={url}\n")
        except Exception:
            pass

        return {
            "error": f"{type(e).__name__}: {e}",
            "artifacts": [
                {
                    "source": "cars.com",
                    "artifact_type": "detail_page",
                    "search_key": vin or listing_id,
                    "search_scope": "detail",
                    "page_num": None,
                    "url": url,
                    "fetched_at": fetched_at,
                    "http_status": None,
                    "content_type": None,
                    "content_bytes": None,
                    "sha256": None,
                    "filepath": filepath,
                    "error": f"{type(e).__name__}: {e}",
                }
            ],
            "meta": {"mode": "fetch", "listing_id": listing_id, "vin": vin},
        }


def _write_dummy_detail_html(listing_id: str, vin: Optional[str]) -> bytes:
    vin_str = vin or ""
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Dummy Cars.com Detail - {listing_id}</title>
  </head>
  <body>
    <h1>Dummy Cars.com Detail Page</h1>

    <section id="primary-listing">
      <script id="primary-listing-json" type="application/json">
        {{
          "listing_id": "{listing_id}",
          "vin": "{vin_str}",
          "seller_customer_id": null,
          "dealer_name": null
        }}
      </script>
    </section>

    <section id="listings-carousel">
      <div class="carousel-item"
           data-listing-id="{listing_id}"
           data-price="0"
           data-mileage="0"></div>
    </section>

  </body>
</html>
"""
    return html.encode("utf-8")


def scrape_detail_dummy(*, run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dummy detail-page scraper processor.

    Returns the same contract your n8n expects:
      { "artifacts": [...], "meta": {...}, "error": None|str }
    """
    listing_id = (payload or {}).get("listing_id")
    vin = (payload or {}).get("vin")
    url = (payload or {}).get("url") or f"https://www.cars.com/vehicledetail/{listing_id}/"

    if not listing_id:
        return {"error": "payload.listing_id is required", "artifacts": [], "meta": {"mode": "dummy"}}

    raw_base = os.environ.get("RAW_BASE", "/data/raw")
    run_dir = os.path.join(raw_base, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    fetched_at = datetime.now(UTC).isoformat()
    filepath = os.path.join(run_dir, f"detail_{listing_id}.html")

    try:
        content = _write_dummy_detail_html(listing_id=listing_id, vin=vin)
        with open(filepath, "wb") as f:
            f.write(content)

        artifact = {
            "source": "cars.com",
            "artifact_type": "detail_page",
            "search_key": vin or listing_id,
            "search_scope": "detail",
            "page_num": None,
            "url": url,
            "fetched_at": fetched_at,
            "http_status": 200,
            "content_type": "text/html; charset=utf-8",
            "content_bytes": len(content),
            "sha256": _sha256_bytes(content),
            "filepath": filepath,
            "error": None,
        }

        return {
            "error": None,
            "artifacts": [artifact],
            "meta": {"mode": "dummy", "listing_id": listing_id, "vin": vin, "wrote": True},
        }

    except Exception as e:
        return {
            "error": f"failed to write dummy detail artifact: {type(e).__name__}: {e}",
            "artifacts": [
                {
                    "source": "cars.com",
                    "artifact_type": "detail_page",
                    "search_key": vin or listing_id,
                    "search_scope": "detail",
                    "page_num": None,
                    "url": url,
                    "fetched_at": fetched_at,
                    "http_status": None,
                    "content_type": None,
                    "content_bytes": None,
                    "sha256": None,
                    "filepath": filepath,
                    "error": f"{type(e).__name__}: {e}",
                }
            ],
            "meta": {"mode": "dummy", "listing_id": listing_id, "vin": vin, "wrote": False},
        }
