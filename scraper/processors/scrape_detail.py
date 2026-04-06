from __future__ import annotations
from datetime import datetime, UTC
from typing import Any, Dict, Optional, Tuple
import hashlib
import logging
import os
import threading
import time

from curl_cffi import requests as cf_requests

# Browser fingerprint to impersonate — curl_cffi uses this for TLS fingerprinting
# to bypass Cloudflare WAF. Rotate to newer versions if this gets blocked.
BROWSER_IMPERSONATE = "chrome146"

# FlareSolverr is used to solve the Cloudflare JS challenge on the first request
# of a batch, then we reuse the resulting cookies for all subsequent curl_cffi fetches.
# Set FLARESOLVERR_URL to empty string to disable and fall back to plain curl_cffi.
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191")

# cf_clearance cookies are typically valid for ~30 minutes from the same IP.
# We conservatively expire the cached session at 25 minutes to avoid edge cases.
_CF_SESSION_TTL = 25 * 60  # seconds

_cf_session_lock = threading.Lock()
_cf_session: Optional[cf_requests.Session] = None
_cf_session_expires_at: float = 0.0

logger = logging.getLogger("scraper")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _bootstrap_cf_session(url: str, timeout_s: int) -> Tuple[cf_requests.Session, bytes, int]:
    """
    Calls FlareSolverr to solve the Cloudflare JS challenge for url.

    Returns (session, html_bytes, http_status).
    - session has cf_clearance cookies + matching User-Agent injected
    - html_bytes is the response body from FlareSolverr (reused as the artifact,
      so we don't make a duplicate request for the first listing)
    """
    import requests as stdlib_requests

    resp = stdlib_requests.post(
        f"{FLARESOLVERR_URL}/v1",
        json={"cmd": "request.get", "url": url, "maxTimeout": timeout_s * 1000},
        timeout=timeout_s + 15,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "ok":
        raise RuntimeError(f"FlareSolverr failed: {data.get('message', data)}")

    solution = data["solution"]
    user_agent = solution["userAgent"]
    html = (solution.get("response") or "").encode("utf-8")
    http_status = solution.get("status", 200)
    cookies = {c["name"]: c["value"] for c in solution.get("cookies", [])}

    session = cf_requests.Session(impersonate=BROWSER_IMPERSONATE)
    session.headers.update({"User-Agent": user_agent})
    for name, value in cookies.items():
        session.cookies.set(name, value)

    logger.info("FlareSolverr bootstrapped CF session (status=%s, cookies=%s)", http_status, list(cookies.keys()))
    return session, html, http_status


def _get_cf_session(url: str, timeout_s: int) -> Tuple[cf_requests.Session, Optional[bytes], Optional[int]]:
    """
    Returns a Cloudflare-bootstrapped curl_cffi session.

    Two cases:
    - Cache hit (session still valid): returns (session, None, None).
      Caller must fetch url itself using the session.
    - Cache miss (expired or first call): calls FlareSolverr, returns
      (session, html_bytes, http_status). Caller should use html_bytes directly
      as the artifact for url — no duplicate fetch needed.
    """
    global _cf_session, _cf_session_expires_at

    with _cf_session_lock:
        now = time.monotonic()
        if _cf_session is not None and now < _cf_session_expires_at:
            return _cf_session, None, None

        session, html, status = _bootstrap_cf_session(url, timeout_s)
        _cf_session = session
        _cf_session_expires_at = now + _CF_SESSION_TTL
        return session, html, status


def _fetch_url(url: str, timeout_s: int) -> Tuple[bytes, int, Optional[str], str]:
    """
    Fetches url using a Cloudflare-bootstrapped curl_cffi session (if FLARESOLVERR_URL
    is set), or a plain curl_cffi session as fallback.

    Returns (content_bytes, http_status, content_type, final_url).
    """
    if FLARESOLVERR_URL:
        try:
            session, bootstrap_html, bootstrap_status = _get_cf_session(url, timeout_s)
            if bootstrap_html is not None:
                # FlareSolverr fetched this URL for us — reuse the response
                return bootstrap_html, bootstrap_status, "text/html; charset=utf-8", url
            # Cache hit — use curl_cffi with the bootstrapped session
            resp = session.get(url, timeout=timeout_s, allow_redirects=True)
            content = resp.content or b""
            return content, resp.status_code, resp.headers.get("content-type"), str(resp.url)
        except Exception as e:
            logger.warning("FlareSolverr/CF session failed (%s), falling back to plain curl_cffi", e)

    # Plain curl_cffi fallback (no FlareSolverr)
    session = cf_requests.Session(impersonate=BROWSER_IMPERSONATE)
    resp = session.get(url, timeout=timeout_s, allow_redirects=True)
    content = resp.content or b""
    return content, resp.status_code, resp.headers.get("content-type"), str(resp.url)


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

    # We always write *something* for auditability.
    # Non-200 responses get written too (useful for debugging blocks/interstitials).
    try:
        content, status, content_type, final_url = _fetch_url(url, timeout_s)
        size = len(content)

        filename = f"detail_{listing_id}__{status}.html"
        filepath = os.path.join(run_dir, filename)
        with open(filepath, "wb") as f:
            f.write(content)

        artifact = {
            "source": "cars.com",
            "artifact_type": "detail_page",
            "search_key": vin or listing_id,
            "search_scope": "detail",
            "page_num": None,
            "url": final_url,
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
                "final_url": final_url,
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
