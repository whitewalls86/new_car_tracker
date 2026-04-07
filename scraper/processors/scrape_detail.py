from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import logging
import os
import threading
import time

from curl_cffi import requests as cf_requests

# Fallback curl_cffi impersonation target used when FlareSolverr is disabled or
# before credentials have been bootstrapped.  The active target is derived
# dynamically from FlareSolverr's reported user-agent so the TLS fingerprint
# always matches the browser that generated the cf_clearance cookie.
_BROWSER_IMPERSONATE_FALLBACK = "chrome142"

# Sorted list of (major_version, curl_cffi_target) for desktop Chrome targets only.
# Update when new curl_cffi releases add targets.
_CHROME_CFFI_TARGETS: List[Tuple[int, str]] = sorted([
    (99,  "chrome99"),
    (100, "chrome100"),
    (101, "chrome101"),
    (104, "chrome104"),
    (107, "chrome107"),
    (110, "chrome110"),
    (116, "chrome116"),
    (119, "chrome119"),
    (120, "chrome120"),
    (123, "chrome123"),
    (124, "chrome124"),
    (131, "chrome131"),
    (136, "chrome136"),
    (142, "chrome142"),
    (145, "chrome145"),
    (146, "chrome146"),
])


def _cffi_target_for_ua(user_agent: str) -> str:
    """Return the best curl_cffi impersonation target for a given user-agent string.

    Parses the Chrome major version and picks an exact match, falling back to the
    nearest lower version so the TLS fingerprint stays consistent with the browser
    that generated the cf_clearance cookie.
    """
    import re
    m = re.search(r"Chrome/(\d+)\.", user_agent)
    if not m:
        return _BROWSER_IMPERSONATE_FALLBACK
    version = int(m.group(1))
    # Exact match
    for v, target in _CHROME_CFFI_TARGETS:
        if v == version:
            return target
    # Nearest lower version
    lower = [(v, t) for v, t in _CHROME_CFFI_TARGETS if v < version]
    if lower:
        return lower[-1][1]
    # Nearest higher version (version is older than anything we know)
    return _CHROME_CFFI_TARGETS[0][1]

# FlareSolverr is used to solve the Cloudflare JS challenge on the first request
# of a batch, then we reuse the resulting cookies for all subsequent curl_cffi fetches.
# Set FLARESOLVERR_URL to empty string to disable and fall back to plain curl_cffi.
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191")

# cf_clearance cookies are typically valid for ~30 minutes from the same IP.
# We conservatively expire the cached credentials at 25 minutes to avoid edge cases.
_CF_SESSION_TTL = 25 * 60  # seconds

# Credentials cache: cookies + user-agent from FlareSolverr.
# Each _fetch_url call creates a fresh curl_cffi Session from these — curl_cffi
# Sessions are not thread-safe for concurrent use, so we never share Session objects.
_cf_credentials_lock = threading.Lock()
_cf_credentials: Optional[Dict[str, Any]] = None  # {"cookies": {...}, "user_agent": "..."}
_cf_credentials_expires_at: float = 0.0

# Adaptive delay for detail fetches: backs off on 403, recovers on success.
_detail_delay_lock = threading.Lock()
_detail_adaptive_delay: float = 0.0

logger = logging.getLogger("scraper")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _get_cf_credentials(url: str, timeout_s: int) -> Tuple[Optional[Dict[str, Any]], Optional[bytes], Optional[int]]:
    """
    Returns CF credentials (cookies + user-agent) needed to bypass Cloudflare.

    Two cases:
    - Cache hit (credentials still valid): returns (credentials, None, None).
      Caller must fetch url itself using a fresh Session built from credentials.
    - Cache miss (expired or first call): calls FlareSolverr, returns
      (credentials, html_bytes, http_status). Caller should use html_bytes directly
      as the artifact for url — no duplicate fetch needed.

    Returns (None, None, None) if FLARESOLVERR_URL is not configured.
    """
    global _cf_credentials, _cf_credentials_expires_at

    if not FLARESOLVERR_URL:
        return None, None, None

    with _cf_credentials_lock:
        now = time.monotonic()
        if _cf_credentials is not None and now < _cf_credentials_expires_at:
            return _cf_credentials, None, None

        # Cache miss — call FlareSolverr
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

        _cf_credentials = {"cookies": cookies, "user_agent": user_agent}
        _cf_credentials_expires_at = now + _CF_SESSION_TTL

        logger.info(
            "FlareSolverr bootstrapped CF credentials (status=%s, cookies=%s)",
            http_status,
            list(cookies.keys()),
        )
        return _cf_credentials, html, http_status


def _invalidate_cf_credentials() -> None:
    """
    Force-expire cached CF credentials so the next _get_cf_credentials call
    triggers a FlareSolverr re-bootstrap.

    Called after a 403 response to handle the case where Cloudflare invalidated
    the cf_clearance cookie before the normal 25-minute TTL.
    """
    global _cf_credentials_expires_at
    with _cf_credentials_lock:
        _cf_credentials_expires_at = 0.0
    logger.warning("CF credentials invalidated — next fetch will re-bootstrap via FlareSolverr")


def _update_detail_delay(is_403: bool) -> None:
    """
    Adjust the module-level adaptive delay based on the last fetch outcome.

    403 → back off: delay = min(max(delay * 2, 0.5), 30.0)
    success → recover: delay = max(delay * 0.85, 0.0)
    """
    global _detail_adaptive_delay
    with _detail_delay_lock:
        old = _detail_adaptive_delay
        if is_403:
            _detail_adaptive_delay = min(max(_detail_adaptive_delay * 2, 0.5), 30.0)
        else:
            _detail_adaptive_delay = max(_detail_adaptive_delay * 0.85, 0.0)
        new = _detail_adaptive_delay
    if is_403:
        logger.warning("Adaptive delay backed off: %.2fs → %.2fs (403 received)", old, new)
    elif old > 0:
        logger.info("Adaptive delay recovering: %.2fs → %.2fs (success)", old, new)


def _fetch_url(url: str, timeout_s: int) -> Tuple[bytes, int, Optional[str], str]:
    """
    Fetches url using a Cloudflare-bootstrapped curl_cffi session (if FLARESOLVERR_URL
    is set), or a plain curl_cffi session as fallback.

    A fresh Session is created per call from the cached credentials so concurrent
    callers never share a Session object (curl_cffi Sessions are not thread-safe).

    Returns (content_bytes, http_status, content_type, final_url).
    """
    if FLARESOLVERR_URL:
        try:
            credentials, bootstrap_html, bootstrap_status = _get_cf_credentials(url, timeout_s)
            if bootstrap_html is not None:
                # FlareSolverr fetched this URL for us — reuse the response
                return bootstrap_html, bootstrap_status, "text/html; charset=utf-8", url
            # Cache hit — build a fresh session from shared credentials
            impersonate = _cffi_target_for_ua(credentials["user_agent"]) if credentials else _BROWSER_IMPERSONATE_FALLBACK
            session = cf_requests.Session(impersonate=impersonate)
            if credentials:
                session.headers.update({"User-Agent": credentials["user_agent"]})
                for name, value in credentials["cookies"].items():
                    session.cookies.set(name, value)
            resp = session.get(url, timeout=timeout_s, allow_redirects=True)
            content = resp.content or b""
            return content, resp.status_code, resp.headers.get("content-type"), str(resp.url)
        except Exception as e:
            logger.warning("FlareSolverr/CF session failed (%s), falling back to plain curl_cffi", e)

    # Plain curl_cffi fallback (no FlareSolverr)
    session = cf_requests.Session(impersonate=_BROWSER_IMPERSONATE_FALLBACK)
    resp = session.get(url, timeout=timeout_s, allow_redirects=True)
    content = resp.content or b""
    return content, resp.status_code, resp.headers.get("content-type"), str(resp.url)


def scrape_detail_fetch(*, run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Production detail-page scraper processor.

    Inputs (payload):
      listing_id: required
      batch_id: optional UUID identifying the scrape batch; used as search_key in
                artifacts so each scrape_jobs row maps 1:1 to its artifacts.
                Defaults to run_id if omitted.
      url: optional (defaults to https://www.cars.com/vehicledetail/<listing_id>/)
      vin: optional
      timeout_s: optional
      headers: optional override headers dict

    Returns:
      { "artifacts": [...], "meta": {...}, "error": None|str }
    """
    listing_id = (payload or {}).get("listing_id")
    vin = (payload or {}).get("vin")
    batch_id = (payload or {}).get("batch_id") or run_id
    url = (payload or {}).get("url") or (f"https://www.cars.com/vehicledetail/{listing_id}/" if listing_id else None)

    logger.info(
        "scrape_detail_fetch: listing_id=%s run_id=%s payload_batch_id=%s resolved_batch_id=%s",
        listing_id, run_id, (payload or {}).get("batch_id"), batch_id,
    )

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

        if status != 200:
            logger.warning(
                "detail fetch HTTP %s for listing_id=%s url=%s",
                status, listing_id, final_url,
            )

        artifact = {
            "source": "cars.com",
            "artifact_type": "detail_page",
            "listing_id": listing_id,
            "search_key": batch_id,
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
                    "listing_id": listing_id,
                    "search_key": batch_id,
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


def scrape_detail_batch(
    *, run_id: str, batch_id: str, listings: List[Dict[str, Any]], max_workers: int = 8
) -> Dict[str, Any]:
    """
    Fetch detail pages for a list of listings concurrently.

    Uses a thread pool with an adaptive per-request delay that backs off when
    403 responses are detected and gradually recovers on success. On each 403,
    the cached CF credentials are also invalidated so the next request triggers
    a fresh FlareSolverr bootstrap.

    batch_id: UUID identifying this batch; written as search_key on every artifact
              so scrape_jobs rows map 1:1 to their artifacts.
    listings: [{"listing_id": ..., "vin": ..., "url": ...}, ...]
    Returns: {"artifacts": [...], "meta": {...}}
    """

    def _fetch_one(item: Dict[str, Any]) -> Dict[str, Any]:
        with _detail_delay_lock:
            delay = _detail_adaptive_delay
        if delay > 0:
            logger.info(
                "Adaptive delay %.2fs before fetching listing_id=%s",
                delay, item.get("listing_id"),
            )
            time.sleep(delay)
        result = scrape_detail_fetch(run_id=run_id, payload={**item, "batch_id": batch_id})
        is_403 = any(a.get("http_status") == 403 for a in result.get("artifacts", []))
        _update_detail_delay(is_403)
        if is_403:
            _invalidate_cf_credentials()
        return result

    all_artifacts: List[Dict[str, Any]] = []
    error_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, listing): listing for listing in listings}
        for future in as_completed(futures):
            try:
                result = future.result()
                all_artifacts.extend(result.get("artifacts", []))
                if result.get("error"):
                    error_count += 1
            except Exception as e:
                logger.warning("Detail batch fetch raised: %s", e)
                error_count += 1

    return {
        "artifacts": all_artifacts,
        "meta": {
            "mode": "batch",
            "total": len(listings),
            "succeeded": len(listings) - error_count,
            "errors": error_count,
        },
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
    batch_id = (payload or {}).get("batch_id") or run_id
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
            "listing_id": listing_id,
            "search_key": batch_id,
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
                    "listing_id": listing_id,
                    "search_key": batch_id,
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
