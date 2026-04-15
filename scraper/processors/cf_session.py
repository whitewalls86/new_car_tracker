"""Shared Cloudflare/FlareSolverr session utilities.

Provides a process-wide credential cache (cf_clearance cookies + user-agent)
populated via FlareSolverr. Both the detail and results scrapers import from
here so a single FlareSolverr bootstrap serves all concurrent scrape jobs.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests as stdlib_requests
from curl_cffi import requests as cf_requests

logger_name = "scraper"
logger = logging.getLogger(logger_name)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://flaresolverr:8191")

# Fallback curl_cffi impersonation target used when FlareSolverr is disabled or
# before credentials have been bootstrapped. The active target is derived
# dynamically from FlareSolverr's reported user-agent so the TLS fingerprint
# always matches the browser that generated the cf_clearance cookie.
BROWSER_IMPERSONATE_FALLBACK = "chrome142"

# cf_clearance cookies are typically valid for ~30 minutes from the same IP.
# We conservatively expire the cached credentials at 25 minutes to avoid edge cases.
_CF_SESSION_TTL = 25 * 60  # seconds

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

# ---------------------------------------------------------------------------
# Process-wide credential cache
# ---------------------------------------------------------------------------

_cf_credentials_lock = threading.Lock()
_cf_credentials: Optional[Dict[str, Any]] = None  # {"cookies": {...}, "user_agent": "..."}
_cf_credentials_expires_at: float = 0.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def cffi_target_for_ua(user_agent: str) -> str:
    """Return the best curl_cffi impersonation target for a given user-agent string.

    Parses the Chrome major version and picks an exact match, falling back to the
    nearest lower version so the TLS fingerprint stays consistent with the browser
    that generated the cf_clearance cookie.
    """
    m = re.search(r"Chrome/(\d+)\.", user_agent)
    if not m:
        return BROWSER_IMPERSONATE_FALLBACK
    version = int(m.group(1))
    for v, target in _CHROME_CFFI_TARGETS:
        if v == version:
            return target
    lower = [(v, t) for v, t in _CHROME_CFFI_TARGETS if v < version]
    if lower:
        return lower[-1][1]
    return _CHROME_CFFI_TARGETS[0][1]


def get_cf_credentials(url: str, timeout_s: int) \
        -> Tuple[Optional[Dict[str, Any]], Optional[bytes], Optional[int]]:
    """Return CF credentials (cookies + user-agent) needed to bypass Cloudflare.

    Two cases:
    - Cache hit (credentials still valid): returns (credentials, None, None).
      Caller must fetch url itself using a fresh Session built from credentials.
    - Cache miss (expired or first call): calls FlareSolverr, returns
      (credentials, html_bytes, http_status). Caller may use html_bytes directly
      as the artifact for url to avoid a duplicate fetch.

    Returns (None, None, None) if FLARESOLVERR_URL is not configured.
    """
    global _cf_credentials, _cf_credentials_expires_at

    if not FLARESOLVERR_URL:
        return None, None, None

    with _cf_credentials_lock:
        now = time.monotonic()
        if _cf_credentials is not None and now < _cf_credentials_expires_at:
            return _cf_credentials, None, None

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


def invalidate_cf_credentials() -> None:
    """Force-expire cached CF credentials so the next get_cf_credentials call
    triggers a FlareSolverr re-bootstrap.

    Call after a 403 response in case Cloudflare invalidated the cf_clearance
    cookie before the normal TTL.
    """
    global _cf_credentials_expires_at
    with _cf_credentials_lock:
        _cf_credentials_expires_at = 0.0
    logger.warning("CF credentials invalidated — next fetch will re-bootstrap via FlareSolverr")


def make_cf_session(credentials: Optional[Dict[str, Any]]) -> cf_requests.Session:
    """Build a fresh curl_cffi Session from cached CF credentials.

    curl_cffi Sessions are not thread-safe for concurrent use, so each caller
    creates its own via this helper.
    """
    impersonate = (
        cffi_target_for_ua(credentials["user_agent"])
        if credentials
        else BROWSER_IMPERSONATE_FALLBACK
    )
    session = cf_requests.Session(impersonate=impersonate)
    if credentials:
        session.headers.update({"User-Agent": credentials["user_agent"]})
        for name, value in credentials["cookies"].items():
            session.cookies.set(name, value)
    return session
