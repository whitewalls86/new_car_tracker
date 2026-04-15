import hashlib
import html as html_lib
import json
import logging
import math
import os
import random
import re
import threading
import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from fastapi import Body

from scraper.processors.cf_session import (
    get_cf_credentials,
    invalidate_cf_credentials,
    make_cf_session,
)
from scraper.processors.fingerprint import human_delay, random_zip

logger = logging.getLogger(__name__)

RAW_BASE = "/data/raw"
BASE_URL = "https://www.cars.com/shopping/results/"
_SITE_ACTIVITY_RE = re.compile(r'data-site-activity="([^"]+)"')
_VIN_RE = re.compile(r'"vin"\s*:\s*"([A-HJ-NPR-Z0-9]{17})"')

JOB_TIMEOUT_S = 40 * 60  # 40 minutes hard limit per job

# ---------------------------------------------------------------------------
# Process-wide adaptive penalty (shared across all concurrent search jobs)
# ---------------------------------------------------------------------------
# On 403: penalty doubles (min 45s, max 120s)
# On success: penalty recovers 10% per page
# All 4 concurrent jobs share the same IP/cookies so a block affects all of them.
_srp_penalty_lock = threading.Lock()
_srp_adaptive_penalty: float = 0.0


def _update_srp_penalty(is_403: bool) -> float:
    """Update and return the process-wide SRP adaptive penalty."""
    global _srp_adaptive_penalty
    with _srp_penalty_lock:
        old = _srp_adaptive_penalty
        if is_403:
            _srp_adaptive_penalty = min(max(_srp_adaptive_penalty * 2, 45.0), 120.0)
        else:
            _srp_adaptive_penalty = max(_srp_adaptive_penalty * 0.90, 0.0)
        new = _srp_adaptive_penalty
    if is_403:
        logger.warning("SRP adaptive penalty backed off: %.1fs → %.1fs (403)", old, new)
    elif old > 0:
        logger.info("SRP adaptive penalty recovering: %.1fs → %.1fs (success)", old, new)
    return new


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def build_results_url(makes: List[str], models: List[str], zip_code: str, scope: str,
                      radius_miles: int, page_num: int,
                      sort_order: Optional[str] = None) -> str:
    params = {
        "makes[]": makes,
        "models[]": models,
        "stock_type": "new",              # keep as-is or make configurable later
        "zip": zip_code,
        "page": page_num,
        "page_size": 100,  # Cars.com ignores this (returns ~22/page)
                           # but we send it for API compat
        "maximum_distance": radius_miles if scope == "local" else "all",
    }
    if sort_order:
        params["sort"] = sort_order
    return BASE_URL + "?" + urlencode(params, doseq=True)


def extract_results_paging_meta(html_text: str) -> Optional[Dict[str, Any]]:
    """
    Extract paging metadata from Cars.com results HTML.

    Source priority:
      0. CarsWeb.SearchController.index script (srp2025, most reliable)
      1. Legacy data-site-activity JSON blob (pre-Jan 2026)
      2. srp2025 spark-card[data-vehicle-details] per-card JSON (fallback)

    Returns:
      {
        "total_listings": int|None,   # raw count from Cars.com API (caps at 10k)
        "total_results": int|None,    # true listing count where available
        "result_per_page": int|None,
        "result_page_number": int|None,
        "result_page_count": int|None,
      }
    or None if not found / parse fails.
    """
    def to_int(x):
        try:
            return int(x)
        except Exception:
            return None

    # --- Source 0: CarsWeb.SearchController.index (srp2025 primary) ---
    try:
        soup = BeautifulSoup(html_text, "lxml")
        ctrl_tag = soup.find("script", id="CarsWeb.SearchController.index")
        if ctrl_tag and ctrl_tag.string and ctrl_tag.string.strip() not in ("", "null"):
            ctrl = json.loads(ctrl_tag.string)
            meta = ctrl.get("srp_results", {}).get("metadata", {})
            page_number = to_int(meta.get("page"))
            if page_number is not None:
                return {
                    "total_listings": to_int(meta.get("total_listings")),
                    "total_results": to_int(meta.get("total_listings")),
                    "result_per_page": to_int(meta.get("page_size")),
                    "result_page_number": page_number,
                    "result_page_count": to_int(meta.get("total_pages")),
                }
    except Exception:
        pass

    # --- Source 1: Legacy data-site-activity ---
    m = _SITE_ACTIVITY_RE.search(html_text)
    if m:
        try:
            decoded = html_lib.unescape(m.group(1))
            obj = json.loads(decoded)

            total_results = to_int(obj.get("total_results"))
            per_page = to_int(obj.get("result_per_page") or obj.get("results_per_page"))
            page_number = to_int(obj.get("result_page_number") or obj.get("results_page_number"))
            page_count = to_int(obj.get("result_page_count") or obj.get("results_page_count"))

            if page_count is None and total_results and per_page:
                page_count = int(math.ceil(total_results / per_page))

            if page_number is not None:
                return {
                    "total_listings": total_results,
                    "total_results": total_results,
                    "result_per_page": per_page,
                    "result_page_number": page_number,
                    "result_page_count": page_count,
                }
        except Exception:
            pass

    # --- Source 2: srp2025 spark-card format (fallback) ---
    try:
        soup = BeautifulSoup(html_text, "lxml")
        cards = soup.select("spark-card[data-vehicle-details]")
        if not cards:
            return None

        # page_number is the same on every card — read from the first one
        raw = cards[0].get("data-vehicle-details") or ""
        v = json.loads(raw)
        metadata = v.get("metadata") or {}
        page_number = to_int(metadata.get("page_number"))
        if page_number is None:
            return None

        # total_results / page_count: try several sources in order of reliability
        total_results = None
        page_count = None

        # 1. data-total-result-count attribute on a pagination element
        pagination_el = soup.select_one("[data-total-result-count]")
        if pagination_el:
            total_results = to_int(pagination_el.get("data-total-result-count"))

        # 2. Meta description: "from 414 Escape models in Houston, TX" (SSR, always present)
        if total_results is None:
            meta_desc = soup.select_one('meta[name="description"]')
            if meta_desc:
                m2 = re.search(
                        r'from\s+([\d,]+)\s+.+?\s+models?\s+in', 
                        meta_desc.get("content", ""), 
                        re.IGNORECASE
                    )
                if m2:
                    total_results = to_int(m2.group(1).replace(",", ""))

        # 3. Legacy text pattern "1-100 of 1,234 results"
        if total_results is None:
            m3 = re.search(r'of\s+([\d,]+)\s+result', html_text, re.IGNORECASE)
            if m3:
                total_results = to_int(m3.group(1).replace(",", ""))

        per_page = len(cards) if cards else None
        if page_count is None and total_results and per_page:
            page_count = int(math.ceil(total_results / per_page))

        return {
            "total_listings": total_results,
            "total_results": total_results,
            "result_per_page": per_page,
            "result_page_number": page_number,
            "result_page_count": page_count,
        }
    except Exception:
        return None


def _fetch_page(url: str, run_dir: str,
                search_key: str, scope: str, page_num: int,
                known_vins: Set[str]) -> Dict[str, Any]:
    """Fetch a single SRP page using a CF-bootstrapped curl_cffi session.

    Retries once on 403 (after re-bootstrapping CF credentials) or on transient
    errors (after a 10s pause). Updates the process-wide adaptive penalty.

    Returns an artifact dict plus extra keys used by the caller:
      - _paging: parsed paging metadata (or None)
      - _page_vins: set of VINs found on this page
      - _new_vins_count: number of previously-unseen VINs
      - _stop: True if pagination should stop after this page
      - _break_no_save: True if page should be discarded (duplicate clamp)
      - _is_error: True if this page should count toward consecutive_errors
    """
    fetched_at = datetime.now(UTC).isoformat()

    def _do_fetch() -> tuple[bytes, int, Optional[str]]:
        """Single fetch attempt. Returns (content, status, content_type)."""
        credentials, _, _ = get_cf_credentials("https://www.cars.com/", 60)
        session = make_cf_session(credentials)
        resp = session.get(url, timeout=30, allow_redirects=True)
        content = resp.content or b""
        return content, resp.status_code, resp.headers.get("content-type")

    def _error_artifact(err_msg: str) -> Dict[str, Any]:
        error_filepath = os.path.join(
            run_dir, f"{search_key}__{scope}__page_{page_num:04d}__ERROR.txt"
        )
        return {
            "source": "cars.com", "artifact_type": "results_page",
            "search_key": search_key, "search_scope": scope,
            "page_num": page_num, "url": url,
            "http_status": None, "content_type": None, "content_bytes": None,
            "sha256": None, "filepath": error_filepath, "fetched_at": fetched_at,
            "error": err_msg[:500].replace("'", ""),
            "paging_meta": None, "page_vins_total": 0, "page_vins_new": 0,
            "_paging": None, "_page_vins": set(), "_new_vins_count": 0,
            "_stop": True, "_break_no_save": False, "_is_error": True,
        }

    # --- Attempt 1 ---
    try:
        content, status, content_type = _do_fetch()
    except Exception as e:
        logger.warning("page %d fetch error (attempt 1): %s — retrying in 10s", page_num, e)
        time.sleep(10)
        try:
            content, status, content_type = _do_fetch()
        except Exception as e2:
            _update_srp_penalty(is_403=False)
            return _error_artifact(f"{type(e2).__name__}: {e2}")

    # --- 403 handling: invalidate + one retry ---
    if status == 403:
        logger.warning("page %d got 403 (attempt 1) — invalidating CF credentials and retrying",
                       page_num)
        invalidate_cf_credentials()
        _update_srp_penalty(is_403=True)
        try:
            content, status, content_type = _do_fetch()
        except Exception as e:
            return _error_artifact(f"{type(e).__name__}: {e}")
        if status == 403:
            logger.warning("page %d still 403 after re-bootstrap", page_num)
            return _error_artifact("HTTP 403")

    # --- Success path ---
    _update_srp_penalty(is_403=False)

    html_text = content.decode("utf-8", errors="replace")
    size = len(content)

    # --- paging metadata ---
    paging = None
    stop = False
    break_no_save = False

    if status != 200:
        stop = True

    if status == 200 and content:
        paging = extract_results_paging_meta(html_text)
        if paging:
            actual_page = paging.get("result_page_number")
            page_count = paging.get("result_page_count")

            # Cars.com clamped to a different page → duplicate territory
            if actual_page is not None and actual_page != page_num:
                break_no_save = True

            if actual_page is not None and page_count is not None and actual_page >= page_count:
                stop = True

            cards_on_page = paging.get("result_per_page") or 0
            if cards_on_page == 0:
                stop = True

    # Save raw HTML
    filename = f"{search_key}__{scope}__page_{page_num:04d}__{status}.html"
    filepath = os.path.join(run_dir, filename)
    with open(filepath, "wb") as f:
        f.write(content)

    # --- VIN extraction ---
    # Cars.com now HTML-encodes JSON in data attributes (&quot; instead of ")
    # so unescape before running the regex.
    page_vins: Set[str] = set()
    new_vins_count = 0
    if status == 200:
        unescaped = html_lib.unescape(html_text)
        for vin_match in _VIN_RE.finditer(unescaped):
            page_vins.add(vin_match.group(1))

    if known_vins and page_vins:
        new_vins = page_vins - known_vins
        new_vins_count = len(new_vins)
        known_vins.update(page_vins)

    artifact = {
        "source": "cars.com",
        "artifact_type": "results_page",
        "search_key": search_key,
        "search_scope": scope,
        "page_num": page_num,
        "url": url,
        "http_status": status,
        "content_type": content_type,
        "content_bytes": size,
        "sha256": sha256_bytes(content) if content else None,
        "filepath": filepath,
        "fetched_at": fetched_at,
        "error": None if status == 200 else f"HTTP {status}",
        "paging_meta": paging,
        "page_vins_total": len(page_vins),
        "page_vins_new": new_vins_count,
    }
    artifact["_paging"] = paging
    artifact["_page_vins"] = page_vins
    artifact["_new_vins_count"] = new_vins_count
    artifact["_stop"] = stop
    artifact["_break_no_save"] = break_no_save
    artifact["_is_error"] = status != 200
    return artifact


def _clean_artifact(artifact: Dict) -> Dict:
    """Remove internal keys before returning to caller."""
    return {k: v for k, v in artifact.items() if not k.startswith("_")}


def scrape_results(
    run_id: str,
    search_key: str,
    scope: str,                   # "national" or "local"
    payload: dict = Body(...),
) -> Dict[str, Any]:
    """
    Fetches results pages for one (search_key, scope), saves raw HTML to disk,
    and returns artifact metadata for n8n to write to Postgres.
    """
    os.makedirs(RAW_BASE, exist_ok=True)

    params = payload.get("params", {})

    makes = params.get("makes") or []
    models = params.get("models") or []
    radius_miles = int(params.get("radius_miles", 200))

    max_listings = int(params.get("max_listings", 2000))
    max_safety_pages = int(params.get("max_safety_pages", 500))
    sort_order = params.get("sort_order")  # e.g. "list_price", "listed_at_desc", etc.

    # Discovery mode: known VINs for early-stop breakpoint
    raw_known_vins = payload.get("known_vins") or []
    known_vins: Set[str] = set(raw_known_vins)

    if scope not in ("national", "local"):
        return {"error": f"Invalid scope '{scope}'", "artifacts": []}

    if not makes or not models:
        return {"error": "Missing makes/models in params", "artifacts": []}

    # --- ZIP rotation: pick a random ZIP per session ---
    zip_code = random_zip(scope)

    # directory per run for organization
    run_dir = os.path.join(RAW_BASE, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    logger.info("scrape_results start: search_key=%s scope=%s zip=%s known_vins=%d",
                search_key, scope, zip_code, len(known_vins))

    artifacts: List[Dict[str, Any]] = []
    job_deadline = time.monotonic() + JOB_TIMEOUT_S
    consecutive_errors = 0

    # === Phase 1: Fetch page 1 to learn total page count ===
    time.sleep(human_delay(1))

    url_p1 = build_results_url(makes, models, zip_code, scope, radius_miles, 1, sort_order)
    result_p1 = _fetch_page(url_p1, run_dir, search_key, scope, 1, known_vins)

    logger.info(
        "page 1: search_key=%s status=%s paging=%s vins=%d new_vins=%d stop=%s break=%s",
        search_key, result_p1.get("http_status"),
        result_p1["_paging"], len(result_p1["_page_vins"]),
        result_p1["_new_vins_count"], result_p1["_stop"], result_p1["_break_no_save"],
    )

    if result_p1["_break_no_save"]:
        logger.info("page 1 break_no_save — aborting: search_key=%s", search_key)
        return {"run_id": run_id, "search_key": search_key,
                "scope": scope, "artifacts": [], "page_1_blocked": False}

    artifacts.append(_clean_artifact(result_p1))

    if result_p1["_stop"]:
        page_1_blocked = result_p1.get("http_status") == 403
        logger.info(
            "page 1 stop — returning: search_key=%s page_1_blocked=%s",
            search_key, page_1_blocked,
        )
        return {"run_id": run_id, "search_key": search_key,
                "scope": scope, "artifacts": artifacts,
                "page_1_blocked": page_1_blocked}

    # --- Determine remaining pages ---
    paging = result_p1["_paging"]
    page_count = None
    if paging:
        page_count = paging.get("result_page_count")
        total_listings = paging.get("total_listings") or 0
        cards_on_page = paging.get("result_per_page") or 0

        # Cap pages based on max_listings
        if total_listings > max_listings and cards_on_page > 0:
            max_pages_for_listings = math.ceil(max_listings / cards_on_page)
            if page_count is not None:
                page_count = min(page_count, max_pages_for_listings)
            else:
                page_count = max_pages_for_listings

    if page_count is None:
        page_count = max_safety_pages

    page_count = min(page_count, max_safety_pages)

    logger.info("page count determined: search_key=%s page_count=%s", search_key, page_count)

    if page_count <= 1:
        logger.info("single page result — returning: search_key=%s", search_key)
        return {"run_id": run_id, "search_key": search_key,
                "scope": scope, "artifacts": artifacts, "page_1_blocked": False}

    # === Phase 2: Fetch remaining pages in randomized order ===
    remaining_pages = list(range(2, page_count + 1))
    random.shuffle(remaining_pages)

    # VIN breakpoint tracking for randomized order:
    # Instead of "3 consecutive pages with 0 new VINs", track the rolling
    # ratio of new VINs across the last N pages.
    recent_new_vin_counts: List[int] = []
    if result_p1["_new_vins_count"] is not None:
        recent_new_vin_counts.append(result_p1["_new_vins_count"])

    timed_out = False
    for page_num in remaining_pages:
        # --- Hard timeout check (before sleeping) ---
        remaining_s = job_deadline - time.monotonic()
        if remaining_s <= 0:
            logger.warning(
                "job timeout reached at page %d — returning partial results: search_key=%s",
                page_num, search_key,
            )
            timed_out = True
            break

        # --- Paced delay: human cadence + process-wide adaptive penalty ---
        with _srp_penalty_lock:
            penalty = _srp_adaptive_penalty
        delay = human_delay(page_num) + penalty
        # Never sleep past the deadline
        time.sleep(min(delay, max(remaining_s - 5, 0)))

        url = build_results_url(makes, models, zip_code, scope,
                                radius_miles, page_num, sort_order)
        result = _fetch_page(url, run_dir, search_key, scope, page_num, known_vins)

        logger.info(
            "page %d: search_key=%s status=%s vins=%d new_vins=%d stop=%s break=%s err=%s",
            page_num, search_key, result.get("http_status"),
            len(result["_page_vins"]), result["_new_vins_count"],
            result["_stop"], result["_break_no_save"], result.get("_is_error"),
        )

        # --- Consecutive error tracking ---
        if result.get("_is_error"):
            consecutive_errors += 1
            if consecutive_errors >= 3:
                logger.warning(
                    "3 consecutive errors — aborting: search_key=%s", search_key
                )
                break
        else:
            consecutive_errors = 0

        if result["_break_no_save"]:
            logger.info("page %d break_no_save — stopping: search_key=%s", page_num, search_key)
            break

        artifacts.append(_clean_artifact(result))

        if result["_stop"]:
            logger.info("page %d stop signal — stopping: search_key=%s", page_num, search_key)
            break

        # --- VIN breakpoint for randomized pages ---
        # Only use rolling average — no single-page check. With random page
        # ordering we may hit high-numbered (older) pages before low-numbered
        # ones; a single zero-new-VIN page does not mean we've caught up.
        if known_vins and result["_page_vins"]:
            recent_new_vin_counts.append(result["_new_vins_count"])

            # Keep a window of the last 5 pages
            if len(recent_new_vin_counts) > 5:
                recent_new_vin_counts.pop(0)

            # If the last 5 pages averaged < 1 new VIN each, we've
            # caught up to known inventory — stop early
            if len(recent_new_vin_counts) >= 5:
                avg_new = sum(recent_new_vin_counts) / len(recent_new_vin_counts)
                if avg_new < 1.0:
                    logger.info(
                        "VIN breakpoint triggered (avg_new=%.2f) — stopping: search_key=%s",
                        avg_new, search_key,
                    )
                    break

    logger.info("scrape_results done: search_key=%s scope=%s artifacts=%d timed_out=%s",
                search_key, scope, len(artifacts), timed_out)
    return {"run_id": run_id, "search_key": search_key, "scope": scope,
            "artifacts": artifacts, "page_1_blocked": False, "timed_out": timed_out}
