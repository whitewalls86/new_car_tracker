import hashlib
import html as html_lib
import json
import math
import os
import random
import re
import time
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from fastapi import Body

from scraper.processors.browser import close_browser, get_context
from scraper.processors.fingerprint import human_delay, random_profile, random_zip

RAW_BASE = "/data/raw"
BASE_URL = "https://www.cars.com/shopping/results/"  # adjust if your real base differs
_SITE_ACTIVITY_RE = re.compile(r'data-site-activity="([^"]+)"')
_VIN_RE = re.compile(r'"vin"\s*:\s*"([A-HJ-NPR-Z0-9]{17})"')


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


def _fetch_page(context, url: str, run_dir: str,
                search_key: str, scope: str, page_num: int,
                known_vins: Set[str]) -> Dict[str, Any]:
    """Fetch a single SRP page using the shared browser context.

    The context is shared across all pages in a session so cookies (including
    cf_clearance) persist between requests.

    Returns an artifact dict plus extra keys used by the caller:
      - _paging: parsed paging metadata (or None)
      - _page_vins: set of VINs found on this page
      - _new_vins_count: number of previously-unseen VINs
      - _stop: True if pagination should stop after this page
      - _break_no_save: True if page should be discarded (duplicate clamp)
    """
    fetched_at = datetime.now(UTC).isoformat()

    page = context.new_page()
    try:
        response = page.goto(url, timeout=30000, wait_until="networkidle")
        # Wait for SRP cards to render (Akamai JS challenge + React hydration)
        try:
            page.wait_for_selector("spark-card[data-vehicle-details]", timeout=10000)
        except Exception:
            pass  # 403 or empty results — still save whatever's on the page

        status = response.status if response else 0
        content_type = response.headers.get("content-type") if response else None
        html_text = page.content()
        content = html_text.encode("utf-8")
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
        return artifact

    except Exception as e:
        error_filepath = os.path.join(
            run_dir, 
            f"{search_key}__{scope}__page_{page_num:04d}__ERROR.txt"
        )
        return {
            "source": "cars.com",
            "artifact_type": "results_page",
            "search_key": search_key,
            "search_scope": scope,
            "page_num": page_num,
            "url": url,
            "http_status": None,
            "content_type": None,
            "content_bytes": None,
            "sha256": None,
            "filepath": error_filepath,
            "fetched_at": fetched_at,
            "error": f"{type(e).__name__}: {str(e)}"[:500].replace("'", ""),
            "paging_meta": None,
            "page_vins_total": 0,
            "page_vins_new": 0,
            "_paging": None,
            "_page_vins": set(),
            "_new_vins_count": 0,
            "_stop": True,  # stop on error — don't burn IP
            "_break_no_save": False,
        }
    finally:
        page.close()


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
    # Use pool from params if provided, otherwise use defaults
    zip_code = random_zip(scope)

    # directory per run for organization
    run_dir = os.path.join(RAW_BASE, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    # --- Pick a consistent fingerprint for this entire session ---
    profile = random_profile()

    artifacts: List[Dict[str, Any]] = []
    context = get_context(profile)

    try:
        # === Phase 0: Warmup ===
        warmup = context.new_page()
        warmup.goto("https://www.cars.com/", wait_until="networkidle", timeout=30000)
        time.sleep(human_delay(0))
        warmup.close()

        # === Phase 1: Fetch page 1 to learn total page count ===
        time.sleep(human_delay(1))

        url_p1 = build_results_url(makes, models, zip_code, scope, radius_miles, 1, sort_order)
        result_p1 = _fetch_page(context, url_p1, run_dir,
                                search_key, scope, 1, known_vins)

        if result_p1["_break_no_save"]:
            return {"run_id": run_id, "search_key": search_key,
                    "scope": scope, "artifacts": [], "page_1_blocked": False}

        artifacts.append(_clean_artifact(result_p1))

        if result_p1["_stop"]:
            page_1_blocked = result_p1.get("http_status") == 403
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

        if page_count <= 1:
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

        for page_num in remaining_pages:
            time.sleep(human_delay(page_num))

            url = build_results_url(makes, models, zip_code, scope,
                                    radius_miles, page_num, sort_order)
            result = _fetch_page(context, url, run_dir,
                                 search_key, scope, page_num, known_vins)

            if result["_break_no_save"]:
                # Cars.com clamped page — we've gone past the end
                break

            artifacts.append(_clean_artifact(result))

            if result["_stop"]:
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
                        break

    finally:
        close_browser()

    return {"run_id": run_id, "search_key": search_key, "scope": scope,
            "artifacts": artifacts, "page_1_blocked": False}
