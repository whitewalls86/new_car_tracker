from fastapi import FastAPI, Body
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional
import os
import hashlib
import json
import html as html_lib
import math
import re

import time
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from processors.browser import get_browser, close_browser


RAW_BASE = "/data/raw"
BASE_URL = "https://www.cars.com/shopping/results/"  # adjust if your real base differs
_SITE_ACTIVITY_RE = re.compile(r'data-site-activity="([^"]+)"')


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def build_results_url(makes: List[str], models: List[str], zip_code: str, scope: str,
                      radius_miles: int, page_num: int, page_size: int) -> str:
    params = {
        "makes[]": makes,
        "models[]": models,
        "stock_type": "new",              # keep as-is or make configurable later
        "zip": zip_code,
        "page": page_num,
        "page_size": page_size,
        "maximum_distance": radius_miles if scope == "local" else "all",
    }
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
                m2 = re.search(r'from\s+([\d,]+)\s+.+?\s+models?\s+in', meta_desc.get("content", ""), re.IGNORECASE)
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
    zip_code = params.get("zip")
    radius_miles = int(params.get("radius_miles", 200))
    page_size = int(params.get("page_size", 100))

    max_listings = int(params.get("max_listings", 2000))
    max_safety_pages = int(params.get("max_safety_pages", 500))

    if scope not in ("national", "local"):
        return {"error": f"Invalid scope '{scope}'", "artifacts": []}

    if not makes or not models or not zip_code:
        return {"error": "Missing makes/models/zip in params", "artifacts": []}

    # directory per run for organization
    run_dir = os.path.join(RAW_BASE, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    artifacts: List[Dict[str, Any]] = []
    browser = get_browser()

    try:
        for page_num in range(1, max_safety_pages + 1):
            if page_num > 1:
                time.sleep(3)
            url = build_results_url(makes, models, zip_code, scope, radius_miles, page_num, page_size)
            fetched_at = datetime.now(UTC).isoformat()

            # Fresh context per page — each page gets a new browser session so
            # Akamai re-evaluates the JS challenge instead of blocking a flagged session.
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            page = context.new_page()
            try:
                response = page.goto(url, timeout=30000, wait_until="domcontentloaded")
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

                # --- decide whether to stop early based on embedded paging meta ---
                paging = None
                stop_after_this_page = False
                if status == 200 and content:
                    paging = extract_results_paging_meta(html_text)
                    if paging:
                        actual_page = paging.get("result_page_number")
                        page_count = paging.get("result_page_count")

                        # If Cars.com clamps you to a different page than requested,
                        # you're in duplicate territory. Stop and DON'T save this artifact.
                        if actual_page is not None and actual_page != page_num:
                            break

                        if actual_page is not None and page_count is not None and actual_page >= page_count:
                            stop_after_this_page = True

                        # Stop if we got zero cards (empty page = past the last real page).
                        cards_on_page = paging.get("result_per_page") or 0
                        if cards_on_page == 0:
                            stop_after_this_page = True

                        # Cap at max_listings if total_listings exceeds it — prevents
                        # pulling hundreds of pages for large national searches.
                        total_listings = paging.get("total_listings") or 0
                        if total_listings > max_listings and cards_on_page > 0:
                            collected = page_num * cards_on_page
                            if collected >= max_listings:
                                stop_after_this_page = True

                # save raw html (only if we decided to keep it)
                filename = f"{search_key}__{scope}__page_{page_num:04d}__{status}.html"
                filepath = os.path.join(run_dir, filename)
                with open(filepath, "wb") as f:
                    f.write(content)

                artifacts.append({
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
                })

                if stop_after_this_page:
                    break

            except Exception as e:
                artifacts.append({
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
                    "filepath": os.path.join(run_dir, f"{search_key}__{scope}__page_{page_num:04d}__ERROR.txt"),
                    "fetched_at": fetched_at,
                    "error": f"{type(e).__name__}: {str(e)}"[:500].replace("'", ""),
                })
            finally:
                context.close()
    finally:
        close_browser()

    return {"run_id": run_id, "search_key": search_key, "scope": scope, "artifacts": artifacts}
