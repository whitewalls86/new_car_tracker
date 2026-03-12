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
import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup


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

    Supports both formats:
      - Legacy (pre-Jan 2026): data-site-activity JSON blob with result_page_number etc.
      - srp2025 (post-Jan 2026): spark-card[data-vehicle-details] per-card JSON with
        metadata.page_number and metadata.position_on_page.

    Returns:
      {
        "total_results": int|None,
        "result_per_page": int|None,
        "result_page_number": int|None,
        "result_page_count": int|None,   # derived if missing
      }
    or None if not found / parse fails.
    """
    def to_int(x):
        try:
            return int(x)
        except Exception:
            return None

    # --- Try legacy data-site-activity first ---
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
                    "total_results": total_results,
                    "result_per_page": per_page,
                    "result_page_number": page_number,
                    "result_page_count": page_count,
                }
        except Exception:
            pass

    # --- Fall back to srp2025 spark-card format ---
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

        # total_results / page_count: look in a nearby pagination element if present
        total_results = None
        page_count = None
        pagination_el = soup.select_one("[data-total-result-count]")
        if pagination_el:
            total_results = to_int(pagination_el.get("data-total-result-count"))
        if total_results is None:
            # Try a text pattern like "1-100 of 1,234 results"
            m2 = re.search(r'of\s+([\d,]+)\s+result', html_text, re.IGNORECASE)
            if m2:
                total_results = to_int(m2.group(1).replace(",", ""))

        per_page = len(cards) if cards else None
        if page_count is None and total_results and per_page:
            page_count = int(math.ceil(total_results / per_page))

        return {
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

    max_pages_cfg = params.get("max_pages", {})
    max_pages = int(max_pages_cfg.get(scope, 1))

    if scope not in ("national", "local"):
        return {"error": f"Invalid scope '{scope}'", "artifacts": []}

    if not makes or not models or not zip_code:
        return {"error": "Missing makes/models/zip in params", "artifacts": []}

    # directory per run for organization
    run_dir = os.path.join(RAW_BASE, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    artifacts: List[Dict[str, Any]] = []
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
    }

    for page_num in range(1, max_pages + 1):
        if page_num > 1:
            time.sleep(3)
        url = build_results_url(makes, models, zip_code, scope, radius_miles, page_num, page_size)
        fetched_at = datetime.now(UTC).isoformat()

        try:
            resp = session.get(url, headers=headers, timeout=30)
            status = resp.status_code
            content_type = resp.headers.get("content-type")
            content = resp.content or b""
            size = len(content)

            # --- NEW: decide whether to stop early based on embedded paging meta ---
            paging = None
            stop_after_this_page = False
            if status == 200 and content:
                try:
                    text = content.decode(resp.encoding or "utf-8", errors="replace")
                except Exception:
                    text = content.decode("utf-8", errors="replace")

                paging = extract_results_paging_meta(text)
                if paging:
                    actual_page = paging.get("result_page_number")
                    page_count = paging.get("result_page_count")

                    # If Cars.com clamps you to a different page than requested,
                    # you're in duplicate territory. Stop and DON'T save this artifact.
                    if actual_page is not None and actual_page != page_num:
                        break

                    if actual_page is not None and page_count is not None and actual_page >= page_count:
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
                # --- optional: include paging meta for debugging / auditing ---
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
                "error": repr(e),
            })

    return {"run_id": run_id, "search_key": search_key, "scope": scope, "artifacts": artifacts}
