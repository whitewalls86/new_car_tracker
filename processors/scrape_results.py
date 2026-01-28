from fastapi import FastAPI, Body
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional
import os
import hashlib
import json
import html as html_lib
import math
import re

import requests
from urllib.parse import urlencode


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

    Returns:
      {
        "total_results": int|None,
        "result_per_page": int|None,
        "result_page_number": int|None,
        "result_page_count": int|None,   # derived if missing
      }
    or None if not found / parse fails.
    """
    m = _SITE_ACTIVITY_RE.search(html_text)
    if not m:
        return None

    raw = m.group(1)
    try:
        decoded = html_lib.unescape(raw)
        obj = json.loads(decoded)

        total_results = obj.get("total_results")
        # note: Cars.com uses result_per_page (singular) in this blob
        per_page = obj.get("result_per_page") or obj.get("results_per_page")
        page_number = obj.get("result_page_number") or obj.get("results_page_number")
        page_count = obj.get("result_page_count") or obj.get("results_page_count")

        # normalize ints where possible
        def to_int(x):
            try:
                return int(x)
            except Exception:
                return None

        total_results = to_int(total_results)
        per_page = to_int(per_page)
        page_number = to_int(page_number)
        page_count = to_int(page_count)

        # derive page_count if missing
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
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.1047.1013 Mobile Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for page_num in range(1, max_pages + 1):
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
