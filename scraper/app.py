from fastapi import FastAPI, Body
from typing import Any, Dict, Optional
import os
from processors.scrape_results import scrape_results
from processors.results_page_cards import (parse_cars_results_page_html, parse_cars_results_page_html_v2)
from processors.scrape_detail import (scrape_detail_dummy, scrape_detail_fetch)
from processors.parse_detail_page import parse_cars_detail_page_html_v1

app = FastAPI()


@app.post("/scrape_results")
def run_scrape_results(
    run_id: str,
    search_key: str,
    scope: str,                   # "national" or "local"
    payload: dict = Body(...),
) -> Dict[str, Any]:
    """
    Fetches results pages for one (search_key, scope), saves raw HTML to disk,
    and returns artifact metadata for n8n to write to Postgres.
    """
    return scrape_results(run_id, search_key, scope, payload)


@app.post("/process/results_pages")
def process_results_pages(payload: dict = Body(...)) -> Dict[str, Any]:
    processor = (payload or {}).get("processor") or "cars_results_page__listings_v1"
    artifact = (payload or {}).get("artifact") or {}
    options = (payload or {}).get("options") or {}

    # --- Validate/coerce artifact_id ---
    raw_id = artifact.get("artifact_id")
    try:
        artifact_id = int(raw_id)
    except (TypeError, ValueError):
        return {
            "processor": processor,
            "artifact_id": raw_id,
            "status": "failed",
            "message": f"artifact_id must be an int or int-like string, got {raw_id!r}",
            "meta": {"raw_artifact_id": raw_id},
            "listings": [],
        }

    # --- Testing override (optional) ---
    force_status: Optional[str] = options.get("force_status")
    if force_status in ("ok", "skipped", "retry", "failed") and force_status != "ok":
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "status": force_status,
            "message": f"forced status: {force_status}",
            "meta": {"forced": True, "force_status": force_status},
            "listings": [],
        }

    # --- Read HTML artifact from disk ---
    filepath = artifact.get("filepath")
    if not filepath:
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "status": "failed",
            "message": "artifact.filepath is required",
            "meta": {},
            "listings": [],
        }

    if not os.path.exists(filepath):
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "status": "failed",
            "message": f"artifact file not found: {filepath}",
            "meta": {"filepath": filepath},
            "listings": [],
        }

    try:
        with open(filepath, "rb") as f:
            html = f.read().decode("utf-8", errors="replace")
    except Exception as e:
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "status": "failed",
            "message": "failed to read artifact file",
            "meta": {
                "filepath": filepath,
                "error": type(e).__name__,
                "error_message": str(e),
            },
            "listings": [],
        }

    # --- Parse ---
    try:
        if processor == "cars_results_page__listings_v1":
            listings, parse_meta = parse_cars_results_page_html(html)
        elif processor == "cars_results_page__listings_v2":
            listings, parse_meta = parse_cars_results_page_html_v2(html)
        else:
            return {
                "processor": processor,
                "artifact_id": artifact_id,
                "status": "failed",
                "message": "results page parsing failed",
                "meta": {
                    "filepath": filepath,
                    "html_len": len(html),
                    "error": "Invalid Processor",
                    "error_message": "Please use a valid processor.",
                },
                "listings": [],
            }
    except Exception as e:
        # If you prefer transient behavior, change status to "retry"
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "status": "failed",
            "message": "results page parsing failed",
            "meta": {
                "filepath": filepath,
                "html_len": len(html),
                "error": type(e).__name__,
                "error_message": str(e),
            },
            "listings": [],
        }

    return {
        "processor": processor,
        "artifact_id": artifact_id,
        "status": "ok",
        "message": f"parsed {len(listings)} listings",
        "meta": parse_meta,
        "listings": listings,
    }


@app.post("/scrape_detail")
def scrape_detail(run_id: str, payload: dict = Body(...)) -> Dict[str, Any]:
    mode = (payload or {}).get("mode") or "fetch"

    if mode == "dummy":
        return scrape_detail_dummy(run_id=run_id, payload=payload)

    if mode == "fetch":
        return scrape_detail_fetch(run_id=run_id, payload=payload)

    return {
        "error": f"unsupported mode: {mode}",
        "artifacts": [],
        "meta": {"mode": mode},
    }


@app.post("/process/detail_pages")
def process_detail_pages(payload: dict = Body(...)) -> Dict[str, Any]:
    processor = (payload or {}).get("processor") or "cars_detail_page__v1"
    artifact = (payload or {}).get("artifact") or {}
    options = (payload or {}).get("options") or {}
    search_key = (artifact or {}).get("search_key") or (payload or {}).get("search_key") or None
    artifact_url = (artifact or {}).get("url") or (payload or {}).get("url") or None

    # --- Validate/coerce artifact_id ---
    raw_id = artifact.get("artifact_id")
    try:
        artifact_id = int(raw_id)
    except (TypeError, ValueError):
        return {
            "processor": processor,
            "artifact_id": raw_id,
            "search_key": search_key,
            "status": "failed",
            "message": f"artifact_id must be an int or int-like string, got {raw_id!r}",
            "meta": {"raw_artifact_id": raw_id},
            "primary": {},
            "carousel": [],
        }

    # --- Testing override (optional) ---
    force_status: Optional[str] = options.get("force_status")
    if force_status in ("ok", "skipped", "retry", "failed") and force_status != "ok":
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "search_key": search_key,
            "status": force_status,
            "message": f"forced status: {force_status}",
            "meta": {"forced": True, "force_status": force_status},
            "primary": {},
            "carousel": [],
        }

    # --- Read HTML artifact from disk ---
    filepath = artifact.get("filepath")
    if not filepath:
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "search_key": search_key,
            "status": "failed",
            "message": "artifact.filepath is required",
            "meta": {},
            "primary": {},
            "carousel": [],
        }

    if not os.path.exists(filepath):
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "search_key": search_key,
            "status": "failed",
            "message": f"artifact file not found: {filepath}",
            "meta": {"filepath": filepath},
            "primary": {},
            "carousel": [],
        }

    try:
        with open(filepath, "rb") as f:
            html = f.read().decode("utf-8", errors="replace")
    except Exception as e:
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "search_key": search_key,
            "status": "failed",
            "message": "failed to read artifact file",
            "meta": {
                "filepath": filepath,
                "error": type(e).__name__,
                "error_message": str(e),
            },
            "primary": {},
            "carousel": [],
        }

    # --- Parse ---
    try:
        if processor == "cars_detail_page__v1":
            primary, carousel, parse_meta = parse_cars_detail_page_html_v1(html, artifact_url)
        else:
            return {
                "processor": processor,
                "artifact_id": artifact_id,
                "search_key": search_key,
                "status": "failed",
                "message": "detail page parsing failed",
                "meta": {"error": "Invalid Processor", "error_message": "Please use a valid processor."},
                "primary": {},
                "carousel": [],
            }
    except Exception as e:
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "search_key": search_key,
            "status": "failed",
            "message": "detail page parsing failed",
            "meta": {
                "filepath": filepath,
                "html_len": len(html),
                "error": type(e).__name__,
                "error_message": str(e),
            },
            "primary": {},
            "carousel": [],
        }

    parse_meta = {**(parse_meta or {}), "artifact_url": artifact_url}

    return {
        "processor": processor,
        "artifact_id": artifact_id,
        "search_key": search_key,
        "status": "ok",
        "message": f"parsed primary + {len(carousel)} carousel items",
        "meta": parse_meta,
        "primary": primary,
        "carousel": carousel,
    }
