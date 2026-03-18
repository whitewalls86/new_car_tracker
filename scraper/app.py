from contextlib import asynccontextmanager
from fastapi import FastAPI, Body, HTTPException
from fastapi.staticfiles import StaticFiles
from typing import Any, Dict, List, Optional
import os
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from processors.scrape_results import scrape_results
from processors.results_page_cards import (parse_cars_results_page_html, parse_cars_results_page_html_v2, parse_cars_results_page_html_v3)
from processors.scrape_detail import (scrape_detail_dummy, scrape_detail_fetch)
from processors.parse_detail_page import parse_cars_detail_page_html_v1
from routers.admin import router as admin_router
from db import get_pool, close_pool

# ---------------------------------------------------------------------------
# In-memory job store for async SRP scraping
# ---------------------------------------------------------------------------
_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=12)


def _run_scrape_job(job_id: str, run_id: str, search_key: str, scope: str, payload: dict):
    """Runs in background thread. Updates in-memory job store."""
    # sync_playwright checks asyncio.get_event_loop().is_running().
    # uvicorn/anyio may leave the main loop visible to worker threads; give
    # this thread its own fresh (non-running) loop so Playwright is happy.
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())

    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
        _jobs[job_id]["started_at"] = __import__("datetime").datetime.utcnow().isoformat()
    try:
        result = scrape_results(run_id, search_key, scope, payload)
        artifacts = result.get("artifacts", [])
        with _jobs_lock:
            _jobs[job_id]["status"] = "completed"
            _jobs[job_id]["artifacts"] = artifacts
            _jobs[job_id]["artifact_count"] = len(artifacts)
    except Exception as e:
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = str(e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize DB pool
    await get_pool()
    yield
    # Shutdown: close DB pool
    await close_pool()


app = FastAPI(lifespan=lifespan)

# Mount admin UI
app.include_router(admin_router, prefix="/admin")


@app.post("/scrape_results")
def run_scrape_results(
    run_id: str,
    search_key: str,
    scope: str,                   # "national" or "local"
    payload: dict = Body(...),
) -> Dict[str, Any]:
    """
    Queues an async SRP scrape job. Returns job_id immediately.
    The scrape runs in a background thread; poll /scrape_results/jobs/completed
    to retrieve results.
    """
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "run_id": run_id,
            "search_key": search_key,
            "scope": scope,
            "status": "queued",
            "artifacts": [],
            "artifact_count": 0,
            "error": None,
            "started_at": None,
        }
    _executor.submit(_run_scrape_job, job_id, run_id, search_key, scope, payload)
    return {"job_id": job_id, "status": "queued"}


@app.get("/scrape_results/jobs/completed")
def get_completed_jobs() -> List[Dict[str, Any]]:
    """Returns all completed or failed jobs. Failed jobs have no artifacts but
    are included so the Job Poller can clear them from memory and mark them in DB."""
    with _jobs_lock:
        return [
            job for job in _jobs.values()
            if job["status"] in ("completed", "failed")
        ]


@app.post("/scrape_results/jobs/{job_id}/fetched")
def mark_job_fetched(job_id: str) -> Dict[str, Any]:
    """Marks a job as fetched and removes it from memory."""
    with _jobs_lock:
        job = _jobs.pop(job_id, None)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found or already fetched")
    return {"job_id": job_id, "status": "fetched"}


@app.get("/scrape_results/jobs")
def list_all_jobs() -> List[Dict[str, Any]]:
    """Lists all in-memory jobs (for debugging/dashboard)."""
    with _jobs_lock:
        return [
            {k: v for k, v in job.items() if k != "artifacts"}
            for job in _jobs.values()
        ]


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
        elif processor == "cars_results_page__listings_v3":
            listings, parse_meta = parse_cars_results_page_html_v3(html)
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


@app.post("/cleanup/artifacts")
def run_cleanup_artifacts(payload: dict = Body(...)) -> Dict[str, Any]:
    """
    Delete raw artifact files from disk.
    Accepts {"artifacts": [{"artifact_id": int, "filepath": str}, ...]}.
    Returns {"results": [{"artifact_id": int, "deleted": bool, "reason": str|None}]}.
    n8n marks deleted_at on rows where deleted=true.
    """
    from processors.cleanup_artifacts import cleanup_artifacts
    artifacts = (payload or {}).get("artifacts", [])
    results = cleanup_artifacts(artifacts)
    deleted_count = sum(1 for r in results if r.get("deleted"))
    return {
        "total": len(results),
        "deleted": deleted_count,
        "failed": len(results) - deleted_count,
        "results": results,
    }
