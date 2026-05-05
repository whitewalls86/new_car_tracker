import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Dict, List

from fastapi import Body, FastAPI, HTTPException

from db import close_pool, get_pool
from scraper.processors.scrape_detail import (
    scrape_detail_batch,
    scrape_detail_dummy,
    scrape_detail_fetch,
)
from scraper.processors.scrape_results import scrape_results
from shared.logging_setup import configure_logging

configure_logging()

logger = logging.getLogger("scraper")

# ---------------------------------------------------------------------------
# In-memory job store for async SRP scraping
# ---------------------------------------------------------------------------
_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=4)


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
            _jobs[job_id]["page_1_blocked"] = result.get("page_1_blocked", False)
            _jobs[job_id]["attempt"] = payload.get("attempt", 1)
    except Exception as e:
        logger.exception(
            "Scrape job %s failed (run_id=%s, search_key=%s, scope=%s)",
            job_id,
            run_id,
            search_key,
            scope
        )
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = str(e)


def _run_detail_batch_job(
        job_id: str,
        run_id: str,
        batch_id: str,
        listings: List[Dict[str, Any]],
        max_workers: int
    ):
    """Runs in background thread. Updates in-memory job store."""
    import asyncio
    asyncio.set_event_loop(asyncio.new_event_loop())

    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
        _jobs[job_id]["started_at"] = __import__("datetime").datetime.utcnow().isoformat()
    try:
        logger.info(
            "detail batch job %s starting: run_id=%s batch_id=%s listing_count=%s",
            job_id, run_id, batch_id, len(listings),
        )
        result = scrape_detail_batch(
            run_id=run_id,
            batch_id=batch_id,
            listings=listings,
            max_workers=max_workers
        )
        artifacts = result.get("artifacts", [])
        with _jobs_lock:
            _jobs[job_id]["status"] = "completed"
            _jobs[job_id]["artifacts"] = artifacts
            _jobs[job_id]["artifact_count"] = len(artifacts)
    except Exception as e:
        logger.exception("Detail batch job %s failed (run_id=%s)", job_id, run_id)
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = str(e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize DB pool.
    await get_pool()
    yield
    # Shutdown: close DB pool
    await close_pool()


app = FastAPI(lifespan=lifespan)


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
            "page_1_blocked": False,
            "attempt": payload.get("attempt", 1),
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


@app.post("/scrape_detail/batch")
def scrape_detail_batch_endpoint(
    run_id: str,
    payload: dict = Body(...),
) -> Dict[str, Any]:
    """
    Queues an async detail-batch scrape job. Returns job_id immediately.
    payload.listings: [{listing_id, vin?, url?}, ...]
    payload.max_workers: optional int (default 8)
    Poll GET /scrape_results/jobs/completed to retrieve results.
    """
    listings = (payload or {}).get("listings") or []
    if not listings:
        raise HTTPException(
            status_code=400,
            detail="payload.listings is required and must be non-empty"
        )

    max_workers = int((payload or {}).get("max_workers") or 8)
    batch_id = (payload or {}).get("batch_id") or str(uuid.uuid4())

    logger.info(
        "scrape_detail/batch received run_id=%s batch_id=%s listing_count=%s payload_keys=%s",
        run_id, batch_id, len(listings), list((payload or {}).keys()),
    )

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "run_id": run_id,
            "batch_id": batch_id,
            "job_type": "detail_batch",
            "listing_count": len(listings),
            "status": "queued",
            "artifacts": [],
            "artifact_count": 0,
            "error": None,
            "started_at": None,
        }
    _executor.submit(_run_detail_batch_job, job_id, run_id, batch_id, listings, max_workers)
    return {
        "job_id": job_id,
        "batch_id": batch_id,
        "status": "queued",
        "listing_count": len(listings)
    }


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    """
    Drain endpoint. Returns 503 while jobs are running or queued so that
    monitoring and Airflow sensors get a clear not-ready signal via status code.
    """
    with _jobs_lock:
        active = [j for j in _jobs.values() if j["status"] in ("queued", "running")]
    if active:
        raise HTTPException(
            status_code=503,
            detail={"ready": False, "active_jobs": len(active)},
        )
    return {"ready": True, "active_jobs": 0}
