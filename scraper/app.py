import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException

from db import close_pool, get_pool
from scraper.processors.parse_detail_page import parse_cars_detail_page_html_v1
from scraper.processors.results_page_cards import parse_cars_results_page_html_v3
from scraper.processors.scrape_detail import (
    scrape_detail_batch,
    scrape_detail_dummy,
    scrape_detail_fetch,
)
from scraper.processors.scrape_results import scrape_results

_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger("scraper")

# ---------------------------------------------------------------------------
# In-memory job store for async SRP scraping
# ---------------------------------------------------------------------------
_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()
_executor = ThreadPoolExecutor(max_workers=4)

# Sync DB config for background threads (psycopg2, not asyncpg)
# Parse from DATABASE_URL to stay in sync with docker-compose
_DATABASE_URL = os.environ.get("DATABASE_URL", "")
if _DATABASE_URL:
    from urllib.parse import urlparse
    _parsed = urlparse(_DATABASE_URL)
    _SYNC_DB_KWARGS = {
        "host": _parsed.hostname or "postgres",
        "port": _parsed.port or 5432,
        "dbname": _parsed.path.lstrip("/") or "cartracker",
        "user": _parsed.username or "cartracker",
        "password": _parsed.password or "",
    }
else:
    _SYNC_DB_KWARGS = {
        "host": "postgres",
        "dbname": "cartracker",
        "user": "cartracker",
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }


def _fetch_known_vins(search_key: str, scope: str) -> List[str]:
    """Fetch all VINs we have ever seen across all searches and sources.
    VINs are globally unique, so no make/model/scope filtering is needed.
    Uses psycopg2 (sync) so it works in background threads.

    Architectural note: this is a justified exception to the principle that the
    scraper should not read from the analytics schema. The alternative — passing
    27k+ VINs over HTTP from n8n — is impractical."""
    import psycopg2
    conn = None
    try:
        conn = psycopg2.connect(**_SYNC_DB_KWARGS)
        with conn.cursor() as cur:
            cur.execute("SELECT vin FROM analytics.int_vehicle_attributes")
            return [row[0] for row in cur.fetchall()]
    except Exception:
        logger.warning("_fetch_known_vins failed — scraping without breakpoint VINs", exc_info=True)
        return []  # degrade gracefully — scrape without breakpoint
    finally:
        if conn is not None:
            conn.close()


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
        # Discovery mode: inject known VINs if not already provided
        if "known_vins" not in payload:
            payload["known_vins"] = _fetch_known_vins(search_key, scope)

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
    # Orphan recovery (marking stale runs/jobs as failed) is handled by the
    # n8n Orphan Checker workflow which runs every 5 minutes — no startup
    # recovery needed here.
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


@app.post("/scrape_results/retry")
def retry_scrape_results(
    run_id: str,
    search_key: str,
    scope: str,
    payload: dict = Body(...),
) -> Dict[str, Any]:
    """
    Retry a search that was blocked (page_1_blocked=True) without going through
    advance_search_rotation. Identical to /scrape_results but semantically distinct
    so n8n can route retries separately from normal scrape jobs.
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


@app.get("/search_configs/{search_key}/known_vins")
async def get_known_vins(search_key: str, scope: str = "national") -> Dict[str, Any]:
    """Returns all known VINs from the analytics layer (globally unique across all searches)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT vin FROM analytics.int_vehicle_attributes WHERE vin IS NOT NULL
        """)
    return {
        "search_key": search_key, 
        "scope": scope, 
        "count": len(rows), 
        "vins": [r["vin"] for r in rows]
    }


@app.post("/process/results_pages")
def process_results_pages(payload: dict = Body(...)) -> Dict[str, Any]:
    processor = (payload or {}).get("processor") or "cars_results_page__listings_v3"
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

    # --- Read HTML artifact: prefer MinIO (Plan 97), fall back to disk ---
    minio_path = artifact.get("minio_path")
    filepath   = artifact.get("filepath")

    if minio_path:
        try:
            from shared.minio import read_html as _read_minio
            html = _read_minio(minio_path).decode("utf-8", errors="replace")
        except Exception as e:
            return {
                "processor": processor,
                "artifact_id": artifact_id,
                "status": "failed",
                "message": "failed to read artifact from MinIO",
                "meta": {
                    "minio_path": minio_path,
                    "error": type(e).__name__,
                    "error_message": str(e),
                },
                "listings": [],
            }
    elif filepath:
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
    else:
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "status": "failed",
            "message": "artifact.minio_path or artifact.filepath is required",
            "meta": {},
            "listings": [],
        }

    # --- Parse ---
    try:
        if processor == "cars_results_page__listings_v3":
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
        logger.exception(
            "Results page parsing failed (artifact_id=%s, processor=%s, filepath=%s)",
            artifact_id, 
            processor, 
            filepath
        )
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

    # --- Read HTML artifact: prefer MinIO (Plan 97), fall back to disk ---
    minio_path = artifact.get("minio_path")
    filepath   = artifact.get("filepath")

    if minio_path:
        try:
            from shared.minio import read_html as _read_minio
            html = _read_minio(minio_path).decode("utf-8", errors="replace")
        except Exception as e:
            return {
                "processor": processor,
                "artifact_id": artifact_id,
                "search_key": search_key,
                "status": "failed",
                "message": "failed to read artifact from MinIO",
                "meta": {
                    "minio_path": minio_path,
                    "error": type(e).__name__,
                    "error_message": str(e),
                },
                "primary": {},
                "carousel": [],
            }
    elif filepath:
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
    else:
        return {
            "processor": processor,
            "artifact_id": artifact_id,
            "search_key": search_key,
            "status": "failed",
            "message": "artifact.minio_path or artifact.filepath is required",
            "meta": {},
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
                "meta": {
                    "error": "Invalid Processor", 
                    "error_message": "Please use a valid processor."
                },
                "primary": {},
                "carousel": [],
            }
    except Exception as e:
        logger.exception(
            "Detail page parsing failed (artifact_id=%s, processor=%s, filepath=%s)",
             artifact_id, 
             processor, 
             filepath
        )
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


@app.get("/logs")
def get_logs(lines: int = 200) -> Dict[str, Any]:
    """Return the last N lines of the application log file."""
    try:
        with open(_LOG_PATH) as f:
            all_lines = f.readlines()
        return {"lines": all_lines[-lines:]}
    except FileNotFoundError:
        return {"lines": []}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    """
    Drain endpoint (Plan 92). Returns 200 when no jobs are in-flight so that
    a deploy can proceed safely. Returns 503 while jobs are running or queued.
    """
    with _jobs_lock:
        active = [j for j in _jobs.values() if j["status"] in ("queued", "running")]
    if active:
        raise HTTPException(
            status_code=503,
            detail={"ready": False, "active_jobs": len(active)},
        )
    return {"ready": True, "active_jobs": 0}
