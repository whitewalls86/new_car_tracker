from contextlib import asynccontextmanager
from fastapi import FastAPI, Body, HTTPException
from typing import Any, Dict, List, Optional
import json
import logging
import os
from logging.handlers import RotatingFileHandler
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from processors.scrape_results import scrape_results
from processors.results_page_cards import (parse_cars_results_page_html, parse_cars_results_page_html_v2, parse_cars_results_page_html_v3)
from processors.scrape_detail import (scrape_detail_dummy, scrape_detail_fetch)
from processors.parse_detail_page import parse_cars_detail_page_html_v1
from db import get_pool, close_pool

_LOG_PATH = "/usr/app/logs/app.log"
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
    except Exception as e:
        logger.exception("Scrape job %s failed (run_id=%s, search_key=%s, scope=%s)", job_id, run_id, search_key, scope)
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
    return {"search_key": search_key, "scope": scope, "count": len(rows), "vins": [r["vin"] for r in rows]}


@app.post("/search_configs/advance_rotation")
async def advance_search_rotation(
    min_idle_minutes: int = 1439,
    min_gap_minutes: int = 230,
) -> Dict[str, Any]:
    """
    Atomically claims the next rotation slot due for scraping.

    Architectural note: this is a justified exception to the principle that n8n
    owns all orchestration logic. The slot-claiming transaction requires an
    atomic DB read-modify-write that cannot be expressed safely in n8n HTTP nodes.

    Two guards:
    1. min_idle_minutes (default 1439 = 23h59m): each slot must wait this long
       before it can fire again. With 6 slots this means ~1 fire/day per slot.
    2. min_gap_minutes (default 230 = ~3h50m): blocks if ANY non-skipped search
       scrape run started within this window. Prevents multiple slots from
       firing in rapid succession even if all have stale timestamps.

    Returns {"slot": null, "configs": []} when nothing is due.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Guard: check time since last non-skipped search scrape run
            last_run = await conn.fetchrow("""
                SELECT started_at
                FROM runs
                WHERE trigger = 'search scrape'
                  AND status NOT IN ('skipped', 'failed', 'requested')
                ORDER BY started_at DESC
                LIMIT 1
            """)
            if last_run and last_run["started_at"]:
                import datetime
                gap = datetime.datetime.now(datetime.timezone.utc) - last_run["started_at"]
                if gap.total_seconds() < min_gap_minutes * 60:
                    return {"slot": None, "configs": [], "reason": "too_soon",
                            "last_run_minutes_ago": round(gap.total_seconds() / 60, 1)}

            # Find the next due slot
            slot_row = await conn.fetchrow("""
                SELECT rotation_slot
                FROM search_configs
                WHERE enabled = true
                  AND rotation_slot IS NOT NULL
                  AND (last_queued_at IS NULL
                       OR last_queued_at < now() - make_interval(mins => $1))
                GROUP BY rotation_slot
                ORDER BY MIN(COALESCE(last_queued_at, '1970-01-01'::timestamptz)), rotation_slot
                LIMIT 1
            """, min_idle_minutes)

            if slot_row is None:
                # Fallback: try legacy single-config (no rotation_slot)
                row = await conn.fetchrow("""
                    SELECT search_key, params
                    FROM search_configs
                    WHERE enabled = true
                      AND rotation_slot IS NULL
                      AND (last_queued_at IS NULL
                           OR last_queued_at < now() - make_interval(mins => $1))
                    ORDER BY rotation_order NULLS LAST, search_key
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """, min_idle_minutes)

                if not row:
                    return {"slot": None, "configs": []}

                await conn.execute(
                    "UPDATE search_configs SET last_queued_at = now() WHERE search_key = $1",
                    row["search_key"],
                )
                raw_params = row["params"]
                params = json.loads(raw_params) if isinstance(raw_params, str) else dict(raw_params)
                return {
                    "slot": None,
                    "configs": [{
                        "search_key": row["search_key"],
                        "params": params,
                        "scopes": params.get("scopes", ["local", "national"]),
                    }],
                }

            slot = slot_row["rotation_slot"]

            # Claim all configs in this slot
            await conn.execute("""
                UPDATE search_configs
                SET last_queued_at = now()
                WHERE enabled = true AND rotation_slot = $1
            """, slot)

            rows = await conn.fetch("""
                SELECT search_key, params
                FROM search_configs
                WHERE enabled = true AND rotation_slot = $1
                ORDER BY rotation_order NULLS LAST, search_key
            """, slot)

    configs = []
    for row in rows:
        raw_params = row["params"]
        params = json.loads(raw_params) if isinstance(raw_params, str) else dict(raw_params)
        configs.append({
            "search_key": row["search_key"],
            "params": params,
            "scopes": params.get("scopes", ["local", "national"]),
        })

    return {
        "slot": slot,
        "configs": configs,
    }


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
        logger.exception("Results page parsing failed (artifact_id=%s, processor=%s, filepath=%s)", artifact_id, processor, filepath)
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
        logger.exception("Detail page parsing failed (artifact_id=%s, processor=%s, filepath=%s)", artifact_id, processor, filepath)
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


@app.get("/logs")
def get_logs(lines: int = 200) -> Dict[str, Any]:
    """Return the last N lines of the application log file."""
    try:
        with open(_LOG_PATH) as f:
            all_lines = f.readlines()
        return {"lines": all_lines[-lines:]}
    except FileNotFoundError:
        return {"lines": []}
