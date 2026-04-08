from fastapi import FastAPI, Body
from typing import Any, Dict, List
import logging, os
from urllib.parse import urlparse

from processors.archive_artifacts import archive_artifacts as _archive_artifacts
from processors.cleanup_artifacts import cleanup_artifacts
from processors.cleanup_parquet import cleanup_parquet as _cleanup_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("archiver")

_DATABASE_URL = os.environ.get("DATABASE_URL", "")
if _DATABASE_URL:
    _p = urlparse(_DATABASE_URL)
    _SYNC_DB_KWARGS = {
        "host": _p.hostname or "postgres", "port": _p.port or 5432,
        "dbname": _p.path.lstrip("/") or "cartracker",
        "user": _p.username or "cartracker", "password": _p.password or "",
    }
else:
    _SYNC_DB_KWARGS = {
        "host": "postgres", "dbname": "cartracker", "user": "cartracker",
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }

app = FastAPI()


@app.post("/archive/artifacts")
def run_archive_artifacts(payload: dict = Body(...)) -> Dict[str, Any]:
    artifacts = (payload or {}).get("artifacts", [])
    results = _archive_artifacts(artifacts, _SYNC_DB_KWARGS)
    archived_count = sum(1 for r in results if r.get("archived"))
    return {"total": len(results), "archived": archived_count,
            "failed": len(results) - archived_count, "results": results}


@app.post("/cleanup/artifacts")
def run_cleanup_artifacts(payload: dict = Body(...)) -> Dict[str, Any]:
    artifacts = (payload or {}).get("artifacts", [])
    results = cleanup_artifacts(artifacts)
    deleted_count = sum(1 for r in results if r.get("deleted"))
    return {"total": len(results), "deleted": deleted_count,
            "failed": len(results) - deleted_count, "results": results}


@app.post("/cleanup/parquet")
def run_cleanup_parquet(payload: dict = Body(...)) -> Dict[str, Any]:
    paths = (payload or {}).get("paths", [])
    results = _cleanup_parquet(paths)
    deleted_count = sum(1 for r in results if r.get("deleted"))
    return {"total": len(results), "deleted": deleted_count,
            "failed": len(results) - deleted_count, "results": results}


@app.get("/health")
def health():
    return {"ok": True}
