import logging
from typing import Any, Dict

from fastapi import Body, FastAPI

from archiver.processors.archive_artifacts import archive_artifacts as _archive_artifacts
from archiver.processors.cleanup_artifacts import cleanup_artifacts
from archiver.processors.cleanup_parquet import cleanup_parquet as _cleanup_parquet
from archiver.processors.cleanup_parquet import run_cleanup_parquet as _run_cleanup_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("archiver")

app = FastAPI()


@app.post("/archive/artifacts")
def run_archive_artifacts(payload: dict = Body(...)) -> Dict[str, Any]:
    artifacts = (payload or {}).get("artifacts", [])
    results = _archive_artifacts(artifacts)
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


@app.post("/cleanup/parquet/run")
def trigger_cleanup_parquet() -> Dict[str, Any]:
    return _run_cleanup_parquet()


@app.get("/health")
def health():
    return {"ok": True}
