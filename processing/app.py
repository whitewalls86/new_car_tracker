"""
Processing service — artifact parsing and observation writes for cartracker.
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query

from processing.processor import claim_batch, process_artifact, queue_is_empty

_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

app = FastAPI()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready")
def ready():
    """Returns queue drain status. Airflow uses this as a sensor before closing a run."""
    empty = queue_is_empty()
    return {"ready": empty, "queue_empty": empty}


@app.post("/process/batch")
def process_batch(
    batch_size: int = Query(default=20, ge=1, le=200),
    artifact_type: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """
    Claim and process a batch of pending/retry artifacts from ops.artifacts_queue.

    Query params:
      batch_size    — how many artifacts to claim (default 20, max 200)
      artifact_type — optional filter: 'results_page' or 'detail_page'

    Returns a summary of outcomes for each claimed artifact.
    """
    artifacts = claim_batch(batch_size=batch_size, artifact_type=artifact_type)
    if not artifacts:
        return {"claimed": 0, "complete": 0, "retry": 0, "skipped": 0, "results": []}

    logger.info(
        "process_batch: claimed %d artifacts (type=%s)", len(artifacts), artifact_type
    )

    results = []
    for artifact in artifacts:
        result = process_artifact(artifact)
        results.append({"artifact_id": artifact["artifact_id"], **result})
        logger.info(
            "artifact_id=%s type=%s status=%s",
            artifact["artifact_id"], artifact.get("artifact_type"), result.get("status"),
        )

    return {
        "claimed": len(artifacts),
        "complete": sum(1 for r in results if r.get("status") == "complete"),
        "retry": sum(1 for r in results if r.get("status") == "retry"),
        "skipped": sum(1 for r in results if r.get("status") == "skip"),
        "results": results,
    }
