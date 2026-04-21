"""
POST /process/artifact/{artifact_id} — single-artifact reprocessing.

Used for:
  - Manual reprocessing of failed artifacts
  - Kafka readiness: Plan 87 will trigger this per-artifact on event receipt
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from processing.queries import INSERT_ARTIFACT_EVENT
from processing.routers.batch import _process_artifact
from shared.db import db_cursor
from shared.job_counter import active_job

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/process/artifact/{artifact_id}")
def process_single_artifact(artifact_id: int) -> Dict[str, Any]:
    """
    Reprocess a single artifact by ID.

    Claims the artifact (sets status='processing'), processes it,
    then marks final status. Returns the processing result.
    """
    with active_job():
        # Fetch the artifact row
        with db_cursor(error_context="artifact: fetch", dict_cursor=True) as cur:
            cur.execute(
                """
                UPDATE ops.artifacts_queue
                SET status = 'processing'
                WHERE artifact_id = %(artifact_id)s
                  AND status IN ('pending', 'retry', 'skip')
                RETURNING artifact_id, minio_path, artifact_type,
                          listing_id, run_id, fetched_at
                """,
                {"artifact_id": artifact_id},
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Artifact {artifact_id} not found or not in reprocessable state",
            )

        artifact = dict(row)

        # Write 'processing' event
        with db_cursor(error_context="artifact: processing event") as cur:
            cur.execute(INSERT_ARTIFACT_EVENT, {
                "artifact_id": artifact["artifact_id"],
                "status": "processing",
                "minio_path": artifact["minio_path"],
                "artifact_type": artifact["artifact_type"],
                "fetched_at": artifact["fetched_at"],
                "listing_id": artifact["listing_id"],
                "run_id": artifact["run_id"],
            })

        result = _process_artifact(artifact)
        return {"artifact_id": artifact_id, **result}
