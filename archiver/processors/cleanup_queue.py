"""
Plan 97: artifacts_queue row cleanup.

Deletes artifacts_queue rows whose status is 'complete' or 'skip'.
'retry' rows are intentionally left in place until resolved.

This replaces the local-file archive step for new-style artifacts that were
written directly to MinIO by the scraper — the HTML is already in MinIO, so
there is nothing to archive; we only need to prune the work-queue table.
"""
import logging
from typing import Any, Dict, List

from archiver.queries import GET_QUEUE_CLEANUP_CANDIDATES
from shared.db import db_cursor

logger = logging.getLogger("archiver")


def cleanup_queue(artifact_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Delete the given artifact_ids from artifacts_queue.

    Accepts a list of integer artifact_ids.
    Returns [{"artifact_id": int, "deleted": bool, "reason": str|None}].
    """
    if not artifact_ids:
        return []

    results = []
    try:
        with db_cursor(error_context="cleanup_queue: delete rows") as cur:
            cur.execute(
                """
                DELETE FROM artifacts_queue
                WHERE  artifact_id = ANY(%s)
                  AND  status IN ('complete', 'skip')
                RETURNING artifact_id
                """,
                (artifact_ids,),
            )
            deleted_ids = {row[0] for row in cur.fetchall()}
    except Exception as e:
        logger.error("cleanup_queue: DELETE failed: %s", e)
        return [
            {"artifact_id": aid, "deleted": False, "reason": f"db_error: {e}"}
            for aid in artifact_ids
        ]

    for aid in artifact_ids:
        if aid in deleted_ids:
            results.append({"artifact_id": aid, "deleted": True, "reason": None})
        else:
            results.append({
                "artifact_id": aid,
                "deleted": False,
                "reason": "not deleted — row missing or status not in (complete, skip)",
            })

    return results


def run_cleanup_queue() -> Dict[str, Any]:
    """
    Full sweep: query eligible rows, delete them.
    Called by POST /cleanup/queue/run (Airflow DAG or manual trigger).
    """
    try:
        with db_cursor(error_context="run_cleanup_queue: get candidates") as cur:
            cur.execute(GET_QUEUE_CLEANUP_CANDIDATES)
            rows = cur.fetchall()
    except Exception as e:
        logger.error("run_cleanup_queue: failed to fetch candidates: %s", e)
        return {"total": 0, "deleted": 0, "failed": 0, "results": [], "error": str(e)}

    if not rows:
        return {"total": 0, "deleted": 0, "failed": 0, "results": []}

    artifact_ids = [row[0] for row in rows]
    results = cleanup_queue(artifact_ids)

    deleted_count = sum(1 for r in results if r.get("deleted"))
    failed_count  = len(results) - deleted_count

    logger.info(
        "run_cleanup_queue: total=%d deleted=%d failed=%d",
        len(results), deleted_count, failed_count,
    )
    return {
        "total":   len(results),
        "deleted": deleted_count,
        "failed":  failed_count,
        "results": results,
    }
