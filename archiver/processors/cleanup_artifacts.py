import logging
import os
from typing import Any, Dict, List

from archiver.queries import GET_CLEANUP_CANDIDATES, MARK_ARTIFACTS_DELETED
from shared.db import db_cursor

logger = logging.getLogger("archiver")


def cleanup_artifacts(artifacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Delete raw artifact files from disk.

    Accepts a list of {"artifact_id": int, "filepath": str} dicts.
    For each, attempts os.remove(). FileNotFoundError is treated as already-deleted (success).
    Returns list of {"artifact_id": int, "deleted": bool, "reason": str|None}.
    """
    results = []
    for item in artifacts:
        artifact_id = item.get("artifact_id")
        filepath = item.get("filepath")

        if not filepath:
            results.append({
                "artifact_id": artifact_id,
                "deleted": False,
                "reason": "no filepath provided",
            })
            continue

        try:
            os.remove(filepath)
            results.append({
                "artifact_id": artifact_id,
                "deleted": True,
                "reason": None,
            })
        except FileNotFoundError:
            # Already gone — treat as success so deleted_at gets set
            results.append({
                "artifact_id": artifact_id,
                "deleted": True,
                "reason": "file not found (already deleted)",
            })
        except Exception as e:
            results.append({
                "artifact_id": artifact_id,
                "deleted": False,
                "reason": f"{type(e).__name__}: {e}",
            })

    return results


def run_cleanup_artifacts() -> Dict[str, Any]:
    """
    Full cleanup cycle for the Airflow DAG (POST /cleanup/artifacts/run).

    1. Query candidates eligible for deletion.
    2. Archive them to Parquet (sets archived_at).
    3. Delete files for successfully archived artifacts.
    4. Mark deleted_at for successfully deleted artifacts.
    """
    from archiver.processors.archive_artifacts import archive_artifacts

    with db_cursor(error_context="run_cleanup_artifacts: get candidates") as cur:
        cur.execute(GET_CLEANUP_CANDIDATES)
        rows = cur.fetchall()

    if not rows:
        return {"total": 0, "archived": 0, "deleted": 0, "failed": 0, "results": []}

    candidates = [
        {"artifact_id": row[0], "filepath": row[1], "archived_at": row[2]}
        for row in rows
    ]

    needs_archive = [c for c in candidates if c["archived_at"] is None]
    already_archived = [c for c in candidates if c["archived_at"] is not None]

    filepath_by_id = {c["artifact_id"]: c["filepath"] for c in candidates}

    archive_results = archive_artifacts(needs_archive) if needs_archive else []
    newly_archived = [
        {"artifact_id": r["artifact_id"], "filepath": filepath_by_id.get(r["artifact_id"])}
        for r in archive_results if r.get("archived")
    ]
    ready_to_delete = newly_archived + [
        {"artifact_id": c["artifact_id"], "filepath": c["filepath"]}
        for c in already_archived
    ]

    delete_results = cleanup_artifacts(ready_to_delete)
    deleted_ids = [r["artifact_id"] for r in delete_results if r.get("deleted")]

    if deleted_ids:
        with db_cursor(error_context="run_cleanup_artifacts: mark deleted") as cur:
            cur.execute(MARK_ARTIFACTS_DELETED, (deleted_ids,))

    deleted_count = len(deleted_ids)
    newly_archived_count = len(newly_archived)
    failed_count = len(candidates) - deleted_count
    logger.info(
        "cleanup_artifacts: total=%d archived=%d deleted=%d failed=%d",
        len(candidates), newly_archived_count, deleted_count, failed_count,
    )
    return {
        "total": len(candidates),
        "archived": newly_archived_count,
        "deleted": deleted_count,
        "failed": failed_count,
        "results": delete_results,
    }
