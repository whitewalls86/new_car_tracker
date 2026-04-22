"""
POST /process/batch — claim and process a batch of pending/retry artifacts.

Wraps the handler body in active_job() from shared/job_counter.py so the
/ready endpoint can report whether a batch is currently running.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query

from processing.processors import (
    parse_cars_detail_page_html_v1,
    parse_cars_results_page_html_v3,
)
from processing.queries import (
    CLAIM_ARTIFACTS,
    INSERT_ARTIFACT_EVENT,
    MARK_ARTIFACT_STATUS,
)
from processing.writers.detail_writer import (
    is_block_page,
    write_detail_active,
    write_detail_blocked,
    write_detail_unlisted,
)
from processing.writers.srp_writer import write_srp_observations
from shared.db import db_cursor
from shared.job_counter import active_job
from shared.minio import read_html

logger = logging.getLogger(__name__)
router = APIRouter()


def _claim_batch(batch_size: int, artifact_type: Optional[str]) -> List[Dict[str, Any]]:
    """Atomically claim up to batch_size pending/retry artifacts."""
    if artifact_type:
        type_filter = "AND artifact_type = %(artifact_type)s"
        params = {"artifact_type": artifact_type, "limit": batch_size}
    else:
        type_filter = ""
        params = {"limit": batch_size}

    sql = CLAIM_ARTIFACTS.format(type_filter=type_filter)

    with db_cursor(error_context="claim_batch", dict_cursor=True) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]

    # Write 'processing' events for all claimed rows
    if rows:
        with db_cursor(error_context="claim_batch: write processing events") as cur:
            for r in rows:
                cur.execute(INSERT_ARTIFACT_EVENT, {
                    "artifact_id": r["artifact_id"],
                    "status": "processing",
                    "minio_path": r["minio_path"],
                    "artifact_type": r["artifact_type"],
                    "fetched_at": r["fetched_at"],
                    "listing_id": r["listing_id"],
                    "run_id": r["run_id"],
                })

    return rows


def _set_status(artifact: Dict[str, Any], status: str) -> None:
    """Update artifact queue status and write an event row."""
    with db_cursor(error_context=f"set_status {status}") as cur:
        cur.execute(MARK_ARTIFACT_STATUS, {
            "status": status,
            "artifact_id": artifact["artifact_id"],
        })
        cur.execute(INSERT_ARTIFACT_EVENT, {
            "artifact_id": artifact["artifact_id"],
            "status": status,
            "minio_path": artifact.get("minio_path"),
            "artifact_type": artifact.get("artifact_type"),
            "fetched_at": artifact.get("fetched_at"),
            "listing_id": artifact.get("listing_id"),
            "run_id": artifact.get("run_id"),
        })


def _read_artifact_html(artifact: Dict[str, Any]) -> str:
    """Read and decompress HTML from MinIO."""
    return read_html(artifact["minio_path"]).decode("utf-8", errors="replace")


def _process_results_page(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single results_page artifact."""
    artifact_id = artifact["artifact_id"]
    fetched_at = artifact["fetched_at"]
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at)

    try:
        html = _read_artifact_html(artifact)
    except Exception as e:
        logger.warning("results_page %s: MinIO read failed: %s", artifact_id, e)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    try:
        listings, parse_meta = parse_cars_results_page_html_v3(html)
    except Exception as e:
        logger.exception("results_page %s: parse failed", artifact_id)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    search_key = artifact.get("search_key")

    try:
        result = write_srp_observations(
            listings, artifact_id, fetched_at, search_key=search_key,
        )
    except Exception as e:
        logger.exception("results_page %s: DB writes failed", artifact_id)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    _set_status(artifact, "complete")
    return {
        "status": "complete",
        "artifact_type": "results_page",
        "listings_parsed": len(listings),
        **result,
    }


def _process_detail_page(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single detail_page artifact."""
    artifact_id = artifact["artifact_id"]
    listing_id = artifact.get("listing_id")
    run_id = artifact.get("run_id")
    fetched_at = artifact["fetched_at"]
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at)

    artifact_url = (
        f"https://www.cars.com/vehicledetail/{listing_id}/" if listing_id else None
    )

    try:
        html = _read_artifact_html(artifact)
    except Exception as e:
        logger.warning("detail_page %s: MinIO read failed: %s", artifact_id, e)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    # Check for 403 block page before parsing
    if is_block_page(html):
        logger.info("detail_page %s: 403 block page detected", artifact_id)
        try:
            result = write_detail_blocked(artifact_id, listing_id, run_id)
        except Exception as e:
            logger.exception("detail_page %s: blocked writes failed", artifact_id)
            _set_status(artifact, "retry")
            return {"status": "retry", "error": str(e)}
        _set_status(artifact, "skip")
        return {"status": "skip", "reason": "block_page", **result}

    try:
        primary, carousel, parse_meta = parse_cars_detail_page_html_v1(html, artifact_url)
    except Exception as e:
        logger.exception("detail_page %s: parse failed", artifact_id)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    resolved_listing_id = primary.get("listing_id") or listing_id
    listing_state = primary.get("listing_state", "active")

    try:
        if listing_state == "unlisted":
            result = write_detail_unlisted(
                primary, artifact_id, fetched_at, resolved_listing_id, run_id,
            )
        else:
            result = write_detail_active(
                primary, carousel, artifact_id, fetched_at,
                resolved_listing_id, run_id,
            )
    except Exception as e:
        logger.exception("detail_page %s: DB writes failed", artifact_id)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    _set_status(artifact, "complete")
    return {
        "status": "complete",
        "artifact_type": "detail_page",
        "listing_state": listing_state,
        "listing_id": resolved_listing_id,
        **result,
    }


def _process_artifact(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch to the correct processor based on artifact_type."""
    artifact_type = artifact.get("artifact_type")
    if artifact_type == "results_page":
        return _process_results_page(artifact)
    if artifact_type == "detail_page":
        return _process_detail_page(artifact)

    logger.warning(
        "Unknown artifact_type=%s for artifact_id=%s",
        artifact_type, artifact.get("artifact_id"),
    )
    _set_status(artifact, "skip")
    return {"status": "skip", "reason": f"unknown artifact_type: {artifact_type}"}


@router.post("/process/batch")
def process_batch(
    batch_size: int = Query(default=50, ge=1, le=2500),
    artifact_type: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """
    Claim and process a batch of pending/retry artifacts.

    Response shape matches Plan 71 Airflow DAG expectations:
      srp_count, detail_count, retry_count, skip_count, silver_write_failures
    """
    with active_job():
        artifacts = _claim_batch(batch_size=batch_size, artifact_type=artifact_type)
        if not artifacts:
            return {
                "srp_count": 0,
                "detail_count": 0,
                "retry_count": 0,
                "skip_count": 0,
                "silver_write_failures": 0,
            }

        logger.info(
            "process_batch: claimed %d artifacts (type=%s)",
            len(artifacts), artifact_type,
        )

        srp_count = 0
        detail_count = 0
        retry_count = 0
        skip_count = 0
        silver_write_failures = 0

        for artifact in artifacts:
            result = _process_artifact(artifact)
            status = result.get("status")

            if status == "complete":
                if result.get("artifact_type") == "results_page":
                    srp_count += 1
                else:
                    detail_count += 1
                # Count silver write failures from successful processing
                if result.get("silver_written", 0) == 0 and status == "complete":
                    silver_write_failures += 1
            elif status == "retry":
                retry_count += 1
            elif status == "skip":
                skip_count += 1

        return {
            "srp_count": srp_count,
            "detail_count": detail_count,
            "retry_count": retry_count,
            "skip_count": skip_count,
            "silver_write_failures": silver_write_failures,
        }
