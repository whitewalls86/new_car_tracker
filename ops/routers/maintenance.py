"""
Pipeline maintenance endpoints — orphan expiry and stale state cleanup.
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter

from ops.queries import (
    EXPIRE_ORPHAN_DETAIL_CLAIMS,
    EXPIRE_ORPHAN_PROCESSING_RUNS,
    EXPIRE_ORPHAN_RUNS,
    EXPIRE_ORPHAN_SCRAPE_JOBS,
    RESET_STALE_ARTIFACT_PROCESSING,
)
from shared.db import db_cursor
from shared.job_counter import active_job

logger = logging.getLogger("pipeline_ops")
router = APIRouter(prefix="/maintenance")

DEFAULT_THRESHOLD = 60


def _run_maintenance_query(sql: str, params: tuple) -> Dict[str, Any]:
    with db_cursor(error_context="maintenance") as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return {"affected": len(rows)}


@router.post("/expire-orphan-runs")
def expire_orphan_runs(threshold_minutes: int = DEFAULT_THRESHOLD) -> Dict[str, Any]:
    with active_job():
        return _run_maintenance_query(EXPIRE_ORPHAN_RUNS, (threshold_minutes, threshold_minutes))


@router.post("/expire-orphan-processing-runs")
def expire_orphan_processing_runs(threshold_minutes: int = DEFAULT_THRESHOLD) -> Dict[str, Any]:
    with active_job():
        params = (threshold_minutes, threshold_minutes)
        return _run_maintenance_query(EXPIRE_ORPHAN_PROCESSING_RUNS, params)


@router.post("/reset-stale-artifact-processing")
def reset_stale_artifact_processing(threshold_minutes: int = DEFAULT_THRESHOLD) -> Dict[str, Any]:
    with active_job():
        params = (threshold_minutes, threshold_minutes)
        return _run_maintenance_query(RESET_STALE_ARTIFACT_PROCESSING, params)


@router.post("/expire-orphan-detail-claims")
def expire_orphan_detail_claims() -> Dict[str, Any]:
    with active_job():
        return _run_maintenance_query(EXPIRE_ORPHAN_DETAIL_CLAIMS, ())


@router.post("/expire-orphan-scrape-jobs")
def expire_orphan_scrape_jobs() -> Dict[str, Any]:
    with active_job():
        return _run_maintenance_query(EXPIRE_ORPHAN_SCRAPE_JOBS, ())
