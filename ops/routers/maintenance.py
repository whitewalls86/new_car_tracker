"""
Pipeline maintenance endpoints — orphan expiry and stale state cleanup.
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter

from ops.queries import EXPIRE_ORPHAN_DETAIL_CLAIMS
from shared.db import db_cursor
from shared.job_counter import active_job

logger = logging.getLogger("pipeline_ops")
router = APIRouter(prefix="/maintenance")


def _run_maintenance_query(sql: str, params: tuple) -> Dict[str, Any]:
    with db_cursor(error_context="maintenance") as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return {"affected": len(rows)}


@router.post("/expire-orphan-detail-claims")
def expire_orphan_detail_claims() -> Dict[str, Any]:
    with active_job():
        return _run_maintenance_query(EXPIRE_ORPHAN_DETAIL_CLAIMS, ())
