"""
Pipeline maintenance endpoints — orphan expiry and stale state cleanup.
"""
import logging
from typing import Any, Dict

from fastapi import APIRouter

from ops.queries import (
    EVICT_DELISTED_COOLDOWNS,
    EXPIRE_ORPHAN_DETAIL_CLAIMS,
    INSERT_ARTIFACT_EVENT,
    INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT,
    MARK_ARTIFACT_STATUS,
    SELECT_LIVE_COOLDOWN_LISTINGS,
    SELECT_PENDING_CLEARED_LISTINGS,
    SELECT_STUCK_PROCESSING_ARTIFACTS,
)
from shared.db import db_cursor
from shared.duckdb_s3 import get_duckdb_s3_connection
from shared.job_counter import active_job
from shared.minio import BUCKET, object_exists

logger = logging.getLogger("pipeline_ops")
router = APIRouter(prefix="/maintenance")

# The blocked_cooldown_events lifecycle log lives in MinIO parquet (flushed from
# staging). Read it directly with a fresh S3-configured DuckDB connection —
# the persisted analytics.duckdb view over the same files would contend with
# dbt's write lock, and the gauges' plain connection has no S3 credentials.
_BLOCKED_EVENTS_PARQUET = f"s3://{BUCKET}/ops_normalized/blocked_cooldown_events/**/*.parquet"

# Listings still counted as blocked: latest lifecycle event is
# 'blocked'/'incremented' (not 'cleared').
_COUNTED_SQL = """
    SELECT listing_id, current_attempts FROM (
        SELECT listing_id,
               arg_max(num_of_attempts, event_at) AS current_attempts,
               arg_max(event_type, event_at)       AS latest_event
        FROM read_parquet(?, hive_partitioning=true)
        GROUP BY listing_id
    ) WHERE latest_event IN ('blocked', 'incremented')
"""


def _run_maintenance_query(sql: str, params: tuple) -> Dict[str, Any]:
    with db_cursor(error_context="maintenance") as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return {"affected": len(rows)}


@router.post("/expire-orphan-detail-claims")
def expire_orphan_detail_claims() -> Dict[str, Any]:
    with active_job():
        return _run_maintenance_query(EXPIRE_ORPHAN_DETAIL_CLAIMS, ())


def _reap_stuck_processing() -> Dict[str, Any]:
    """Reset artifacts stranded in 'processing': re-queue (retry) when the MinIO
    object still exists, else abandon (skip) to avoid an infinite retry loop."""
    with db_cursor(error_context="reap: select stuck", dict_cursor=True) as cur:
        cur.execute(SELECT_STUCK_PROCESSING_ARTIFACTS)
        rows = [dict(r) for r in cur.fetchall()]

    retried = skipped = 0
    for r in rows:
        new_status = "retry" if object_exists(r["minio_path"]) else "skip"
        with db_cursor(error_context=f"reap: mark {new_status}") as cur:
            cur.execute(MARK_ARTIFACT_STATUS, {
                "status": new_status, "artifact_id": r["artifact_id"],
            })
            cur.execute(INSERT_ARTIFACT_EVENT, {
                "artifact_id": r["artifact_id"], "status": new_status,
                "minio_path": r["minio_path"], "artifact_type": r["artifact_type"],
                "fetched_at": r["fetched_at"], "listing_id": r["listing_id"],
                "run_id": r["run_id"],
            })
        if new_status == "retry":
            retried += 1
        else:
            skipped += 1

    if rows:
        logger.info("reap_stuck_processing: %d stuck → %d retry, %d skip",
                    len(rows), retried, skipped)
    return {"stuck": len(rows), "retried": retried, "skipped": skipped}


@router.post("/reap-stuck-processing")
def reap_stuck_processing() -> Dict[str, Any]:
    with active_job():
        return _reap_stuck_processing()


def _evict_delisted_cooldowns() -> Dict[str, Any]:
    """Delete blocked_cooldown rows for listings gone from price_observations,
    emitting a 'cleared' event per row so the cohort mart drops them."""
    with db_cursor(error_context="evict delisted cooldowns") as cur:
        cur.execute(EVICT_DELISTED_COOLDOWNS)
        removed = cur.fetchall()
        for listing_id, num_attempts in removed:
            cur.execute(INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT, {
                "listing_id": listing_id, "num_of_attempts": num_attempts,
            })
    if removed:
        logger.info("evict_delisted_cooldowns: removed %d rows", len(removed))
    return {"evicted": len(removed)}


@router.post("/evict-delisted-cooldowns")
def evict_delisted_cooldowns() -> Dict[str, Any]:
    with active_job():
        return _evict_delisted_cooldowns()


def _reconcile_cooldown_cohorts() -> Dict[str, Any]:
    """Emit 'cleared' events for listings still counted as blocked in the
    analytics store but absent from the live blocked_cooldown table (deleted
    long ago without a lifecycle event). Idempotent: skips listings that already
    have a pending 'cleared' event not yet flushed to the analytics store."""
    try:
        con = get_duckdb_s3_connection()
        try:
            counted = con.execute(_COUNTED_SQL, [_BLOCKED_EVENTS_PARQUET]).fetchall()
        finally:
            con.close()
    except Exception as e:
        # No parquet yet (fresh env / never flushed) — nothing to reconcile.
        if "No files found" in str(e):
            logger.info("reconcile: no blocked_cooldown_events parquet yet")
            return {"counted": 0, "live": 0, "pending_cleared": 0, "cleared": 0}
        raise

    counted = {str(lid): att for lid, att in counted}

    with db_cursor(error_context="reconcile: live+pending") as cur:
        cur.execute(SELECT_LIVE_COOLDOWN_LISTINGS)
        live = {r[0] for r in cur.fetchall()}
        cur.execute(SELECT_PENDING_CLEARED_LISTINGS)
        pending = {r[0] for r in cur.fetchall()}

    orphans = [
        (lid, att) for lid, att in counted.items()
        if lid not in live and lid not in pending
    ]

    if orphans:
        from psycopg2.extras import execute_values
        with db_cursor(error_context="reconcile: emit cleared") as cur:
            execute_values(
                cur,
                "INSERT INTO staging.blocked_cooldown_events "
                "(listing_id, event_type, num_of_attempts) VALUES %s",
                [(lid, "cleared", att) for lid, att in orphans],
            )
        logger.info("reconcile_cooldown_cohorts: emitted %d 'cleared' events "
                    "(counted=%d live=%d pending=%d)",
                    len(orphans), len(counted), len(live), len(pending))

    return {
        "counted": len(counted), "live": len(live),
        "pending_cleared": len(pending), "cleared": len(orphans),
    }


@router.post("/reconcile-cooldown-cohorts")
def reconcile_cooldown_cohorts() -> Dict[str, Any]:
    with active_job():
        return _reconcile_cooldown_cohorts()
