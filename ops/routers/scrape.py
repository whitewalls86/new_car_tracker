"""
Scrape coordination endpoints — rotation, claim management.
Centralised here so any scraper VM can call ops rather than owning this logic itself.
"""
import datetime
import json
import logging
import uuid
from typing import Any, Dict, List

from fastapi import APIRouter
from pydantic import BaseModel

from shared.db import db_cursor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/scrape", tags=["scrape"])


# ---------------------------------------------------------------------------
# Rotation
# ---------------------------------------------------------------------------

@router.post("/rotation/advance")
def advance_rotation(
    min_idle_minutes: int = 1439,
    min_gap_minutes: int = 230,
) -> Dict[str, Any]:
    """
    Atomically claims the next rotation slot due for scraping.

    Two guards:
    1. min_idle_minutes (default 1439 = 23h59m): each slot must wait this long
       before it can fire again.
    2. min_gap_minutes (default 230 = ~3h50m): blocks if ANY non-skipped search
       scrape run started within this window. Prevents multiple slots from
       firing in rapid succession even if all have stale timestamps.

    Returns {"slot": null, "configs": []} when nothing is due.
    """
    with db_cursor(error_context="advance_rotation") as cur:
        # Guard: check time since last non-skipped search scrape run
        cur.execute("""
            SELECT started_at
            FROM runs
            WHERE trigger = 'search scrape'
              AND status NOT IN ('skipped', 'failed', 'requested')
            ORDER BY started_at DESC
            LIMIT 1
        """)
        last_run = cur.fetchone()
        if last_run and last_run[0]:
            gap = datetime.datetime.now(datetime.timezone.utc) - last_run[0]
            if gap.total_seconds() < min_gap_minutes * 60:
                return {
                    "slot": None,
                    "configs": [],
                    "reason": "too_soon",
                    "last_run_minutes_ago": round(gap.total_seconds() / 60, 1),
                }

        # Find the next due slot
        cur.execute("""
            SELECT rotation_slot
            FROM search_configs
            WHERE enabled = true
              AND rotation_slot IS NOT NULL
              AND (last_queued_at IS NULL
                   OR last_queued_at < now() - make_interval(mins => %s))
            GROUP BY rotation_slot
            ORDER BY MIN(COALESCE(last_queued_at, '1970-01-01'::timestamptz)), rotation_slot
            LIMIT 1
        """, (min_idle_minutes,))
        slot_row = cur.fetchone()

        if slot_row is None:
            # Fallback: try legacy single-config (no rotation_slot)
            cur.execute("""
                SELECT search_key, params
                FROM search_configs
                WHERE enabled = true
                  AND rotation_slot IS NULL
                  AND (last_queued_at IS NULL
                       OR last_queued_at < now() - make_interval(mins => %s))
                ORDER BY rotation_order NULLS LAST, search_key
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """, (min_idle_minutes,))
            row = cur.fetchone()

            if not row:
                return {"slot": None, "configs": []}

            cur.execute(
                "UPDATE search_configs SET last_queued_at = now() WHERE search_key = %s",
                (row[0],),
            )
            raw_params = row[1]
            params = json.loads(raw_params) if isinstance(raw_params, str) else dict(raw_params)
            return {
                "slot": None,
                "configs": [{
                    "search_key": row[0],
                    "params": params,
                    "scopes": params.get("scopes", ["local", "national"]),
                }],
            }

        slot = slot_row[0]

        # Claim all configs in this slot
        cur.execute("""
            UPDATE search_configs
            SET last_queued_at = now()
            WHERE enabled = true AND rotation_slot = %s
        """, (slot,))

        cur.execute("""
            SELECT search_key, params
            FROM search_configs
            WHERE enabled = true AND rotation_slot = %s
            ORDER BY rotation_order NULLS LAST, search_key
        """, (slot,))
        rows = cur.fetchall()

    configs = []
    for row in rows:
        raw_params = row[1]
        params = json.loads(raw_params) if isinstance(raw_params, str) else dict(raw_params)
        configs.append({
            "search_key": row[0],
            "params": params,
            "scopes": params.get("scopes", ["local", "national"]),
        })

    return {"slot": slot, "configs": configs}


# ---------------------------------------------------------------------------
# Claim management
# ---------------------------------------------------------------------------

class ReleaseResult(BaseModel):
    listing_id: str
    status: str  # 'ok' | 'failed' | 'skipped'


class ReleaseRequest(BaseModel):
    run_id: str
    results: List[ReleaseResult]


@router.post("/claims/claim-batch")
def claim_batch(batch_size: int = 450) -> Dict[str, Any]:
    """
    Atomically claims the next batch of listings from the detail scrape queue.

    Creates a run row, inserts claims into detail_scrape_claims using
    ON CONFLICT DO UPDATE so stale claims are re-claimed cleanly.

    Returns {run_id, listings: [{listing_id, vin, canonical_detail_url, ...}]}.
    Returns {run_id, listings: []} if the queue is empty.
    """
    run_id = str(uuid.uuid4())

    with db_cursor(error_context="claim_batch") as cur:
        cur.execute(
            """INSERT INTO runs 
                (run_id, status, trigger)
               VALUES (%s::uuid, 'running', 'detail scrape')
            """,
            (run_id,),
        )

        cur.execute("""
            WITH batch AS (
                SELECT q.*
                FROM ops.ops_detail_scrape_queue q
                LEFT JOIN detail_scrape_claims c
                    ON c.listing_id = q.listing_id::uuid
                   AND c.status = 'running'
                WHERE c.listing_id IS NULL
                ORDER BY q.priority, q.listing_id
                LIMIT %s
            ),
            claimed AS (
                INSERT INTO detail_scrape_claims (listing_id, claimed_by, claimed_at, status)
                SELECT b.listing_id::uuid, %s, now(), 'running'
                FROM batch b
                ON CONFLICT (listing_id) DO UPDATE
                    SET claimed_by = EXCLUDED.claimed_by,
                        claimed_at = EXCLUDED.claimed_at,
                        status     = 'running'
                    WHERE detail_scrape_claims.status != 'running'
                RETURNING listing_id
            )
            SELECT b.* FROM batch b
            JOIN claimed c ON c.listing_id = b.listing_id::uuid
        """, (batch_size, run_id))

        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    listings = [dict(zip(col_names, row)) for row in rows]

    if not listings:
        # Nothing to scrape — mark run as skipped
        with db_cursor(error_context="claim_batch_skip") as cur:
            cur.execute(
                "UPDATE runs SET status = 'skipped', finished_at = now() WHERE run_id = %s::uuid",
                (run_id,),
            )

    return {"run_id": run_id, "listings": listings}


@router.post("/claims/release")
def release_claims(body: ReleaseRequest) -> Dict[str, Any]:
    """
    Releases claims after a scrape batch completes.

    Deletes claim rows for the given run_id and marks the run as finished.
    The run status is 'completed' if all results are ok/skipped, 'failed' if
    any result failed.
    """
    run_id = body.run_id
    results = body.results

    listing_ids = [r.listing_id for r in results]
    error_count = sum(1 for r in results if r.status == "failed")
    run_status = "failed" if error_count == len(results) and results else "completed"

    with db_cursor(error_context="release_claims") as cur:
        if listing_ids:
            cur.execute(
                "DELETE FROM detail_scrape_claims"
                " WHERE listing_id = ANY(%s::uuid[]) AND claimed_by = %s",
                (listing_ids, run_id),
            )

        cur.execute("""
            UPDATE runs
            SET status = %s,
                finished_at = now(),
                error_count = %s,
                total_count = %s
            WHERE run_id = %s::uuid
        """, (run_status, error_count, len(results), run_id))

    return {
        "run_id": run_id,
        "status": run_status,
        "total": len(results),
        "errors": error_count,
    }
