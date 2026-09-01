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

from ops.queries import (
    CLAIM_DETAIL_SCRAPE_BATCH,
    DELETE_DETAIL_SCRAPE_CLAIMS,
    MARK_ROTATION_SLOT_QUEUED,
    MARK_SEARCH_CONFIG_QUEUED,
    RECORD_DETAIL_FETCHES,
    SELECT_LAST_QUEUED_AT,
    SELECT_LEGACY_SEARCH_CONFIG,
    SELECT_NEXT_ROTATION_SLOT,
    SELECT_ROTATION_SLOT_CONFIGS,
)
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
    2. min_gap_minutes (default 230 = ~3h50m): blocks if ANY config's
       last_queued_at falls within this window. Prevents multiple slots from
       firing in rapid succession even if all have stale timestamps.

    Returns {"slot": null, "configs": [], "run_id": null} when nothing is due.
    Returns {"slot": ..., "configs": [...], "run_id": "<uuid>"} when work is claimed.
    run_id is a fresh UUID for the caller to pass to the scraper; no runs row is inserted.
    """
    with db_cursor(error_context="advance_rotation") as cur:
        # Guard: check time since last search config was queued
        cur.execute(SELECT_LAST_QUEUED_AT)
        row = cur.fetchone()
        last_queued = row[0] if row else None
        if last_queued:
            gap = datetime.datetime.now(datetime.timezone.utc) - last_queued
            if gap.total_seconds() < min_gap_minutes * 60:
                return {
                    "slot": None,
                    "configs": [],
                    "run_id": None,
                    "reason": "too_soon",
                    "last_run_minutes_ago": round(gap.total_seconds() / 60, 1),
                }

        # Find the next due slot
        cur.execute(SELECT_NEXT_ROTATION_SLOT, (min_idle_minutes,))
        slot_row = cur.fetchone()

        if slot_row is None:
            # Fallback: try legacy single-config (no rotation_slot)
            cur.execute(SELECT_LEGACY_SEARCH_CONFIG, (min_idle_minutes,))
            row = cur.fetchone()

            if not row:
                return {"slot": None, "configs": [], "run_id": None}

            cur.execute(MARK_SEARCH_CONFIG_QUEUED, (row[0],))
            raw_params = row[1]
            params = json.loads(raw_params) if isinstance(raw_params, str) else dict(raw_params)
            return {
                "slot": None,
                "run_id": str(uuid.uuid4()),
                "configs": [{
                    "search_key": row[0],
                    "params": params,
                    "scopes": params.get("scopes", ["local", "national"]),
                }],
            }

        slot = slot_row[0]

        # Claim all configs in this slot
        cur.execute(MARK_ROTATION_SLOT_QUEUED, (slot,))

        cur.execute(SELECT_ROTATION_SLOT_CONFIGS, (slot,))
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

    return {"slot": slot, "run_id": str(uuid.uuid4()), "configs": configs}


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
        cur.execute(CLAIM_DETAIL_SCRAPE_BATCH, (batch_size, run_id))

        rows = cur.fetchall()
        if cur.description is None:
            raise ValueError("Query returned no result set")
        col_names = [desc[0] for desc in cur.description]

    listings = [dict(zip(col_names, row)) for row in rows]

    return {"run_id": run_id, "listings": listings}


# A detail request was spent for these outcomes, so the fetch backoff applies.
# 'skipped' is excluded: that listing was never attempted.
FETCH_SPENDING_STATUSES = ("ok", "failed")


@router.post("/claims/release")
def release_claims(body: ReleaseRequest) -> Dict[str, Any]:
    """
    Releases claims after a scrape batch completes.

    Deletes the claim rows for the given run_id and records, on the same
    transaction, that a detail request was spent on each listing the scraper
    actually attempted.

    That second write is the loop guard (Plan 147). Before it, the only thing
    stopping a listing being re-fetched was a timestamp written by the
    *processing* service two hops downstream, so any break in that chain —
    processing paused, crashed, backed up, or a parser gap — left the claims
    deleted, the guard unwritten, and the same batch re-claimed fifteen minutes
    later. The component that spends the resource now records having spent it.

    last_detail_fetched_at is set for 'ok' and 'failed' results but not
    'skipped': the column means "we spent a request", and a failed fetch spent
    one. Blocked and delisted listings keep their existing cooldown paths; this
    does not replace them.

    Note that the three-way rule is a contract no caller exercises today.
    scrape_detail_pages reports 'ok' for every claimed listing regardless of
    outcome, which is correct for a loop guard — the batch consumed requests
    either way — so in production every released listing is stamped.

    The DELETE is this handler's only other statement. It does not mark any run
    finished and never has, despite what its docstring claimed until Plan 147:
    there is no runs-table write here.
    """
    run_id = body.run_id
    results = body.results

    listing_ids = [r.listing_id for r in results]
    fetched_ids = [r.listing_id for r in results if r.status in FETCH_SPENDING_STATUSES]
    error_count = sum(1 for r in results if r.status == "failed")

    with db_cursor(error_context="release_claims") as cur:
        if listing_ids:
            cur.execute(DELETE_DETAIL_SCRAPE_CLAIMS, (listing_ids, run_id))
        if fetched_ids:
            # Same cursor, so this commits or rolls back with the DELETE above:
            # a released claim and its recorded fetch are never separated.
            # ops connects as PGUSER=cartracker, the owner of
            # ops.price_observations, so no additional grant is needed.
            cur.execute(RECORD_DETAIL_FETCHES, (fetched_ids,))

    return {
        "run_id": run_id,
        "total": len(results),
        "errors": error_count,
        "fetches_recorded": len(fetched_ids),
    }
