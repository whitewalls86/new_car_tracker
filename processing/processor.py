"""
Core artifact claim/process/release logic for the processing service.

Borrowed parse logic from scraper/app.py /process/results_pages and
/process/detail_pages endpoints. Added DB write path (HOT table upserts,
carousel hints, claim release) that n8n previously handled.

Flow:
  1. claim_batch()      — SELECT FOR UPDATE SKIP LOCKED, set status='processing'
  2. process_artifact() — dispatch by artifact_type
  3. _set_status()      — UPDATE artifacts_queue + INSERT artifacts_queue_events
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from scraper.processors.parse_detail_page import parse_cars_detail_page_html_v1
from scraper.processors.results_page_cards import parse_cars_results_page_html_v3
from shared.db import db_cursor
from shared.minio import read_html

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Queue management
# ---------------------------------------------------------------------------

def claim_batch(
    batch_size: int,
    artifact_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Atomically claim up to batch_size pending/retry artifacts.

    Uses FOR UPDATE SKIP LOCKED so concurrent callers never double-claim.
    Sets status='processing' and writes a 'processing' event for each row.
    Returns the claimed rows as plain dicts.
    """
    if artifact_type:
        type_filter = "AND artifact_type = %s"
        params: tuple = (artifact_type, batch_size)
    else:
        type_filter = ""
        params = (batch_size,)

    with db_cursor(error_context="claim_batch", dict_cursor=True) as cur:
        cur.execute(
            f"""
            UPDATE ops.artifacts_queue
            SET status = 'processing'
            WHERE artifact_id IN (
                SELECT artifact_id FROM ops.artifacts_queue
                WHERE status IN ('pending', 'retry')
                {type_filter}
                ORDER BY artifact_id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING artifact_id, minio_path, artifact_type,
                      listing_id, run_id, fetched_at
            """,
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]

    if rows:
        with db_cursor(error_context="claim_batch: write processing events") as cur:
            cur.executemany(
                """
                INSERT INTO staging.artifacts_queue_events
                    (artifact_id, status, minio_path, artifact_type,
                     fetched_at, listing_id, run_id)
                VALUES (%s, 'processing', %s, %s, %s, %s, %s)
                """,
                [
                    (
                        r["artifact_id"], r["minio_path"], r["artifact_type"],
                        r["fetched_at"], r["listing_id"], r["run_id"],
                    )
                    for r in rows
                ],
            )

    return rows


def _set_status(artifact: Dict[str, Any], status: str) -> None:
    """Update artifact queue status and write an event row."""
    with db_cursor(error_context=f"set_status {status}") as cur:
        cur.execute(
            "UPDATE ops.artifacts_queue SET status = %s WHERE artifact_id = %s",
            (status, artifact["artifact_id"]),
        )
        cur.execute(
            """
            INSERT INTO staging.artifacts_queue_events
                (artifact_id, status, minio_path, artifact_type,
                 fetched_at, listing_id, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                artifact["artifact_id"], status,
                artifact.get("minio_path"), artifact.get("artifact_type"),
                artifact.get("fetched_at"), artifact.get("listing_id"),
                artifact.get("run_id"),
            ),
        )


def queue_is_empty() -> bool:
    """Return True when no pending or retry artifacts remain."""
    with db_cursor(error_context="queue_is_empty", dict_cursor=True) as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.artifacts_queue"
            " WHERE status IN ('pending', 'retry')"
        )
        return cur.fetchone()["cnt"] == 0


# ---------------------------------------------------------------------------
# HTML read (mirrors scraper /process/* endpoints)
# ---------------------------------------------------------------------------

def _read_html(artifact: Dict[str, Any]) -> str:
    return read_html(artifact["minio_path"]).decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# HOT table helpers
# ---------------------------------------------------------------------------

def _upsert_price_observation(
    cur,
    *,
    listing_id: str,
    vin: Optional[str],
    price: Optional[int],
    make: Optional[str],
    model: Optional[str],
    last_seen_at: datetime,
    artifact_id: int,
) -> None:
    cur.execute(
        """
        INSERT INTO ops.price_observations
            (listing_id, vin, price, make, model, last_seen_at, last_artifact_id)
        VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (listing_id) DO UPDATE SET
            vin              = EXCLUDED.vin,
            price            = EXCLUDED.price,
            make             = EXCLUDED.make,
            model            = EXCLUDED.model,
            last_seen_at     = EXCLUDED.last_seen_at,
            last_artifact_id = EXCLUDED.last_artifact_id
        """,
        (listing_id, vin, price, make, model, last_seen_at, artifact_id),
    )


# ---------------------------------------------------------------------------
# Artifact processors
# ---------------------------------------------------------------------------

def process_results_page(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Parse results page HTML and upsert ops.price_observations."""
    artifact_id = artifact["artifact_id"]
    fetched_at = artifact["fetched_at"]
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at)

    try:
        html = _read_html(artifact)
    except Exception as e:
        logger.warning("results_page %s: MinIO read failed: %s", artifact_id, e)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    try:
        listings, parse_meta = parse_cars_results_page_html_v3(html)
    except Exception as e:
        logger.exception("results_page %s: parse failed", artifact_id)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e), "parse_meta": {}}

    upserted = 0
    errors = 0
    with db_cursor(error_context=f"results_page upserts {artifact_id}") as cur:
        for listing in listings:
            listing_id = listing.get("listing_id")
            if not listing_id:
                continue
            try:
                _upsert_price_observation(
                    cur,
                    listing_id=listing_id,
                    vin=listing.get("vin"),
                    price=listing.get("price"),
                    make=listing.get("make"),
                    model=listing.get("model"),
                    last_seen_at=fetched_at,
                    artifact_id=artifact_id,
                )
                upserted += 1
            except Exception as e:
                logger.warning(
                    "results_page %s: upsert failed listing_id=%s: %s",
                    artifact_id, listing_id, e,
                )
                errors += 1

    _set_status(artifact, "complete")
    return {
        "status": "complete",
        "listings_parsed": len(listings),
        "upserted": upserted,
        "errors": errors,
        "parse_meta": parse_meta,
    }


def process_detail_page(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Parse detail page HTML and write to HOT tables + carousel hints."""
    artifact_id = artifact["artifact_id"]
    listing_id = artifact.get("listing_id")
    fetched_at = artifact["fetched_at"]
    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at)

    artifact_url = (
        f"https://www.cars.com/vehicledetail/{listing_id}/" if listing_id else None
    )

    try:
        html = _read_html(artifact)
    except Exception as e:
        logger.warning("detail_page %s: MinIO read failed: %s", artifact_id, e)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e)}

    try:
        primary, carousel, parse_meta = parse_cars_detail_page_html_v1(html, artifact_url)
    except Exception as e:
        logger.exception("detail_page %s: parse failed", artifact_id)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e), "parse_meta": {}}

    resolved_listing_id = primary.get("listing_id") or listing_id
    listing_state = primary.get("listing_state", "active")
    vin = primary.get("vin")

    try:
        with db_cursor(error_context=f"detail_page writes {artifact_id}") as cur:
            if listing_state == "unlisted":
                if resolved_listing_id:
                    cur.execute(
                        "DELETE FROM ops.price_observations WHERE listing_id = %s::uuid",
                        (resolved_listing_id,),
                    )
            else:
                if resolved_listing_id:
                    _upsert_price_observation(
                        cur,
                        listing_id=resolved_listing_id,
                        vin=vin,
                        price=primary.get("price"),
                        make=primary.get("make"),
                        model=primary.get("model"),
                        last_seen_at=fetched_at,
                        artifact_id=artifact_id,
                    )
                if vin and resolved_listing_id:
                    cur.execute(
                        """
                        INSERT INTO ops.vin_to_listing
                            (vin, listing_id, mapped_at, artifact_id)
                        VALUES (%s, %s::uuid, %s, %s)
                        ON CONFLICT (vin) DO UPDATE SET
                            listing_id  = EXCLUDED.listing_id,
                            mapped_at   = EXCLUDED.mapped_at,
                            artifact_id = EXCLUDED.artifact_id
                        """,
                        (vin, resolved_listing_id, fetched_at, artifact_id),
                    )

            # Carousel hints — insert all, dbt filters by search_configs
            if carousel and resolved_listing_id:
                cur.executemany(
                    """
                    INSERT INTO public.detail_carousel_hints
                        (artifact_id, fetched_at, source_listing_id, listing_id,
                         price, mileage, body, condition, year)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            artifact_id, fetched_at, resolved_listing_id,
                            c["listing_id"], c.get("price"), c.get("mileage"),
                            c.get("body"), c.get("condition"), c.get("year"),
                        )
                        for c in carousel
                        if c.get("listing_id")
                    ],
                )

            # Release the detail scrape claim
            if resolved_listing_id:
                cur.execute(
                    "DELETE FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid",
                    (resolved_listing_id,),
                )
                cur.execute(
                    """
                    INSERT INTO staging.detail_scrape_claim_events
                        (listing_id, run_id, status)
                    VALUES (%s::uuid, %s::uuid, 'processed')
                    """,
                    (resolved_listing_id, artifact.get("run_id")),
                )

    except Exception as e:
        logger.exception("detail_page %s: DB writes failed", artifact_id)
        _set_status(artifact, "retry")
        return {"status": "retry", "error": str(e), "parse_meta": parse_meta}

    _set_status(artifact, "complete")
    return {
        "status": "complete",
        "listing_state": listing_state,
        "listing_id": resolved_listing_id,
        "vin": vin,
        "carousel_hints": len(carousel),
        "parse_meta": parse_meta,
    }


def process_artifact(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch to the correct processor based on artifact_type."""
    artifact_type = artifact.get("artifact_type")
    if artifact_type == "results_page":
        return process_results_page(artifact)
    if artifact_type == "detail_page":
        return process_detail_page(artifact)
    logger.warning(
        "Unknown artifact_type=%s for artifact_id=%s",
        artifact_type, artifact.get("artifact_id"),
    )
    _set_status(artifact, "skip")
    return {"status": "skip", "reason": f"unknown artifact_type: {artifact_type}"}
