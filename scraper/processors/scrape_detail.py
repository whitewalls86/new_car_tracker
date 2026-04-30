from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from scraper.processors.cf_session import (
    FLARESOLVERR_URL,
    get_cf_credentials,
    invalidate_cf_credentials,
    make_cf_session,
)
from scraper.queries import (
    GET_BLOCKED_COOLDOWN_ATTEMPTS,
    INSERT_BLOCKED_COOLDOWN_EVENT,
    UPSERT_BLOCKED_COOLDOWN,
)

# Adaptive delay for detail fetches: backs off on 403, recovers on success.
_detail_delay_lock = threading.Lock()
_detail_adaptive_delay: float = 0.0

logger = logging.getLogger("scraper")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _update_detail_delay(is_403: bool) -> None:
    """
    Adjust the module-level adaptive delay based on the last fetch outcome.

    403 → back off: delay = min(max(delay * 2, 0.5), 30.0)
    success → recover: delay = max(delay * 0.85, 0.0)
    """
    global _detail_adaptive_delay
    with _detail_delay_lock:
        old = _detail_adaptive_delay
        if is_403:
            _detail_adaptive_delay = min(max(_detail_adaptive_delay * 2, 0.5), 30.0)
        else:
            _detail_adaptive_delay = max(_detail_adaptive_delay * 0.85, 0.0)
        new = _detail_adaptive_delay
    if is_403:
        logger.warning("Adaptive delay backed off: %.2fs → %.2fs (403 received)", old, new)
    elif old > 0:
        logger.info("Adaptive delay recovering: %.2fs → %.2fs (success)", old, new)


def _fetch_url(url: str, timeout_s: int) -> tuple[bytes, int, Optional[str], str]:
    """Fetch url using a CF-bootstrapped curl_cffi session, or plain curl_cffi fallback.

    Returns (content_bytes, http_status, content_type, final_url).
    """
    if FLARESOLVERR_URL:
        try:
            credentials, bootstrap_html, bootstrap_status = get_cf_credentials(url, timeout_s)
            if bootstrap_html is not None:
                return bootstrap_html, bootstrap_status, "text/html; charset=utf-8", url
            session = make_cf_session(credentials)
            resp = session.get(url, timeout=timeout_s, allow_redirects=True)
            content = resp.content or b""
            return content, resp.status_code, resp.headers.get("content-type"), str(resp.url)
        except Exception as e:
            logger.warning(
                "FlareSolverr/CF session failed (%s), falling back to plain curl_cffi", e
            )

    session = make_cf_session(None)
    resp = session.get(url, timeout=timeout_s, allow_redirects=True)
    content = resp.content or b""
    return content, resp.status_code, resp.headers.get("content-type"), str(resp.url)


def scrape_detail_fetch(*, run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Production detail-page scraper processor.

    Inputs (payload):
      listing_id: required
      batch_id: optional UUID identifying the scrape batch; used as search_key in
                artifacts so each scrape_jobs row maps 1:1 to its artifacts.
                Defaults to run_id if omitted.
      url: optional (defaults to https://www.cars.com/vehicledetail/<listing_id>/)
      vin: optional
      timeout_s: optional
      headers: optional override headers dict

    Returns:
      { "artifacts": [...], "meta": {...}, "error": None|str }
    """
    listing_id = (payload or {}).get("listing_id")
    vin = (payload or {}).get("vin")
    batch_id = (payload or {}).get("batch_id") or run_id
    default_url = (
        f"https://www.cars.com/vehicledetail/{listing_id}/"
        if listing_id
        else None
    )
    url = (payload or {}).get("url") or default_url

    logger.info(
        "scrape_detail_fetch: listing_id=%s run_id=%s payload_batch_id=%s resolved_batch_id=%s",
        listing_id, run_id, (payload or {}).get("batch_id"), batch_id,
    )

    if not listing_id:
        return {
            "error": "payload.listing_id is required", 
            "artifacts": [], 
            "meta": {"mode": "fetch"}
        }
    if not url:
        return {
            "error": "payload.url could not be derived", 
            "artifacts": [], 
            "meta": {"mode": "fetch", "listing_id": listing_id}
        }

    raw_base = os.environ.get("RAW_BASE", "/data/raw")
    run_dir = os.path.join(raw_base, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    fetched_at = datetime.now(UTC).isoformat()
    timeout_s = int((payload or {}).get("timeout_s") or 30)

    # We always write *something* for auditability.
    # Non-200 responses get written too (useful for debugging blocks/interstitials).
    try:
        content, status, content_type, final_url = _fetch_url(url, timeout_s)
        size = len(content)

        filename = f"detail_{listing_id}__{status}.html"
        filepath = os.path.join(run_dir, filename)
        with open(filepath, "wb") as f:
            f.write(content)

        # Write to MinIO and record in artifacts_queue (Plan 97)
        minio_path = None
        queue_artifact_id = None
        try:
            from shared.db import db_cursor
            from shared.minio import make_key, write_html

            key = make_key("detail_page", fetched_at)
            minio_path = write_html(key, content)

            with db_cursor(error_context="scrape_detail_fetch: insert artifacts_queue") as cur:
                cur.execute(
                    """
                    INSERT INTO ops.artifacts_queue
                        (minio_path, artifact_type, listing_id, run_id, fetched_at, status)
                    VALUES (%s, 'detail_page', %s, %s, %s, 'pending')
                    RETURNING artifact_id
                    """,
                    (minio_path, str(listing_id), run_id or None, fetched_at),
                )
                queue_artifact_id = cur.fetchone()[0]
                cur.execute(
                    """
                    INSERT INTO staging.artifacts_queue_events (
                        artifact_id, status, minio_path, artifact_type, 
                        fetched_at, listing_id, run_id
                    )
                    VALUES (%s, 'pending', %s, 'detail_page', %s, %s, %s)
                    """,
                    (queue_artifact_id, minio_path, fetched_at,
                     str(listing_id) if listing_id else None, run_id or None),
                )
        except Exception as _minio_err:
            logger.warning("MinIO/queue write failed (non-fatal): %s", _minio_err)

        if status != 200:
            logger.warning(
                "detail fetch HTTP %s for listing_id=%s url=%s",
                status, listing_id, final_url,
            )

        if status == 403:
            try:
                from shared.db import db_cursor
                with db_cursor(error_context="scrape_detail_fetch: blocked_cooldown") as cur:
                    cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
                    cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": listing_id})
                    row = cur.fetchone()
                    num_attempts = row[0] if row else 1
                    event_type = "blocked" if num_attempts == 1 else "incremented"
                    cur.execute(INSERT_BLOCKED_COOLDOWN_EVENT, {
                        "listing_id": listing_id,
                        "event_type": event_type,
                        "num_of_attempts": num_attempts,
                    })
                logger.warning(
                    "detail fetch 403 listing_id=%s: blocked_cooldown updated (attempts=%d)",
                    listing_id, num_attempts,
                )
            except Exception as _blocked_err:
                logger.warning("blocked_cooldown write failed (non-fatal): %s", _blocked_err)

        artifact = {
            "source": "cars.com",
            "artifact_type": "detail_page",
            "listing_id": listing_id,
            "search_key": batch_id,
            "search_scope": "detail",
            "page_num": None,
            "url": final_url,
            "fetched_at": fetched_at,
            "http_status": status,
            "content_type": content_type,
            "content_bytes": size,
            "sha256": _sha256_bytes(content) if content else None,
            "filepath": filepath,
            "minio_path": minio_path,
            "queue_artifact_id": queue_artifact_id,
            "error": None if status == 200 else f"HTTP {status}",
        }

        return {
            "error": None if status == 200 else f"HTTP {status}",
            "artifacts": [artifact],
            "meta": {
                "mode": "fetch",
                "listing_id": listing_id,
                "vin": vin,
                "final_url": final_url,
            },
        }

    except Exception as e:
        # Write an error marker file so every attempt leaves a disk trace.
        filename = f"detail_{listing_id}__ERROR.txt"
        filepath = os.path.join(run_dir, filename)
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"{type(e).__name__}: {e}\n")
                f.write(f"url={url}\n")
        except Exception:
            pass

        return {
            "error": f"{type(e).__name__}: {e}",
            "artifacts": [
                {
                    "source": "cars.com",
                    "artifact_type": "detail_page",
                    "listing_id": listing_id,
                    "search_key": batch_id,
                    "search_scope": "detail",
                    "page_num": None,
                    "url": url,
                    "fetched_at": fetched_at,
                    "http_status": None,
                    "content_type": None,
                    "content_bytes": None,
                    "sha256": None,
                    "filepath": filepath,
                    "error": f"{type(e).__name__}: {e}",
                }
            ],
            "meta": {"mode": "fetch", "listing_id": listing_id, "vin": vin},
        }


def scrape_detail_batch(
    *, run_id: str, batch_id: str, listings: List[Dict[str, Any]], max_workers: int = 8
) -> Dict[str, Any]:
    """
    Fetch detail pages for a list of listings concurrently.

    Uses a thread pool with an adaptive per-request delay that backs off when
    403 responses are detected and gradually recovers on success. On each 403,
    the cached CF credentials are also invalidated so the next request triggers
    a fresh FlareSolverr bootstrap.

    batch_id: UUID identifying this batch; written as search_key on every artifact
              so scrape_jobs rows map 1:1 to their artifacts.
    listings: [{"listing_id": ..., "vin": ..., "url": ...}, ...]
    Returns: {"artifacts": [...], "meta": {...}}
    """

    def _fetch_one(item: Dict[str, Any]) -> Dict[str, Any]:
        with _detail_delay_lock:
            delay = _detail_adaptive_delay
        if delay > 0:
            logger.info(
                "Adaptive delay %.2fs before fetching listing_id=%s",
                delay, item.get("listing_id"),
            )
            time.sleep(delay)
        result = scrape_detail_fetch(run_id=run_id, payload={**item, "batch_id": batch_id})
        is_403 = any(a.get("http_status") == 403 for a in result.get("artifacts", []))
        _update_detail_delay(is_403)
        if is_403:
            invalidate_cf_credentials()
        return result

    all_artifacts: List[Dict[str, Any]] = []
    error_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, listing): listing for listing in listings}
        for future in as_completed(futures):
            try:
                result = future.result()
                all_artifacts.extend(result.get("artifacts", []))
                if result.get("error"):
                    error_count += 1
            except Exception as e:
                logger.warning("Detail batch fetch raised: %s", e)
                error_count += 1

    return {
        "artifacts": all_artifacts,
        "meta": {
            "mode": "batch",
            "total": len(listings),
            "succeeded": len(listings) - error_count,
            "errors": error_count,
        },
    }


def _write_dummy_detail_html(listing_id: str, vin: Optional[str]) -> bytes:
    vin_str = vin or ""
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Dummy Cars.com Detail - {listing_id}</title>
  </head>
  <body>
    <h1>Dummy Cars.com Detail Page</h1>

    <section id="primary-listing">
      <script id="primary-listing-json" type="application/json">
        {{
          "listing_id": "{listing_id}",
          "vin": "{vin_str}",
          "seller_customer_id": null,
          "dealer_name": null
        }}
      </script>
    </section>

    <section id="listings-carousel">
      <div class="carousel-item"
           data-listing-id="{listing_id}"
           data-price="0"
           data-mileage="0"></div>
    </section>

  </body>
</html>
"""
    return html.encode("utf-8")


def scrape_detail_dummy(*, run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dummy detail-page scraper processor.

    Returns the same contract your n8n expects:
      { "artifacts": [...], "meta": {...}, "error": None|str }
    """
    listing_id = (payload or {}).get("listing_id")
    vin = (payload or {}).get("vin")
    batch_id = (payload or {}).get("batch_id") or run_id
    url = (payload or {}).get("url") or f"https://www.cars.com/vehicledetail/{listing_id}/"

    if not listing_id:
        return {
            "error": "payload.listing_id is required", 
            "artifacts": [], 
            "meta": {"mode": "dummy"}
        }

    raw_base = os.environ.get("RAW_BASE", "/data/raw")
    run_dir = os.path.join(raw_base, f"run_{run_id}")
    os.makedirs(run_dir, exist_ok=True)

    fetched_at = datetime.now(UTC).isoformat()
    filepath = os.path.join(run_dir, f"detail_{listing_id}.html")

    try:
        content = _write_dummy_detail_html(listing_id=listing_id, vin=vin)
        with open(filepath, "wb") as f:
            f.write(content)

        artifact = {
            "source": "cars.com",
            "artifact_type": "detail_page",
            "listing_id": listing_id,
            "search_key": batch_id,
            "search_scope": "detail",
            "page_num": None,
            "url": url,
            "fetched_at": fetched_at,
            "http_status": 200,
            "content_type": "text/html; charset=utf-8",
            "content_bytes": len(content),
            "sha256": _sha256_bytes(content),
            "filepath": filepath,
            "error": None,
            "minio_path": None,
            "queue_artifact_id": None,
        }

        return {
            "error": None,
            "artifacts": [artifact],
            "meta": {"mode": "dummy", "listing_id": listing_id, "vin": vin, "wrote": True},
        }

    except Exception as e:
        return {
            "error": f"failed to write dummy detail artifact: {type(e).__name__}: {e}",
            "artifacts": [
                {
                    "source": "cars.com",
                    "artifact_type": "detail_page",
                    "listing_id": listing_id,
                    "search_key": batch_id,
                    "search_scope": "detail",
                    "page_num": None,
                    "url": url,
                    "fetched_at": fetched_at,
                    "http_status": None,
                    "content_type": None,
                    "content_bytes": None,
                    "sha256": None,
                    "filepath": filepath,
                    "error": f"{type(e).__name__}: {e}",
                    "minio_path": None,
                    "queue_artifact_id": None,
                }
            ],
            "meta": {"mode": "dummy", "listing_id": listing_id, "vin": vin, "wrote": False},
        }
