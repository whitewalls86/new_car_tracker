"""
Backfill ops.blocked_cooldown and staging.blocked_cooldown_events from scraper logs.

The 403-at-fetch-time write path was added in fix/403-rate-tracking. This script:

  1. Queries MinIO blocked_cooldown_events Parquet to identify the data gap
     (MAX(event_at) before Apr 30 → gap start; MIN(event_at) after Apr 29 → gap end).
  2. Parses app.log* and keeps 403s that fall within the gap window.
  3. Aggregates log data per listing_id: count, first_attempted_at, last_attempted_at.
  4. Inserts one staging.blocked_cooldown_events row per log line (full history).
  5. Cross-references MinIO detail_scrape_claim_events to exclude listing_ids that had
     a successful scrape after their last 403 — those are no longer blocked.
  6. Upserts ops.blocked_cooldown for the remaining (still-blocked) listing_ids:
       - INSERT for listing_ids not already present
       - UPDATE num_of_attempts + timestamps for ones already present

Usage (on VM, with DB + MinIO env vars set):
    python3 scripts/backfill_blocked_cooldown.py \
        --log-dir /mnt/data/docker-volumes/cartracker_scraper_logs/_data \
        [--dry-run]

Env vars required:
    DATABASE_URL or PGHOST/PGPORT/PGDATABASE/PGUSER/POSTGRES_PASSWORD
    MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
    MINIO_BUCKET (default: bronze)
"""
import argparse
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO_ENDPOINT      = os.environ.get("MINIO_ENDPOINT", "").replace("http://", "")
MINIO_USER          = os.environ.get("MINIO_ROOT_USER", "")
MINIO_PASSWORD      = os.environ.get("MINIO_ROOT_PASSWORD", "")
BUCKET              = os.environ.get("MINIO_BUCKET", "bronze")
EVENTS_PREFIX       = f"s3://{BUCKET}/ops/blocked_cooldown_events"
CLAIM_EVENTS_PREFIX = f"s3://{BUCKET}/ops/detail_scrape_claim_events"


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------

def _duckdb_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}'")
    con.execute(f"SET s3_access_key_id='{MINIO_USER}'")
    con.execute(f"SET s3_secret_access_key='{MINIO_PASSWORD}'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")
    return con


# ---------------------------------------------------------------------------
# Step 1: identify the gap window from MinIO
# ---------------------------------------------------------------------------

def get_gap_window() -> tuple[datetime | None, datetime | None]:
    """
    gap_start = MAX(event_at) WHERE event_at < '2026-04-30'  → last event before gap
    gap_end   = MIN(event_at) WHERE event_at > '2026-04-29'  → first event after fix

    Either may be None; both None means no Parquet exists yet → backfill everything.
    """
    con = _duckdb_con()
    gap_start = gap_end = None

    try:
        row = con.execute(f"""
            SELECT
                MAX(CASE WHEN event_at < '2026-04-30' THEN event_at END) AS gap_start,
                MIN(CASE WHEN event_at > '2026-04-29' THEN event_at END) AS gap_end
            FROM read_parquet('{EVENTS_PREFIX}/**/*.parquet')
        """).fetchone()
        if row:
            gap_start, gap_end = row[0], row[1]
            if gap_start and gap_start.tzinfo is None:
                gap_start = gap_start.replace(tzinfo=timezone.utc)
            if gap_end and gap_end.tzinfo is None:
                gap_end = gap_end.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.info("No blocked_cooldown_events in MinIO (%s) — backfilling all", e)

    logger.info("Gap window: %s → %s", gap_start or "beginning", gap_end or "end of logs")
    return gap_start, gap_end


# ---------------------------------------------------------------------------
# Step 2: parse logs, filter to gap
# ---------------------------------------------------------------------------

_LOG_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ WARNING scraper: "
    r"detail fetch HTTP 403 for listing_id=(?P<listing_id>[0-9a-f-]+) "
)


def parse_logs(
    log_dir: Path,
    gap_start: datetime | None,
    gap_end: datetime | None,
) -> list[tuple[datetime, str]]:
    """
    Return (timestamp, listing_id) tuples within (gap_start, gap_end), sorted
    chronologically.
    """
    events = []
    log_files = sorted(log_dir.glob("app.log*"))
    if not log_files:
        logger.error("No app.log* files found in %s", log_dir)
        sys.exit(1)

    logger.info("Parsing %d log file(s)", len(log_files))
    for log_file in log_files:
        with open(log_file, encoding="utf-8", errors="replace") as f:
            for line in f:
                m = _LOG_RE.match(line)
                if not m:
                    continue
                ts = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
                if gap_start is not None and ts <= gap_start:
                    continue
                if gap_end is not None and ts >= gap_end:
                    continue
                events.append((ts, m.group("listing_id")))

    events.sort(key=lambda e: e[0])
    logger.info(
        "Found %d 403 events (%d unique listing_ids) in gap",
        len(events), len({lid for _, lid in events}),
    )
    return events


# ---------------------------------------------------------------------------
# Step 3: aggregate per listing_id
# ---------------------------------------------------------------------------

def aggregate(
    events: list[tuple[datetime, str]],
) -> dict[str, dict]:
    """
    Returns {listing_id: {count, first_at, last_at}} from the sorted event list.
    """
    agg: dict[str, dict] = {}
    for ts, listing_id in events:
        if listing_id not in agg:
            agg[listing_id] = {"count": 0, "first_at": ts, "last_at": ts}
        rec = agg[listing_id]
        rec["count"] += 1
        if ts < rec["first_at"]:
            rec["first_at"] = ts
        if ts > rec["last_at"]:
            rec["last_at"] = ts
    logger.info("Aggregated %d unique listing_ids", len(agg))
    return agg


# ---------------------------------------------------------------------------
# Step 5: find listing_ids with a successful scrape after their last 403
# ---------------------------------------------------------------------------

def get_successfully_scraped(agg: dict[str, dict]) -> set[str]:
    """
    Query MinIO detail_scrape_claim_events for listing_ids that have a
    status='processed' event AFTER their last 403 timestamp.
    These are no longer blocked and should be excluded from blocked_cooldown.
    """
    con = _duckdb_con()

    # Build a VALUES table so DuckDB can do the per-listing timestamp comparison.
    values_rows = ", ".join(
        f"('{lid}'::uuid, '{rec['last_at'].isoformat()}'::timestamptz)"
        for lid, rec in agg.items()
    )

    try:
        rows = con.execute(f"""
            WITH last_403 AS (
                SELECT column0 AS listing_id, column1 AS last_403_at
                FROM (VALUES {values_rows}) t
            )
            SELECT DISTINCT c.listing_id::text
            FROM read_parquet('{CLAIM_EVENTS_PREFIX}/**/*.parquet') c
            JOIN last_403 l ON c.listing_id::uuid = l.listing_id
            WHERE c.status = 'processed'
              AND c.event_at > l.last_403_at
        """).fetchall()
        cleared = {row[0] for row in rows}
    except Exception as e:
        logger.warning("Could not query claim events from MinIO (%s) — assuming none cleared", e)
        cleared = set()

    if cleared:
        logger.info(
            "%d listing_ids had a successful scrape after their last 403 — "
            "excluding from blocked_cooldown", len(cleared),
        )
    return cleared


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn():
    database_url = os.environ.get("DATABASE_URL", "")
    if database_url:
        p = urlparse(database_url)
        kwargs = {
            "host":     p.hostname or "postgres",
            "port":     p.port or 5432,
            "dbname":   p.path.lstrip("/") or "cartracker",
            "user":     p.username or "cartracker",
            "password": p.password or "",
        }
    else:
        kwargs = {
            "host":     os.environ.get("PGHOST", "postgres"),
            "port":     int(os.environ.get("PGPORT", "5432")),
            "dbname":   os.environ.get("PGDATABASE", "cartracker"),
            "user":     os.environ.get("PGUSER", "cartracker"),
            "password": os.environ.get("POSTGRES_PASSWORD", ""),
        }
    import psycopg2
    return psycopg2.connect(**kwargs)


_INSERT_EVENT = """
INSERT INTO staging.blocked_cooldown_events
    (listing_id, event_type, num_of_attempts, event_at)
VALUES
    (%(listing_id)s, %(event_type)s, %(num_of_attempts)s, %(event_at)s)
"""

# Insert for new listing_ids; update counts + timestamps for existing ones.
# Does NOT touch listing_ids that have been successfully scraped since their last 403.
_UPSERT_COOLDOWN = """
INSERT INTO ops.blocked_cooldown
    (listing_id, first_attempted_at, last_attempted_at, num_of_attempts)
VALUES
    (%(listing_id)s, %(first_at)s, %(last_at)s, %(count)s)
ON CONFLICT (listing_id) DO UPDATE SET
    first_attempted_at = LEAST(ops.blocked_cooldown.first_attempted_at, EXCLUDED.first_attempted_at),
    last_attempted_at  = GREATEST(ops.blocked_cooldown.last_attempted_at, EXCLUDED.last_attempted_at),
    num_of_attempts    = ops.blocked_cooldown.num_of_attempts + EXCLUDED.num_of_attempts
"""


# ---------------------------------------------------------------------------
# Steps 4 + 6: write events + update cooldown
# ---------------------------------------------------------------------------

def _get_existing_cooldown_ids(cur, listing_ids: list[str]) -> set[str]:
    """Return the subset of listing_ids already present in ops.blocked_cooldown."""
    cur.execute(
        "SELECT listing_id::text FROM ops.blocked_cooldown"
        " WHERE listing_id = ANY(%s::uuid[])",
        (listing_ids,),
    )
    return {row[0] for row in cur.fetchall()}


def backfill(
    events: list[tuple[datetime, str]],
    agg: dict[str, dict],
    cleared: set[str],
    dry_run: bool,
) -> None:
    conn = _get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cooldown_ids = [lid for lid in agg if lid not in cleared]
            existing_ids = _get_existing_cooldown_ids(cur, cooldown_ids)
            insert_ids = [lid for lid in cooldown_ids if lid not in existing_ids]
            update_ids = [lid for lid in cooldown_ids if lid in existing_ids]

            if dry_run:
                _print_dry_run_summary(events, agg, cleared, insert_ids, update_ids, existing_ids)
                return

            # Step 4: insert one event row per log line (all listing_ids, incl. cleared).
            attempt_counts: dict[str, int] = {}
            for event_at, listing_id in events:
                attempt_counts[listing_id] = attempt_counts.get(listing_id, 0) + 1
                num_attempts = attempt_counts[listing_id]
                cur.execute(_INSERT_EVENT, {
                    "listing_id": listing_id,
                    "event_type": "blocked" if num_attempts == 1 else "incremented",
                    "num_of_attempts": num_attempts,
                    "event_at": event_at,
                })

            # Step 6: upsert blocked_cooldown for still-blocked listing_ids only.
            for listing_id in cooldown_ids:
                rec = agg[listing_id]
                cur.execute(_UPSERT_COOLDOWN, {
                    "listing_id": listing_id,
                    "first_at":   rec["first_at"],
                    "last_at":    rec["last_at"],
                    "count":      rec["count"],
                })

            conn.commit()
            logger.info(
                "Done. %d event rows inserted into staging.blocked_cooldown_events. "
                "%d listing_ids inserted + %d updated in blocked_cooldown. "
                "%d skipped (successfully scraped after last 403).",
                len(events), len(insert_ids), len(update_ids), len(cleared),
            )

    except Exception:
        conn.rollback()
        logger.exception("Backfill failed — rolled back")
        sys.exit(1)
    finally:
        conn.close()


def _print_dry_run_summary(
    events: list[tuple[datetime, str]],
    agg: dict[str, dict],
    cleared: set[str],
    insert_ids: list[str],
    update_ids: list[str],
    existing_ids: set[str],
) -> None:
    sep = "-" * 60

    print(f"\n{sep}")
    print("STEP 2 — Log events in gap")
    print(sep)
    print(f"  Total 403 log lines:         {len(events)}")
    print(f"  Unique listing_ids:          {len(agg)}")
    if events:
        print(f"  Earliest event:              {events[0][0]}")
        print(f"  Latest event:                {events[-1][0]}")

    print(f"\n{sep}")
    print("STEP 3 — Aggregation per listing_id")
    print(sep)
    counts = [rec["count"] for rec in agg.values()]
    print(f"  Listing_ids with 1 block:    {sum(1 for c in counts if c == 1)}")
    print(f"  Listing_ids with 2 blocks:   {sum(1 for c in counts if c == 2)}")
    print(f"  Listing_ids with 3+ blocks:  {sum(1 for c in counts if c >= 3)}")
    print(f"  Max blocks for one listing:  {max(counts) if counts else 0}")

    print(f"\n{sep}")
    print("STEP 4 — staging.blocked_cooldown_events inserts")
    print(sep)
    first_blocks = sum(1 for rec in agg.values())
    incremented  = len(events) - first_blocks
    print(f"  Rows to insert (total):      {len(events)}")
    print(f"    event_type='blocked':       {first_blocks}")
    print(f"    event_type='incremented':   {incremented}")

    print(f"\n{sep}")
    print("STEP 5 — Listings cleared by successful scrape")
    print(sep)
    print(f"  Excluded from blocked_cooldown: {len(cleared)}")
    if cleared:
        for lid in sorted(cleared):
            rec = agg[lid]
            print(f"    {lid}  (last_403={rec['last_at']})")

    print(f"\n{sep}")
    print("STEP 6 — ops.blocked_cooldown upsert")
    print(sep)
    print(f"  Total to upsert:             {len(insert_ids) + len(update_ids)}")
    print(f"  INSERT (new rows):           {len(insert_ids)}")
    print(f"  UPDATE (existing rows):      {len(update_ids)}")
    if update_ids:
        print("  Existing rows that will be updated:")
        for lid in sorted(update_ids):
            rec = agg[lid]
            print(
                f"    {lid}  +{rec['count']} attempts  "
                f"first={rec['first_at']}  last={rec['last_at']}"
            )
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log-dir",
        default="/mnt/data/docker-volumes/cartracker_scraper_logs/_data",
        help="Directory containing app.log* files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without touching the DB",
    )
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    if not log_dir.is_dir():
        logger.error("--log-dir %s does not exist", log_dir)
        sys.exit(1)

    gap_start, gap_end = get_gap_window()
    events = parse_logs(log_dir, gap_start=gap_start, gap_end=gap_end)

    if not events:
        logger.info("No gap events found — nothing to backfill")
        return

    agg = aggregate(events)
    cleared = get_successfully_scraped(agg)
    backfill(events, agg, cleared, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
