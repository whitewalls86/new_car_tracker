#!/usr/bin/env python3
"""Size the fetched-but-unenriched detail population from the lake.

Plan 147 Stage 3. The plan defers escalating backoff -- and an attempt
counter -- until this number is known:

    a permanently unparseable page still loops, at 4x/day instead of 96x/day.
    A 24x improvement is enough to close the acute defect, and escalating
    backoff or an attempt counter is deliberately deferred until the new
    metric below shows whether that population is material.

Plan 147 proposed answering that with a live Prometheus gauge over
``ops.price_observations``. It is answered here instead, because the events
already record it and a gauge cannot: a gauge is a point-in-time count, and
the question is "how many times has this listing cost us a request and
returned nothing", which is a per-listing count over a window.

The join is:

* ``ops_normalized/artifacts_queue_events`` -- one row per queue transition,
  flushed from ``staging.artifacts_queue_events`` by the archiver. Carries
  ``artifact_id``, ``status``, ``fetched_at``, ``listing_id``. This is what we
  fetched.
* ``silver_normalized/observations`` -- parsed observations, keyed by
  ``artifact_id``. This is what came back.

A detail artifact with no observation is a request we spent for nothing.

Two traps, both hit while writing this:

* **The event partition is on ``event_at``, not ``fetched_at``.** A recovery
  sweep that marks April artifacts ``recovered`` in August files those events
  under ``year=2026/month=8``. Reading the partition as "August fetches"
  overstates the fruitless count by an order of magnitude -- 341,903 April
  artifacts appeared in the August partition when this was written. Filter on
  ``fetched_at``, which is what ``--since`` does.
* **Observations for a month-boundary artifact land in the next partition.**
  The observation scan therefore spans one month either side of the window.

Run it where the lake and DuckDB both are -- the archiver container has
duckdb, boto3, pyarrow and ``shared/``::

    scp -i ssh-key-2026-04-08.key scripts/oneoff/audit_fruitless_detail_fetches.py \\
        ubuntu@147.224.199.86:/tmp/
    ssh ... 'docker cp /tmp/audit_fruitless_detail_fetches.py \\
        cartracker-archiver:/app/ && docker exec -w /app cartracker-archiver \\
        python audit_fruitless_detail_fetches.py'

Usage::

    python audit_fruitless_detail_fetches.py                    # last full month
    python audit_fruitless_detail_fetches.py --since 2026-08-01
    python audit_fruitless_detail_fetches.py --since 2026-08-01 --until 2026-09-01
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, "/app")

DUCKDB_MEMORY_LIMIT = "2GB"
DUCKDB_THREADS = 2


def _month_span(start: date, end: date) -> list[tuple[int, int]]:
    """(year, month) pairs covering [start, end], one month either side.

    The margin is not decoration: an artifact fetched on the last day of a
    month is routinely observed on the first day of the next, and its
    observation is partitioned by the later date. Without the margin those
    artifacts read as fruitless.
    """
    cur = date(start.year, start.month, 1) - timedelta(days=1)
    stop = date(end.year, end.month, 28) + timedelta(days=35)
    seen: list[tuple[int, int]] = []
    while cur <= stop:
        pair = (cur.year, cur.month)
        if pair not in seen:
            seen.append(pair)
        cur = date(cur.year + (cur.month // 12), (cur.month % 12) + 1, 1)
    return seen


def _in_list(pairs: list[tuple[int, int]], year_col: str, month_col: str) -> str:
    terms = " OR ".join(
        f"({year_col} = {y} AND {month_col} = {m})" for y, m in pairs
    )
    return f"({terms})"


def main() -> int:
    today = date.today()
    default_since = date(today.year, today.month, 1) - timedelta(days=1)
    default_since = date(default_since.year, default_since.month, 1)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default=default_since.isoformat(),
                    help="first fetched_at date, inclusive (YYYY-MM-DD)")
    ap.add_argument("--until", default=None,
                    help="last fetched_at date, exclusive (YYYY-MM-DD)")
    ap.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "bronze"))
    args = ap.parse_args()

    since = datetime.fromisoformat(args.since).date()
    until = datetime.fromisoformat(args.until).date() if args.until else today + timedelta(days=1)

    from shared.duckdb_s3 import get_duckdb_s3_connection

    con = get_duckdb_s3_connection()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads={DUCKDB_THREADS}")

    aqe = f"s3://{args.bucket}/ops_normalized/artifacts_queue_events/**/*.parquet"
    obs = f"s3://{args.bucket}/silver_normalized/observations/**/*.parquet"
    span = _month_span(since, until)

    base = f"""
    WITH ev AS (
        SELECT artifact_id,
               any_value(listing_id)     AS listing_id,
               min(fetched_at)           AS fetched_at,
               arg_max(status, event_at) AS last_status
        FROM read_parquet('{aqe}', hive_partitioning=true)
        WHERE artifact_type = 'detail_page'
          AND {_in_list(span, 'year', 'month')}
        GROUP BY artifact_id
        HAVING min(fetched_at) >= TIMESTAMP '{since}'
           AND min(fetched_at) <  TIMESTAMP '{until}'
    ),
    obs AS (
        SELECT DISTINCT artifact_id
        FROM read_parquet('{obs}', hive_partitioning=true)
        WHERE source = 'detail'
          AND {_in_list(span, 'obs_year', 'obs_month')}
    ),
    j AS (
        SELECT ev.*, (obs.artifact_id IS NOT NULL) AS produced
        FROM ev LEFT JOIN obs USING (artifact_id)
    )
    """

    def show(title: str, sql: str, limit: int = 60) -> None:
        print(f"\n=== {title} ===", flush=True)
        rows = con.execute(sql).fetchall()
        print(" | ".join(d[0] for d in con.description))
        for row in rows[:limit]:
            print(" | ".join("NULL" if v is None else str(v) for v in row))
        if len(rows) > limit:
            print(f"... {len(rows) - limit} more rows")

    print(f"detail fetches with fetched_at in [{since}, {until})")

    show("headline", base + """
        SELECT count(*) AS fetches,
               count(*) FILTER (WHERE NOT produced) AS fruitless,
               round(100.0 * count(*) FILTER (WHERE NOT produced) / count(*), 3) AS pct,
               count(DISTINCT listing_id) FILTER (WHERE NOT produced) AS listings_affected
        FROM j
    """)

    # Which terminal status the wasted fetches reached. A fetch that reached a
    # terminal status with no observation is a parser gap; one still pending is
    # backlog that will clear itself.
    show("fruitless fetches by terminal status", base + """
        SELECT last_status, count(*) AS fruitless,
               count(DISTINCT listing_id) AS listings
        FROM j WHERE NOT produced GROUP BY 1 ORDER BY 2 DESC
    """)

    # The decision number. Escalating backoff only pays if the waste is
    # concentrated on listings that recur; a long tail of one-offs is noise no
    # backoff can catch, because there is no second attempt to suppress.
    show("fruitless fetches per listing", base + """
        SELECT n_fruitless, count(*) AS listings, sum(n_fruitless) AS wasted_fetches
        FROM (
            SELECT listing_id, count(*) FILTER (WHERE NOT produced) AS n_fruitless
            FROM j WHERE listing_id IS NOT NULL GROUP BY listing_id
        )
        WHERE n_fruitless > 0
        GROUP BY 1 ORDER BY 1
    """)

    show("daily rate", base + """
        SELECT date_trunc('day', fetched_at)::date AS day,
               count(*) AS fetches,
               count(*) FILTER (WHERE NOT produced) AS fruitless,
               round(100.0 * count(*) FILTER (WHERE NOT produced) / count(*), 2) AS pct
        FROM j GROUP BY 1 ORDER BY 1
    """)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
