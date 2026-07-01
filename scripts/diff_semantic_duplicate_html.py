"""Fetch two semantically identical detail artifacts and diff their raw HTML.

Answers the question: where exactly do Cars.com pages differ between consecutive
scrapes of a listing whose parsed vehicle state was unchanged?

Run inside the dbt_runner container:
    python scripts/diff_semantic_duplicate_html.py
    python scripts/diff_semantic_duplicate_html.py --context 10
    python scripts/diff_semantic_duplicate_html.py --listing-id <id>
"""
from __future__ import annotations

import argparse
import difflib
import os
import sys

import duckdb
import zstandard as zstd


SILVER_PATH = "s3://bronze/silver/observations/**/*.parquet"
ARTIFACT_EVENTS_PATH = "s3://bronze/ops/artifacts_queue_events/**/*.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--listing-id",
        default=None,
        help="Pin to a specific listing_id instead of picking the highest-duplicate group.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip the top N duplicate groups and audit the next one (default: 0).",
    )
    parser.add_argument(
        "--context",
        type=int,
        default=5,
        help="Lines of context around each diff hunk (default: 5).",
    )
    parser.add_argument(
        "--source-pattern",
        default="%detail%",
        help="SQL ILIKE pattern for detail-source observations.",
    )
    return parser.parse_args()


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET s3_endpoint='minio:9000'")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_access_key_id=?", [os.environ.get("MINIO_ROOT_USER", "cartracker")])
    con.execute("SET s3_secret_access_key=?", [os.environ["MINIO_ROOT_PASSWORD"]])
    return con


def find_pair(
    con: duckdb.DuckDBPyConnection,
    source_pattern: str,
    listing_id: str | None,
    offset: int = 0,
) -> tuple[dict, dict] | None:
    """Return (artifact_a, artifact_b) — two consecutive scrapes with identical parsed state."""
    listing_filter = f"AND listing_id = '{listing_id}'" if listing_id else ""

    query = f"""
    WITH obs AS (
        SELECT
            artifact_id,
            listing_id,
            source,
            fetched_at,
            sha256(concat_ws('|',
                coalesce(vin::VARCHAR, ''),
                coalesce(price::VARCHAR, ''),
                coalesce(mileage::VARCHAR, ''),
                coalesce(msrp::VARCHAR, ''),
                coalesce(make::VARCHAR, ''),
                coalesce(model::VARCHAR, ''),
                coalesce(trim::VARCHAR, ''),
                coalesce(year::VARCHAR, ''),
                coalesce(stock_type::VARCHAR, ''),
                coalesce(fuel_type::VARCHAR, ''),
                coalesce(body_style::VARCHAR, ''),
                coalesce(dealer_name::VARCHAR, ''),
                coalesce(dealer_zip::VARCHAR, ''),
                coalesce(customer_id::VARCHAR, ''),
                coalesce(seller_id::VARCHAR, ''),
                coalesce(dealer_city::VARCHAR, ''),
                coalesce(dealer_state::VARCHAR, ''),
                coalesce(seller_customer_id::VARCHAR, '')
            )) AS parsed_fingerprint,
            price,
            mileage,
            make,
            model,
            vin
        FROM read_parquet('{SILVER_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE source ILIKE ?
          AND listing_id IS NOT NULL
          AND artifact_id IS NOT NULL
          {listing_filter}
    ),

    best_group AS (
        SELECT listing_id, parsed_fingerprint
        FROM obs
        GROUP BY listing_id, parsed_fingerprint
        HAVING count(DISTINCT artifact_id) >= 2
        ORDER BY count(DISTINCT artifact_id) DESC
        LIMIT 1 OFFSET {int(offset)}
    ),

    two_artifacts AS (
        SELECT
            o.artifact_id,
            o.listing_id,
            o.fetched_at,
            o.price,
            o.mileage,
            o.make,
            o.model,
            o.vin,
            o.parsed_fingerprint,
            row_number() OVER (ORDER BY o.fetched_at) AS rn
        FROM obs o
        JOIN best_group g USING (listing_id, parsed_fingerprint)
        QUALIFY rn <= 2
    ),

    artifact_paths AS (
        SELECT
            artifact_id,
            any_value(minio_path) AS minio_path
        FROM read_parquet('{ARTIFACT_EVENTS_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE artifact_type = 'detail_page'
          AND minio_path IS NOT NULL
        GROUP BY artifact_id
    )

    SELECT
        t.rn,
        t.artifact_id,
        t.listing_id,
        t.fetched_at,
        t.price,
        t.mileage,
        t.make,
        t.model,
        t.vin,
        t.parsed_fingerprint,
        p.minio_path
    FROM two_artifacts t
    LEFT JOIN artifact_paths p USING (artifact_id)
    ORDER BY t.rn
    """

    rows = con.execute(query, [source_pattern]).fetchall()
    cols = [d[0] for d in con.description]
    records = [dict(zip(cols, r)) for r in rows]

    if len(records) < 2:
        return None
    return records[0], records[1]


def fetch_html(con: duckdb.DuckDBPyConnection, minio_path: str) -> str:
    if not minio_path.startswith("s3://"):
        minio_path = f"s3://bronze/{minio_path.lstrip('/')}"
    escaped = minio_path.replace("'", "''")
    row = con.execute(f"SELECT content FROM read_blob('{escaped}')").fetchone()
    compressed = bytes(row[0])
    raw = zstd.ZstdDecompressor().decompress(compressed)
    return raw.decode("utf-8", errors="replace")


def main() -> int:
    args = parse_args()
    con = connect_duckdb()

    print("Searching for a semantic duplicate pair...")
    pair = find_pair(con, args.source_pattern, args.listing_id, args.offset)
    if pair is None:
        print("No semantic duplicate pair found with MinIO paths for both artifacts.")
        return 1

    a, b = pair
    print(f"\nListing:      {a['listing_id']}")
    print(f"Vehicle:      {a['make']} {a['model']} (VIN: {a['vin']})")
    print(f"Price:        {a['price']}")
    print(f"Fingerprint:  {a['parsed_fingerprint']}")
    print(f"\nArtifact A:   {a['artifact_id']}  fetched_at={a['fetched_at']}")
    print(f"Artifact B:   {b['artifact_id']}  fetched_at={b['fetched_at']}")

    print("\nFetching artifact A...")
    html_a = fetch_html(con, a["minio_path"])
    print(f"  {len(html_a):,} chars")

    print("Fetching artifact B...")
    html_b = fetch_html(con, b["minio_path"])
    print(f"  {len(html_b):,} chars")

    lines_a = html_a.splitlines(keepends=True)
    lines_b = html_b.splitlines(keepends=True)

    diff = list(difflib.unified_diff(
        lines_a,
        lines_b,
        fromfile=f"A: artifact_id={a['artifact_id']} fetched_at={a['fetched_at']}",
        tofile=f"B: artifact_id={b['artifact_id']} fetched_at={b['fetched_at']}",
        n=args.context,
    ))

    changed_lines = sum(1 for l in diff if l.startswith(("+", "-")) and not l.startswith(("+++", "---")))
    hunks = sum(1 for l in diff if l.startswith("@@"))

    print(f"\nDiff summary: {changed_lines} changed lines across {hunks} hunks")
    print(f"Total lines:  A={len(lines_a):,}  B={len(lines_b):,}")
    print(f"\n{'='*80}\n")

    if diff:
        sys.stdout.writelines(diff)
    else:
        print("Files are byte-identical after decompression.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
