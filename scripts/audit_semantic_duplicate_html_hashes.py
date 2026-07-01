"""Audit raw HTML hashes for a small sample of semantic duplicate artifacts.

This script answers a narrow Plan 110 question:

    When silver observations say repeated detail artifacts produced identical
    parsed vehicle state, are the stored HTML blobs also byte-identical?

It intentionally samples a few high-duplicate groups instead of scanning all
HTML objects. It uses DuckDB for MinIO Parquet reads and targeted blob reads, so
it is intended to run in the dbt_runner container.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from typing import Any

import duckdb


SILVER_PATH = "s3://bronze/silver/observations/**/*.parquet"
ARTIFACT_EVENTS_PATH = "s3://bronze/ops/artifacts_queue_events/**/*.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample semantic duplicate detail artifacts and hash their HTML blobs."
    )
    parser.add_argument("--groups", type=int, default=5, help="Number of duplicate groups.")
    parser.add_argument(
        "--artifacts-per-group",
        type=int,
        default=5,
        help="Number of artifact examples per group.",
    )
    parser.add_argument(
        "--source-pattern",
        default="%detail%",
        help="SQL ILIKE pattern for detail-source observations.",
    )
    parser.add_argument(
        "--show-paths",
        action="store_true",
        help="Print full MinIO paths for sampled artifacts.",
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


def fetch_sample(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> list[dict[str, Any]]:
    query = f"""
    WITH obs AS (
        SELECT *
        FROM read_parquet('{SILVER_PATH}', hive_partitioning=true, union_by_name=true)
    ),

    detail_obs AS (
        SELECT
            artifact_id,
            listing_id,
            vin,
            source,
            fetched_at,
            price,
            mileage,
            msrp,
            make,
            model,
            trim,
            year,
            stock_type,
            fuel_type,
            body_style,
            dealer_name,
            dealer_zip,
            customer_id,
            seller_id,
            dealer_city,
            dealer_state,
            seller_customer_id
        FROM obs
        WHERE source ILIKE ?
          AND listing_id IS NOT NULL
          AND artifact_id IS NOT NULL
    ),

    fingerprinted AS (
        SELECT
            *,
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
            )) AS parsed_fingerprint
        FROM detail_obs
    ),

    duplicate_groups AS (
        SELECT
            listing_id,
            parsed_fingerprint,
            count(DISTINCT artifact_id) AS artifact_count,
            min(fetched_at) AS first_seen_at,
            max(fetched_at) AS last_seen_at
        FROM fingerprinted
        GROUP BY listing_id, parsed_fingerprint
        HAVING count(DISTINCT artifact_id) > 1
        ORDER BY artifact_count DESC, last_seen_at DESC
        LIMIT {int(args.groups)}
    ),

    sampled_artifacts AS (
        SELECT
            f.listing_id,
            f.parsed_fingerprint,
            f.artifact_id,
            f.fetched_at,
            f.price,
            f.mileage,
            f.vin,
            g.artifact_count,
            g.first_seen_at,
            g.last_seen_at,
            row_number() OVER (
                PARTITION BY f.listing_id, f.parsed_fingerprint
                ORDER BY f.fetched_at
            ) AS sample_rank
        FROM fingerprinted f
        JOIN duplicate_groups g
          USING (listing_id, parsed_fingerprint)
        QUALIFY sample_rank <= {int(args.artifacts_per_group)}
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
        s.listing_id,
        s.parsed_fingerprint,
        s.artifact_count,
        s.first_seen_at,
        s.last_seen_at,
        s.sample_rank,
        s.artifact_id,
        s.fetched_at,
        s.price,
        s.mileage,
        s.vin,
        p.minio_path
    FROM sampled_artifacts s
    LEFT JOIN artifact_paths p USING (artifact_id)
    ORDER BY s.artifact_count DESC, s.listing_id, s.parsed_fingerprint, s.sample_rank
    """
    columns = [desc[0] for desc in con.execute(query, [args.source_pattern]).description]
    rows = con.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def s3_uri_to_duckdb_path(minio_path: str) -> str:
    if minio_path.startswith("s3://"):
        return minio_path
    return f"s3://bronze/{minio_path.lstrip('/')}"


def hash_blob(con: duckdb.DuckDBPyConnection, minio_path: str) -> tuple[int | None, str | None, str | None]:
    path = s3_uri_to_duckdb_path(minio_path)
    escaped = path.replace("'", "''")
    try:
        row = con.execute(
            f"""
            SELECT
                octet_length(content) AS compressed_bytes,
                sha256(content) AS compressed_hash
            FROM read_blob('{escaped}')
            """
        ).fetchone()
    except Exception as exc:  # noqa: BLE001 - audit should continue per artifact.
        return None, None, str(exc)
    return int(row[0]), str(row[1]), None


def print_results(
    con: duckdb.DuckDBPyConnection, rows: list[dict[str, Any]], show_paths: bool
) -> None:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["listing_id"]), str(row["parsed_fingerprint"]))].append(row)

    if not grouped:
        print("No semantic duplicate groups found.")
        return

    total_artifacts = 0
    total_hash_matches = 0
    groups_with_identical_html = 0

    for group_index, ((listing_id, parsed_fingerprint), group_rows) in enumerate(
        grouped.items(), start=1
    ):
        artifact_count = group_rows[0]["artifact_count"]
        print()
        print(f"GROUP {group_index}")
        print(f"listing_id: {listing_id}")
        print(f"semantic_artifact_count: {artifact_count}")
        print(f"semantic_window: {group_rows[0]['first_seen_at']} -> {group_rows[0]['last_seen_at']}")
        print(f"parsed_fingerprint: {parsed_fingerprint}")

        hash_counts: dict[str, int] = defaultdict(int)
        for row in group_rows:
            total_artifacts += 1
            minio_path = row.get("minio_path")
            compressed_bytes = None
            compressed_hash = None
            error = None
            if minio_path:
                compressed_bytes, compressed_hash, error = hash_blob(con, str(minio_path))
                if compressed_hash:
                    hash_counts[compressed_hash] += 1

            if compressed_hash and hash_counts[compressed_hash] > 1:
                total_hash_matches += 1

            path_text = f" path={minio_path}" if show_paths else ""
            print(
                "  "
                f"rank={row['sample_rank']} artifact_id={row['artifact_id']} "
                f"fetched_at={row['fetched_at']} price={row['price']} mileage={row['mileage']} "
                f"compressed_bytes={compressed_bytes} compressed_hash={compressed_hash} "
                f"error={error}{path_text}"
            )

        unique_hashes = len(hash_counts)
        if unique_hashes == 1 and hash_counts:
            groups_with_identical_html += 1
        print(
            f"group_hash_summary: sampled={len(group_rows)} "
            f"unique_compressed_hashes={unique_hashes} hash_counts={dict(hash_counts)}"
        )

    print()
    print("SUMMARY")
    print(f"sampled_groups: {len(grouped)}")
    print(f"sampled_artifacts: {total_artifacts}")
    print(f"groups_with_all_sampled_html_identical: {groups_with_identical_html}")
    print(f"repeat_hash_matches_after_first_seen: {total_hash_matches}")


def main() -> int:
    args = parse_args()
    con = connect_duckdb()
    rows = fetch_sample(con, args)
    print_results(con, rows, args.show_paths)
    return 0


if __name__ == "__main__":
    sys.exit(main())
