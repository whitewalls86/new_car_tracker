"""Diff-log storage analysis for high-observation listings.

For each sampled listing, builds the complete chronological artifact sequence,
computes unified diffs between consecutive scrapes, compresses and writes each
diff to MinIO, then compares real compressed sizes under two storage models:

  full-store:   store every scraped HTML blob (current approach)
  diff-store:   store base file + compressed diff for each subsequent scrape

Correlates diff size spikes with semantic state changes to show what drives
storage cost under each model.

Run inside dbt_runner container:
    python scripts/diff_log_analysis.py
    python scripts/diff_log_analysis.py --listings 3 --min-observations 50
    python scripts/diff_log_analysis.py --max-per-day 3  # exclude dealer_unenriched outliers
    python scripts/diff_log_analysis.py --cleanup   # delete scratch diffs after
"""
from __future__ import annotations

import argparse
import difflib
import sys
from dataclasses import dataclass, field
from typing import Any

import duckdb
import zstandard as zstd

from shared.duckdb_s3 import get_duckdb_s3_connection

SILVER_PATH = "s3://bronze/silver/observations/**/*.parquet"
ARTIFACT_EVENTS_PATH = "s3://bronze/ops/artifacts_queue_events/**/*.parquet"
SCRATCH_PREFIX = "scratch/diff_analysis"
ZSTD_LEVEL = 9


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listings", type=int, default=3,
                        help="Number of listings to analyse (default: 3).")
    parser.add_argument("--min-observations", type=int, default=50,
                        help="Minimum detail artifact count (default: 50).")
    parser.add_argument("--max-per-day", type=float, default=5.0,
                        help="Max artifacts per day; excludes perpetually-re-scraped outliers "
                             "(default: 5.0).")
    parser.add_argument("--source-pattern", default="%detail%",
                        help="SQL ILIKE pattern for detail-source observations.")
    parser.add_argument("--cleanup", action="store_true",
                        help="Delete scratch diffs from MinIO after analysis.")
    parser.add_argument("--skip-profile", action="store_true",
                        help="Skip the full silver layer profile scan "
                             "(faster, uses estimated counts).")
    return parser.parse_args()


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    return get_duckdb_s3_connection()


def get_s3fs():
    from shared.minio import get_s3fs as _get_s3fs
    return _get_s3fs()


def find_listings(
    con: duckdb.DuckDBPyConnection,
    source_pattern: str,
    min_observations: int,
    limit: int,
    max_per_day: float = 5.0,
) -> list[dict]:
    query = f"""
    WITH obs AS (
        SELECT
            artifact_id,
            listing_id,
            source,
            fetched_at,
            make,
            model,
            year,
            price,
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
        FROM read_parquet('{SILVER_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE source ILIKE ?
          AND listing_id IS NOT NULL
          AND artifact_id IS NOT NULL
    ),
    counts AS (
        SELECT
            listing_id,
            any_value(make) AS make,
            any_value(model) AS model,
            any_value(year) AS year,
            any_value(price) AS price,
            count(DISTINCT artifact_id) AS artifact_count,
            count(DISTINCT parsed_fingerprint) AS state_count,
            -- artifacts per day; NULL when all artifacts share the same timestamp
            CASE
                WHEN max(fetched_at) > min(fetched_at)
                THEN count(DISTINCT artifact_id)::DOUBLE /
                     (extract(epoch FROM (max(fetched_at) - min(fetched_at))) / 86400.0)
                ELSE NULL
            END AS artifacts_per_day
        FROM obs
        GROUP BY listing_id
        HAVING count(DISTINCT artifact_id) >= {min_observations}
           AND (
               max(fetched_at) = min(fetched_at)
               OR count(DISTINCT artifact_id)::DOUBLE /
                  (extract(epoch FROM (max(fetched_at) - min(fetched_at))) / 86400.0)
                  <= {max_per_day}
           )
        ORDER BY artifact_count DESC
        LIMIT {limit}
    )
    SELECT * FROM counts
    """
    rows = con.execute(query, [source_pattern]).fetchall()
    cols = [d[0] for d in con.description]
    return [dict(zip(cols, r)) for r in rows]


def get_artifacts(
    con: duckdb.DuckDBPyConnection,
    listing_id: str,
    source_pattern: str,
) -> list[dict]:
    query = f"""
    WITH obs AS (
        SELECT
            artifact_id,
            listing_id,
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
            price
        FROM read_parquet('{SILVER_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE source ILIKE ?
          AND listing_id = ?
          AND artifact_id IS NOT NULL
    ),
    paths AS (
        SELECT artifact_id, any_value(minio_path) AS minio_path
        FROM read_parquet('{ARTIFACT_EVENTS_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE artifact_type = 'detail_page' AND minio_path IS NOT NULL
        GROUP BY artifact_id
    )
    SELECT o.artifact_id, o.fetched_at, o.parsed_fingerprint, o.price, p.minio_path
    FROM obs o
    LEFT JOIN paths p USING (artifact_id)
    -- Silver can have duplicate rows for the same artifact_id (reprocessing artefact);
    -- keep only the earliest occurrence so fingerprint comparisons are stable.
    QUALIFY row_number() OVER (PARTITION BY o.artifact_id ORDER BY o.fetched_at) = 1
    ORDER BY o.fetched_at
    """
    rows = con.execute(query, [source_pattern, str(listing_id)]).fetchall()
    cols = [d[0] for d in con.description]
    return [dict(zip(cols, r)) for r in rows]


def fetch_html(con: duckdb.DuckDBPyConnection, minio_path: str) -> str:
    if not minio_path.startswith("s3://"):
        minio_path = f"s3://bronze/{minio_path.lstrip('/')}"
    escaped = minio_path.replace("'", "''")
    row = con.execute(f"SELECT content FROM read_blob('{escaped}')").fetchone()
    return zstd.ZstdDecompressor().decompress(bytes(row[0])).decode("utf-8", errors="replace")


def fetch_blob_size(con: duckdb.DuckDBPyConnection, minio_path: str) -> int:
    if not minio_path.startswith("s3://"):
        minio_path = f"s3://bronze/{minio_path.lstrip('/')}"
    escaped = minio_path.replace("'", "''")
    row = con.execute(f"SELECT octet_length(content) FROM read_blob('{escaped}')").fetchone()
    return int(row[0])


def compress_and_write(fs: Any, bucket: str, key: str, data: bytes) -> int:
    compressed = zstd.ZstdCompressor(level=ZSTD_LEVEL).compress(data)
    with fs.open(f"{bucket}/{key}", "wb") as f:
        f.write(compressed)
    return len(compressed)


@dataclass
class ArtifactResult:
    artifact_id: int
    fetched_at: Any
    price: int | None
    parsed_fingerprint: str
    semantic_changed: bool
    full_compressed_bytes: int
    diff_compressed_bytes: int | None  # None for base
    diff_scratch_path: str | None
    is_base: bool


@dataclass
class ListingResult:
    listing_id: str
    make: str
    model: str
    year: int | None
    artifact_count: int
    state_count: int
    artifacts: list[ArtifactResult] = field(default_factory=list)

    @property
    def full_store_bytes(self) -> int:
        return sum(a.full_compressed_bytes for a in self.artifacts)

    @property
    def diffstore_bytes(self) -> int:
        total = 0
        for a in self.artifacts:
            if a.is_base:
                total += a.full_compressed_bytes
            elif a.diff_compressed_bytes is not None:
                total += a.diff_compressed_bytes
        return total

    @property
    def semantic_changes(self) -> int:
        return sum(1 for a in self.artifacts if a.semantic_changed)

    @property
    def savings_pct(self) -> float:
        if self.full_store_bytes == 0:
            return 0.0
        return 100.0 * (self.full_store_bytes - self.diffstore_bytes) / self.full_store_bytes


def analyse_listing(
    con: duckdb.DuckDBPyConnection,
    fs: Any,
    listing: dict,
    source_pattern: str,
    bucket: str = "bronze",
) -> ListingResult:
    result = ListingResult(
        listing_id=str(listing["listing_id"]),
        make=str(listing["make"] or ""),
        model=str(listing["model"] or ""),
        year=listing["year"],
        artifact_count=listing["artifact_count"],
        state_count=listing["state_count"],
    )

    artifacts = get_artifacts(con, result.listing_id, source_pattern)
    artifacts = [a for a in artifacts if a.get("minio_path")]

    print(f"  {len(artifacts)} artifacts with MinIO paths (of {listing['artifact_count']} total)")

    prev_html: str | None = None
    prev_fingerprint: str | None = None

    for i, art in enumerate(artifacts):
        artifact_id = art["artifact_id"]
        minio_path = art["minio_path"]
        fingerprint = str(art["parsed_fingerprint"])

        print(
            f"  [{i+1}/{len(artifacts)}] artifact_id={artifact_id}"
            f" fetched_at={art['fetched_at']}",
            end="", flush=True,
        )

        full_bytes = fetch_blob_size(con, minio_path)
        html = fetch_html(con, minio_path)

        semantic_changed = (prev_fingerprint is not None and fingerprint != prev_fingerprint)
        is_base = (i == 0)
        diff_bytes = None
        scratch_path = None

        if not is_base and prev_html is not None:
            lines_a = prev_html.splitlines(keepends=True)
            lines_b = html.splitlines(keepends=True)
            diff_lines = list(difflib.unified_diff(lines_a, lines_b, n=3))
            diff_text = "".join(diff_lines).encode("utf-8", errors="replace")
            scratch_path = f"{SCRATCH_PREFIX}/{result.listing_id}/{artifact_id}.diff.zst"
            diff_bytes = compress_and_write(fs, bucket, scratch_path, diff_text)

        marker = " [STATE CHANGE]" if semantic_changed else ""
        if is_base:
            print(f" full={full_bytes:,}B (base){marker}")
        else:
            print(f" full={full_bytes:,}B diff={diff_bytes:,}B{marker}")

        result.artifacts.append(ArtifactResult(
            artifact_id=artifact_id,
            fetched_at=art["fetched_at"],
            price=art["price"],
            parsed_fingerprint=fingerprint,
            semantic_changed=semantic_changed,
            full_compressed_bytes=full_bytes,
            diff_compressed_bytes=diff_bytes,
            diff_scratch_path=scratch_path,
            is_base=is_base,
        ))

        prev_html = html
        prev_fingerprint = fingerprint

    return result


def print_listing_summary(r: ListingResult) -> None:
    print(f"\n  {'='*70}")
    print(f"  {r.make} {r.model} {r.year}  listing_id={r.listing_id}")
    print(f"  artifacts={len(r.artifacts)}  semantic_states={r.state_count}"
          f"  semantic_changes={r.semantic_changes}")
    print()
    print(f"  {'fetched_at':<32} {'semantic':>8} {'full_B':>10} {'diff_B':>10} {'ratio':>7}")
    print(f"  {'-'*32} {'-'*8} {'-'*10} {'-'*10} {'-'*7}")
    for a in r.artifacts:
        changed = "CHANGED" if a.semantic_changed else ("-" if not a.is_base else "BASE")
        diff_col = f"{a.diff_compressed_bytes:,}" if a.diff_compressed_bytes is not None else "n/a"
        ratio = ""
        if a.diff_compressed_bytes is not None and a.full_compressed_bytes:
            ratio = f"{100*a.diff_compressed_bytes/a.full_compressed_bytes:.1f}%"
        print(f"  {str(a.fetched_at):<32} {changed:>8}"
              f" {a.full_compressed_bytes:>10,} {diff_col:>10} {ratio:>7}")
    print()
    print(f"  Full-store total:   {r.full_store_bytes:>12,} B"
          f"  ({r.full_store_bytes/1024/1024:.2f} MiB)")
    print(f"  Diff-store total:   {r.diffstore_bytes:>12,} B"
          f"  ({r.diffstore_bytes/1024/1024:.2f} MiB)")
    print(f"  Savings:            {r.savings_pct:.1f}%")


def get_silver_profile(
    con: duckdb.DuckDBPyConnection,
    source_pattern: str,
) -> dict:
    """
    Aggregate the full silver layer to get real listing/artifact/state counts.

    Returns:
      listing_count      - distinct listings (= number of diff-store base files)
      total_artifacts    - total deduplicated detail artifact rows
      total_states       - total distinct (listing_id, fingerprint) pairs
                           (= canonical-hash store file count)
      stable_diffs       - artifacts that are NOT a state change and NOT a base
                           (total_artifacts - total_states)
      changed_diffs      - first artifact per new state after the base
                           (total_states - listing_count)
    """
    print("\nScanning full silver layer for aggregate stats (this may take a minute)...")
    query = f"""
    WITH obs AS (
        SELECT
            artifact_id,
            listing_id,
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
        FROM read_parquet('{SILVER_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE source ILIKE ?
          AND listing_id IS NOT NULL
          AND artifact_id IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY artifact_id ORDER BY fetched_at) = 1
    ),
    per_listing AS (
        SELECT
            listing_id,
            count(DISTINCT artifact_id)     AS artifact_count,
            count(DISTINCT parsed_fingerprint) AS state_count
        FROM obs
        GROUP BY listing_id
    )
    SELECT
        count(*)            AS listing_count,
        sum(artifact_count) AS total_artifacts,
        sum(state_count)    AS total_states
    FROM per_listing
    """
    row = con.execute(query, [source_pattern]).fetchone()
    if row is None:
        raise RuntimeError("Silver profile query returned no rows")
    listing_count   = int(row[0])
    total_artifacts = int(row[1])
    total_states    = int(row[2])
    # base file = one per listing; each additional state = a changed diff; rest = stable diffs
    changed_diffs = total_states - listing_count
    stable_diffs  = total_artifacts - total_states
    return {
        "listing_count":   listing_count,
        "total_artifacts": total_artifacts,
        "total_states":    total_states,
        "changed_diffs":   changed_diffs,
        "stable_diffs":    stable_diffs,
    }


def print_extrapolation(results: list[ListingResult], profile: dict | None = None) -> None:
    total_artifacts = sum(len(r.artifacts) for r in results)
    if total_artifacts == 0:
        return

    all_full = [a.full_compressed_bytes for r in results for a in r.artifacts]
    all_diff_stable = [a.diff_compressed_bytes for r in results for a in r.artifacts
                       if a.diff_compressed_bytes is not None and not a.semantic_changed]
    all_diff_changed = [a.diff_compressed_bytes for r in results for a in r.artifacts
                        if a.diff_compressed_bytes is not None and a.semantic_changed]

    avg_full          = sum(all_full) / len(all_full) if all_full else 0
    avg_diff_stable   = sum(all_diff_stable) / len(all_diff_stable) if all_diff_stable else 0
    avg_diff_changed  = sum(all_diff_changed) / len(all_diff_changed) if all_diff_changed else 0

    print(f"\n{'='*72}")
    print("EXTRAPOLATION TO FULL SILVER LAYER")
    print(f"{'='*72}")
    print(f"\nSample stats ({total_artifacts} artifacts across {len(results)} listings):")
    print(f"  avg full compressed size:          {avg_full:>10,.0f} B")
    print(f"  avg diff size (stable state):      {avg_diff_stable:>10,.0f} B"
          f"  (n={len(all_diff_stable)})")
    print(f"  avg diff size (state changed):     {avg_diff_changed:>10,.0f} B"
          f"  (n={len(all_diff_changed)})")

    if profile:
        lc  = profile["listing_count"]
        ta  = profile["total_artifacts"]
        ts  = profile["total_states"]
        cd  = profile["changed_diffs"]
        sd  = profile["stable_diffs"]

        full_store       = ta * avg_full
        # diff-store: one base per listing + stable diffs + changed diffs
        diffstore        = (lc * avg_full) + (sd * avg_diff_stable) + (cd * avg_diff_changed)
        # canonical-hash store: one full file per distinct (listing, state) pair
        canonical_store  = ts * avg_full

        dup_rate = sd / (ta - lc) if (ta - lc) > 0 else 0

        print("\nSilver layer profile (actual counts):")
        print(f"  distinct listings:                 {lc:>12,}")
        print(f"  total detail artifacts:            {ta:>12,}")
        print(f"  distinct listing states:           {ts:>12,}")
        print(f"  stable diffs  (no state change):   {sd:>12,}  ({dup_rate*100:.1f}% of non-base)")
        print(f"  changed diffs (state transition):  {cd:>12,}")
        print("\nStorage estimates:")
        print(f"  Full-store (current):              {full_store/1024**3:>10.2f} GiB")
        print(f"  Diff-store (infinite retention):   {diffstore/1024**3:>10.2f} GiB  "
              f"({100*(full_store-diffstore)/full_store:.1f}% savings)")
        print(f"  Canonical-hash store (Plan 114):   {canonical_store/1024**3:>10.2f} GiB  "
              f"({100*(full_store-canonical_store)/full_store:.1f}% savings)")
        print()
        print("Canonical-hash store = one file per distinct listing state (infinite retention).")
        print("Diff-store has higher ops complexity for ~similar savings.")
    else:
        # Fallback: old uniform extrapolation with a warning
        SILVER_TOTAL = 5_804_559
        SEMANTIC_DUP_RATE = 0.8613
        stable_count  = int(SILVER_TOTAL * SEMANTIC_DUP_RATE)
        changed_count = SILVER_TOTAL - stable_count
        base_count    = len(results)
        full_store_total  = SILVER_TOTAL * avg_full
        diffstore_total = (
            (avg_full * base_count)
            + (stable_count * avg_diff_stable)
            + (changed_count * avg_diff_changed)
        )
        print("\nSilver layer assumptions "
              "(estimated — run without --skip-profile for real counts):")
        print(f"  total detail artifacts:            {SILVER_TOTAL:>12,}")
        print(f"  semantic duplicate rate:           {SEMANTIC_DUP_RATE*100:.2f}%")
        print("\nStorage estimates:")
        print(f"  Full-store (current):              {full_store_total/1024**3:>10.2f} GiB")
        print(f"  Diff-store (infinite retention):   {diffstore_total/1024**3:>10.2f} GiB")
        if full_store_total > 0:
            savings = 100 * (full_store_total - diffstore_total) / full_store_total
            print(f"  Projected savings:                 {savings:.1f}%")


def cleanup_scratch(fs: Any, results: list[ListingResult], bucket: str = "bronze") -> None:
    paths = [a.diff_scratch_path for r in results for a in r.artifacts if a.diff_scratch_path]
    print(f"\nCleaning up {len(paths)} scratch diffs...")
    for p in paths:
        try:
            fs.rm(f"{bucket}/{p}")
        except Exception as e:
            print(f"  warning: could not delete {p}: {e}")
    print("Done.")


def main() -> int:
    args = parse_args()
    con = connect_duckdb()
    fs = get_s3fs()

    print(f"Finding listings with {args.min_observations}+ detail observations"
          f" (max {args.max_per_day}/day)...")
    listings = find_listings(
        con, args.source_pattern, args.min_observations, args.listings, args.max_per_day,
    )

    if not listings:
        print("No listings found matching criteria.")
        return 1

    print(f"Found {len(listings)} listings:\n")
    for listing in listings:
        per_day = (
            f"{listing['artifacts_per_day']:.1f}/day"
            if listing["artifacts_per_day"] is not None else "n/a"
        )
        print(f"  {listing['make']} {listing['model']} {listing['year']}  "
              f"artifacts={listing['artifact_count']}  states={listing['state_count']}  "
              f"freq={per_day}  listing_id={listing['listing_id']}")

    results: list[ListingResult] = []

    for listing in listings:
        print(f"\nAnalysing {listing['make']} {listing['model']}"
              f" (listing_id={listing['listing_id']})...")
        result = analyse_listing(con, fs, listing, args.source_pattern)
        print_listing_summary(result)
        results.append(result)

    profile = None
    if not args.skip_profile:
        profile = get_silver_profile(con, args.source_pattern)
    print_extrapolation(results, profile)

    if args.cleanup:
        cleanup_scratch(fs, results)
    else:
        scratch_paths = [
            a.diff_scratch_path for r in results for a in r.artifacts if a.diff_scratch_path
        ]
        print(f"\nScratch diffs left in MinIO at bronze/{SCRATCH_PREFIX}/")
        print(f"({len(scratch_paths)} files). Run with --cleanup to remove.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
