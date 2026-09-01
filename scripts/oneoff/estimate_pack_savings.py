"""Plan 131 Stage 0d: measure what packing actually buys, and what grouping costs.

Answers one question the plan cannot settle by argument:

    How many bytes does grouping a listing's captures into one frame save over
    today's one-object-per-capture, and how much of that saving does calendar
    bucketing give back?

The whole margin between the two candidate layouts is ``groups x (B - D)``:

    B   bytes for the FIRST capture in a group (a full page, compressed alone)
    D   marginal bytes for each SUBSEQUENT capture in the same frame

If ``D`` is close to ``B`` there is no grouping win and monthly bucketing costs
nothing. If ``D`` is far below ``B`` the win is real and bucketing gives up a
measurable share of it. Nothing in Plan 131's design can be decided without
these two numbers, and neither has ever been measured.

**Read-only.** Fetches objects and compresses in memory. Writes nothing to MinIO
and mutates nothing. Safe to run while the Plan 129 backfill is working.

Run it in the ``archiver`` container -- it carries zstandard, boto3, duckdb and
``shared/`` -- specifically so ``processing`` stays free for that backfill:

    docker exec -it cartracker-archiver \\
        python -m scripts.oneoff.estimate_pack_savings --listings 150 --json-out /tmp/pack.json

Sampling is by whole LISTING, not by artifact, because a grouping measurement
needs every capture of the listings it samples. Listings are ordered by
``hash(listing_id)`` so a re-run with the same arguments returns the same
sample -- a storage decision that cannot be reproduced cannot be audited.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

LOG = logging.getLogger("pack_savings")

SILVER_PATH = "s3://bronze/silver_normalized/observations/**/*.parquet"
ARTIFACT_EVENTS_PATH = "s3://bronze/ops_normalized/artifacts_queue_events/**/*.parquet"

#: Production write settings, so the baseline is like-for-like rather than
#: flattering. Plan 129 lesson 5: say which baseline you mean.
ZSTD_LEVEL = 9

#: Measured group counts for detail artifacts captured 2026-04-01 onward
#: (Plan 131 Stage 0c, 2026-08-13). Used to project the sample onto the corpus.
#: A projection that re-derives group counts from a small sample would inherit
#: the sample's shape; these are census figures over the whole population.
CORPUS_ARTIFACTS = 4_557_751
CORPUS_GROUPS_PER_LISTING = 224_459
CORPUS_GROUPS_MONTHLY = 441_448

#: MinIO single-drive layout: 4 KB directory + the payload file rounded up to a
#: 4 KB block, with an 8 KB practical floor per object (Plan 114, Plan 129).
#: This is the difference between a saving you can quote and one you can spend.
OBJECT_DIR_BYTES = 4096
OBJECT_BLOCK_BYTES = 4096
OBJECT_MIN_FILE_BYTES = 8192


def physical_bytes(payload: int) -> int:
    """Disk cost of storing *payload* as its own MinIO object."""
    blocks = math.ceil(payload / OBJECT_BLOCK_BYTES) * OBJECT_BLOCK_BYTES
    return max(OBJECT_MIN_FILE_BYTES, blocks) + OBJECT_DIR_BYTES


@dataclass
class Capture:
    artifact_id: int
    listing_id: str
    capture_month: str
    fetched_at: Any
    content: bytes


@dataclass
class Totals:
    listings_sampled: int = 0
    listings_measured: int = 0
    captures_fetched: int = 0
    raw_bytes: int = 0
    skipped_no_path: int = 0
    failures: list[dict[str, str]] = field(default_factory=list)


# --------------------------------------------------------------------------
# sampling
# --------------------------------------------------------------------------

def fetch_listing_sample(
    con: Any,
    *,
    listings: int,
    min_captures: int,
    max_captures: int,
    since: str,
    source_pattern: str = "%detail%",
) -> list[dict[str, Any]]:
    """Return every capture of *listings* whole listings, deterministically chosen.

    The calendar bucket is derived from ``fetched_at``, not from the
    ``obs_year``/``obs_month`` hive partitions: the bucket under test is capture
    time, and the partition columns are not guaranteed to track it.

    ``max_captures`` exists to bound the fetch budget -- one real listing has
    4,467 captures. It biases the result AGAINST grouping, because the listings
    it drops are exactly the ones where grouping wins most, so the reported
    saving is a floor. The count of dropped listings is reported.
    """
    query = f"""
    WITH obs AS (
        SELECT artifact_id, listing_id, fetched_at
        FROM read_parquet('{SILVER_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE source ILIKE ?
          AND listing_id IS NOT NULL
          AND artifact_id IS NOT NULL
          AND fetched_at >= ?
    ),
    one_row_per_artifact AS (
        SELECT artifact_id,
               any_value(listing_id) AS listing_id,
               min(fetched_at)       AS fetched_at
        FROM obs GROUP BY artifact_id
    ),
    life AS (
        SELECT listing_id, count(*) AS captures
        FROM one_row_per_artifact GROUP BY listing_id
    ),
    picked AS (
        SELECT listing_id FROM life
        WHERE captures >= ? AND captures <= ?
        ORDER BY hash(listing_id)
        LIMIT ?
    ),
    artifact_paths AS (
        SELECT artifact_id, any_value(minio_path) AS minio_path
        FROM read_parquet('{ARTIFACT_EVENTS_PATH}', hive_partitioning=true, union_by_name=true)
        WHERE artifact_type = 'detail_page' AND minio_path IS NOT NULL
        GROUP BY artifact_id
    )
    SELECT o.listing_id,
           o.artifact_id,
           strftime(o.fetched_at, '%Y-%m') AS capture_month,
           o.fetched_at,
           p.minio_path
    FROM one_row_per_artifact o
    JOIN picked USING (listing_id)
    JOIN artifact_paths p USING (artifact_id)
    ORDER BY o.listing_id, o.fetched_at
    """
    params = [source_pattern, since, min_captures, max_captures, listings]
    con.execute(query, params)
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in con.fetchall()]


def collect_captures(
    rows: Sequence[dict[str, Any]],
    totals: Totals,
    *,
    throttle_ms: int = 0,
    progress_every: int = 250,
) -> dict[str, list[Capture]]:
    """Fetch raw HTML for every sampled capture, grouped by listing.

    Bytes are used exactly as ``read_html`` returns them -- no utf-8 round trip,
    which would silently change the lengths being measured. ``read_html``
    resolves the frame's own dictionary id, so pre-dictionary and
    dictionary-compressed objects both decode.
    """
    from shared.minio import read_html

    by_listing: dict[str, list[Capture]] = defaultdict(list)
    for row in rows:
        minio_path = row.get("minio_path")
        if not minio_path:
            totals.skipped_no_path += 1
            continue
        try:
            payload = read_html(str(minio_path))
        except Exception as exc:  # noqa: BLE001 - one bad object must not end the run.
            totals.failures.append(
                {"minio_path": str(minio_path), "stage": "fetch", "error": str(exc)}
            )
            continue

        by_listing[str(row["listing_id"])].append(
            Capture(
                artifact_id=int(row["artifact_id"]),
                listing_id=str(row["listing_id"]),
                capture_month=str(row["capture_month"]),
                fetched_at=row["fetched_at"],
                content=payload,
            )
        )
        totals.captures_fetched += 1
        totals.raw_bytes += len(payload)
        if throttle_ms:
            time.sleep(throttle_ms / 1000.0)
        if progress_every and totals.captures_fetched % progress_every == 0:
            LOG.info(
                "PROGRESS | captures=%d listings=%d failures=%d",
                totals.captures_fetched, len(by_listing), len(totals.failures),
            )
    return dict(by_listing)


# --------------------------------------------------------------------------
# measurement
# --------------------------------------------------------------------------

def _compress(content: bytes, dict_id: Optional[int]) -> int:
    from shared.compression import compress_frame

    return len(compress_frame(content, level=ZSTD_LEVEL, dict_id=dict_id))


def measure_listing(captures: Sequence[Capture], dict_id: Optional[int]) -> dict[str, Any]:
    """Measure one listing under all three layouts.

    Members are concatenated with no separator. A real pack carries member
    offsets in its sidecar index, so a separator would measure bytes the format
    does not spend.
    """
    ordered = sorted(captures, key=lambda c: (c.fetched_at, c.artifact_id))

    per_object = [_compress(c.content, dict_id) for c in ordered]
    first_alone = per_object[0]

    grouped = _compress(b"".join(c.content for c in ordered), dict_id)

    by_month: dict[str, list[Capture]] = defaultdict(list)
    for c in ordered:
        by_month[c.capture_month].append(c)
    monthly = sum(
        _compress(b"".join(c.content for c in members), dict_id)
        for members in by_month.values()
    )

    return {
        "listing_id": ordered[0].listing_id,
        "captures": len(ordered),
        "months": len(by_month),
        "raw_bytes": sum(len(c.content) for c in ordered),
        "baseline_bytes": sum(per_object),
        "baseline_physical": sum(physical_bytes(n) for n in per_object),
        "grouped_bytes": grouped,
        "monthly_bytes": monthly,
        "first_alone_bytes": first_alone,
    }


def derive_b_and_d(measurements: Sequence[dict[str, Any]]) -> dict[str, float]:
    """Derive B and D from the aggregate, not from a per-listing average.

    Aggregate first: a per-listing mean would weight a 2-capture listing the
    same as a 400-capture one, and the projection is artifact-weighted.
    """
    multi = [m for m in measurements if m["captures"] >= 2]
    if not multi:
        return {}

    total_first = sum(m["first_alone_bytes"] for m in multi)
    total_grouped = sum(m["grouped_bytes"] for m in multi)
    marginal_captures = sum(m["captures"] - 1 for m in multi)

    b = total_first / len(multi)
    d = (total_grouped - total_first) / marginal_captures if marginal_captures else float("nan")
    return {
        "B_first_capture_bytes": b,
        "D_marginal_capture_bytes": d,
        "D_over_B": (d / b) if b else float("nan"),
        "listings_used": len(multi),
        "marginal_captures": marginal_captures,
    }


def project_corpus(bd: dict[str, float]) -> dict[str, Any]:
    """Project B and D onto the measured corpus group counts.

    Cost of a layout is ``groups*B + (artifacts - groups)*D``. Baseline is the
    degenerate case where every artifact is its own group.
    """
    if not bd:
        return {}
    b, d = bd["B_first_capture_bytes"], bd["D_marginal_capture_bytes"]

    def layout(name: str, groups: int, pack_objects: int) -> dict[str, Any]:
        logical = groups * b + (CORPUS_ARTIFACTS - groups) * d
        # Packs are few and large, so the per-object floor is negligible for
        # them -- but it is charged, not waved away.
        physical = logical + pack_objects * (OBJECT_DIR_BYTES + OBJECT_BLOCK_BYTES)
        return {"layout": name, "groups": groups, "pack_objects": pack_objects,
                "logical_bytes": logical, "physical_bytes": physical}

    baseline_logical = CORPUS_ARTIFACTS * b
    baseline_physical = CORPUS_ARTIFACTS * physical_bytes(int(round(b)))

    rows = [
        layout("per-listing grouping (ceiling)", CORPUS_GROUPS_PER_LISTING, 12),
        layout("monthly capture bucket", CORPUS_GROUPS_MONTHLY, 12),
    ]
    for r in rows:
        r["logical_saving_pct"] = 100.0 * (1 - r["logical_bytes"] / baseline_logical)
        r["physical_saving_pct"] = 100.0 * (1 - r["physical_bytes"] / baseline_physical)

    monthly, ceiling = rows[1], rows[0]
    return {
        "baseline_logical_bytes": baseline_logical,
        "baseline_physical_bytes": baseline_physical,
        "layouts": rows,
        "bucketing_cost_bytes": monthly["logical_bytes"] - ceiling["logical_bytes"],
        "bucketing_cost_pct_of_ceiling_saving": (
            100.0 * (monthly["logical_bytes"] - ceiling["logical_bytes"])
            / (baseline_logical - ceiling["logical_bytes"])
            if baseline_logical > ceiling["logical_bytes"] else float("nan")
        ),
    }


def summarize(measurements: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not measurements:
        return {}
    agg = {k: sum(m[k] for m in measurements) for k in
           ("captures", "raw_bytes", "baseline_bytes", "baseline_physical",
            "grouped_bytes", "monthly_bytes")}
    base = agg["baseline_bytes"]
    return {
        **agg,
        "listings": len(measurements),
        "grouped_saving_pct": 100.0 * (1 - agg["grouped_bytes"] / base) if base else 0.0,
        "monthly_saving_pct": 100.0 * (1 - agg["monthly_bytes"] / base) if base else 0.0,
        "monthly_retains_pct_of_grouping": (
            100.0 * (base - agg["monthly_bytes"]) / (base - agg["grouped_bytes"])
            if base > agg["grouped_bytes"] else float("nan")
        ),
    }


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------

def _mb(n: float) -> str:
    return f"{n / (1024 * 1024):,.1f} MiB"


def print_report(report: dict[str, Any]) -> None:
    out = print
    t = report["totals"]
    out("")
    out("=" * 74)
    out("Plan 131 Stage 0d -- pack savings")
    out("=" * 74)
    out(f"listings sampled  {t['listings_sampled']:,}   measured {t['listings_measured']:,}")
    out(f"captures fetched  {t['captures_fetched']:,}   raw {_mb(t['raw_bytes'])}")
    if t["skipped_no_path"]:
        out(f"skipped (no minio_path)  {t['skipped_no_path']:,}")
    if t["failures"]:
        out(f"fetch failures    {len(t['failures']):,}  (first: {t['failures'][0]['error'][:60]})")

    for variant, block in report["variants"].items():
        s, bd = block["summary"], block["b_and_d"]
        if not s:
            continue
        out("")
        out(f"--- {variant} " + "-" * (69 - len(variant)))
        out(f"  today   (one object per capture)  {_mb(s['baseline_bytes'])}"
            f"   physical {_mb(s['baseline_physical'])}")
        out(f"  monthly capture bucket            {_mb(s['monthly_bytes'])}"
            f"   saving {s['monthly_saving_pct']:5.1f}%")
        out(f"  per-listing grouping (ceiling)    {_mb(s['grouped_bytes'])}"
            f"   saving {s['grouped_saving_pct']:5.1f}%")
        out(f"  monthly retains {s['monthly_retains_pct_of_grouping']:.1f}% of the grouping win")
        if bd:
            out("")
            out(f"  B  first capture in a group   {bd['B_first_capture_bytes']:>10,.0f} bytes")
            out(f"  D  each subsequent capture    {bd['D_marginal_capture_bytes']:>10,.0f} bytes"
                f"   (D/B = {bd['D_over_B']:.3f})")
        proj = block["projection"]
        if proj:
            out("")
            out(f"  projected onto {CORPUS_ARTIFACTS:,} corpus artifacts:")
            out(f"    {'layout':<34}{'logical':>13}{'saving':>9}{'physical':>13}{'saving':>9}")
            out(f"    {'today (one object per capture)':<34}"
                f"{_mb(proj['baseline_logical_bytes']):>13}{'--':>9}"
                f"{_mb(proj['baseline_physical_bytes']):>13}{'--':>9}")
            for r in proj["layouts"]:
                out(f"    {r['layout']:<34}{_mb(r['logical_bytes']):>13}"
                    f"{r['logical_saving_pct']:>8.1f}%{_mb(r['physical_bytes']):>13}"
                    f"{r['physical_saving_pct']:>8.1f}%")
            out(f"    cost of calendar bucketing: {_mb(proj['bucketing_cost_bytes'])}"
                f"  ({proj['bucketing_cost_pct_of_ceiling_saving']:.1f}% of the grouping win)")

    out("")
    out("Reminder: the sample caps captures per listing, which drops exactly the")
    out("listings where grouping wins most. Treat every saving here as a floor.")
    out("")


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--listings", type=int, default=150,
                   help="Whole listings to sample (default 150).")
    p.add_argument("--min-captures", type=int, default=2,
                   help="Skip listings with fewer captures; they cannot show a grouping "
                        "effect. Default 2. They are 0.3%% of artifacts.")
    p.add_argument("--max-captures", type=int, default=300,
                   help="Skip listings with more captures, to bound the fetch budget. "
                        "Biases the result AGAINST grouping. Default 300.")
    p.add_argument("--since", default="2026-04-01",
                   help="Earliest fetched_at to sample; detail HTML paths only exist "
                        "from 2026-04 onward. Default 2026-04-01.")
    p.add_argument("--dict-id", type=int, default=None,
                   help="Dictionary to compress with. Default: the configured production "
                        "dictionary, which is what makes the baseline like-for-like.")
    p.add_argument("--no-dict-control", action="store_true",
                   help="Also measure with no dictionary, to show how much of the "
                        "dictionary's job a large frame window does on its own.")
    p.add_argument("--throttle-ms", type=int, default=0,
                   help="Sleep between object fetches. Use if the Plan 129 backfill is "
                        "running and you want to stay out of its way.")
    p.add_argument("--allow-no-dictionary", action="store_true",
                   help="Permit measuring with no dictionary. Refused by default: the "
                        "production baseline is dictionary-compressed, and an "
                        "undictionaried baseline silently OVERSTATES the pack win.")
    p.add_argument("--json-out", type=Path, default=None)
    p.add_argument("--verbose", action="store_true")

    # No single container has both duckdb (to sample) and shared.compression (to
    # measure): archiver has the first, processing the second. Same split Plan 129
    # hit, same two flags.
    p.add_argument("--sample-only", action="store_true",
                   help="Run the DuckDB sample query, write it, and stop. For the "
                        "archiver container, which has duckdb.")
    p.add_argument("--sample-out", type=Path, default=None,
                   help="Where --sample-only writes. Default: stdout.")
    p.add_argument("--sample-in", type=Path, default=None,
                   help="Measure from a sample file instead of querying. For the "
                        "processing container, which has the dictionary read path.")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # --- sample: needs duckdb (archiver) ---------------------------------
    if args.sample_in:
        rows = json.loads(args.sample_in.read_text(encoding="utf-8"))
        LOG.info("loaded %d sampled captures from %s", len(rows), args.sample_in)
    else:
        from shared.duckdb_s3 import get_duckdb_s3_connection

        con = get_duckdb_s3_connection()
        rows = fetch_listing_sample(
            con,
            listings=args.listings,
            min_captures=args.min_captures,
            max_captures=args.max_captures,
            since=args.since,
        )
        if not rows:
            LOG.error("sample is empty -- nothing to measure")
            return 2

    if args.sample_only:
        payload = json.dumps(rows, indent=2, default=str)
        if args.sample_out:
            args.sample_out.parent.mkdir(parents=True, exist_ok=True)
            args.sample_out.write_text(payload, encoding="utf-8")
            LOG.info("wrote %d captures to %s", len(rows), args.sample_out)
        else:
            print(payload)
        return 0

    # --- measure: needs shared.compression (processing) -------------------
    from shared.compression import configured_dictionary_id

    dict_id = args.dict_id if args.dict_id is not None else configured_dictionary_id()
    if not dict_id and not args.allow_no_dictionary:
        LOG.error(
            "no dictionary resolved (dictionary_id=%s). Production writes "
            "dictionary-compressed frames, so measuring without one would compare "
            "packs against an inflated baseline and overstate the win. Pass "
            "--dict-id <id>, or --allow-no-dictionary if that is genuinely intended. "
            "Note HTML_COMPRESSION_DICT_ID is set on the writer (scraper), not on "
            "readers, which resolve the id from each frame's own header.",
            dict_id or 0,
        )
        return 2
    LOG.info("compressing at level %d with dictionary_id=%s", ZSTD_LEVEL, dict_id or 0)

    totals = Totals()
    totals.listings_sampled = len({r["listing_id"] for r in rows})
    LOG.info("sampled %d listings / %d captures", totals.listings_sampled, len(rows))

    by_listing = collect_captures(rows, totals, throttle_ms=args.throttle_ms)
    if not by_listing:
        LOG.error("fetched nothing -- every object failed or had no path")
        return 2

    variants: dict[str, Any] = {}
    wanted = [("dictionary", dict_id)]
    if args.no_dict_control:
        wanted.append(("no dictionary (control)", None))

    for name, did in wanted:
        LOG.info("measuring variant: %s", name)
        measurements = [
            measure_listing(caps, did) for caps in by_listing.values() if caps
        ]
        variants[name] = {
            "summary": summarize(measurements),
            "b_and_d": derive_b_and_d(measurements),
            "projection": project_corpus(derive_b_and_d(measurements)),
            "per_listing": measurements,
        }
    totals.listings_measured = len(by_listing)

    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        "dictionary_id": dict_id,
        "corpus": {
            "artifacts": CORPUS_ARTIFACTS,
            "groups_per_listing": CORPUS_GROUPS_PER_LISTING,
            "groups_monthly": CORPUS_GROUPS_MONTHLY,
        },
        "totals": {
            "listings_sampled": totals.listings_sampled,
            "listings_measured": totals.listings_measured,
            "captures_fetched": totals.captures_fetched,
            "raw_bytes": totals.raw_bytes,
            "skipped_no_path": totals.skipped_no_path,
            "failures": totals.failures,
        },
        "variants": variants,
    }

    print_report(report)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        LOG.info("wrote %s", args.json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
