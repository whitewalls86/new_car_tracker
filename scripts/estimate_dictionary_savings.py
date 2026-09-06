"""Plan 129 Stage 0: does the trained-dictionary win survive a corpus-scale sample?

Plan 114 Stage 3 measured a **61.2%** storage saving from compressing bronze
HTML against a trained zstd dictionary instead of as independent frames. That
number came from 60 artifacts, 12 listings, one capture era. It is the reason
Plan 129 exists, and it is nowhere near enough evidence to take on a permanent,
critical dependency for every object written against it.

This script re-runs the measurement on a broad sample and reports whether it
clears the Stage 0 gate:

    >= 40% saving on held-out listings from held-out months.

Both holdouts, together. That is the whole point of the script, because each
one on its own can be gamed by the sample:

**Held-out listings.** Repeat captures of one listing are near-identical, so an
artifact-level random split puts some captures of listing X in training and
others in test, and the dictionary scores well by memorising the test set.
Plan 114 Stage 3 hit exactly this, caught it, and had to re-run; it moved the
headline by ~5 points on a sample too small for it to do worse. Real writes are
for listings no dictionary has seen.

**Held-out months.** A dictionary trained and tested inside one capture era
never meets markup drift, and drift is precisely what makes a dictionary go
stale in production. cars.com ships a new build token in the page suffix; the
question is what else moves with it.

So the report carries four splits rather than one, and the extra three are not
padding -- they *price the traps*:

===========================  =========================================
split                        what its number means
===========================  =========================================
``leaky_reference``          Artifact-level random split. Deliberately
                             leaky, reported so the size of the leak is
                             visible rather than assumed small.
``listing_disjoint``         Disjoint listings, same months. Comparable
                             to the Plan 114 Stage 3 figure.
``month_disjoint``           Held-out months, listings may recur.
                             Isolates markup drift.
``listing_and_month``        Both. **This is the gate.**
===========================  =========================================

If ``listing_and_month`` clears 40%, Plan 129 Stage 1 is worth building. If it
does not, the operational cost of a permanent dictionary dependency is not
worth a marginal win and the plan should stop -- which is a perfectly good
outcome for a measurement script to produce.

Read-only. Never writes to MinIO, never deletes, never trains a dictionary that
anything else will use. Stage 1's ``train_html_dictionary.py`` is the script
that registers a real one.

Where it runs
-------------
Needs DuckDB (lake sampling), boto3 + zstandard (object reads) and network
reach to MinIO. No single container has all of it, so the practical shape is
``--sample-out`` from the dbt-runner container, then ``--sample-in`` locally
over an SSH tunnel. See ``--help``.

Memory: documents are held uncompressed to train on, so a 2000-artifact sample
at ~190 KB/page is roughly 400 MB resident. Lower ``--sample-size`` on a small
host.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

from shared.query_loader import load_query

LOG = logging.getLogger("dictionary_savings")

#: This script's statements, loaded rather than typed. Plan 129's own scripts
#: are production -- ``train_html_dictionary`` produces the dictionary the write
#: path compresses against, and it imports ``collect_documents`` from here -- so
#: the SQL owes what production SQL owes: a file, a Layer 2 test that executes
#: it against a real engine, and a line in the execution record.
SQL_DIR = Path(__file__).resolve().parent / "sql"
SELECT_AVAILABLE_CAPTURE_MONTHS = load_query(SQL_DIR, "select_available_capture_months")
SELECT_CORPUS_SAMPLE = load_query(SQL_DIR, "select_corpus_sample")

# Layout as written by archiver/processors/flush_silver_observations.py and
# flush_staging_events.py.
SILVER_PATH = "s3://bronze/silver_normalized/observations/**/*.parquet"
ARTIFACT_EVENTS_PATH = "s3://bronze/ops_normalized/artifacts_queue_events/**/*.parquet"

#: zstd level the write path uses (``shared/minio.py:ZSTD_LEVEL``). The
#: baseline is recompressed at this level rather than read from stored object
#: sizes: some objects predate Plan 116's move from level 3, and scoring a
#: level-9 candidate against level-3 stored bytes would credit this plan with
#: Plan 116's savings.
DEFAULT_ZSTD_LEVEL = 9

#: Dictionary sizes to try, in KB. 112 KB is the zstd ``--train`` default and
#: was the largest Plan 114 tried -- it was not shown to be the optimum, so
#: 256 KB is included to find out whether the curve is still climbing.
DEFAULT_DICT_SIZES_KB = (16, 32, 112, 256)

DEFAULT_SAMPLE_SIZE = 2000

#: The Stage 0 gate, from the plan's Success Criteria.
GATE_SAVING_PCT = 40.0

#: Objects in ``bronze`` on the production VM, measured 2026-08-08 (Plan 114,
#: "Storage Accounting"). Used only to amortize the dictionary's own bytes,
#: which is how a ~112 KB permanent blob becomes a rounding error.
CORPUS_OBJECT_COUNT = 3_918_760

#: Below these, a split is not measured at all. A dictionary trained on a
#: handful of documents, or scored on a handful, produces a ratio that means
#: nothing -- and a meaningless number in a report gets quoted later as if it
#: meant something. Refusing to produce one is the safer failure.
MIN_TRAIN_DOCUMENTS = 16
MIN_TEST_DOCUMENTS = 8
MIN_TRAIN_LISTINGS = 4
MIN_TEST_LISTINGS = 4


class SplitLeak(RuntimeError):
    """A split declared disjoint is not disjoint.

    Raised rather than warned. The entire value of this script is that its
    numbers are trustworthy, and a leaked split produces a number that looks
    like the others and is quietly wrong.
    """


# ── Records ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Document:
    """One sampled artifact's raw HTML, with the keys the splits need."""

    artifact_id: int
    listing_id: str
    capture_month: str
    content: bytes


@dataclass
class Split:
    name: str
    description: str
    train: list[Document]
    test: list[Document]
    require_disjoint_listings: bool
    require_disjoint_months: bool


@dataclass
class Totals:
    sampled_rows: int = 0
    fetched: int = 0
    skipped_no_path: int = 0
    failures: list[dict[str, str]] = field(default_factory=list)


# ── Sampling ──────────────────────────────────────────────────────────────────

def fetch_available_months(
    con: Any, source_pattern: str, *, silver_path: str = SILVER_PATH
) -> list[str]:
    """Distinct capture months present in the observation lake, oldest first.

    Reads only the hive partition columns, so this is a directory listing
    rather than a scan.

    *silver_path* defaults to the MinIO glob and exists so a Layer 2 test can
    point the same statement at local fixture Parquet -- the fixture-mode split
    ``lake_snapshot_cohort.open_duckdb_connection`` already makes for the
    selectors, which is what lets this statement be executed against a real
    engine rather than only read.
    """
    rows = con.execute(
        SELECT_AVAILABLE_CAPTURE_MONTHS.format(silver_path=silver_path),
        [source_pattern],
    ).fetchall()
    return [str(row[0]) for row in rows]


def fetch_corpus_sample(
    con: Any,
    *,
    months: Sequence[str],
    sample_size: int,
    source_pattern: str = "%detail%",
    silver_path: str = SILVER_PATH,
    artifact_events_path: str = ARTIFACT_EVENTS_PATH,
) -> list[dict[str, Any]]:
    """Sample detail artifacts evenly across *months*.

    Deliberately **not** ``audit_semantic_duplicate_html_hashes.fetch_sample``,
    which selects the highest-duplicate-count groups. That sample is right for
    "when parsed state is unchanged, what do the bytes do?" and wrong here: it
    over-represents repeat captures of the same listing by construction, which
    is the exact bias the listing-disjoint split exists to defeat. A corpus
    storage estimate needs a sample that looks like the corpus.

    Ordering is by ``hash(artifact_id)`` rather than ``random()`` so a re-run
    with the same arguments returns the same artifacts -- a storage decision
    that cannot be reproduced cannot be audited.
    """
    if not months:
        return []

    per_month = max(1, math.ceil(sample_size / len(months)))
    placeholders = ", ".join("?" for _ in months)

    query = SELECT_CORPUS_SAMPLE.format(
        silver_path=silver_path,
        artifact_events_path=artifact_events_path,
        month_placeholders=placeholders,
        per_month=int(per_month),
    )
    params = [source_pattern, *months]
    columns = [desc[0] for desc in con.execute(query, params).description]
    return [dict(zip(columns, row)) for row in con.fetchall()]


def collect_documents(
    rows: Sequence[dict[str, Any]],
    totals: Totals,
    *,
    max_documents: int = 0,
    progress_every: int = 100,
) -> list[Document]:
    """Fetch and decompress every sampled artifact's raw HTML.

    Bytes are kept exactly as stored decompresses them -- no utf-8 round trip.
    Compression operates on bytes, and decoding to ``str`` with
    ``errors="replace"`` would silently change the very lengths being measured.
    """
    from shared.minio import read_html

    documents: list[Document] = []
    for row in rows:
        if max_documents and len(documents) >= max_documents:
            break
        totals.sampled_rows += 1

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

        documents.append(
            Document(
                artifact_id=_as_int(row.get("artifact_id")) or 0,
                listing_id=str(row.get("listing_id")),
                capture_month=str(row.get("capture_month")),
                content=payload,
            )
        )
        totals.fetched += 1
        if progress_every and totals.fetched % progress_every == 0:
            LOG.info(
                "PROGRESS | fetched=%d listings=%d failures=%d",
                totals.fetched,
                len({d.listing_id for d in documents}),
                len(totals.failures),
            )

    return documents


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ── Splits ────────────────────────────────────────────────────────────────────

def choose_holdout_months(documents: Sequence[Document], count: int = 1) -> list[str]:
    """The most recent *count* months present, which are the ones held out.

    Recency is the right axis rather than a random month: production trains on
    history and compresses what arrives next, so a holdout that sits in the
    future of its training set is the honest simulation of that.
    """
    months = sorted({d.capture_month for d in documents})
    if len(months) <= count:
        return []
    return months[-count:]


def build_splits(
    documents: Sequence[Document],
    *,
    holdout_months: Sequence[str],
) -> list[Split]:
    """Construct the four splits, each labelled with what it is allowed to leak.

    Listings are partitioned by a stable alternation of the sorted listing ids
    rather than by sampling, so the split is reproducible and the two halves
    stay balanced across months.
    """
    documents = list(documents)
    holdout = set(holdout_months)

    listings = sorted({d.listing_id for d in documents})
    train_listings = set(listings[::2])
    test_listings = set(listings[1::2])

    splits: list[Split] = [
        Split(
            name="leaky_reference",
            description=(
                "Artifact-level random split. Deliberately leaky: repeat captures "
                "of one listing land on both sides, so the dictionary can memorise "
                "the test set. Reported to size the leak, never to justify the plan."
            ),
            train=documents[::2],
            test=documents[1::2],
            require_disjoint_listings=False,
            require_disjoint_months=False,
        ),
        Split(
            name="listing_disjoint",
            description=(
                "Disjoint listings, months unconstrained. Comparable to the "
                "Plan 114 Stage 3 figure of -61.2%."
            ),
            train=[d for d in documents if d.listing_id in train_listings],
            test=[d for d in documents if d.listing_id in test_listings],
            require_disjoint_listings=True,
            require_disjoint_months=False,
        ),
    ]

    if holdout:
        splits.append(
            Split(
                name="month_disjoint",
                description=(
                    "Held-out months, listings unconstrained. Isolates markup "
                    f"drift. Holdout: {', '.join(sorted(holdout))}."
                ),
                train=[d for d in documents if d.capture_month not in holdout],
                test=[d for d in documents if d.capture_month in holdout],
                require_disjoint_listings=False,
                require_disjoint_months=True,
            )
        )
        splits.append(
            Split(
                name="listing_and_month_disjoint",
                description=(
                    "Held-out listings from held-out months. This is the Stage 0 "
                    "gate."
                ),
                train=[
                    d
                    for d in documents
                    if d.capture_month not in holdout and d.listing_id in train_listings
                ],
                # A listing seen in training is excluded here even if this
                # capture of it is in a holdout month: the requirement is that
                # the dictionary has never seen the *listing*, and one capture
                # of it in training is enough to teach the vehicle's own text.
                test=[
                    d
                    for d in documents
                    if d.capture_month in holdout and d.listing_id not in train_listings
                ],
                require_disjoint_listings=True,
                require_disjoint_months=True,
            )
        )

    return splits


def validate_split(split: Split) -> None:
    """Enforce a split's own disjointness claim. Raises :class:`SplitLeak`."""
    if split.require_disjoint_listings:
        shared = {d.listing_id for d in split.train} & {d.listing_id for d in split.test}
        if shared:
            raise SplitLeak(
                f"split {split.name!r} declares disjoint listings but shares "
                f"{len(shared)}: {sorted(shared)[:5]}"
            )
    if split.require_disjoint_months:
        shared_months = {d.capture_month for d in split.train} & {
            d.capture_month for d in split.test
        }
        if shared_months:
            raise SplitLeak(
                f"split {split.name!r} declares disjoint months but shares "
                f"{sorted(shared_months)}"
            )
    overlap = {d.artifact_id for d in split.train} & {d.artifact_id for d in split.test}
    if overlap:
        raise SplitLeak(
            f"split {split.name!r} has {len(overlap)} artifacts on both sides"
        )


def split_too_small(split: Split) -> Optional[str]:
    """Why this split cannot produce a meaningful ratio, or ``None`` if it can."""
    checks = (
        (len(split.train), MIN_TRAIN_DOCUMENTS, "train documents"),
        (len(split.test), MIN_TEST_DOCUMENTS, "test documents"),
        (len({d.listing_id for d in split.train}), MIN_TRAIN_LISTINGS, "train listings"),
        (len({d.listing_id for d in split.test}), MIN_TEST_LISTINGS, "test listings"),
    )
    shortfalls = [
        f"{label}: {actual} < {minimum}" for actual, minimum, label in checks if actual < minimum
    ]
    return "; ".join(shortfalls) or None


# ── Measurement ───────────────────────────────────────────────────────────────

def train_dictionary(
    samples: Sequence[bytes],
    dict_size: int,
    *,
    optimize_cover: bool = False,
) -> Any:
    """Train one dictionary. ``optimize_cover`` searches COVER parameters.

    The search is much slower (minutes, not seconds, on a corpus-scale sample)
    and is off by default, because Stage 0 is deciding whether the effect is
    real -- not tuning it.
    """
    import zstandard as zstd

    if optimize_cover:
        return zstd.train_dictionary(dict_size, list(samples), steps=4, threads=-1)
    return zstd.train_dictionary(dict_size, list(samples))


def measure_split(
    split: Split,
    *,
    dict_sizes_kb: Sequence[int],
    zstd_level: int = DEFAULT_ZSTD_LEVEL,
    optimize_cover: bool = False,
) -> dict[str, Any]:
    """Score every dictionary size on one split's held-out documents.

    The baseline and every candidate compress the **same** test documents at
    the **same** level, so the only variable is the dictionary. Savings are
    computed on held-out documents only; training documents never enter a
    numerator or a denominator.
    """
    import zstandard as zstd

    validate_split(split)

    summary: dict[str, Any] = {
        "split": split.name,
        "description": split.description,
        "train_documents": len(split.train),
        "test_documents": len(split.test),
        "train_listings": len({d.listing_id for d in split.train}),
        "test_listings": len({d.listing_id for d in split.test}),
        "train_months": sorted({d.capture_month for d in split.train}),
        "test_months": sorted({d.capture_month for d in split.test}),
        "zstd_level": zstd_level,
        "results": [],
    }

    too_small = split_too_small(split)
    if too_small:
        summary["skipped"] = too_small
        return summary

    test_content = [d.content for d in split.test]
    train_content = [d.content for d in split.train]

    plain = zstd.ZstdCompressor(level=zstd_level).compress
    baseline_bytes = sum(len(plain(content)) for content in test_content)
    summary["test_raw_bytes"] = sum(len(content) for content in test_content)
    summary["baseline_compressed_bytes"] = baseline_bytes

    for size_kb in dict_sizes_kb:
        dict_size = int(size_kb) * 1024
        try:
            trained = train_dictionary(
                train_content, dict_size, optimize_cover=optimize_cover
            )
        except Exception as exc:  # noqa: BLE001 - an untrainable size is a result.
            LOG.warning("Training %d KB dictionary failed: %s", size_kb, exc)
            summary["results"].append({"dict_size_kb": size_kb, "error": str(exc)})
            continue

        with_dict = zstd.ZstdCompressor(level=zstd_level, dict_data=trained).compress
        compressed_bytes = sum(len(with_dict(content)) for content in test_content)
        dictionary_bytes = len(trained.as_bytes())

        summary["results"].append(
            {
                "dict_size_kb": size_kb,
                # The realised size, which can be under the requested cap when
                # the sample has less shared material than the cap allows.
                "dictionary_bytes": dictionary_bytes,
                "dictionary_id": trained.dict_id(),
                "compressed_bytes": compressed_bytes,
                "pct_of_baseline": round(100.0 * compressed_bytes / (baseline_bytes or 1), 4),
                "saving_pct": round(
                    100.0 * (baseline_bytes - compressed_bytes) / (baseline_bytes or 1), 4
                ),
                # One dictionary serves the whole corpus, so its own bytes are
                # a rounding error -- but stating that is cheaper than leaving
                # a reader to wonder whether the 112 KB was netted off.
                "dictionary_bytes_per_artifact_at_corpus_scale": round(
                    dictionary_bytes / CORPUS_OBJECT_COUNT, 6
                ),
            }
        )

    return summary


def best_result(summary: dict[str, Any]) -> Optional[dict[str, Any]]:
    """The dictionary size with the highest saving on this split, if any.

    ``max`` returns the *first* maximum and results are in ascending size
    order, so a tie resolves to the smaller dictionary. That is the bias to
    want: two sizes that compress identically differ only in how much
    permanent blob Stage 1 has to carry forever.
    """
    scored = [r for r in summary.get("results", []) if "saving_pct" in r]
    return max(scored, key=lambda r: r["saving_pct"]) if scored else None


def evaluate_gate(summaries: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Decide Stage 0 against the strict split only.

    A missing strict split is **not** a pass. It means the sample could not
    support the measurement -- most often only one capture month was
    available -- and the honest report of that is "undecided", not silence.
    """
    strict = next(
        (s for s in summaries if s["split"] == "listing_and_month_disjoint"), None
    )
    if strict is None:
        return {
            "gate_pct": GATE_SAVING_PCT,
            "decided": False,
            "reason": (
                "no listing-and-month-disjoint split; the sample spans a single "
                "capture month, so markup drift was never tested"
            ),
        }
    if "skipped" in strict:
        return {
            "gate_pct": GATE_SAVING_PCT,
            "decided": False,
            "reason": f"strict split too small to measure ({strict['skipped']})",
        }

    best = best_result(strict)
    if best is None:
        return {
            "gate_pct": GATE_SAVING_PCT,
            "decided": False,
            "reason": "no dictionary size trained successfully on the strict split",
        }
    return {
        "gate_pct": GATE_SAVING_PCT,
        "decided": True,
        "passed": best["saving_pct"] >= GATE_SAVING_PCT,
        "best_dict_size_kb": best["dict_size_kb"],
        "saving_pct": best["saving_pct"],
    }


# ── Report ────────────────────────────────────────────────────────────────────

def build_report(
    documents: Sequence[Document],
    totals: Totals,
    args: argparse.Namespace,
) -> dict[str, Any]:
    holdout_months = list(args.holdout_months or choose_holdout_months(documents))
    splits = build_splits(documents, holdout_months=holdout_months)
    # Sweeping dictionary sizes to pick one for Stage 1 is a question only the
    # gate split answers, and training a 1 MB dictionary is slow enough that
    # measuring the other three costs 4x for no extra insight.
    if args.only_splits:
        splits = [s for s in splits if s.name in set(args.only_splits)]
    summaries = [
        measure_split(
            split,
            dict_sizes_kb=args.dict_sizes,
            zstd_level=args.zstd_level,
            optimize_cover=args.optimize_cover,
        )
        for split in splits
    ]

    months = sorted({d.capture_month for d in documents})
    by_month: dict[str, int] = defaultdict(int)
    for document in documents:
        by_month[document.capture_month] += 1

    return {
        "plan": "129",
        "stage": 0,
        "sample": {
            "documents": len(documents),
            "listings": len({d.listing_id for d in documents}),
            "months": months,
            "documents_per_month": dict(sorted(by_month.items())),
            "raw_bytes": sum(len(d.content) for d in documents),
            "holdout_months": holdout_months,
            "sampled_rows": totals.sampled_rows,
            "skipped_no_path": totals.skipped_no_path,
            "failures": totals.failures[:20],
            "failure_count": len(totals.failures),
        },
        "zstd_level": args.zstd_level,
        "optimize_cover": args.optimize_cover,
        "splits": summaries,
        "gate": evaluate_gate(summaries),
    }


def _fmt_bytes(value: int) -> str:
    step = 1024.0
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(size) < step or unit == "GiB":
            return f"{size:,.1f} {unit}"
        size /= step
    return f"{size:,.1f} GiB"


def print_report(report: dict[str, Any]) -> None:
    sample = report["sample"]
    print()
    print("=" * 78)
    print("PLAN 129 STAGE 0 | trained zstd dictionary, held-out savings")
    print("=" * 78)
    print(
        f"sample: {sample['documents']} documents / {sample['listings']} listings / "
        f"{len(sample['months'])} months ({_fmt_bytes(sample['raw_bytes'])} raw)"
    )
    print(f"months: {', '.join(sample['months']) or '(none)'}")
    print(f"holdout months: {', '.join(sample['holdout_months']) or '(none)'}")
    if sample["failure_count"]:
        print(f"fetch failures: {sample['failure_count']}")
    print(f"zstd level: {report['zstd_level']}   optimize_cover: {report['optimize_cover']}")

    for summary in report["splits"]:
        print()
        print(f"-- {summary['split']} " + "-" * (74 - len(summary["split"])))
        for line in textwrap.wrap(summary["description"], width=72):
            print(f"   {line}")
        print(
            f"   train: {summary['train_documents']} docs / "
            f"{summary['train_listings']} listings"
            f"   test: {summary['test_documents']} docs / "
            f"{summary['test_listings']} listings"
        )
        if "skipped" in summary:
            print(f"   SKIPPED -- {summary['skipped']}")
            continue

        print(
            f"   baseline (plain zstd-{summary['zstd_level']} on the same test docs): "
            f"{_fmt_bytes(summary['baseline_compressed_bytes'])}"
        )
        print(f"   {'dict':>8}  {'compressed':>12}  {'% of base':>10}  {'saving':>9}")
        for result in summary["results"]:
            if "error" in result:
                print(f"   {result['dict_size_kb']:>6}KB  training failed: {result['error']}")
                continue
            print(
                f"   {result['dict_size_kb']:>6}KB  "
                f"{_fmt_bytes(result['compressed_bytes']):>12}  "
                f"{result['pct_of_baseline']:>9.2f}%  "
                f"{result['saving_pct']:>8.2f}%"
            )

    gate = report["gate"]
    print()
    print("=" * 78)
    if not gate["decided"]:
        print(f"GATE UNDECIDED (>= {gate['gate_pct']:.0f}% required) -- {gate['reason']}")
    elif gate["passed"]:
        print(
            f"GATE PASSED: {gate['saving_pct']:.2f}% saving on held-out listings from "
            f"held-out months, with a {gate['best_dict_size_kb']} KB dictionary "
            f"(>= {gate['gate_pct']:.0f}% required)."
        )
    else:
        print(
            f"GATE FAILED: best held-out saving {gate['saving_pct']:.2f}% "
            f"({gate['best_dict_size_kb']} KB dictionary) is below the "
            f"{gate['gate_pct']:.0f}% required. Plan 129 should stop here."
        )
    print("=" * 78)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan 129 Stage 0: measure trained-zstd-dictionary savings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Typical two-step run (no container has both DuckDB and boto3):\n"
            "  # in dbt-runner: sample the lake\n"
            "  python -m scripts.estimate_dictionary_savings --sample-only \\\n"
            "      --sample-out /tmp/p129_sample.json\n"
            "  # locally, over an SSH tunnel to MinIO: read objects and measure\n"
            "  python -m scripts.estimate_dictionary_savings \\\n"
            "      --sample-in /tmp/p129_sample.json --json-out /tmp/p129_report.json\n"
        ),
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=None,
        metavar="YYYY-MM",
        help="Capture months to sample. Default: every month present in the lake.",
    )
    parser.add_argument(
        "--holdout-months",
        nargs="+",
        default=None,
        metavar="YYYY-MM",
        help="Months to hold out for test. Default: the most recent month sampled.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"Artifacts to sample, spread evenly across months (default {DEFAULT_SAMPLE_SIZE}).",
    )
    parser.add_argument(
        "--dict-sizes",
        type=int,
        nargs="+",
        default=list(DEFAULT_DICT_SIZES_KB),
        metavar="KB",
        help="Dictionary sizes to try, in KB (default: %(default)s).",
    )
    parser.add_argument(
        "--optimize-cover",
        action="store_true",
        help="Search COVER parameters when training. Much slower.",
    )
    parser.add_argument(
        "--only-splits",
        nargs="+",
        default=None,
        metavar="NAME",
        help=(
            "Measure only these splits (e.g. listing_and_month_disjoint). "
            "Useful when sweeping dictionary sizes, where only the gate split "
            "informs the choice. Default: all."
        ),
    )
    parser.add_argument(
        "--zstd-level",
        type=int,
        default=DEFAULT_ZSTD_LEVEL,
        help=(
            "Compression level for both baseline and candidates (default "
            "%(default)s, matching shared/minio.py)."
        ),
    )
    parser.add_argument(
        "--source-pattern",
        default="%detail%",
        help="SQL ILIKE pattern for detail-source observations.",
    )
    parser.add_argument(
        "--sample-only",
        action="store_true",
        help="Sample the lake and write --sample-out, then stop. No object reads.",
    )
    parser.add_argument(
        "--sample-out",
        type=Path,
        default=None,
        help="Write the sampled rows here as JSON.",
    )
    parser.add_argument(
        "--sample-in",
        type=Path,
        default=None,
        help="Read sampled rows from this JSON instead of querying the lake.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Write the full report here as JSON.",
    )
    parser.add_argument(
        "--max-documents",
        type=int,
        default=0,
        help="Stop after fetching this many documents (0 = no limit).",
    )
    parser.add_argument("--verbose", action="store_true", help="Debug logging.")
    return parser.parse_args(argv)


def load_sample(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Sampled rows, from ``--sample-in`` or from a fresh lake query."""
    if args.sample_in:
        with open(args.sample_in, encoding="utf-8") as handle:
            rows = json.load(handle)
        LOG.info("Loaded %d sampled rows from %s", len(rows), args.sample_in)
        return list(rows)

    from shared.duckdb_s3 import get_duckdb_s3_connection

    con = get_duckdb_s3_connection()
    months = list(args.months or fetch_available_months(con, args.source_pattern))
    LOG.info("Sampling %d artifacts across months: %s", args.sample_size, ", ".join(months))
    rows = fetch_corpus_sample(
        con,
        months=months,
        sample_size=args.sample_size,
        source_pattern=args.source_pattern,
    )
    LOG.info("Sampled %d rows", len(rows))
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
    LOG.info("Wrote %s", path)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    rows = load_sample(args)
    if args.sample_out:
        _write_json(args.sample_out, rows)
    if args.sample_only:
        return 0
    if not rows:
        LOG.error("Empty sample; nothing to measure.")
        return 1

    totals = Totals()
    documents = collect_documents(rows, totals, max_documents=args.max_documents)
    if not documents:
        LOG.error("Fetched no documents (%d failures).", len(totals.failures))
        return 1

    report = build_report(documents, totals, args)
    print_report(report)
    if args.json_out:
        _write_json(args.json_out, report)

    gate = report["gate"]
    if not gate["decided"]:
        return 2
    return 0 if gate["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
