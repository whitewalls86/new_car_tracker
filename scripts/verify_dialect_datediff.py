"""
Plan 125 Gate B: prove dialect.sql's spark__datediff_hours / spark__datediff_days
reproduce DuckDB's datediff() exactly, on a few hundred generated cases.

Two modes, because the two engines live in different images:

    # 1. In the dbt image (has duckdb): regenerate the case corpus.
    docker run --rm --entrypoint python -v "$PWD:/app" -w /app cartracker-dbt-local \
      scripts/verify_dialect_datediff.py --generate

    # 2. In the lakehouse image (has pyspark): check Spark against it.
    docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
      -p local-lakehouse run --rm lakehouse-worker \
      python -m scripts.verify_dialect_datediff --check

The corpus (tests/fixtures/datediff_cases.json) is committed WITH DuckDB's
answers baked in, so --check is runnable without the dbt image and the expected
values are reviewable in a diff rather than regenerated on trust.

Why this exists
---------------
spark__datediff_hours truncates both operands to the hour and then diffs,
because DuckDB's datediff('hour', a, b) counts HOUR BOUNDARIES CROSSED, not
elapsed time. The original Gate B probe checked only 6 hand-picked cases, and
its one negative case had both endpoints on exact hour boundaries -- so it never
discriminated against the naive (unix_timestamp(b) - unix_timestamp(a)) / 3600
translation, which is wrong in the other direction. Truncation-toward-zero bugs
bite hardest on NEGATIVE pairs with partial hours: 03:10 -> 00:30 is -3, but the
naive form gives -2.

These feed run_duration_hours and hours_until_change -- real model features --
so a miss is silent feature drift, not an error.

--check therefore asserts two things, not one:
  * Spark's macro matches DuckDB on every case; and
  * the corpus actually DISCRIMINATES against the naive translation (i.e. the
    naive form fails some cases). A corpus that both forms pass would prove
    nothing, which is exactly how the 6-case probe passed while leaving the bug
    reachable. If the naive form ever stops failing, that is a broken corpus,
    and --check fails loudly rather than reporting a hollow success.

The Spark side renders the REAL macro bodies out of dbt/macros/dialect.sql
rather than re-typing the expressions here: a hand-copy would only prove itself
self-consistent, not that the macro the models compile with is correct.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

DIALECT_PATH = os.environ.get("DIALECT_SQL_PATH", "/app/dbt/macros/dialect.sql")
CASES_PATH = os.environ.get(
    "DATEDIFF_CASES_PATH", "/app/tests/fixtures/datediff_cases.json"
)

Case = Dict[str, object]


def build_pairs() -> List[Tuple[str, str]]:
    """Generate the timestamp pairs. Deterministic (fixed seed) so the corpus is
    stable in review; every pair is emitted in BOTH directions so the negative
    cases are as well covered as the positive ones."""
    random.seed(20260716)
    pairs: List[Tuple[str, str]] = []

    def add(a: str, b: str) -> None:
        pairs.append((a, b))
        pairs.append((b, a))

    # Hand-picked adversarial cases.
    add("2026-01-01 03:10:00", "2026-01-01 00:30:00")  # partial hours, both ends
    add("2026-01-01 02:01:00", "2026-01-01 01:59:00")  # 2 min, 1 boundary crossed
    add("2026-01-01 01:59:59", "2026-01-01 02:00:01")  # 1s either side of boundary
    add("2026-01-01 02:00:00", "2026-01-01 02:00:01")
    add("2026-01-01 01:59:59", "2026-01-01 02:00:00")
    add("2026-01-01 00:01:00", "2026-01-01 23:59:00")  # cross-day, partial
    add("2026-01-01 23:59:00", "2026-01-02 00:01:00")
    add("2026-01-31 22:45:00", "2026-02-01 03:15:00")  # cross-month
    add("2025-12-31 23:30:00", "2026-01-01 00:30:00")  # cross-year
    add("2026-01-01 01:00:00", "2026-01-01 01:00:00")  # identical
    add("2026-01-01 01:00:00.999", "2026-01-01 01:01:00")  # sub-second
    add("2026-01-01 01:59:59.999", "2026-01-01 02:00:00")
    # A US DST date. Both sessions are pinned to UTC, so there must be no jump --
    # this asserts that rather than assuming it.
    add("2026-03-08 01:30:00", "2026-03-08 03:30:00")
    add("2028-02-28 23:10:00", "2028-02-29 00:50:00")  # leap day
    add("2020-01-01 00:30:00", "2026-01-01 03:10:00")  # large span (bigint cast)

    base = datetime(2026, 1, 1)
    # Uniform random pairs, arbitrary minute/second offsets.
    for _ in range(200):
        a = base + timedelta(
            minutes=random.randint(0, 60 * 24 * 60), seconds=random.randint(0, 59)
        )
        b = base + timedelta(
            minutes=random.randint(0, 60 * 24 * 60), seconds=random.randint(0, 59)
        )
        add(a.strftime("%Y-%m-%d %H:%M:%S"), b.strftime("%Y-%m-%d %H:%M:%S"))

    # Near-boundary pairs. Uniform random pairs essentially never land within
    # seconds of an exact hour, which is precisely where truncate-vs-elapsed
    # disagree -- so bias a block of cases there deliberately.
    for _ in range(200):
        a = base + timedelta(hours=random.randint(0, 24 * 60)) + timedelta(
            seconds=random.choice([-2, -1, 0, 1, 2, 59, 3599])
        )
        b = base + timedelta(hours=random.randint(0, 24 * 60)) + timedelta(
            seconds=random.choice([-2, -1, 0, 1, 2, 59, 3599])
        )
        add(a.strftime("%Y-%m-%d %H:%M:%S"), b.strftime("%Y-%m-%d %H:%M:%S"))

    return pairs


def generate(path: str) -> int:
    """Compute DuckDB's answers for every pair and write the corpus."""
    import duckdb

    con = duckdb.connect()
    cases: List[Case] = []
    for a, b in build_pairs():
        hours, days = con.execute(
            "select datediff('hour', cast($a as timestamp), cast($b as timestamp)), "
            "       datediff('day',  cast($a as timestamp), cast($b as timestamp))",
            {"a": a, "b": b},
        ).fetchone()
        cases.append({"a": a, "b": b, "duckdb_hours": hours, "duckdb_days": days})

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(cases, fh, indent=1)
        fh.write("\n")

    negatives = sum(1 for c in cases if c["duckdb_hours"] < 0)
    print(f"wrote {len(cases)} cases with real DuckDB answers -> {path}")
    print(f"  negative-direction cases: {negatives}")
    print(f"  distinct duckdb_hours values: {len({c['duckdb_hours'] for c in cases})}")
    return 0


def extract_macro_body(source: str, macro_name: str) -> str:
    """Pull one {% macro name(...) %}...{% endmacro %} body out of dialect.sql."""
    match = re.search(
        r"\{%\s*macro\s+"
        + re.escape(macro_name)
        + r"\s*\([^)]*\)\s*%\}(.*?)\{%\s*endmacro\s*%\}",
        source,
        re.DOTALL,
    )
    if not match:
        raise SystemExit(f"macro {macro_name!r} not found in {DIALECT_PATH}")
    return match.group(1).strip()


def naive_hours_sql(a: str, b: str) -> str:
    """The translation the audit originally proposed, kept here solely so
    --check can prove the corpus discriminates against it."""
    return (
        f"cast((unix_timestamp(timestamp '{b}') - unix_timestamp(timestamp '{a}')) "
        "/ 3600 as bigint)"
    )


def check(cases_path: str, dialect_path: str) -> int:
    from jinja2 import Template
    from pyspark.sql import SparkSession

    from shared.iceberg_catalog import spark_conf_for_dbt_session

    with open(dialect_path) as fh:
        source = fh.read()
    hours_body = extract_macro_body(source, "spark__datediff_hours")
    days_body = extract_macro_body(source, "spark__datediff_days")
    print(f"spark__datediff_hours := {' '.join(hours_body.split())}")
    print(f"spark__datediff_days  := {' '.join(days_body.split())}")

    with open(cases_path) as fh:
        cases: List[Case] = json.load(fh)
    print(f"loaded {len(cases)} cases with DuckDB expected values from {cases_path}")

    builder = SparkSession.builder.appName("cartracker-datediff-verify")
    for key, value in spark_conf_for_dbt_session().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    tz = spark.conf.get("spark.sql.session.timeZone", None)
    print(f"spark session timeZone={tz}")
    if tz != "UTC":
        # datediff_hours truncates to the hour, so a non-UTC session would shift
        # which hour each operand falls in. Fail rather than measure nonsense.
        raise SystemExit(f"session timeZone is {tz!r}, expected UTC")

    def render(body: str, a: str, b: str) -> str:
        return Template(body).render(start_ts=f"timestamp '{a}'", end_ts=f"timestamp '{b}'")

    selects = [
        f"select {i} as idx, ({render(hours_body, c['a'], c['b'])}) as spark_hours, "
        f"({render(days_body, c['a'], c['b'])}) as spark_days, "
        f"({naive_hours_sql(c['a'], c['b'])}) as naive_hours"
        for i, c in enumerate(cases)
    ]

    # Batched: 800+ single-row round trips would take minutes for no more signal.
    results: Dict[int, Tuple[int, int, int]] = {}
    chunk_size = 100
    for start in range(0, len(selects), chunk_size):
        sql = " union all ".join(selects[start : start + chunk_size])
        for row in spark.sql(sql).collect():
            results[row["idx"]] = (row["spark_hours"], row["spark_days"], row["naive_hours"])

    hour_mismatches: List[str] = []
    day_mismatches: List[str] = []
    naive_would_fail = 0
    for i, c in enumerate(cases):
        spark_hours, spark_days, naive_hours = results[i]
        if spark_hours != c["duckdb_hours"]:
            hour_mismatches.append(
                f"{c['a']} -> {c['b']}: duckdb={c['duckdb_hours']} spark={spark_hours}"
            )
        if spark_days != c["duckdb_days"]:
            day_mismatches.append(
                f"{c['a']} -> {c['b']}: duckdb={c['duckdb_days']} spark={spark_days}"
            )
        if naive_hours != c["duckdb_hours"]:
            naive_would_fail += 1

    total = len(cases)
    print()
    print(f"datediff_hours: {total - len(hour_mismatches)}/{total} match DuckDB")
    print(f"datediff_days:  {total - len(day_mismatches)}/{total} match DuckDB")
    print(
        f"naive (unix_timestamp diff / 3600) would MISS {naive_would_fail}/{total} "
        "-- the corpus discriminates against it"
    )
    for label, mismatches in (("hours", hour_mismatches), ("days", day_mismatches)):
        for line in mismatches[:10]:
            print(f"  MISMATCH {label}: {line}")

    if naive_would_fail == 0:
        raise SystemExit(
            "The corpus does not discriminate against the naive translation, so "
            "passing it proves nothing. Fix build_pairs() -- see module docstring."
        )
    if hour_mismatches or day_mismatches:
        return 1
    print("\nALL CASES MATCH.")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Verify the datediff dialect macros.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--generate",
        action="store_true",
        help="Regenerate the corpus with DuckDB's answers (run in the dbt image).",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="Check Spark's macros against the corpus (run in the lakehouse image).",
    )
    parser.add_argument("--cases-path", default=CASES_PATH)
    parser.add_argument("--dialect-path", default=DIALECT_PATH)
    args = parser.parse_args(argv)

    if args.generate:
        return generate(args.cases_path)
    return check(args.cases_path, args.dialect_path)


if __name__ == "__main__":
    sys.exit(main())
