"""
Plan 125 Gate A: parity check between the DuckDB and Spark/Iceberg builds of
`mart_block_rate`, from the same Plan 120 seeded snapshot.

    docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
      -p local-lakehouse run --rm lakehouse-worker \
      python -m scripts.compare_gate_a_parity

Gate A parity is expected to be EXACT. The chain is two models over one Parquet
source with no incremental logic, no floating-point aggregation, and no
rounding: the only dialect change is `::timestamp` -> `cast(... as timestamp)`,
which is the same operation. So unlike the Gate B/C chains (which will need a
stated numeric tolerance for the F5/F10/F12 rounding items), any difference
here is a real defect, not drift -- and this script asserts equality rather
than closeness.

Checks (per the Gate A success criteria):
  * the Spark output is really an Iceberg table in the Lakekeeper catalog,
    not a silently-created spark_catalog table (the defaultCatalog trap);
  * row count;
  * key grain -- distinct `hour` count and duplicate keys on both sides;
  * min/max `hour`;
  * aggregate blocked/incremented counts and the other measures;
  * full row-by-row equality on the shared key.

Rows are plain lists of dicts rather than DataFrames: this model is one row
per hour, so pandas buys nothing, and staying dependency-free keeps
compare_mart_block_rate() unit-testable in the normal CI job (which installs
neither pandas nor pyspark).
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from shared.iceberg_catalog import (
    CATALOG_NAME,
    WAREHOUSE_NAME,
    require_spike_namespace,
    spark_conf_for_dbt_session,
    table_identifier,
)

TABLE_NAME = "mart_block_rate"
DEFAULT_DUCKDB_PATH = "/data/analytics/analytics.duckdb"
KEY = "hour"
MEASURES = (
    "new_blocks",
    "block_increments",
    "total_block_events",
    "unique_listings_blocked",
    "max_attempts_seen",
)

Row = Dict[str, Any]


@dataclass
class CheckResult:
    name: str
    passed: bool
    duckdb_value: object = None
    spark_value: object = None
    detail: str = ""

    def render(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        body = f"  [{status}] {self.name}"
        if self.duckdb_value is not None or self.spark_value is not None:
            body += f": duckdb={self.duckdb_value!r} spark={self.spark_value!r}"
        if self.detail:
            body += f"\n         {self.detail}"
        return body


def compare_mart_block_rate(
    duck_rows: Sequence[Row],
    spark_rows: Sequence[Row],
    allow_empty: bool = False,
) -> List[CheckResult]:
    """Compare the two builds. Pure function over two row lists, so it is
    unit-testable without Spark, DuckDB, MinIO, or Docker.

    `allow_empty=False` (the default) treats two empty builds as a FAILURE, not
    a match. Every equality check below is trivially satisfied by 0 == 0, so an
    unseeded MinIO, a wrong source path, or a filter that silently matches
    nothing would otherwise report "PARITY PASSED" while proving exactly
    nothing. For a gate whose entire purpose is evidence, vacuous success is
    the most dangerous possible outcome. Pass allow_empty=True only to test the
    degenerate case deliberately.
    """
    checks: List[CheckResult] = []

    if not allow_empty:
        checks.append(
            CheckResult(
                "output is non-empty (both builds)",
                bool(duck_rows) and bool(spark_rows),
                len(duck_rows),
                len(spark_rows),
                detail=(
                    ""
                    if duck_rows and spark_rows
                    else "Empty output makes every check below vacuous. Check that "
                    "MinIO is seeded from a Plan 120 snapshot and that the source "
                    "path resolves -- this is not a parity result."
                ),
            )
        )

    checks.append(
        CheckResult(
            "row count", len(duck_rows) == len(spark_rows), len(duck_rows), len(spark_rows)
        )
    )

    duck_keys = [r[KEY] for r in duck_rows]
    spark_keys = [r[KEY] for r in spark_rows]
    checks.append(
        CheckResult(
            f"distinct {KEY} count",
            len(set(duck_keys)) == len(set(spark_keys)),
            len(set(duck_keys)),
            len(set(spark_keys)),
        )
    )

    # Grain is the point of this model (`hour` is unique/not_null-tested on the
    # DuckDB side), so prove neither engine duplicated it rather than assuming.
    duck_dupes = len(duck_keys) - len(set(duck_keys))
    spark_dupes = len(spark_keys) - len(set(spark_keys))
    checks.append(
        CheckResult(
            f"duplicate {KEY} keys (expect 0 on both)",
            duck_dupes == 0 and spark_dupes == 0,
            duck_dupes,
            spark_dupes,
        )
    )

    if duck_keys and spark_keys:
        for label, fn in (("min", min), ("max", max)):
            d, s = fn(duck_keys), fn(spark_keys)
            checks.append(CheckResult(f"{label} {KEY}", d == s, d, s))

    for measure in MEASURES:
        duck_missing = any(measure not in r for r in duck_rows)
        spark_missing = any(measure not in r for r in spark_rows)
        if duck_missing or spark_missing:
            checks.append(
                CheckResult(
                    f"sum({measure})", False, detail=f"column {measure} missing from output"
                )
            )
            continue
        d = sum(r[measure] or 0 for r in duck_rows)
        s = sum(r[measure] or 0 for r in spark_rows)
        checks.append(CheckResult(f"sum({measure})", d == s, d, s))

    checks.append(_compare_rows(duck_rows, spark_rows))
    return checks


def _compare_rows(duck_rows: Sequence[Row], spark_rows: Sequence[Row]) -> CheckResult:
    """Full row-by-row equality, joined on the key. Catches per-hour errors
    that cancel out in the aggregate totals above."""
    duck_by_key = {r[KEY]: r for r in duck_rows}
    spark_by_key = {r[KEY]: r for r in spark_rows}

    mismatches: List[str] = []
    for key in sorted(set(duck_by_key) | set(spark_by_key)):
        if key not in duck_by_key:
            mismatches.append(f"{key}: present in spark only")
            continue
        if key not in spark_by_key:
            mismatches.append(f"{key}: present in duckdb only")
            continue
        for measure in MEASURES:
            d = duck_by_key[key].get(measure)
            s = spark_by_key[key].get(measure)
            if d != s:
                mismatches.append(f"{key} {measure}: duckdb={d!r} spark={s!r}")

    if mismatches:
        shown = "\n         ".join(mismatches[:10])
        suffix = (
            f"\n         ... and {len(mismatches) - 10} more" if len(mismatches) > 10 else ""
        )
        return CheckResult("row-by-row equality", False, detail=shown + suffix)
    return CheckResult(
        "row-by-row equality", True, detail=f"{len(duck_by_key)} rows identical"
    )


def assert_landed_in_iceberg(spark) -> CheckResult:
    """Guard: prove the Spark side is reading a real Iceberg table registered
    in the Lakekeeper catalog. Without this, a parity run could compare DuckDB
    against a table that quietly materialized in `spark_catalog`."""
    require_spike_namespace(WAREHOUSE_NAME)
    fqn = table_identifier(TABLE_NAME)
    described = {
        r["col_name"].strip(): (r["data_type"] or "").strip()
        for r in spark.sql(f"DESCRIBE EXTENDED {fqn}").collect()
    }
    provider = described.get("Provider", "")
    location = described.get("Location", "")
    ok = provider.lower() == "iceberg" and location.startswith("s3://")
    return CheckResult(
        "spark output is an Iceberg table in the catalog",
        ok,
        detail=f"catalog={CATALOG_NAME} provider={provider!r} location={location!r}",
    )


def read_duckdb(path: str) -> List[Row]:
    import duckdb

    con = duckdb.connect(path, read_only=True)
    try:
        cursor = con.execute(f"SELECT * FROM main.{TABLE_NAME}")
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        con.close()


def read_iceberg(spark) -> List[Row]:
    return [row.asDict() for row in spark.table(table_identifier(TABLE_NAME)).collect()]


def build_spark_session():
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("cartracker-gate-a-parity")
    for key, value in spark_conf_for_dbt_session().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-path",
        default=os.environ.get("DUCKDB_PATH", DEFAULT_DUCKDB_PATH),
        help="analytics.duckdb built from the same seeded snapshot.",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help=(
            "Treat two empty builds as parity instead of a failure. Off by "
            "default: 0 == 0 satisfies every check while proving nothing, so an "
            "unseeded MinIO would otherwise report success."
        ),
    )
    args = parser.parse_args(argv)

    if not os.path.exists(args.duckdb_path):
        print(
            f"No DuckDB build at {args.duckdb_path}. Build the same chain on the "
            "duckdb target first -- see docs/plan_125_duckdb_to_iceberg_migration.md "
            "(Gate A local validation)."
        )
        return 2

    spark = build_spark_session()
    checks = [assert_landed_in_iceberg(spark)]

    duck_rows = read_duckdb(args.duckdb_path)
    spark_rows = read_iceberg(spark)
    print(
        f"Comparing {TABLE_NAME}: duckdb={len(duck_rows)} rows "
        f"({args.duckdb_path}) vs iceberg={len(spark_rows)} rows "
        f"({table_identifier(TABLE_NAME)})"
    )
    checks.extend(
        compare_mart_block_rate(duck_rows, spark_rows, allow_empty=args.allow_empty)
    )

    print("\nGate A parity checks:")
    for check in checks:
        print(check.render())

    failed = [c for c in checks if not c.passed]
    print(f"\n{len(checks) - len(failed)}/{len(checks)} checks passed.")
    if failed:
        print(
            "PARITY FAILED. Gate A parity is expected to be exact -- there is no "
            "rounding or incremental logic in this chain to explain a difference."
        )
        return 1
    print("PARITY PASSED (exact).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
