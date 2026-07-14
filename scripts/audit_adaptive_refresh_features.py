"""Read-only audit of Plan 112 adaptive-refresh feature model outputs.

Gate 0 preflight (docs/plan_112_refresh_policy_backtesting.md,
docs/adaptive_refresh_feature_audit.md): checks row counts, key uniqueness,
null rates, freshness, and grain sanity for the dbt-materialized intermediate
models that will feed backtest replay and MLflow experiments. Does not
require Spark, Iceberg, or MLflow — DuckDB only.

Usage:
  python scripts/audit_adaptive_refresh_features.py
  python scripts/audit_adaptive_refresh_features.py --db-path /data/analytics/analytics.duckdb
  python scripts/audit_adaptive_refresh_features.py --markdown
  python scripts/audit_adaptive_refresh_features.py --json-out /tmp/audit.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TableSpec:
    name: str
    grain: str
    grain_keys: list[str]
    not_null_columns: list[str]
    timestamp_column: str | None = None
    source_column: str | None = None
    vin_column: str | None = None
    listing_column: str | None = None
    duration_columns: list[str] = field(default_factory=list)


TABLE_SPECS: list[TableSpec] = [
    TableSpec(
        name="int_listing_state_fingerprints",
        grain="artifact_id (detail-only)",
        grain_keys=["artifact_id"],
        not_null_columns=["vin17", "listing_id", "artifact_id", "fetched_at", "parsed_fingerprint"],
        timestamp_column="fetched_at",
        vin_column="vin17",
        listing_column="listing_id",
    ),
    TableSpec(
        name="int_listing_state_runs",
        grain="one row per (vin17, run_started_at); multiple runs per vin17 (detail-only)",
        grain_keys=["vin17", "run_started_at"],
        not_null_columns=[
            "vin17", "listing_id", "parsed_fingerprint", "run_started_at", "run_ended_at",
            "artifact_count", "run_duration_hours", "is_open_run",
        ],
        timestamp_column="run_ended_at",
        vin_column="vin17",
        listing_column="listing_id",
        duration_columns=["run_duration_hours", "hours_until_change"],
    ),
    TableSpec(
        name="int_listing_observation_fingerprints",
        grain="observation_id (all-source: detail, srp, carousel)",
        grain_keys=["observation_id"],
        not_null_columns=[
            "observation_id", "artifact_id", "listing_id", "source", "fetched_at",
            "parsed_fingerprint",
        ],
        timestamp_column="fetched_at",
        source_column="source",
        vin_column="vin17",
        listing_column="listing_id",
    ),
    TableSpec(
        name="int_listing_observation_runs",
        grain="one row per (listing_id, run_started_at); multiple runs per listing_id (all-source)",
        grain_keys=["listing_id", "run_started_at"],
        not_null_columns=[
            "listing_id", "observation_state_key", "run_started_at", "run_ended_at",
            "observation_count", "detail_observation_count", "srp_observation_count",
            "carousel_observation_count", "distinct_source_count", "detail_seen", "srp_seen",
            "carousel_seen", "run_duration_hours", "is_open_run",
        ],
        timestamp_column="run_ended_at",
        vin_column="vin17",
        listing_column="listing_id",
        duration_columns=["run_duration_hours", "hours_until_next_observation"],
    ),
    TableSpec(
        name="int_listing_volatility_features",
        grain="one row per vin17",
        grain_keys=["vin17"],
        not_null_columns=[
            "vin17", "listing_id", "latest_fetched_at", "first_seen_at", "total_state_changes",
            "listing_id_change_count", "days_since_last_state_change",
            "unchanged_observation_streak", "listing_state_change_count",
            "price_change_count_7d", "price_change_count_30d",
            "all_source_unchanged_observation_streak", "all_source_detail_observation_count",
            "all_source_srp_observation_count", "all_source_carousel_observation_count",
            "all_source_non_detail_refresh_seen",
        ],
        timestamp_column="latest_fetched_at",
        vin_column="vin17",
        listing_column="listing_id",
    ),
]

# mart_detail_refresh_priority does not exist yet — see
# docs/adaptive_refresh_feature_audit.md for what currently replaces it.
NOT_YET_BUILT = ["mart_detail_refresh_priority"]


def get_connection(db_path: str, read_only: bool = True):
    import duckdb

    return duckdb.connect(db_path, read_only=read_only)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def row_count(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {_quote_ident(table)}").fetchone()[0]


def distinct_count(con, table: str, columns: list[str]) -> int:
    cols = ", ".join(_quote_ident(c) for c in columns)
    query = f"SELECT COUNT(*) FROM (SELECT DISTINCT {cols} FROM {_quote_ident(table)})"
    return con.execute(query).fetchone()[0]


def duplicate_group_count(con, table: str, columns: list[str]) -> int:
    """Number of distinct key combinations that appear more than once."""
    cols = ", ".join(_quote_ident(c) for c in columns)
    query = (
        f"SELECT COUNT(*) FROM ("
        f"SELECT {cols} FROM {_quote_ident(table)} "
        f"GROUP BY {cols} HAVING COUNT(*) > 1"
        f")"
    )
    return con.execute(query).fetchone()[0]


def null_counts(con, table: str, columns: list[str]) -> dict[str, int]:
    results = {}
    for col in columns:
        results[col] = con.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(table)} WHERE {_quote_ident(col)} IS NULL"
        ).fetchone()[0]
    return results


def min_max_timestamp(con, table: str, column: str) -> dict[str, Any]:
    col = _quote_ident(column)
    row = con.execute(f"SELECT MIN({col}), MAX({col}) FROM {_quote_ident(table)}").fetchone()
    return {
        "min": str(row[0]) if row[0] is not None else None,
        "max": str(row[1]) if row[1] is not None else None,
    }


def value_distribution(con, table: str, column: str) -> dict[str, int]:
    rows = con.execute(
        f"SELECT {_quote_ident(column)}, COUNT(*) FROM {_quote_ident(table)} "
        f"GROUP BY {_quote_ident(column)} ORDER BY COUNT(*) DESC"
    ).fetchall()
    return {str(value): count for value, count in rows}


def negative_duration_counts(con, table: str, columns: list[str]) -> dict[str, int]:
    results = {}
    for col in columns:
        results[col] = con.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(table)} WHERE {_quote_ident(col)} < 0"
        ).fetchone()[0]
    return results


def audit_table(con, spec: TableSpec) -> dict[str, Any]:
    result: dict[str, Any] = {
        "table": spec.name, "grain": spec.grain, "missing": False, "checks": {},
    }

    try:
        total_rows = row_count(con, spec.name)
    except Exception as exc:
        result["missing"] = True
        result["error"] = str(exc)
        result["checks_skipped"] = [
            "row_count",
            "grain_distinct_count",
            "duplicate_group_count",
            "null_counts",
            "timestamp_range",
            "source_distribution",
            "vin_listing_coverage",
            "negative_durations",
        ]
        return result

    checks = result["checks"]
    checks["row_count"] = total_rows
    checks["grain_distinct_count"] = distinct_count(con, spec.name, spec.grain_keys)
    checks["duplicate_group_count"] = duplicate_group_count(con, spec.name, spec.grain_keys)
    checks["null_counts"] = null_counts(con, spec.name, spec.not_null_columns)

    if spec.timestamp_column:
        checks["timestamp_range"] = min_max_timestamp(con, spec.name, spec.timestamp_column)

    if spec.source_column:
        checks["source_distribution"] = value_distribution(con, spec.name, spec.source_column)

    coverage: dict[str, Any] = {}
    if spec.vin_column:
        coverage["distinct_vin_count"] = distinct_count(con, spec.name, [spec.vin_column])
        coverage["null_vin_count"] = null_counts(con, spec.name, [spec.vin_column])[spec.vin_column]
    if spec.listing_column:
        coverage["distinct_listing_count"] = distinct_count(con, spec.name, [spec.listing_column])
        listing_nulls = null_counts(con, spec.name, [spec.listing_column])
        coverage["null_listing_count"] = listing_nulls[spec.listing_column]
    if coverage:
        checks["vin_listing_coverage"] = coverage

    if spec.duration_columns:
        checks["negative_durations"] = negative_duration_counts(
            con, spec.name, spec.duration_columns
        )

    return result


def run_audit(con) -> list[dict[str, Any]]:
    return [audit_table(con, spec) for spec in TABLE_SPECS]


def format_markdown(results: list[dict[str, Any]]) -> str:
    lines = ["# Adaptive Refresh Feature Audit", ""]
    for r in results:
        lines.append(f"## {r['table']}")
        lines.append(f"grain: {r['grain']}")
        lines.append("")
        if r["missing"]:
            lines.append(f"**MISSING** — {r.get('error', 'table not found')}")
            lines.append("")
            continue
        checks = r["checks"]
        lines.append(f"- row_count: {checks['row_count']}")
        lines.append(f"- grain_distinct_count: {checks['grain_distinct_count']}")
        lines.append(f"- duplicate_group_count: {checks['duplicate_group_count']}")
        lines.append(f"- null_counts: {checks['null_counts']}")
        if "timestamp_range" in checks:
            lines.append(f"- timestamp_range: {checks['timestamp_range']}")
        if "source_distribution" in checks:
            lines.append(f"- source_distribution: {checks['source_distribution']}")
        if "vin_listing_coverage" in checks:
            lines.append(f"- vin_listing_coverage: {checks['vin_listing_coverage']}")
        if "negative_durations" in checks:
            lines.append(f"- negative_durations: {checks['negative_durations']}")
        lines.append("")
    if NOT_YET_BUILT:
        lines.append("## Not yet built")
        for name in NOT_YET_BUILT:
            lines.append(f"- {name} — see docs/adaptive_refresh_feature_audit.md")
        lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DUCKDB_PATH", "/data/analytics/analytics.duckdb"),
        help="Path to the DuckDB analytics database "
        "(default: $DUCKDB_PATH or /data/analytics/analytics.duckdb)",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON (default output format)")
    parser.add_argument(
        "--markdown", action="store_true", help="Print a readable markdown summary instead of JSON"
    )
    parser.add_argument(
        "--json-out", type=str, default=None, help="Also write JSON results to this path"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    con = get_connection(args.db_path)
    try:
        results = run_audit(con)
    finally:
        con.close()

    if args.markdown:
        print(format_markdown(results))
    else:
        print(json.dumps(results, indent=2))

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(results, f, indent=2)

    any_missing = any(r["missing"] for r in results)
    return 1 if any_missing else 0


if __name__ == "__main__":
    sys.exit(main())
