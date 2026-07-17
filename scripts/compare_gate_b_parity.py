"""
Plan 125 Gate B: parity check between the DuckDB and Spark/Iceberg builds of the
ten-model volatility chain, from the same Plan 120 seeded snapshot.

    docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
      -p local-lakehouse run --rm lakehouse-worker \
      python -m scripts.compare_gate_b_parity

Both builds MUST be run with the same --vars '{"as_of_at": ...}'. Without it
int_listing_volatility_features falls back to now(), the two builds run minutes
apart, and every days_since_* feature drifts for reasons that have nothing to do
with the engines. --as-of-at here only labels the report; it does not build.

Tolerance: EXACT EQUALITY on every field, with arg_max/arg_min tie rows
enumerated as a separate, explicitly reported category. No blanket numeric
tolerance. The reasoning matters, because the opposite choice is the intuitive
one:

  * Every divergence measured at Gate B is either exactly reproducible via
    dbt/macros/dialect.sql (bround for ::int, the FILTER on max_by, truncate-
    then-diff datediff, double for bare ::numeric) or a genuine tie
    nondeterminism. There is no third category of "close enough" difference for
    a tolerance to absorb.
  * A +/-1 numeric tolerance would specifically HIDE the F12 truncation bug that
    cast_to_int/bround exists to fix: DuckDB's ::int rounds and Spark's cast
    truncates, which is a one-dollar difference on every benchmark row -- i.e.
    exactly the magnitude a +/-1 tolerance forgives. The bug and the tolerance
    are the same size. That is disqualifying.
  * Ties are different in kind, not degree: DuckDB takes the first row and Spark
    the last, and NEITHER engine guarantees either. The DuckDB model is already
    non-deterministic under ties, so a tie difference is not a port defect and a
    tiebreak added here would silently change production DuckDB behaviour. So
    ties are reported, counted, and excluded from the verdict -- never hidden,
    never "fixed".

Tie keys are computed from the SOURCE data (see tie_keys_for_price_history and
tie_keys_for_vin_listing_meta) rather than inferred from the differences
themselves, so a real defect on a tie-affected key cannot disguise itself as a
tie.

Rows are plain lists of dicts, and every comparison is a pure function over
them, so the whole comparison layer is unit-testable in the normal CI job --
which installs neither pandas nor pyspark. Same pattern as Gate A.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from shared.iceberg_catalog import (
    CATALOG_NAME,
    WAREHOUSE_NAME,
    require_spike_namespace,
    spark_conf_for_dbt_session,
    table_identifier,
)

DEFAULT_DUCKDB_PATH = "/data/analytics/analytics.duckdb"

Row = Dict[str, Any]
Key = Any


@dataclass
class ModelSpec:
    """What parity means for one model.

    `key` is the grain to join on for row-by-row comparison. `tie_columns` are
    columns derived from arg_max/arg_min, whose differences are reported as ties
    rather than failures IF and ONLY IF the key is independently known (from the
    source data) to have a tie.
    """

    name: str
    key: Sequence[str]
    # Columns the schema file tests as not_null; parity checks null counts on
    # them so a port that starts emitting nulls is caught even if counts match.
    null_checked: Sequence[str] = ()
    # Timestamp columns to compare min/max on (freshness).
    freshness: Sequence[str] = ()
    # Column whose value distribution is compared (e.g. `source`).
    distribution: Optional[str] = None
    tie_columns: Sequence[str] = ()
    # If set, the key is expected to be row-unique (duplicates are a failure).
    unique_key: bool = True


# The eight materialized models. stg_observations/stg_price_events are ephemeral
# on spark and views on duckdb -- no stored output to compare, by construction.
MODEL_SPECS: Tuple[ModelSpec, ...] = (
    ModelSpec(
        name="int_price_history",
        key=("vin",),
        null_checked=("vin", "current_price"),
        freshness=("first_seen_at", "last_seen_at", "price_observed_at"),
        # current_price = arg_max(price, event_at), first_price = arg_min(...).
        tie_columns=("current_price", "first_price"),
    ),
    ModelSpec(
        name="int_listing_state_fingerprints",
        key=("artifact_id",),
        null_checked=("artifact_id", "vin17", "listing_id", "fetched_at"),
        freshness=("fetched_at",),
    ),
    ModelSpec(
        name="int_listing_observation_fingerprints",
        key=("observation_id",),
        null_checked=("observation_id", "artifact_id", "listing_id"),
        freshness=("fetched_at",),
        distribution="source",
    ),
    ModelSpec(
        name="int_listing_state_runs",
        key=("vin17", "listing_id", "parsed_fingerprint", "run_started_at"),
        null_checked=("vin17", "listing_id"),
        freshness=("run_started_at", "run_ended_at"),
        # Multi-row per vin17 by design (no `unique` test on vin17), so the
        # comparison key is the full run identity, not the entity.
        unique_key=False,
    ),
    ModelSpec(
        name="int_listing_observation_runs",
        key=("listing_id", "observation_state_key", "run_started_at"),
        null_checked=("listing_id",),
        freshness=("run_started_at", "run_ended_at"),
        unique_key=False,
    ),
    ModelSpec(
        name="int_latest_observation",
        key=("vin17",),
        null_checked=("vin17", "source", "make"),
        freshness=("fetched_at",),
        distribution="source",
    ),
    ModelSpec(
        name="int_benchmarks",
        key=("make", "model"),
        null_checked=("make", "model"),
    ),
    ModelSpec(
        name="int_listing_volatility_features",
        key=("vin17",),
        null_checked=("vin17",),
        freshness=("latest_fetched_at", "first_seen_at"),
        # customer_id/make/model come from arg_max in vin_listing_meta, and the
        # dealer_*/make_model_* stats and price_vs_make_model_median are all
        # derived from them, so a tie upstream can surface in any of these.
        tie_columns=(
            "dealer_avg_run_length_hours",
            "dealer_median_run_length_hours",
            "make_model_avg_run_length_hours",
            "make_model_median_run_length_hours",
            "price_vs_make_model_median",
        ),
    ),
)


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


@dataclass
class TieReport:
    """arg_max/arg_min tie differences: reported, counted, never hidden, and
    never counted as failures. See the module docstring."""

    model: str
    entries: List[str] = field(default_factory=list)

    def render(self) -> str:
        if not self.entries:
            return f"  [none] {self.model}: no tie-caused differences"
        shown = "\n         ".join(self.entries[:10])
        suffix = (
            f"\n         ... and {len(self.entries) - 10} more"
            if len(self.entries) > 10
            else ""
        )
        return (
            f"  [TIES] {self.model}: {len(self.entries)} tie-caused difference(s) "
            f"-- NOT failures; both engines are within their documented\n"
            f"         (non-)guarantees and DuckDB is already nondeterministic here.\n"
            f"         {shown}{suffix}"
        )


def _key_of(row: Row, spec: ModelSpec) -> Key:
    """Build the comparison key, normalizing timestamps to UTC instants.

    The _runs models key on run_started_at, and DuckDB returns tz-aware while
    Spark returns naive (see _values_equal). Without normalizing here, every key
    would appear to exist on only one side and the row comparison would report
    total divergence for a pure representation difference.
    """
    return tuple(_normalize_key_part(row.get(col)) for col in spec.key)


def _normalize_key_part(value):
    import datetime as _dt

    if isinstance(value, _dt.datetime):
        return _as_utc(value)
    return value


def compare_model(
    spec: ModelSpec,
    duck_rows: Sequence[Row],
    spark_rows: Sequence[Row],
    tie_keys: Optional[Set[Key]] = None,
    allow_empty: bool = False,
) -> Tuple[List[CheckResult], TieReport]:
    """Compare one model's two builds. Pure function over two row lists.

    `tie_keys` is the set of comparison keys independently known (from source
    data) to be affected by an arg_max/arg_min tie. Differences confined to
    `spec.tie_columns` on those keys are reported as ties instead of failures.
    A difference in ANY other column, or on any other key, is a failure even if
    the key is tie-affected -- a tie explains the tied column, nothing else.
    """
    tie_keys = tie_keys or set()
    checks: List[CheckResult] = []
    ties = TieReport(spec.name)

    # Gate A's guard, kept deliberately: every equality check below is satisfied
    # by 0 == 0, so an unseeded MinIO or a filter matching nothing would report
    # "PARITY PASSED" while proving nothing. For an evidence gate, vacuous
    # success is the most dangerous possible outcome.
    if not allow_empty:
        checks.append(
            CheckResult(
                f"{spec.name}: output is non-empty (both builds)",
                bool(duck_rows) and bool(spark_rows),
                len(duck_rows),
                len(spark_rows),
                detail=(
                    ""
                    if duck_rows and spark_rows
                    else "Empty output makes every check below vacuous. Check MinIO "
                    "is seeded and both builds ran -- this is not a parity result."
                ),
            )
        )

    checks.append(
        CheckResult(
            f"{spec.name}: row count",
            len(duck_rows) == len(spark_rows),
            len(duck_rows),
            len(spark_rows),
        )
    )

    duck_keys = [_key_of(r, spec) for r in duck_rows]
    spark_keys = [_key_of(r, spec) for r in spark_rows]
    checks.append(
        CheckResult(
            f"{spec.name}: distinct key count {tuple(spec.key)}",
            len(set(duck_keys)) == len(set(spark_keys)),
            len(set(duck_keys)),
            len(set(spark_keys)),
        )
    )

    duck_dupes = len(duck_keys) - len(set(duck_keys))
    spark_dupes = len(spark_keys) - len(set(spark_keys))
    if spec.unique_key:
        # The merge models' whole safety argument is that the key is row-unique
        # (Iceberg MERGE would raise a cardinality error otherwise), so prove it
        # rather than trusting the schema test ran.
        checks.append(
            CheckResult(
                f"{spec.name}: duplicate keys (expect 0 on both)",
                duck_dupes == 0 and spark_dupes == 0,
                duck_dupes,
                spark_dupes,
            )
        )
    else:
        # Multi-row-per-entity models: the comparison key still has to be unique
        # or the row-by-row join below is meaningless.
        checks.append(
            CheckResult(
                f"{spec.name}: comparison key is unique (grain sanity)",
                duck_dupes == 0 and spark_dupes == 0,
                duck_dupes,
                spark_dupes,
                detail=(
                    ""
                    if duck_dupes == 0 and spark_dupes == 0
                    else "The comparison key does not identify a row; parity below "
                    "cannot be trusted. Widen ModelSpec.key."
                ),
            )
        )

    checks.append(_compare_columns_present(spec, duck_rows, spark_rows))

    for col in spec.null_checked:
        d = sum(1 for r in duck_rows if r.get(col) is None)
        s = sum(1 for r in spark_rows if r.get(col) is None)
        checks.append(
            CheckResult(f"{spec.name}: null count in {col}", d == s, d, s)
        )

    for col in spec.freshness:
        d_vals = [r[col] for r in duck_rows if r.get(col) is not None]
        s_vals = [r[col] for r in spark_rows if r.get(col) is not None]
        if not d_vals and not s_vals:
            continue
        for label, fn in (("min", min), ("max", max)):
            d = fn(d_vals) if d_vals else None
            s = fn(s_vals) if s_vals else None
            checks.append(
                CheckResult(f"{spec.name}: {label}({col})", _values_equal(d, s), d, s)
            )

    if spec.distribution:
        d_dist = _distribution(duck_rows, spec.distribution)
        s_dist = _distribution(spark_rows, spec.distribution)
        checks.append(
            CheckResult(
                f"{spec.name}: {spec.distribution} distribution",
                d_dist == s_dist,
                d_dist,
                s_dist,
            )
        )

    row_check, ties = _compare_rows(spec, duck_rows, spark_rows, tie_keys)
    checks.append(row_check)
    return checks, ties


def _distribution(rows: Sequence[Row], col: str) -> Dict[Any, int]:
    out: Dict[Any, int] = {}
    for r in rows:
        out[r.get(col)] = out.get(r.get(col), 0) + 1
    return dict(sorted(out.items(), key=lambda kv: repr(kv[0])))


def _compare_columns_present(
    spec: ModelSpec, duck_rows: Sequence[Row], spark_rows: Sequence[Row]
) -> CheckResult:
    """Column-set equality.

    This is the only thing standing between int_latest_observation's newly
    explicit column list and silent schema drift: `select * exclude (_rn)` used
    to track stg_observations automatically, and now it does not. A column added
    upstream but not here would show up as a missing column -- here.
    """
    duck_cols = set(duck_rows[0]) if duck_rows else set()
    spark_cols = set(spark_rows[0]) if spark_rows else set()
    if duck_cols == spark_cols:
        return CheckResult(
            f"{spec.name}: column set", True, detail=f"{len(duck_cols)} columns match"
        )
    only_duck = sorted(duck_cols - spark_cols)
    only_spark = sorted(spark_cols - duck_cols)
    return CheckResult(
        f"{spec.name}: column set",
        False,
        detail=f"duckdb-only={only_duck} spark-only={only_spark}",
    )


def _compare_rows(
    spec: ModelSpec,
    duck_rows: Sequence[Row],
    spark_rows: Sequence[Row],
    tie_keys: Set[Key],
) -> Tuple[CheckResult, TieReport]:
    """Full row-by-row equality on the shared key, exact on every field."""
    ties = TieReport(spec.name)
    duck_by_key = {_key_of(r, spec): r for r in duck_rows}
    spark_by_key = {_key_of(r, spec): r for r in spark_rows}
    shared_cols = (set(duck_rows[0]) & set(spark_rows[0])) if duck_rows and spark_rows else set()

    mismatches: List[str] = []
    for key in sorted(set(duck_by_key) | set(spark_by_key), key=repr):
        if key not in duck_by_key:
            mismatches.append(f"{key}: present in spark only")
            continue
        if key not in spark_by_key:
            mismatches.append(f"{key}: present in duckdb only")
            continue
        for col in sorted(shared_cols):
            d = duck_by_key[key].get(col)
            s = spark_by_key[key].get(col)
            if _values_equal(d, s):
                continue
            if col in spec.tie_columns and key in tie_keys:
                ties.entries.append(f"{key} {col}: duckdb={d!r} spark={s!r} (tie)")
            else:
                mismatches.append(f"{key} {col}: duckdb={d!r} spark={s!r}")

    if mismatches:
        shown = "\n         ".join(mismatches[:10])
        suffix = (
            f"\n         ... and {len(mismatches) - 10} more"
            if len(mismatches) > 10
            else ""
        )
        return (
            CheckResult(
                f"{spec.name}: row-by-row equality (exact)", False, detail=shown + suffix
            ),
            ties,
        )
    detail = f"{len(duck_by_key)} rows identical across {len(shared_cols)} columns"
    if ties.entries:
        detail += f" (excluding {len(ties.entries)} tie-caused difference(s), reported separately)"
    return CheckResult(f"{spec.name}: row-by-row equality (exact)", True, detail=detail), ties


def _values_equal(d: Any, s: Any) -> bool:
    """Exact equality, with three representation-only allowances. None of these
    is a numeric tolerance -- each normalizes a TYPE difference between the two
    engines' Python drivers, then compares exactly.

      * tz-aware vs naive datetimes. DuckDB's TIMESTAMPTZ comes back tz-aware
        (Etc/UTC); Spark has no TIMESTAMPTZ at all, so its instant-typed
        TIMESTAMP comes back NAIVE, rendered in spark.sql.session.timeZone.
        A naive datetime is treated as UTC and compared as an instant -- which
        is sound ONLY because that session timezone is pinned to UTC by
        spark_conf_for_dbt_session(), exactly the same dependency
        cast_to_timestamptz rests on. main() asserts the pin is actually in
        force rather than trusting it; without that assert this branch could
        silently equate two different instants.
      * Decimal vs float, for decimal(5,2) columns.
      * bool vs 0/1.

    Anything else compares with ==. In particular no epsilon is applied to
    floats: price_vs_make_model_median is a double on both sides by
    construction (cast_to_numeric), so it should match bit-for-bit, and if it
    ever does not, that is the finding -- not something to round away.
    """
    import datetime as _dt

    if d is None or s is None:
        return d is None and s is None
    if isinstance(d, _dt.datetime) and isinstance(s, _dt.datetime):
        return _as_utc(d) == _as_utc(s)
    if isinstance(d, bool) or isinstance(s, bool):
        return bool(d) == bool(s)
    if _is_number(d) and _is_number(s):
        return float(d) == float(s)
    return d == s


def _as_utc(value):
    """Normalize a datetime to a UTC-aware instant. A naive value is assumed to
    be UTC -- see _values_equal for why that assumption is checked, not free."""
    import datetime as _dt

    if value.tzinfo is None:
        return value.replace(tzinfo=_dt.timezone.utc)
    return value.astimezone(_dt.timezone.utc)


def assert_spark_session_is_utc(spark) -> CheckResult:
    """The timestamp comparison in _values_equal equates naive Spark datetimes
    with tz-aware DuckDB ones by assuming the Spark session renders UTC. If that
    pin were ever lost, the assumption would quietly turn real offset errors
    into passes -- the exact failure mode this gate exists to prevent. So assert
    it as a first-class check rather than relying on it implicitly."""
    tz = spark.conf.get("spark.sql.session.timeZone", None)
    return CheckResult(
        "spark session timeZone is UTC (precondition for timestamp comparison)",
        tz == "UTC",
        detail=(
            f"spark.sql.session.timeZone={tz!r}. Spark has no TIMESTAMPTZ, so its "
            "naive timestamps are only comparable to DuckDB's tz-aware ones while "
            "this is UTC."
        ),
    )


def _is_number(v: Any) -> bool:
    import decimal

    return isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool)


# --- tie discovery, from source data ------------------------------------------


TIE_QUERIES: Dict[str, str] = {
    # int_price_history: current_price = arg_max(price, event_at) per vin. A tie
    # exists when the vin's max(event_at) carries >1 DISTINCT price. Same shape
    # for arg_min at min(event_at) -> first_price.
    "int_price_history": """
        select distinct vin
        from (
            select vin, event_at, count(distinct price) as n
            from {price_events}
            group by vin, event_at
            having count(distinct price) > 1
        ) t
        where (vin, event_at) in (
            select vin, max(event_at) from {price_events} group by vin
        ) or (vin, event_at) in (
            select vin, min(event_at) from {price_events} group by vin
        )
    """,
    # int_listing_volatility_features: customer_id/make/model = arg_max(x,
    # fetched_at) per (vin17, listing_id). A tie exists when the max fetched_at
    # for that pair carries >1 distinct value of any of them.
    "int_listing_volatility_features": """
        select distinct vin17
        from (
            select vin17, listing_id, fetched_at,
                   count(distinct customer_id) as n_cust,
                   count(distinct make)        as n_make,
                   count(distinct model)       as n_model
            from {observations}
            where source = 'detail' and vin17 is not null
            group by vin17, listing_id, fetched_at
            having count(distinct customer_id) > 1
                or count(distinct make) > 1
                or count(distinct model) > 1
        ) t
    """,
}


def discover_tie_keys(spec: ModelSpec, duckdb_sql: Callable[[str], List[Row]]) -> Set[Key]:
    """Find keys whose arg_max/arg_min ordering column has a genuine tie.

    Computed from the SOURCE, not from the observed differences: deriving ties
    from the differences would let any real defect on a tied key excuse itself.
    """
    query = TIE_QUERIES.get(spec.name)
    if not query:
        return set()
    rows = duckdb_sql(query)
    return {tuple(r[col] for col in spec.key) for r in rows}


# --- IO -----------------------------------------------------------------------


def _configure_duckdb_minio(con) -> None:
    """Wire this read-only DuckDB connection to MinIO.

    Needed only for the tie queries: stg_price_events/stg_observations are
    DuckDB *views* over the Parquet in MinIO, so reading them re-fetches from
    S3 rather than from the .duckdb file. The materialized model tables the
    parity itself compares need none of this.

    This mirrors profiles.yml's duckdb `settings:` block from the same env vars.
    It is a second copy of that wiring, which is worth naming -- but it is
    DuckDB's MinIO config, NOT the Iceberg catalog config, so it does not
    weaken the Gate 0.5 R1/R2 guarantee that a catalog swap edits
    shared/iceberg_catalog.py alone.
    """
    endpoint = (
        os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        .replace("http://", "")
        .replace("https://", "")
    )
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}'")
    con.execute(f"SET s3_access_key_id='{os.environ.get('MINIO_ROOT_USER', 'cartracker')}'")
    con.execute(
        f"SET s3_secret_access_key='{os.environ.get('MINIO_ROOT_PASSWORD', '')}'"
    )
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")


def read_duckdb_table(path: str, table: str) -> List[Row]:
    import duckdb

    con = duckdb.connect(path, read_only=True)
    try:
        cursor = con.execute(f"SELECT * FROM main.{table}")
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        con.close()


def read_iceberg_table(spark, table: str) -> List[Row]:
    return [row.asDict() for row in spark.table(table_identifier(table)).collect()]


def assert_landed_in_iceberg(spark, table: str) -> CheckResult:
    """Prove the Spark side is a real Iceberg table in the Lakekeeper catalog.
    Without this a parity run could compare DuckDB against a table that quietly
    materialized in spark_catalog -- the Gate A defaultCatalog trap."""
    require_spike_namespace(WAREHOUSE_NAME)
    fqn = table_identifier(table)
    described = {
        r["col_name"].strip(): (r["data_type"] or "").strip()
        for r in spark.sql(f"DESCRIBE EXTENDED {fqn}").collect()
    }
    provider = described.get("Provider", "")
    location = described.get("Location", "")
    ok = provider.lower() == "iceberg" and location.startswith("s3://")
    return CheckResult(
        f"{table}: spark output is an Iceberg table in the catalog",
        ok,
        detail=f"catalog={CATALOG_NAME} provider={provider!r} location={location!r}",
    )


def build_spark_session():
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("cartracker-gate-b-parity")
    for key, value in spark_conf_for_dbt_session().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Plan 125 Gate B parity check.")
    parser.add_argument(
        "--duckdb-path",
        default=os.environ.get("DUCKDB_PATH", DEFAULT_DUCKDB_PATH),
        help="analytics.duckdb built from the same seeded snapshot and as_of_at.",
    )
    parser.add_argument(
        "--select",
        action="append",
        default=None,
        dest="selected",
        help="Model name to compare (repeatable). Default: all eight.",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help=(
            "Treat empty builds as parity instead of failure. Off by default: "
            "0 == 0 satisfies every check while proving nothing."
        ),
    )
    args = parser.parse_args(argv)

    if not os.path.exists(args.duckdb_path):
        print(
            f"No DuckDB build at {args.duckdb_path}. Build the same chain on the "
            "duckdb target with the SAME --vars as_of_at first -- see "
            "docs/plan_125_duckdb_to_iceberg_migration.md."
        )
        return 2

    specs = MODEL_SPECS
    if args.selected:
        specs = tuple(s for s in MODEL_SPECS if s.name in set(args.selected))
        if not specs:
            parser.error(f"no model matches {args.selected}")

    import duckdb

    con = duckdb.connect(args.duckdb_path, read_only=True)
    _configure_duckdb_minio(con)

    def duckdb_sql(query: str) -> List[Row]:
        sql = query.format(
            price_events="main.stg_price_events", observations="main.stg_observations"
        )
        cursor = con.execute(sql)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    spark = build_spark_session()
    all_checks: List[CheckResult] = [assert_spark_session_is_utc(spark)]
    all_ties: List[TieReport] = []

    try:
        for spec in specs:
            all_checks.append(assert_landed_in_iceberg(spark, spec.name))
            duck_rows = read_duckdb_table(args.duckdb_path, spec.name)
            spark_rows = read_iceberg_table(spark, spec.name)
            tie_keys = discover_tie_keys(spec, duckdb_sql)
            print(
                f"Comparing {spec.name}: duckdb={len(duck_rows)} rows vs "
                f"iceberg={len(spark_rows)} rows"
                + (f" ({len(tie_keys)} tie-affected key(s) in source)" if tie_keys else "")
            )
            checks, ties = compare_model(
                spec, duck_rows, spark_rows, tie_keys, allow_empty=args.allow_empty
            )
            all_checks.extend(checks)
            all_ties.append(ties)
    finally:
        con.close()

    print("\nGate B parity checks:")
    for check in all_checks:
        print(check.render())

    print("\narg_max/arg_min tie report (informational, not failures):")
    for tie in all_ties:
        print(tie.render())

    failed = [c for c in all_checks if not c.passed]
    tie_total = sum(len(t.entries) for t in all_ties)
    print(f"\n{len(all_checks) - len(failed)}/{len(all_checks)} checks passed.")
    print(f"{tie_total} tie-caused difference(s), excluded from the verdict by design.")
    if failed:
        print(
            "PARITY FAILED. Gate B parity is exact by design -- every measured "
            "divergence is either reproducible via dbt/macros/dialect.sql or a "
            "tie, and ties are already accounted for above. A failure here is a "
            "real difference, not drift."
        )
        return 1
    print("PARITY PASSED (exact).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
