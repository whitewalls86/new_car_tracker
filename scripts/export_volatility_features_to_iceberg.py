"""
Plan 112 Gate A3: VM/local-manual rehearsal writing the real dbt
`int_listing_volatility_features` feature table to Iceberg via Lakekeeper +
MinIO, through the same profile-gated `lakehouse-worker` used by A2.

VM/local only -- not run in CI (see docs/plan_112_refresh_policy_backtesting.md's A3 section).
Reads the real production-derived analytics DuckDB file **read-only**:

    docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \\
      run --rm lakehouse-worker python -m scripts.export_volatility_features_to_iceberg rehearsal

Subcommands:
    export    - read int_listing_volatility_features from the analytics
                DuckDB file (read-only), validate the source, and
                create-or-replace the Iceberg table
                (cartracker_experiments.volatility_features_snapshot)
    info      - print captured metadata (snapshots, row/vin counts,
                freshness, location) as JSON
    cleanup   - drop the table and delete its lakehouse_spike/ MinIO prefix
                (same non-PURGE, real-location cleanup as A2)
    rehearsal - export -> info -> cleanup (unless --keep), the A3 VM entry
                point

Guarded exactly like A2 (scripts/spike_iceberg_lakehouse.py):
cartracker_experiments namespace only, lakehouse_spike/warehouse/ MinIO
prefix only. The DuckDB source is mounted `:ro` in docker-compose.lakehouse.yml
-- this script never opens a write connection to it and never touches
silver/, ops_normalized/, bronze html/, or any other MinIO prefix.
"""
import argparse
import json
import os
import sys

from scripts.spike_iceberg_lakehouse import cleanup_keys
from shared.iceberg_catalog import (
    CATALOG_NAME,
    WAREHOUSE_NAME,
    key_prefix_from_location,
    require_spike_namespace,
    spark_conf_for_rest_catalog,
    table_identifier,
)
from shared.minio import BUCKET, get_boto3_client

TABLE_NAME = "volatility_features_snapshot"
SOURCE_TABLE = "int_listing_volatility_features"
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/analytics/analytics.duckdb")


def capture_metadata(
    table_name: str,
    snapshots: list[dict],
    row_count: int,
    distinct_vin17: int,
    max_latest_fetched_at,
    location: str,
) -> dict:
    """Build the plain, JSON-serializable metadata dict Gate B's MLflow
    bridge consumes (plan Sec 2.5/3.6), extended with the real-table
    validation fields A3 requires (distinct_vin17, freshness). No live
    Spark session required -- unit-tested directly with a fake snapshots
    list.
    """
    if not snapshots:
        raise ValueError("capture_metadata requires at least one snapshot")
    return {
        "catalog": CATALOG_NAME,
        "table": f"{WAREHOUSE_NAME}.{table_name}",
        "current_snapshot_id": snapshots[-1]["snapshot_id"],
        "snapshots": [s["snapshot_id"] for s in snapshots],
        "location": location,
        "row_count": row_count,
        "distinct_vin17": distinct_vin17,
        "max_latest_fetched_at": (
            str(max_latest_fetched_at) if max_latest_fetched_at is not None else None
        ),
    }


def validate_source_counts(row_count: int, distinct_vin17: int, null_vin17_count: int) -> None:
    """Guard against a corrupt/unexpected source snapshot before writing
    anything to Iceberg. vin17 is int_listing_volatility_features' declared
    primary key (not_null + unique in
    dbt/models/intermediate/int_listing_volatility_features.schema.yml), so
    every row must have a non-null vin17 and the table must have exactly one
    row per VIN.
    """
    if null_vin17_count:
        raise ValueError(f"source has {null_vin17_count} row(s) with a null vin17")
    if distinct_vin17 != row_count:
        raise ValueError(
            f"source distinct vin17 count ({distinct_vin17}) does not match "
            f"row count ({row_count}); expected exactly one row per VIN"
        )


def validate_iceberg_matches_source(iceberg_row_count: int, source_row_count: int) -> None:
    """Guard: the Iceberg table's row count must match the DuckDB source
    exactly -- proves the Spark write neither dropped nor duplicated rows."""
    if iceberg_row_count != source_row_count:
        raise ValueError(
            f"Iceberg table row count ({iceberg_row_count}) does not match "
            f"source row count ({source_row_count})"
        )


def _read_source_duckdb():
    """Open the analytics DuckDB file read-only, validate it, and return
    (source_df, row_count, distinct_vin17, max_latest_fetched_at).

    Reads straight to a pandas DataFrame via `.df()` rather than going
    through `.arrow()` -- confirmed on the VM that `.arrow()` returns a
    `pyarrow.lib.RecordBatchReader` (no `.to_pandas()`) rather than a
    `pyarrow.Table` on the duckdb version this image resolves to; `.df()`
    is unambiguous and is all `cmd_export` needs before handing off to
    `spark.createDataFrame`.
    """
    import duckdb

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        row_count, distinct_vin17, null_vin17_count, max_latest_fetched_at = con.execute(
            f"""
            SELECT
                count(*),
                count(DISTINCT vin17),
                count(*) FILTER (WHERE vin17 IS NULL),
                max(latest_fetched_at)
            FROM {SOURCE_TABLE}
            """
        ).fetchone()
        validate_source_counts(row_count, distinct_vin17, null_vin17_count)
        source_df = con.execute(f"SELECT * FROM {SOURCE_TABLE}").df()
    finally:
        con.close()
    return source_df, row_count, distinct_vin17, max_latest_fetched_at


def _get_spark():
    from pyspark.sql import SparkSession

    require_spike_namespace(WAREHOUSE_NAME)
    builder = SparkSession.builder.appName("lakehouse-gate-a3-volatility-features")
    for key, value in spark_conf_for_rest_catalog().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _ensure_namespace(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{WAREHOUSE_NAME}")


def _snapshots(spark, table_name):
    rows = spark.sql(
        f"SELECT snapshot_id, committed_at FROM {table_identifier(table_name)}.snapshots "
        "ORDER BY committed_at"
    ).collect()
    return [{"snapshot_id": row["snapshot_id"]} for row in rows]


def _table_location(spark, table_name):
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table_identifier(table_name)}").collect()
    for row in rows:
        if row["col_name"] == "Location":
            return row["data_type"]
    return ""


def cmd_export(args):
    source_df, source_row_count, distinct_vin17, _max_latest_fetched_at = _read_source_duckdb()

    spark = _get_spark()
    _ensure_namespace(spark)
    df = spark.createDataFrame(source_df)
    df.writeTo(table_identifier(TABLE_NAME)).createOrReplace()

    iceberg_row_count = spark.table(table_identifier(TABLE_NAME)).count()
    validate_iceberg_matches_source(iceberg_row_count, source_row_count)
    print(
        f"Wrote {table_identifier(TABLE_NAME)} with {iceberg_row_count} rows "
        f"(source row_count={source_row_count}, distinct_vin17={distinct_vin17})."
    )


def cmd_info(args):
    spark = _get_spark()
    snapshots = _snapshots(spark, TABLE_NAME)
    table_df = spark.table(table_identifier(TABLE_NAME))
    row_count = table_df.count()
    distinct_vin17 = table_df.select("vin17").distinct().count()
    max_latest_fetched_at = table_df.selectExpr("max(latest_fetched_at) AS m").collect()[0]["m"]
    location = _table_location(spark, TABLE_NAME)
    metadata = capture_metadata(
        TABLE_NAME, snapshots, row_count, distinct_vin17, max_latest_fetched_at, location
    )
    print(json.dumps(metadata, indent=2))
    return metadata


def cmd_cleanup(args):
    spark = _get_spark()

    # Read the table's real location BEFORE dropping it -- Lakekeeper
    # allocates its own (UUID-based) object paths, not a
    # <namespace>/<table_name> convention (see key_prefix_from_location).
    location = _table_location(spark, TABLE_NAME)
    prefix = f"{key_prefix_from_location(location)}/"

    # No PURGE -- see scripts/spike_iceberg_lakehouse.py's cmd_cleanup for
    # why: Lakekeeper rejects the S3 request-signing calls PURGE issues for a
    # table it has already unregistered. Delete the MinIO objects ourselves
    # instead, directly, with our own static credentials.
    spark.sql(f"DROP TABLE IF EXISTS {table_identifier(TABLE_NAME)}")
    print(f"Dropped {table_identifier(TABLE_NAME)}.")

    client = get_boto3_client()
    paginator = client.get_paginator("list_objects_v2")
    all_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    keys_to_delete = cleanup_keys(all_keys, prefix)
    if keys_to_delete:
        client.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": k} for k in keys_to_delete]},
        )
    print(f"Deleted {len(keys_to_delete)} object(s) under {prefix}.")


def cmd_rehearsal(args):
    cmd_export(args)
    metadata = cmd_info(args)
    if not args.keep:
        cmd_cleanup(args)
    return metadata


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("export")
    sub.add_parser("info")
    sub.add_parser("cleanup")
    rehearsal_parser = sub.add_parser("rehearsal")
    rehearsal_parser.add_argument(
        "--keep", action="store_true", help="skip cleanup at the end (debugging only)"
    )

    args = parser.parse_args(argv)
    commands = {
        "export": cmd_export,
        "info": cmd_info,
        "cleanup": cmd_cleanup,
        "rehearsal": cmd_rehearsal,
    }
    commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main() or 0)
