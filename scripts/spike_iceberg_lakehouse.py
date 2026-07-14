"""
Plan 112 Gate A2: PySpark Iceberg write/read/append/time-travel/cleanup spike
against Lakekeeper + MinIO.

Runs through the profile-gated one-shot `lakehouse-worker` container (see
docker-compose.lakehouse.yml, lakehouse/Dockerfile):

    docker compose -f docker-compose.lakehouse.yml -p cartracker-lakehouse \\
      run --rm lakehouse-worker python -m scripts.spike_iceberg_lakehouse roundtrip

A2's table is fixture-derived, not the real
`int_listing_volatility_features` snapshot (that is A3, VM-only, real prod
data). To keep this script decoupled from the dbt/silver schema, the fixture
here is a small deterministic synthetic dataset generated in-process (VIN,
observed_at, price, mileage) rather than a Parquet slice read from an
existing seeded fixture -- a deliberate scope simplification over the
original plan text, called out in docs/runbook_lakehouse.md.

Subcommands:
    write     - create the table from the first synthetic batch (snapshot 1)
    append    - append the second synthetic batch (snapshot 2)
    info      - print captured metadata (snapshots, row count, location) as JSON
    cleanup   - drop the table and delete its lakehouse_spike/ MinIO prefix
    roundtrip - write -> append -> time-travel assertion -> info -> cleanup
                (unless --keep), the CI/A2 entry point

Every operation is guarded to the `cartracker_experiments` namespace and the
`lakehouse_spike/` MinIO prefix (shared.iceberg_catalog) -- nothing here can
touch silver/, ops_normalized/, bronze html/, or any other namespace.
"""
import argparse
import json
import sys

from shared.iceberg_catalog import (
    CATALOG_NAME,
    WAREHOUSE_NAME,
    require_spike_namespace,
    require_spike_prefix,
    spark_conf_for_rest_catalog,
    table_identifier,
    table_location_prefix,
)
from shared.minio import BUCKET, get_boto3_client

TABLE_NAME = "spike_fixture"

_BATCH_1 = [
    ("1HGCM82633A004352", "2024-01-01T00:00:00Z", 24999.0, 12000),
    ("2T1BURHE0JC014906", "2024-01-01T00:00:00Z", 18999.0, 34000),
    ("3FA6P0H74HR123456", "2024-01-01T00:00:00Z", 15999.0, 51000),
    ("5YJ3E1EA7KF317000", "2024-01-01T00:00:00Z", 41999.0, 8000),
    ("WBA3B1C50DF123456", "2024-01-01T00:00:00Z", 22999.0, 29000),
]

_BATCH_2 = [
    ("1HGCM82633A004352", "2024-01-02T00:00:00Z", 24499.0, 12100),
    ("2T1BURHE0JC014906", "2024-01-02T00:00:00Z", 18799.0, 34150),
    ("3FA6P0H74HR123456", "2024-01-02T00:00:00Z", 15999.0, 51200),
    ("5YJ3E1EA7KF317000", "2024-01-02T00:00:00Z", 41999.0, 8050),
    ("WBA3B1C50DF123456", "2024-01-02T00:00:00Z", 22499.0, 29300),
]

_SCHEMA_COLUMNS = ["vin17", "observed_at", "price", "mileage"]


def capture_metadata(
    table_name: str, snapshots: list[dict], row_count: int, location: str
) -> dict:
    """Build the plain, JSON-serializable metadata dict Gate B's MLflow
    bridge consumes (plan Sec 2.5/3.6). No live Spark session required --
    unit-tested directly with a fake snapshots list.
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
    }


def cleanup_keys(all_keys: list[str], table_name: str) -> list[str]:
    """Filter a bucket's full key listing down to the ones this table's
    cleanup is allowed to delete: only keys under this table's own
    lakehouse_spike/ prefix. Raises if any candidate key would fall outside
    that prefix (shared.iceberg_catalog.require_spike_prefix) -- a guard
    against a future table-name typo silently widening the delete.
    """
    prefix = f"{table_location_prefix(table_name)}/"
    matching = [k for k in all_keys if k.startswith(prefix)]
    for key in matching:
        require_spike_prefix(key)
    return matching


def _get_spark():
    from pyspark.sql import SparkSession

    require_spike_namespace(WAREHOUSE_NAME)
    builder = SparkSession.builder.appName("lakehouse-gate-a-spike")
    for key, value in spark_conf_for_rest_catalog().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _ensure_namespace(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{WAREHOUSE_NAME}")


def cmd_write(args):
    spark = _get_spark()
    _ensure_namespace(spark)
    df = spark.createDataFrame(_BATCH_1, _SCHEMA_COLUMNS)
    df.writeTo(table_identifier(TABLE_NAME)).create()
    print(f"Created {table_identifier(TABLE_NAME)} with {df.count()} rows (snapshot 1).")


def cmd_append(args):
    spark = _get_spark()
    df = spark.createDataFrame(_BATCH_2, _SCHEMA_COLUMNS)
    df.writeTo(table_identifier(TABLE_NAME)).append()
    total = spark.table(table_identifier(TABLE_NAME)).count()
    print(
        f"Appended {df.count()} rows to {table_identifier(TABLE_NAME)}; "
        f"total now {total} (snapshot 2)."
    )


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


def cmd_info(args):
    spark = _get_spark()
    snapshots = _snapshots(spark, TABLE_NAME)
    row_count = spark.table(table_identifier(TABLE_NAME)).count()
    location = _table_location(spark, TABLE_NAME)
    metadata = capture_metadata(TABLE_NAME, snapshots, row_count, location)
    print(json.dumps(metadata, indent=2))
    return metadata


def cmd_cleanup(args):
    spark = _get_spark()
    spark.sql(f"DROP TABLE IF EXISTS {table_identifier(TABLE_NAME)} PURGE")
    print(f"Dropped {table_identifier(TABLE_NAME)}.")

    client = get_boto3_client()
    prefix = f"{table_location_prefix(TABLE_NAME)}/"
    paginator = client.get_paginator("list_objects_v2")
    all_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]
    keys_to_delete = cleanup_keys(all_keys, TABLE_NAME)
    if keys_to_delete:
        client.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": k} for k in keys_to_delete]},
        )
    print(f"Deleted {len(keys_to_delete)} object(s) under {prefix}.")


def cmd_roundtrip(args):
    spark = _get_spark()
    _ensure_namespace(spark)

    df1 = spark.createDataFrame(_BATCH_1, _SCHEMA_COLUMNS)
    df1.writeTo(table_identifier(TABLE_NAME)).create()

    df2 = spark.createDataFrame(_BATCH_2, _SCHEMA_COLUMNS)
    df2.writeTo(table_identifier(TABLE_NAME)).append()

    snapshots = _snapshots(spark, TABLE_NAME)
    assert len(snapshots) == 2, f"expected 2 snapshots, got {len(snapshots)}"

    first_snapshot_id = snapshots[0]["snapshot_id"]
    first_snapshot_rows = (
        spark.read.option("snapshot-id", first_snapshot_id)
        .table(table_identifier(TABLE_NAME))
        .count()
    )
    current_rows = spark.table(table_identifier(TABLE_NAME)).count()
    assert first_snapshot_rows == len(_BATCH_1), (
        f"time travel to snapshot 1 returned {first_snapshot_rows} rows, "
        f"expected {len(_BATCH_1)}"
    )
    assert current_rows == len(_BATCH_1) + len(_BATCH_2), (
        f"current table has {current_rows} rows, expected "
        f"{len(_BATCH_1) + len(_BATCH_2)}"
    )
    print(
        f"Time-travel proof: snapshot 1 = {first_snapshot_rows} rows, "
        f"current = {current_rows} rows."
    )

    location = _table_location(spark, TABLE_NAME)
    metadata = capture_metadata(TABLE_NAME, snapshots, current_rows, location)
    print(json.dumps(metadata, indent=2))

    if not args.keep:
        cmd_cleanup(args)

    return metadata


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("write")
    sub.add_parser("append")
    sub.add_parser("info")
    sub.add_parser("cleanup")
    roundtrip_parser = sub.add_parser("roundtrip")
    roundtrip_parser.add_argument(
        "--keep", action="store_true", help="skip cleanup at the end (debugging only)"
    )

    args = parser.parse_args(argv)
    commands = {
        "write": cmd_write,
        "append": cmd_append,
        "info": cmd_info,
        "cleanup": cmd_cleanup,
        "roundtrip": cmd_roundtrip,
    }
    commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main() or 0)
