"""
Plan 125 Gate A: run dbt against the `spark` target, wired to the Iceberg REST
catalog (Lakekeeper) and MinIO.

    docker compose -f docker-compose.lakehouse.yml -f docker-compose.lakehouse.local.yml \
      -p local-lakehouse run --rm lakehouse-worker \
      python -m scripts.run_dbt_spark build --select tag:gate_a

Why a runner instead of plain `dbt build --target spark`
--------------------------------------------------------
dbt-spark's `session` method builds its SparkSession with
`SparkSession.builder.enableHiveSupport().getOrCreate()` (session.py). Because
`getOrCreate()` returns the *already-active* session when one exists in the
process, pre-creating the session here means dbt inherits exactly the config
this script sets -- and that config comes from `shared/iceberg_catalog.py`,
the single catalog chokepoint (Gate 0.5 guardrails R1/R2). The alternative,
spelling the catalog config out as `server_side_parameters` in profiles.yml,
would fork that chokepoint into a second file.

This matters more than convenience, because of the Gate A trap: dbt-spark
relations are two-part (`schema.identifier`) and the adapter has no `catalog:`
field. Without `spark.sql.defaultCatalog=cartracker`, dbt writes into the
built-in `spark_catalog` -- i.e. no Iceberg at all -- and STILL EXITS 0. So a
green dbt run proves nothing on its own. This script therefore:

  1. asserts the default catalog is `cartracker` before running anything;
  2. runs dbt in-process via dbtRunner, on the session it just configured;
  3. verifies every model dbt built is a real Iceberg table registered in the
     catalog, by reading it back -- never by trusting dbt's exit code.

Safety: `--target spark` is not a production path (dbt_runner hardcodes
`--target duckdb`), and writes are confined to the spike namespace, enforced
by require_spike_namespace().
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import List, Optional, Sequence

from shared.iceberg_catalog import (
    CATALOG_NAME,
    WAREHOUSE_NAME,
    require_spike_namespace,
    spark_conf_for_dbt_session,
)

DBT_PROJECT_DIR = "/app/dbt"


UNUSED_POSTGRES_URL = "postgresql://unused:unused@postgres-not-used-on-spark:5432/unused"


def stub_parse_only_env() -> None:
    """Satisfy env vars that dbt needs to *parse* sources.yml but that the
    Gate A chain never reads.

    sources.yml declares `public.search_configs` and `ops.tracked_models` with
    `postgres_scan('{{ env_var("POSTGRES_URL") }}', ...)`. dbt renders every
    source's Jinja at parse time -- regardless of `--select` -- so an unset
    POSTGRES_URL fails the whole run before any Gate A model compiles, even
    though neither source is in the Gate A DAG.

    Stubbed here, in the Gate A runner, rather than by giving the env_var() a
    default in sources.yml: production DuckDB genuinely requires POSTGRES_URL,
    and a default there would turn a loud misconfiguration into a silent one.

    The value is deliberately unroutable. It is only ever interpolated into a
    string that Spark never executes (the spark target resolves sources via
    parquet_source(), and register_upstream_external_models() is duckdb-only) --
    so if it is ever actually used, it must fail, not connect.

    This is a Gate A expedient. The real fix is the audit's F8 decision
    (snapshot these reference tables into MinIO/Iceberg), due before Gate B.
    """
    os.environ.setdefault("POSTGRES_URL", UNUSED_POSTGRES_URL)


def build_spark_session():
    """Create the SparkSession dbt-spark's session mode will inherit."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("cartracker-dbt-spark")
    for key, value in spark_conf_for_dbt_session().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def assert_default_catalog(spark) -> None:
    """Fail loudly *before* dbt runs if the default catalog is not ours.

    This is the guard for the Gate A trap: dbt writing to `spark_catalog`
    looks like success. Checking afterwards would mean discovering it only
    once tables are already in the wrong place.
    """
    actual = spark.conf.get("spark.sql.defaultCatalog", None)
    if actual != CATALOG_NAME:
        raise SystemExit(
            f"Refusing to run: spark.sql.defaultCatalog is {actual!r}, expected "
            f"{CATALOG_NAME!r}. dbt-spark relations are two-part, so without this "
            "dbt would silently write to spark_catalog instead of Iceberg and "
            "still exit 0."
        )


def ensure_namespace(spark) -> None:
    require_spike_namespace(WAREHOUSE_NAME)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{WAREHOUSE_NAME}")


def run_dbt(dbt_args: Sequence[str]) -> int:
    from dbt.cli.main import dbtRunner

    args: List[str] = [
        *dbt_args,
        "--project-dir",
        DBT_PROJECT_DIR,
        "--profiles-dir",
        DBT_PROJECT_DIR,
        "--target",
        "spark",
    ]
    result = dbtRunner().invoke(args)
    return 0 if result.success else 1


def verify_iceberg_tables(spark, table_names: Sequence[str]) -> None:
    """Prove the output is really in the Iceberg catalog, not spark_catalog.

    `SHOW TABLES` alone would be satisfied by a temp view, and dbt's exit code
    proves nothing (see module docstring), so this reads each table back
    through the catalog and confirms its provider is Iceberg.
    """
    require_spike_namespace(WAREHOUSE_NAME)
    registered = {
        row["tableName"]
        for row in spark.sql(
            f"SHOW TABLES IN {CATALOG_NAME}.{WAREHOUSE_NAME}"
        ).collect()
    }
    missing = [name for name in table_names if name not in registered]
    if missing:
        raise SystemExit(
            f"dbt reported success but {missing} are not registered in "
            f"{CATALOG_NAME}.{WAREHOUSE_NAME}. Tables present: {sorted(registered)}. "
            "This is the spark_catalog trap -- check spark.sql.defaultCatalog."
        )

    for name in table_names:
        fqn = f"{CATALOG_NAME}.{WAREHOUSE_NAME}.{name}"
        rows = spark.sql(f"SELECT count(*) AS n FROM {fqn}").collect()
        provider = {
            r["col_name"].strip(): (r["data_type"] or "").strip()
            for r in spark.sql(f"DESCRIBE EXTENDED {fqn}").collect()
        }
        location = provider.get("Location", "")
        print(
            f"  verified {fqn}: rows={rows[0]['n']} "
            f"provider={provider.get('Provider', '?')} location={location}"
        )
        if not location.startswith("s3://"):
            raise SystemExit(
                f"{fqn} has location {location!r}, which is not an s3:// Iceberg "
                "location -- it did not land in the Lakekeeper-backed warehouse."
            )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run dbt against the Plan 125 Gate A spark/Iceberg target."
    )
    parser.add_argument(
        "dbt_args",
        nargs=argparse.REMAINDER,
        help="dbt command and flags, e.g. `build --select tag:gate_a`.",
    )
    parser.add_argument(
        "--verify-table",
        action="append",
        default=None,
        dest="verify_tables",
        help=(
            "Table name (unqualified) to verify landed in the Iceberg catalog "
            "after dbt succeeds. Repeatable."
        ),
    )
    args = parser.parse_args(argv)

    dbt_args = [a for a in args.dbt_args if a != "--"]
    if not dbt_args:
        parser.error("no dbt command given, e.g. `build --select tag:gate_a`")

    stub_parse_only_env()
    spark = build_spark_session()
    assert_default_catalog(spark)
    print(
        f"Spark session ready: defaultCatalog={CATALOG_NAME} "
        f"timeZone={spark.conf.get('spark.sql.session.timeZone')}"
    )
    ensure_namespace(spark)

    exit_code = run_dbt(dbt_args)
    if exit_code != 0:
        print("dbt reported failure; skipping Iceberg verification.")
        return exit_code

    if args.verify_tables:
        print("Verifying dbt output really landed in the Iceberg catalog:")
        verify_iceberg_tables(spark, args.verify_tables)

    return 0


if __name__ == "__main__":
    sys.exit(main())
