"""
Spark / Iceberg REST catalog configuration helper for the Plan 112 Gate A2
spike (docker-compose.lakehouse.yml, lakehouse-worker). Mirrors
shared/duckdb_s3.py's env-driven config pattern for the Spark side.

Kept dependency-free (no pyspark import) so it can be unit-tested in the
regular `unit-tests` CI job, which does not install pyspark.
"""
import os

from shared.minio import BUCKET

CATALOG_NAME = "cartracker"
WAREHOUSE_NAME = os.environ.get("ICEBERG_WAREHOUSE_NAME", "cartracker_experiments")
# Dedicated top-level MinIO prefix, disjoint from silver/, ops_normalized/,
# and bronze html/. Nothing production reads it.
SPIKE_PREFIX = "lakehouse_spike/warehouse"


class UnsafeNamespaceError(RuntimeError):
    """Raised when a Gate A spike operation targets a namespace other than
    the spike warehouse/namespace."""


class UnsafePrefixError(RuntimeError):
    """Raised when a Gate A spike operation would touch a MinIO key outside
    the isolated lakehouse_spike/ prefix."""


def require_spike_namespace(namespace: str) -> None:
    """Guard: Gate A scripts only ever create/drop tables in the
    cartracker_experiments namespace (plan Sec 2.8 naming rule)."""
    if namespace != WAREHOUSE_NAME:
        raise UnsafeNamespaceError(
            f"Gate A spike scripts only operate against the {WAREHOUSE_NAME!r} "
            f"namespace; refusing to touch {namespace!r}."
        )


def require_spike_prefix(key: str) -> None:
    """Guard: cleanup only ever deletes MinIO keys under lakehouse_spike/."""
    if not key.startswith(f"{SPIKE_PREFIX}/"):
        raise UnsafePrefixError(
            f"Gate A cleanup only ever touches keys under {SPIKE_PREFIX}/; "
            f"refusing to touch {key!r}."
        )


def table_identifier(table_name: str) -> str:
    return f"{CATALOG_NAME}.{WAREHOUSE_NAME}.{table_name}"


def table_location_prefix(table_name: str) -> str:
    return f"{SPIKE_PREFIX}/{WAREHOUSE_NAME}/{table_name}"


def spark_conf_for_rest_catalog() -> dict:
    """Build the Spark session config dict wiring Iceberg's SparkCatalog at
    spark.sql.catalog.cartracker to Lakekeeper's REST endpoint, and Iceberg's
    native S3FileIO (iceberg-aws-bundle, AWS SDK v2) to MinIO. Env-driven, no
    live Spark session required to construct or unit-test this.

    S3FileIO, not Hadoop-AWS's S3AFileSystem, is deliberate: Lakekeeper hands
    Spark `s3://...` table locations (not `s3a://`), and S3FileIO is what
    actually serves that scheme -- Hadoop's generic FileSystem has no
    registered handler for a bare `s3` scheme.
    """
    catalog_uri = os.environ["LAKEKEEPER_CATALOG_URI"]
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG_NAME}.type": "rest",
        f"spark.sql.catalog.{CATALOG_NAME}.uri": catalog_uri,
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse": WAREHOUSE_NAME,
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint": minio_endpoint,
        f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access": "true",
        f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id": minio_user,
        f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key": minio_password,
    }


def warehouse_storage_payload() -> dict:
    """Lakekeeper management-API warehouse-registration payload for the
    single Gate A `cartracker_experiments` warehouse. Points the storage
    profile at the isolated lakehouse_spike/ prefix of the real `bronze`
    bucket (VM/local) or the CI override's throwaway bucket -- never any
    other prefix.
    """
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return {
        "warehouse-name": WAREHOUSE_NAME,
        "storage-profile": {
            "type": "s3",
            "bucket": BUCKET,
            "key-prefix": SPIKE_PREFIX,
            "endpoint": minio_endpoint,
            "region": "local",
            "path-style-access": True,
            "flavor": "s3-compat",
            # Static access-key credentials only (below) -- Gate A has no STS
            # vending set up. Required field per Lakekeeper's S3Profile schema.
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": os.environ.get("MINIO_ROOT_USER", ""),
            "secret-access-key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
        },
    }
