"""
Spark / Iceberg REST catalog configuration helper for the Plan 112 Gate A2
spike (docker-compose.lakehouse.yml, lakehouse-worker). Mirrors
shared/duckdb_s3.py's env-driven config pattern for the Spark side.

Plan 125 Gate 0.5 (catalog-neutral preflight) makes this module the single
catalog-config chokepoint (report guardrail R1) and splits it in two:

  * Consumer-facing config (`catalog_uri`, `spark_conf_for_rest_catalog`)
    standardizes on the neutral `ICEBERG_CATALOG_*` env names, and still
    accepts the legacy `LAKEKEEPER_CATALOG_URI` as a fallback during the
    migration (see `catalog_uri`). Either way the Lakekeeper name stops at
    this module: swapping Lakekeeper for another Iceberg REST catalog edits
    this file rather than every Spark/dbt script (R2).
  * Provisioning-facing config (`warehouse_storage_payload`) stays
    Lakekeeper-specific -- its management-API schema has no neutral
    equivalent, and per R6 that coupling is confined to provisioning (R6).

Kept dependency-free (no pyspark import) so it can be unit-tested in the
regular `unit-tests` CI job, which does not install pyspark.
"""
import os

from shared.minio import BUCKET

# Spark-side catalog alias. Deliberately named for this project, not for the
# catalog implementation behind it: it is baked into every `cartracker.<ns>.<table>`
# identifier in scripts, dbt models, and captured MLflow provenance, so it must
# stay stable across a future catalog swap (Plan 125 Gate 0.5 / R1).
CATALOG_NAME = "cartracker"
WAREHOUSE_NAME = os.environ.get("ICEBERG_WAREHOUSE_NAME", "cartracker_experiments")
# Dedicated top-level MinIO prefix, disjoint from silver/, ops_normalized/,
# and bronze html/. Nothing production reads it.
SPIKE_PREFIX = "lakehouse_spike/warehouse"


class CatalogConfigError(RuntimeError):
    """Raised when neither the neutral nor the legacy catalog URI env var is
    set, so no Iceberg REST endpoint can be resolved."""


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


def catalog_uri() -> str:
    """Resolve the Iceberg REST catalog endpoint for consumer (Spark/dbt)
    code, preferring the neutral Plan 125 name over the legacy Lakekeeper one.

    Resolution order (Plan 125 Gate 0.5 / R2):

      1. ICEBERG_CATALOG_URI    -- neutral, what new config should set.
      2. LAKEKEEPER_CATALOG_URI -- legacy fallback, kept so existing A2/A3/A4
         local, CI, and VM env files and shells keep working unchanged. It is
         a compatibility shim, not a second supported name; remove it once no
         environment sets it.

    Raises CatalogConfigError (not KeyError) if neither is set, so the error
    names both accepted vars rather than only whichever we happened to read
    first.
    """
    uri = os.environ.get("ICEBERG_CATALOG_URI") or os.environ.get(
        "LAKEKEEPER_CATALOG_URI"
    )
    if not uri:
        raise CatalogConfigError(
            "No Iceberg catalog URI configured: set ICEBERG_CATALOG_URI "
            "(preferred) or the legacy LAKEKEEPER_CATALOG_URI."
        )
    return uri


def table_identifier(table_name: str) -> str:
    return f"{CATALOG_NAME}.{WAREHOUSE_NAME}.{table_name}"


def key_prefix_from_location(location: str) -> str:
    """Convert a table's actual `s3://bucket/...` location (as reported by
    Spark/Iceberg) into a bucket-relative MinIO key prefix for boto3
    listing/deletion.

    Lakekeeper allocates its own (UUID-based) object paths under the
    warehouse's key-prefix -- it does NOT follow a
    `<namespace>/<table_name>` naming convention. Cleanup must read a
    table's real location rather than reconstructing a guessed path, or it
    silently deletes nothing. Still guarded by require_spike_prefix so a
    location outside lakehouse_spike/ is rejected rather than trusted.
    """
    scheme_prefix = f"s3://{BUCKET}/"
    if not location.startswith(scheme_prefix):
        raise UnsafePrefixError(
            f"Table location {location!r} does not start with {scheme_prefix!r}; "
            "refusing to derive a cleanup prefix from it."
        )
    key_prefix = location[len(scheme_prefix):]
    require_spike_prefix(f"{key_prefix}/")
    return key_prefix


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
    uri = catalog_uri()
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{CATALOG_NAME}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG_NAME}.type": "rest",
        f"spark.sql.catalog.{CATALOG_NAME}.uri": uri,
        f"spark.sql.catalog.{CATALOG_NAME}.warehouse": WAREHOUSE_NAME,
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint": minio_endpoint,
        f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access": "true",
        f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id": minio_user,
        f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key": minio_password,
    }


def spark_conf_for_s3a_reads() -> dict:
    """Hadoop-AWS `s3a://` config, for Spark reading plain (non-Iceberg)
    Parquet -- i.e. the normalized silver/ops_normalized files that dbt
    staging models sit on top of.

    This is a SEPARATE mechanism from spark_conf_for_rest_catalog()'s
    S3FileIO, and both are needed. S3FileIO is Iceberg-internal: it only
    serves Iceberg's own table reads/writes. A plain `spark.read.parquet(...)`
    never touches Iceberg, so it resolves the URI scheme through Hadoop's
    FileSystem API instead, which needs hadoop-aws on the classpath and its
    own credentials. Without this, reading normalized Parquet fails with
    `UnsupportedFileSystemException: No FileSystem for scheme "s3"`
    (verified at Gate A).

    `s3a://` (not `s3://`) is deliberate: hadoop-aws only registers the
    `s3a` scheme. Iceberg table locations keep using `s3://` via S3FileIO --
    the two schemes coexist in one session, each served by its own stack.
    """
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]
    return {
        "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
        "spark.hadoop.fs.s3a.access.key": minio_user,
        "spark.hadoop.fs.s3a.secret.key": minio_password,
        # MinIO serves bucket-as-path, not bucket-as-subdomain.
        "spark.hadoop.fs.s3a.path.style.access": "true",
        # The local/VM MinIO endpoint is plain http.
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
            minio_endpoint.startswith("https://")
        ).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }


def spark_conf_for_dbt_session() -> dict:
    """Full Spark session config for a dbt-spark (`method: session`) run
    against the Iceberg REST catalog.

    Composed from the two read/write mechanisms above, plus the two settings
    that fail SILENTLY if omitted (docs/plan_125_portability_audit.md, "Two
    config details that will bite Gate A"):

      * spark.sql.defaultCatalog -- dbt-spark relations are two-part
        (`schema.identifier`); the adapter has no `catalog:` profile field.
        Without this, dbt resolves into the built-in `spark_catalog` and
        writes NO Iceberg at all, while still exiting 0.
      * spark.sql.session.timeZone -- Spark has no TIMESTAMPTZ; its TIMESTAMP
        is instant-typed and resolves offsets against the session zone.
        Unpinned, every timestamp silently shifts by the host's local offset,
        surfacing as parity drift rather than an error.
    """
    conf = spark_conf_for_rest_catalog()
    conf.update(spark_conf_for_s3a_reads())
    conf["spark.sql.defaultCatalog"] = CATALOG_NAME
    conf["spark.sql.session.timeZone"] = "UTC"
    return conf


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
