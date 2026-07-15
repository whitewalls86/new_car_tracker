"""
Plan 112 Gate A2: unit tests for shared/iceberg_catalog.py -- the Spark-conf
builder, namespace/prefix guards, and warehouse-registration payload. No live
Spark/Lakekeeper/MinIO required.
"""
import pytest

from shared.iceberg_catalog import (
    CATALOG_NAME,
    SPIKE_PREFIX,
    WAREHOUSE_NAME,
    UnsafeNamespaceError,
    UnsafePrefixError,
    require_spike_namespace,
    require_spike_prefix,
    spark_conf_for_rest_catalog,
    table_identifier,
    table_location_prefix,
    warehouse_storage_payload,
)


class TestNamespaceGuard:
    def test_allows_spike_namespace(self):
        require_spike_namespace(WAREHOUSE_NAME)  # must not raise

    def test_rejects_other_namespace(self):
        with pytest.raises(UnsafeNamespaceError):
            require_spike_namespace("cartracker_silver")


class TestPrefixGuard:
    def test_allows_spike_prefix_key(self):
        require_spike_prefix(f"{SPIKE_PREFIX}/cartracker_experiments/spike_fixture/data/f1.parquet")

    def test_rejects_silver_prefix(self):
        with pytest.raises(UnsafePrefixError):
            require_spike_prefix("silver_normalized/observations/f1.parquet")

    def test_rejects_ops_normalized_prefix(self):
        with pytest.raises(UnsafePrefixError):
            require_spike_prefix("ops_normalized/price_observation_events/f1.parquet")


class TestTableIdentifiers:
    def test_table_identifier_uses_catalog_and_warehouse(self):
        assert table_identifier("spike_fixture") == f"{CATALOG_NAME}.{WAREHOUSE_NAME}.spike_fixture"

    def test_table_location_prefix_is_under_spike_prefix(self):
        prefix = table_location_prefix("spike_fixture")
        assert prefix.startswith(f"{SPIKE_PREFIX}/")
        assert prefix.endswith("/spike_fixture")


class TestSparkConfBuilder:
    def test_requires_lakekeeper_catalog_uri(self, monkeypatch):
        monkeypatch.delenv("LAKEKEEPER_CATALOG_URI", raising=False)
        with pytest.raises(KeyError):
            spark_conf_for_rest_catalog()

    def test_builds_expected_rest_catalog_conf(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        monkeypatch.setenv("MINIO_ROOT_USER", "cartracker")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")
        monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")

        conf = spark_conf_for_rest_catalog()

        assert conf[f"spark.sql.catalog.{CATALOG_NAME}"] == "org.apache.iceberg.spark.SparkCatalog"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.type"] == "rest"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.uri"] == "http://lakekeeper:8181/catalog"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.warehouse"] == WAREHOUSE_NAME
        io_impl = conf[f"spark.sql.catalog.{CATALOG_NAME}.io-impl"]
        assert io_impl == "org.apache.iceberg.aws.s3.S3FileIO"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id"] == "cartracker"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key"] == "secret"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access"] == "true"


class TestWarehouseStoragePayload:
    def test_points_at_bronze_bucket_and_spike_prefix(self, monkeypatch):
        monkeypatch.setenv("MINIO_ROOT_USER", "cartracker")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")

        payload = warehouse_storage_payload()

        assert payload["warehouse-name"] == WAREHOUSE_NAME
        assert payload["storage-profile"]["bucket"] == "bronze"
        assert payload["storage-profile"]["key-prefix"] == SPIKE_PREFIX
        assert payload["storage-profile"]["sts-enabled"] is False
        assert payload["storage-credential"]["access-key-id"] == "cartracker"
        assert payload["storage-credential"]["secret-access-key"] == "secret"
