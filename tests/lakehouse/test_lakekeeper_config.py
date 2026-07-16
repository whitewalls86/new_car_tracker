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
    CatalogConfigError,
    UnsafeNamespaceError,
    UnsafePrefixError,
    catalog_uri,
    key_prefix_from_location,
    require_spike_namespace,
    require_spike_prefix,
    spark_conf_for_rest_catalog,
    table_identifier,
    warehouse_storage_payload,
)
from shared.minio import BUCKET


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


class TestKeyPrefixFromLocation:
    def test_strips_bucket_scheme_prefix(self):
        # Real shape: Lakekeeper allocates a UUID-based path, not
        # <namespace>/<table_name> -- the helper must not assume otherwise.
        location = f"s3://{BUCKET}/{SPIKE_PREFIX}/019f65f6-b861-7363-bc46-0dd926f68637"
        prefix = key_prefix_from_location(location)
        assert prefix == f"{SPIKE_PREFIX}/019f65f6-b861-7363-bc46-0dd926f68637"

    def test_rejects_location_outside_spike_prefix(self):
        with pytest.raises(UnsafePrefixError):
            key_prefix_from_location(f"s3://{BUCKET}/silver_normalized/observations")

    def test_rejects_location_in_a_different_bucket(self):
        with pytest.raises(UnsafePrefixError):
            key_prefix_from_location(f"s3://some-other-bucket/{SPIKE_PREFIX}/abc")


@pytest.fixture
def no_catalog_env(monkeypatch):
    """Neither catalog URI var set -- a developer's shell may export either."""
    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    monkeypatch.delenv("LAKEKEEPER_CATALOG_URI", raising=False)
    return monkeypatch


class TestCatalogUriResolution:
    """Plan 125 Gate 0.5 / R2: consumers resolve the catalog endpoint from the
    neutral name, with the legacy Lakekeeper name kept only as a fallback."""

    def test_prefers_neutral_env_var(self, no_catalog_env):
        no_catalog_env.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        assert catalog_uri() == "http://catalog:8181/catalog"

    def test_falls_back_to_legacy_lakekeeper_env_var(self, no_catalog_env):
        no_catalog_env.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        assert catalog_uri() == "http://lakekeeper:8181/catalog"

    def test_neutral_wins_when_both_are_set(self, no_catalog_env):
        """A catalog swap sets the neutral var; a stale legacy var left over in
        an env file or shell must not silently pull consumers back."""
        no_catalog_env.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        no_catalog_env.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        assert catalog_uri() == "http://catalog:8181/catalog"

    def test_raises_naming_both_vars_when_neither_is_set(self, no_catalog_env):
        with pytest.raises(CatalogConfigError) as exc:
            catalog_uri()
        assert "ICEBERG_CATALOG_URI" in str(exc.value)
        assert "LAKEKEEPER_CATALOG_URI" in str(exc.value)

    def test_empty_value_is_not_treated_as_configured(self, no_catalog_env):
        """`ICEBERG_CATALOG_URI=` in a compose/env file must fall through to the
        legacy var, not configure Spark with an empty endpoint."""
        no_catalog_env.setenv("ICEBERG_CATALOG_URI", "")
        no_catalog_env.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        assert catalog_uri() == "http://lakekeeper:8181/catalog"


class TestCatalogAliasIsNeutral:
    def test_catalog_alias_does_not_name_an_implementation(self):
        """The Spark alias is baked into every table identifier and captured
        provenance string, so it must survive a catalog swap unchanged."""
        assert CATALOG_NAME == "cartracker"
        for vendor in ("lakekeeper", "polaris", "gravitino", "unity"):
            assert vendor not in CATALOG_NAME.lower()


class TestSparkConfBuilder:
    def test_requires_a_catalog_uri(self, no_catalog_env):
        with pytest.raises(CatalogConfigError):
            spark_conf_for_rest_catalog()

    def test_uses_neutral_catalog_uri(self, no_catalog_env):
        no_catalog_env.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        no_catalog_env.setenv("MINIO_ROOT_USER", "cartracker")
        no_catalog_env.setenv("MINIO_ROOT_PASSWORD", "secret")

        conf = spark_conf_for_rest_catalog()

        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.uri"] == "http://catalog:8181/catalog"

    def test_builds_expected_rest_catalog_conf_from_legacy_env(self, monkeypatch):
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
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


class TestConsumerConfigIsCatalogNeutral:
    """Plan 125 Gate 0.5 / R1+R2: the Spark conf a consumer script receives is
    plain Iceberg REST -- swapping catalogs must not require touching it."""

    def test_spark_conf_carries_no_lakekeeper_specific_keys_or_values(self, no_catalog_env):
        no_catalog_env.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        no_catalog_env.setenv("MINIO_ROOT_USER", "cartracker")
        no_catalog_env.setenv("MINIO_ROOT_PASSWORD", "secret")

        conf = spark_conf_for_rest_catalog()

        # The URI value is operator-supplied and may legitimately name a host;
        # the config keys and the rest of the wiring must not.
        for key, value in conf.items():
            assert "lakekeeper" not in key.lower()
            if key != f"spark.sql.catalog.{CATALOG_NAME}.uri":
                assert "lakekeeper" not in str(value).lower()

    def test_storage_profile_payload_is_not_reachable_from_spark_conf(self, no_catalog_env):
        """The Lakekeeper management-API payload stays provisioning-only (R6):
        none of its storage-profile shape leaks into consumer Spark config."""
        no_catalog_env.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        no_catalog_env.setenv("MINIO_ROOT_USER", "cartracker")
        no_catalog_env.setenv("MINIO_ROOT_PASSWORD", "secret")

        conf = spark_conf_for_rest_catalog()

        for provisioning_key in ("warehouse-name", "storage-profile", "storage-credential"):
            assert provisioning_key not in conf


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
