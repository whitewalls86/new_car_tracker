"""
Plan 125 Gate A: unit tests for the dbt-spark session configuration --
shared/iceberg_catalog.spark_conf_for_dbt_session() and the
scripts/run_dbt_spark.py guards.

These exist because Gate A's two worst failure modes are SILENT (see
docs/plan_125_portability_audit.md, "Two config details that will bite Gate A"):
without spark.sql.defaultCatalog, dbt writes to spark_catalog and still exits 0;
without spark.sql.session.timeZone, every timestamp shifts. Neither raises. A
regression in this config would not fail any dbt run -- so it has to fail here.

No live Spark/Lakekeeper/MinIO: the conf builder is env-driven and pure, and
run_dbt_spark imports pyspark/dbt only inside functions.
"""
import pytest

from scripts.run_dbt_spark import (
    UNUSED_POSTGRES_URL,
    assert_default_catalog,
    stub_parse_only_env,
)
from shared.iceberg_catalog import CATALOG_NAME, spark_conf_for_dbt_session


@pytest.fixture
def catalog_env(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://lakekeeper:8181/catalog")
    monkeypatch.setenv("MINIO_ROOT_USER", "cartracker")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "cartracker123")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")


class TestSparkConfForDbtSession:
    def test_pins_default_catalog_to_cartracker(self, catalog_env):
        """The single most important line in the Gate A config: dbt-spark
        relations are two-part, so without this dbt silently writes to
        spark_catalog -- no Iceberg -- and still exits 0."""
        assert spark_conf_for_dbt_session()["spark.sql.defaultCatalog"] == CATALOG_NAME

    def test_pins_session_timezone_to_utc(self, catalog_env):
        """Spark has no TIMESTAMPTZ; an unpinned session zone silently shifts
        every timestamp, surfacing as parity drift rather than an error."""
        assert spark_conf_for_dbt_session()["spark.sql.session.timeZone"] == "UTC"

    def test_includes_iceberg_rest_catalog_config(self, catalog_env):
        conf = spark_conf_for_dbt_session()

        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.type"] == "rest"
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.uri"] == "http://lakekeeper:8181/catalog"
        assert (
            conf[f"spark.sql.catalog.{CATALOG_NAME}.io-impl"]
            == "org.apache.iceberg.aws.s3.S3FileIO"
        )

    def test_includes_s3a_config_for_plain_parquet_reads(self, catalog_env):
        """Iceberg's S3FileIO only serves Iceberg tables. Reading the raw
        ops_normalized Parquet is a plain Hadoop FileSystem read, which needs
        its own s3a credentials or it fails with 'No FileSystem for scheme'."""
        conf = spark_conf_for_dbt_session()

        assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
        assert conf["spark.hadoop.fs.s3a.access.key"] == "cartracker"
        assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"

    def test_ssl_disabled_for_http_endpoint(self, catalog_env):
        assert spark_conf_for_dbt_session()["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"

    def test_ssl_enabled_for_https_endpoint(self, catalog_env, monkeypatch):
        monkeypatch.setenv("MINIO_ENDPOINT", "https://minio.example.com")

        assert spark_conf_for_dbt_session()["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"

    def test_iceberg_keeps_s3_scheme_while_hadoop_uses_s3a(self, catalog_env):
        """The two schemes coexist deliberately: Lakekeeper hands out s3://
        locations served by S3FileIO, while hadoop-aws only registers s3a://.
        Neither should be rewritten to match the other."""
        conf = spark_conf_for_dbt_session()

        assert "s3.endpoint" in " ".join(conf.keys())
        assert conf[f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access"] == "true"


class _FakeSparkConf:
    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


class _FakeSpark:
    def __init__(self, values):
        self.conf = _FakeSparkConf(values)


class TestAssertDefaultCatalog:
    def test_passes_when_default_catalog_is_ours(self):
        assert_default_catalog(_FakeSpark({"spark.sql.defaultCatalog": CATALOG_NAME}))

    def test_raises_when_default_catalog_is_spark_catalog(self):
        """The exact trap: this is what an unconfigured session looks like, and
        dbt would happily build into it and exit 0."""
        with pytest.raises(SystemExit) as exc:
            assert_default_catalog(_FakeSpark({"spark.sql.defaultCatalog": "spark_catalog"}))

        assert "spark_catalog" in str(exc.value)

    def test_raises_when_default_catalog_unset(self):
        with pytest.raises(SystemExit):
            assert_default_catalog(_FakeSpark({}))


class TestStubParseOnlyEnv:
    def test_sets_unroutable_postgres_url_when_unset(self, monkeypatch):
        """dbt renders every source's Jinja at parse time regardless of
        --select, so the postgres_scan sources need POSTGRES_URL to exist even
        though Gate A never reads them."""
        monkeypatch.delenv("POSTGRES_URL", raising=False)
        stub_parse_only_env()

        import os

        assert os.environ["POSTGRES_URL"] == UNUSED_POSTGRES_URL

    def test_does_not_override_a_real_postgres_url(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_URL", "postgresql://real:real@postgres:5432/cartracker")
        stub_parse_only_env()

        import os

        assert os.environ["POSTGRES_URL"] == "postgresql://real:real@postgres:5432/cartracker"
