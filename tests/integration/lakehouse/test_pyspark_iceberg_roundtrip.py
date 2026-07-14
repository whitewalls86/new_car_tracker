"""
Plan 112 Gate A2: PySpark Iceberg write/read/append/time-travel/cleanup
round-trip against a real Lakekeeper + MinIO stack.

Runs in the dedicated `lakehouse` GitHub Actions job, against the same
job-local, throwaway stack A1's smoke test uses (see
docker-compose.lakehouse.yml, docker-compose.lakehouse.ci.yml,
docs/runbook_lakehouse.md). Requires `pyspark` + the Iceberg-Spark-runtime/
Hadoop-AWS jars, installed only in this job (§4.5 of the Gate A/B plan) --
skipped automatically if pyspark isn't importable, so this file does not
break the regular `unit-tests` job if ever collected there by mistake.
"""
import os

import pytest

pytestmark = pytest.mark.integration

pyspark = pytest.importorskip("pyspark", reason="pyspark only installed in the lakehouse CI job")


def _base_uri():
    return os.environ.get("LAKEKEEPER_BASE_URI", "http://localhost:18181")


@pytest.fixture(autouse=True, scope="module")
def _lakehouse_env():
    os.environ.setdefault("LAKEKEEPER_CATALOG_URI", f"{_base_uri()}/catalog")
    os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:19000")
    os.environ.setdefault("MINIO_ROOT_USER", "cartracker")
    os.environ.setdefault("MINIO_ROOT_PASSWORD", "cartracker123")
    os.environ.setdefault("ICEBERG_WAREHOUSE_NAME", "cartracker_experiments")


@pytest.fixture(scope="module")
def registered_warehouse():
    from scripts.register_lakehouse_warehouse import register_warehouse

    register_warehouse()


class TestPySparkIcebergRoundtrip:
    def test_write_append_time_travel_cleanup(self, registered_warehouse):
        import argparse

        from scripts.spike_iceberg_lakehouse import cmd_roundtrip

        metadata = cmd_roundtrip(argparse.Namespace(keep=False))

        assert len(metadata["snapshots"]) == 2
        assert metadata["row_count"] == 10
        assert metadata["table"] == "cartracker_experiments.spike_fixture"
