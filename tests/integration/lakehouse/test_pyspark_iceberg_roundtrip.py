"""
Plan 112 Gate A2: PySpark Iceberg write/read/append/time-travel/cleanup
round-trip against a real Lakekeeper + MinIO stack.

The `lakehouse` GitHub Actions job's actual A2 round-trip runs this same
logic (scripts.spike_iceberg_lakehouse.cmd_roundtrip) inside the
`lakehouse-worker` container via `docker compose run`, not via pytest on the
bare runner -- that way CI exercises the same lakehouse/Dockerfile image and
container-DNS networking the VM/runbook path uses. This test file is a
local-dev convenience for iterating against a stack reachable from the host
(e.g. the CI override's published ports, or a manually port-forwarded VM
stack) with pyspark installed on the host Python -- skipped automatically if
pyspark isn't importable, so it never breaks the regular `unit-tests` job if
ever collected there by mistake.

Uses importlib.util.find_spec (not pytest.importorskip) for that presence
check specifically so it stays a lazy, collection-safe skipif condition,
matching every other conditionally-runnable integration test in this repo
(see e.g. tests/integration/dbt/test_incremental_models_real_build.py's
`shutil.which("dbt") is None` check) -- importorskip actually performs the
import inline at module-import time, which fires before pytest's `-m`
marker filtering ever gets a chance to deselect the test, so it would show
up as SKIPPED instead of cleanly deselected under `-m "not integration"`.
"""
import importlib.util
import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        importlib.util.find_spec("pyspark") is None,
        reason="pyspark only installed in the lakehouse CI job",
    ),
]


def _base_uri():
    return os.environ.get("LAKEKEEPER_BASE_URI", "http://localhost:18181")


@pytest.fixture(autouse=True, scope="module")
def _lakehouse_env():
    # Mirrors docker-compose.lakehouse.yml's lakehouse-worker env: the neutral
    # var is what the Spark/consumer path reads (Plan 125 Gate 0.5), the legacy
    # one is what the Lakekeeper warehouse-registration step below uses.
    os.environ.setdefault("ICEBERG_CATALOG_URI", f"{_base_uri()}/catalog")
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
