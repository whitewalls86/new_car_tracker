"""
Plan 112 Gate A1: Lakekeeper management info smoke test.

Runs in the dedicated `lakehouse` GitHub Actions job (see
docker-compose.lakehouse.yml, docker-compose.lakehouse.ci.yml, and
docs/plan_112_refresh_policy_backtesting.md), against a job-local, throwaway Lakekeeper +
MinIO + Postgres stack. Proves the catalog itself, independent of any
client engine -- no Spark, no PyIceberg (Gate A2/A2b scope).

Known limitation (documented in docs/plan_112_refresh_policy_backtesting.md): this exercises
only the warehouse-free management /v1/info endpoint. Iceberg REST /v1/config
and namespace CRUD are NOT attempted here -- they need a registered warehouse,
a Lakekeeper-specific management-API step deferred to A2 when a warehouse is
actually needed for table writes.
"""
import os

import pytest
import requests

pytestmark = pytest.mark.integration


def _base_uri():
    return os.environ.get("LAKEKEEPER_BASE_URI", "http://localhost:18181")


class TestLakekeeperManagementInfo:
    def test_info_endpoint_responds(self):
        resp = requests.get(f"{_base_uri()}/management/v1/info", timeout=10)
        assert resp.status_code == 200

    def test_info_endpoint_returns_json(self):
        resp = requests.get(f"{_base_uri()}/management/v1/info", timeout=10)
        body = resp.json()
        assert isinstance(body, dict)
