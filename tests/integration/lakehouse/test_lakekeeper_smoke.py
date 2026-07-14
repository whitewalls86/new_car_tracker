"""
Plan 112 Gate A1: Lakekeeper REST config smoke test.

Runs in the dedicated `lakehouse` GitHub Actions job (see
docker-compose.lakehouse.yml, docker-compose.lakehouse.ci.yml, and
docs/runbook_lakehouse.md), against a job-local, throwaway Lakekeeper +
MinIO + Postgres stack. Proves the catalog itself, independent of any
client engine -- no Spark, no PyIceberg (Gate A2/A2b scope).

Known limitation (documented in docs/runbook_lakehouse.md): this exercises
only the REST /v1/config endpoint, which is the one part of the Iceberg REST
Catalog spec guaranteed stable across Lakekeeper versions without first
registering a warehouse. Namespace CRUD is NOT attempted here -- it needs a
registered warehouse (a Lakekeeper-specific management-API step deferred to
A2, when a warehouse is actually needed for table writes).
"""
import os

import pytest
import requests

pytestmark = pytest.mark.integration


def _catalog_uri():
    return os.environ.get("LAKEKEEPER_CATALOG_URI", "http://localhost:18181/catalog")


class TestLakekeeperRestConfig:
    def test_config_endpoint_responds(self):
        resp = requests.get(f"{_catalog_uri()}/v1/config", timeout=10)
        assert resp.status_code == 200

    def test_config_endpoint_returns_json(self):
        resp = requests.get(f"{_catalog_uri()}/v1/config", timeout=10)
        body = resp.json()
        # Per the Iceberg REST Catalog spec, /v1/config responds with
        # "defaults"/"overrides" config maps (possibly empty).
        assert isinstance(body, dict)
