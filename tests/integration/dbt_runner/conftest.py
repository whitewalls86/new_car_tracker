"""
dbt_runner integration test fixtures.

Validation tests use TestClient — they test request parsing and error responses
before any dbt subprocess is invoked.
"""
import os
import tempfile

# Set env vars before importing anything that reads them at module level.
os.environ.setdefault("DUCKDB_PATH", os.path.join(tempfile.gettempdir(), "dbt_runner_test.duckdb"))
os.environ.setdefault("LOG_PATH", os.path.join(tempfile.gettempdir(), "dbt_runner_test.log"))

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from dbt_runner.app import app  # noqa: E402


@pytest.fixture(scope="session")
def api_client():
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client
