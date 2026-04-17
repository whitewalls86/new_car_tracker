"""
dbt_runner integration test fixtures.

Build tests call dbt subprocess directly (same pattern as Layer 2) — no HTTP involved.
Validation tests use TestClient since they fail before the subprocess is ever called.
"""
import os
import subprocess
import tempfile

# Set env vars before importing anything that reads them at module level.
os.environ.setdefault("PGHOST", "localhost")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGDATABASE", "cartracker")
os.environ.setdefault("PGUSER", "cartracker")
os.environ.setdefault("POSTGRES_PASSWORD", "cartracker")
os.environ.setdefault("LOG_PATH", os.path.join(tempfile.gettempdir(), "dbt_runner_test.log"))

from urllib.parse import urlparse  # noqa: E402

import psycopg2  # noqa: E402
import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

from dbt_runner.app import app  # noqa: E402

_DBT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../dbt")
)
_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)


def _parse_dsn(url: str) -> dict:
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 5432,
        "dbname": p.path.lstrip("/") or "cartracker",
        "user": p.username or "cartracker",
        "password": p.password or "cartracker",
    }


@pytest.fixture(scope="session")
def api_client():
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client


@pytest.fixture(scope="session")
def run_dbt_intent():
    """
    Returns a callable that runs dbt build for a given selector string.
    Same pattern as the Layer 2 run_dbt fixture.
    """
    def _run(select: str):
        result = subprocess.run(
            [
                "dbt", "build",
                "--select", select,
                "--target", "ci",
                "--profiles-dir", ".",
                "--fail-fast",
            ],
            cwd=_DBT_DIR,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(
                f"dbt build failed for selector '{select}':\n"
                f"{result.stdout}\n{result.stderr}"
            )
        return result

    return _run


@pytest.fixture()
def verify_cur():
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = True
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        yield cur
    conn.close()
