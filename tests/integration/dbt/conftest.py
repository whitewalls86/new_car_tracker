"""
Layer 2 dbt integration test fixtures.

Key differences from Layer 1:
- dbt_conn uses autocommit=True so seeded rows are immediately visible to the
  dbt subprocess (which runs in a separate process and cannot see open transactions).
- Teardown (TRUNCATE) is the responsibility of each test module, not this conftest,
  because different modules touch different source tables.
- run_dbt shells out `dbt build` and fails the test on non-zero exit.
- analytics_ci_cur reads from the analytics_ci schema where the ci target writes.
"""
import os
import subprocess
from urllib.parse import urlparse

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)
_DBT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../dbt"))


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
def dbt_conn():
    """
    Session-scoped autocommit connection.

    autocommit=True means every INSERT is committed immediately, making seeded
    rows visible to the dbt subprocess without any explicit COMMIT call.
    """
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture()
def dbt_cur(dbt_conn):
    """Function-scoped RealDictCursor on the autocommit connection (for seeding)."""
    with dbt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        yield cur


@pytest.fixture(scope="session")
def run_dbt():
    """
    Returns a callable that shells out `dbt build --select <selector>`.

    Usage inside a test or module-scoped fixture:
        run_dbt("stg_srp_observations stg_raw_artifacts int_listing_to_vin")

    Fails the test immediately if dbt exits non-zero.
    """
    def _run(select: str):
        result = subprocess.run(
            [
                "dbt", "build",
                "--select", select,
                "--target", "ci",
                "--profiles-dir", ".",
                "--full-refresh",
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
def analytics_ci_cur(dbt_conn):
    """
    Function-scoped RealDictCursor pre-set to the analytics_ci schema.

    Use this to read dbt model output after run_dbt completes.
    """
    with dbt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SET search_path TO analytics_ci")
        yield cur
