"""
SQL integration test fixtures.

Provides both Postgres (viewer_cur) and DuckDB (duckdb_con) connections.
The DuckDB connection reads the analytics.duckdb file produced by
`dbt build --target duckdb` earlier in the same CI run.
"""
import os

import pytest

DUCKDB_PATH = os.environ.get("DUCKDB_PATH")


@pytest.fixture(scope="session")
def duckdb_con():
    if not DUCKDB_PATH:
        pytest.skip("DUCKDB_PATH not set — skipping DuckDB smoke tests")
    import duckdb
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@pytest.fixture()
def airflow_metadata(cur):
    """
    Binds a test to Airflow's real `airflow.task_instance` / `airflow.dag_run`.

    Airflow owns these tables through its own Alembic migrations, not Flyway.
    They live in the `airflow` schema of this same database in production
    (`airflow.task_instance` held 438,355 rows on 2026-08-25), and CI now
    creates them the way production does -- `airflow db migrate` run as
    `airflow_user`, whose `search_path = airflow`, after Flyway.

    This replaces the `airflow_metadata_standin` stopgap, which created
    minimal tables carrying only the columns the drain queries read. The
    stand-in caught schema *qualification* bugs and nothing else: a column
    renamed by an Airflow upgrade passed against our own definition and broke
    in production. Binding to the real tables is the point -- if an upgrade
    moves a column these queries read, CI is where that surfaces.

    Absence is a skip locally and a failure in CI (`REQUIRE_AIRFLOW_SCHEMA`).
    A skip in CI would silently restore the blind spot this fixture exists to
    close, which is how the drain queries reached production unexecuted.
    """
    cur.execute(
        """SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'airflow'
              AND table_name IN ('task_instance', 'dag_run', 'alembic_version')"""
    )
    present = {row["table_name"] for row in cur.fetchall()}
    missing = {"task_instance", "dag_run", "alembic_version"} - present
    if missing:
        reason = (
            f"the airflow schema is missing {sorted(missing)} -- run "
            "`airflow db migrate` against this database as airflow_user"
        )
        if os.environ.get("REQUIRE_AIRFLOW_SCHEMA"):
            pytest.fail(reason)
        pytest.skip(reason)
    return cur
