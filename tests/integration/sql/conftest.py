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
def airflow_metadata_standin(cur):
    """
    Minimal `airflow.task_instance` / `airflow.dag_run`, created per-test.

    Airflow owns these tables through its own migrations, not Flyway. In
    production they live in the `airflow` schema of this same database
    (`airflow.task_instance` held 438,355 rows on 2026-08-25); in CI, Flyway
    creates the empty `airflow` schema and Airflow's metadata DB is SQLite, so
    the tables never exist. That gap is why `ops/coordination_drain.py` shipped
    querying `task_instance` unqualified: no test could execute it.

    These stand-ins carry only the columns the drain queries read, so they
    verify SQL shape and schema qualification -- an unqualified `task_instance`
    still fails here, because `airflow` is not on the ops role's search_path.
    They deliberately do NOT verify Airflow's real schema: a column renamed by
    an Airflow upgrade would pass here and break in production. Replacing this
    with a real `airflow db migrate` against the CI Postgres is tracked
    separately.

    The `cur` fixture's transaction is rolled back, so nothing is left behind.
    """
    cur.execute("CREATE SCHEMA IF NOT EXISTS airflow")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS airflow.task_instance (
               dag_id text, task_id text, state text, start_date timestamptz)"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS airflow.dag_run (
               dag_id text, run_id text, state text, start_date timestamptz)"""
    )
    return cur
