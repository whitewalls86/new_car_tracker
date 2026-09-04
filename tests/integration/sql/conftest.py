"""
SQL integration test fixtures.

Provides both Postgres (viewer_cur) and DuckDB (duckdb_con) connections.
The DuckDB connection reads the analytics.duckdb file produced by
`dbt build --target duckdb` earlier in the same CI run.
"""
import os
from pathlib import Path

import pytest

DUCKDB_PATH = os.environ.get("DUCKDB_PATH")


@pytest.fixture(scope="session")
def duckdb_con():
    """The DuckDB file a real ``dbt build --target duckdb`` produced.

    Absence is a skip locally and a failure in CI (``REQUIRE_DUCKDB``), the
    same arrangement ``airflow_metadata`` below uses and for the same reason.
    55 of this suite's tests take this fixture -- every dashboard query and
    both analytics snapshots -- so a ``DUCKDB_PATH`` that quietly went missing,
    or a dbt build that produced no file, would skip a quarter of Layer 2 and
    leave the step green. That is the blind spot this plan exists to close,
    reappearing in the instrument itself.
    """
    if not DUCKDB_PATH or not Path(DUCKDB_PATH).exists():
        reason = (
            f"no DuckDB file to read (DUCKDB_PATH={DUCKDB_PATH!r}) -- run "
            "`dbt build --target duckdb` and point DUCKDB_PATH at its output"
        )
        if os.environ.get("REQUIRE_DUCKDB"):
            pytest.fail(reason)
        pytest.skip(reason)
    import duckdb
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@pytest.fixture(scope="session")
def duckdb_s3_con():
    """A DuckDB connection reading MinIO directly, built the way production does.

    Distinct from ``duckdb_con`` above and not a variant of it: that one opens
    the *file* a dbt build produced, while this one is
    ``shared.duckdb_s3.get_duckdb_s3_connection()`` -- the same call
    ``ops/routers/maintenance.py`` makes, reading the ops_normalized Parquet
    with S3 credentials the analytics file's connection does not carry.

    Absence is a skip locally and a failure in CI (``REQUIRE_MINIO``), for the
    same reason as the two fixtures around it: a Layer 2 test that skips
    executes no SQL, and the statement it covers is the one deciding which
    listings get a 'cleared' event written for them.
    """
    if not os.environ.get("MINIO_ENDPOINT"):
        reason = (
            "MINIO_ENDPOINT is not set -- no MinIO for the ops_normalized "
            "Parquet statements to read"
        )
        if os.environ.get("REQUIRE_MINIO"):
            pytest.fail(reason)
        pytest.skip(reason)
    from shared.duckdb_s3 import get_duckdb_s3_connection
    con = get_duckdb_s3_connection()
    yield con
    con.close()


@pytest.fixture(scope="session")
def blocked_cooldown_parquet():
    """The glob ``ops/routers/maintenance.py`` passes as a bound parameter.

    Built here from the same ``shared.minio.BUCKET`` the router reads, rather
    than typed out: a retyped path is the paraphrase this suite exists to
    prevent, one level below the statement.
    """
    from shared.minio import BUCKET
    return f"s3://{BUCKET}/ops_normalized/blocked_cooldown_events/**/*.parquet"


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
