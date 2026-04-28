"""
SQL integration test fixtures.

Provides both Postgres (viewer_cur) and DuckDB (duckdb_con) connections.
The DuckDB connection reads the analytics.duckdb file produced by
`dbt build --target duckdb` earlier in the same CI run.
"""
import os

import duckdb
import pytest

DUCKDB_PATH = os.environ.get("DUCKDB_PATH")


@pytest.fixture(scope="session")
def duckdb_con():
    if not DUCKDB_PATH:
        pytest.skip("DUCKDB_PATH not set — skipping DuckDB smoke tests")
    return duckdb.connect(DUCKDB_PATH, read_only=True)
