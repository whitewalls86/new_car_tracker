import os
import tempfile
from unittest.mock import MagicMock

import pytest
from psycopg2 import DatabaseError, OperationalError, ProgrammingError

# Service modules configure logging at import time. Keep unit-test collection
# portable outside the Linux containers while preserving an explicit LOG_PATH.
os.environ.setdefault("LOG_PATH", os.path.join(tempfile.gettempdir(), "cartracker_test.log"))


@pytest.fixture
def mock_db_conn(mocker):
    """Returns a mock psycopg2 connection"""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_connect = mocker.patch("psycopg2.connect")
    # When psycopg2.connect() is called, return mock_conn
    mock_connect.return_value = mock_conn
    # When mock_conn.cursor() is called, return mock_cursor
    mock_conn.cursor.return_value = mock_cursor
    return mock_connect, mock_conn, mock_cursor


@pytest.fixture
def mock_cursor_context(mocker):
    """Returns a mock psycopg2 connection with a configurable cursor."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cursor
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mocker.patch("psycopg2.connect", return_value=conn)

    return conn, cursor


@pytest.fixture
def mock_db_connection_error(mocker):
    """Mock psycopg2.connect to raise OperationalError (DB unreachable)"""
    return mocker.patch("psycopg2.connect", side_effect=OperationalError("Connection refused"))


@pytest.fixture
def mock_db_sql_error(mocker):
    """Mock psycopg2 cursor to raise ProgrammingError (bad SQL)"""
    mock_conn = MagicMock()
    cursor_enter = mock_conn.cursor.return_value.__enter__.return_value
    cursor_enter.execute.side_effect = ProgrammingError("Bad SQL")
    return mocker.patch("psycopg2.connect", return_value=mock_conn)


@pytest.fixture
def mock_db_database_error(mocker):
    """Mock psycopg2.connect to raise OperationalError (DB unreachable)"""
    return mocker.patch("psycopg2.connect", side_effect=DatabaseError("Other Error"))


@pytest.fixture
def mock_logger_error(mocker):
    """Mock shared.db logger.error for database operation error logging"""
    return mocker.patch("shared.db.logger.error")


@pytest.fixture
def mock_requests(mocker):
    """Replace the HTTP client `ops/routers/admin.py` calls dbt_runner through.

    Plan 162 Stage AA. This patched the *global* `requests.get`, `.post` and
    `.delete`, which is two problems. It silences an outbound call from any
    module a test happens to touch, not just the one under test. And it names
    no caller, so a rule reading the seam cannot tell which service the
    fabricated responses behind it belong to -- the fabrications were invisible
    to `test_no_mock_invents_a_service_response` for exactly that reason.

    `delete` is gone with the `/dbt/intents/{name}` call it existed for.
    """
    return {
        "get": mocker.patch("ops.routers.admin.http_requests.get"),
        "post": mocker.patch("ops.routers.admin.http_requests.post"),
    }
