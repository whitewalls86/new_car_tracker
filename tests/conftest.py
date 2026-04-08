from unittest.mock import MagicMock

import pytest
from psycopg2 import DatabaseError, OperationalError, ProgrammingError


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
    """ Returns a mock psycopg2 connection with a configurable cursor. """
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
    """Mock requests.get, requests.post, requests.delete for API tests"""
    post = mocker.patch("requests.post")
    get = mocker.patch("requests.get")
    delete = mocker.patch("requests.delete")
    return {
        'post': post,
        'get': get,
        'delete': delete,
    }
