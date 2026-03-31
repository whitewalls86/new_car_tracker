import pytest
from unittest.mock import MagicMock
from psycopg2 import OperationalError, ProgrammingError, DatabaseError


@pytest.fixture
def mock_db_conn(mocker):
    """Returns a mock psycopg2 connection"""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_connect = mocker.patch("psycopg2.connect")
    mock_connect.return_value = mock_conn  # When psycopg2.connect() is called, return mock_conn
    mock_conn.cursor.return_value = mock_cursor  # When mock_conn.cursor() is called, return mock_cursor
    return mock_connect, mock_conn, mock_cursor


@pytest.fixture
def mock_cursor_context(mocker):
    """ Returns a mock psycopg2 connection with a configurable cursor. """
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cursor
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture
def mock_db_connection_error(mocker):
    """Mock psycopg2.connect to raise OperationalError (DB unreachable)"""
    return mocker.patch("psycopg2.connect", side_effect=OperationalError("Connection refused"))


@pytest.fixture
def mock_db_sql_error(mocker):
    """Mock psycopg2 cursor to raise ProgrammingError (bad SQL)"""
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value.execute.side_effect = ProgrammingError("Bad SQL")
    return mocker.patch("psycopg2.connect", return_value=mock_conn)


@pytest.fixture
def mock_db_database_error(mocker):
    """Mock psycopg2.connect to raise OperationalError (DB unreachable)"""
    return mocker.patch("psycopg2.connect", side_effect=DatabaseError("Other Error"))


@pytest.fixture
def mock_requests(mocker):
    """Mock requests.get, requests.post for API tests"""
    post = mocker.patch("requests.post")
    get = mocker.patch("requests.get")
    return {
        'post': post,
        'get': get
    }
