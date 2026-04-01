"""
Unit tests for shared.db module (db_cursor context manager).
"""
import pytest
from shared.db import db_cursor


def test_db_cursor_connection_error(mocker, mock_logger_error):
    """Test that connection errors are logged and re-raised."""
    mocker.patch("shared.db.get_conn", side_effect=Exception("Connection failed"))

    with pytest.raises(Exception, match="Connection failed"):
        with db_cursor(error_context="PYTEST"):
            pass

    assert "PYTEST: encountered DB error." in mock_logger_error.call_args[0][0]


def test_db_cursor_execute_error(mocker, mock_logger_error):
    """Test that SQL execution errors are logged and re-raised."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.execute.side_effect = Exception("SQL error")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("shared.db.get_conn", return_value=mock_conn)

    with pytest.raises(Exception, match="SQL error"):
        with db_cursor(error_context="PYTEST") as cur:
            cur.execute("SELECT 1")

    assert "PYTEST: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_db_cursor_commit_on_success(mocker):
    """Test that cursor commits on successful execution."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("shared.db.get_conn", return_value=mock_conn)

    with db_cursor() as cur:
        cur.execute("SELECT 1")

    mock_conn.commit.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SELECT 1")


def test_db_cursor_rollback_on_error(mocker, mock_logger_error):
    """Test that cursor rolls back on execution error."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.execute.side_effect = Exception("SQL error")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("shared.db.get_conn", return_value=mock_conn)

    with pytest.raises(Exception):
        with db_cursor(error_context="PYTEST") as cur:
            cur.execute("SELECT 1")

    mock_conn.rollback.assert_called_once()
    assert "PYTEST: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_db_cursor_always_closes(mocker):
    """Test that connection is always closed, even on error."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.execute.side_effect = Exception("SQL error")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("shared.db.get_conn", return_value=mock_conn)

    try:
        with db_cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        pass

    mock_conn.close.assert_called_once()


def test_db_cursor_fetchone(mocker):
    """Test cursor can fetch a single row."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.fetchone.return_value = (1, "test", True)
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("shared.db.get_conn", return_value=mock_conn)

    with db_cursor() as cur:
        cur.execute("SELECT * FROM table WHERE id = %s", (1,))
        result = cur.fetchone()

    assert result == (1, "test", True)


def test_db_cursor_fetchall(mocker):
    """Test cursor can fetch all rows."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.fetchall.return_value = [(1, "a"), (2, "b"), (3, "c")]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("shared.db.get_conn", return_value=mock_conn)

    with db_cursor() as cur:
        cur.execute("SELECT * FROM table")
        results = cur.fetchall()

    assert len(results) == 3
    assert results[0] == (1, "a")


def test_db_cursor_rowcount(mocker):
    """Test cursor rowcount is accessible."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_cursor.rowcount = 5
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mocker.patch("shared.db.get_conn", return_value=mock_conn)

    with db_cursor() as cur:
        cur.execute("DELETE FROM table WHERE id > %s", (10,))
        rowcount = cur.rowcount

    assert rowcount == 5
