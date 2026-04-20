"""
Unit tests for shared.db module (db_cursor context manager and DB_KWARGS resolution).
"""
import importlib

import pytest

from shared.db import db_cursor

# ---------------------------------------------------------------------------
# DB_KWARGS resolution — DATABASE_URL vs PG* env vars
# ---------------------------------------------------------------------------

def _reload_db_kwargs(monkeypatch, env: dict) -> dict:
    """
    Set env vars via monkeypatch, reload shared.db, and return its DB_KWARGS.
    monkeypatch restores the original env after the test.
    """
    # Clear both DATABASE_URL and PG* so defaults don't bleed in
    for key in ("DATABASE_URL", "PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "POSTGRES_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    for key, val in env.items():
        monkeypatch.setenv(key, val)

    import shared.db as db_mod
    importlib.reload(db_mod)
    return dict(db_mod.DB_KWARGS)


class TestDbKwargsResolution:
    def test_database_url_parsed_correctly(self, monkeypatch):
        kwargs = _reload_db_kwargs(monkeypatch, {
            "DATABASE_URL": "postgresql://alice:secret@myhost:5433/mydb"
        })
        assert kwargs["host"] == "myhost"
        assert kwargs["port"] == 5433
        assert kwargs["dbname"] == "mydb"
        assert kwargs["user"] == "alice"
        assert kwargs["password"] == "secret"

    def test_database_url_takes_precedence_over_pg_vars(self, monkeypatch):
        kwargs = _reload_db_kwargs(monkeypatch, {
            "DATABASE_URL": "postgresql://url_user:url_pass@url_host:5432/url_db",
            "PGHOST": "pg_host",
            "PGUSER": "pg_user",
        })
        # DATABASE_URL wins — PG* vars must be ignored
        assert kwargs["host"] == "url_host"
        assert kwargs["user"] == "url_user"

    def test_pg_vars_used_when_no_database_url(self, monkeypatch):
        kwargs = _reload_db_kwargs(monkeypatch, {
            "PGHOST": "pg_host",
            "PGPORT": "5435",
            "PGDATABASE": "pg_db",
            "PGUSER": "pg_user",
            "POSTGRES_PASSWORD": "pg_pass",
        })
        assert kwargs["host"] == "pg_host"
        assert kwargs["port"] == 5435
        assert kwargs["dbname"] == "pg_db"
        assert kwargs["user"] == "pg_user"
        assert kwargs["password"] == "pg_pass"

    def test_pg_defaults_when_no_env_set(self, monkeypatch):
        kwargs = _reload_db_kwargs(monkeypatch, {})
        assert kwargs["host"] == "postgres"
        assert kwargs["port"] == 5432
        assert kwargs["dbname"] == "cartracker"
        assert kwargs["user"] == "cartracker"
        assert kwargs["password"] == ""

    def test_database_url_defaults_for_missing_components(self, monkeypatch):
        # Minimal URL — host only, no port/user/db
        kwargs = _reload_db_kwargs(monkeypatch, {
            "DATABASE_URL": "postgresql://myhost/mydb"
        })
        assert kwargs["host"] == "myhost"
        assert kwargs["dbname"] == "mydb"
        # Port and user fall back to defaults from the URL parser
        assert kwargs["port"] == 5432 or kwargs["port"] is None  # psycopg2 default


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
