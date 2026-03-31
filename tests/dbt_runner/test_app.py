from dbt_runner import app
from datetime import datetime as dt


def test_db_execute_connect_error(mock_db_connection_error, mock_logger_error):
    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.NONE, error_context='PYTEST')

    assert result is None
    assert "PYTEST: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_db_execute_database_error(mock_db_database_error, mock_logger_error):
    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.NONE, error_context='PYTEST')

    assert result is None
    assert "PYTEST: encountered DB error." in mock_logger_error.call_args[0][0]


def test_db_execute_sql_error(mock_db_sql_error, mock_logger_error):
    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.NONE, error_context='PYTEST')

    assert result is None
    assert "PYTEST: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_db_fetch_one_with_results(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (1, 2, 3)

    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.ONE, error_context='PYTEST')

    assert result == (1, 2, 3)
    cursor.fetchall.assert_not_called()


def test_db_fetch_one_no_results(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = None

    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.ONE, error_context='PYTEST')

    assert result == ()


def test_db_fetch_none(mock_cursor_context):
    conn, cursor = mock_cursor_context

    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.NONE, error_context='PYTEST')

    assert result is True
    cursor.fetchone.assert_not_called()
    cursor.fetchall.assert_not_called()


def test_db_fetch_all_with_results(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchall.return_value = [(1, 2, 3), (4, 5, 6)]

    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.ALL, error_context='PYTEST')

    assert result == [(1, 2, 3), (4, 5, 6)]
    cursor.fetchone.assert_not_called()


def test_db_fetch_all_no_results(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchall.return_value = None

    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.ALL, error_context='PYTEST')

    assert result == []
    cursor.fetchone.assert_not_called()


def test_db_fetch_rowcount_with_results(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.rowcount = 1

    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.ROWCOUNT, error_context='PYTEST')

    assert result == 1
    cursor.fetchone.assert_not_called()
    cursor.fetchall.assert_not_called()


def test_db_fetch_rowcount_no_results(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.rowcount = None

    result = app._db_execute(sql='SELECT 1', fetch=app.FetchMode.ROWCOUNT, error_context='PYTEST')

    assert result == 0
    cursor.fetchone.assert_not_called()
    cursor.fetchall.assert_not_called()


def test_acquire_lock_connection_error(mock_db_connection_error, mock_logger_error):
    result = app._acquire_lock('test')

    assert result is False
    assert "Acquire-Lock: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_acquire_lock_execution_error(mock_db_sql_error, mock_logger_error):
    result = app._acquire_lock('test')
    assert result is False
    assert "Acquire-Lock: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_acquire_lock_db_error(mock_db_database_error, mock_logger_error):
    result = app._acquire_lock('test')
    assert result is False
    assert "Acquire-Lock: encountered DB error." in mock_logger_error.call_args[0][0]


def test_acquire_lock_known_values(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (True,)  # Configure mock to return a row

    after_detail = app._acquire_lock('after_detail')
    after_srp = app._acquire_lock('after_srp')
    manual = app._acquire_lock('manual')

    assert after_detail is True
    assert after_srp is True
    assert manual is True


def test_acquire_lock_unknown_value(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (True,)  # Configure mock to return a row

    null_value = app._acquire_lock(None)
    empty_string = app._acquire_lock('')
    num_values = app._acquire_lock(123)

    assert null_value is True
    assert empty_string is True
    assert num_values is True


def test_release_lock_connection_error(mock_db_connection_error, mock_logger_error):
    result = app._release_lock()
    assert result is None
    assert "Release-Lock: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_release_lock_execution_error(mock_db_sql_error, mock_logger_error):
    result = app._release_lock()
    assert result is None
    assert "Release-Lock: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_release_lock_db_error(mock_db_database_error, mock_logger_error):
    result = app._release_lock()
    assert result is None
    assert "Release-Lock: encountered DB error." in mock_logger_error.call_args[0][0]


def test_release_lock_success(mock_cursor_context):
    conn, cursor = mock_cursor_context
    app._release_lock()
    cursor.execute.assert_called_once()


def test_lock_status_connection_error(mock_db_connection_error, mock_logger_error):
    result = app._lock_status()
    assert result['locked_by'] == "DB Error"
    assert "Lock-Status: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_lock_status_execution_error(mock_db_sql_error, mock_logger_error):
    result = app._lock_status()
    assert result['locked_by'] == "DB Error"
    assert "Lock-Status: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_lock_status_database_error(mock_db_database_error, mock_logger_error):
    result = app._lock_status()
    assert result['locked_by'] == "DB Error"
    assert "Lock-Status: encountered DB error." in mock_logger_error.call_args[0][0]


def test_lock_status_is_locked(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (True, dt.fromisoformat("2026-03-30T12:00:03.237000+00:00"), "Some Process")
    result = app._lock_status()

    assert result['locked_by'] == "Some Process"
    assert result['locked'] is True
    assert result['locked_at'] == "2026-03-30T12:00:03.237000+00:00"


def test_lock_status_is_locked_no_time(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (True, None, "Some Process")
    result = app._lock_status()

    assert result['locked_by'] == "Some Process"
    assert result['locked'] is True
    assert result['locked_at'] is None


def test_lock_status_is_unlocked(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (False, None, None)
    result = app._lock_status()

    assert result['locked_by'] is None
    assert result['locked'] is False
    assert result['locked_at'] is None


def test_lock_status_no_rows(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = None
    result = app._lock_status()

    assert result['locked_by'] is None
    assert result['locked'] is False
    assert result['locked_at'] is None


# def test_record_run_connection_error(mock_db_connection_error, mock_logger_error):


# def test_record_run_sql_error(mock_db_sql_error, mock_logger_error):


# def test_record_run_database_error(mock_db_database_error, mock_logger_error):


# def test_record_run_normal_std_out(mock_cursor_context):


# def test_record_run_empty_std_out(mock_cursor_context):


# def test_record_run_wrong_data_types(mock_cursor_context):

