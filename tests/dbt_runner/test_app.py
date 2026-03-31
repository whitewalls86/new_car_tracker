from dbt_runner import app


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


def test_acquire_lock_known_values(mock_db_conn):
    connect, conn, cursor = mock_db_conn
    cursor.fetchone.return_value = (True,)  # Configure mock to return a row

    after_detail = app._acquire_lock('after_detail')
    after_srp = app._acquire_lock('after_srp')
    manual = app._acquire_lock('manual')

    assert after_detail is True
    assert after_srp is True
    assert manual is True


def test_acquire_lock_unknown_value(mock_db_conn):
    connect, conn, cursor = mock_db_conn
    cursor.fetchone.return_value = (True,)  # Configure mock to return a row

    null_value = app._acquire_lock(None)
    empty_string = app._acquire_lock('')
    num_values = app._acquire_lock(123)

    assert null_value is True
    assert empty_string is True
    assert num_values is True

