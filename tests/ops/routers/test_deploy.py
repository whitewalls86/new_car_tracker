from datetime import datetime as dt

from ops.routers import deploy


def test_intent_status_connection_error(
    mock_db_connection_error, mock_logger_error
):
    result = deploy._intent_status()
    expected = {"intent": "none", "requested_at": None, "requested_by": None}
    assert result == expected
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Status: Unable to connect to Postgres database." in error_msg


def test_intent_status_db_error(mock_db_database_error, mock_logger_error):
    result = deploy._intent_status()
    expected = {"intent": "none", "requested_at": None, "requested_by": None}
    assert result == expected
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Status: encountered DB error." in error_msg


def test_intent_status_execution_error(mock_db_sql_error, mock_logger_error):
    result = deploy._intent_status()
    assert result == {"intent": "none", "requested_at": None, "requested_by": None}
    assert "Intent-Status: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_intent_status_good_read(mock_cursor_context, mock_logger_error):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "pending",  # intent
        dt.fromisoformat("2025-01-01T12:00:00"),  # requested_at
        "deploy_bot",  # requested_by
        3,  # number_running
        dt.fromisoformat("2025-01-01T12:00:00")  # min_started_at
    )

    result = deploy._intent_status()

    assert result == {"intent": "pending", "requested_at": "2025-01-01T12:00:00",
                      "requested_by": "deploy_bot", "number_running": 3,
                      "min_started_at": "2025-01-01T12:00:00" }
    

def test_intent_status_bad_read(mock_cursor_context, mock_logger_error):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "pending",  # intent
        "2025-01-01T12:00:00",  # requested_at
        "deploy_bot",  # requested_by
        3,  # number_running
        "2025-01-01T12:00:00"  # min_started_at
    )

    result = deploy._intent_status()

    assert result == {"intent": "none", "requested_at": None, "requested_by": None}


def test_intent_status_no_row(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = None
    
    result = deploy._intent_status()
    
    assert result == {"intent": "none", "requested_at": None, "requested_by": None}


def test_intent_release_connection_error(
    mock_db_connection_error, mock_logger_error
):
    result = deploy._intent_release()
    assert result is False
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Release: Unable to connect to Postgres database." in error_msg


def test_intent_release_db_error(mock_db_database_error, mock_logger_error):
    result = deploy._intent_release()
    assert result is False
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Release: encountered DB error." in error_msg


def test_intent_release_execution_error(mock_db_sql_error, mock_logger_error):
    result = deploy._intent_release()
    assert result is False
    assert "Intent-Release: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_intent_release_success(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.return_value = ('none',)
    result = deploy._intent_release()

    assert result is True


def test_intent_release_no_return(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.return_value = None
    result = deploy._intent_release()

    assert result is True


def test_set_intent_connection_error(
    mock_db_connection_error, mock_logger_error
):
    result = deploy._set_intent('test')
    assert result == "error"
    error_msg = mock_logger_error.call_args[0][0]
    assert "Set-Intent: Unable to connect to Postgres database." in error_msg


def test_set_intent_db_error(mock_db_database_error, mock_logger_error):
    result = deploy._set_intent('test')
    assert result == "error"
    assert "Set-Intent: encountered DB error." in mock_logger_error.call_args[0][0]


def test_set_intent_execution_error(mock_db_sql_error, mock_logger_error):
    result = deploy._set_intent('test')
    assert result == "error"
    assert "Set-Intent: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_set_intent_success(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = ('pending',)
    result = deploy._set_intent('test')

    assert result == "ok"


def test_set_intent_no_return(mock_cursor_context, mock_router_logger_warning):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = None
    result = deploy._set_intent('test')

    assert result == "locked"
    assert "Intent failed to set — already locked." in mock_router_logger_warning.call_args[0][0]



def test_get_deploy_health(mock_client, mock_intent_status):
    response = mock_client.get("/deploy/status")
    assert response.status_code == 200
    mock_intent_status.assert_called_once()


def test_set_deploy_health(mock_client, mock_set_intent):
    response = mock_client.post("/deploy/start")
    assert response.status_code == 200
    mock_set_intent.assert_called_once()


def test_set_deploy_health_already_locked(mock_client, mock_set_intent):
    mock_set_intent.return_value = "locked"
    response = mock_client.post("/deploy/start")
    assert response.status_code == 409


def test_set_deploy_health_db_error(mock_client, mock_set_intent):
    mock_set_intent.return_value = "error"
    response = mock_client.post("/deploy/start")
    assert response.status_code == 503


def test_set_deploy_complete(mock_client, mock_intent_release):
    response = mock_client.post("/deploy/complete")
    assert response.status_code == 200
    mock_intent_release.assert_called_once()


def test_set_deploy_complete_db_error(mock_client, mock_intent_release):
    mock_intent_release.return_value = False
    response = mock_client.post("/deploy/complete")
    assert response.status_code == 503
