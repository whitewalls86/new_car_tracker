from datetime import datetime as dt

from ops.routers import deploy

#: The shape returned when there is no row or the read failed. pause_long_jobs
#: carries the column default; it only means anything while intent is
#: 'pending', so reporting it here is shape, not a claim about a deploy.
_NO_INTENT = {
    "intent": "none",
    "requested_at": None,
    "requested_by": None,
    "pause_long_jobs": True,
}


def test_intent_status_connection_error(
    mock_db_connection_error, mock_logger_error
):
    result = deploy._intent_status()
    expected = _NO_INTENT
    assert result == expected
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Status: Unable to connect to Postgres database." in error_msg


def test_intent_status_db_error(mock_db_database_error, mock_logger_error):
    result = deploy._intent_status()
    expected = _NO_INTENT
    assert result == expected
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Status: encountered DB error." in error_msg


def test_intent_status_execution_error(mock_db_sql_error, mock_logger_error):
    result = deploy._intent_status()
    assert result == _NO_INTENT
    assert "Intent-Status: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_intent_status_good_read(mock_cursor_context, mock_logger_error):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "pending",  # intent
        dt.fromisoformat("2025-01-01T12:00:00"),  # requested_at
        "deploy_bot",  # requested_by
        3,  # number_running
        dt.fromisoformat("2025-01-01T12:00:00"),  # min_started_at
        True,  # pause_long_jobs
    )

    result = deploy._intent_status()

    assert result == {"intent": "pending", "requested_at": "2025-01-01T12:00:00",
                      "requested_by": "deploy_bot", "number_running": 3,
                      "min_started_at": "2025-01-01T12:00:00",
                      "pause_long_jobs": True}
    

def test_intent_status_bad_read(mock_cursor_context, mock_logger_error):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "pending",  # intent
        "2025-01-01T12:00:00",  # requested_at
        "deploy_bot",  # requested_by
        3,  # number_running
        "2025-01-01T12:00:00",  # min_started_at
        True,  # pause_long_jobs
    )

    result = deploy._intent_status()

    assert result == _NO_INTENT


def test_intent_status_no_row(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = None
    
    result = deploy._intent_status()
    
    assert result == _NO_INTENT


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


# ---------------------------------------------------------------------------
# pause_long_jobs (Plan 131 Stage 5 D3b)
#
# Asks the pack and prune jobs to stop at their next safe boundary. The default
# is the load-bearing part: the safe behaviour should be the one you get by
# forgetting, because the caller who forgets is a deploy script, not a person
# weighing it up.
# ---------------------------------------------------------------------------

def test_deploy_start_pauses_long_jobs_by_default(mock_client, mock_set_intent):
    response = mock_client.post("/deploy/start")

    assert response.status_code == 200
    assert mock_set_intent.call_args[0] == ("Deploy Declared", True)


def test_deploy_start_accepts_pause_long_jobs_false(mock_client, mock_set_intent):
    response = mock_client.post("/deploy/start", json={"pause_long_jobs": False})

    assert response.status_code == 200
    assert mock_set_intent.call_args[0] == ("Deploy Declared", False)


def test_deploy_start_with_an_empty_body_still_pauses(mock_client, mock_set_intent):
    # The existing callers post no body at all. They must keep getting the
    # safe default rather than a 422.
    response = mock_client.post("/deploy/start", json={})

    assert response.status_code == 200
    assert mock_set_intent.call_args[0] == ("Deploy Declared", True)


def test_set_intent_writes_the_pause_flag(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = ("pending",)

    deploy._set_intent("test", False)

    sql, params = cursor.execute.call_args[0]
    assert "pause_long_jobs = %s" in sql
    assert params == ("test", False, deploy.STALE_LOCK_MINUTES)


def test_set_intent_defaults_the_pause_flag_to_true(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = ("pending",)

    deploy._set_intent("test")

    assert cursor.execute.call_args[0][1] == ("test", True, deploy.STALE_LOCK_MINUTES)


def test_intent_status_reports_the_pause_flag(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "pending", None, "deploy_bot", 0, None, False,
    )

    assert deploy._intent_status()["pause_long_jobs"] is False


def test_set_deploy_complete(mock_client, mock_intent_release):
    response = mock_client.post("/deploy/complete")
    assert response.status_code == 200
    mock_intent_release.assert_called_once()


def test_set_deploy_complete_db_error(mock_client, mock_intent_release):
    mock_intent_release.return_value = False
    response = mock_client.post("/deploy/complete")
    assert response.status_code == 503
