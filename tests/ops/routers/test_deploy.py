import json
from datetime import datetime as dt

import pytest

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


def test_intent_status_connection_error(mock_db_connection_error, mock_logger_error):
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

    assert result == {
        "intent": "pending",
        "requested_at": "2025-01-01T12:00:00",
        "requested_by": "deploy_bot",
        "number_running": 3,
        "min_started_at": "2025-01-01T12:00:00",
        "pause_long_jobs": True,
    }


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


def test_intent_release_connection_error(mock_db_connection_error, mock_logger_error):
    result = deploy._intent_release()
    assert result == ("unavailable", None)
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Release: Unable to connect to Postgres database." in error_msg


def test_intent_release_db_error(mock_db_database_error, mock_logger_error):
    result = deploy._intent_release()
    assert result == ("error", "Other Error")
    error_msg = mock_logger_error.call_args[0][0]
    assert "Intent-Release: encountered DB error." in error_msg


def test_intent_release_execution_error(mock_db_sql_error, mock_logger_error):
    result = deploy._intent_release()
    assert result == ("error", "Bad SQL")
    assert "Intent-Release: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_intent_release_success(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [
        ("deploy", "requested", 7, "test"),
        ("none",),
        (8,),
    ]
    result = deploy._intent_release()

    assert result == ("ok", None)
    assert "generation, requested_by" in cursor.execute.call_args_list[1].args[0]


def test_legacy_release_can_finish_facade_deploy_after_validation(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [
        ("deploy", "validating", 7, "test"),
        ("none",),
        (8,),
    ]

    assert deploy._intent_release() == ("ok", None)
    coordination_update = cursor.execute.call_args_list[-2].args[0]
    assert "generation = generation + 1" in coordination_update
    assert cursor.execute.call_args_list[-1].args[1] == (
        8,
        "validating",
        "none",
        "deploy",
        "test",
    )


def test_legacy_release_cannot_clear_other_coordination_kind(mock_cursor_context):
    """Refusing here is the facade working, so it is 'locked' and not 'error'.

    This is the outcome the bare `bool` mislabelled worst: a deliberate policy
    refusal rendered as 503 "Database unavailable", which is both the wrong
    component and the wrong kind of answer.
    """
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "service_maintenance",
        "validating",
        7,
        "test",
    )

    status, detail = deploy._intent_release()

    assert status == "locked"
    assert "service_maintenance" in detail and "validating" in detail
    assert not any("UPDATE deploy_intent" in call.args[0] for call in cursor.execute.call_args_list)


def test_intent_release_no_return(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [(None, "none", 0, None), None]
    result = deploy._intent_release()

    assert result == ("error", "deploy_intent has no row id=1")


# ---------------------------------------------------------------------------
# Which failure it was (Plan 162 Stage K)
#
# Every one of these returned the bare string "error" and rendered as 503
# "Database unavailable". Only the first of the three is about an unavailable
# database; the other two are a healthy Postgres refusing a statement, and
# saying otherwise sent a production diagnosis after the one component that was
# working. The exception types are psycopg2's own, raised through the real
# `db_cursor`, so this asserts the classification rather than a restatement of
# it.
# ---------------------------------------------------------------------------


def test_set_intent_connection_error(mock_db_connection_error, mock_logger_error):
    result = deploy._set_intent("test")
    assert result == ("unavailable", None)
    error_msg = mock_logger_error.call_args[0][0]
    assert "Set-Intent: Unable to connect to Postgres database." in error_msg


def test_set_intent_db_error(mock_db_database_error, mock_logger_error):
    result = deploy._set_intent("test")
    assert result == ("error", "Other Error")
    assert "Set-Intent: encountered DB error." in mock_logger_error.call_args[0][0]


def test_set_intent_execution_error(mock_db_sql_error, mock_logger_error):
    result = deploy._set_intent("test")
    assert result == ("error", "Bad SQL")
    assert "Set-Intent: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_a_failed_write_is_logged_with_the_exception_that_caused_it(
    mock_db_sql_error, mock_logger_error
):
    """The log is the other half. `logger.error(msg)` named the operation and
    dropped the exception, so the constraint that rejected the row appeared
    nowhere -- not in the response, not in the log, not on the operator's
    screen. Stage 7's placeholder defect wore the same face one day earlier.
    """
    deploy._set_intent("test")

    assert mock_logger_error.call_args.kwargs.get("exc_info") is True


def test_set_intent_success(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [(None, "none"), ("pending",), (7,)]
    result = deploy._set_intent("test")

    assert result == ("ok", None)
    coordination_update = cursor.execute.call_args_list[-2]
    sql, params = coordination_update.args
    assert "generation = generation + 1" in sql
    assert json.loads(params[0]) == list(deploy.LEGACY_DEPLOY_TARGETS)
    assert json.loads(params[1]) == list(deploy.LEGACY_DEPLOY_SCOPE)
    assert cursor.execute.call_args_list[-1].args[1] == (
        7,
        "none",
        "requested",
        "deploy",
        "test",
    )


def test_set_intent_event_failure_rolls_back_both_records(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [(None, "none"), ("pending",), (7,)]

    def fail_event(sql, params=None):
        if "INSERT INTO staging.coordination_state_events" in sql:
            raise RuntimeError("history unavailable")

    cursor.execute.side_effect = fail_event

    assert deploy._set_intent("test") == ("error", "history unavailable")
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_set_intent_expands_explicit_service_targets(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [(None, "none"), ("pending",), (7,)]

    assert deploy._set_intent("test", targets={"statsd-exporter"}) == ("ok", None)

    _, params = cursor.execute.call_args_list[-2].args
    assert json.loads(params[0]) == [
        "airflow-apiserver",
        "airflow-dag-processor",
        "airflow-scheduler",
        "airflow-triggerer",
        "statsd-exporter",
    ]
    assert json.loads(params[1]) == ["airflow_control", "observability"]


def test_set_intent_rejects_unknown_explicit_target_before_database(mock_cursor_context):
    _, cursor = mock_cursor_context

    assert deploy._set_intent("test", targets={"future-service"}) == ("invalid", None)
    cursor.execute.assert_not_called()


def test_set_intent_no_return(mock_cursor_context, mock_router_logger_warning):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [(None, "none"), None]
    result = deploy._set_intent("test")

    assert result == ("locked", None)
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
    mock_set_intent.return_value = deploy.IntentResult("locked")
    response = mock_client.post("/deploy/start")
    assert response.status_code == 409


def test_set_deploy_health_unreachable_database(mock_client, mock_set_intent):
    mock_set_intent.return_value = deploy.IntentResult("unavailable")
    response = mock_client.post("/deploy/start")
    assert response.status_code == 503
    assert response.json()["detail"] == "Database unavailable."


def test_a_refused_write_names_the_cause_instead_of_blaming_the_database(
    mock_client, mock_set_intent
):
    """The response an operator actually reads.

    On 2026-09-01 `POST /deploy/start {"targets":["dashboard"]}` answered 503
    "Database unavailable" while Postgres was healthy throughout; the real
    cause was the CHECK constraint below, and it reached nobody. A refusal is
    a 500 -- the request was well formed and the server failed to record it --
    and it carries what the database said.
    """
    violation = (
        'new row for relation "coordination_state" violates check constraint '
        '"coordination_state_check"'
    )
    mock_set_intent.return_value = deploy.IntentResult("error", violation)

    response = mock_client.post("/deploy/start")

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert "coordination_state_check" in detail
    assert "Database unavailable" not in detail


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
    assert mock_set_intent.call_args[0] == ("Deploy Declared", True, None)


def test_deploy_start_accepts_pause_long_jobs_false(mock_client, mock_set_intent):
    response = mock_client.post("/deploy/start", json={"pause_long_jobs": False})

    assert response.status_code == 200
    assert mock_set_intent.call_args[0] == ("Deploy Declared", False, None)


def test_deploy_start_with_an_empty_body_still_pauses(mock_client, mock_set_intent):
    # The existing callers post no body at all. They must keep getting the
    # safe default rather than a 422.
    response = mock_client.post("/deploy/start", json={})

    assert response.status_code == 200
    assert mock_set_intent.call_args[0] == ("Deploy Declared", True, None)


def test_deploy_start_passes_explicit_targets(mock_client, mock_set_intent):
    response = mock_client.post("/deploy/start", json={"targets": ["processing"]})

    assert response.status_code == 200
    assert mock_set_intent.call_args[0] == (
        "Deploy Declared",
        True,
        {"processing"},
    )


@pytest.mark.parametrize("targets", [[], ["processing", "processing"], [3], "processing"])
def test_deploy_start_rejects_malformed_targets(mock_client, mock_set_intent, targets):
    response = mock_client.post("/deploy/start", json={"targets": targets})

    assert response.status_code == 422
    mock_set_intent.assert_not_called()


def test_set_intent_writes_the_pause_flag(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [(None, "none"), ("pending",), (7,)]

    deploy._set_intent("test", False)

    sql, params = next(
        call.args
        for call in cursor.execute.call_args_list
        if "pause_long_jobs = %s" in call.args[0]
    )
    assert "pause_long_jobs = %s" in sql
    assert params == ("test", False, deploy.STALE_LOCK_MINUTES)


def test_set_intent_defaults_the_pause_flag_to_true(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.side_effect = [(None, "none"), ("pending",), (7,)]

    deploy._set_intent("test")

    deploy_update = next(
        call for call in cursor.execute.call_args_list if "pause_long_jobs = %s" in call.args[0]
    )
    assert deploy_update.args[1] == (
        "test",
        True,
        deploy.STALE_LOCK_MINUTES,
    )


def test_set_intent_refuses_active_coordination(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = ("host_maintenance", "requested")

    assert deploy._set_intent("test") == ("locked", None)

    assert not any("UPDATE deploy_intent" in call.args[0] for call in cursor.execute.call_args_list)


def test_intent_status_reports_the_pause_flag(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = (
        "pending",
        None,
        "deploy_bot",
        0,
        None,
        False,
    )

    assert deploy._intent_status()["pause_long_jobs"] is False


def test_set_deploy_complete(mock_client, mock_intent_release):
    response = mock_client.post("/deploy/complete")
    assert response.status_code == 200
    mock_intent_release.assert_called_once()


def test_a_refused_release_is_a_409_naming_who_holds_the_record(
    mock_client, mock_intent_release
):
    """The other half of the same incident, on the way out.

    `redeploy.sh` calls `/deploy/complete` from its exit trap with `|| echo`,
    so this response is the operator's only account of a fleet left paused.
    A host window holding the record is not a database being unavailable.
    """
    mock_intent_release.return_value = deploy.IntentResult(
        "locked", "host_maintenance coordination holds the record in phase 'active'"
    )

    response = mock_client.post("/deploy/complete")

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "host_maintenance" in detail
    assert "Database unavailable" not in detail


def test_a_failed_release_names_the_cause_instead_of_blaming_the_database(
    mock_client, mock_intent_release
):
    mock_intent_release.return_value = deploy.IntentResult(
        "error", 'relation "deploy_intent" does not exist'
    )

    response = mock_client.post("/deploy/complete")

    assert response.status_code == 500
    assert "deploy_intent" in response.json()["detail"]


def test_set_deploy_complete_unreachable_database(mock_client, mock_intent_release):
    mock_intent_release.return_value = deploy.IntentResult("unavailable")
    response = mock_client.post("/deploy/complete")
    assert response.status_code == 503
