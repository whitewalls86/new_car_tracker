import pytest

from dbt_runner import app
from datetime import datetime as dt
from fastapi import HTTPException


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


def test_record_run_connection_error(mock_db_connection_error, mock_logger_error, record_run_defaults):
    result = app._record_run(**record_run_defaults)
    assert result is False
    assert "Record-Run: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_record_run_sql_error(mock_db_sql_error, mock_logger_error, record_run_defaults):
    result = app._record_run(**record_run_defaults)
    assert result is False
    assert "Record-Run: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_record_run_database_error(mock_db_database_error, mock_logger_error, record_run_defaults):
    result = app._record_run(**record_run_defaults)
    assert result is False
    assert "Record-Run: encountered DB error." in mock_logger_error.call_args[0][0]


def test_record_run_success(mock_cursor_context, record_run_defaults):
    _conn, _cursor = mock_cursor_context
    result = app._record_run(**record_run_defaults)

    assert result is True


def test_record_run_empty_std_out(mock_cursor_context, record_run_defaults):
    _conn, _cursor = mock_cursor_context
    result = app._record_run(**{**record_run_defaults, "stdout": ""})

    assert result is True


def test_record_run_wrong_returncode_type(mock_cursor_context, record_run_defaults):
    _conn, _cursor = mock_cursor_context
    result = app._record_run(**{**record_run_defaults, "returncode": 'ok'})

    assert result is True


def test_record_run_wrong_ok_type(mock_cursor_context, record_run_defaults):
    _conn, _cursor = mock_cursor_context
    result = app._record_run(**{**record_run_defaults, "ok": 'True'})

    assert result is True


def test_record_run_wrong_timestamp_types(mock_cursor_context, record_run_defaults):
    _conn, _cursor = mock_cursor_context
    with pytest.raises(TypeError):
        app._record_run(**{**record_run_defaults, "started_at": "now", "finished_at": "when"})


def test_load_intents_connection_error(mock_db_connection_error, mock_logger_error):
    result = app._load_intents()
    assert result == {
        "after_srp": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"],
        "after_detail": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"],
    }
    assert "Load-Intents: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_load_intents_sql_error(mock_db_sql_error, mock_logger_error):
    result = app._load_intents()
    assert result == {
        "after_srp": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"],
        "after_detail": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"],
    }
    assert "Load-Intents: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_load_intents_database_error(mock_db_database_error, mock_logger_error):
    result = app._load_intents()
    assert result == {
        "after_srp": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"],
        "after_detail": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"],
    }
    assert "Load-Intents: encountered DB error." in mock_logger_error.call_args[0][0]


def test_load_intents_success(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.fetchall.return_value = [
        ("after_detail", ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"]),
        ("after_srp", ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"])
    ]

    result = app._load_intents()

    assert result == {
        "after_srp": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"],
        "after_detail": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"],
    }


def test_load_intents_empty(mock_cursor_context, mock_logger_warning):
    conn, cursor = mock_cursor_context
    cursor.fetchall.return_value = []

    result = app._load_intents()

    assert result == {
        "after_srp": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"],
        "after_detail": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"],
    }
    assert "Could not load intents from DB, using fallback" in mock_logger_warning.call_args[0][0]
    assert cursor.fetchall.call_count == 1


def test_save_intent_connection_error(mock_db_connection_error, mock_logger_error):
    result = app._save_intent(intent_name="after_detail", select_args=[])
    assert result is False
    assert "Save-Intent: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_save_intent_sql_error(mock_db_sql_error, mock_logger_error):
    result = app._save_intent(intent_name="after_detail", select_args=[])
    assert result is False
    assert "Save-Intent: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_save_intent_database_error(mock_db_database_error, mock_logger_error):
    result = app._save_intent(intent_name="after_detail", select_args=[])
    assert result is False
    assert "Save-Intent: encountered DB error." in mock_logger_error.call_args[0][0]


def test_save_intent_success(mock_cursor_context):
    _conn, _cursor = mock_cursor_context
    result = app._save_intent(intent_name="after_detail", select_args=[])
    assert result is True


def test_save_intent_no_intent(mock_cursor_context):
    _conn, _cursor = mock_cursor_context
    result = app._save_intent(intent_name="", select_args=[])
    assert result is True


def test_delete_intent_connection_error(mock_db_connection_error, mock_logger_error):
    result = app._delete_intent(intent_name="after_detail")
    assert result is False
    assert "Delete-Intent: Unable to connect to Postgres database." in mock_logger_error.call_args[0][0]


def test_delete_intent_sql_error(mock_db_sql_error, mock_logger_error):
    result = app._delete_intent(intent_name="after_detail")
    assert result is False
    assert "Delete-Intent: SQL execution failed." in mock_logger_error.call_args[0][0]


def test_delete_intent_database_error(mock_db_database_error, mock_logger_error):
    result = app._delete_intent(intent_name="after_detail")
    assert result is False
    assert "Delete-Intent: encountered DB error." in mock_logger_error.call_args[0][0]


def test_delete_intent_success(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.rowcount = 1
    result = app._delete_intent(intent_name="after_detail")
    assert result is True


def test_delete_intent_no_rows_deleted(mock_cursor_context):
    conn, cursor = mock_cursor_context
    cursor.rowcount = 0
    result = app._delete_intent(intent_name="after_detail")
    assert result is False


# Invalid token patterns
INVALID_TOKENS = [
    "model one",          # space
    "model!",             # exclamation
    "model#tag",          # hash
    "model$var",          # dollar
    "model&other",        # ampersand
    "model()",            # parentheses
    "",                   # empty
    "modèl",              # non-ASCII
    "model@hostname#fail", # has #
]

# Valid token patterns
VALID_TOKENS = [
    "model",
    "stg_raw_artifacts+",
    "package.model",
    "schema:table",
    "s3://bucket/path",
    "tag-with-dashes",
]

@pytest.mark.parametrize("token", INVALID_TOKENS)
def test_validate_tokens_invalid(token):
    with pytest.raises(HTTPException) as exc_info:
        app._validate_tokens([token], "test_field")
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize("token", VALID_TOKENS)
def test_validate_tokens_valid(token):
    # Should not raise
    app._validate_tokens([token], "test_field")


def test_cap_short_string():
    result = app._cap("This is a short string.")
    assert result == "This is a short string."


def test_cap_short_limit():
    result = app._cap(s="This is a short string.", limit=5)
    assert result == "ring."


def test_cap_max_length():
    long_string = 'a' * 25000
    result = app._cap(s=long_string)
    assert len(result) == 20000
    assert result == 'a'*20000


def test_cap_none_value():
    result = app._cap(s=None)
    assert result == ""


def test_cap_empty_string():
    result = app._cap(s="")
    assert result == ""
    

def test_get_health(mock_client):
    response = mock_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_get_logs_file_not_found(mock_client, mock_log_file_not_found):
    response = mock_client.get("/logs")
    assert response.json() == {"lines": []}
    assert response.status_code == 200


def test_get_logs_permission_error(mock_client, mock_log_permission_error):
    with pytest.raises(PermissionError):
        response = mock_client.get("/logs")


def test_get_logs_default(mock_client, mock_log_file):
    response = mock_client.get("/logs")
    assert response.status_code == 200
    assert len(response.json()["lines"]) == 200


def test_get_logs_custom_lines(mock_client, mock_log_file):
    response = mock_client.get("/logs?lines=50")
    assert len(response.json()["lines"]) == 50
    assert response.status_code == 200


def test_get_lock_status(mock_client, mocker):
    mocker.patch("dbt_runner.app._lock_status", return_value={"locked": False, "locked_at": None, "locked_by": None})
    response = mock_client.get("/dbt/lock")
    assert response.status_code == 200


def test_get_intents_normal(mock_client, mocker):
    mocker.patch("dbt_runner.app._load_intents", return_value={"after_srp": ["model_a", "model_b"]})
    response = mock_client.get("/dbt/intents")
    assert response.status_code == 200
    assert response.json() == {"intents": {"after_srp": {"select": ["model_a", "model_b"]}}}


def test_get_intents_fallback(mock_client, mocker):
    mocker.patch("dbt_runner.app._load_intents", return_value={
                    "after_srp": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"],
                    "after_detail": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"],
    })
    response = mock_client.get("/dbt/intents")
    assert response.status_code == 200
    assert response.json() == {"intents": {
                                "after_srp": { "select": ["stg_raw_artifacts+", "stg_srp_observations+", "stg_detail_carousel_hints+"]},
                                "after_detail": {"select": ["stg_raw_artifacts+", "stg_detail_observations+", "stg_detail_carousel_hints+"]},
                                }}
    

def test_set_intents_no_select_args(mock_client, mocker):
    mocker.patch("dbt_runner.app._save_intent", return_value=True)
    response = mock_client.post("/dbt/intents", json={"intent_name": "after_srp"})
    assert response.status_code == 200
    assert response.json() == {"ok": True, "intent_name": "after_srp", "select_args": []}


def test_set_intents_no_intent(mock_client, mocker):
    response = mock_client.post("/dbt/intents", json={"select_args": ["model_a"]})
    assert response.status_code == 400
    assert "intent_name is required" in response.json()["detail"]


def test_set_intents_with_select_args(mock_client, mocker):
    mocker.patch("dbt_runner.app._save_intent", return_value=True)
    response = mock_client.post("/dbt/intents", json={"intent_name": "after_srp", "select_args": "model_a model_b"})
    assert response.status_code == 200
    assert response.json()["select_args"] == ["model_a", "model_b"]


def test_set_intents_invalid_tokens(mock_client, mocker):
    response = mock_client.post("/dbt/intents", json={"intent_name": "after_srp", "select_args": ["model a"]})
    assert response.status_code == 400


def test_set_intents_failed_to_save(mock_client, mocker):
    mocker.patch("dbt_runner.app._save_intent", return_value=False)
    response = mock_client.post("/dbt/intents", json={"intent_name": "after_srp", "select_args": ["model_a"]})
    assert response.status_code == 409
    assert response.json()["detail"] == "failed to write to DB"


def test_delete_intents_success(mock_client, mocker):
    mocker.patch("dbt_runner.app._delete_intent", return_value=True)
    response = mock_client.delete("/dbt/intents/after_srp")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "deleted": "after_srp"}


def test_delete_intents_failure(mock_client, mocker):
    mocker.patch("dbt_runner.app._delete_intent", return_value=False)
    response = mock_client.delete("/dbt/intents/nonexistent")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_get_docs_status_available(mock_client, mocker):
    mocker.patch("os.path.exists", return_value=True)
    response = mock_client.get("/dbt/docs/status")
    assert response.status_code == 200
    assert response.json() == {"available": True}


def test_get_docs_status_not_available(mock_client, mocker):
    mocker.patch("os.path.exists", return_value=False)
    response = mock_client.get("/dbt/docs/status")
    assert response.status_code == 200
    assert response.json() == {"available": False}


def test_get_docs_status_permission_error(mock_client, mocker):
    mocker.patch("os.path.exists", side_effect=PermissionError)
    with pytest.raises(PermissionError):
        response = mock_client.get("/dbt/docs/status")


def test_dbt_docs_generate_success(mock_client, mocker):
    mock_run = mocker.patch("subprocess.run")
    # First call (dbt deps), second call (dbt docs generate)
    mock_run.side_effect = [
        mocker.MagicMock(returncode=0, stdout="deps ok", stderr=""),
        mocker.MagicMock(returncode=0, stdout="docs ok", stderr=""),
    ]
    response = mock_client.post("/dbt/docs/generate")
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_dbt_docs_generate_packages_missing(mock_client, mocker):
    mocker.patch("subprocess.run", return_value=mocker.MagicMock(returncode=1, stdout="", stderr="error: packages not found"))
    response = mock_client.post("/dbt/docs/generate")
    assert response.status_code == 500


def test_dbt_docs_generate_failed_to_generate(mock_client, mocker):
    mock_run = mocker.patch("subprocess.run")
    # First call (dbt deps) succeeds, second call (dbt docs generate) fails
    mock_run.side_effect = [
        mocker.MagicMock(returncode=0, stdout="deps ok", stderr=""),
        mocker.MagicMock(returncode=1, stdout="", stderr="error: generation failed"),
    ]
    response = mock_client.post("/dbt/docs/generate")
    assert response.status_code == 500


def test_dbt_build_with_intent_no_select(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"intent": "after_srp"})
    assert response.status_code == 200
    assert response.json()["select"] == ["model_a"]


def test_dbt_build_no_intent_with_select(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 200
    assert response.json()["intent"] is None


def test_dbt_build_with_intent_with_select(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"intent": "after_srp", "select": ["model_x"]})
    assert response.status_code == 200
    assert response.json()["select"] == ["model_x"]


def test_dbt_build_no_intent_no_select(mock_client, mocker):
    response = mock_client.post("/dbt/build", json={})
    assert response.status_code == 400
    assert "Provide either 'intent' or 'select'" in response.json()["detail"]


def test_dbt_build_with_intent_no_select_bad_intent(mock_client, mocker):
    mocker.patch("dbt_runner.app._load_intents", return_value={"after_srp": ["model_a"]})
    response = mock_client.post("/dbt/build", json={"intent": "unknown_intent"})
    assert response.status_code == 400
    assert "Unknown intent" in response.json()["detail"]


def test_dbt_build_select_invalid_tokens(mock_client):
    response = mock_client.post("/dbt/build", json={"select": ["model a"]})
    assert response.status_code == 400


def test_dbt_build_exclude_invalid_tokens(mock_client):
    response = mock_client.post("/dbt/build", json={"select": ["model_a"], "exclude": ["model#tag"]})
    assert response.status_code == 400


def test_dbt_build_succeeds(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 200
    assert mock_dbt_build_happy_path["release_lock"].called


def test_dbt_build_fails(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=1, stdout="error output", stderr="error")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 500


def test_dbt_build_lock_held(mock_client, mocker):
    mocker.patch("dbt_runner.app._acquire_lock", return_value=False)
    mocker.patch("dbt_runner.app._lock_status", return_value={"locked": True, "locked_by": "other_caller"})
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "dbt_locked"


def test_dbt_build_record_run_called(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"intent": "after_srp"})
    assert response.status_code == 200
    mock_dbt_build_happy_path["record_run"].assert_called_once()
    call_args = mock_dbt_build_happy_path["record_run"].call_args[0]
    assert call_args[3] == "after_srp"  # intent
    assert call_args[4] == ["model_a"]  # select


def test_dbt_build_record_failed(mock_client, mock_dbt_build_happy_path, mock_app_logger_error, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    mock_dbt_build_happy_path["record_run"].return_value = False
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 200  # Request succeeded despite record failure
    assert "Logging Run Failed" in mock_app_logger_error.call_args[0][0]


def test_dbt_build_lock_release_fails(mock_client, mocker, mock_app_logger_error, mock_dbt_build_happy_path):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    mock_dbt_build_happy_path["release_lock"].return_value = None

    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 200  # Build succeeded, but lock release failed
    assert "Failed to release dbt lock" in mock_app_logger_error.call_args[0][0]


def test_dbt_build_select_as_string(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"select": "model_a"})
    assert response.status_code == 200
    assert response.json()["select"] == ["model_a"]


def test_dbt_build_exclude_as_string(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"], "exclude": "model_b"})
    assert response.status_code == 200
    assert "--exclude" in response.json()["cmd"]


def test_dbt_build_full_refresh_flag(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"], "full_refresh": True})
    assert response.status_code == 200
    assert "--full-refresh" in response.json()["cmd"]
