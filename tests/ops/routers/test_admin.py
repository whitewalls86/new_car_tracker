# =============================================================================
# Test plan for ops/routers/admin.py
# =============================================================================
#
# HELPERS
# -------
# test_slug_basic                   - spaces/slashes become hyphens, lowercased
# test_slug_strips_invalid_chars    - special chars removed
# test_parse_comma_list_basic       - splits and trims correctly
# test_parse_comma_list_empty       - empty strings filtered out
# test_row_to_dict_string_params    - JSON string params are unpacked to dict
# test_row_to_dict_dict_params      - dict params passed through unchanged
# test_stringify_uuids              - UUID values converted to strings
#
# _fetch_dbt_context
# ------------------
# test_fetch_dbt_context_all_ok         - all three requests succeed, returns lock/intents/docs
# test_fetch_dbt_context_lock_fails     - lock request fails, defaults used
# test_fetch_dbt_context_intents_fails  - intents request fails, defaults used
# test_fetch_dbt_context_docs_fails     - docs request fails, defaults used
# test_fetch_dbt_context_all_fail       - all requests fail, full defaults returned
#
# GET /searches/
# --------------
# test_list_searches_ok             - 200, configs rendered
# test_list_searches_db_error       - DB error → still returns 200 or handles gracefully
#
# GET /searches/new
# -----------------
# test_new_search_form_ok           - 200, editing=False, sort_options present
#
# GET /searches/{search_key}/edit
# --------------------------------
# test_edit_search_form_found       - 200, editing=True, config populated
# test_edit_search_form_not_found   - redirects to /admin/searches/
#
# POST /searches/ (create)
# ------------------------
# test_create_search_ok             - valid form data → redirects to /admin/searches/
# test_create_search_invalid_params - bad zip/makes → 422, form re-rendered with error
# test_create_search_duplicate_key  - duplicate key → 422, "already exists" error
# test_create_search_db_error       - non-duplicate DB error → 422 with error message
# test_create_search_scope_defaults - no scopes selected → defaults to local+national
#
# POST /searches/{search_key} (update)
# -------------------------------------
# test_update_search_ok             - valid form → redirects to /admin/searches/
# test_update_search_invalid_params - bad params → 422, form re-rendered with error
# test_update_search_scope_defaults - no scopes → defaults to local+national
#
# POST /searches/{search_key}/toggle
# ------------------------------------
# test_toggle_search_ok             - toggles enabled, redirects
#
# POST /searches/{search_key}/delete
# ------------------------------------
# test_delete_search_ok             - renames to _deleted_... prefix, disables, redirects
#
# GET /runs
# ---------
# test_list_runs_ok                 - 200, runs list rendered
#
# GET /runs/{run_id}
# ------------------
# test_run_detail_found             - 200, run + jobs rendered
# test_run_detail_not_found         - redirects to /admin/runs
#
# GET /dbt
# --------
# test_dbt_dashboard_ok             - 200, context includes lock/intents/docs
#
# POST /dbt/trigger
# -----------------
# test_dbt_trigger_with_intent      - posts intent to dbt_runner, renders result
# test_dbt_trigger_with_select      - select_override takes precedence over intent
# test_dbt_trigger_dbt_runner_fails - dbt_runner error → trigger_ok=False
# test_dbt_trigger_request_fails    - network error → error dict in trigger_result
#
# POST /dbt/intents
# -----------------
# test_dbt_intent_upsert_ok         - posts to dbt_runner, redirects to /admin/dbt
# test_dbt_intent_upsert_fails      - request fails, still redirects (silent fail)
#
# POST /dbt/intents/{name}/delete
# --------------------------------
# test_dbt_intent_delete_ok         - deletes from dbt_runner, redirects
# test_dbt_intent_delete_fails      - request fails, still redirects (silent fail)
#
# POST /dbt/docs/generate
# -----------------------
# test_dbt_docs_generate_ok         - success → docs_ok=True
# test_dbt_docs_generate_fails      - non-200 → docs_ok=False
# test_dbt_docs_generate_error      - network error → error dict in docs_result
#
# GET /logs
# ---------
# test_view_logs_ok                 - all sources return lines
# test_view_logs_scraper_fails      - scraper unreachable → scraper_lines=[]
# test_view_logs_dbt_fails          - dbt_runner unreachable → dbt_lines=[]
# test_view_logs_file_not_found     - ops log missing → ops_lines=[]
#
# GET /deploy
# -----------
# test_deploy_panel_ok              - 200, status rendered
#
# POST /deploy/start
# ------------------
# test_deploy_start_ok              - calls _set_intent, redirects
#
# POST /deploy/complete
# ---------------------
# test_deploy_complete_ok           - calls _intent_release, redirects
#

import json
import uuid
from unittest.mock import MagicMock

import pytest
from fastapi.responses import HTMLResponse

from ops.routers import admin

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_templates(mocker):
    """Prevent actual template rendering — return HTMLResponse respecting status_code kwarg."""
    def _fake_response(*args, **kwargs):
        status_code = kwargs.get("status_code", 200)
        return HTMLResponse("<html/>", status_code=status_code)

    return mocker.patch("ops.routers.admin.templates.TemplateResponse", side_effect=_fake_response)


@pytest.fixture
def mock_get_conn(mocker):
    """Mock ops.routers.admin.get_conn with a configurable cursor."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cursor
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mocker.patch("ops.routers.admin.get_conn", return_value=conn)
    return conn, cursor


@pytest.fixture
def mock_dbt_context(mocker):
    """Mock _fetch_dbt_context to avoid HTTP calls in endpoint tests."""
    return mocker.patch(
        "ops.routers.admin._fetch_dbt_context",
        return_value={"lock": {}, "intents": {}, "docs_available": False},
    )


@pytest.fixture
def mock_deploy_functions(mocker):
    return {
        "intent_status": mocker.patch(
            "ops.routers.admin._intent_status",
            return_value={"intent": "none"},
        ),
        "set_intent": mocker.patch(
            "ops.routers.admin._set_intent", return_value=True
        ),
        "intent_release": mocker.patch(
            "ops.routers.admin._intent_release", return_value=True
        ),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def test_slug_basic():
    assert admin._slug("Honda CR-V") == "honda-cr-v"
    assert admin._slug("Ford/F-150") == "ford-f-150"


def test_slug_strips_invalid_chars():
    assert admin._slug("Test! @#$%") == "test-"


def test_parse_comma_list_basic():
    assert admin._parse_comma_list("Honda, Toyota, Ford") == ["Honda", "Toyota", "Ford"]


def test_parse_comma_list_empty():
    assert admin._parse_comma_list("Honda,,  , Toyota") == ["Honda", "Toyota"]


def test_row_to_dict_string_params():
    row = {"search_key": "test", "params": '{"makes": ["Honda"]}'}
    result = admin._row_to_dict(row)
    assert result["params"] == {"makes": ["Honda"]}


def test_row_to_dict_dict_params():
    row = {"search_key": "test", "params": {"makes": ["Honda"]}}
    result = admin._row_to_dict(row)
    assert result["params"] == {"makes": ["Honda"]}


def test_stringify_uuids():
    test_uuid = uuid.uuid4()
    result = admin._stringify_uuids({"id": test_uuid, "name": "test"})
    assert isinstance(result["id"], str)
    assert result["name"] == "test"


# ---------------------------------------------------------------------------
# _fetch_dbt_context
# ---------------------------------------------------------------------------

def test_fetch_dbt_context_all_ok(mock_requests):
    mock_requests["get"].return_value.json.side_effect = [
        {"locked": True, "locked_at": None, "locked_by": "test"},
        {"intents": {"after_srp": {"select": ["model_a"]}}},
        {"available": True},
    ]
    mock_requests["get"].return_value.status_code = 200

    result = admin._fetch_dbt_context()

    assert result["lock"] == {"locked": True, "locked_at": None, "locked_by": "test"}
    assert result["intents"] == {"after_srp": {"select": ["model_a"]}}
    assert result["docs_available"] is True


def test_fetch_dbt_context_all_fail(mock_requests):
    mock_requests["get"].side_effect = Exception("Connection refused")

    result = admin._fetch_dbt_context()

    assert result["lock"] == {"locked": False, "locked_at": None, "locked_by": None}
    assert result["intents"] == {}
    assert result["docs_available"] is False


def test_fetch_dbt_context_lock_fails(mock_requests):
    mock_requests["get"].side_effect = [
        Exception("timeout"),
        MagicMock(**{"json.return_value": {"intents": {}}}),
        MagicMock(**{"json.return_value": {"available": False}}),
    ]

    result = admin._fetch_dbt_context()

    assert result["lock"] == {"locked": False, "locked_at": None, "locked_by": None}


# ---------------------------------------------------------------------------
# GET /searches/
# ---------------------------------------------------------------------------

def test_list_searches_ok(mock_client, mock_cursor_context, mock_templates):
    conn, cursor = mock_cursor_context
    cursor.fetchall.return_value = []

    response = mock_client.get("/admin/searches/")

    assert response.status_code == 200
    mock_templates.assert_called_once()


def test_list_searches_bad_db(mock_client, mock_db_connection_error, mock_templates):
    response = mock_client.get("/admin/searches/")

    assert response.status_code == 503
    mock_templates.assert_called_once()


# ---------------------------------------------------------------------------
# GET /searches/new
# ---------------------------------------------------------------------------

def test_new_search_form_ok(mock_client, mock_templates):
    response = mock_client.get("/admin/searches/new")

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["editing"] is False
    assert call_kwargs["context"]["sort_options"] is not None


# ---------------------------------------------------------------------------
# GET /searches/{search_key}/edit
# ---------------------------------------------------------------------------

def test_edit_search_form_found(mock_client, mock_cursor_context, mock_templates):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "search_key": "honda-crv",
        "enabled": True,
        "source": None,
        "params": '{"makes": ["Honda"]}',
        "rotation_order": None,
        "last_queued_at": None,
    }

    response = mock_client.get("/admin/searches/honda-crv/edit")

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["editing"] is True


def test_edit_search_form_not_found(mock_client, mock_cursor_context, mock_templates):
    conn, cursor = mock_cursor_context
    cursor.fetchone.return_value = None

    response = mock_client.get("/admin/searches/nonexistent/edit", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/searches/"


def test_edit_search_form_db_error(mock_client, mock_db_connection_error, mock_templates):
    response = mock_client.get("/admin/searches/honda-crv/edit")

    assert response.status_code == 503
    mock_templates.assert_called_once()


# ---------------------------------------------------------------------------
# GET /dbt
# ---------------------------------------------------------------------------

def test_dbt_dashboard_ok(mock_client, mock_dbt_context, mock_templates):
    response = mock_client.get("/admin/dbt")

    assert response.status_code == 200
    mock_dbt_context.assert_called_once()


# ---------------------------------------------------------------------------
# POST /dbt/trigger
# ---------------------------------------------------------------------------

def test_dbt_trigger_with_intent(mock_client, mock_requests, mock_dbt_context, mock_templates):
    mock_requests["post"].return_value.status_code = 200
    mock_requests["post"].return_value.json.return_value = {"ok": True}

    response = mock_client.post("/admin/dbt/trigger", data={"intent": "after_srp"})

    assert response.status_code == 200
    payload = mock_requests["post"].call_args.kwargs["json"]
    assert payload["intent"] == "after_srp"


def test_dbt_trigger_with_select_override(
    mock_client, mock_requests, mock_dbt_context, mock_templates
):
    mock_requests["post"].return_value.status_code = 200
    mock_requests["post"].return_value.json.return_value = {"ok": True}

    response = mock_client.post("/admin/dbt/trigger", data={
        "intent": "after_srp",
        "select_override": "model_a model_b",
    })

    assert response.status_code == 200
    payload = mock_requests["post"].call_args.kwargs["json"]
    assert payload["select"] == ["model_a", "model_b"]
    assert "intent" not in payload


def test_dbt_trigger_request_fails(mock_client, mock_requests, mock_dbt_context, mock_templates):
    mock_requests["post"].side_effect = Exception("timeout")

    response = mock_client.post("/admin/dbt/trigger", data={"intent": "after_srp"})

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert "error" in call_kwargs["context"]["trigger_result"]


# ---------------------------------------------------------------------------
# POST /dbt/intents
# ---------------------------------------------------------------------------

def test_dbt_intent_upsert_ok(mock_client, mock_requests):
    mock_requests["post"].return_value.status_code = 200

    response = mock_client.post("/admin/dbt/intents", data={
        "intent_name": "after_srp",
        "select_args": "model_a model_b",
    }, follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/dbt"


def test_dbt_intent_upsert_fails(mock_client, mock_requests):
    mock_requests["post"].side_effect = Exception("timeout")

    response = mock_client.post("/admin/dbt/intents", data={
        "intent_name": "after_srp",
        "select_args": "model_a",
    }, follow_redirects=False)

    assert response.status_code == 303


# ---------------------------------------------------------------------------
# POST /dbt/intents/{name}/delete
# ---------------------------------------------------------------------------

def test_dbt_intent_delete_ok(mock_client, mock_requests):
    mock_requests["delete"].return_value.status_code = 200

    response = mock_client.post("/admin/dbt/intents/after_srp/delete", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/dbt"


def test_dbt_intent_delete_fails(mock_client, mock_requests):
    mock_requests["delete"].side_effect = Exception("timeout")

    response = mock_client.post("/admin/dbt/intents/after_srp/delete", follow_redirects=False)

    assert response.status_code == 303


# ---------------------------------------------------------------------------
# POST /dbt/docs/generate
# ---------------------------------------------------------------------------

def test_dbt_docs_generate_ok(mock_client, mock_requests, mock_dbt_context, mock_templates):
    mock_requests["post"].return_value.status_code = 200
    mock_requests["post"].return_value.json.return_value = {"ok": True}

    response = mock_client.post("/admin/dbt/docs/generate")

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["docs_ok"] is True


def test_dbt_docs_generate_fails(mock_client, mock_requests, mock_dbt_context, mock_templates):
    mock_requests["post"].return_value.status_code = 500
    mock_requests["post"].return_value.json.return_value = {"ok": False}

    response = mock_client.post("/admin/dbt/docs/generate")

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["docs_ok"] is False


def test_dbt_docs_generate_error(mock_client, mock_requests, mock_dbt_context, mock_templates):
    mock_requests["post"].side_effect = Exception("timeout")

    response = mock_client.post("/admin/dbt/docs/generate")

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert "error" in call_kwargs["context"]["docs_result"]


# ---------------------------------------------------------------------------
# GET /logs
# ---------------------------------------------------------------------------

def test_view_logs_ok(mock_client, mock_requests, mock_templates, mocker):
    mock_requests["get"].return_value.json.return_value = {"lines": ["line1\n"]}
    mocker.patch("builtins.open", mocker.mock_open(read_data="ops line\n"))

    response = mock_client.get("/admin/logs")

    assert response.status_code == 200


def test_view_logs_scraper_fails(mock_client, mock_requests, mock_templates, mocker):
    mock_requests["get"].side_effect = Exception("timeout")
    mocker.patch("builtins.open", side_effect=FileNotFoundError)

    response = mock_client.get("/admin/logs")

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["scraper_lines"] == []


def test_view_logs_file_not_found(mock_client, mock_requests, mock_templates, mocker):
    mock_requests["get"].return_value.json.return_value = {"lines": []}
    mocker.patch("builtins.open", side_effect=FileNotFoundError)

    response = mock_client.get("/admin/logs")

    assert response.status_code == 200
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["ops_lines"] == []


# ---------------------------------------------------------------------------
# GET /deploy
# ---------------------------------------------------------------------------

def test_deploy_panel_ok(mock_client, mock_deploy_functions, mock_templates):
    response = mock_client.get("/admin/deploy")

    assert response.status_code == 200
    mock_deploy_functions["intent_status"].assert_called_once()


# ---------------------------------------------------------------------------
# POST /deploy/start
# ---------------------------------------------------------------------------

def test_deploy_start_ok(mock_client, mock_deploy_functions):
    response = mock_client.post("/admin/deploy/start", follow_redirects=False)

    assert response.status_code == 303
    mock_deploy_functions["set_intent"].assert_called_once_with("Admin UI")


# ---------------------------------------------------------------------------
# POST /deploy/complete
# ---------------------------------------------------------------------------

def test_deploy_complete_ok(mock_client, mock_deploy_functions):
    response = mock_client.post("/admin/deploy/complete", follow_redirects=False)

    assert response.status_code == 303
    mock_deploy_functions["intent_release"].assert_called_once()


# ---------------------------------------------------------------------------
# POST /searches/ (create)
# ---------------------------------------------------------------------------

VALID_SEARCH_FORM = {
    "search_key": "honda crv",
    "makes": "Honda",
    "models": "CR-V",
    "zip": "77080",
    "radius_miles": "200",
    "max_listings": "2000",
    "max_safety_pages": "30",
    "scope_local": "true",
    "scope_national": "true",
}


def test_create_search_ok(mock_client, mock_cursor_context, mock_templates):
    response = mock_client.post("/admin/searches/", data=VALID_SEARCH_FORM, follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/searches/"


def test_create_search_invalid_params(mock_client, mock_templates):
    response = mock_client.post("/admin/searches/", data={
        **VALID_SEARCH_FORM, "zip": "ABCDE"
    })

    assert response.status_code == 422
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["error"] is not None


def test_create_search_duplicate_key(mock_client, mock_cursor_context, mock_templates):
    conn, cursor = mock_cursor_context
    cursor.execute.side_effect = Exception("duplicate key value violates unique constraint")

    response = mock_client.post("/admin/searches/", data=VALID_SEARCH_FORM)

    assert response.status_code == 422
    call_kwargs = mock_templates.call_args.kwargs
    assert "already exists" in call_kwargs["context"]["error"]


def test_create_search_scope_defaults(mock_client, mock_cursor_context, mock_templates):
    """When no scopes are selected, should default to local+national."""
    conn, cursor = mock_cursor_context
    form_data = {k: v for k, v in VALID_SEARCH_FORM.items()
                 if k not in ("scope_local", "scope_national")}

    response = mock_client.post("/admin/searches/", data=form_data, follow_redirects=False)

    assert response.status_code == 303
    insert_call = cursor.execute.call_args
    params_json = json.loads(insert_call.args[1][2])
    assert set(params_json["scopes"]) == {"local", "national"}


# ---------------------------------------------------------------------------
# POST /searches/{search_key} (update)
# ---------------------------------------------------------------------------

def test_update_search_ok(mock_client, mock_cursor_context, mock_templates):
    response = mock_client.post(
        "/admin/searches/honda-crv",
        data=VALID_SEARCH_FORM,
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/searches/"


def test_update_search_invalid_params(mock_client, mock_templates):
    response = mock_client.post("/admin/searches/honda-crv", data={
        **VALID_SEARCH_FORM, "zip": "ABCDE"
    })

    assert response.status_code == 422
    call_kwargs = mock_templates.call_args.kwargs
    assert call_kwargs["context"]["error"] is not None


def test_update_search_db_error(mock_client, mock_db_connection_error, mock_templates):
    response = mock_client.post("/admin/searches/honda-crv", data=VALID_SEARCH_FORM)

    assert response.status_code == 503
    mock_templates.assert_called_once()


# ---------------------------------------------------------------------------
# POST /searches/{search_key}/toggle
# ---------------------------------------------------------------------------

def test_toggle_search_ok(mock_client, mock_cursor_context):
    response = mock_client.post("/admin/searches/honda-crv/toggle", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/searches/"


def test_toggle_search_db_error(mock_client, mock_db_connection_error, mock_templates):
    response = mock_client.post("/admin/searches/honda-crv/toggle")

    assert response.status_code == 503
    mock_templates.assert_called_once()


# ---------------------------------------------------------------------------
# POST /searches/{search_key}/delete
# ---------------------------------------------------------------------------

def test_delete_search_ok(mock_client, mock_cursor_context):
    conn, cursor = mock_cursor_context

    response = mock_client.post("/admin/searches/honda-crv/delete", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/searches/"
    execute_args = cursor.execute.call_args.args[1]
    assert execute_args[0].startswith("_deleted_honda-crv_")
    assert execute_args[1] == "honda-crv"


def test_delete_search_db_error(mock_client, mock_db_connection_error, mock_templates):
    response = mock_client.post("/admin/searches/honda-crv/delete")

    assert response.status_code == 503
    mock_templates.assert_called_once()


def test_create_search_db_error(mock_client, mock_db_connection_error, mock_templates):
    response = mock_client.post("/admin/searches/", data=VALID_SEARCH_FORM)

    assert response.status_code == 503
    mock_templates.assert_called_once()


def test_update_search_scope_defaults(mock_client, mock_cursor_context):
    conn, cursor = mock_cursor_context
    form_data = {k: v for k, v in VALID_SEARCH_FORM.items()
                 if k not in ("scope_local", "scope_national")}

    response = mock_client.post("/admin/searches/honda-crv", data=form_data, follow_redirects=False)

    assert response.status_code == 303
    update_call = cursor.execute.call_args
    params_json = json.loads(update_call.args[1][1])
    assert set(params_json["scopes"]) == {"local", "national"}
