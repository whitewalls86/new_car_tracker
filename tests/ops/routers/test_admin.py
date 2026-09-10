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
# _fetch_dbt_context   (Plan 162 Stage AA: was lock/intents/docs, two of which
#                       dbt_runner stopped serving in April)
# ------------------
# test_fetch_dbt_context_reads_ready_and_docs        - /ready, /dbt/selectors, /dbt/docs/status
# test_fetch_dbt_context_reads_a_503_as_busy_not_broken - a declared refusal is evidence
# test_fetch_dbt_context_unreachable_is_unknown_not_idle - None, so the panel can say so
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
# test_dbt_dashboard_ok             - 200, context includes build state/selectors/docs
#
# POST /dbt/trigger
# -----------------
# test_dbt_trigger_sends_a_selector - posts a named cadence dbt_runner will accept
# test_dbt_trigger_with_select_override - raw tokens take precedence over the cadence
# test_dbt_trigger_request_fails    - network error → error dict in trigger_result
#
# POST /dbt/intents, POST /dbt/intents/{name}/delete
# --------------------------------------------------
# Deleted by Plan 162 Stage AA. dbt_runner removed the endpoints behind them in
# April (`9f08336`) and both callers swallowed the failure, so the panel had
# managed nothing since. dbt's own `selectors.yml` is the replacement, reached
# through `/dbt/selectors` and `--selector`.
#
# POST /dbt/docs/generate
# -----------------------
# test_dbt_docs_generate_ok         - success → docs_ok=True
# test_dbt_docs_generate_fails      - non-200 → docs_ok=False
# test_dbt_docs_generate_error      - network error → error dict in docs_result
#
# GET /logs
# ---------
# test_view_logs_shows_this_services_own_file - the ops log, read from disk
# test_view_logs_file_not_found     - ops log missing → ops_lines=[]
# The scraper and dbt_runner panes were removed by Plan 162 Stage AA: neither
# service has served `GET /logs` since May and both fetches were swallowed, so
# each pane read "unreachable" whether the service was down or healthy. Plan
# 104's Loki holds those logs and the page links there.
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
from ops.routers.deploy import IntentResult
from tests.response_fixtures import produced_by
from tests.service_contracts import service_response

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
        return_value=produced_by(
            "_fetch_dbt_context", build_state=None, selectors=[], docs_available=False,
        ),
    )


@pytest.fixture
def mock_deploy_functions(mocker):
    return {
        "intent_status": mocker.patch(
            "ops.routers.admin._intent_status",
            return_value=produced_by("_intent_status", intent='none'),
        ),
        "set_intent": mocker.patch(
            "ops.routers.admin._set_intent", return_value=IntentResult("ok")
        ),
        "intent_release": mocker.patch(
            "ops.routers.admin._intent_release", return_value=IntentResult("ok")
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

def test_fetch_dbt_context_reads_ready_and_docs(mock_requests):
    """Plan 162 Stage AA: two of the three calls this made did not exist.

    `GET /dbt/lock` and `GET /dbt/intents` were removed in April and every call
    here was wrapped in `except: pass`, so this test asserted a lock shape and
    an intent map that dbt_runner had not sent for months. `/ready` is what
    carries build state, and it is a route the contract declares.
    """
    mock_requests["get"].return_value.status_code = 200
    mock_requests["get"].return_value.json.side_effect = [
        service_response("dbt_runner", "GET", "/ready", ready=False, active_jobs=2),
        service_response(
            "dbt_runner", "GET", "/dbt/selectors", selectors=["hourly_core"],
        ),
        service_response("dbt_runner", "GET", "/dbt/docs/status", available=True),
    ]

    result = admin._fetch_dbt_context()

    assert result["build_state"]["active_jobs"] == 2
    assert result["docs_available"] is True
    assert result["selectors"] == ["hourly_core"]


def test_fetch_dbt_context_reads_a_503_as_busy_not_broken(mock_requests):
    """A declared refusal is evidence, not a failure.

    `/ready` answers 503 while a build runs and carries the counts in `detail`
    -- the same body `ops/coordination_drain.py` reads as *known* positive
    evidence. A panel that rendered that as "unreachable" would report a
    working build as a broken service.
    """
    not_ready = service_response("dbt_runner", "GET", "/ready", "503")
    not_ready["detail"].update(active_jobs=1, reason="jobs in flight")

    busy = mock_requests["get"].return_value
    busy.status_code = 503
    busy.json.side_effect = [
        not_ready,
        service_response("dbt_runner", "GET", "/dbt/selectors"),
        service_response("dbt_runner", "GET", "/dbt/docs/status"),
    ]

    result = admin._fetch_dbt_context()

    assert result["build_state"]["active_jobs"] == 1


def test_fetch_dbt_context_unreachable_is_unknown_not_idle(mock_requests):
    """`None` rather than a zeroed build state, so the panel can say so.

    The old code substituted `{"locked": False}` on any failure, which renders
    identically to a healthy idle runner -- the service being down and the
    service being free looked the same.
    """
    mock_requests["get"].side_effect = Exception("Connection refused")

    result = admin._fetch_dbt_context()

    assert result["build_state"] is None
    assert result["docs_available"] is False


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

def test_dbt_trigger_sends_a_selector(mock_client, mock_requests, mock_dbt_context, mock_templates):
    """Plan 162 Stage AA: this sent `intent`, which nothing read.

    `dbt_build` reads select/exclude/full_refresh/fail_fast and never read
    `intent`, so the panel's dropdown had no effect at either end. It now sends
    `selector`, a name from the same `selectors.yml` dbt itself reads.
    """
    mock_requests["post"].return_value.status_code = 200
    mock_requests["post"].return_value.json.return_value = {"ok": True}

    response = mock_client.post("/admin/dbt/trigger", data={"selector": "hourly_core"})

    assert response.status_code == 200
    payload = mock_requests["post"].call_args.kwargs["json"]
    assert payload["selector"] == "hourly_core"


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

LOG_LINE = "ops line\n"


def test_view_logs_shows_this_services_own_file(mock_client, mock_templates, mocker):
    """Plan 162 Stage AA: two of the three panes were fed by dead routes.

    `GET /logs` on scraper and on dbt_runner stopped existing in May, and both
    fetches were wrapped in `except: pass`, so the page rendered "unreachable"
    whether the service was down or healthy. Plan 104's Loki is where those
    logs are, and the page links there instead.
    """
    mocker.patch("builtins.open", mocker.mock_open(read_data=LOG_LINE))

    response = mock_client.get("/admin/logs")

    assert response.status_code == 200
    context = mock_templates.call_args.kwargs["context"]
    assert context["ops_lines"] == [LOG_LINE]
    assert context["grafana_url"]


def test_view_logs_file_not_found(mock_client, mock_templates, mocker):
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
    mock_deploy_functions["set_intent"].return_value = IntentResult("ok")

    response = mock_client.post("/admin/deploy/request", follow_redirects=False)

    assert response.status_code == 303
    mock_deploy_functions["set_intent"].assert_called_once_with("Admin UI")


@pytest.mark.parametrize(
    ("outcome", "status_code"),
    [("locked", 409), ("invalid", 409), ("unavailable", 503), ("error", 500)],
)
def test_deploy_request_reports_every_refusal(
    mock_client, mock_deploy_functions, mock_templates, outcome, status_code
):
    """Plan 162 Stage AA, G27. All five outcomes rendered as one 303 before this.

    `_set_intent` answers ok/locked/invalid/unavailable/error and the button
    discarded it, so an operator saw the same page whether the intent was
    recorded or Postgres refused the write. The codes match what the API pair
    answers for the same status, so the two surfaces cannot disagree about what
    `locked` means.
    """
    mock_deploy_functions["set_intent"].return_value = IntentResult(outcome, "because")

    response = mock_client.post("/admin/deploy/request", follow_redirects=False)

    assert response.status_code == status_code


# ---------------------------------------------------------------------------
# POST /deploy/complete
# ---------------------------------------------------------------------------

def test_deploy_complete_ok(mock_client, mock_deploy_functions):
    mock_deploy_functions["intent_release"].return_value = IntentResult("ok")

    response = mock_client.post("/admin/deploy/release", follow_redirects=False)

    assert response.status_code == 303


@pytest.mark.parametrize(
    ("outcome", "status_code"),
    [("locked", 409), ("unavailable", 503), ("error", 500)],
)
def test_deploy_release_reports_every_refusal(
    mock_client, mock_deploy_functions, mock_templates, outcome, status_code
):
    """The quieter failure of the two, per `_intent_release`'s own docstring.

    A release that fails leaves every gated DAG parked, and until Plan 162
    Stage AA the button reported that exactly as it reported success.
    """
    mock_deploy_functions["intent_release"].return_value = IntentResult(outcome, "because")

    response = mock_client.post("/admin/deploy/release", follow_redirects=False)

    assert response.status_code == status_code


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
