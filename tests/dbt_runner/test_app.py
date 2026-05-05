import pytest
from fastapi import HTTPException

from dbt_runner import app

# ---------------------------------------------------------------------------
# _validate_tokens
# ---------------------------------------------------------------------------

INVALID_TOKENS = [
    "model one",           # space
    "model!",              # exclamation
    "model#tag",           # hash
    "model$var",           # dollar
    "model&other",         # ampersand
    "model()",             # parentheses
    "",                    # empty
    "modèl",               # non-ASCII
    "model@hostname#fail", # has #
]

VALID_TOKENS = [
    "model",
    "stg_observations+",
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
    app._validate_tokens([token], "test_field")


# ---------------------------------------------------------------------------
# _cap
# ---------------------------------------------------------------------------

def test_cap_short_string():
    result = app._cap("This is a short string.")
    assert result == "This is a short string."


def test_cap_short_limit():
    result = app._cap(s="This is a short string.", limit=5)
    assert result == "ring."


def test_cap_max_length():
    long_string = "a" * 25000
    result = app._cap(s=long_string)
    assert len(result) == 20000
    assert result == "a" * 20000


def test_cap_none_value():
    result = app._cap(s=None)
    assert result == ""


def test_cap_empty_string():
    result = app._cap(s="")
    assert result == ""


# ---------------------------------------------------------------------------
# /health and /ready
# ---------------------------------------------------------------------------

def test_get_health(mock_client):
    response = mock_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_get_ready_when_idle(mock_client, mocker):
    mocker.patch("dbt_runner.app.is_idle", return_value=True)
    response = mock_client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"ready": True}


def test_get_ready_when_busy(mock_client, mocker):
    mocker.patch("dbt_runner.app.is_idle", return_value=False)
    response = mock_client.get("/ready")
    assert response.status_code == 503
    assert response.json()["detail"]["ready"] is False
    assert response.json()["detail"]["reason"] == "jobs in flight"


# ---------------------------------------------------------------------------
# /dbt/docs/status
# ---------------------------------------------------------------------------

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
        mock_client.get("/dbt/docs/status")


# ---------------------------------------------------------------------------
# /dbt/docs/generate
# ---------------------------------------------------------------------------

def test_dbt_docs_generate_success(mock_client, mocker):
    mock_run = mocker.patch("subprocess.run")
    mock_run.side_effect = [
        mocker.MagicMock(returncode=0, stdout="deps ok", stderr=""),
        mocker.MagicMock(returncode=0, stdout="docs ok", stderr=""),
    ]
    response = mock_client.post("/dbt/docs/generate")
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_dbt_docs_generate_packages_missing(mock_client, mocker):
    mock_result = mocker.MagicMock(
        returncode=1, stdout="", stderr="error: packages not found"
    )
    mocker.patch("subprocess.run", return_value=mock_result)
    response = mock_client.post("/dbt/docs/generate")
    assert response.status_code == 500


def test_dbt_docs_generate_failed_to_generate(mock_client, mocker):
    mock_run = mocker.patch("subprocess.run")
    mock_run.side_effect = [
        mocker.MagicMock(returncode=0, stdout="deps ok", stderr=""),
        mocker.MagicMock(returncode=1, stdout="", stderr="error: generation failed"),
    ]
    response = mock_client.post("/dbt/docs/generate")
    assert response.status_code == 500


# ---------------------------------------------------------------------------
# /dbt/build
# ---------------------------------------------------------------------------

def test_dbt_build_no_select_builds_all(mock_client, mock_dbt_build_happy_path, mocker):
    """Empty payload is valid — runs dbt build on all models."""
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={})
    assert response.status_code == 200
    assert response.json()["select"] == "all"


def test_dbt_build_with_select(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 200
    assert response.json()["select"] == ["model_a"]


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


def test_dbt_build_select_invalid_tokens(mock_client):
    response = mock_client.post("/dbt/build", json={"select": ["model a"]})
    assert response.status_code == 400


def test_dbt_build_exclude_invalid_tokens(mock_client):
    payload = {"select": ["model_a"], "exclude": ["model#tag"]}
    response = mock_client.post("/dbt/build", json=payload)
    assert response.status_code == 400


def test_dbt_build_lock_held(mock_client, mocker):
    """Returns 409 when a build is already in progress."""
    mocker.patch("dbt_runner.app.is_idle", return_value=False)
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "dbt_build_in_progress"


def test_dbt_build_succeeds(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=0, stdout="ok", stderr="")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_dbt_build_fails(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = \
        mocker.MagicMock(returncode=1, stdout="error output", stderr="error")
    response = mock_client.post("/dbt/build", json={"select": ["model_a"]})
    assert response.status_code == 500
