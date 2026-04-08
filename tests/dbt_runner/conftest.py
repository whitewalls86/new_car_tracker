from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from dbt_runner.app import app


@pytest.fixture
def mock_logger_error(mocker):
    """Mock shared.db logger.error for database operation error logging"""
    return mocker.patch("shared.db.logger.error")

@pytest.fixture
def mock_app_logger_error(mocker):
    """Mock dbt_runner.app logger.error for app-level error logging"""
    return mocker.patch("dbt_runner.app.logger.error")

@pytest.fixture
def mock_logger_warning(mocker):
    """Mock dbt_runner.app logger.warning"""
    return mocker.patch("dbt_runner.app.logger.warning")


@pytest.fixture
def record_run_defaults():
    return {
        "started_at": datetime(2025, 1, 1, 12, 0, 0),
        "finished_at": datetime(2025, 1, 1, 12, 5, 0),
        "ok": True,
        "intent": "after_srp",
        "select": ["model_a", "model_b"],
        "stdout": "PASS=5 WARN=0 ERROR=0 SKIP=1",
        "returncode": 0,
    }


@pytest.fixture
def mock_client():
    return TestClient(app)


@pytest.fixture
def mock_log_file_not_found(mocker):
    return mocker.patch("builtins.open", side_effect=FileNotFoundError)

@pytest.fixture
def mock_log_permission_error(mocker):
    return mocker.patch("builtins.open", side_effect=PermissionError)

@pytest.fixture
def mock_log_file(mocker):
    fake_lines = [f"line {i}\n" for i in range(300)]
    mock_file = mocker.mock_open()
    mock_file.return_value.__enter__.return_value.readlines.return_value = fake_lines
    return mocker.patch("builtins.open", mock_file)


@pytest.fixture
def mock_dbt_build_happy_path(mocker):
    """Standard mocks for dbt_build tests"""
    return {
        "acquire_lock": mocker.patch(
            "dbt_runner.app._acquire_lock", return_value=True
        ),
        "release_lock": mocker.patch(
            "dbt_runner.app._release_lock", return_value=True
        ),
        "record_run": mocker.patch(
            "dbt_runner.app._record_run", return_value=True
        ),
        "load_intents": mocker.patch(
            "dbt_runner.app._load_intents",
            return_value={"after_srp": ["model_a"]},
        ),
        "subprocess_run": mocker.patch("subprocess.run"),
    }
