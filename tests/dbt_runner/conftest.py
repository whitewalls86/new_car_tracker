import pytest
from fastapi.testclient import TestClient

from dbt_runner.app import app


@pytest.fixture
def mock_app_logger_error(mocker):
    """Mock dbt_runner.app logger.error for app-level error logging"""
    return mocker.patch("dbt_runner.app.logger.error")


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
    """Standard mocks for dbt_build tests — patches is_idle and subprocess.run."""
    mocker.patch("dbt_runner.app.is_idle", return_value=True)
    return {
        "subprocess_run": mocker.patch("subprocess.run"),
    }
