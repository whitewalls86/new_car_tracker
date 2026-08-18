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
def mock_dbt_build_happy_path(mocker):
    """Standard mocks for dbt_build tests — patches is_idle and subprocess.run."""
    mocker.patch("dbt_runner.app.is_idle", return_value=True)
    return {
        "subprocess_run": mocker.patch("subprocess.run"),
        "snapshot_refresh": mocker.patch(
            "dbt_runner.app.snapshot_manager.refresh",
            return_value={
                "ok": True,
                "status": "ok",
                "attempted_at": "2026-08-18T18:00:00Z",
                "last_success_at": "2026-08-18T18:00:00Z",
                "duration_seconds": 0.1,
            },
        ),
    }
