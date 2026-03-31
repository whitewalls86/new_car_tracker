import pytest
from datetime import datetime


@pytest.fixture
def mock_logger_error(mocker):
    """Mock ops logger.error (adjust path if ops has multiple logging modules)"""
    return mocker.patch("dbt_runner.app.logger.error")

@pytest.fixture
def mock_logger_warning(mocker):
    """Mock ops logger.error (adjust path if ops has multiple logging modules)"""
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
        "returncode": 200,
    }
