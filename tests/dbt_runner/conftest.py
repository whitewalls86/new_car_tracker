import pytest


@pytest.fixture
def mock_logger_error(mocker):
    """Mock ops logger.error (adjust path if ops has multiple logging modules)"""
    return mocker.patch("dbt_runner.app.logger.error")