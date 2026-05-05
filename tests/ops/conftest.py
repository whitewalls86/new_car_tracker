import pytest
from fastapi.testclient import TestClient

from ops.app import app


@pytest.fixture
def mock_client():
    return TestClient(app)


@pytest.fixture
def mock_router_logger_warning(mocker):
    """Mock dbt_runner.app logger.warning"""
    return mocker.patch("ops.routers.deploy.logger.warning")


@pytest.fixture
def mock_intent_status(mocker):
    intent_status = {
        "intent": "pending",
        "requested_at": None,
        "requested_by": None,
    }
    return mocker.patch(
        "ops.routers.deploy._intent_status", return_value=intent_status
    )

@pytest.fixture
def mock_set_intent(mocker):
    return mocker.patch("ops.routers.deploy._set_intent", return_value="ok")

@pytest.fixture
def mock_intent_release(mocker):
    return mocker.patch("ops.routers.deploy._intent_release", return_value=True)
