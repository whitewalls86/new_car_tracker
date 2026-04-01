import pytest
from fastapi.testclient import TestClient
from ops.app import app


@pytest.fixture
def mock_client():
    return TestClient(app)