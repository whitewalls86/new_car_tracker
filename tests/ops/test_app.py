from ops.app import app
import pytest
from fastapi.responses import RedirectResponse


def test_get_health(mock_client):
    response = mock_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_get_admin(mock_client):
    response = mock_client.get("/admin", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/admin/searches/"


def test_get_base_url(mock_client):
    response = mock_client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/admin/searches/"
