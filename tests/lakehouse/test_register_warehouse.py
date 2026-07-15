"""
Plan 112 Gate A2: unit tests for scripts/register_lakehouse_warehouse.py's
bootstrap + warehouse-registration idempotency logic. HTTP is fully mocked --
no live Lakekeeper required.
"""
import io
import json
import urllib.error
from unittest.mock import patch

import pytest

import scripts.register_lakehouse_warehouse as module
from scripts.register_lakehouse_warehouse import (
    _management_base_uri,
    ensure_bootstrapped,
    register_warehouse,
    server_bootstrapped,
    warehouse_exists,
)
from shared.iceberg_catalog import WAREHOUSE_NAME


def _urlopen_response(status, body):
    class _Resp(io.BytesIO):
        def __init__(self):
            super().__init__(json.dumps(body).encode())
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    return _Resp()


class TestManagementBaseUri:
    def test_strips_catalog_suffix(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        assert _management_base_uri() == "http://lakekeeper:8181"


class TestServerBootstrapped:
    def test_true_when_info_reports_bootstrapped(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        response = _urlopen_response(200, {"bootstrapped": True})
        with patch("urllib.request.urlopen", return_value=response):
            assert server_bootstrapped() is True

    def test_false_when_info_reports_not_bootstrapped(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        response = _urlopen_response(200, {"bootstrapped": False})
        with patch("urllib.request.urlopen", return_value=response):
            assert server_bootstrapped() is False

    def test_raises_on_non_200(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        error = urllib.error.HTTPError("url", 500, "err", {}, io.BytesIO(b"{}"))
        with patch("urllib.request.urlopen", side_effect=error):
            with pytest.raises(RuntimeError):
                server_bootstrapped()


class TestEnsureBootstrapped:
    def test_noop_when_already_bootstrapped(self, monkeypatch, capsys):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        response = _urlopen_response(200, {"bootstrapped": True})
        with patch("urllib.request.urlopen", return_value=response) as mock_urlopen:
            ensure_bootstrapped()
        assert mock_urlopen.call_count == 1  # only the info GET, no bootstrap POST
        assert "already bootstrapped" in capsys.readouterr().out

    def test_bootstraps_when_fresh_server(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        info_response = _urlopen_response(200, {"bootstrapped": False})
        bootstrap_response = _urlopen_response(204, {})
        side_effect = [info_response, bootstrap_response]
        with patch("urllib.request.urlopen", side_effect=side_effect) as mock_urlopen:
            ensure_bootstrapped()
        assert mock_urlopen.call_count == 2

    def test_treats_409_as_success(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        info_response = _urlopen_response(200, {"bootstrapped": False})
        conflict = urllib.error.HTTPError("url", 409, "conflict", {}, io.BytesIO(b"{}"))
        with patch("urllib.request.urlopen", side_effect=[info_response, conflict]):
            ensure_bootstrapped()  # must not raise


class TestWarehouseExists:
    def test_true_when_name_present(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        response = _urlopen_response(200, {"warehouses": [{"name": WAREHOUSE_NAME}]})
        with patch("urllib.request.urlopen", return_value=response):
            assert warehouse_exists(WAREHOUSE_NAME) is True

    def test_false_when_name_absent(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        response = _urlopen_response(200, {"warehouses": [{"name": "some_other_warehouse"}]})
        with patch("urllib.request.urlopen", return_value=response):
            assert warehouse_exists(WAREHOUSE_NAME) is False

    def test_raises_on_non_200(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        error = urllib.error.HTTPError("url", 500, "err", {}, io.BytesIO(b"{}"))
        with patch("urllib.request.urlopen", side_effect=error):
            with pytest.raises(RuntimeError):
                warehouse_exists(WAREHOUSE_NAME)


class TestRegisterWarehouse:
    """register_warehouse() always calls ensure_bootstrapped() first --
    stubbed out here so these tests focus purely on the warehouse-creation
    idempotency, matching TestEnsureBootstrapped's own coverage of that step.
    """

    def test_noop_when_already_registered(self, monkeypatch, capsys):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        monkeypatch.setenv("MINIO_ROOT_USER", "cartracker")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")
        monkeypatch.setattr(module, "ensure_bootstrapped", lambda: None)
        response = _urlopen_response(200, {"warehouses": [{"name": WAREHOUSE_NAME}]})
        with patch("urllib.request.urlopen", return_value=response) as mock_urlopen:
            register_warehouse()
        assert mock_urlopen.call_count == 1  # only the GET, no POST
        assert "already registered" in capsys.readouterr().out

    def test_registers_when_absent(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        monkeypatch.setenv("MINIO_ROOT_USER", "cartracker")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")
        monkeypatch.setattr(module, "ensure_bootstrapped", lambda: None)
        get_response = _urlopen_response(200, {"warehouses": []})
        post_response = _urlopen_response(201, {"warehouse-id": "abc123"})
        side_effect = [get_response, post_response]
        with patch("urllib.request.urlopen", side_effect=side_effect) as mock_urlopen:
            register_warehouse()
        assert mock_urlopen.call_count == 2

    def test_treats_409_as_success(self, monkeypatch):
        monkeypatch.setenv("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")
        monkeypatch.setenv("MINIO_ROOT_USER", "cartracker")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")
        monkeypatch.setattr(module, "ensure_bootstrapped", lambda: None)
        get_response = _urlopen_response(200, {"warehouses": []})
        conflict = urllib.error.HTTPError("url", 409, "conflict", {}, io.BytesIO(b"{}"))
        with patch("urllib.request.urlopen", side_effect=[get_response, conflict]):
            register_warehouse()  # must not raise
