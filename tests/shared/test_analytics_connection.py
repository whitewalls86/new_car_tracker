from pathlib import Path

import pytest

from shared.analytics_connection import get_analytics_connection


def test_uses_default_path_and_read_only_mode(monkeypatch, mocker):
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    connect = mocker.patch("duckdb.connect", return_value=mocker.MagicMock())

    get_analytics_connection()

    connect.assert_called_once_with("/data/analytics/analytics.duckdb", read_only=True)


def test_uses_environment_path(monkeypatch, mocker):
    monkeypatch.setenv("DUCKDB_PATH", "/tmp/environment.duckdb")
    connect = mocker.patch("duckdb.connect", return_value=mocker.MagicMock())

    get_analytics_connection()

    connect.assert_called_once_with("/tmp/environment.duckdb", read_only=True)


def test_explicit_path_overrides_environment(monkeypatch, mocker):
    monkeypatch.setenv("DUCKDB_PATH", "/tmp/environment.duckdb")
    connect = mocker.patch("duckdb.connect", return_value=mocker.MagicMock())

    get_analytics_connection(Path("explicit.duckdb"))

    connect.assert_called_once_with("explicit.duckdb", read_only=True)


def test_rejects_unknown_backend(monkeypatch):
    monkeypatch.setenv("ANALYTICS_BACKEND", "iceberg")

    with pytest.raises(ValueError, match="Unsupported analytics backend: iceberg"):
        get_analytics_connection()


def test_propagates_connection_errors(mocker):
    connect = mocker.patch("duckdb.connect", side_effect=RuntimeError("locked"))

    with pytest.raises(RuntimeError, match="locked"):
        get_analytics_connection(path="analytics.duckdb", backend="duckdb")

    connect.assert_called_once_with("analytics.duckdb", read_only=True)
