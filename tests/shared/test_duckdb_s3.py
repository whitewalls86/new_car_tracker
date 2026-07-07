"""
Unit tests for shared/duckdb_s3.py

duckdb.connect is patched so no real MinIO/network access is needed.
"""
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_duckdb_connect(mocker):
    mock_con = MagicMock()
    mocker.patch("duckdb.connect", return_value=mock_con)
    return mock_con


class TestNormalizeEndpoint:
    def test_strips_http_scheme(self):
        from shared.duckdb_s3 import _normalize_endpoint
        assert _normalize_endpoint("http://minio:9000") == "minio:9000"

    def test_strips_https_scheme(self):
        from shared.duckdb_s3 import _normalize_endpoint
        assert _normalize_endpoint("https://minio:9000") == "minio:9000"

    def test_leaves_bare_host_port_unchanged(self):
        from shared.duckdb_s3 import _normalize_endpoint
        assert _normalize_endpoint("minio:9000") == "minio:9000"


class TestGetDuckdbS3Connection:
    def test_loads_httpfs(self, mock_duckdb_connect):
        from shared.duckdb_s3 import get_duckdb_s3_connection
        get_duckdb_s3_connection()
        calls = [c.args[0] for c in mock_duckdb_connect.execute.call_args_list]
        assert "INSTALL httpfs" in calls
        assert "LOAD httpfs" in calls

    def test_endpoint_is_normalized_and_bound_as_parameter(self, mocker, mock_duckdb_connect):
        mocker.patch("shared.duckdb_s3.ENDPOINT", "http://minio:9000")
        from shared.duckdb_s3 import get_duckdb_s3_connection
        get_duckdb_s3_connection()
        endpoint_call = next(
            c for c in mock_duckdb_connect.execute.call_args_list
            if c.args[0] == "SET s3_endpoint=?"
        )
        assert endpoint_call.args[1] == ["minio:9000"]

    def test_credentials_are_bound_not_interpolated(self, mocker, mock_duckdb_connect):
        mocker.patch("shared.duckdb_s3.ACCESS", "cartracker")
        mocker.patch("shared.duckdb_s3.SECRET", "o'brien-secret")
        from shared.duckdb_s3 import get_duckdb_s3_connection
        get_duckdb_s3_connection()

        access_call = next(
            c for c in mock_duckdb_connect.execute.call_args_list
            if c.args[0] == "SET s3_access_key_id=?"
        )
        secret_call = next(
            c for c in mock_duckdb_connect.execute.call_args_list
            if c.args[0] == "SET s3_secret_access_key=?"
        )
        assert access_call.args[1] == ["cartracker"]
        assert secret_call.args[1] == ["o'brien-secret"]
        # No SQL statement should ever contain the raw secret value.
        for c in mock_duckdb_connect.execute.call_args_list:
            assert "o'brien-secret" not in c.args[0]

    def test_sets_path_style_and_disables_ssl(self, mock_duckdb_connect):
        from shared.duckdb_s3 import get_duckdb_s3_connection
        get_duckdb_s3_connection()
        calls = {c.args[0]: c.args[1] for c in mock_duckdb_connect.execute.call_args_list
                 if len(c.args) > 1}
        assert calls["SET s3_use_ssl=?"] == [False]
        assert calls["SET s3_url_style=?"] == ["path"]

    def test_returns_the_connection(self, mock_duckdb_connect):
        from shared.duckdb_s3 import get_duckdb_s3_connection
        assert get_duckdb_s3_connection() is mock_duckdb_connect
