"""Unit tests for archiver/processors/cleanup_parquet.py"""
from unittest.mock import MagicMock

from archiver.processors.cleanup_parquet import cleanup_parquet, run_cleanup_parquet

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_db_cursor(mocker, months):
    """
    Mock db_cursor so the first call (SELECT) returns `months` from fetchall,
    and the second call (UPDATE) is a no-op. Both share the same mock cursor.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = months
    mock_cm = MagicMock()
    mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cm.__exit__ = MagicMock(return_value=False)
    mock_db_cursor = mocker.patch(
        "archiver.processors.cleanup_parquet.db_cursor", return_value=mock_cm
    )
    return mock_db_cursor, mock_cursor


class TestCleanupParquet:
    def test_empty_list(self, mock_s3fs):
        result = cleanup_parquet([])
        assert result == []
        mock_s3fs.rm.assert_not_called()

    def test_single_path_deleted(self, mock_s3fs):
        result = cleanup_parquet(["bronze/html/year=2026/month=01/"])
        expected = [{
            "path": "bronze/html/year=2026/month=01/",
            "deleted": True,
            "reason": None,
        }]
        assert result == expected
        mock_s3fs.rm.assert_called_once_with(
            "bronze/html/year=2026/month=01/", recursive=True
        )

    def test_multiple_paths_all_deleted(self, mock_s3fs):
        paths = [
            "bronze/html/year=2025/month=11/",
            "bronze/html/year=2025/month=12/",
        ]
        results = cleanup_parquet(paths)
        assert len(results) == 2
        assert all(r["deleted"] is True for r in results)
        assert all(r["reason"] is None for r in results)

    def test_already_deleted_treated_as_success(self, mock_s3fs):
        mock_s3fs.rm.side_effect = FileNotFoundError("no such path")
        result = cleanup_parquet(["bronze/html/year=2025/month=10/"])
        assert result[0]["deleted"] is True
        assert result[0]["reason"] == "already_deleted"

    def test_unexpected_error_is_failure(self, mock_s3fs):
        mock_s3fs.rm.side_effect = Exception("connection timeout")
        result = cleanup_parquet(["bronze/html/year=2025/month=09/"])
        assert result[0]["deleted"] is False
        assert "connection timeout" in result[0]["reason"]

    def test_mixed_results(self, mock_s3fs):
        mock_s3fs.rm.side_effect = [
            None,                           # first path: success
            FileNotFoundError("gone"),      # second path: already deleted
            Exception("timeout"),           # third path: failure
        ]
        paths = [
            "bronze/html/year=2025/month=11/",
            "bronze/html/year=2025/month=10/",
            "bronze/html/year=2025/month=09/",
        ]
        results = cleanup_parquet(paths)
        assert results[0] == {"path": paths[0], "deleted": True, "reason": None}
        assert results[1]["deleted"] is True
        assert results[1]["reason"] == "already_deleted"
        assert results[2]["deleted"] is False
        assert "timeout" in results[2]["reason"]

    def test_rm_called_with_recursive(self, mock_s3fs):
        cleanup_parquet(["bronze/html/year=2026/month=03/"])
        _, kwargs = mock_s3fs.rm.call_args
        assert kwargs.get("recursive") is True

    def test_path_preserved_in_result(self, mock_s3fs):
        path = "bronze/html/year=2026/month=02/"
        result = cleanup_parquet([path])
        assert result[0]["path"] == path


# ---------------------------------------------------------------------------
# run_cleanup_parquet
# ---------------------------------------------------------------------------

class TestRunCleanupParquet:
    def test_no_expired_months_returns_zeros(self, mocker, mock_s3fs):
        _patch_db_cursor(mocker, months=[])
        result = run_cleanup_parquet()
        assert result == {"total": 0, "deleted": 0, "failed": 0, "results": []}
        mock_s3fs.rm.assert_not_called()

    def test_no_expired_months_skips_mark_deleted(self, mocker, mock_s3fs):
        mock_db_cursor, _ = _patch_db_cursor(mocker, months=[])
        run_cleanup_parquet()
        # db_cursor called once (SELECT), never a second time (UPDATE)
        assert mock_db_cursor.call_count == 1

    def test_paths_built_from_expired_months(self, mocker, mock_s3fs):
        _patch_db_cursor(mocker, months=[(2025, 11), (2025, 12)])
        run_cleanup_parquet()
        deleted_paths = [call.args[0] for call in mock_s3fs.rm.call_args_list]
        assert "bronze/html/year=2025/month=11/" in deleted_paths
        assert "bronze/html/year=2025/month=12/" in deleted_paths

    def test_mark_deleted_called_after_minio_cleanup(self, mocker, mock_s3fs):
        mock_db_cursor, _ = _patch_db_cursor(mocker, months=[(2026, 1)])
        run_cleanup_parquet()
        # db_cursor called twice: SELECT then UPDATE
        assert mock_db_cursor.call_count == 2

    def test_returns_correct_counts(self, mocker, mock_s3fs):
        _patch_db_cursor(mocker, months=[(2025, 10), (2025, 11)])
        result = run_cleanup_parquet()
        assert result["total"] == 2
        assert result["deleted"] == 2
        assert result["failed"] == 0

    def test_partial_minio_failure_reflected_in_counts(self, mocker, mock_s3fs):
        _patch_db_cursor(mocker, months=[(2025, 10), (2025, 11)])
        mock_s3fs.rm.side_effect = [None, Exception("timeout")]
        result = run_cleanup_parquet()
        assert result["total"] == 2
        assert result["deleted"] == 1
        assert result["failed"] == 1

    def test_mark_deleted_still_called_on_partial_minio_failure(self, mocker, mock_s3fs):
        """DB mark-deleted runs regardless of MinIO failures."""
        mock_db_cursor, _ = _patch_db_cursor(mocker, months=[(2025, 10)])
        mock_s3fs.rm.side_effect = Exception("timeout")
        run_cleanup_parquet()
        assert mock_db_cursor.call_count == 2

    def test_results_list_included_in_response(self, mocker, mock_s3fs):
        _patch_db_cursor(mocker, months=[(2026, 3)])
        result = run_cleanup_parquet()
        assert len(result["results"]) == 1
        assert result["results"][0]["path"] == "bronze/html/year=2026/month=3/"
