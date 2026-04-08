"""Unit tests for archiver/processors/cleanup_parquet.py"""
from processors.cleanup_parquet import cleanup_parquet


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
