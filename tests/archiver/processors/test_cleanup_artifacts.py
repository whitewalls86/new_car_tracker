"""Unit tests for archiver/processors/cleanup_artifacts.py"""
from archiver.processors.cleanup_artifacts import cleanup_artifacts


class TestCleanupArtifacts:
    def test_empty_list(self):
        assert cleanup_artifacts([]) == []

    def test_success(self, mocker):
        mocker.patch("os.remove")
        result = cleanup_artifacts([{"artifact_id": 1, "filepath": "/data/file.html"}])
        assert result == [{"artifact_id": 1, "deleted": True, "reason": None}]

    def test_file_not_found_treated_as_success(self, mocker):
        mocker.patch("os.remove", side_effect=FileNotFoundError("gone"))
        result = cleanup_artifacts([{"artifact_id": 2, "filepath": "/data/missing.html"}])
        assert result[0]["deleted"] is True
        assert "not found" in result[0]["reason"].lower()

    def test_permission_error_is_failure(self, mocker):
        mocker.patch("os.remove", side_effect=PermissionError("denied"))
        result = cleanup_artifacts([{"artifact_id": 3, "filepath": "/data/locked.html"}])
        assert result[0]["deleted"] is False
        assert "PermissionError" in result[0]["reason"]
        assert "denied" in result[0]["reason"]

    def test_no_filepath_key(self):
        result = cleanup_artifacts([{"artifact_id": 5}])
        assert result[0]["deleted"] is False
        assert result[0]["reason"] == "no filepath provided"

    def test_empty_filepath(self):
        result = cleanup_artifacts([{"artifact_id": 6, "filepath": ""}])
        assert result[0]["deleted"] is False
        assert result[0]["reason"] == "no filepath provided"

    def test_none_filepath(self):
        result = cleanup_artifacts([{"artifact_id": 7, "filepath": None}])
        assert result[0]["deleted"] is False
        assert result[0]["reason"] == "no filepath provided"

    def test_mixed_list(self, mocker):
        mocker.patch("os.remove", side_effect=[
            None,
            FileNotFoundError("gone"),
            PermissionError("no"),
        ])
        items = [
            {"artifact_id": 10, "filepath": "/a.html"},
            {"artifact_id": 11, "filepath": "/b.html"},
            {"artifact_id": 12, "filepath": "/c.html"},
        ]
        results = cleanup_artifacts(items)
        assert results[0] == {"artifact_id": 10, "deleted": True, "reason": None}
        assert results[1]["deleted"] is True
        assert results[2]["deleted"] is False

    def test_artifact_id_preserved(self, mocker):
        mocker.patch("os.remove")
        result = cleanup_artifacts([{"artifact_id": 999, "filepath": "/x.html"}])
        assert result[0]["artifact_id"] == 999

    def test_os_remove_called_with_correct_path(self, mocker):
        mock_remove = mocker.patch("os.remove")
        cleanup_artifacts([{"artifact_id": 1, "filepath": "/data/specific.html"}])
        mock_remove.assert_called_once_with("/data/specific.html")

    def test_multiple_successes_all_returned(self, mocker):
        mocker.patch("os.remove")
        items = [{"artifact_id": i, "filepath": f"/file{i}.html"} for i in range(5)]
        results = cleanup_artifacts(items)
        assert len(results) == 5
        assert all(r["deleted"] is True for r in results)
