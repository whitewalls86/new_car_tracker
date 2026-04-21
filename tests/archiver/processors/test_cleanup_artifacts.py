"""Unit tests for archiver/processors/cleanup_artifacts.py"""
from unittest.mock import MagicMock

from archiver.processors.cleanup_artifacts import cleanup_artifacts, run_cleanup_artifacts

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_db_cursor(mocker, candidates):
    """
    Mock db_cursor so the first call (SELECT) returns `candidates` from fetchall,
    and the second call (UPDATE) is a no-op. Both share the same mock cursor.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = candidates
    mock_cm = MagicMock()
    mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cm.__exit__ = MagicMock(return_value=False)
    mock_db_cursor = mocker.patch(
        "archiver.processors.cleanup_artifacts.db_cursor", return_value=mock_cm
    )
    return mock_db_cursor, mock_cursor


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


# ---------------------------------------------------------------------------
# run_cleanup_artifacts
# ---------------------------------------------------------------------------

class TestRunCleanupArtifacts:
    def _patch_archive(self, mocker, results):
        return mocker.patch(
            "archiver.processors.archive_artifacts.archive_artifacts",
            return_value=results,
        )

    def _patch_cleanup(self, mocker, results):
        return mocker.patch(
            "archiver.processors.cleanup_artifacts.cleanup_artifacts",
            return_value=results,
        )

    def test_no_candidates_returns_zeros(self, mocker):
        _patch_db_cursor(mocker, candidates=[])
        result = run_cleanup_artifacts()
        assert result == {"total": 0, "archived": 0, "deleted": 0, "failed": 0, "results": []}

    def test_no_candidates_skips_archive_and_cleanup(self, mocker):
        _patch_db_cursor(mocker, candidates=[])
        mock_archive = self._patch_archive(mocker, [])
        run_cleanup_artifacts()
        mock_archive.assert_not_called()

    def test_no_candidates_skips_mark_deleted(self, mocker):
        mock_db_cursor, _ = _patch_db_cursor(mocker, candidates=[])
        run_cleanup_artifacts()
        assert mock_db_cursor.call_count == 1

    def test_candidates_passed_to_archive(self, mocker):
        candidates = [(1, "/a.html", None), (2, "/b.html", None)]
        _patch_db_cursor(mocker, candidates=candidates)
        mock_archive = self._patch_archive(mocker, [
            {"artifact_id": 1, "archived": True, "reason": None},
            {"artifact_id": 2, "archived": True, "reason": None},
        ])
        self._patch_cleanup(mocker, [
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 2, "deleted": True, "reason": None},
        ])
        run_cleanup_artifacts()
        mock_archive.assert_called_once_with([
            {"artifact_id": 1, "filepath": "/a.html"},
            {"artifact_id": 2, "filepath": "/b.html"},
        ])

    def test_only_archived_passed_to_cleanup(self, mocker):
        candidates = [(1, "/a.html", None), (2, "/b.html", None)]
        _patch_db_cursor(mocker, candidates=candidates)
        self._patch_archive(mocker, [
            {"artifact_id": 1, "archived": True, "reason": None},
            {"artifact_id": 2, "archived": False, "reason": "parquet_error"},
        ])
        mock_cleanup = self._patch_cleanup(mocker, [
            {"artifact_id": 1, "deleted": True, "reason": None},
        ])
        run_cleanup_artifacts()
        # only artifact 1 (archived=True) goes to cleanup, with filepath reconstructed
        mock_cleanup.assert_called_once_with([
            {"artifact_id": 1, "filepath": "/a.html"},
        ])

    def test_mark_deleted_called_with_deleted_ids(self, mocker):
        candidates = [(1, "/a.html", None), (2, "/b.html", None)]
        mock_db_cursor, mock_cursor = _patch_db_cursor(mocker, candidates=candidates)
        self._patch_archive(mocker, [
            {"artifact_id": 1, "archived": True, "reason": None},
            {"artifact_id": 2, "archived": True, "reason": None},
        ])
        self._patch_cleanup(mocker, [
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 2, "deleted": False, "reason": "PermissionError"},
        ])
        run_cleanup_artifacts()
        # db_cursor called twice: SELECT then UPDATE
        assert mock_db_cursor.call_count == 2

    def test_mark_deleted_skipped_when_none_deleted(self, mocker):
        candidates = [(1, "/a.html", None)]
        mock_db_cursor, _ = _patch_db_cursor(mocker, candidates=candidates)
        self._patch_archive(mocker, [{"artifact_id": 1, "archived": False, "reason": "error"}])
        self._patch_cleanup(mocker, [])
        run_cleanup_artifacts()
        assert mock_db_cursor.call_count == 1

    def test_returns_correct_counts_full_success(self, mocker):
        candidates = [(1, "/a.html", None), (2, "/b.html", None)]
        _patch_db_cursor(mocker, candidates=candidates)
        self._patch_archive(mocker, [
            {"artifact_id": 1, "archived": True, "reason": None},
            {"artifact_id": 2, "archived": True, "reason": None},
        ])
        self._patch_cleanup(mocker, [
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 2, "deleted": True, "reason": None},
        ])
        result = run_cleanup_artifacts()
        assert result["total"] == 2
        assert result["archived"] == 2
        assert result["deleted"] == 2
        assert result["failed"] == 0

    def test_returns_correct_counts_partial_failure(self, mocker):
        candidates = [(1, "/a.html", None), (2, "/b.html", None), (3, "/c.html", None)]
        _patch_db_cursor(mocker, candidates=candidates)
        self._patch_archive(mocker, [
            {"artifact_id": 1, "archived": True, "reason": None},
            {"artifact_id": 2, "archived": False, "reason": "error"},
            {"artifact_id": 3, "archived": True, "reason": None},
        ])
        self._patch_cleanup(mocker, [
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 3, "deleted": False, "reason": "PermissionError"},
        ])
        result = run_cleanup_artifacts()
        assert result["total"] == 3
        assert result["archived"] == 2
        assert result["deleted"] == 1
        assert result["failed"] == 2

    def test_delete_results_included_in_response(self, mocker):
        candidates = [(1, "/a.html", None)]
        _patch_db_cursor(mocker, candidates=candidates)
        self._patch_archive(mocker, [{"artifact_id": 1, "archived": True, "reason": None}])
        delete_results = [{"artifact_id": 1, "deleted": True, "reason": None}]
        self._patch_cleanup(mocker, delete_results)
        result = run_cleanup_artifacts()
        assert result["results"] == delete_results
