"""Unit tests for archiver/processors/cleanup_queue.py"""
from unittest.mock import MagicMock

from archiver.processors.cleanup_queue import cleanup_queue, run_cleanup_queue

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_db_cursor(mocker, candidates=None):
    """
    Patch db_cursor so the SELECT returns `candidates` from fetchall.
    Returns (mock_db_cursor, mock_cursor).
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = candidates or []
    mock_cm = MagicMock()
    mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cm.__exit__ = MagicMock(return_value=False)
    mock_db_cursor = mocker.patch(
        "archiver.processors.cleanup_queue.db_cursor", return_value=mock_cm
    )
    return mock_db_cursor, mock_cursor


# ---------------------------------------------------------------------------
# cleanup_queue (targeted delete)
# ---------------------------------------------------------------------------

class TestCleanupQueue:
    def test_empty_list_returns_empty(self):
        assert cleanup_queue([]) == []

    def test_deleted_ids_marked_true(self, mocker):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1,), (2,)]
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cm.__exit__ = MagicMock(return_value=False)
        mocker.patch("archiver.processors.cleanup_queue.db_cursor", return_value=mock_cm)

        results = cleanup_queue([1, 2])
        assert all(r["deleted"] is True for r in results)

    def test_ids_not_in_db_result_marked_false(self, mocker):
        # DB RETURNING only returns rows that actually matched the DELETE
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1,)]  # only 1 deleted; 2 missing
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cm.__exit__ = MagicMock(return_value=False)
        mocker.patch("archiver.processors.cleanup_queue.db_cursor", return_value=mock_cm)

        results = cleanup_queue([1, 2])
        deleted = {r["artifact_id"]: r["deleted"] for r in results}
        assert deleted[1] is True
        assert deleted[2] is False

    def test_not_deleted_reason_is_descriptive(self, mocker):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cm.__exit__ = MagicMock(return_value=False)
        mocker.patch("archiver.processors.cleanup_queue.db_cursor", return_value=mock_cm)

        results = cleanup_queue([99])
        assert results[0]["deleted"] is False
        assert results[0]["reason"] is not None

    def test_db_error_returns_all_failed(self, mocker):
        mocker.patch(
            "archiver.processors.cleanup_queue.db_cursor",
            side_effect=Exception("connection refused"),
        )
        results = cleanup_queue([1, 2, 3])
        assert all(r["deleted"] is False for r in results)
        assert all("db_error" in r["reason"] for r in results)

    def test_artifact_ids_preserved_in_output(self, mocker):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(7,), (8,)]
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cm.__exit__ = MagicMock(return_value=False)
        mocker.patch("archiver.processors.cleanup_queue.db_cursor", return_value=mock_cm)

        results = cleanup_queue([7, 8])
        ids = {r["artifact_id"] for r in results}
        assert ids == {7, 8}

    def test_output_order_matches_input(self, mocker):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(3,), (1,), (2,)]
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cm.__exit__ = MagicMock(return_value=False)
        mocker.patch("archiver.processors.cleanup_queue.db_cursor", return_value=mock_cm)

        results = cleanup_queue([1, 2, 3])
        assert [r["artifact_id"] for r in results] == [1, 2, 3]


# ---------------------------------------------------------------------------
# run_cleanup_queue (full sweep)
# ---------------------------------------------------------------------------

class TestRunCleanupQueue:
    def test_no_candidates_returns_zeros(self, mocker):
        _patch_db_cursor(mocker, candidates=[])
        result = run_cleanup_queue()
        assert result == {"total": 0, "deleted": 0, "failed": 0, "results": []}

    def test_no_candidates_skips_delete(self, mocker):
        mock_db, _ = _patch_db_cursor(mocker, candidates=[])
        mock_cleanup = mocker.patch("archiver.processors.cleanup_queue.cleanup_queue")
        run_cleanup_queue()
        mock_cleanup.assert_not_called()

    def test_candidates_forwarded_to_cleanup_queue(self, mocker):
        # Rows returned: (artifact_id, minio_path, artifact_type, status)
        candidates = [(10, "s3://b/k1", "results_page", "complete"),
                      (11, "s3://b/k2", "detail_page", "skip")]
        _patch_db_cursor(mocker, candidates=candidates)
        mock_cleanup = mocker.patch(
            "archiver.processors.cleanup_queue.cleanup_queue",
            return_value=[
                {"artifact_id": 10, "deleted": True, "reason": None},
                {"artifact_id": 11, "deleted": True, "reason": None},
            ],
        )
        run_cleanup_queue()
        mock_cleanup.assert_called_once_with([10, 11])

    def test_returns_correct_counts_all_deleted(self, mocker):
        candidates = [(1, "s3://b/k", "results_page", "complete")]
        _patch_db_cursor(mocker, candidates=candidates)
        mocker.patch(
            "archiver.processors.cleanup_queue.cleanup_queue",
            return_value=[{"artifact_id": 1, "deleted": True, "reason": None}],
        )
        result = run_cleanup_queue()
        assert result["total"] == 1
        assert result["deleted"] == 1
        assert result["failed"] == 0

    def test_returns_correct_counts_partial_failure(self, mocker):
        candidates = [(1, "s3://b/k1", "results_page", "complete"),
                      (2, "s3://b/k2", "detail_page", "skip")]
        _patch_db_cursor(mocker, candidates=candidates)
        mocker.patch(
            "archiver.processors.cleanup_queue.cleanup_queue",
            return_value=[
                {"artifact_id": 1, "deleted": True, "reason": None},
                {
                    "artifact_id": 2, 
                    "deleted": False, 
                    "reason": "not deleted — row missing or status not in (complete, skip)"
                },
            ],
        )
        result = run_cleanup_queue()
        assert result["total"] == 2
        assert result["deleted"] == 1
        assert result["failed"] == 1

    def test_db_error_on_fetch_returns_error_key(self, mocker):
        mocker.patch(
            "archiver.processors.cleanup_queue.db_cursor",
            side_effect=Exception("db gone"),
        )
        result = run_cleanup_queue()
        assert result["total"] == 0
        assert "error" in result
