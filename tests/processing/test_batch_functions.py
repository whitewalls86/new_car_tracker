"""Unit tests for processing/routers/batch.py internal functions.

Covers the branches that test_batch_router.py skips by mocking _process_artifact:
  - _process_results_page: MinIO failure, parse failure, write failure, success
  - _process_detail_page:  MinIO failure, block page, unlisted, active, write failure
  - _process_artifact:     dispatch by type, unknown type
"""
from unittest.mock import MagicMock

from processing.routers.batch import (
    _process_artifact,
    _process_detail_page,
    _process_results_page,
)


def _srp_artifact(artifact_id=1):
    return {
        "artifact_id": artifact_id,
        "minio_path": f"bronze/srp_{artifact_id}.html.zst",
        "artifact_type": "results_page",
        "listing_id": None,
        "run_id": "run-0000-0000-0000-000000000001",
        "fetched_at": "2026-04-20T12:00:00",
        "search_key": "honda_accord",
    }


def _detail_artifact(artifact_id=2, listing_id="aaaa-0000-0000-0000-000000000001"):
    return {
        "artifact_id": artifact_id,
        "minio_path": f"bronze/detail_{artifact_id}.html.zst",
        "artifact_type": "detail_page",
        "listing_id": listing_id,
        "run_id": "run-0000-0000-0000-000000000001",
        "fetched_at": "2026-04-20T12:00:00",
    }


# ---------------------------------------------------------------------------
# _process_results_page
# ---------------------------------------------------------------------------

class TestProcessResultsPage:
    def test_minio_read_failure_returns_retry(self, mocker):
        mocker.patch(
            "processing.routers.batch._read_artifact_html",
            side_effect=Exception("connection refused"),
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_results_page(_srp_artifact())

        assert result["status"] == "retry"

    def test_minio_read_failure_sets_retry_status(self, mocker):
        mocker.patch(
            "processing.routers.batch._read_artifact_html",
            side_effect=Exception("bucket missing"),
        )
        mock_set = mocker.patch("processing.routers.batch._set_status")

        _process_results_page(_srp_artifact())

        mock_set.assert_called_once_with(mocker.ANY, "retry")

    def test_parse_failure_returns_retry(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_results_page_html_v3",
            side_effect=Exception("parse error"),
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_results_page(_srp_artifact())

        assert result["status"] == "retry"

    def test_write_failure_returns_retry(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_results_page_html_v3",
            return_value=([{"listing_id": "x"}], {}),
        )
        mocker.patch(
            "processing.routers.batch.write_srp_observations",
            side_effect=Exception("DB down"),
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_results_page(_srp_artifact())

        assert result["status"] == "retry"

    def test_success_returns_complete_with_srp_fields(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_results_page_html_v3",
            return_value=([{"listing_id": "x"}, {"listing_id": "y"}], {}),
        )
        mocker.patch(
            "processing.routers.batch.write_srp_observations",
            return_value={"silver_written": 2},
        )
        mock_set = mocker.patch("processing.routers.batch._set_status")

        result = _process_results_page(_srp_artifact())

        assert result["status"] == "complete"
        assert result["artifact_type"] == "results_page"
        assert result["listings_parsed"] == 2
        mock_set.assert_called_once_with(mocker.ANY, "complete")

    def test_empty_parse_result_still_completes(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_results_page_html_v3",
            return_value=([], {}),
        )
        mocker.patch(
            "processing.routers.batch.write_srp_observations",
            return_value={"silver_written": 0},
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_results_page(_srp_artifact())

        assert result["status"] == "complete"
        assert result["listings_parsed"] == 0


# ---------------------------------------------------------------------------
# _process_detail_page
# ---------------------------------------------------------------------------

class TestProcessDetailPage:
    def test_minio_read_failure_returns_retry(self, mocker):
        mocker.patch(
            "processing.routers.batch._read_artifact_html",
            side_effect=Exception("timeout"),
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_detail_page(_detail_artifact())

        assert result["status"] == "retry"


    def test_parse_failure_returns_retry(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_detail_page_html_v1",
            side_effect=Exception("parse error"),
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_detail_page(_detail_artifact())

        assert result["status"] == "retry"

    def test_active_listing_calls_write_detail_active(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_detail_page_html_v1",
            return_value=(
                {"listing_id": "aaaa-0000-0000-0000-000000000001", "listing_state": "active"},
                [],
                {},
            ),
        )
        mock_active = mocker.patch(
            "processing.routers.batch.write_detail_active",
            return_value={"silver_written": 1},
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_detail_page(_detail_artifact())

        assert result["status"] == "complete"
        assert result["listing_state"] == "active"
        mock_active.assert_called_once()

    def test_unlisted_calls_write_detail_unlisted(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_detail_page_html_v1",
            return_value=(
                {"listing_id": "aaaa-0000-0000-0000-000000000001", "listing_state": "unlisted"},
                [],
                {},
            ),
        )
        mock_unlisted = mocker.patch(
            "processing.routers.batch.write_detail_unlisted",
            return_value={"silver_written": 1},
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_detail_page(_detail_artifact())

        assert result["status"] == "complete"
        assert result["listing_state"] == "unlisted"
        mock_unlisted.assert_called_once()

    def test_write_failure_returns_retry(self, mocker):
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_detail_page_html_v1",
            return_value=(
                {"listing_id": "aaaa-0000-0000-0000-000000000001", "listing_state": "active"},
                [],
                {},
            ),
        )
        mocker.patch(
            "processing.routers.batch.write_detail_active",
            side_effect=Exception("DB error"),
        )
        mocker.patch("processing.routers.batch._set_status")

        result = _process_detail_page(_detail_artifact())

        assert result["status"] == "retry"

    def test_listing_id_resolved_from_artifact_when_missing_from_parse(self, mocker):
        """When parser returns no listing_id, falls back to artifact.listing_id."""
        mocker.patch("processing.routers.batch._read_artifact_html", return_value="<html/>")
        mocker.patch(
            "processing.routers.batch.parse_cars_detail_page_html_v1",
            return_value=({"listing_id": None, "listing_state": "active"}, [], {}),
        )
        mocker.patch(
            "processing.routers.batch.write_detail_active",
            return_value={"silver_written": 1},
        )
        mocker.patch("processing.routers.batch._set_status")

        artifact = _detail_artifact(listing_id="bbbb-0000-0000-0000-000000000002")
        result = _process_detail_page(artifact)

        assert result["listing_id"] == "bbbb-0000-0000-0000-000000000002"


# ---------------------------------------------------------------------------
# _set_status / _read_artifact_html
# ---------------------------------------------------------------------------

class TestSetStatus:
    def _fake_cursor_ctx(self, mocker):
        from contextlib import contextmanager
        cursor = MagicMock()

        @contextmanager
        def _ctx(**kw):
            yield cursor

        mocker.patch("processing.routers.batch.db_cursor", side_effect=_ctx)
        return cursor

    def test_set_status_executes_two_queries(self, mocker):
        from processing.routers.batch import _set_status

        cursor = self._fake_cursor_ctx(mocker)
        _set_status(_srp_artifact(), "complete")
        assert cursor.execute.call_count == 2

    def test_set_status_passes_status_to_first_query(self, mocker):
        from processing.routers.batch import _set_status

        cursor = self._fake_cursor_ctx(mocker)
        _set_status(_srp_artifact(artifact_id=7), "retry")
        first_call_params = cursor.execute.call_args_list[0][0][1]
        assert first_call_params["status"] == "retry"
        assert first_call_params["artifact_id"] == 7


class TestReadArtifactHtml:
    def test_decodes_minio_bytes_as_utf8(self, mocker):
        from processing.routers.batch import _read_artifact_html

        mocker.patch(
            "processing.routers.batch.read_html",
            return_value=b"<html>hello</html>",
        )
        result = _read_artifact_html(_srp_artifact())
        assert result == "<html>hello</html>"

    def test_passes_minio_path_to_read_html(self, mocker):
        from processing.routers.batch import _read_artifact_html

        mock_read = mocker.patch(
            "processing.routers.batch.read_html",
            return_value=b"",
        )
        artifact = _srp_artifact(artifact_id=99)
        _read_artifact_html(artifact)
        mock_read.assert_called_once_with(artifact["minio_path"])


# ---------------------------------------------------------------------------
# _process_artifact
# ---------------------------------------------------------------------------

class TestProcessArtifact:
    def test_dispatches_results_page_to_srp_processor(self, mocker):
        mock_srp = mocker.patch(
            "processing.routers.batch._process_results_page",
            return_value={"status": "complete", "artifact_type": "results_page"},
        )

        _process_artifact(_srp_artifact())

        mock_srp.assert_called_once()

    def test_dispatches_detail_page_to_detail_processor(self, mocker):
        mock_detail = mocker.patch(
            "processing.routers.batch._process_detail_page",
            return_value={"status": "complete", "artifact_type": "detail_page"},
        )

        _process_artifact(_detail_artifact())

        mock_detail.assert_called_once()

    def test_unknown_type_returns_skip(self, mocker):
        mocker.patch("processing.routers.batch._set_status")

        artifact = _srp_artifact()
        artifact["artifact_type"] = "carousel_page"

        result = _process_artifact(artifact)

        assert result["status"] == "skip"
        assert "carousel_page" in result["reason"]
