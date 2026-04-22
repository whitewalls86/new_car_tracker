"""Unit tests for archiver/processors/flush_staging_events.py"""
from datetime import datetime, timezone
from unittest.mock import MagicMock

from archiver.processors.flush_staging_events import (
    _TABLE_CONFIGS,
    _flush_one,
    flush_staging_events,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EVENT_AT = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def _make_aq_row(event_id=1):
    """Tuple matching artifacts_queue_events db_columns order."""
    return (
        event_id,                                           # event_id
        100,                                                # artifact_id
        "pending",                                          # status
        _EVENT_AT,                                          # event_at
        "s3://bronze/html/year=2026/month=4/f.html.zst",   # minio_path
        "results_page",                                     # artifact_type
        _EVENT_AT,                                          # fetched_at
        "listing-abc",                                      # listing_id
        "run-xyz",                                          # run_id
    )


def _make_mock_conn(max_pk=10, rows=None, rowcount=1):
    """
    Build a mock psycopg2 connection.

    Three sequential cursor blocks in _flush_one call:
      1. fetchone()  → (max_pk,)
      2. fetchall()  → rows
      3. rowcount    → rowcount (DELETE)
    All three share the same mock cursor object.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (max_pk,)
    mock_cursor.fetchall.return_value = rows or []
    mock_cursor.rowcount = rowcount

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def _aq_config():
    """Return the artifacts_queue_events table config."""
    return next(c for c in _TABLE_CONFIGS if "artifacts_queue" in c["table"])


# ---------------------------------------------------------------------------
# _flush_one — empty table
# ---------------------------------------------------------------------------

class TestFlushOneEmpty:
    def test_returns_zero_when_table_empty(self, mocker):
        mock_conn, mock_cursor = _make_mock_conn(max_pk=None)
        mocker.patch("pyarrow.parquet.write_to_dataset")

        result = _flush_one(_aq_config(), mock_conn, MagicMock())

        assert result["flushed"] == 0
        assert result["error"] is None

    def test_write_not_called_when_table_empty(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=None)
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        mock_write.assert_not_called()

    def test_commit_not_called_when_table_empty(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=None)
        mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        mock_conn.commit.assert_not_called()


# ---------------------------------------------------------------------------
# _flush_one — happy path
# ---------------------------------------------------------------------------

class TestFlushOneSuccess:
    def test_returns_flushed_count(self, mocker):
        rows = [_make_aq_row(1), _make_aq_row(2)]
        mock_conn, mock_cursor = _make_mock_conn(max_pk=2, rows=rows, rowcount=2)
        mock_cursor.rowcount = 2
        mocker.patch("pyarrow.parquet.write_to_dataset")

        result = _flush_one(_aq_config(), mock_conn, MagicMock())

        assert result["flushed"] == 2
        assert result["error"] is None

    def test_table_name_in_result(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mocker.patch("pyarrow.parquet.write_to_dataset")

        result = _flush_one(_aq_config(), mock_conn, MagicMock())

        assert result["table"] == "staging.artifacts_queue_events"

    def test_write_to_dataset_called_once(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        mock_write.assert_called_once()

    def test_write_uses_zstd_compression(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        _, kwargs = mock_write.call_args
        assert kwargs.get("compression") == "zstd"

    def test_write_partitions_by_year_month(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        _, kwargs = mock_write.call_args
        assert kwargs.get("partition_cols") == ["year", "month"]

    def test_write_targets_correct_minio_prefix(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        _, kwargs = mock_write.call_args
        assert "ops/artifacts_queue_events" in kwargs.get("root_path", "")

    def test_commit_called_after_delete(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        mock_conn.commit.assert_called_once()

    def test_year_month_derived_from_event_at(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")

        _flush_one(_aq_config(), mock_conn, MagicMock())

        arrow_table = mock_write.call_args[0][0]
        assert arrow_table.column("year").to_pylist() == [2026]
        assert arrow_table.column("month").to_pylist() == [4]


# ---------------------------------------------------------------------------
# _flush_one — error handling
# ---------------------------------------------------------------------------

class TestFlushOneErrors:
    def test_parquet_write_error_returns_error_key(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("s3 down"))

        result = _flush_one(_aq_config(), mock_conn, MagicMock())

        assert result["flushed"] == 0
        assert result["error"] is not None

    def test_parquet_write_error_does_not_delete(self, mocker):
        mock_conn, mock_cursor = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("s3 down"))

        _flush_one(_aq_config(), mock_conn, MagicMock())

        # Cursor should only be used twice (max_pk + fetchall), never for DELETE
        assert mock_conn.commit.call_count == 0

    def test_db_error_during_max_pk_returns_error(self, mocker):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: mock_cursor
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.side_effect = Exception("db gone")
        mock_conn.cursor.return_value = mock_cursor
        mocker.patch("pyarrow.parquet.write_to_dataset")

        result = _flush_one(_aq_config(), mock_conn, MagicMock())

        assert result["flushed"] == 0
        assert result["error"] is not None

    def test_rollback_called_on_error(self, mocker):
        mock_conn, mock_cursor = _make_mock_conn(max_pk=1, rows=[_make_aq_row()])
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("boom"))

        _flush_one(_aq_config(), mock_conn, MagicMock())

        mock_conn.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# flush_staging_events — orchestration
# ---------------------------------------------------------------------------

class TestFlushStagingEvents:
    def test_all_tables_processed(self, mocker):
        mock_conn, _ = _make_mock_conn(max_pk=None)  # empty tables
        mocker.patch(
            "archiver.processors.flush_staging_events.get_conn", return_value=mock_conn
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_s3fs", return_value=MagicMock()
        )

        result = flush_staging_events()

        assert len(result["tables"]) == len(_TABLE_CONFIGS)

    def test_total_flushed_aggregates_per_table(self, mocker):
        mocker.patch(
            "archiver.processors.flush_staging_events._flush_one",
            side_effect=[
                {"table": f"staging.t{i}", "flushed": 10, "error": None}
                for i in range(len(_TABLE_CONFIGS))
            ],
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_conn", return_value=MagicMock()
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_s3fs", return_value=MagicMock()
        )

        result = flush_staging_events()

        assert result["total_flushed"] == 10 * len(_TABLE_CONFIGS)

    def test_one_table_failure_does_not_abort_others(self, mocker):
        n = len(_TABLE_CONFIGS)
        side_effects = [{"table": f"staging.t{i}", "flushed": 5, "error": None} for i in range(n)]
        side_effects[1] = {"table": "staging.bad", "flushed": 0, "error": "boom"}

        mocker.patch(
            "archiver.processors.flush_staging_events._flush_one",
            side_effect=side_effects,
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_conn", return_value=MagicMock()
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_s3fs", return_value=MagicMock()
        )

        result = flush_staging_events()

        assert len(result["tables"]) == n
        assert result["total_flushed"] == 5 * (n - 1)
        assert result["error"] is not None  # top-level error set when any table fails

    def test_no_error_key_when_all_succeed(self, mocker):
        n = len(_TABLE_CONFIGS)
        mocker.patch(
            "archiver.processors.flush_staging_events._flush_one",
            side_effect=[
                {"table": f"staging.t{i}", "flushed": 0, "error": None}
                for i in range(n)
            ],
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_conn", return_value=MagicMock()
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_s3fs", return_value=MagicMock()
        )

        result = flush_staging_events()

        assert result["error"] is None

    def test_db_connect_failure_returns_error(self, mocker):
        mocker.patch(
            "archiver.processors.flush_staging_events.get_conn",
            side_effect=Exception("connection refused"),
        )

        result = flush_staging_events()

        assert result["total_flushed"] == 0
        assert result["error"] is not None

    def test_conn_always_closed(self, mocker):
        mock_conn = MagicMock()
        mocker.patch(
            "archiver.processors.flush_staging_events.get_conn", return_value=mock_conn
        )
        mocker.patch(
            "archiver.processors.flush_staging_events.get_s3fs", return_value=MagicMock()
        )
        mocker.patch(
            "archiver.processors.flush_staging_events._flush_one",
            side_effect=Exception("unexpected crash"),
        )

        flush_staging_events()

        mock_conn.close.assert_called_once()
