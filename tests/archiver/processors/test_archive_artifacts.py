"""Unit tests for archiver/processors/archive_artifacts.py"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, mock_open

from processors.archive_artifacts import archive_artifacts

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DB_KWARGS = {"host": "localhost", "dbname": "test", "user": "test", "password": "test"}

_FETCHED_AT = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)

def _make_db_row(artifact_id=1):
    """Return a tuple matching the SELECT column order in archive_artifacts."""
    return (
        artifact_id,        # artifact_id
        "run-uuid-001",     # run_id
        "cars.com",         # source
        "detail_page",      # artifact_type
        "toyota_rav4",      # search_key
        "national",         # search_scope
        "https://cars.com/vehicledetail/abc/",  # url
        _FETCHED_AT,        # fetched_at
        200,                # http_status
        30000,              # content_bytes
        "abc123sha",        # sha256
        None,               # error
        None,               # page_num
    )


def _patch_db(mocker, rows):
    """
    Patch psycopg2.connect for both calls archive_artifacts makes:
      1. SELECT metadata (returns rows)
      2. UPDATE archived_at (no return value needed)
    Both calls return the same mock_conn; the cursor context manager
    returns mock_cursor which exposes fetchall().
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_connect = mocker.patch("psycopg2.connect", return_value=mock_conn)
    return mock_conn, mock_cursor, mock_connect


def _patch_parquet(mocker):
    """Patch pyarrow.parquet.write_to_dataset so no real S3 write happens."""
    return mocker.patch("pyarrow.parquet.write_to_dataset")


# ---------------------------------------------------------------------------
# Empty / trivial cases
# ---------------------------------------------------------------------------

class TestArchiveArtifactsEmpty:
    def test_empty_list_returns_empty(self):
        result = archive_artifacts([], _DB_KWARGS)
        assert result == []


# ---------------------------------------------------------------------------
# DB failure
# ---------------------------------------------------------------------------

class TestArchiveArtifactsDbError:
    def test_db_connect_error_marks_all_failed(self, mocker, mock_s3fs):
        mocker.patch("psycopg2.connect", side_effect=Exception("connection refused"))
        artifacts = [{"artifact_id": 1, "filepath": "/data/1.html"}]
        results = archive_artifacts(artifacts, _DB_KWARGS)
        assert len(results) == 1
        assert results[0]["archived"] is False
        assert "db_error" in results[0]["reason"]

    def test_db_error_covers_all_artifact_ids(self, mocker, mock_s3fs):
        mocker.patch("psycopg2.connect", side_effect=Exception("boom"))
        artifacts = [
            {"artifact_id": 1, "filepath": "/a.html"},
            {"artifact_id": 2, "filepath": "/b.html"},
        ]
        results = archive_artifacts(artifacts, _DB_KWARGS)
        assert {r["artifact_id"] for r in results} == {1, 2}
        assert all(r["archived"] is False for r in results)


# ---------------------------------------------------------------------------
# Artifact not found in DB
# ---------------------------------------------------------------------------

class TestArchiveArtifactsNotInDb:
    def test_artifact_missing_from_db_marked_not_found(self, mocker, mock_s3fs):
        _patch_db(mocker, rows=[])  # DB returns no rows
        _patch_parquet(mocker)
        result = archive_artifacts([{"artifact_id": 99, "filepath": "/x.html"}], _DB_KWARGS)
        assert result[0]["archived"] is False
        assert result[0]["reason"] == "not_found_in_db"


# ---------------------------------------------------------------------------
# File read errors
# ---------------------------------------------------------------------------

class TestArchiveArtifactsFileRead:
    def test_file_read_error_marks_artifact_failed(self, mocker, mock_s3fs):
        _patch_db(mocker, rows=[_make_db_row(1)])
        _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", side_effect=OSError("disk error"))
        result = archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        assert result[0]["archived"] is False
        assert "read_error" in result[0]["reason"]

    def test_missing_file_writes_empty_html(self, mocker, mock_s3fs):
        """If filepath doesn't exist on disk, archive with empty html bytes (best-effort)."""
        _patch_db(mocker, rows=[_make_db_row(1)])
        mock_write = _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=False)
        result = archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        assert result[0]["archived"] is True
        mock_write.assert_called_once()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestArchiveArtifactsSuccess:
    def test_successful_archive(self, mocker, mock_s3fs):
        _patch_db(mocker, rows=[_make_db_row(1)])
        mock_write = _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        result = archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        assert result == [{"artifact_id": 1, "archived": True, "reason": None}]
        mock_write.assert_called_once()

    def test_archived_at_updated_on_success(self, mocker, mock_s3fs):
        """archived_at should be set in DB for successfully archived artifacts."""
        _, _, mock_connect = _patch_db(mocker, rows=[_make_db_row(1)])
        _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        # psycopg2.connect is called twice: once for SELECT, once for UPDATE archived_at
        assert mock_connect.call_count == 2

    def test_archived_at_db_failure_does_not_raise(self, mocker, mock_s3fs):
        """A failure setting archived_at should be logged but not bubble up."""
        mock_conn, _, _ = _patch_db(mocker, rows=[_make_db_row(1)])
        _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        # Make the second connect (archived_at update) raise
        mock_conn.commit.side_effect = Exception("db write failed")
        result = archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        # Should still report archived=True (Parquet write succeeded)
        assert result[0]["archived"] is True

    def test_write_to_dataset_uses_zstd(self, mocker, mock_s3fs):
        _patch_db(mocker, rows=[_make_db_row(1)])
        mock_write = _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        _, kwargs = mock_write.call_args
        assert kwargs.get("compression") == "zstd"

    def test_write_to_dataset_partition_cols(self, mocker, mock_s3fs):
        _patch_db(mocker, rows=[_make_db_row(1)])
        mock_write = _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        _, kwargs = mock_write.call_args
        assert "year" in kwargs.get("partition_cols", [])
        assert "month" in kwargs.get("partition_cols", [])
        assert "artifact_type" in kwargs.get("partition_cols", [])

    def test_year_month_derived_from_fetched_at(self, mocker, mock_s3fs):
        """Partition year/month must match the fetched_at timestamp."""
        _patch_db(mocker, rows=[_make_db_row(1)])
        mock_write = _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        # The table passed to write_to_dataset should have year=2026, month=3
        table_arg = mock_write.call_args[0][0]
        years = table_arg.column("year").to_pylist()
        months = table_arg.column("month").to_pylist()
        assert years == [2026]
        assert months == [3]

    def test_bucket_created_when_missing(self, mocker, mock_s3fs):
        mock_s3fs.exists.return_value = False  # bucket doesn't exist yet
        _patch_db(mocker, rows=[_make_db_row(1)])
        _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        mock_s3fs.mkdir.assert_called_once()

    def test_bucket_not_recreated_when_exists(self, mocker, mock_s3fs):
        mock_s3fs.exists.return_value = True
        _patch_db(mocker, rows=[_make_db_row(1)])
        _patch_parquet(mocker)
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        archive_artifacts([{"artifact_id": 1, "filepath": "/data/1.html"}], _DB_KWARGS)
        mock_s3fs.mkdir.assert_not_called()


# ---------------------------------------------------------------------------
# Parquet write failure
# ---------------------------------------------------------------------------

class TestArchiveArtifactsParquetError:
    def test_parquet_write_error_marks_all_rows_failed(self, mocker, mock_s3fs):
        _patch_db(mocker, rows=[_make_db_row(1), _make_db_row(2)])
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("s3 write failed"))
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html/>"))
        artifacts = [
            {"artifact_id": 1, "filepath": "/1.html"},
            {"artifact_id": 2, "filepath": "/2.html"},
        ]
        results = archive_artifacts(artifacts, _DB_KWARGS)
        assert all(r["archived"] is False for r in results)
        assert all("parquet_write_error" in r["reason"] for r in results)
