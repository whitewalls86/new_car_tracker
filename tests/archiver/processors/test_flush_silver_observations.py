"""Unit tests for archiver/processors/flush_silver_observations.py"""
from datetime import datetime, timezone
from unittest.mock import MagicMock

from archiver.processors.flush_silver_observations import flush_silver_observations

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FETCHED_AT = datetime(2026, 4, 15, 10, 0, 0, tzinfo=timezone.utc)

# Tuple matching _DB_COLUMNS order (id first, then all observation fields)
_DB_COLUMNS = [
    "id",
    "artifact_id", "listing_id", "vin", "canonical_detail_url",
    "source", "listing_state", "fetched_at",
    "price", "make", "model", "trim", "year", "mileage", "msrp",
    "stock_type", "fuel_type", "body_style",
    "dealer_name", "dealer_zip", "customer_id", "seller_id",
    "dealer_street", "dealer_city", "dealer_state", "dealer_phone",
    "dealer_website", "dealer_cars_com_url", "dealer_rating",
    "financing_type", "seller_zip", "seller_customer_id",
    "page_number", "position_on_page", "trid", "isa_context",
    "body", "condition",
]


def _make_row(row_id=1, source="detail"):
    """Return a minimal observation tuple with real values for key fields."""
    nulls = [None] * len(_DB_COLUMNS)
    row = list(nulls)
    row[_DB_COLUMNS.index("id")]          = row_id
    row[_DB_COLUMNS.index("artifact_id")] = 100
    row[_DB_COLUMNS.index("listing_id")]  = "listing-abc"
    row[_DB_COLUMNS.index("source")]      = source
    row[_DB_COLUMNS.index("listing_state")] = "active"
    row[_DB_COLUMNS.index("fetched_at")]  = _FETCHED_AT
    row[_DB_COLUMNS.index("price")]       = 25000
    row[_DB_COLUMNS.index("make")]        = "Toyota"
    row[_DB_COLUMNS.index("model")]       = "RAV4"
    return tuple(row)


def _make_mock_conn(max_id=10, rows=None, rowcount=1):
    """
    Build a mock psycopg2 connection for flush_silver_observations.

    Three cursor blocks:
      1. fetchone()  → (max_id,)
      2. fetchall()  → rows
      3. rowcount    → rowcount (DELETE)
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (max_id,)
    mock_cursor.fetchall.return_value = rows or []
    mock_cursor.rowcount = rowcount

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# Empty table
# ---------------------------------------------------------------------------

class TestFlushSilverEmpty:
    def test_empty_table_returns_zero(self, mocker):
        mock_conn, _ = _make_mock_conn(max_id=None)
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            return_value=mock_conn,
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_s3fs",
            return_value=MagicMock(),
        )
        mocker.patch("pyarrow.parquet.write_to_dataset")

        result = flush_silver_observations()

        assert result["flushed"] == 0
        assert result["error"] is None

    def test_write_not_called_when_empty(self, mocker):
        mock_conn, _ = _make_mock_conn(max_id=None)
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            return_value=mock_conn,
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_s3fs",
            return_value=MagicMock(),
        )
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")

        flush_silver_observations()

        mock_write.assert_not_called()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestFlushSilverSuccess:
    def _run(self, mocker, rows, rowcount=None):
        mock_conn, _ = _make_mock_conn(
            max_id=len(rows), rows=rows, rowcount=rowcount or len(rows)
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            return_value=mock_conn,
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_s3fs",
            return_value=MagicMock(),
        )
        mock_write = mocker.patch("pyarrow.parquet.write_to_dataset")
        return flush_silver_observations(), mock_write, mock_conn

    def test_returns_flushed_count(self, mocker):
        rows = [_make_row(1), _make_row(2)]
        result, _, _ = self._run(mocker, rows, rowcount=2)
        assert result["flushed"] == 2
        assert result["error"] is None

    def test_write_to_dataset_called_once(self, mocker):
        _, mock_write, _ = self._run(mocker, [_make_row()])
        mock_write.assert_called_once()

    def test_write_uses_zstd_compression(self, mocker):
        _, mock_write, _ = self._run(mocker, [_make_row()])
        _, kwargs = mock_write.call_args
        assert kwargs.get("compression") == "zstd"

    def test_partitions_by_source_year_month_day(self, mocker):
        _, mock_write, _ = self._run(mocker, [_make_row()])
        _, kwargs = mock_write.call_args
        assert kwargs.get("partition_cols") == ["source", "obs_year", "obs_month", "obs_day"]

    def test_root_path_is_silver_observations(self, mocker):
        _, mock_write, _ = self._run(mocker, [_make_row()])
        _, kwargs = mock_write.call_args
        assert "silver/observations" in kwargs.get("root_path", "")

    def test_commit_called(self, mocker):
        _, _, mock_conn = self._run(mocker, [_make_row()])
        mock_conn.commit.assert_called_once()

    def test_partition_values_derived_from_fetched_at(self, mocker):
        _, mock_write, _ = self._run(mocker, [_make_row()])
        arrow_table = mock_write.call_args[0][0]
        assert arrow_table.column("obs_year").to_pylist()  == [2026]
        assert arrow_table.column("obs_month").to_pylist() == [4]
        assert arrow_table.column("obs_day").to_pylist()   == [15]

    def test_source_partition_value_preserved(self, mocker):
        _, mock_write, _ = self._run(mocker, [_make_row(source="srp")])
        arrow_table = mock_write.call_args[0][0]
        assert arrow_table.column("source").to_pylist() == ["srp"]

    def test_written_at_set_to_now(self, mocker):
        before = datetime.now(timezone.utc)
        _, mock_write, _ = self._run(mocker, [_make_row()])
        after = datetime.now(timezone.utc)
        arrow_table = mock_write.call_args[0][0]
        written_at = arrow_table.column("written_at").to_pylist()[0]
        assert before <= written_at.replace(tzinfo=timezone.utc) <= after

    def test_id_column_not_in_parquet(self, mocker):
        _, mock_write, _ = self._run(mocker, [_make_row()])
        arrow_table = mock_write.call_args[0][0]
        assert "id" not in arrow_table.schema.names

    def test_conn_closed_on_success(self, mocker):
        _, _, mock_conn = self._run(mocker, [_make_row()])
        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestFlushSilverErrors:
    def test_db_connect_failure_returns_error(self, mocker):
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            side_effect=Exception("connection refused"),
        )

        result = flush_silver_observations()

        assert result["flushed"] == 0
        assert result["error"] is not None

    def test_parquet_write_error_returns_error(self, mocker):
        mock_conn, _ = _make_mock_conn(max_id=1, rows=[_make_row()])
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            return_value=mock_conn,
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_s3fs",
            return_value=MagicMock(),
        )
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("s3 down"))

        result = flush_silver_observations()

        assert result["flushed"] == 0
        assert result["error"] is not None

    def test_parquet_write_error_does_not_delete(self, mocker):
        mock_conn, _ = _make_mock_conn(max_id=1, rows=[_make_row()])
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            return_value=mock_conn,
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_s3fs",
            return_value=MagicMock(),
        )
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("s3 down"))

        flush_silver_observations()

        mock_conn.commit.assert_not_called()

    def test_conn_closed_on_error(self, mocker):
        mock_conn, _ = _make_mock_conn(max_id=1, rows=[_make_row()])
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            return_value=mock_conn,
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_s3fs",
            return_value=MagicMock(),
        )
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("boom"))

        flush_silver_observations()

        mock_conn.close.assert_called_once()

    def test_rollback_called_on_error(self, mocker):
        mock_conn, _ = _make_mock_conn(max_id=1, rows=[_make_row()])
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_conn",
            return_value=mock_conn,
        )
        mocker.patch(
            "archiver.processors.flush_silver_observations.get_s3fs",
            return_value=MagicMock(),
        )
        mocker.patch("pyarrow.parquet.write_to_dataset", side_effect=Exception("boom"))

        flush_silver_observations()

        mock_conn.rollback.assert_called_once()
