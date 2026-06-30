"""Unit tests for archiver/processors/compact_silver.py.

All use in-memory PyArrow tables + mocked s3fs. No real MinIO required.
"""
from datetime import date, timedelta
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pytest

# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------

_TODAY = date(2026, 6, 30)
_WATERMARK = _TODAY - timedelta(days=2)  # 2026-06-28

_BASE = "bronze/silver/observations"


def _make_table(n: int = 3, include_all_sort_cols: bool = True) -> pa.Table:
    """Build a minimal test table. If include_all_sort_cols is False, omit dealer columns."""
    data = {
        "listing_id": [f"L00{i}" for i in range(n)],
        "price": pa.array([30000 - i * 1000 for i in range(n)], type=pa.int32()),
    }
    if include_all_sort_cols:
        data.update({
            "make": ["Toyota", "Ford", "Honda"][:n],
            "model": ["Camry", "F-150", "Civic"][:n],
            "dealer_state": ["TX", "CA", "NY"][:n],
            "dealer_name": ["Dealer B", "Dealer A", "Dealer C"][:n],
            "year": pa.array([2023, 2024, 2022][:n], type=pa.int16()),
            "trim": ["SE", "XLT", "LX"][:n],
        })
    return pa.table(data)


@pytest.fixture
def mock_fs():
    fs = MagicMock()
    fs.info.return_value = {"size": 1000}
    return fs


@pytest.fixture(autouse=True)
def _patch_today():
    with patch("archiver.processors.compact_silver._today_utc", return_value=_TODAY):
        yield


@pytest.fixture(autouse=True)
def _patch_bucket():
    with patch("archiver.processors.compact_silver.BUCKET", "bronze"):
        yield


def _mock_pq(mocker, table: pa.Table, *, num_rows: int | None = None):
    """Mock pq.read_table, pq.write_table, and pq.ParquetFile for _compact_one."""
    mocker.patch("archiver.processors.compact_silver.pq.read_table", return_value=table)
    mocker.patch("archiver.processors.compact_silver.pq.write_table")
    mock_pf = MagicMock()
    mock_pf.metadata.num_rows = num_rows if num_rows is not None else len(table)
    mocker.patch("archiver.processors.compact_silver.pq.ParquetFile", return_value=mock_pf)


def _day_path(source: str, year: int, month: int, day: int) -> str:
    return f"bronze/silver/observations/source={source}/obs_year={year}/obs_month={month}/obs_day={day}"


# ---------------------------------------------------------------------------
# test_full_compaction_happy_path
# ---------------------------------------------------------------------------

class TestFullCompactionHappyPath:
    def test_reads_parts_writes_tmp_deletes_renames(self, mock_fs, mocker):
        """N part files → .tmp written → parts deleted → rename to compacted."""
        import archiver.processors.compact_silver as mod

        path = _day_path("detail", 2026, 6, 28)
        part_files = [f"{path}/part-aaa-0.parquet", f"{path}/part-bbb-0.parquet"]
        table = _make_table(3)

        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = [
            [f"{_BASE}/source=detail"],
            [f"{_BASE}/source=detail/obs_year=2026"],
            [f"{_BASE}/source=detail/obs_year=2026/obs_month=6"],
            [path],
            part_files,
        ]
        # 2 part files each return the same 3-row table → concat produces 6 rows
        _mock_pq(mocker, table, num_rows=6)

        result = mod.compact_silver(max_partitions=10)

        assert result["error"] is None
        assert result["compacted"] == 1
        assert result["incremental"] == 0
        assert result["failed"] == 0
        assert len(result["partitions"]) == 1

        p = result["partitions"][0]
        assert p["ok"] is True
        assert p["state"] == "needs_compaction"
        assert p["files_merged"] == 2
        assert p["rows"] == 6  # 2 part files × 3 rows each

        # .tmp was the write destination
        write_call = mod.pq.write_table.call_args
        assert write_call.args[1].endswith("compacted-2026-06-28.parquet.tmp")

        # all part files deleted
        for f in part_files:
            mock_fs.rm.assert_any_call(f)

        # renamed from .tmp to final
        rename_src, rename_dst = mock_fs.rename.call_args.args
        assert rename_src.endswith(".parquet.tmp")
        assert rename_dst.endswith("compacted-2026-06-28.parquet")
        assert not rename_dst.endswith(".tmp")


# ---------------------------------------------------------------------------
# test_sort_order_applied
# ---------------------------------------------------------------------------

class TestSortOrderApplied:
    def test_output_rows_sorted_by_sort_cols(self, mock_fs, mocker):
        """Written table is sorted by SORT_COLS (make → model → … → listing_id)."""
        import archiver.processors.compact_silver as mod

        # Deliberately unsorted: Honda < Ford < Toyota by make
        unsorted = pa.table({
            "make": ["Toyota", "Ford", "Honda"],
            "model": ["Camry", "F-150", "Civic"],
            "dealer_state": ["TX", "CA", "NY"],
            "dealer_name": ["Dealer B", "Dealer A", "Dealer C"],
            "year": pa.array([2023, 2024, 2022], type=pa.int16()),
            "trim": ["SE", "XLT", "LX"],
            "listing_id": ["L003", "L001", "L002"],
        })

        captured = []

        def _capture_write(tbl, *args, **kwargs):
            captured.append(tbl)

        mocker.patch("archiver.processors.compact_silver.pq.read_table", return_value=unsorted)
        mocker.patch("archiver.processors.compact_silver.pq.write_table", side_effect=_capture_write)
        mock_pf = MagicMock()
        mock_pf.metadata.num_rows = 3
        mocker.patch("archiver.processors.compact_silver.pq.ParquetFile", return_value=mock_pf)

        from archiver.processors.compact_silver import _compact_one
        path = _day_path("detail", 2026, 6, 28)
        _compact_one(mock_fs, path, "needs_compaction", [], [f"{path}/part-x-0.parquet"], date(2026, 6, 28))

        assert len(captured) == 1
        written_makes = captured[0].column("make").to_pylist()
        assert written_makes == sorted(written_makes), "Rows should be sorted by make (first SORT_COL)"


# ---------------------------------------------------------------------------
# test_sort_cols_absent_from_schema_handled
# ---------------------------------------------------------------------------

class TestSortColsAbsentFromSchema:
    def test_missing_sort_cols_do_not_error(self, mock_fs, mocker):
        """Table without some SORT_COLS (e.g. srp) — absent cols are skipped, no error."""
        table_no_dealer = _make_table(3, include_all_sort_cols=False)

        _mock_pq(mocker, table_no_dealer)

        from archiver.processors.compact_silver import _compact_one
        path = _day_path("srp", 2026, 6, 28)
        result = _compact_one(
            mock_fs, path, "needs_compaction", [], [f"{path}/part-x-0.parquet"], date(2026, 6, 28)
        )

        assert result["ok"] is True
        assert result["rows"] == 3


# ---------------------------------------------------------------------------
# test_skips_done_partition
# ---------------------------------------------------------------------------

class TestSkipsDonePartition:
    def test_done_partition_skipped_no_write(self, mock_fs, mocker):
        """Partition with only compacted-*.parquet → skipped; no reads or writes."""
        import archiver.processors.compact_silver as mod

        path = _day_path("detail", 2026, 6, 1)
        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = [
            [f"{_BASE}/source=detail"],
            [f"{_BASE}/source=detail/obs_year=2026"],
            [f"{_BASE}/source=detail/obs_year=2026/obs_month=6"],
            [path],
            [f"{path}/compacted-2026-06-01.parquet"],  # only compacted file
        ]
        mock_read = mocker.patch("archiver.processors.compact_silver.pq.read_table")
        mock_write = mocker.patch("archiver.processors.compact_silver.pq.write_table")

        result = mod.compact_silver(max_partitions=10)

        assert result["skipped"] == 1
        assert result["compacted"] == 0
        assert result["partitions"] == []
        mock_read.assert_not_called()
        mock_write.assert_not_called()


# ---------------------------------------------------------------------------
# test_skips_empty_partition
# ---------------------------------------------------------------------------

class TestSkipsEmptyPartition:
    def test_empty_partition_skipped_gracefully(self, mock_fs, mocker):
        """Partition with no parquet files → skipped gracefully."""
        import archiver.processors.compact_silver as mod

        path = _day_path("detail", 2026, 6, 1)
        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = [
            [f"{_BASE}/source=detail"],
            [f"{_BASE}/source=detail/obs_year=2026"],
            [f"{_BASE}/source=detail/obs_year=2026/obs_month=6"],
            [path],
            ["path/somefile.csv"],  # no parquet files
        ]
        mock_write = mocker.patch("archiver.processors.compact_silver.pq.write_table")

        result = mod.compact_silver(max_partitions=10)

        assert result["compacted"] == 0
        assert result["skipped"] == 0
        assert result["partitions"] == []
        mock_write.assert_not_called()


# ---------------------------------------------------------------------------
# test_incremental_compaction
# ---------------------------------------------------------------------------

class TestIncrementalCompaction:
    def test_reads_compacted_and_part_files_writes_new_compacted(self, mock_fs, mocker):
        """Incremental state: re-reads compacted + new part files, deletes all originals."""
        import archiver.processors.compact_silver as mod

        path = _day_path("detail", 2026, 6, 10)
        compacted_file = f"{path}/compacted-2026-06-10.parquet"
        new_part_file = f"{path}/part-new-0.parquet"

        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = [
            [f"{_BASE}/source=detail"],
            [f"{_BASE}/source=detail/obs_year=2026"],
            [f"{_BASE}/source=detail/obs_year=2026/obs_month=6"],
            [path],
            [compacted_file, new_part_file],
        ]
        table = _make_table(2)
        # 2 files each return the same 2-row table → concat produces 4 rows
        _mock_pq(mocker, table, num_rows=4)

        result = mod.compact_silver(max_partitions=10)

        assert result["incremental"] == 1
        assert result["compacted"] == 0
        p = result["partitions"][0]
        assert p["state"] == "incremental"
        assert p["files_merged"] == 2

        # Both files were read
        read_calls = [c.args[0] for c in mod.pq.read_table.call_args_list]
        assert compacted_file in read_calls
        assert new_part_file in read_calls

        # Both files deleted
        mock_fs.rm.assert_any_call(compacted_file)
        mock_fs.rm.assert_any_call(new_part_file)


# ---------------------------------------------------------------------------
# test_watermark_excludes_yesterday
# ---------------------------------------------------------------------------

class TestWatermarkExcludesYesterday:
    def test_obs_day_yesterday_not_included(self, mock_fs, mocker):
        """obs_day == today - 1 is NOT processed (inside the 2-day watermark buffer)."""
        import archiver.processors.compact_silver as mod

        yesterday = _TODAY - timedelta(days=1)
        path = _day_path("detail", yesterday.year, yesterday.month, yesterday.day)

        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = [
            [f"{_BASE}/source=detail"],
            [f"{_BASE}/source=detail/obs_year={yesterday.year}"],
            [f"{_BASE}/source=detail/obs_year={yesterday.year}/obs_month={yesterday.month}"],
            [path],
        ]
        mock_write = mocker.patch("archiver.processors.compact_silver.pq.write_table")

        result = mod.compact_silver(max_partitions=10)

        assert result["compacted"] == 0
        assert result["partitions"] == []
        mock_write.assert_not_called()


# ---------------------------------------------------------------------------
# test_watermark_includes_two_days_ago
# ---------------------------------------------------------------------------

class TestWatermarkIncludesTwoDaysAgo:
    def test_obs_day_two_days_ago_is_included(self, mock_fs, mocker):
        """obs_day == today - 2 IS included in discovery."""
        import archiver.processors.compact_silver as mod

        two_ago = _TODAY - timedelta(days=2)
        path = _day_path("detail", two_ago.year, two_ago.month, two_ago.day)
        part_file = f"{path}/part-abc-0.parquet"

        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = [
            [f"{_BASE}/source=detail"],
            [f"{_BASE}/source=detail/obs_year={two_ago.year}"],
            [f"{_BASE}/source=detail/obs_year={two_ago.year}/obs_month={two_ago.month}"],
            [path],
            [part_file],
        ]
        _mock_pq(mocker, _make_table(2))

        result = mod.compact_silver(max_partitions=10)

        assert result["compacted"] == 1
        assert result["partitions"][0]["date"] == two_ago.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# test_max_partitions_respected
# ---------------------------------------------------------------------------

class TestMaxPartitionsRespected:
    def test_only_max_partitions_processed(self, mock_fs, mocker):
        """20 eligible partitions → only 10 processed."""
        import archiver.processors.compact_silver as mod

        # Build 20 partitions for different days, all within watermark
        days = [date(2026, 5, d) for d in range(1, 21)]
        paths = [_day_path("detail", d.year, d.month, d.day) for d in days]
        part_files_per_path = {p: [f"{p}/part-x-0.parquet"] for p in paths}

        # Simulate discovery
        ls_returns = [
            [f"{_BASE}/source=detail"],
            [f"{_BASE}/source=detail/obs_year=2026"],
        ]
        # Each month dir lists its day dirs; 20 days across May
        # We'll structure as a single month for simplicity
        ls_returns.append([f"{_BASE}/source=detail/obs_year=2026/obs_month=5"])
        ls_returns.append(paths)  # all 20 day dirs in one month
        # Each day dir ls call returns one part file
        for p in paths:
            ls_returns.append(part_files_per_path[p])

        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = ls_returns
        _mock_pq(mocker, _make_table(2))

        result = mod.compact_silver(max_partitions=10)

        assert result["compacted"] == 10
        assert len(result["partitions"]) == 10


# ---------------------------------------------------------------------------
# test_oldest_first_ordering
# ---------------------------------------------------------------------------

class TestOldestFirstOrdering:
    def test_processing_order_ascending_by_date_then_source(self, mock_fs, mocker):
        """Partitions are processed oldest-first by (date, source)."""
        import archiver.processors.compact_silver as mod

        # Three partitions out of order
        dates_sources = [
            (date(2026, 6, 5), "srp"),
            (date(2026, 6, 1), "detail"),
            (date(2026, 6, 1), "carousel"),
        ]
        paths = [_day_path(s, d.year, d.month, d.day) for d, s in dates_sources]

        # _list_day_partitions processes each source completely (year→month→day)
        # before moving to the next. Classify calls all happen after full discovery.
        detail_day = _day_path("detail", 2026, 6, 1)
        carousel_day = _day_path("carousel", 2026, 6, 1)
        srp_day = _day_path("srp", 2026, 6, 5)
        ls_returns = [
            # discovery: base
            [f"{_BASE}/source=detail", f"{_BASE}/source=carousel", f"{_BASE}/source=srp"],
            # discovery: detail fully
            [f"{_BASE}/source=detail/obs_year=2026"],
            [f"{_BASE}/source=detail/obs_year=2026/obs_month=6"],
            [detail_day],
            # discovery: carousel fully
            [f"{_BASE}/source=carousel/obs_year=2026"],
            [f"{_BASE}/source=carousel/obs_year=2026/obs_month=6"],
            [carousel_day],
            # discovery: srp fully
            [f"{_BASE}/source=srp/obs_year=2026"],
            [f"{_BASE}/source=srp/obs_year=2026/obs_month=6"],
            [srp_day],
            # classify: in discovery order (detail, carousel, srp)
            [f"{detail_day}/part-x-0.parquet"],
            [f"{carousel_day}/part-x-0.parquet"],
            [f"{srp_day}/part-x-0.parquet"],
        ]

        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        mock_fs.ls.side_effect = ls_returns
        _mock_pq(mocker, _make_table(1))

        result = mod.compact_silver(max_partitions=10)

        processed = [(p["date"], p["source"]) for p in result["partitions"]]
        assert processed == sorted(processed), "Partitions must be processed oldest-first"
        assert processed[0] == ("2026-06-01", "carousel")
        assert processed[1] == ("2026-06-01", "detail")
        assert processed[2] == ("2026-06-05", "srp")


# ---------------------------------------------------------------------------
# test_failed_partition_does_not_abort_run
# ---------------------------------------------------------------------------

class TestFailedPartitionDoesNotAbortRun:
    def test_one_failure_others_still_processed(self, mock_fs, mocker):
        """One partition errors → others still processed; failed count incremented."""
        import archiver.processors.compact_silver as mod

        path_ok = _day_path("detail", 2026, 6, 1)
        path_fail = _day_path("carousel", 2026, 6, 1)
        part_ok = f"{path_ok}/part-x-0.parquet"
        part_fail = f"{path_fail}/part-y-0.parquet"

        mocker.patch("archiver.processors.compact_silver.get_s3fs", return_value=mock_fs)
        # Discovery processes each source fully before classify calls
        mock_fs.ls.side_effect = [
            [f"{_BASE}/source=detail", f"{_BASE}/source=carousel"],
            # detail fully
            [f"{_BASE}/source=detail/obs_year=2026"],
            [f"{_BASE}/source=detail/obs_year=2026/obs_month=6"],
            [path_ok],
            # carousel fully
            [f"{_BASE}/source=carousel/obs_year=2026"],
            [f"{_BASE}/source=carousel/obs_year=2026/obs_month=6"],
            [path_fail],
            # classify (in discovery order: detail, carousel)
            [part_ok],
            [part_fail],
        ]

        call_count = [0]

        def _read_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("simulated read failure")
            return _make_table(2)

        mocker.patch("archiver.processors.compact_silver.pq.read_table", side_effect=_read_side_effect)
        mocker.patch("archiver.processors.compact_silver.pq.write_table")
        mock_pf = MagicMock()
        mock_pf.metadata.num_rows = 2
        mocker.patch("archiver.processors.compact_silver.pq.ParquetFile", return_value=mock_pf)

        result = mod.compact_silver(max_partitions=10)

        assert result["failed"] == 1
        assert result["compacted"] == 1
        failed_p = next(p for p in result["partitions"] if not p["ok"])
        ok_p = next(p for p in result["partitions"] if p["ok"])
        assert "simulated read failure" in failed_p["error"]
        assert ok_p["ok"] is True


# ---------------------------------------------------------------------------
# test_tmp_preserved_for_manual_recovery
# ---------------------------------------------------------------------------

class TestTmpPreservedForManualRecovery:
    def test_tmp_not_cleaned_up_when_rename_fails(self, mock_fs, mocker):
        """If rename fails after originals are deleted, .tmp is left in place (not cleaned up)."""
        from archiver.processors.compact_silver import _compact_one

        path = _day_path("detail", 2026, 6, 28)
        part_file = f"{path}/part-abc-0.parquet"
        table = _make_table(3)

        _mock_pq(mocker, table)
        mock_fs.rename.side_effect = OSError("rename failed")

        with pytest.raises(OSError, match="rename failed"):
            _compact_one(mock_fs, path, "needs_compaction", [], [part_file], date(2026, 6, 28))

        # .tmp was NOT deleted (no rm call on the tmp path)
        tmp_path = f"{path}/compacted-2026-06-28.parquet.tmp"
        rm_calls = [c.args[0] for c in mock_fs.rm.call_args_list]
        assert tmp_path not in rm_calls, ".tmp must be preserved for manual recovery"

        # The original part file WAS deleted (before rename was attempted)
        assert part_file in rm_calls
