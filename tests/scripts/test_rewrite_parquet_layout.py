"""Unit tests for scripts/rewrite_parquet_layout.py

Groups:
  A - CLI validation (--dataset/--all, --source, --month, invalid names)
  B - discovery: silver day partitions grouped into month units
  C - discovery: ops month partitions handled as one unit
  D - dry-run safety (no writes, no renames, no deletes)
  E - data transformation: concat, sort, missing sort columns
  F - apply flow: row count preserved, output path, tmp visibility
  G - apply flow: row count mismatch aborts rename
  H - apply flow: rename failure leaves tmp and reports error
  I - apply flow: old prefix untouched
  J - baseline audit: load, lookup, mismatch fails unit
  K - report structure
  L - never-delete guarantee
"""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_table(
    n: int,
    *,
    schema: pa.Schema | None = None,
) -> pa.Table:
    """Build a small Arrow table with n rows for testing."""
    if schema is None:
        schema = pa.schema([
            pa.field("event_id", pa.int64()),
            pa.field("listing_id", pa.string()),
            pa.field("event_at", pa.timestamp("us", tz="UTC")),
        ])
    arrays = []
    for f in schema:
        if pa.types.is_int64(f.type) or pa.types.is_int32(f.type):
            arrays.append(pa.array(list(range(n)), type=f.type))
        elif pa.types.is_string(f.type):
            arrays.append(pa.array([f"v{i}" for i in range(n)], type=f.type))
        elif pa.types.is_timestamp(f.type):
            base = datetime(2026, 6, 1, tzinfo=timezone.utc)
            arrays.append(pa.array([base] * n, type=f.type))
        else:
            arrays.append(pa.array([None] * n, type=f.type))
    return pa.table(dict(zip([f.name for f in schema], arrays)), schema=schema)


def _make_parquet_bytes(n: int, schema: pa.Schema | None = None) -> bytes:
    """Return raw Parquet bytes for a table with n rows."""
    table = _make_table(n, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.read()


def _mock_paginator(entries: list[tuple[str, int]]):
    """Mock boto3 client whose paginator yields the given (key, size) entries."""
    contents = [{"Key": k, "Size": s} for k, s in entries]
    page = {"Contents": contents} if contents else {}
    paginator = MagicMock()
    paginator.paginate.return_value = [page]
    client = MagicMock()
    client.get_paginator.return_value = paginator
    return client


def _make_unit(
    dataset: str = "price_observation_events",
    source: str | None = None,
    year: int = 2026,
    month: int = 6,
    keys: list[str] | None = None,
    source_bytes: int = 1000,
):
    from scripts.rewrite_parquet_layout import RewriteUnit

    if keys is None:
        keys = [f"ops/{dataset}/year={year}/month={month}/part-0.parquet"]
    source_prefix = (
        f"silver/observations/source={source}/obs_year={year}/obs_month={month}/"
        if dataset == "silver_observations"
        else f"ops/{dataset}/year={year}/month={month}/"
    )
    target_prefix = (
        f"silver_normalized/observations/source={source}/obs_year={year}/obs_month={month}/"
        if dataset == "silver_observations"
        else f"ops_normalized/{dataset}/year={year}/month={month}/"
    )
    return RewriteUnit(
        dataset=dataset,
        source=source,
        year=year,
        month=month,
        source_prefix=source_prefix,
        target_prefix=target_prefix,
        source_keys=keys,
        source_bytes=source_bytes,
    )


# ---------------------------------------------------------------------------
# Group A: CLI validation
# ---------------------------------------------------------------------------


class TestCliValidation:
    def test_no_selector_fails(self):
        from scripts.rewrite_parquet_layout import parse_args

        with pytest.raises(SystemExit) as exc:
            parse_args([])
        assert exc.value.code != 0

    def test_dataset_and_all_mutually_exclusive(self):
        from scripts.rewrite_parquet_layout import parse_args

        with pytest.raises(SystemExit) as exc:
            parse_args(["--all", "--dataset", "silver_observations"])
        assert exc.value.code != 0

    def test_all_alone_succeeds(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--all"])
        assert args.all is True
        assert args.datasets is None

    def test_single_dataset_succeeds(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--dataset", "silver_observations"])
        assert args.datasets == ["silver_observations"]

    def test_invalid_dataset_rejected(self):
        from scripts.rewrite_parquet_layout import parse_args

        with pytest.raises(SystemExit) as exc:
            parse_args(["--dataset", "nonexistent_table"])
        assert exc.value.code != 0

    def test_source_rejected_for_non_silver_dataset(self):
        from scripts.rewrite_parquet_layout import parse_args

        with pytest.raises(SystemExit) as exc:
            parse_args(["--dataset", "price_observation_events", "--source", "detail"])
        assert exc.value.code != 0

    def test_source_valid_for_silver(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--dataset", "silver_observations", "--source", "detail"])
        assert args.source == "detail"

    def test_source_with_all_is_valid(self):
        """--all includes silver; --source filtering applies to silver units only."""
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--all", "--source", "carousel"])
        assert args.all is True
        assert args.source == "carousel"

    def test_invalid_month_format_rejected(self):
        from scripts.rewrite_parquet_layout import parse_args

        for bad in ["2026", "06-2026", "2026/06", "not-a-date"]:
            with pytest.raises(SystemExit):
                parse_args(["--all", "--month", bad])

    def test_valid_month_parsed(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--all", "--month", "2026-06"])
        assert args.month == "2026-06"

    def test_default_is_dry_run(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--all"])
        assert args.dry_run is True
        assert args.apply is False

    def test_apply_disables_dry_run(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--all", "--apply"])
        assert args.apply is True
        assert args.dry_run is False

    def test_limit_partitions_parsed(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--all", "--limit-partitions", "5"])
        assert args.limit_partitions == 5


# ---------------------------------------------------------------------------
# Group B: discovery — silver units
# ---------------------------------------------------------------------------


class TestSilverDiscovery:
    def _keys(self, source="detail", year=2026, month=6, days=(1, 2, 3)):
        """Build silver keys spanning multiple days for one source+month."""
        return [
            (
                f"silver/observations/source={source}"
                f"/obs_year={year}/obs_month={month}/obs_day={d}/part-{i}.parquet",
                1024,
            )
            for i, d in enumerate(days)
        ]

    def test_multiple_days_collapsed_into_one_unit(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = self._keys(days=(1, 2, 3))
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter=None, month_filter=None)

        assert len(units) == 1
        assert units[0].source == "detail"
        assert units[0].year == 2026
        assert units[0].month == 6
        assert len(units[0].source_keys) == 3

    def test_different_months_produce_separate_units(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = self._keys(month=6) + self._keys(month=7)
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter=None, month_filter=None)

        assert len(units) == 2
        months = {u.month for u in units}
        assert months == {6, 7}

    def test_different_sources_produce_separate_units(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = self._keys(source="detail") + self._keys(source="carousel")
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter=None, month_filter=None)

        assert len(units) == 2
        sources = {u.source for u in units}
        assert sources == {"detail", "carousel"}

    def test_source_filter_applied(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = self._keys(source="detail") + self._keys(source="carousel")
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter="detail", month_filter=None)

        assert len(units) == 1
        assert units[0].source == "detail"

    def test_month_filter_applied(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = self._keys(month=6) + self._keys(month=7)
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter=None, month_filter=(2026, 6))

        assert len(units) == 1
        assert units[0].month == 6

    def test_source_prefix_contains_all_days(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = self._keys(days=(1, 15, 28))
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter=None, month_filter=None)

        unit = units[0]
        # source_prefix does not contain obs_day= component
        assert "obs_day" not in unit.source_prefix
        assert f"obs_year={unit.year}" in unit.source_prefix
        assert f"obs_month={unit.month}" in unit.source_prefix

    def test_target_prefix_is_normalized(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = self._keys()
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter=None, month_filter=None)

        assert units[0].target_prefix.startswith("silver_normalized/observations/")

    def test_unexpected_keys_ignored(self):
        from scripts.rewrite_parquet_layout import _discover_silver_units

        entries = [
            ("silver/observations/orphan.parquet", 100),
            (
                "silver/observations/source=detail"
                "/obs_year=2026/obs_month=6/obs_day=1/part-0.parquet",
                200,
            ),
        ]
        client = _mock_paginator(entries)
        units = _discover_silver_units(client, "bronze", source_filter=None, month_filter=None)

        assert len(units) == 1
        assert len(units[0].source_keys) == 1


# ---------------------------------------------------------------------------
# Group C: discovery — ops units
# ---------------------------------------------------------------------------


class TestOpsDiscovery:
    def _ops_entries(self, table="price_observation_events", year=2026, month=6, n=3):
        return [
            (f"ops/{table}/year={year}/month={month}/part-{i}.parquet", 512)
            for i in range(n)
        ]

    def test_multiple_files_in_same_month_are_one_unit(self):
        from scripts.rewrite_parquet_layout import _discover_ops_units

        entries = self._ops_entries(n=5)
        client = _mock_paginator(entries)
        units = _discover_ops_units(client, "bronze", "price_observation_events", month_filter=None)

        assert len(units) == 1
        assert len(units[0].source_keys) == 5

    def test_different_months_produce_separate_units(self):
        from scripts.rewrite_parquet_layout import _discover_ops_units

        entries = self._ops_entries(month=6) + self._ops_entries(month=7)
        client = _mock_paginator(entries)
        units = _discover_ops_units(client, "bronze", "price_observation_events", month_filter=None)

        assert len(units) == 2

    def test_month_filter_applied(self):
        from scripts.rewrite_parquet_layout import _discover_ops_units

        entries = self._ops_entries(month=6) + self._ops_entries(month=7)
        client = _mock_paginator(entries)
        units = _discover_ops_units(
            client, "bronze", "price_observation_events", month_filter=(2026, 6)
        )

        assert len(units) == 1
        assert units[0].month == 6

    def test_target_prefix_is_normalized(self):
        from scripts.rewrite_parquet_layout import _discover_ops_units

        entries = self._ops_entries()
        client = _mock_paginator(entries)
        units = _discover_ops_units(client, "bronze", "price_observation_events", month_filter=None)

        assert units[0].target_prefix.startswith("ops_normalized/price_observation_events/")

    def test_all_supported_ops_datasets_discoverable(self):
        from scripts.rewrite_parquet_layout import SUPPORTED_DATASETS, _discover_ops_units

        for dataset in SUPPORTED_DATASETS:
            if dataset == "silver_observations":
                continue
            entries = [(f"ops/{dataset}/year=2026/month=6/part-0.parquet", 512)]
            client = _mock_paginator(entries)
            units = _discover_ops_units(client, "bronze", dataset, month_filter=None)
            assert len(units) == 1, f"Expected 1 unit for {dataset}"

    def test_limit_partitions_respected(self):
        from scripts.rewrite_parquet_layout import discover_units

        # 3 months of price_observation_events
        entries = [
            (f"ops/price_observation_events/year=2026/month={m}/part-0.parquet", 512)
            for m in (6, 7, 8)
        ]
        client = _mock_paginator(entries)
        units = discover_units(
            client, "bronze", ["price_observation_events"], limit_partitions=2
        )
        assert len(units) == 2


# ---------------------------------------------------------------------------
# Group D: dry-run safety
# ---------------------------------------------------------------------------


class TestDryRunSafety:
    def _make_fs_mock(self, existing_parquet: list[str] | None = None):
        fs = MagicMock()
        fs.ls.return_value = existing_parquet or []
        return fs

    def test_dry_run_never_calls_write_table(self):
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        fs = self._make_fs_mock()

        with patch("pyarrow.parquet.read_metadata") as mock_meta:
            mock_meta.return_value.num_row_groups = 0
            mock_meta.return_value.schema.to_arrow_schema.return_value = pa.schema([])
            _dry_run_unit(unit, fs, "bronze")

        fs.open.assert_not_called()

    def test_dry_run_never_calls_rename(self):
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        fs = self._make_fs_mock()

        with patch("pyarrow.parquet.read_metadata") as mock_meta:
            mock_meta.return_value.num_row_groups = 0
            mock_meta.return_value.schema.to_arrow_schema.return_value = pa.schema([])
            _dry_run_unit(unit, fs, "bronze")

        fs.rename.assert_not_called()
        fs.move.assert_not_called()

    def test_dry_run_never_calls_pq_write_table(self):
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        fs = self._make_fs_mock()

        with (
            patch("pyarrow.parquet.read_metadata") as mock_meta,
            patch("pyarrow.parquet.write_table") as mock_write,
        ):
            mock_meta.return_value.num_row_groups = 0
            mock_meta.return_value.schema.to_arrow_schema.return_value = pa.schema([])
            _dry_run_unit(unit, fs, "bronze")
            mock_write.assert_not_called()

    def test_dry_run_does_not_call_pq_read_table(self):
        """dry_run reads only metadata (footer), never full column data."""
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        fs = self._make_fs_mock()

        with (
            patch("pyarrow.parquet.read_metadata") as mock_meta,
            patch("pyarrow.parquet.read_table") as mock_read,
        ):
            mock_meta.return_value.num_row_groups = 0
            mock_meta.return_value.schema.to_arrow_schema.return_value = pa.schema([])
            _dry_run_unit(unit, fs, "bronze")
            mock_read.assert_not_called()

    def test_dry_run_result_status_ok_when_would_proceed(self):
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        fs = self._make_fs_mock()

        with patch("pyarrow.parquet.read_metadata") as mock_meta:
            mock_meta.return_value.num_row_groups = 1
            rg = MagicMock()
            rg.num_rows = 10
            mock_meta.return_value.row_group.return_value = rg
            mock_meta.return_value.schema.to_arrow_schema.return_value = pa.schema([
                pa.field("event_id", pa.int64()),
            ])
            result = _dry_run_unit(unit, fs, "bronze")

        assert result.status == "ok"
        assert result.rows_source == 10

    def test_dry_run_skip_existing_when_target_has_parquet(self):
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        existing = [
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old.parquet"
        ]
        fs = self._make_fs_mock(existing_parquet=existing)

        with patch("pyarrow.parquet.write_table") as mock_write:
            result = _dry_run_unit(unit, fs, "bronze")
            mock_write.assert_not_called()

        assert result.status == "skipped"


# ---------------------------------------------------------------------------
# Group E: data transformation (pure Arrow, no I/O)
# ---------------------------------------------------------------------------


class TestDataTransformation:
    def test_concat_sort_sorts_by_available_column(self):
        from scripts.rewrite_parquet_layout import _concat_sort

        schema = pa.schema([pa.field("event_at", pa.timestamp("us", tz="UTC"))])
        t1 = pa.table(
            {"event_at": [datetime(2026, 6, 3, tzinfo=timezone.utc)]},
            schema=schema,
        )
        t2 = pa.table(
            {"event_at": [datetime(2026, 6, 1, tzinfo=timezone.utc)]},
            schema=schema,
        )
        result = _concat_sort([t1, t2], sort_cols=["event_at"])
        dates = result.column("event_at").to_pylist()
        assert dates[0] < dates[1]

    def test_concat_sort_missing_sort_columns_do_not_fail(self):
        from scripts.rewrite_parquet_layout import _concat_sort

        schema = pa.schema([pa.field("event_id", pa.int64())])
        t = pa.table({"event_id": [3, 1, 2]}, schema=schema)
        # Sort cols not present in schema — should not raise
        result = _concat_sort([t], sort_cols=["event_at", "listing_id", "artifact_id"])
        assert len(result) == 3

    def test_concat_sort_preserves_row_count(self):
        from scripts.rewrite_parquet_layout import _concat_sort

        schema = pa.schema([pa.field("event_id", pa.int64())])
        tables = [
            pa.table({"event_id": list(range(10))}, schema=schema),
            pa.table({"event_id": list(range(10, 25))}, schema=schema),
        ]
        result = _concat_sort(tables, sort_cols=["event_id"])
        assert len(result) == 25

    def test_concat_sort_uses_only_existing_columns(self):
        """Sort columns that exist are used; absent ones are silently ignored."""
        from scripts.rewrite_parquet_layout import _concat_sort

        schema = pa.schema([
            pa.field("event_id", pa.int64()),
            pa.field("event_at", pa.timestamp("us", tz="UTC")),
        ])
        t = _make_table(5, schema=schema)
        # listing_id and artifact_id not in schema — no error
        result = _concat_sort([t], sort_cols=["event_at", "listing_id", "artifact_id"])
        assert len(result) == 5

    def test_schema_fingerprint_stable_across_column_order(self):
        from scripts.rewrite_parquet_layout import _schema_fingerprint

        s1 = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string())])
        s2 = pa.schema([pa.field("b", pa.string()), pa.field("a", pa.int64())])
        assert _schema_fingerprint(s1) == _schema_fingerprint(s2)

    def test_schema_fingerprint_differs_for_different_schemas(self):
        from scripts.rewrite_parquet_layout import _schema_fingerprint

        s1 = pa.schema([pa.field("a", pa.int64())])
        s2 = pa.schema([pa.field("a", pa.string())])
        assert _schema_fingerprint(s1) != _schema_fingerprint(s2)


# ---------------------------------------------------------------------------
# Group F: apply flow — row count, output path, tmp visibility
# ---------------------------------------------------------------------------


class TestApplyFlow:
    def _make_meta_mock(self, rows: int) -> MagicMock:
        """Build a mock pyarrow Parquet metadata object returning `rows` rows."""
        rg = MagicMock()
        rg.num_rows = rows
        meta = MagicMock()
        meta.num_row_groups = 1
        meta.row_group.return_value = rg
        return meta

    def _run_apply(
        self,
        unit,
        source_table: pa.Table,
        *,
        write_rows_override: int | None = None,
        rename_raises: Exception | None = None,
        existing_parquet: list[str] | None = None,
        baseline_rows: int | None = None,
        replace_existing: bool = False,
    ):
        """Run _apply_unit with mocked pyarrow I/O and an s3fs mock."""
        from scripts.rewrite_parquet_layout import _apply_unit

        rows_written = write_rows_override if write_rows_override is not None else len(source_table)
        written_meta = self._make_meta_mock(rows_written)

        fs_mock = MagicMock()
        fs_mock.ls.return_value = existing_parquet or []
        if rename_raises is not None:
            fs_mock.rename.side_effect = rename_raises

        with (
            patch("pyarrow.parquet.read_table", return_value=source_table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=written_meta),
        ):
            result = _apply_unit(
                unit, fs_mock, "bronze",
                baseline_rows=baseline_rows,
                replace_existing=replace_existing,
            )

        return result, fs_mock

    def test_row_count_preserved(self):
        """Row count in source == rows_written in the result."""
        unit = _make_unit()
        table = _make_table(25)
        result, _ = self._run_apply(unit, table)

        assert result.rows_source == 25
        assert result.rows_written == 25
        assert result.status == "ok"

    def test_output_path_uses_normalized_prefix(self):
        """target_file is under the normalized prefix (ops_normalized/ or silver_normalized/)."""
        unit = _make_unit()
        table = _make_table(5)
        result, _ = self._run_apply(unit, table)

        assert result.target_file is not None
        assert result.target_file.startswith(unit.target_prefix)

    def test_output_filename_contains_uuid(self):
        """Final file uses part-<uuid>-0.parquet naming convention."""
        unit = _make_unit()
        table = _make_table(5)
        result, _ = self._run_apply(unit, table)

        import re
        assert re.search(r"part-[0-9a-f-]{36}-0\.parquet$", result.target_file or "")

    def test_rename_called_with_tmp_and_final_paths(self):
        """fs.rename is called from .parquet.tmp → .parquet."""
        unit = _make_unit()
        table = _make_table(5)
        result, fs_mock = self._run_apply(unit, table)

        assert fs_mock.rename.called
        rename_args = fs_mock.rename.call_args[0]
        src, dst = rename_args[0], rename_args[1]
        assert src.endswith(".parquet.tmp")
        assert dst.endswith(".parquet")
        assert not dst.endswith(".parquet.tmp")

    def test_tmp_extension_does_not_match_parquet_glob(self):
        """.parquet.tmp files are NOT returned by a *.parquet glob pattern."""
        import fnmatch

        tmp_name = "part-abc-0.parquet.tmp"
        assert not fnmatch.fnmatch(tmp_name, "*.parquet")

    def test_tmp_key_not_equal_to_final_key(self):
        """The tmp key used for writing is different from the final renamed key."""
        unit = _make_unit()
        table = _make_table(5)
        _, fs_mock = self._run_apply(unit, table)

        rename_args = fs_mock.rename.call_args[0]
        assert rename_args[0] != rename_args[1]

    def test_skip_existing_when_target_has_parquet(self):
        """If the target prefix already has .parquet files, the unit is skipped."""
        unit = _make_unit()
        table = _make_table(5)
        result, fs_mock = self._run_apply(
            unit, table,
            existing_parquet=["bronze/ops_normalized/price_observation_events/year=2026/month=6/part-old.parquet"],
        )

        assert result.status == "skipped"
        # No write or rename should have happened
        fs_mock.rename.assert_not_called()

    def test_schema_fingerprint_in_result(self):
        from scripts.rewrite_parquet_layout import _schema_fingerprint

        unit = _make_unit()
        table = _make_table(5)
        result, _ = self._run_apply(unit, table)

        expected_fp = _schema_fingerprint(table.schema)
        assert result.schema_fingerprint == expected_fp

    def test_sort_columns_applied_to_silver(self):
        """Silver units are sorted by fetched_at, listing_id, artifact_id."""
        from scripts.rewrite_parquet_layout import _SORT_COLS

        assert "fetched_at" in _SORT_COLS["silver_observations"]
        assert "listing_id" in _SORT_COLS["silver_observations"]

    def test_sort_priority_for_ops_tables(self):
        """Ops event tables have event_at as the primary sort column."""
        from scripts.rewrite_parquet_layout import _SORT_COLS

        for dataset, sort_cols in _SORT_COLS.items():
            if dataset == "silver_observations":
                continue
            assert sort_cols[0] == "event_at", (
                f"{dataset}: expected event_at as first sort col, got {sort_cols[0]}"
            )

    def test_real_parquet_round_trip_row_count(self):
        """End-to-end: read real Parquet bytes → concat+sort → write → validate count."""
        from scripts.rewrite_parquet_layout import _concat_sort, _schema_fingerprint

        N = 30
        schema = pa.schema([
            pa.field("event_id", pa.int64()),
            pa.field("event_at", pa.timestamp("us", tz="UTC")),
            pa.field("listing_id", pa.string()),
        ])
        t1 = _make_table(N // 2, schema=schema)
        t2 = _make_table(N - N // 2, schema=schema)

        # Write to BytesIO (real Parquet I/O, no MinIO)
        buf1, buf2 = io.BytesIO(), io.BytesIO()
        pq.write_table(t1, buf1)
        pq.write_table(t2, buf2)
        buf1.seek(0)
        buf2.seek(0)

        # Read back and concat+sort
        rt1 = pq.read_table(buf1)
        rt2 = pq.read_table(buf2)
        combined = _concat_sort([rt1, rt2], sort_cols=["event_at", "listing_id", "event_id"])

        assert len(combined) == N

        # Write combined to BytesIO and read back row count from metadata
        out_buf = io.BytesIO()
        pq.write_table(combined, out_buf)
        out_buf.seek(0)
        meta = pq.read_metadata(out_buf)
        rows_written = sum(meta.row_group(i).num_rows for i in range(meta.num_row_groups))
        assert rows_written == N

        # Schema fingerprint is stable
        fp = _schema_fingerprint(combined.schema)
        assert len(fp) == 12  # 12-char hex from MD5


# ---------------------------------------------------------------------------
# Group G: row count mismatch aborts rename
# ---------------------------------------------------------------------------


class TestRowCountMismatchAbortsRename:
    def test_mismatch_status_is_failed(self):
        unit = _make_unit()
        table = _make_table(10)
        result, _ = TestApplyFlow()._run_apply(unit, table, write_rows_override=5)

        assert result.status == "failed"
        assert result.error is not None
        assert "mismatch" in result.error.lower()

    def test_mismatch_rename_not_called(self):
        unit = _make_unit()
        table = _make_table(10)
        _, fs_mock = TestApplyFlow()._run_apply(unit, table, write_rows_override=5)

        fs_mock.rename.assert_not_called()

    def test_mismatch_error_mentions_tmp_path(self):
        """Error message references the tmp file path for manual recovery."""
        unit = _make_unit()
        table = _make_table(10)
        result, _ = TestApplyFlow()._run_apply(unit, table, write_rows_override=5)

        assert ".parquet.tmp" in (result.error or "")

    def test_mismatch_rows_written_recorded(self):
        """rows_written is populated even on mismatch (for diagnostics)."""
        unit = _make_unit()
        table = _make_table(10)
        result, _ = TestApplyFlow()._run_apply(unit, table, write_rows_override=3)

        assert result.rows_source == 10
        assert result.rows_written == 3


# ---------------------------------------------------------------------------
# Group H: rename failure leaves tmp and reports error
# ---------------------------------------------------------------------------


class TestRenameFailure:
    def test_rename_failure_status_is_failed(self):
        unit = _make_unit()
        table = _make_table(5)
        result, _ = TestApplyFlow()._run_apply(
            unit, table, rename_raises=OSError("connection reset")
        )

        assert result.status == "failed"
        assert result.error is not None

    def test_rename_failure_error_mentions_tmp_path(self):
        """Error message references the tmp path for manual recovery."""
        unit = _make_unit()
        table = _make_table(5)
        result, _ = TestApplyFlow()._run_apply(
            unit, table, rename_raises=OSError("connection reset")
        )

        assert ".parquet.tmp" in (result.error or "")

    def test_rename_failure_target_file_is_none(self):
        """target_file is not set when rename did not complete."""
        unit = _make_unit()
        table = _make_table(5)
        result, _ = TestApplyFlow()._run_apply(
            unit, table, rename_raises=OSError("timeout")
        )

        assert result.target_file is None

    def test_rename_failure_rows_source_and_written_recorded(self):
        """rows_source and rows_written are recorded even on rename failure."""
        unit = _make_unit()
        table = _make_table(7)
        result, _ = TestApplyFlow()._run_apply(
            unit, table, rename_raises=OSError("timeout")
        )

        assert result.rows_source == 7
        assert result.rows_written == 7


# ---------------------------------------------------------------------------
# Group I: old prefix untouched
# ---------------------------------------------------------------------------


class TestOldPrefixUntouched:
    def test_write_table_never_called_with_old_prefix(self):
        """pq.write_table is never called with a path under the old prefix."""
        unit = _make_unit()
        table = _make_table(5)

        fs_mock = MagicMock()
        fs_mock.ls.return_value = []

        written_paths: list[str] = []

        def _track_write(t, path, **kwargs):
            written_paths.append(path)

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table", side_effect=_track_write),
            patch("pyarrow.parquet.read_metadata") as mock_meta,
        ):
            rg = MagicMock()
            rg.num_rows = len(table)
            mock_meta.return_value.num_row_groups = 1
            mock_meta.return_value.row_group.return_value = rg
            from scripts.rewrite_parquet_layout import _apply_unit
            _apply_unit(unit, fs_mock, "bronze")

        old_prefixes = ("silver/observations/", "ops/price_observation_events/")
        for path in written_paths:
            for old in old_prefixes:
                assert old not in path, (
                    f"write_table called with old-prefix path: {path}"
                )

    def test_rename_never_targets_old_prefix(self):
        """fs.rename is never called with old-prefix paths."""
        unit = _make_unit()
        table = _make_table(5)

        fs_mock = MagicMock()
        fs_mock.ls.return_value = []

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata") as mock_meta,
        ):
            rg = MagicMock()
            rg.num_rows = len(table)
            mock_meta.return_value.num_row_groups = 1
            mock_meta.return_value.row_group.return_value = rg
            from scripts.rewrite_parquet_layout import _apply_unit
            _apply_unit(unit, fs_mock, "bronze")

        if fs_mock.rename.called:
            args = fs_mock.rename.call_args[0]
            for arg in args:
                assert "ops/price_observation_events/" not in arg


# ---------------------------------------------------------------------------
# Group J: baseline audit
# ---------------------------------------------------------------------------


class TestBaselineAudit:
    def _baseline_json(self, dataset: str, partitions: list[dict]) -> dict:
        return {
            "generated_at": "2026-07-01T10:00:00+00:00",
            "datasets": {dataset: {"partitions": partitions}},
        }

    def test_load_baseline_audit_silver_aggregates_across_days(self, tmp_path):
        from scripts.rewrite_parquet_layout import load_baseline_audit

        base = "silver/observations/source=detail"
        data = self._baseline_json("silver_observations", [
            {"path": f"{base}/obs_year=2026/obs_month=6/obs_day=1/", "rows": 100},
            {"path": f"{base}/obs_year=2026/obs_month=6/obs_day=2/", "rows": 150},
            {"path": f"{base}/obs_year=2026/obs_month=7/obs_day=1/", "rows": 200},
        ])
        f = tmp_path / "audit.json"
        f.write_text(json.dumps(data))

        baseline = load_baseline_audit(f)
        silver = baseline["silver_observations"]
        # Month 6: 100 + 150 = 250 rows
        assert silver[("detail", 2026, 6)] == 250
        # Month 7: 200 rows
        assert silver[("detail", 2026, 7)] == 200

    def test_load_baseline_audit_ops_aggregates(self, tmp_path):
        from scripts.rewrite_parquet_layout import load_baseline_audit

        data = self._baseline_json("price_observation_events", [
            {"path": "ops/price_observation_events/year=2026/month=6/", "rows": 1000},
            {"path": "ops/price_observation_events/year=2026/month=7/", "rows": 2000},
        ])
        f = tmp_path / "audit.json"
        f.write_text(json.dumps(data))

        baseline = load_baseline_audit(f)
        ops = baseline["price_observation_events"]
        assert ops[(2026, 6)] == 1000
        assert ops[(2026, 7)] == 2000

    def test_load_baseline_audit_skips_null_rows(self, tmp_path):
        from scripts.rewrite_parquet_layout import load_baseline_audit

        data = self._baseline_json("price_observation_events", [
            {"path": "ops/price_observation_events/year=2026/month=6/", "rows": None},
            {"path": "ops/price_observation_events/year=2026/month=7/", "rows": 500},
        ])
        f = tmp_path / "audit.json"
        f.write_text(json.dumps(data))

        baseline = load_baseline_audit(f)
        ops = baseline["price_observation_events"]
        assert (2026, 6) not in ops
        assert ops[(2026, 7)] == 500

    def test_baseline_mismatch_fails_unit_before_rename(self):
        """If source rows != baseline rows, the unit fails and rename is not called."""
        unit = _make_unit()
        table = _make_table(10)  # 10 rows in source

        result, fs_mock = TestApplyFlow()._run_apply(
            unit, table, baseline_rows=15  # baseline expects 15
        )

        assert result.status == "failed"
        assert result.baseline_mismatch is not None
        assert "10" in result.baseline_mismatch
        assert "15" in result.baseline_mismatch
        fs_mock.rename.assert_not_called()

    def test_baseline_match_proceeds_normally(self):
        """If source rows == baseline rows, the unit succeeds."""
        unit = _make_unit()
        table = _make_table(10)

        result, _ = TestApplyFlow()._run_apply(unit, table, baseline_rows=10)

        assert result.status == "ok"
        assert result.baseline_mismatch is None

    def test_baseline_mismatch_no_write_occurs(self):
        """Write is not attempted when baseline mismatch detected."""
        unit = _make_unit()
        table = _make_table(10)

        fs_mock = MagicMock()
        fs_mock.ls.return_value = []

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table") as mock_write,
            patch("pyarrow.parquet.read_metadata"),
        ):
            from scripts.rewrite_parquet_layout import _apply_unit
            _apply_unit(unit, fs_mock, "bronze", baseline_rows=99)
            mock_write.assert_not_called()

    def test_baseline_lookup_returns_none_for_missing_dataset(self, tmp_path):
        from scripts.rewrite_parquet_layout import _baseline_lookup, load_baseline_audit

        data = {"generated_at": "...", "datasets": {}}
        f = tmp_path / "audit.json"
        f.write_text(json.dumps(data))

        baseline = load_baseline_audit(f)
        unit = _make_unit()
        assert _baseline_lookup(baseline, unit) is None

    def test_no_baseline_supplied_unit_proceeds(self):
        """Without --baseline-audit, no baseline check is performed."""
        unit = _make_unit()
        table = _make_table(5)
        result, _ = TestApplyFlow()._run_apply(unit, table, baseline_rows=None)
        assert result.status == "ok"


# ---------------------------------------------------------------------------
# Group K: report structure
# ---------------------------------------------------------------------------


class TestReportStructure:
    def _make_result(self, status="ok", **kwargs):
        defaults = dict(
            dataset="price_observation_events",
            source=None,
            year=2026,
            month=6,
            source_prefix="ops/price_observation_events/year=2026/month=6/",
            target_prefix="ops_normalized/price_observation_events/year=2026/month=6/",
            source_files=3,
            source_bytes=1024,
            rows_source=100,
            rows_written=100,
            ts_min="2026-06-01T00:00:00+00:00",
            ts_max="2026-06-30T23:59:59+00:00",
            schema_fingerprint="abc123def456",
            status=status,
            error=None,
            target_file="ops_normalized/price_observation_events/year=2026/month=6/part-uuid-0.parquet",
            baseline_mismatch=None,
        )
        defaults.update(kwargs)
        return type("UnitResult", (), defaults)()

    def test_report_has_required_top_level_keys(self):
        from scripts.rewrite_parquet_layout import build_report

        report = build_report(
            [], dry_run=True, bucket="bronze", datasets=["price_observation_events"]
        )
        for key in ("generated_at", "mode", "bucket", "datasets", "units"):
            assert key in report, f"Missing key: {key}"

    def test_report_mode_dry_run(self):
        from scripts.rewrite_parquet_layout import build_report

        report = build_report([], dry_run=True, bucket="bronze", datasets=[])
        assert report["mode"] == "dry_run"

    def test_report_mode_apply(self):
        from scripts.rewrite_parquet_layout import build_report

        report = build_report([], dry_run=False, bucket="bronze", datasets=[])
        assert report["mode"] == "apply"

    def test_unit_has_required_fields(self):
        from scripts.rewrite_parquet_layout import UnitResult, build_report

        r = UnitResult(
            dataset="price_observation_events",
            source=None,
            year=2026,
            month=6,
            source_prefix="ops/price_observation_events/year=2026/month=6/",
            target_prefix="ops_normalized/price_observation_events/year=2026/month=6/",
            source_files=3,
            source_bytes=1024,
            rows_source=100,
            rows_written=100,
            ts_min=None,
            ts_max=None,
            schema_fingerprint="abc",
            status="ok",
        )
        report = build_report(
            [r], dry_run=False, bucket="bronze", datasets=["price_observation_events"]
        )
        unit = report["units"][0]

        required = [
            "dataset", "source", "year", "month", "source_prefix", "target_prefix",
            "source_files", "source_bytes", "rows_source", "rows_written",
            "ts_min", "ts_max", "schema_fingerprint", "status", "error",
            "baseline_mismatch", "replaced_files",
        ]
        for key in required:
            assert key in unit, f"Missing unit key: {key}"

    def test_report_json_serializable(self):
        from scripts.rewrite_parquet_layout import UnitResult, build_report

        r = UnitResult(
            dataset="price_observation_events",
            source=None,
            year=2026,
            month=6,
            source_prefix="ops/price_observation_events/year=2026/month=6/",
            target_prefix="ops_normalized/price_observation_events/year=2026/month=6/",
            source_files=1,
            source_bytes=512,
            status="ok",
        )
        report = build_report(
            [r], dry_run=True, bucket="bronze", datasets=["price_observation_events"]
        )
        raw = json.dumps(report, indent=2)
        parsed = json.loads(raw)
        assert parsed["units"][0]["status"] == "ok"

    def test_json_out_written_to_file(self, tmp_path):
        from scripts.rewrite_parquet_layout import build_report

        report = build_report([], dry_run=True, bucket="bronze", datasets=[])
        out = tmp_path / "report.json"
        out.write_text(json.dumps(report, indent=2))
        data = json.loads(out.read_text())
        assert "generated_at" in data


# ---------------------------------------------------------------------------
# Group L: never-delete guarantee
# ---------------------------------------------------------------------------


class TestNeverDeleteGuarantee:
    def _run_apply(self, table=None):
        from scripts.rewrite_parquet_layout import _apply_unit

        unit = _make_unit()
        if table is None:
            table = _make_table(5)
        fs_mock = MagicMock()
        fs_mock.ls.return_value = []

        rg = MagicMock()
        rg.num_rows = len(table)
        meta_mock = MagicMock()
        meta_mock.num_row_groups = 1
        meta_mock.row_group.return_value = rg

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=meta_mock),
        ):
            _apply_unit(unit, fs_mock, "bronze")
        return fs_mock

    def test_fs_rm_never_called_in_apply(self):
        fs_mock = self._run_apply()
        fs_mock.rm.assert_not_called()

    def test_fs_delete_file_never_called(self):
        fs_mock = self._run_apply()
        fs_mock.delete_file.assert_not_called()

    def test_delete_object_never_called(self):
        """The script must never call boto3 delete_object (S3 object deletion)."""
        import ast
        import inspect

        import scripts.rewrite_parquet_layout as module

        source = inspect.getsource(module)
        tree = ast.parse(source)

        calls_found: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in ("delete_object", "recursive_delete"):
                        calls_found.append(node.func.attr)
        assert not calls_found, f"Found forbidden calls: {calls_found}"

    def test_no_cleanup_in_script_module(self):
        """cleanup_parquet is not imported in the rewrite script."""
        import scripts.rewrite_parquet_layout as module

        source_text = open(module.__file__).read()
        assert "cleanup_parquet" not in source_text
        assert "import delete_object" not in source_text

    def test_fs_rm_never_called_on_rename_failure(self):
        """Even when rename fails, no rm/delete is called to clean up the tmp."""
        from scripts.rewrite_parquet_layout import _apply_unit

        unit = _make_unit()
        table = _make_table(5)
        fs_mock = MagicMock()
        fs_mock.ls.return_value = []
        fs_mock.rename.side_effect = OSError("network error")

        rg = MagicMock()
        rg.num_rows = len(table)
        meta_mock = MagicMock()
        meta_mock.num_row_groups = 1
        meta_mock.row_group.return_value = rg

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=meta_mock),
        ):
            _apply_unit(unit, fs_mock, "bronze")

        fs_mock.rm.assert_not_called()
        fs_mock.delete_file.assert_not_called()


# ---------------------------------------------------------------------------
# Group M: --replace-existing-target (Phase 6 delta rewrite)
# ---------------------------------------------------------------------------


class TestReplaceExistingTarget:
    """Tests for --replace-existing-target finalization mode."""

    def test_cli_flag_accepted(self):
        from scripts.rewrite_parquet_layout import parse_args

        args = parse_args(["--all", "--apply", "--replace-existing-target"])
        assert args.replace_existing_target is True

    def test_replace_mode_does_not_skip_existing_target(self):
        """With replace_existing=True, a unit with existing normalized files proceeds."""
        unit = _make_unit()
        table = _make_table(10)
        existing = [
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old.parquet"
        ]

        result, _ = TestApplyFlow()._run_apply(
            unit, table, existing_parquet=existing, replace_existing=True
        )
        assert result.status == "ok"

    def test_replace_mode_calls_fs_rm_on_old_normalized_file(self):
        """fs.rm is called on the previously-existing normalized file after rename."""
        unit = _make_unit()
        table = _make_table(10)
        old_path = (
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old-uuid-0.parquet"
        )
        _, fs_mock = TestApplyFlow()._run_apply(
            unit, table, existing_parquet=[old_path], replace_existing=True
        )

        fs_mock.rm.assert_called_once_with(old_path)

    def test_replace_mode_rm_called_after_rename(self):
        """fs.rename is called before fs.rm (new file must be live before old is removed)."""
        unit = _make_unit()
        table = _make_table(5)
        old_path = (
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old.parquet"
        )

        call_order: list[str] = []

        def _track_rename(*args, **kwargs):
            call_order.append("rename")

        def _track_rm(*args, **kwargs):
            call_order.append("rm")

        _, fs_mock = TestApplyFlow()._run_apply(
            unit, table, existing_parquet=[old_path], replace_existing=True
        )
        fs_mock.rename.side_effect = None  # already called; just inspect call_args_list
        # Re-run with tracking side effects
        from scripts.rewrite_parquet_layout import _apply_unit

        fs_mock2 = MagicMock()
        fs_mock2.ls.return_value = [old_path]
        fs_mock2.rename.side_effect = _track_rename
        fs_mock2.rm.side_effect = _track_rm

        rg = MagicMock()
        rg.num_rows = len(table)
        meta = MagicMock()
        meta.num_row_groups = 1
        meta.row_group.return_value = rg

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=meta),
        ):
            _apply_unit(unit, fs_mock2, "bronze", replace_existing=True)

        assert call_order == ["rename", "rm"], (
            f"Expected rename before rm, got: {call_order}"
        )

    def test_replace_mode_replaced_files_count_in_result(self):
        """result.replaced_files equals the number of old normalized files deleted."""
        unit = _make_unit()
        table = _make_table(5)
        old_files = [
            "bronze/ops_normalized/price_observation_events/year=2026/month=6/part-a.parquet",
            "bronze/ops_normalized/price_observation_events/year=2026/month=6/part-b.parquet",
        ]
        result, _ = TestApplyFlow()._run_apply(
            unit, table, existing_parquet=old_files, replace_existing=True
        )

        assert result.replaced_files == 2

    def test_replace_mode_old_source_prefix_not_touched(self):
        """fs.rm is never called with old-prefix paths (silver/observations/ or ops/)."""
        unit = _make_unit()
        table = _make_table(5)
        old_normalized = [
            "bronze/ops_normalized/price_observation_events/year=2026/month=6/part-old.parquet"
        ]
        _, fs_mock = TestApplyFlow()._run_apply(
            unit, table, existing_parquet=old_normalized, replace_existing=True
        )

        for rm_call in fs_mock.rm.call_args_list:
            path = rm_call[0][0]
            assert "ops/price_observation_events/" not in path
            assert "silver/observations/" not in path

    def test_replace_mode_without_existing_target_succeeds_normally(self):
        """replace_existing=True on a unit with no existing target works like normal mode."""
        unit = _make_unit()
        table = _make_table(8)
        result, fs_mock = TestApplyFlow()._run_apply(
            unit, table, existing_parquet=[], replace_existing=True
        )

        assert result.status == "ok"
        fs_mock.rm.assert_not_called()

    def test_replace_mode_baseline_accepts_more_rows_than_baseline(self):
        """Delta mode: source_rows > baseline_rows is ok (final flush added rows)."""
        unit = _make_unit()
        table = _make_table(20)  # 20 source rows > baseline of 15

        result, _ = TestApplyFlow()._run_apply(
            unit, table, baseline_rows=15, replace_existing=True
        )
        assert result.status == "ok"
        assert result.baseline_mismatch is None

    def test_replace_mode_baseline_accepts_equal_rows(self):
        """Delta mode: source_rows == baseline_rows is ok (no new rows from flush)."""
        unit = _make_unit()
        table = _make_table(15)

        result, _ = TestApplyFlow()._run_apply(
            unit, table, baseline_rows=15, replace_existing=True
        )
        assert result.status == "ok"

    def test_replace_mode_baseline_fails_on_fewer_rows(self):
        """Delta mode: source_rows < baseline_rows → regression → fail before write."""
        unit = _make_unit()
        table = _make_table(10)  # 10 < baseline of 15 = data loss

        result, fs_mock = TestApplyFlow()._run_apply(
            unit, table, baseline_rows=15, replace_existing=True
        )
        assert result.status == "failed"
        assert "regression" in (result.baseline_mismatch or "").lower()
        assert result.baseline_mismatch is not None
        fs_mock.rename.assert_not_called()

    def test_replace_mode_baseline_regression_no_write_occurs(self):
        """pq.write_table is not called when baseline regression detected in replace mode."""
        unit = _make_unit()
        table = _make_table(5)

        fs_mock = MagicMock()
        fs_mock.ls.return_value = [
            "bronze/ops_normalized/price_observation_events/year=2026/month=6/part-old.parquet"
        ]

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table") as mock_write,
            patch("pyarrow.parquet.read_metadata"),
        ):
            from scripts.rewrite_parquet_layout import _apply_unit
            _apply_unit(unit, fs_mock, "bronze", baseline_rows=99, replace_existing=True)
            mock_write.assert_not_called()

    def test_replace_mode_dry_run_reports_would_replace(self):
        """In dry-run with replace_existing=True, status is ok and replaced_files is set."""
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        existing = [
            "bronze/ops_normalized/price_observation_events/year=2026/month=6/part-old.parquet"
        ]
        fs_mock = MagicMock()
        fs_mock.ls.return_value = existing

        with patch("pyarrow.parquet.read_metadata") as mock_meta:
            rg = MagicMock()
            rg.num_rows = 5
            mock_meta.return_value.num_row_groups = 1
            mock_meta.return_value.row_group.return_value = rg
            mock_meta.return_value.schema.to_arrow_schema.return_value = pa.schema([])
            result = _dry_run_unit(unit, fs_mock, "bronze", replace_existing=True)

        assert result.status == "ok"
        assert result.replaced_files == 1

    def test_replace_mode_dry_run_no_rm_called(self):
        """Dry-run with replace_existing=True never calls fs.rm."""
        from scripts.rewrite_parquet_layout import _dry_run_unit

        unit = _make_unit()
        existing = [
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old.parquet"
        ]
        fs_mock = MagicMock()
        fs_mock.ls.return_value = existing

        with patch("pyarrow.parquet.read_metadata") as mock_meta:
            mock_meta.return_value.num_row_groups = 0
            mock_meta.return_value.schema.to_arrow_schema.return_value = pa.schema([])
            _dry_run_unit(unit, fs_mock, "bronze", replace_existing=True)

        fs_mock.rm.assert_not_called()

    def test_replace_mode_report_includes_replaced_files_field(self):
        """The JSON report includes replaced_files for each unit."""
        from scripts.rewrite_parquet_layout import UnitResult, build_report

        r = UnitResult(
            dataset="price_observation_events",
            source=None,
            year=2026,
            month=6,
            source_prefix="ops/price_observation_events/year=2026/month=6/",
            target_prefix="ops_normalized/price_observation_events/year=2026/month=6/",
            source_files=2,
            source_bytes=2048,
            status="ok",
            replaced_files=1,
        )
        report = build_report(
            [r], dry_run=False, bucket="bronze", datasets=["price_observation_events"]
        )
        assert report["units"][0]["replaced_files"] == 1

    def test_replace_mode_rm_failure_sets_status_failed(self):
        """fs.rm failure after rename must set status=failed, not ok."""
        unit = _make_unit()
        table = _make_table(8)
        old_path = (
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old.parquet"
        )
        from scripts.rewrite_parquet_layout import _apply_unit

        fs_mock = MagicMock()
        fs_mock.ls.return_value = [old_path]
        fs_mock.rm.side_effect = OSError("permission denied")

        rg = MagicMock()
        rg.num_rows = len(table)
        meta = MagicMock()
        meta.num_row_groups = 1
        meta.row_group.return_value = rg

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=meta),
        ):
            result = _apply_unit(unit, fs_mock, "bronze", replace_existing=True)

        assert result.status == "failed"

    def test_replace_mode_rm_failure_records_target_file_for_recovery(self):
        """When rm fails, target_file is still set so the operator knows what was written."""
        unit = _make_unit()
        table = _make_table(8)
        old_path = (
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old.parquet"
        )
        from scripts.rewrite_parquet_layout import _apply_unit

        fs_mock = MagicMock()
        fs_mock.ls.return_value = [old_path]
        fs_mock.rm.side_effect = OSError("timeout")

        rg = MagicMock()
        rg.num_rows = len(table)
        meta = MagicMock()
        meta.num_row_groups = 1
        meta.row_group.return_value = rg

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=meta),
        ):
            result = _apply_unit(unit, fs_mock, "bronze", replace_existing=True)

        assert result.target_file is not None
        assert result.target_file.endswith(".parquet")

    def test_replace_mode_rm_failure_error_mentions_failed_path(self):
        """Error message names the file(s) that need manual cleanup."""
        unit = _make_unit()
        table = _make_table(8)
        old_path = (
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-old.parquet"
        )
        from scripts.rewrite_parquet_layout import _apply_unit

        fs_mock = MagicMock()
        fs_mock.ls.return_value = [old_path]
        fs_mock.rm.side_effect = OSError("network error")

        rg = MagicMock()
        rg.num_rows = len(table)
        meta = MagicMock()
        meta.num_row_groups = 1
        meta.row_group.return_value = rg

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=meta),
        ):
            result = _apply_unit(unit, fs_mock, "bronze", replace_existing=True)

        assert old_path in (result.error or "")

    def test_replace_mode_rm_failure_replaced_files_counts_successes_only(self):
        """replaced_files counts only successfully deleted files."""
        unit = _make_unit()
        table = _make_table(8)
        old_a = (
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-a.parquet"
        )
        old_b = (
            "bronze/ops_normalized/price_observation_events"
            "/year=2026/month=6/part-b.parquet"
        )
        from scripts.rewrite_parquet_layout import _apply_unit

        fs_mock = MagicMock()
        fs_mock.ls.return_value = [old_a, old_b]
        # First rm succeeds, second fails
        fs_mock.rm.side_effect = [None, OSError("timeout")]

        rg = MagicMock()
        rg.num_rows = len(table)
        meta = MagicMock()
        meta.num_row_groups = 1
        meta.row_group.return_value = rg

        with (
            patch("pyarrow.parquet.read_table", return_value=table),
            patch("pyarrow.parquet.write_table"),
            patch("pyarrow.parquet.read_metadata", return_value=meta),
        ):
            result = _apply_unit(unit, fs_mock, "bronze", replace_existing=True)

        assert result.status == "failed"
        assert result.replaced_files == 1  # one succeeded before the failure
