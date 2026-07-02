"""Unit tests for scripts/audit_parquet_layout.py

Groups:
  A - parser validation (--dataset / --all mutual exclusion and requirement)
  B - dataset-to-prefix mapping
  C - partition path parsing helpers
  D - small file detection
  E - unexpected path detection
  F - row count from Parquet metadata (real fixture, no full read)
  G - schema variant detection across sampled files (real fixtures)
  H - JSON output structure
  I - Markdown output structure
  J - no mutation guarantee (put/delete/copy never called)
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ── Shared fixtures ───────────────────────────────────────────────────────────


def _make_parquet(tmp_path: Path, name: str, schema: pa.Schema, rows: int = 5) -> Path:
    """Write a small Parquet file with synthetic data and return its path."""
    arrays = []
    for field in schema:
        if pa.types.is_int64(field.type) or pa.types.is_int32(field.type):
            arrays.append(pa.array(list(range(rows)), type=field.type))
        elif pa.types.is_string(field.type):
            arrays.append(pa.array([f"val_{i}" for i in range(rows)], type=field.type))
        elif pa.types.is_timestamp(field.type):
            import datetime
            ts_base = datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc)
            arrays.append(pa.array(
                [ts_base.replace(hour=i % 24) for i in range(rows)],
                type=field.type,
            ))
        else:
            arrays.append(pa.array([None] * rows, type=field.type))
    table = pa.table(dict(zip([f.name for f in schema], arrays)), schema=schema)
    path = tmp_path / name
    pq.write_table(table, str(path))
    return path


_SILVER_SCHEMA = pa.schema([
    pa.field("listing_id", pa.string()),
    pa.field("price", pa.int32()),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
])

_OPS_SCHEMA = pa.schema([
    pa.field("event_id", pa.int64()),
    pa.field("listing_id", pa.string()),
    pa.field("event_at", pa.timestamp("us", tz="UTC")),
])

_ALT_SCHEMA = pa.schema([
    pa.field("event_id", pa.int64()),
    pa.field("listing_id", pa.string()),
    pa.field("extra_col", pa.string()),
    pa.field("event_at", pa.timestamp("us", tz="UTC")),
])


def _mock_paginator(entries: list[tuple[str, int]]):
    """Build a mock boto3 paginator that returns the given (key, size) entries."""
    contents = [{"Key": k, "Size": s} for k, s in entries]
    page = {"Contents": contents} if contents else {}
    paginator = MagicMock()
    paginator.paginate.return_value = [page]
    client = MagicMock()
    client.get_paginator.return_value = paginator
    return client


# ── Group A: parser validation ────────────────────────────────────────────────


class TestParserValidation:
    def test_no_dataset_or_all_fails(self):
        from scripts.audit_parquet_layout import parse_args

        with pytest.raises(SystemExit) as exc:
            parse_args([])
        assert exc.value.code != 0

    def test_dataset_and_all_together_fails(self):
        from scripts.audit_parquet_layout import parse_args

        with pytest.raises(SystemExit) as exc:
            parse_args(["--all", "--dataset", "silver_observations"])
        assert exc.value.code != 0

    def test_all_alone_succeeds(self):
        from scripts.audit_parquet_layout import parse_args

        args = parse_args(["--all"])
        assert args.all is True
        assert args.datasets is None

    def test_single_dataset_succeeds(self):
        from scripts.audit_parquet_layout import parse_args

        args = parse_args(["--dataset", "silver_observations"])
        assert args.datasets == ["silver_observations"]
        assert args.all is False

    def test_multiple_datasets_repeatable(self):
        from scripts.audit_parquet_layout import parse_args

        args = parse_args([
            "--dataset", "silver_observations",
            "--dataset", "price_observation_events",
        ])
        assert "silver_observations" in args.datasets
        assert "price_observation_events" in args.datasets

    def test_invalid_dataset_name_fails(self):
        from scripts.audit_parquet_layout import parse_args

        with pytest.raises(SystemExit) as exc:
            parse_args(["--dataset", "nonexistent_table"])
        assert exc.value.code != 0

    def test_json_out_path_parsed(self, tmp_path):
        from scripts.audit_parquet_layout import parse_args

        out = tmp_path / "report.json"
        args = parse_args(["--all", "--json-out", str(out)])
        assert args.json_out == out

    def test_markdown_out_path_parsed(self, tmp_path):
        from scripts.audit_parquet_layout import parse_args

        out = tmp_path / "report.md"
        args = parse_args(["--all", "--markdown-out", str(out)])
        assert args.markdown_out == out

    def test_sample_files_default(self):
        from scripts.audit_parquet_layout import parse_args

        args = parse_args(["--all"])
        assert args.sample_files == 3

    def test_sample_files_override(self):
        from scripts.audit_parquet_layout import parse_args

        args = parse_args(["--all", "--sample-files", "10"])
        assert args.sample_files == 10


# ── Group B: dataset-to-prefix mapping ───────────────────────────────────────


class TestDatasetPrefixMapping:
    @pytest.mark.parametrize("dataset,expected_prefix", [
        ("silver_observations", "silver/observations/"),
        ("price_observation_events", "ops/price_observation_events/"),
        ("vin_to_listing_events", "ops/vin_to_listing_events/"),
        ("blocked_cooldown_events", "ops/blocked_cooldown_events/"),
        ("detail_scrape_claim_events", "ops/detail_scrape_claim_events/"),
        ("artifacts_queue_events", "ops/artifacts_queue_events/"),
    ])
    def test_prefix_correct(self, dataset, expected_prefix):
        from scripts.audit_parquet_layout import DATASET_CONFIGS

        assert DATASET_CONFIGS[dataset]["prefix"] == expected_prefix

    def test_all_supported_datasets_have_config(self):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, SUPPORTED_DATASETS

        for name in SUPPORTED_DATASETS:
            assert name in DATASET_CONFIGS
            config = DATASET_CONFIGS[name]
            assert "prefix" in config
            assert "expected_pattern" in config
            assert "partition_template" in config


# ── Group C: partition path helpers ──────────────────────────────────────────


class TestPartitionPathHelpers:
    def test_partition_path_of_silver(self):
        from scripts.audit_parquet_layout import _partition_path_of

        key = (
            "silver/observations/source=detail"
            "/obs_year=2026/obs_month=6/obs_day=15/part-abc.parquet"
        )
        assert _partition_path_of(key) == (
            "silver/observations/source=detail/obs_year=2026/obs_month=6/obs_day=15/"
        )

    def test_partition_path_of_ops(self):
        from scripts.audit_parquet_layout import _partition_path_of

        key = "ops/price_observation_events/year=2026/month=6/part-abc.parquet"
        assert _partition_path_of(key) == "ops/price_observation_events/year=2026/month=6/"

    _SILVER_MATCH = [
        "silver/observations/source=detail/obs_year=2026/obs_month=6/obs_day=15/part-abc.parquet",
        (
            "silver/observations/source=carousel"
            "/obs_year=2025/obs_month=12/obs_day=31/compacted-2025-12-31.parquet"
        ),
    ]

    @pytest.mark.parametrize("key", _SILVER_MATCH)
    def test_silver_expected_pattern_matches(self, key):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, _is_expected_key

        pattern = DATASET_CONFIGS["silver_observations"]["expected_pattern"]
        assert _is_expected_key(key, pattern)

    _SILVER_REJECT = [
        # missing obs_day
        "silver/observations/source=detail/obs_year=2026/obs_month=6/part-abc.parquet",
        "silver/observations/part-abc.parquet",  # flat
        # wrong ext
        "silver/observations/source=detail/obs_year=2026/obs_month=6/obs_day=15/file.csv",
        # directory
        "silver/observations/source=detail/obs_year=2026/obs_month=6/obs_day=15/",
    ]

    @pytest.mark.parametrize("key", _SILVER_REJECT)
    def test_silver_expected_pattern_rejects(self, key):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, _is_expected_key

        pattern = DATASET_CONFIGS["silver_observations"]["expected_pattern"]
        assert not _is_expected_key(key, pattern)

    _OPS_MATCH = [
        (
            "price_observation_events",
            "ops/price_observation_events/year=2026/month=6/part-abc.parquet",
        ),
        (
            "vin_to_listing_events",
            "ops/vin_to_listing_events/year=2025/month=12/part-xyz.parquet",
        ),
        (
            "artifacts_queue_events",
            "ops/artifacts_queue_events/year=2026/month=1/compacted.parquet",
        ),
    ]

    @pytest.mark.parametrize("table,key", _OPS_MATCH)
    def test_ops_expected_pattern_matches(self, table, key):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, _is_expected_key

        pattern = DATASET_CONFIGS[table]["expected_pattern"]
        assert _is_expected_key(key, pattern)

    _OPS_REJECT = [
        # no month
        (
            "price_observation_events",
            "ops/price_observation_events/year=2026/part-abc.parquet",
        ),
        # wrong table
        (
            "price_observation_events",
            "ops/other_table/year=2026/month=6/part-abc.parquet",
        ),
        # dir
        (
            "price_observation_events",
            "ops/price_observation_events/year=2026/month=6/",
        ),
    ]

    @pytest.mark.parametrize("table,key", _OPS_REJECT)
    def test_ops_expected_pattern_rejects(self, table, key):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, _is_expected_key

        pattern = DATASET_CONFIGS[table]["expected_pattern"]
        assert not _is_expected_key(key, pattern)


# ── Group D: small file detection ─────────────────────────────────────────────


class TestSmallFileDetection:
    def test_small_file_counted(self):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, audit_dataset

        key = "ops/price_observation_events/year=2026/month=6/part-small.parquet"
        size = 512 * 1024  # 512 KiB — under 1 MiB threshold

        client = _mock_paginator([(key, size)])
        with patch("scripts.audit_parquet_layout.read_file_metadata", return_value=None):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=1,
            )

        assert result["small_files"] == 1

    def test_large_file_not_counted(self):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, audit_dataset

        key = "ops/price_observation_events/year=2026/month=6/part-large.parquet"
        size = 5 * 1024 * 1024  # 5 MiB

        client = _mock_paginator([(key, size)])
        with patch("scripts.audit_parquet_layout.read_file_metadata", return_value=None):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=1,
            )

        assert result["small_files"] == 0

    def test_small_file_threshold_is_one_mib(self):
        from scripts.audit_parquet_layout import SMALL_FILE_THRESHOLD

        assert SMALL_FILE_THRESHOLD == 1 * 1024 * 1024

    def test_mixed_sizes(self):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, audit_dataset

        entries = [
            ("ops/price_observation_events/year=2026/month=6/part-a.parquet", 512 * 1024),
            ("ops/price_observation_events/year=2026/month=6/part-b.parquet", 2 * 1024 * 1024),
            ("ops/price_observation_events/year=2026/month=6/part-c.parquet", 100),
        ]
        client = _mock_paginator(entries)
        with patch("scripts.audit_parquet_layout.read_file_metadata", return_value=None):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=0,
            )

        assert result["small_files"] == 2  # 512 KiB and 100 B
        assert result["total_objects"] == 3
        assert result["total_bytes"] == 512 * 1024 + 2 * 1024 * 1024 + 100


# ── Group E: unexpected path detection ───────────────────────────────────────


class TestUnexpectedPathDetection:
    def test_unexpected_path_detected(self):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, audit_dataset

        good_key = (
            "silver/observations/source=detail"
            "/obs_year=2026/obs_month=6/obs_day=15/part-a.parquet"
        )
        bad_key = "silver/observations/orphan_file.parquet"  # no partition depth

        entries = [
            (good_key, 1024 * 1024),
            (bad_key, 500),
        ]
        client = _mock_paginator(entries)
        with patch("scripts.audit_parquet_layout.read_file_metadata", return_value=None):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "silver_observations",
                DATASET_CONFIGS["silver_observations"],
                sample_files=1,
            )

        assert bad_key in result["unexpected_paths"]
        assert good_key not in result["unexpected_paths"]
        assert result["total_objects"] == 2

    def test_no_unexpected_paths_when_all_conform(self):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, audit_dataset

        key = (
            "silver/observations/source=detail"
            "/obs_year=2026/obs_month=6/obs_day=15/part-a.parquet"
        )
        client = _mock_paginator([(key, 2 * 1024 * 1024)])
        with patch("scripts.audit_parquet_layout.read_file_metadata", return_value=None):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "silver_observations",
                DATASET_CONFIGS["silver_observations"],
                sample_files=1,
            )

        assert result["unexpected_paths"] == []


# ── Group F: row count from Parquet metadata ──────────────────────────────────


class TestRowCountFromMetadata:
    def test_row_count_matches_fixture_without_full_read(self, tmp_path):
        """read_file_metadata returns correct row count from footer metadata only."""
        parquet_path = _make_parquet(tmp_path, "test.parquet", _SILVER_SCHEMA, rows=7)

        # read_file_metadata in the script uses pq.read_metadata with filesystem.
        # Here we bypass the filesystem by patching pq.read_metadata to use the
        # local file, verifying the mechanism works.
        actual_meta = pq.read_metadata(str(parquet_path))

        with patch("pyarrow.parquet.read_metadata", return_value=actual_meta):
            from scripts.audit_parquet_layout import read_file_metadata

            info = read_file_metadata("bronze", "any/key.parquet", MagicMock())

        assert info is not None
        assert info.rows == 7

    def test_row_count_zero_for_empty_fixture(self, tmp_path):
        parquet_path = _make_parquet(tmp_path, "empty.parquet", _SILVER_SCHEMA, rows=0)
        actual_meta = pq.read_metadata(str(parquet_path))

        with patch("pyarrow.parquet.read_metadata", return_value=actual_meta):
            from scripts.audit_parquet_layout import read_file_metadata

            info = read_file_metadata("bronze", "any/key.parquet", MagicMock())

        assert info is not None
        assert info.rows == 0

    def test_metadata_read_failure_returns_none(self):
        """If pq.read_metadata raises, read_file_metadata returns None without crashing."""
        with patch("pyarrow.parquet.read_metadata", side_effect=OSError("not found")):
            from scripts.audit_parquet_layout import read_file_metadata

            info = read_file_metadata("bronze", "missing/key.parquet", MagicMock())

        assert info is None

    def test_partition_rows_aggregated_from_all_files(self, tmp_path):
        """audit_dataset sums rows from ALL files in a partition, not just sample_files."""
        from scripts.audit_parquet_layout import DATASET_CONFIGS, FileMetaInfo, audit_dataset

        # 5 files in the same partition; sample_files=2 should still give rows from all 5
        keys = [
            f"ops/price_observation_events/year=2026/month=6/part-{i}.parquet"
            for i in range(5)
        ]
        client = _mock_paginator([(k, 2 * 1024 * 1024) for k in keys])

        def _fake_meta(bucket, key, fs):
            return FileMetaInfo(rows=10, schema_fingerprint="abc123", ts_min=None, ts_max=None)

        with patch("scripts.audit_parquet_layout.read_file_metadata", side_effect=_fake_meta):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=2,  # deliberately lower than file count
            )

        assert len(result["partitions"]) == 1
        part = result["partitions"][0]
        assert part["rows"] == 50  # all 5 files × 10 rows each
        assert part["metadata_sampled"] == 5
        assert part["metadata_read"] == 5
        assert part["metadata_failures"] == 0

    def test_schema_sampling_limited_by_sample_files(self, tmp_path):
        """Schema fingerprints are collected from at most sample_files per partition."""
        from scripts.audit_parquet_layout import (
            DATASET_CONFIGS,
            FileMetaInfo,
            _schema_fingerprint,
            audit_dataset,
        )

        # 5 files with 3 different schema fingerprints cycling
        fp_a = _schema_fingerprint(_OPS_SCHEMA)
        fp_b = _schema_fingerprint(_ALT_SCHEMA)
        fps_cycle = [fp_a, fp_b, fp_a, fp_b, fp_a]

        keys = [
            f"ops/price_observation_events/year=2026/month=6/part-{i}.parquet"
            for i in range(5)
        ]
        client = _mock_paginator([(k, 2 * 1024 * 1024) for k in keys])

        _calls = [0]

        def _cycling_meta(bucket, key, fs):
            fp = fps_cycle[_calls[0] % len(fps_cycle)]
            _calls[0] += 1
            return FileMetaInfo(rows=5, schema_fingerprint=fp, ts_min=None, ts_max=None)

        with patch("scripts.audit_parquet_layout.read_file_metadata", side_effect=_cycling_meta):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=1,  # only first file contributes to schema
            )

        # With sample_files=1, only the first fingerprint is seen
        assert result["schema_variants"] == 1
        # But row count is still from all 5 files
        assert result["partitions"][0]["rows"] == 25

    def test_rows_null_when_all_metadata_reads_fail(self, tmp_path):
        """rows is None (not 0) when every footer read fails for a partition."""
        from scripts.audit_parquet_layout import DATASET_CONFIGS, audit_dataset

        key = "ops/price_observation_events/year=2026/month=6/part-a.parquet"
        client = _mock_paginator([(key, 2 * 1024 * 1024)])

        with patch(
            "scripts.audit_parquet_layout.read_file_metadata", return_value=None
        ):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=3,
            )

        part = result["partitions"][0]
        assert part["rows"] is None  # not 0
        assert part["metadata_sampled"] == 1
        assert part["metadata_read"] == 0
        assert part["metadata_failures"] == 1


# ── Group G: schema variant detection ────────────────────────────────────────


class TestSchemaVariantDetection:
    def test_two_files_same_schema_one_variant(self, tmp_path):
        from scripts.audit_parquet_layout import (
            DATASET_CONFIGS,
            FileMetaInfo,
            _schema_fingerprint,
            audit_dataset,
        )

        fp = _schema_fingerprint(_OPS_SCHEMA)

        key_a = "ops/price_observation_events/year=2026/month=6/part-a.parquet"
        key_b = "ops/price_observation_events/year=2026/month=6/part-b.parquet"
        client = _mock_paginator([
            (key_a, 2 * 1024 * 1024),
            (key_b, 2 * 1024 * 1024),
        ])

        def _same_schema(bucket, key, fs):
            return FileMetaInfo(rows=5, schema_fingerprint=fp, ts_min=None, ts_max=None)

        with patch("scripts.audit_parquet_layout.read_file_metadata", side_effect=_same_schema):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=5,
            )

        assert result["schema_variants"] == 1

    def test_two_files_different_schema_two_variants(self, tmp_path):
        from scripts.audit_parquet_layout import (
            DATASET_CONFIGS,
            FileMetaInfo,
            _schema_fingerprint,
            audit_dataset,
        )

        fp_a = _schema_fingerprint(_OPS_SCHEMA)
        fp_b = _schema_fingerprint(_ALT_SCHEMA)
        assert fp_a != fp_b

        key_a = "ops/price_observation_events/year=2026/month=6/part-a.parquet"
        key_b = "ops/price_observation_events/year=2026/month=7/part-b.parquet"
        client = _mock_paginator([
            (key_a, 2 * 1024 * 1024),
            (key_b, 2 * 1024 * 1024),
        ])

        _fps = [fp_a, fp_b]
        _calls = [0]

        def _alternating_schema(bucket, key, fs):
            idx = _calls[0] % 2
            _calls[0] += 1
            return FileMetaInfo(rows=5, schema_fingerprint=_fps[idx], ts_min=None, ts_max=None)

        _patch = "scripts.audit_parquet_layout.read_file_metadata"
        with patch(_patch, side_effect=_alternating_schema):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=5,
            )

        assert result["schema_variants"] == 2

    def test_schema_fingerprint_differs_for_different_schemas(self):
        from scripts.audit_parquet_layout import _schema_fingerprint

        fp1 = _schema_fingerprint(_OPS_SCHEMA)
        fp2 = _schema_fingerprint(_ALT_SCHEMA)
        assert fp1 != fp2

    def test_schema_fingerprint_same_for_identical_schemas(self):
        from scripts.audit_parquet_layout import _schema_fingerprint

        schema_copy = pa.schema([
            pa.field("event_id", pa.int64()),
            pa.field("listing_id", pa.string()),
            pa.field("event_at", pa.timestamp("us", tz="UTC")),
        ])
        assert _schema_fingerprint(_OPS_SCHEMA) == _schema_fingerprint(schema_copy)

    def test_schema_fingerprint_same_for_reordered_columns(self):
        """Column reordering does not produce a different fingerprint (logical compat)."""
        from scripts.audit_parquet_layout import _schema_fingerprint

        schema_orig = pa.schema([
            pa.field("event_id", pa.int64()),
            pa.field("listing_id", pa.string()),
            pa.field("event_at", pa.timestamp("us", tz="UTC")),
        ])
        schema_reordered = pa.schema([
            pa.field("listing_id", pa.string()),
            pa.field("event_at", pa.timestamp("us", tz="UTC")),
            pa.field("event_id", pa.int64()),
        ])
        assert _schema_fingerprint(schema_orig) == _schema_fingerprint(schema_reordered)

    def test_schema_variants_from_real_fixtures(self, tmp_path):
        """Integration: two local Parquet fixtures with different schemas → 2 variants."""
        from scripts.audit_parquet_layout import (
            DATASET_CONFIGS,
            FileMetaInfo,
            _schema_fingerprint,
            audit_dataset,
        )

        path_a = _make_parquet(tmp_path, "a.parquet", _OPS_SCHEMA, rows=3)
        path_b = _make_parquet(tmp_path, "b.parquet", _ALT_SCHEMA, rows=4)

        meta_a = pq.read_metadata(str(path_a))
        meta_b = pq.read_metadata(str(path_b))

        key_a = "ops/price_observation_events/year=2026/month=6/part-a.parquet"
        key_b = "ops/price_observation_events/year=2026/month=7/part-b.parquet"

        client = _mock_paginator([
            (key_a, 2 * 1024 * 1024),
            (key_b, 2 * 1024 * 1024),
        ])

        _metas = {key_a: meta_a, key_b: meta_b}

        def _real_meta(bucket, key, fs):
            m = _metas.get(key)
            if m is None:
                return None
            schema = m.schema.to_arrow_schema()
            return FileMetaInfo(
                rows=sum(m.row_group(i).num_rows for i in range(m.num_row_groups)),
                schema_fingerprint=_schema_fingerprint(schema),
                ts_min=None,
                ts_max=None,
            )

        with patch("scripts.audit_parquet_layout.read_file_metadata", side_effect=_real_meta):
            result = audit_dataset(
                client, MagicMock(), "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=5,
            )

        assert result["schema_variants"] == 2


# ── Group H: JSON output ──────────────────────────────────────────────────────


class TestJsonOutput:
    def _make_mock_result(self, name: str) -> dict:
        return {
            "prefix": f"ops/{name}/",
            "expected_partition_pattern": "year=<Y>/month=<M>/",
            "total_objects": 10,
            "total_bytes": 50 * 1024 * 1024,
            "partition_count": 3,
            "small_files": 1,
            "schema_variants": 1,
            "unexpected_paths": [],
            "partitions": [
                {
                    "path": f"ops/{name}/year=2026/month=6/",
                    "objects": 10,
                    "bytes": 50 * 1024 * 1024,
                    "rows": 1000,
                    "metadata_sampled": 10,
                    "metadata_read": 10,
                    "metadata_failures": 0,
                    "schema_fingerprint": "abc123def456",
                    "ts_min": "2026-06-01T00:00:00+00:00",
                    "ts_max": "2026-06-30T23:59:59+00:00",
                }
            ],
        }

    def test_json_contains_dataset_keys(self, tmp_path):
        from scripts.audit_parquet_layout import build_json_report

        results = {
            "silver_observations": self._make_mock_result("silver_observations"),
            "price_observation_events": self._make_mock_result("price_observation_events"),
        }
        report = build_json_report(results)

        assert "generated_at" in report
        assert "datasets" in report
        assert "silver_observations" in report["datasets"]
        assert "price_observation_events" in report["datasets"]

    def test_json_output_valid_and_serializable(self, tmp_path):
        from scripts.audit_parquet_layout import build_json_report

        results = {"silver_observations": self._make_mock_result("silver_observations")}
        report = build_json_report(results)

        # Must round-trip through JSON without error
        raw = json.dumps(report, indent=2)
        parsed = json.loads(raw)
        assert parsed["datasets"]["silver_observations"]["total_objects"] == 10

    def test_json_written_to_path(self, tmp_path):
        from scripts.audit_parquet_layout import build_json_report

        results = {"silver_observations": self._make_mock_result("silver_observations")}
        report = build_json_report(results)
        out = tmp_path / "audit.json"
        out.write_text(json.dumps(report, indent=2))

        data = json.loads(out.read_text())
        assert "datasets" in data
        assert "silver_observations" in data["datasets"]

    def test_json_dataset_has_required_fields(self):
        from scripts.audit_parquet_layout import build_json_report

        results = {"price_observation_events": self._make_mock_result("price_observation_events")}
        report = build_json_report(results)
        ds = report["datasets"]["price_observation_events"]

        required = [
            "prefix", "expected_partition_pattern", "total_objects", "total_bytes",
            "partition_count", "small_files", "schema_variants",
            "unexpected_paths", "partitions",
        ]
        for key in required:
            assert key in ds, f"Missing key: {key}"

    def test_json_partition_has_required_fields(self):
        from scripts.audit_parquet_layout import build_json_report

        results = {"price_observation_events": self._make_mock_result("price_observation_events")}
        report = build_json_report(results)
        part = report["datasets"]["price_observation_events"]["partitions"][0]

        required = [
            "path", "objects", "bytes", "rows",
            "metadata_sampled", "metadata_read", "metadata_failures",
            "schema_fingerprint", "ts_min", "ts_max",
        ]
        for key in required:
            assert key in part, f"Missing partition key: {key}"


# ── Group I: Markdown output ──────────────────────────────────────────────────


class TestMarkdownOutput:
    def _make_report(self) -> dict:
        return {
            "generated_at": "2026-07-01T10:00:00+00:00",
            "datasets": {
                "silver_observations": {
                    "prefix": "silver/observations/",
                    "expected_partition_pattern": (
                        "source=<source>/obs_year=<Y>/obs_month=<M>/obs_day=<D>/"
                    ),
                    "total_objects": 100,
                    "total_bytes": 500 * 1024 * 1024,
                    "partition_count": 10,
                    "small_files": 5,
                    "schema_variants": 1,
                    "unexpected_paths": ["silver/observations/stray.csv"],
                    "partitions": [
                        {
                            "path": (
                                "silver/observations/source=detail"
                                "/obs_year=2026/obs_month=6/obs_day=15/"
                            ),
                            "objects": 1,
                            "bytes": 8 * 1024 * 1024,
                            "rows": 37412,
                            "metadata_sampled": 1,
                            "metadata_read": 1,
                            "metadata_failures": 0,
                            "schema_fingerprint": "abc123def456",
                            "ts_min": "2026-06-15T00:00:00+00:00",
                            "ts_max": "2026-06-15T23:59:59+00:00",
                        }
                    ],
                }
            },
        }

    def test_markdown_contains_pipe_rows(self):
        from scripts.audit_parquet_layout import build_markdown_report

        md = build_markdown_report(self._make_report())
        pipe_lines = [line for line in md.splitlines() if "|" in line]
        assert len(pipe_lines) > 0

    def test_markdown_contains_dataset_name(self):
        from scripts.audit_parquet_layout import build_markdown_report

        md = build_markdown_report(self._make_report())
        assert "silver_observations" in md

    def test_markdown_contains_partition_info(self):
        from scripts.audit_parquet_layout import build_markdown_report

        md = build_markdown_report(self._make_report())
        assert "source=detail" in md

    def test_markdown_unexpected_paths_listed(self):
        from scripts.audit_parquet_layout import build_markdown_report

        md = build_markdown_report(self._make_report())
        assert "stray.csv" in md

    def test_markdown_written_to_path(self, tmp_path):
        from scripts.audit_parquet_layout import build_markdown_report

        out = tmp_path / "audit.md"
        md = build_markdown_report(self._make_report())
        out.write_text(md)

        text = out.read_text()
        assert "|" in text
        assert "silver_observations" in text

    def test_markdown_summary_table_headers(self):
        from scripts.audit_parquet_layout import build_markdown_report

        md = build_markdown_report(self._make_report())
        assert "Objects" in md
        assert "Total Size" in md
        assert "Partitions" in md


# ── Group J: no mutation guarantee ───────────────────────────────────────────


class TestNoMutation:
    def _run_audit(self, client, fs_mock=None):
        from scripts.audit_parquet_layout import DATASET_CONFIGS, audit_dataset

        key = "ops/price_observation_events/year=2026/month=6/part-a.parquet"
        entries = [(key, 2 * 1024 * 1024)]

        paginator = MagicMock()
        contents = [{"Key": k, "Size": s} for k, s in entries]
        paginator.paginate.return_value = [{"Contents": contents}]
        client.get_paginator.return_value = paginator

        if fs_mock is None:
            fs_mock = MagicMock()

        with patch("scripts.audit_parquet_layout.read_file_metadata", return_value=None):
            audit_dataset(
                client, fs_mock, "bronze",
                "price_observation_events",
                DATASET_CONFIGS["price_observation_events"],
                sample_files=1,
            )
        return client, fs_mock

    def test_put_object_never_called(self):
        client = MagicMock()
        self._run_audit(client)
        client.put_object.assert_not_called()

    def test_delete_object_never_called(self):
        client = MagicMock()
        self._run_audit(client)
        client.delete_object.assert_not_called()

    def test_copy_object_never_called(self):
        client = MagicMock()
        self._run_audit(client)
        client.copy_object.assert_not_called()

    def test_fs_rm_never_called(self):
        client = MagicMock()
        fs_mock = MagicMock()
        self._run_audit(client, fs_mock)
        fs_mock.rm.assert_not_called()

    def test_fs_put_never_called(self):
        client = MagicMock()
        fs_mock = MagicMock()
        self._run_audit(client, fs_mock)
        # s3fs uses 'open' in write mode or 'put' — neither should be called
        fs_mock.put.assert_not_called()

    def test_never_calls_rename_on_fs(self):
        client = MagicMock()
        fs_mock = MagicMock()
        self._run_audit(client, fs_mock)
        fs_mock.rename.assert_not_called()
        fs_mock.copy.assert_not_called()
        fs_mock.move.assert_not_called()
