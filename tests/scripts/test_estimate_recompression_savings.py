"""Unit tests for scripts/estimate_recompression_savings.py

Groups:
  A - zstd round-trip math
  B - measure_object with mocked boto3
  C - build_prefixes and prefix construction
  D - sampler logic
  E - extrapolation math
  F - failure accumulation
  G - read-only contract (no put/delete/copy ever called)
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import zstandard as zstd

# ── Shared helpers ────────────────────────────────────────────────────────────

def _compress(data: bytes, level: int = 3) -> bytes:
    return zstd.ZstdCompressor(level=level).compress(data)


def _make_client(compressed_body: bytes | None = None, error: Exception | None = None):
    """Return a mock boto3 client whose get_object returns compressed_body."""
    client = MagicMock()
    if error is not None:
        client.get_object.side_effect = error
    else:
        body = MagicMock()
        body.read.return_value = compressed_body or b""
        client.get_object.return_value = {"Body": body}
    return client


def _obj(
    key: str = "html/year=2026/month=6/artifact_type=detail_page/aaa.html.zst",
    size: int = 100,
):
    from scripts.estimate_recompression_savings import ObjectInfo
    return ObjectInfo(key=key, size=size)


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        prefix=None, year=None, month=None,
        artifact_type="detail_page", bucket="bronze",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ── Group A: zstd round-trip math ────────────────────────────────────────────

class TestZstdRoundTrip:
    def test_measure_correct_fields(self):
        from scripts.estimate_recompression_savings import measure_object
        html = b"<html>" + b"A" * 10_000 + b"</html>"
        compressed = _compress(html, level=3)
        client = _make_client(compressed)
        result = measure_object(client, "bronze", _obj(size=len(compressed)))
        assert result.error is None
        assert result.old_compressed == len(compressed)
        assert result.raw_bytes == len(html)
        assert result.new_compressed > 0
        assert result.saved_bytes == result.old_compressed - result.new_compressed

    def test_level9_smaller_than_level3_for_repetitive_html(self):
        from scripts.estimate_recompression_savings import measure_object
        html = b"<html><body>" * 5_000  # highly repetitive → level 9 wins
        compressed = _compress(html, level=3)
        client = _make_client(compressed)
        result = measure_object(client, "bronze", _obj())
        assert result.error is None
        assert result.new_compressed <= result.old_compressed

    def test_saved_bytes_exact(self):
        from scripts.estimate_recompression_savings import measure_object
        html = b"hello world content " * 1_000
        compressed = _compress(html, level=3)
        client = _make_client(compressed)
        result = measure_object(client, "bronze", _obj())
        assert result.saved_bytes == result.old_compressed - result.new_compressed

    def test_incompressible_data_no_crash(self):
        import os

        from scripts.estimate_recompression_savings import measure_object
        random_bytes = os.urandom(10_000)
        compressed = _compress(random_bytes, level=3)
        client = _make_client(compressed)
        result = measure_object(client, "bronze", _obj())
        # Should not crash; savings may be zero or negative
        assert result.error is None
        assert result.old_compressed == len(compressed)
        assert result.raw_bytes == len(random_bytes)


# ── Group B: measure_object with mocked boto3 ────────────────────────────────

class TestMeasureObject:
    def test_correct_result_fields(self):
        from scripts.estimate_recompression_savings import measure_object
        html = b"<p>test content</p>" * 500
        compressed = _compress(html, level=3)
        result = measure_object(_make_client(compressed), "bronze", _obj())
        assert result.error is None
        assert result.old_compressed == len(compressed)
        assert result.raw_bytes == len(html)
        assert result.new_compressed > 0

    def test_put_object_never_called(self):
        from scripts.estimate_recompression_savings import measure_object
        client = _make_client(_compress(b"<html>x</html>" * 100))
        measure_object(client, "bronze", _obj())
        client.put_object.assert_not_called()

    def test_delete_object_never_called(self):
        from scripts.estimate_recompression_savings import measure_object
        client = _make_client(_compress(b"<html>x</html>" * 100))
        measure_object(client, "bronze", _obj())
        client.delete_object.assert_not_called()

    def test_copy_object_never_called(self):
        from scripts.estimate_recompression_savings import measure_object
        client = _make_client(_compress(b"<html>x</html>" * 100))
        measure_object(client, "bronze", _obj())
        client.copy_object.assert_not_called()

    def test_client_error_sets_error_field(self):
        from botocore.exceptions import ClientError

        from scripts.estimate_recompression_savings import measure_object
        err = ClientError({"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject")
        result = measure_object(_make_client(error=err), "bronze", _obj())
        assert result.error is not None
        assert len(result.error) > 0

    def test_client_error_does_not_raise(self):
        from botocore.exceptions import ClientError

        from scripts.estimate_recompression_savings import measure_object
        err = ClientError({"Error": {"Code": "500", "Message": ""}}, "GetObject")
        result = measure_object(_make_client(error=err), "bronze", _obj())
        assert result is not None  # no exception propagated

    def test_corrupt_zstd_sets_error_field(self):
        from scripts.estimate_recompression_savings import measure_object
        result = measure_object(_make_client(b"not_valid_zstd_garbage"), "bronze", _obj())
        assert result.error is not None

    def test_corrupt_zstd_does_not_raise(self):
        from scripts.estimate_recompression_savings import measure_object
        result = measure_object(_make_client(b"\x00\x01\x02\x03"), "bronze", _obj())
        assert result is not None


# ── Group C: build_prefixes and prefix construction ───────────────────────────

class TestBuildPrefixes:
    def test_prefix_passthrough(self):
        from scripts.estimate_recompression_savings import build_prefixes
        args = _args(prefix="html/year=2026/month=5/artifact_type=detail_page/")
        assert build_prefixes(args, MagicMock()) == [
            "html/year=2026/month=5/artifact_type=detail_page/"
        ]

    def test_year_and_month_constructs_prefix(self):
        from scripts.estimate_recompression_savings import build_prefixes
        args = _args(year=2026, month=5, artifact_type="detail_page")
        assert build_prefixes(args, MagicMock()) == [
            "html/year=2026/month=5/artifact_type=detail_page/"
        ]

    def test_results_page_artifact_type(self):
        from scripts.estimate_recompression_savings import build_prefixes
        args = _args(year=2026, month=3, artifact_type="results_page")
        result = build_prefixes(args, MagicMock())
        assert result == ["html/year=2026/month=3/artifact_type=results_page/"]

    def test_year_without_month_calls_discover(self):
        from scripts.estimate_recompression_savings import build_prefixes
        args = _args(year=2026, month=None, artifact_type="detail_page", bucket="bronze")
        with patch(
            "scripts.estimate_recompression_savings.discover_months_for_year",
            return_value=[(2026, 3), (2026, 4)],
        ) as mock_discover:
            result = build_prefixes(args, MagicMock())
        mock_discover.assert_called_once()
        assert sorted(result) == [
            "html/year=2026/month=3/artifact_type=detail_page/",
            "html/year=2026/month=4/artifact_type=detail_page/",
        ]

    def test_month_without_year_argparse_error(self):
        result = subprocess.run(
            [sys.executable, "scripts/estimate_recompression_savings.py", "--month", "5"],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parents[2]),
        )
        assert result.returncode != 0

    def test_no_selector_argparse_error(self):
        result = subprocess.run(
            [sys.executable, "scripts/estimate_recompression_savings.py"],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parents[2]),
        )
        assert result.returncode != 0


# ── Group D: sampler logic ────────────────────────────────────────────────────

class TestSampler:
    def test_systematic_rate_half(self):
        from scripts.estimate_recompression_savings import make_sampler
        sampler = make_sampler(0.5, False)  # stride = round(1/0.5) = 2
        sampled = [i for i in range(10) if sampler(i)]
        assert sampled == [0, 2, 4, 6, 8]

    def test_systematic_rate_tenth(self):
        from scripts.estimate_recompression_savings import make_sampler
        sampler = make_sampler(0.1, False)  # stride = 10
        sampled = [i for i in range(100) if sampler(i)]
        assert len(sampled) == 10

    def test_systematic_full_rate(self):
        from scripts.estimate_recompression_savings import make_sampler
        sampler = make_sampler(1.0, False)  # stride = 1, all sampled
        sampled = [i for i in range(10) if sampler(i)]
        assert sampled == list(range(10))

    def test_bernoulli_rate_zero_none_sampled(self):
        from scripts.estimate_recompression_savings import make_sampler
        with patch("scripts.estimate_recompression_savings.random.random", return_value=0.5):
            sampler = make_sampler(0.0, True)  # 0.5 < 0.0 is always False
            sampled = [i for i in range(10) if sampler(i)]
        assert sampled == []

    def test_bernoulli_rate_one_all_sampled(self):
        from scripts.estimate_recompression_savings import make_sampler
        sampler = make_sampler(1.0, True)  # random.random() < 1.0 always True
        sampled = [i for i in range(10) if sampler(i)]
        assert len(sampled) == 10

    def test_systematic_reproducible(self):
        from scripts.estimate_recompression_savings import make_sampler
        sampler1 = make_sampler(0.2, False)
        sampler2 = make_sampler(0.2, False)
        assert [sampler1(i) for i in range(20)] == [sampler2(i) for i in range(20)]


# ── Group E: extrapolation math ───────────────────────────────────────────────

class TestExtrapolationMath:
    def test_20pct_savings_projection(self, capsys):
        from scripts.estimate_recompression_savings import Stats, print_summary
        stats = Stats(
            scanned=1_000,
            sampled=50,
            skipped=950,
            failed=0,
            listed_bytes=1_000_000,
            old_compressed_bytes=50_000,
            raw_bytes_total=300_000,
            new_compressed_bytes=40_000,  # 20% savings
        )
        print_summary(stats, sample_rate=0.05)
        out = capsys.readouterr().out
        assert "20.0%" in out

    def test_projection_includes_listed_bytes(self, tmp_path):
        from scripts.estimate_recompression_savings import Stats, print_summary
        stats = Stats(
            scanned=100,
            sampled=10,
            skipped=90,
            failed=0,
            listed_bytes=1_000_000,
            old_compressed_bytes=50_000,
            raw_bytes_total=250_000,
            new_compressed_bytes=40_000,  # 10_000 saved of 50_000 = 20%
        )
        out_path = tmp_path / "proj.json"
        print_summary(stats, sample_rate=0.1, json_out=out_path)
        import json
        data = json.loads(out_path.read_text())
        # projected_saved_bytes = 1_000_000 * 10_000 / 50_000 = 200_000
        assert data["projected_saved_bytes"] == 200_000

    def test_zero_sampled_no_crash(self, capsys):
        from scripts.estimate_recompression_savings import Stats, print_summary
        stats = Stats(scanned=100, sampled=0, listed_bytes=500_000)
        print_summary(stats, sample_rate=0.05)
        out = capsys.readouterr().out
        assert "0.0%" in out

    def test_json_out_written(self, tmp_path):
        from scripts.estimate_recompression_savings import Stats, print_summary
        stats = Stats(
            scanned=100,
            sampled=10,
            listed_bytes=1_000_000,
            old_compressed_bytes=50_000,
            new_compressed_bytes=40_000,
        )
        out_path = tmp_path / "summary.json"
        print_summary(stats, sample_rate=0.1, json_out=out_path)
        assert out_path.exists()
        data = __import__("json").loads(out_path.read_text())
        assert data["savings_pct"] == pytest.approx(20.0, abs=0.01)
        assert data["recommendation"] in ("WORTH IT", "MAYBE", "SKIP")

    def test_recommendation_thresholds(self):
        from scripts.estimate_recompression_savings import recommendation
        assert recommendation(20.0) == "WORTH IT"
        assert recommendation(15.0) == "WORTH IT"
        assert recommendation(14.9) == "MAYBE"
        assert recommendation(5.0) == "MAYBE"
        assert recommendation(4.9) == "SKIP"
        assert recommendation(0.0) == "SKIP"


# ── Group F: failure accumulation ────────────────────────────────────────────

class TestFailureAccumulation:
    def test_failed_count_and_sampled(self):
        from scripts.estimate_recompression_savings import Stats, measure_object
        html = b"<html>ok</html>" * 200
        good = _make_client(_compress(html))
        bad = _make_client(b"corrupt_junk_not_zstd")
        stats = Stats()
        for i in range(10):
            client = bad if i < 3 else good
            result = measure_object(client, "bronze", _obj(key=f"key_{i}.html.zst"))
            stats.sampled += 1
            if result.error:
                stats.failed += 1
                if len(stats.failed_keys) < 5:
                    stats.failed_keys.append(f"key_{i}.html.zst")
        assert stats.failed == 3
        assert stats.sampled == 10
        assert len(stats.failed_keys) <= 5

    def test_failed_keys_capped_at_5(self):
        from scripts.estimate_recompression_savings import Stats, measure_object
        bad = _make_client(b"junk")
        stats = Stats()
        for i in range(10):
            result = measure_object(bad, "bronze", _obj(key=f"key_{i}.html.zst"))
            stats.sampled += 1
            if result.error:
                stats.failed += 1
                if len(stats.failed_keys) < 5:
                    stats.failed_keys.append(f"key_{i}.html.zst")
        assert stats.failed == 10
        assert len(stats.failed_keys) == 5

    def test_failed_objects_count_as_sampled(self):
        from scripts.estimate_recompression_savings import Stats, measure_object
        bad = _make_client(b"junk")
        stats = Stats()
        for i in range(5):
            result = measure_object(bad, "bronze", _obj(key=f"key_{i}.html.zst"))
            stats.sampled += 1
            if result.error:
                stats.failed += 1
        assert stats.sampled == 5
        assert stats.failed == 5


# ── Group G: read-only contract ───────────────────────────────────────────────

class TestReadOnlyContract:
    def _run_main_with_mocks(self) -> MagicMock:
        """Run main() end-to-end with mocked MinIO, return the boto3 client mock."""
        from scripts.estimate_recompression_savings import main

        html = b"<html>content</html>" * 200
        compressed = _compress(html, level=3)

        mock_client = MagicMock()
        body = MagicMock()
        body.read.return_value = compressed
        mock_client.get_object.return_value = {"Body": body}

        # Paginator returns one page with one object
        mock_page = {
            "Contents": [
                {
                    "Key": "html/year=2026/month=6/artifact_type=detail_page/abc.html.zst",
                    "Size": len(compressed),
                }
            ]
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [mock_page]
        mock_client.get_paginator.return_value = mock_paginator

        with (
            patch(
                "scripts.estimate_recompression_savings.get_boto3_client",
                return_value=mock_client,
            ),
            patch("scripts.estimate_recompression_savings.get_s3fs", return_value=MagicMock()),
            patch(
                "sys.argv",
                [
                    "script",
                    "--prefix", "html/year=2026/month=6/artifact_type=detail_page/",
                    "--sample-rate", "1.0",
                    "--limit", "1",
                ],
            ),
        ):
            main()

        return mock_client

    def test_put_object_never_called(self):
        client = self._run_main_with_mocks()
        client.put_object.assert_not_called()

    def test_delete_object_never_called(self):
        client = self._run_main_with_mocks()
        client.delete_object.assert_not_called()

    def test_copy_object_never_called(self):
        client = self._run_main_with_mocks()
        client.copy_object.assert_not_called()

    def test_only_get_object_called(self):
        client = self._run_main_with_mocks()
        # Confirm get_object was the only S3 mutating call attempted
        called_methods = {c[0] for c in client.method_calls}
        write_methods = {"put_object", "delete_object", "copy_object",
                         "create_multipart_upload", "upload_part", "complete_multipart_upload"}
        assert called_methods.isdisjoint(write_methods), (
            f"Unexpected write method(s) called: {called_methods & write_methods}"
        )
