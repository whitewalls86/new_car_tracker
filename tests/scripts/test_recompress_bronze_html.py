"""Unit tests for scripts/recompress_bronze_html.py

Groups:
  A - dry-run safety (no writes ever in dry-run)
  B - apply size gating (only-smaller / force)
  C - failure handling (download error, zstd error)
  D - checkpoint resume and persistence
  E - delete_object never called
  F - summary counts
  G - selector / prefix construction
"""
from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
import zstandard as zstd

# ── Shared helpers ────────────────────────────────────────────────────────────


def _compress(data: bytes, level: int = 3) -> bytes:
    return zstd.ZstdCompressor(level=level).compress(data)


def _make_client(body_bytes: bytes | None = None, error: Exception | None = None):
    """Return a mock boto3 client whose get_object returns body_bytes."""
    client = MagicMock()
    if error is not None:
        client.get_object.side_effect = error
    else:
        body = MagicMock()
        body.read.return_value = body_bytes if body_bytes is not None else b""
        client.get_object.return_value = {"Body": body}
    return client


def _obj(
    key: str = "html/year=2026/month=6/artifact_type=detail_page/abc.html.zst",
    size: int = 100,
):
    from scripts.recompress_bronze_html import ObjectInfo
    return ObjectInfo(key=key, size=size)


def _summary():
    from scripts.recompress_bronze_html import Summary
    return Summary()


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        prefix=None, year=None, month=None,
        artifact_type="detail_page", bucket="bronze",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# Data pre-compressed at level 9: decompressing + recompressing at level 9 yields the
# exact same bytes (zstd is deterministic), so new_size == old_size → skip path.
_REPETITIVE_RAW = b"<html><body>" + b"content " * 3000 + b"</body></html>"
_L9_COMPRESSED = _compress(_REPETITIVE_RAW, level=9)

# Sizes for mock-controlled tests.
_MOCK_OLD_SIZE = 100
_MOCK_NEW_SMALLER = 90   # new < old → recompress
_MOCK_NEW_LARGER = 110   # new > old → skip (unless --force)


@contextmanager
def _mock_zstd(new_size: int):
    """Patch zstd so decompress returns raw bytes and compress returns new_size bytes."""
    raw = b"fake raw content"
    new_bytes = b"x" * new_size
    decomp = MagicMock(decompress=MagicMock(return_value=raw))
    comp = MagicMock(compress=MagicMock(return_value=new_bytes))
    with (
        patch("zstandard.ZstdDecompressor", return_value=decomp),
        patch("zstandard.ZstdCompressor", return_value=comp),
    ):
        yield


# ── Group A: dry-run safety ───────────────────────────────────────────────────


class TestDryRunNeverWrites:
    def test_put_object_not_called_in_dry_run(self):
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        with _mock_zstd(_MOCK_NEW_SMALLER):
            process_object(
                client, "bronze", _obj(),
                apply=False, force=False,
                checkpoint_keys=set(), checkpoint_path=None,
                summary=_summary(),
            )
        client.put_object.assert_not_called()

    def test_dry_run_counts_would_recompress(self):
        """new < old in dry-run → recompressed counter incremented, no write."""
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        s = _summary()
        with _mock_zstd(_MOCK_NEW_SMALLER):
            process_object(
                client, "bronze", _obj(),
                apply=False, force=False,
                checkpoint_keys=set(), checkpoint_path=None,
                summary=s,
            )
        assert s.recompressed == 1
        assert s.skipped == 0
        assert s.failed == 0
        client.put_object.assert_not_called()

    def test_dry_run_skip_not_smaller(self):
        """new >= old in dry-run → skipped counter incremented, no write."""
        from scripts.recompress_bronze_html import process_object

        # _L9_COMPRESSED recompressed at level-9 gives same size → skip
        client = _make_client(_L9_COMPRESSED)
        s = _summary()
        process_object(
            client, "bronze", _obj(),
            apply=False, force=False,
            checkpoint_keys=set(), checkpoint_path=None,
            summary=s,
        )
        assert s.skipped == 1
        assert s.recompressed == 0
        client.put_object.assert_not_called()

    def test_delete_object_not_called_in_dry_run(self):
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        with _mock_zstd(_MOCK_NEW_SMALLER):
            process_object(
                client, "bronze", _obj(),
                apply=False, force=False,
                checkpoint_keys=set(), checkpoint_path=None,
                summary=_summary(),
            )
        client.delete_object.assert_not_called()


# ── Group B: apply size gating ────────────────────────────────────────────────


class TestApplySizeGating:
    def test_apply_writes_when_smaller(self):
        """new < old in apply mode → put_object called."""
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        s = _summary()
        with _mock_zstd(_MOCK_NEW_SMALLER):
            process_object(
                client, "bronze", _obj(),
                apply=True, force=False,
                checkpoint_keys=set(), checkpoint_path=None,
                summary=s,
            )
        client.put_object.assert_called_once()
        assert s.recompressed == 1
        assert s.skipped == 0

    def test_apply_skips_when_not_smaller(self):
        """new >= old in apply mode → no put_object."""
        from scripts.recompress_bronze_html import process_object

        # _L9_COMPRESSED → re-level-9 yields same size → skip
        client = _make_client(_L9_COMPRESSED)
        s = _summary()
        process_object(
            client, "bronze", _obj(),
            apply=True, force=False,
            checkpoint_keys=set(), checkpoint_path=None,
            summary=s,
        )
        client.put_object.assert_not_called()
        assert s.skipped == 1
        assert s.recompressed == 0

    def test_apply_force_writes_even_when_not_smaller(self):
        """With --force, put_object is called even when new >= old."""
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        s = _summary()
        with _mock_zstd(_MOCK_NEW_LARGER):
            process_object(
                client, "bronze", _obj(),
                apply=True, force=True,
                checkpoint_keys=set(), checkpoint_path=None,
                summary=s,
            )
        client.put_object.assert_called_once()
        assert s.recompressed == 1
        assert s.skipped == 0

    def test_apply_byte_totals_tracked_for_recompressed(self):
        """Recompressed objects contribute old/new sizes to byte totals."""
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        s = _summary()
        with _mock_zstd(_MOCK_NEW_SMALLER):
            process_object(
                client, "bronze", _obj(),
                apply=True, force=False,
                checkpoint_keys=set(), checkpoint_path=None,
                summary=s,
            )
        assert s.old_bytes == _MOCK_OLD_SIZE
        assert s.new_bytes == _MOCK_NEW_SMALLER

    def test_apply_byte_totals_equal_for_skipped(self):
        """Skipped objects contribute old_size to both old_bytes and new_bytes (unchanged)."""
        from scripts.recompress_bronze_html import process_object

        client = _make_client(_L9_COMPRESSED)
        s = _summary()
        process_object(
            client, "bronze", _obj(),
            apply=True, force=False,
            checkpoint_keys=set(), checkpoint_path=None,
            summary=s,
        )
        assert s.old_bytes == len(_L9_COMPRESSED)
        assert s.new_bytes == len(_L9_COMPRESSED)
        assert s.saved_bytes == 0


# ── Group C: failure handling ─────────────────────────────────────────────────


class TestFailureHandling:
    def test_download_failure_increments_failed(self):
        from botocore.exceptions import ClientError

        from scripts.recompress_bronze_html import process_object

        err = ClientError({"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject")
        client = _make_client(error=err)
        s = _summary()
        process_object(
            client, "bronze", _obj(),
            apply=True, force=False,
            checkpoint_keys=set(), checkpoint_path=None,
            summary=s,
        )
        assert s.failed == 1
        assert s.recompressed == 0

    def test_download_failure_does_not_raise(self):
        from botocore.exceptions import ClientError

        from scripts.recompress_bronze_html import process_object

        err = ClientError({"Error": {"Code": "500", "Message": ""}}, "GetObject")
        client = _make_client(error=err)
        # Must not raise
        process_object(
            client, "bronze", _obj(),
            apply=True, force=False,
            checkpoint_keys=set(), checkpoint_path=None,
            summary=_summary(),
        )

    def test_zstd_decompress_failure_increments_failed(self):
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"not_valid_zstd_data_at_all_xxxx")
        s = _summary()
        process_object(
            client, "bronze", _obj(),
            apply=True, force=False,
            checkpoint_keys=set(), checkpoint_path=None,
            summary=s,
        )
        assert s.failed == 1
        assert s.recompressed == 0

    def test_zstd_decompress_failure_does_not_raise(self):
        from scripts.recompress_bronze_html import process_object

        client = _make_client(b"\x00\x01\x02\x03")
        # Must not raise
        process_object(
            client, "bronze", _obj(),
            apply=True, force=False,
            checkpoint_keys=set(), checkpoint_path=None,
            summary=_summary(),
        )

    def test_multiple_failures_loop_continues(self):
        """Failures on individual objects do not stop processing of subsequent objects."""
        from botocore.exceptions import ClientError

        from scripts.recompress_bronze_html import process_object

        err = ClientError({"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject")
        s = _summary()
        with _mock_zstd(_MOCK_NEW_SMALLER):
            for i in range(5):
                client = (
                    _make_client(error=err) if i < 2
                    else _make_client(b"x" * _MOCK_OLD_SIZE)
                )
                process_object(
                    client, "bronze", _obj(key=f"key_{i}.html.zst"),
                    apply=True, force=False,
                    checkpoint_keys=set(), checkpoint_path=None,
                    summary=s,
                )

        assert s.failed == 2
        assert s.recompressed == 3


# ── Group D: checkpoint ───────────────────────────────────────────────────────


class TestCheckpoint:
    def test_load_checkpoint_nonexistent_returns_empty(self, tmp_path):
        from scripts.recompress_bronze_html import load_checkpoint

        result = load_checkpoint(tmp_path / "missing.json")
        assert result == set()

    def test_load_checkpoint_returns_saved_keys(self, tmp_path):
        from scripts.recompress_bronze_html import Summary, load_checkpoint, save_checkpoint

        ckpt = tmp_path / "ckpt.json"
        keys = {"key_a.html.zst", "key_b.html.zst"}
        save_checkpoint(ckpt, keys, Summary())
        loaded = load_checkpoint(ckpt)
        assert loaded == keys

    def test_checkpoint_resume_skips_done_key(self, tmp_path):
        """A key present in the checkpoint is never passed to process_object (no download)."""
        from scripts.recompress_bronze_html import (
            ObjectInfo,
            Summary,
            load_checkpoint,
            process_object,
            save_checkpoint,
        )

        done_key = "html/year=2026/month=6/artifact_type=detail_page/done.html.zst"
        ckpt = tmp_path / "ckpt.json"
        save_checkpoint(ckpt, {done_key}, Summary())

        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        checkpoint_keys = load_checkpoint(ckpt)
        assert done_key in checkpoint_keys

        # Simulate the main loop's skip logic: only call process_object when not in checkpoint.
        obj = ObjectInfo(key=done_key, size=100)
        if obj.key not in checkpoint_keys:
            process_object(
                client, "bronze", obj,
                apply=True, force=False,
                checkpoint_keys=checkpoint_keys,
                checkpoint_path=ckpt,
                summary=Summary(),
            )

        client.get_object.assert_not_called()

    def test_checkpoint_written_after_successful_apply(self, tmp_path):
        """After a successful apply, the checkpoint file contains the written key."""
        from scripts.recompress_bronze_html import process_object

        key = "html/year=2026/month=6/artifact_type=detail_page/abc.html.zst"
        ckpt = tmp_path / "ckpt.json"
        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        checkpoint_keys: set[str] = set()
        s = _summary()

        with _mock_zstd(_MOCK_NEW_SMALLER):
            process_object(
                client, "bronze", _obj(key=key),
                apply=True, force=False,
                checkpoint_keys=checkpoint_keys,
                checkpoint_path=ckpt,
                summary=s,
            )

        assert ckpt.exists()
        data = json.loads(ckpt.read_text())
        assert key in data["processed_keys"]

    def test_checkpoint_not_written_in_dry_run(self, tmp_path):
        """Dry-run never writes checkpoint even when it would recompress."""
        from scripts.recompress_bronze_html import process_object

        ckpt = tmp_path / "ckpt.json"
        client = _make_client(b"x" * _MOCK_OLD_SIZE)

        with _mock_zstd(_MOCK_NEW_SMALLER):
            process_object(
                client, "bronze", _obj(),
                apply=False, force=False,
                checkpoint_keys=set(),
                checkpoint_path=ckpt,
                summary=_summary(),
            )

        assert not ckpt.exists()

    def test_checkpoint_not_written_on_skip(self, tmp_path):
        """Skipped objects (new >= old, no --force) do not update checkpoint."""
        from scripts.recompress_bronze_html import process_object

        ckpt = tmp_path / "ckpt.json"
        client = _make_client(_L9_COMPRESSED)
        checkpoint_keys: set[str] = set()

        process_object(
            client, "bronze", _obj(),
            apply=True, force=False,
            checkpoint_keys=checkpoint_keys,
            checkpoint_path=ckpt,
            summary=_summary(),
        )

        assert not ckpt.exists()
        assert len(checkpoint_keys) == 0

    def test_checkpoint_corrupt_file_starts_fresh(self, tmp_path):
        from scripts.recompress_bronze_html import load_checkpoint

        ckpt = tmp_path / "ckpt.json"
        ckpt.write_text("{corrupt json{{")
        result = load_checkpoint(ckpt)
        assert result == set()

    def test_save_checkpoint_is_atomic(self, tmp_path):
        """save_checkpoint writes via .tmp then replaces — no .tmp left after call."""
        from scripts.recompress_bronze_html import Summary, save_checkpoint

        ckpt = tmp_path / "ckpt.json"
        save_checkpoint(ckpt, {"k1", "k2"}, Summary())

        assert ckpt.exists()
        assert not ckpt.with_suffix(".tmp").exists()


# ── Group E: delete_object never called ───────────────────────────────────────


class TestNeverDeletes:
    def _run(self, client, *, apply: bool, force: bool = False,
             new_size: int = _MOCK_NEW_SMALLER) -> None:
        from scripts.recompress_bronze_html import process_object

        with _mock_zstd(new_size):
            process_object(
                client, "bronze", _obj(),
                apply=apply, force=force,
                checkpoint_keys=set(), checkpoint_path=None,
                summary=_summary(),
            )

    def test_delete_never_called_dry_run(self):
        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        self._run(client, apply=False)
        client.delete_object.assert_not_called()

    def test_delete_never_called_apply(self):
        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        self._run(client, apply=True)
        client.delete_object.assert_not_called()

    def test_delete_never_called_apply_force(self):
        # force=True with new > old: still writes, but never deletes
        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        self._run(client, apply=True, force=True, new_size=_MOCK_NEW_LARGER)
        client.delete_object.assert_not_called()

    def test_delete_never_called_on_failure(self):
        from botocore.exceptions import ClientError

        err = ClientError({"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject")
        client = _make_client(error=err)
        self._run(client, apply=True)
        client.delete_object.assert_not_called()

    def test_delete_never_called_on_skip(self):
        # new > old → skip → still no delete
        client = _make_client(b"x" * _MOCK_OLD_SIZE)
        self._run(client, apply=True, force=False, new_size=_MOCK_NEW_LARGER)
        client.delete_object.assert_not_called()


# ── Group F: summary counts ───────────────────────────────────────────────────


class TestSummaryCounts:
    def test_mixed_recompress_skip_fail(self):
        """10 objects: 7 recompressed, 2 skipped, 1 failed → correct counts."""
        from botocore.exceptions import ClientError

        from scripts.recompress_bronze_html import process_object

        err = ClientError({"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject")

        # Within the mock, compress always returns _MOCK_NEW_SMALLER bytes.
        # Skip objects: body is (_MOCK_NEW_SMALLER - 5) bytes → new > old → skip.
        # Recompress objects: body is _MOCK_OLD_SIZE bytes → new < old → write.
        _OLD_SKIP = _MOCK_NEW_SMALLER - 5
        _OLD_RECOMP = _MOCK_OLD_SIZE

        s = _summary()
        with _mock_zstd(_MOCK_NEW_SMALLER):
            for i in range(10):
                if i == 0:
                    client = _make_client(error=err)
                elif i in (1, 2):
                    client = _make_client(b"x" * _OLD_SKIP)
                else:
                    client = _make_client(b"x" * _OLD_RECOMP)
                process_object(
                    client, "bronze",
                    _obj(key=f"html/year=2026/month=6/artifact_type=detail_page/k{i}.html.zst"),
                    apply=True, force=False,
                    checkpoint_keys=set(), checkpoint_path=None,
                    summary=s,
                )

        assert s.recompressed == 7
        assert s.skipped == 2
        assert s.failed == 1
        assert s.processed == 9  # recompressed + skipped

    def test_summary_to_dict_keys(self):
        from scripts.recompress_bronze_html import Summary

        s = Summary(
            scanned=10, processed=9, recompressed=7, skipped=2, failed=1,
            old_bytes=1000, new_bytes=900,
        )
        d = s.to_dict()
        assert d["saved_bytes"] == 100
        assert d["savings_pct"] == pytest.approx(10.0, abs=0.01)

    def test_json_out_contains_summary(self, tmp_path):
        from scripts.recompress_bronze_html import Summary, print_summary

        s = Summary(
            scanned=5, processed=4, recompressed=3, skipped=1, failed=1,
            old_bytes=3000, new_bytes=2700,
        )
        out = tmp_path / "summary.json"
        print_summary(s, dry_run=True, json_out=out)

        data = json.loads(out.read_text())
        assert data["recompressed"] == 3
        assert data["skipped"] == 1
        assert data["failed"] == 1
        assert data["mode"] == "dry_run"

    def test_savings_pct_zero_when_no_old_bytes(self):
        from scripts.recompress_bronze_html import Summary

        s = Summary(old_bytes=0, new_bytes=0)
        assert s.savings_pct == 0.0


# ── Group G: selector / prefix construction ───────────────────────────────────


class TestSelectors:
    def test_prefix_passthrough(self):
        from scripts.recompress_bronze_html import build_prefixes

        args = _args(prefix="html/year=2026/month=5/artifact_type=detail_page/")
        assert build_prefixes(args, MagicMock()) == [
            "html/year=2026/month=5/artifact_type=detail_page/"
        ]

    def test_year_and_month_builds_prefix(self):
        from scripts.recompress_bronze_html import build_prefixes

        args = _args(year=2026, month=6, artifact_type="detail_page")
        assert build_prefixes(args, MagicMock()) == [
            "html/year=2026/month=6/artifact_type=detail_page/"
        ]

    def test_year_and_month_results_page(self):
        from scripts.recompress_bronze_html import build_prefixes

        args = _args(year=2026, month=3, artifact_type="results_page")
        result = build_prefixes(args, MagicMock())
        assert result == ["html/year=2026/month=3/artifact_type=results_page/"]

    def test_year_without_month_discovers_months(self):
        from scripts.recompress_bronze_html import build_prefixes

        args = _args(year=2026, month=None, artifact_type="detail_page", bucket="bronze")
        with patch(
            "scripts.recompress_bronze_html._discover_months_for_year",
            return_value=[(2026, 4), (2026, 5)],
        ) as mock_discover:
            result = build_prefixes(args, MagicMock())

        mock_discover.assert_called_once()
        assert sorted(result) == [
            "html/year=2026/month=4/artifact_type=detail_page/",
            "html/year=2026/month=5/artifact_type=detail_page/",
        ]

    def test_no_months_found_returns_empty(self):
        from scripts.recompress_bronze_html import build_prefixes

        args = _args(year=2099, month=None, artifact_type="detail_page", bucket="bronze")
        with patch(
            "scripts.recompress_bronze_html._discover_months_for_year",
            return_value=[],
        ):
            result = build_prefixes(args, MagicMock())
        assert result == []

    def test_iter_prefix_filters_html_zst(self):
        """iter_prefix only yields .html.zst entries."""
        from scripts.recompress_bronze_html import ObjectInfo, iter_prefix

        mock_page = {
            "Contents": [
                {
                    "Key": "html/year=2026/month=6/artifact_type=detail_page/a.html.zst",
                    "Size": 100,
                },
                {
                    "Key": "html/year=2026/month=6/artifact_type=detail_page/b.parquet",
                    "Size": 200,
                },
                {
                    "Key": "html/year=2026/month=6/artifact_type=detail_page/c.html.zst",
                    "Size": 300,
                },
            ]
        }
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [mock_page]
        client = MagicMock()
        client.get_paginator.return_value = mock_paginator

        prefix = "html/year=2026/month=6/artifact_type=detail_page/"
        results = list(iter_prefix(client, "bronze", prefix))
        assert len(results) == 2
        assert all(isinstance(r, ObjectInfo) for r in results)
        assert all(r.key.endswith(".html.zst") for r in results)
