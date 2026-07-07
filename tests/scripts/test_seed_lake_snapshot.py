"""Unit tests for scripts/seed_lake_snapshot.py (Plan 120, Phase 4)."""
from __future__ import annotations

import io
import json
import tarfile
from unittest.mock import MagicMock

import pytest
import zstandard as zstd

from scripts.lake_snapshot_common import (
    ChecksumMismatchError,
    LakeSnapshotError,
    ProductionTargetError,
    sha256_file,
)
from scripts.seed_lake_snapshot import ensure_bucket, main, seed_lake_snapshot


def _make_tar_zst(archive_path, files, raw_members=None):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        for info, content in raw_members or []:
            tar.addfile(info, io.BytesIO(content) if content is not None else None)
    compressed = zstd.ZstdCompressor(level=3).compress(buf.getvalue())
    archive_path.write_bytes(compressed)
    return archive_path


def _build_snapshot(tmp_path, files=None, raw_members=None):
    files = files if files is not None else {
        "silver_normalized/observations/source=detail/obs_year=2026/obs_month=7/part-000.parquet":
            b"a" * 10,
        "ops_normalized/price_observation_events/year=2026/month=7/part-000.parquet": b"b" * 20,
        "ops_normalized/vin_to_listing_events/year=2026/month=7/part-000.parquet": b"c" * 5,
        "ops_normalized/blocked_cooldown_events/year=2026/month=7/part-000.parquet": b"d" * 5,
        "expected/feature_audit_summary.json": b"{}",
    }
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    archive = _make_tar_zst(snapshot_dir / "snapshot.tar.zst", files, raw_members)
    manifest = {
        "snapshot_id": "adaptive-refresh-2026-07-07-000000",
        "archive": {
            "sha256": sha256_file(archive),
            "bytes": archive.stat().st_size,
            "path": "snapshot.tar.zst",
        },
    }
    (snapshot_dir / "manifest.json").write_text(json.dumps(manifest))
    return archive, manifest


def _mock_client(existing_objects=None):
    client = MagicMock()
    existing_objects = existing_objects or {}

    def list_objects_v2(Bucket, Prefix, **kwargs):
        keys = [k for k in existing_objects if k.startswith(Prefix)]
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    client.list_objects_v2.side_effect = list_objects_v2
    return client


class TestSeedLakeSnapshot:
    def test_refuses_production_target_without_override(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        client = _mock_client()
        with pytest.raises(ProductionTargetError):
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="https://cartracker.info", bucket="bronze",
                clear_prefixes=False, allow_production_target=False, client=client,
            )
        client.upload_file.assert_not_called()

    def test_allows_production_target_with_override(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        client = _mock_client()
        result = seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="https://cartracker.info", bucket="bronze",
            clear_prefixes=False, allow_production_target=True, client=client,
        )
        assert result["uploaded_files"] == 5

    def test_rejects_path_traversal_member(self, tmp_path):
        info = tarfile.TarInfo(name="../evil.txt")
        info.size = 3
        archive, _ = _build_snapshot(
            tmp_path, files={"expected/ok.json": b"{}"}, raw_members=[(info, b"pwn")],
        )
        client = _mock_client()
        with pytest.raises(LakeSnapshotError):
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False, client=client,
            )
        client.upload_file.assert_not_called()
        assert not (tmp_path / "evil.txt").exists()

    def test_fails_on_checksum_mismatch(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        manifest_path = archive.with_name("manifest.json")
        manifest = json.loads(manifest_path.read_text())
        manifest["archive"]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest))
        client = _mock_client()

        with pytest.raises(ChecksumMismatchError):
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False, client=client,
            )
        client.upload_file.assert_not_called()

    def test_uploads_expected_object_keys(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        client = _mock_client()

        seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False, client=client,
        )

        uploaded_keys = {call.args[1:] for call in client.upload_file.call_args_list}
        keys_only = {args[1] for args in uploaded_keys}
        assert keys_only == {
            "silver_normalized/observations/source=detail/obs_year=2026/obs_month=7/part-000.parquet",
            "ops_normalized/price_observation_events/year=2026/month=7/part-000.parquet",
            "ops_normalized/vin_to_listing_events/year=2026/month=7/part-000.parquet",
            "ops_normalized/blocked_cooldown_events/year=2026/month=7/part-000.parquet",
            "expected/feature_audit_summary.json",
        }
        for args in uploaded_keys:
            assert args[0] == "bronze"

    def test_clear_prefixes_deletes_only_fixture_prefixes(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        existing = {
            "silver_normalized/observations/old.parquet": None,
            "ops_normalized/price_observation_events/old.parquet": None,
            "expected/old.json": None,
            "html/year=2026/keep.html.zst": None,
        }
        client = _mock_client(existing_objects=existing)

        seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=True, allow_production_target=False, client=client,
        )

        queried_prefixes = {c.kwargs["Prefix"] for c in client.list_objects_v2.call_args_list}
        assert queried_prefixes == {"silver_normalized/", "ops_normalized/", "expected/"}

        deleted_keys = set()
        for call in client.delete_objects.call_args_list:
            deleted_keys.update(obj["Key"] for obj in call.kwargs["Delete"]["Objects"])
        assert deleted_keys == {
            "silver_normalized/observations/old.parquet",
            "ops_normalized/price_observation_events/old.parquet",
            "expected/old.json",
        }
        assert "html/year=2026/keep.html.zst" not in deleted_keys

    def test_returns_useful_counts(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        client = _mock_client()

        result = seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False, client=client,
        )

        assert result["uploaded_files"] == 5
        assert result["uploaded_bytes"] == 10 + 20 + 5 + 5 + len(b"{}")
        assert set(result["uploaded_by_prefix"]) == {
            "silver_normalized", "ops_normalized", "expected",
        }
        assert result["deleted_objects"] == 0

    def test_ensure_bucket_creates_missing_bucket(self):
        from botocore.exceptions import ClientError

        client = MagicMock()
        client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket",
        )

        ensure_bucket(client, "bronze")

        client.create_bucket.assert_called_once_with(Bucket="bronze")

    def test_ensure_bucket_skips_create_when_bucket_exists(self):
        client = MagicMock()
        client.head_bucket.return_value = {}

        ensure_bucket(client, "bronze")

        client.create_bucket.assert_not_called()

    def test_seed_lake_snapshot_ensures_bucket_before_upload(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        client = _mock_client()

        seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False, client=client,
        )

        client.head_bucket.assert_called_once_with(Bucket="bronze")

    def test_missing_manifest_raises_clear_error(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        archive.with_name("manifest.json").unlink()
        client = _mock_client()
        with pytest.raises(LakeSnapshotError):
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False, client=client,
            )

    def test_main_cli_smoke(self, tmp_path, capsys, monkeypatch):
        archive, _ = _build_snapshot(tmp_path)
        client = _mock_client()
        monkeypatch.setattr(
            "scripts.seed_lake_snapshot.build_boto3_client", lambda endpoint: client,
        )

        result = main([
            "--snapshot", str(archive),
            "--minio-endpoint", "http://localhost:9000",
            "--bucket", "bronze",
        ])

        assert result["uploaded_files"] == 5
        printed = capsys.readouterr().out
        assert '"uploaded_files": 5' in printed
