"""Unit tests for scripts/download_lake_snapshot.py (Plan 120, Phase 4)."""
from __future__ import annotations

import io
import json
import tarfile

import httpx
import pytest
import zstandard as zstd

from scripts.download_lake_snapshot import download_api, download_local, main
from scripts.lake_snapshot_common import ChecksumMismatchError, LakeSnapshotError, sha256_file


def _make_tar_zst(archive_path, files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    compressed = zstd.ZstdCompressor(level=3).compress(buf.getvalue())
    archive_path.write_bytes(compressed)
    return archive_path


def _build_snapshot(tmp_path, snapshot_id="adaptive-refresh-2026-07-07-000000"):
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    archive = _make_tar_zst(
        build_dir / "snapshot.tar.zst", files={"expected/feature_audit_summary.json": b"{}"},
    )
    manifest = {
        "snapshot_id": snapshot_id,
        "archive": {
            "sha256": sha256_file(archive),
            "bytes": archive.stat().st_size,
            "path": "snapshot.tar.zst",
        },
    }
    manifest_path = build_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path, archive, manifest


# ── Local/offline mode ────────────────────────────────────────────────────

class TestDownloadLocal:
    def test_copies_archive_and_writes_manifest(self, tmp_path):
        manifest_path, archive, manifest = _build_snapshot(tmp_path)
        out_dir = tmp_path / "out"

        dest_archive = download_local(manifest_path, archive, out_dir)

        assert dest_archive.exists()
        assert dest_archive.read_bytes() == archive.read_bytes()
        dest_manifest = dest_archive.parent / "manifest.json"
        assert json.loads(dest_manifest.read_text()) == manifest

    def test_infers_archive_path_from_manifest_when_omitted(self, tmp_path):
        manifest_path, archive, _ = _build_snapshot(tmp_path)
        out_dir = tmp_path / "out"

        dest_archive = download_local(manifest_path, None, out_dir)
        assert dest_archive.exists()

    def test_fails_on_checksum_mismatch(self, tmp_path):
        manifest_path, archive, manifest = _build_snapshot(tmp_path)
        manifest["archive"]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest))
        out_dir = tmp_path / "out"

        with pytest.raises(ChecksumMismatchError):
            download_local(manifest_path, archive, out_dir)

    def test_checksum_mismatch_leaves_no_bad_or_tmp_archive(self, tmp_path):
        manifest_path, archive, manifest = _build_snapshot(tmp_path)
        manifest["archive"]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest))
        out_dir = tmp_path / "out"

        with pytest.raises(ChecksumMismatchError):
            download_local(manifest_path, archive, out_dir)

        dest_dir = out_dir / manifest["snapshot_id"]
        assert not (dest_dir / "snapshot.tar.zst").exists()
        assert not (dest_dir / "snapshot.tar.zst.tmp").exists()

    def test_missing_archive_raises_clear_error(self, tmp_path):
        manifest_path, archive, _ = _build_snapshot(tmp_path)
        archive.unlink()
        with pytest.raises(LakeSnapshotError):
            download_local(manifest_path, archive, tmp_path / "out")

    def test_main_local_mode_prints_path(self, tmp_path, capsys):
        manifest_path, archive, _ = _build_snapshot(tmp_path)
        out_dir = tmp_path / "out"

        result = main([
            "--manifest-path", str(manifest_path),
            "--archive-path", str(archive),
            "--out", str(out_dir),
        ])

        printed = capsys.readouterr().out.strip()
        assert printed == result
        assert (out_dir / "adaptive-refresh-2026-07-07-000000" / "snapshot.tar.zst").exists()

    def test_main_requires_manifest_path_alongside_archive_path(self, tmp_path):
        _, archive, _ = _build_snapshot(tmp_path)
        with pytest.raises(LakeSnapshotError):
            main(["--archive-path", str(archive)])

    def test_main_requires_a_mode(self):
        with pytest.raises(LakeSnapshotError):
            main([])


# ── API mode ──────────────────────────────────────────────────────────────

class TestDownloadApi:
    def _mock_client(self, manifest, archive_bytes, latest_snapshot_id=None):
        snapshot_id = manifest["snapshot_id"]

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path.endswith("/latest"):
                return httpx.Response(200, json={"snapshot_id": latest_snapshot_id or snapshot_id})
            if path.endswith("/download"):
                return httpx.Response(200, content=archive_bytes)
            if path.endswith(f"/{snapshot_id}"):
                return httpx.Response(200, json=manifest)
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        return httpx.Client(transport=transport, base_url="https://cartracker.info")

    def test_download_api_by_snapshot_id(self, tmp_path):
        manifest_path, archive, manifest = _build_snapshot(tmp_path)
        client = self._mock_client(manifest, archive.read_bytes())

        dest_archive = download_api(
            base_url="https://cartracker.info", token="tok",
            latest=False, snapshot_id=manifest["snapshot_id"],
            out_dir=tmp_path / "out", client=client,
        )
        assert dest_archive.read_bytes() == archive.read_bytes()

    def test_download_api_latest_resolves_snapshot_id(self, tmp_path):
        manifest_path, archive, manifest = _build_snapshot(tmp_path)
        client = self._mock_client(manifest, archive.read_bytes())

        dest_archive = download_api(
            base_url="https://cartracker.info", token="tok",
            latest=True, snapshot_id=None,
            out_dir=tmp_path / "out", client=client,
        )
        assert dest_archive.parent.name == manifest["snapshot_id"]

    def test_download_api_requires_base_url(self):
        with pytest.raises(LakeSnapshotError):
            download_api(base_url=None, token="tok", latest=True, snapshot_id=None, out_dir=None)

    def test_download_api_requires_token(self):
        with pytest.raises(LakeSnapshotError):
            download_api(
                base_url="https://cartracker.info", token=None,
                latest=True, snapshot_id=None, out_dir=None,
            )

    def test_download_api_checksum_mismatch_raises(self, tmp_path):
        manifest_path, archive, manifest = _build_snapshot(tmp_path)
        manifest["archive"]["sha256"] = "0" * 64
        client = self._mock_client(manifest, archive.read_bytes())

        with pytest.raises(ChecksumMismatchError):
            download_api(
                base_url="https://cartracker.info", token="tok",
                latest=False, snapshot_id=manifest["snapshot_id"],
                out_dir=tmp_path / "out", client=client,
            )


# ── Against the real ops Gate F router (Plan 120 Gate F wire compatibility) ─

class TestDownloadApiAgainstOpsRouter:
    """
    Exercises download_api() against the actual ops FastAPI app (in-process,
    via ASGITransport) instead of a hand-rolled mock transport, proving the
    Gate F route shapes in ops/routers/snapshots.py match what this
    downloader script expects on the wire.
    """

    def test_latest_and_download_round_trip(self, tmp_path, mocker):
        from fastapi.testclient import TestClient

        from ops.app import app
        from ops.routers import snapshots as snapshots_router

        manifest_path, archive, manifest = _build_snapshot(
            tmp_path, snapshot_id="adaptive-refresh-2026-07-07-174500",
        )
        archive_bytes = archive.read_bytes()
        alias = {
            "snapshot_id": manifest["snapshot_id"],
            "export_fingerprint": "abc123",
            "archive_key": "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
            "archive_manifest_key": "snapshot_archives/fingerprints/abc123/archive_manifest.json",
            "archive_bytes": len(archive_bytes),
            "archive_sha256": manifest["archive"]["sha256"],
        }
        manifest["archive"]["path"] = alias["archive_key"]

        mocker.patch.object(snapshots_router, "SNAPSHOT_DOWNLOAD_TOKEN", "test-token")

        def fake_read_json(key):
            if key == "ci_snapshots/adaptive_refresh/latest.json":
                return alias
            if key == alias["archive_manifest_key"]:
                return manifest
            if key.endswith(f"aliases/{manifest['snapshot_id']}.json"):
                return alias
            return None

        mocker.patch.object(snapshots_router, "read_json", side_effect=fake_read_json)
        mocker.patch.object(snapshots_router, "object_size", return_value=len(archive_bytes))
        mocker.patch.object(
            snapshots_router, "open_stream", return_value=iter([archive_bytes]),
        )

        client = TestClient(
            app,
            base_url="https://cartracker.info",
            headers={"Authorization": "Bearer test-token"},
        )
        dest_archive = download_api(
            base_url="https://cartracker.info", token="test-token",
            latest=True, snapshot_id=None,
            out_dir=tmp_path / "out", client=client,
        )

        assert dest_archive.read_bytes() == archive_bytes
        assert dest_archive.parent.name == manifest["snapshot_id"]
