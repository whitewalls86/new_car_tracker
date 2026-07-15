"""Unit tests for ops/routers/snapshots.py — Plan 120 Gate F download API."""
import pytest

from ops.routers import snapshots

BASE = "/admin/snapshots/adaptive-refresh"
AUTH = {"Authorization": "Bearer test-token"}


@pytest.fixture(autouse=True)
def _token(mocker):
    """Configure a known token for every test in this module."""
    mocker.patch.object(snapshots, "SNAPSHOT_DOWNLOAD_TOKEN", "test-token")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TestAuth:
    def test_missing_authorization_header_is_401(self, mock_client):
        resp = mock_client.get(f"{BASE}/latest")
        assert resp.status_code == 401

    def test_wrong_token_is_403(self, mock_client):
        resp = mock_client.get(f"{BASE}/latest", headers={"Authorization": "Bearer nope"})
        assert resp.status_code == 403

    def test_malformed_header_is_401(self, mock_client):
        resp = mock_client.get(f"{BASE}/latest", headers={"Authorization": "test-token"})
        assert resp.status_code == 401

    def test_unconfigured_token_is_503(self, mock_client, mocker):
        mocker.patch.object(snapshots, "SNAPSHOT_DOWNLOAD_TOKEN", "")
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# GET /latest
# ---------------------------------------------------------------------------

class TestLatest:
    def test_returns_pointer_json(self, mock_client, mocker):
        pointer = {
            "snapshot_id": "adaptive-refresh-2026-07-07-174500",
            "export_fingerprint": "abc123",
            "archive_key": "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
            "archive_manifest_key": "snapshot_archives/fingerprints/abc123/archive_manifest.json",
            "archive_bytes": 1024,
            "archive_sha256": "deadbeef",
        }
        mocker.patch.object(snapshots, "read_json", return_value=pointer)
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json() == pointer

    def test_missing_latest_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", return_value=None)
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 404

    def test_read_error_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", side_effect=RuntimeError("boom"))
        resp = mock_client.get(f"{BASE}/latest", headers=AUTH)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /{snapshot_id}
# ---------------------------------------------------------------------------

ALIAS = {
    "snapshot_id": "adaptive-refresh-2026-07-07-174500",
    "export_fingerprint": "abc123",
    "archive_key": "snapshot_archives/fingerprints/abc123/snapshot.tar.zst",
    "archive_manifest_key": "snapshot_archives/fingerprints/abc123/archive_manifest.json",
    "archive_bytes": 1024,
    "archive_sha256": "deadbeef",
}
MANIFEST = {
    "snapshot_id": ALIAS["snapshot_id"],
    "tier": "edge",
    "archive": {
        "path": "snapshot.tar.zst",
        "bytes": 1024,
        "sha256": "deadbeef",
        "file_count": 3,
    },
}


class TestSnapshotManifest:
    def test_resolves_through_alias(self, mock_client, mocker):
        alias_key = "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-2026-07-07-174500.json"
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if key == alias_key else MANIFEST,
        )
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)
        assert resp.status_code == 200
        assert resp.json() == MANIFEST

    def test_missing_alias_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", return_value=None)
        resp = mock_client.get(f"{BASE}/nonexistent-snapshot", headers=AUTH)
        assert resp.status_code == 404

    def test_missing_manifest_is_404(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500", headers=AUTH)
        assert resp.status_code == 404

    def test_invalid_snapshot_id_rejected(self, mock_client):
        resp = mock_client.get(f"{BASE}/..%2f..%2fetc%2fpasswd", headers=AUTH)
        assert resp.status_code in (400, 404)

    def test_invalid_snapshot_id_with_dots_rejected(self, mock_client, mocker):
        read_json_mock = mocker.patch.object(snapshots, "read_json")
        resp = mock_client.get(f"{BASE}/adaptive..refresh", headers=AUTH)
        assert resp.status_code == 400
        read_json_mock.assert_not_called()


# ---------------------------------------------------------------------------
# GET /{snapshot_id}/download
# ---------------------------------------------------------------------------

class TestDownload:
    def test_streams_archive_bytes(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        mocker.patch.object(snapshots, "object_size", return_value=1024)
        mocker.patch.object(snapshots, "open_stream", return_value=iter([b"chunk-1", b"chunk-2"]))

        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH)

        assert resp.status_code == 200
        assert resp.content == b"chunk-1chunk-2"
        assert resp.headers["content-type"] == "application/zstd"
        assert resp.headers["content-length"] == "1024"
        assert resp.headers["x-archive-sha256"] == "deadbeef"
        assert "adaptive-refresh-2026-07-07-174500" in resp.headers["content-disposition"]

    def test_missing_alias_is_404(self, mock_client, mocker):
        mocker.patch.object(snapshots, "read_json", return_value=None)
        resp = mock_client.get(f"{BASE}/nonexistent-snapshot/download", headers=AUTH)
        assert resp.status_code == 404

    def test_missing_archive_object_is_404(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        mocker.patch.object(snapshots, "object_size", return_value=None)
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH)
        assert resp.status_code == 404

    def test_open_stream_error_is_404(self, mock_client, mocker):
        mocker.patch.object(
            snapshots, "read_json",
            side_effect=lambda key: ALIAS if "aliases/" in key else None,
        )
        mocker.patch.object(snapshots, "object_size", return_value=1024)
        mocker.patch.object(snapshots, "open_stream", side_effect=RuntimeError("boom"))
        resp = mock_client.get(f"{BASE}/adaptive-refresh-2026-07-07-174500/download", headers=AUTH)
        assert resp.status_code == 404

    def test_invalid_snapshot_id_rejected(self, mock_client, mocker):
        object_size_mock = mocker.patch.object(snapshots, "object_size")
        resp = mock_client.get(f"{BASE}/../etc/download", headers=AUTH)
        assert resp.status_code in (400, 404)
        object_size_mock.assert_not_called()
