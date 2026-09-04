"""Unit tests for scripts/lake_snapshot_common.py (Plan 120, Phase 4)."""
from __future__ import annotations

import io
import tarfile

import pytest
import zstandard as zstd

from scripts.lake_snapshot_common import (
    ChecksumMismatchError,
    LakeSnapshotError,
    ProductionTargetError,
    check_production_target,
    get_archive_meta,
    is_production_like_bucket,
    is_production_like_endpoint,
    is_production_like_postgres_url,
    safe_extract_tar_zst,
    sha256_file,
    verify_archive_checksum,
)


def _make_tar_zst(archive_path, files=None, raw_members=None):
    """Build a .tar.zst archive. raw_members lets tests add unsafe entries
    (path traversal, symlinks) that Path-based construction couldn't express."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in (files or {}).items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
        for info, content in raw_members or []:
            tar.addfile(info, io.BytesIO(content) if content is not None else None)
    compressed = zstd.ZstdCompressor(level=3).compress(buf.getvalue())
    archive_path.write_bytes(compressed)
    return archive_path


# ── Checksums ─────────────────────────────────────────────────────────────

class TestChecksums:
    def test_sha256_file_matches_hashlib(self, tmp_path):
        import hashlib
        p = tmp_path / "data.bin"
        p.write_bytes(b"hello world" * 100)
        assert sha256_file(p) == hashlib.sha256(b"hello world" * 100).hexdigest()

    def test_get_archive_meta_rich_shape(self):
        manifest = {"archive": {"sha256": "abc123", "bytes": 42, "path": "snapshot.tar.zst"}}
        meta = get_archive_meta(manifest)
        assert meta == {"sha256": "abc123", "bytes": 42, "path": "snapshot.tar.zst"}

    def test_get_archive_meta_flat_shape(self):
        manifest = {"archive_sha256": "def456"}
        meta = get_archive_meta(manifest)
        assert meta["sha256"] == "def456"
        assert meta["path"] == "snapshot.tar.zst"

    def test_get_archive_meta_missing_checksum_raises(self):
        with pytest.raises(LakeSnapshotError):
            get_archive_meta({"snapshot_id": "adaptive-refresh-x"})

    def test_verify_archive_checksum_passes(self, tmp_path):
        archive = _make_tar_zst(tmp_path / "snapshot.tar.zst", files={"a.txt": b"hi"})
        manifest = {"archive": {"sha256": sha256_file(archive)}}
        assert verify_archive_checksum(archive, manifest) == sha256_file(archive)

    def test_verify_archive_checksum_mismatch_raises(self, tmp_path):
        archive = _make_tar_zst(tmp_path / "snapshot.tar.zst", files={"a.txt": b"hi"})
        manifest = {"archive": {"sha256": "0" * 64}}
        with pytest.raises(ChecksumMismatchError):
            verify_archive_checksum(archive, manifest)


# ── Safe extraction ───────────────────────────────────────────────────────

class TestSafeExtraction:
    def test_extracts_normal_members(self, tmp_path):
        archive = _make_tar_zst(
            tmp_path / "snapshot.tar.zst",
            files={"silver_normalized/observations/part-000.parquet": b"parquetbytes"},
        )
        dest = tmp_path / "out"
        safe_extract_tar_zst(archive, dest)
        extracted = dest / "silver_normalized/observations/part-000.parquet"
        assert extracted.read_bytes() == b"parquetbytes"

    def test_rejects_path_traversal_member(self, tmp_path):
        info = tarfile.TarInfo(name="../evil.txt")
        info.size = 3
        archive = _make_tar_zst(
            tmp_path / "snapshot.tar.zst", raw_members=[(info, b"pwn")],
        )
        dest = tmp_path / "out"
        with pytest.raises(LakeSnapshotError):
            safe_extract_tar_zst(archive, dest)
        assert not (tmp_path / "evil.txt").exists()

    def test_rejects_absolute_path_member(self, tmp_path):
        info = tarfile.TarInfo(name="/etc/evil.txt")
        info.size = 3
        archive = _make_tar_zst(
            tmp_path / "snapshot.tar.zst", raw_members=[(info, b"pwn")],
        )
        with pytest.raises(LakeSnapshotError):
            safe_extract_tar_zst(archive, tmp_path / "out")

    def test_rejects_symlink_member(self, tmp_path):
        info = tarfile.TarInfo(name="link")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        archive = _make_tar_zst(
            tmp_path / "snapshot.tar.zst", raw_members=[(info, None)],
        )
        with pytest.raises(LakeSnapshotError):
            safe_extract_tar_zst(archive, tmp_path / "out")


# ── Production target guard ───────────────────────────────────────────────

class TestProductionGuard:
    @pytest.mark.parametrize("endpoint", [
        "http://localhost:9000",
        "http://127.0.0.1:9000",
        "http://minio:9000",
    ])
    def test_local_endpoints_are_safe(self, endpoint):
        assert is_production_like_endpoint(endpoint) is False

    @pytest.mark.parametrize("endpoint", [
        "https://cartracker.info",
        "http://147.224.199.86:9000",
        "https://some-public-host.example.com",
        "http://172.32.0.1:9000",  # outside the 172.16.0.0/12 private range
    ])
    def test_production_like_endpoints_detected(self, endpoint):
        assert is_production_like_endpoint(endpoint) is True

    @pytest.mark.parametrize("endpoint", [
        "http://172.16.0.5:9000",
        "http://172.31.0.1:9000",
    ])
    def test_172_16_private_range_is_safe(self, endpoint):
        assert is_production_like_endpoint(endpoint) is False

    def test_production_like_bucket_detected(self):
        assert is_production_like_bucket("prod-bronze") is True
        assert is_production_like_bucket("bronze") is False

    def test_check_production_target_raises_without_override(self):
        with pytest.raises(ProductionTargetError):
            check_production_target("https://cartracker.info", "bronze", False)

    def test_check_production_target_allows_with_override(self):
        check_production_target("https://cartracker.info", "bronze", True)

    def test_check_production_target_allows_local(self):
        check_production_target("http://localhost:9000", "bronze", False)


class TestPostgresGuard:
    """Plan 162 Stage 10. Stricter than the MinIO guard above, deliberately:
    this half runs a DELETE against a live operational table."""

    @pytest.mark.parametrize("url", [
        "postgresql://u:p@localhost:5432/cartracker",
        "postgresql://u:p@127.0.0.1:5432/cartracker",
        "postgresql://u:p@10.0.0.5:5432/cartracker",
        "postgresql://u:p@172.16.0.5:5432/cartracker",
    ])
    def test_loopback_and_private_addresses_pass(self, url):
        assert is_production_like_postgres_url(url) is False

    @pytest.mark.parametrize("url", [
        "postgresql://u:p@cartracker.info:5432/cartracker",
        "postgresql://u:p@147.224.199.86:5432/cartracker",
        "postgresql://u:p@db.example.com:5432/cartracker",
        "",
        "not-a-url",
    ])
    def test_everything_else_is_production_like(self, url):
        assert is_production_like_postgres_url(url) is True

    def test_a_compose_service_name_does_not_pass_here_though_minio_does(self):
        """The one asymmetry between the two guards, asserted so it cannot be
        tidied away as an inconsistency.

        `http://minio:9000` is treated as dev-shaped because the MinIO half
        writes fixture objects under known prefixes. `postgres:5432` is not,
        because from inside the production Compose network that name *is*
        production, and the Postgres half deletes rows.
        """
        assert is_production_like_endpoint("http://minio:9000") is False
        assert is_production_like_postgres_url(
            "postgresql://u:p@postgres:5432/cartracker"
        ) is True

    def test_check_production_target_reads_the_url_alongside_the_others(self):
        with pytest.raises(ProductionTargetError):
            check_production_target(
                "http://localhost:9000", "bronze", False,
                postgres_url="postgresql://u:p@postgres:5432/cartracker",
            )

    def test_an_omitted_url_is_not_checked(self):
        """Seeding MinIO alone is a supported local workflow, and an empty
        default must not turn into a refusal for callers that never asked for
        the Postgres half."""
        check_production_target("http://localhost:9000", "bronze", False)

    def test_the_override_covers_the_postgres_half_too(self):
        check_production_target(
            "http://localhost:9000", "bronze", True,
            postgres_url="postgresql://u:p@cartracker.info:5432/cartracker",
        )
