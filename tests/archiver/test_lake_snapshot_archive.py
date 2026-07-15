"""Unit tests for archiver/processors/lake_snapshot_archive.py (Plan 120
Gate E).

Uses local `base_path` fixture mode (like the sibling
tests/archiver/test_lake_snapshot_export.py) so packaging/checksum/reuse
logic can be exercised against real files on disk without mocking
boto3/s3fs — MinIO-mode integration coverage belongs in
tests/integration/archiver.
"""
import json
import os

import pytest

from archiver.processors.lake_snapshot_archive import (
    ArchiveResult,
    LakeSnapshotArchiveError,
    alias_pointer_path,
    archive_manifest_path,
    archive_object_path,
    build_archive_manifest,
    latest_pointer_path,
    list_data_files,
    load_archive_manifest,
    package_snapshot_archive,
    promote_snapshot_pointers,
)


def _seed_data(tmp_path, data_path="snapshot_exports/fingerprints/fp1/generations/gen1/data"):
    root = tmp_path / data_path
    (root / "silver_normalized/observations/source=detail").mkdir(parents=True)
    (root / "ops_normalized/price_observation_events").mkdir(parents=True)
    (root / "silver_normalized/observations/source=detail/part-0.parquet").write_bytes(b"AAA")
    (root / "ops_normalized/price_observation_events/part-0.parquet").write_bytes(b"BBB")
    return data_path


def _export_manifest(fingerprint="fp1", data_path=None):
    return {
        "export_fingerprint": fingerprint,
        "planning_fingerprint": "plan1",
        "snapshot_id": "adaptive-refresh-2026-07-15",
        "tier": "edge",
        "data_path": data_path,
        "tables": {"silver_observations": {"rows": 1, "files": 1, "sha256": ["x"], "error": None}},
    }


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

class TestPathHelpers:
    def test_archive_manifest_path(self):
        assert archive_manifest_path("snapshot_archives", "fp1") == (
            "snapshot_archives/fingerprints/fp1/archive_manifest.json"
        )

    def test_archive_object_path(self):
        assert archive_object_path("snapshot_archives", "fp1") == (
            "snapshot_archives/fingerprints/fp1/snapshot.tar.zst"
        )

    def test_strips_trailing_slash(self):
        assert archive_manifest_path("snapshot_archives/", "fp1") == (
            "snapshot_archives/fingerprints/fp1/archive_manifest.json"
        )

    def test_latest_pointer_path(self):
        assert latest_pointer_path("ci_snapshots/adaptive_refresh") == (
            "ci_snapshots/adaptive_refresh/latest.json"
        )

    def test_alias_pointer_path(self):
        assert alias_pointer_path("ci_snapshots/adaptive_refresh", "adaptive-refresh-x") == (
            "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-x.json"
        )


# ---------------------------------------------------------------------------
# Safe, deterministic file listing
# ---------------------------------------------------------------------------

class TestListDataFiles:
    def test_lists_all_files_sorted(self, tmp_path):
        data_path = _seed_data(tmp_path)
        files = list_data_files(str(tmp_path), data_path)
        assert files == sorted(files)
        assert "silver_normalized/observations/source=detail/part-0.parquet" in files
        assert "ops_normalized/price_observation_events/part-0.parquet" in files

    def test_deterministic_across_calls(self, tmp_path):
        data_path = _seed_data(tmp_path)
        first = list_data_files(str(tmp_path), data_path)
        second = list_data_files(str(tmp_path), data_path)
        assert first == second

    def test_symlink_to_file_is_skipped(self, tmp_path, mocker):
        """Simulates a symlink via os.path.islink rather than actually
        creating one with os.symlink: real symlink creation needs elevated
        privilege on Windows (unlike Linux), and the sibling extraction-side
        safety test (scripts/lake_snapshot_common.py's
        test_rejects_symlink_member) avoids the same OS dependency by
        declaring a synthetic tarfile SYMTYPE header instead of touching the
        filesystem. There's no header-trick equivalent for the packing side
        (we're the ones walking real files), so mocking os.path.islink is
        the cross-platform-safe way to exercise the same logic here."""
        data_path = _seed_data(tmp_path)
        root = tmp_path / data_path
        link = root / "silver_normalized/observations/source=detail/sneaky.parquet"
        link.write_bytes(b"stand-in for a symlink target")  # a real file, not a real symlink

        real_islink = os.path.islink
        link_norm = os.path.normpath(str(link))

        def _fake_islink(path):
            # normpath, not string equality: list_data_files's internal
            # os.walk-built path mixes forward/back slashes (the data_path
            # component embeds forward slashes; os.walk's own recursion
            # joins with the native separator), which never string-matches
            # pathlib's all-native-separator rendering of the same file.
            return os.path.normpath(str(path)) == link_norm or real_islink(path)

        mocker.patch("os.path.islink", side_effect=_fake_islink)

        files = list_data_files(str(tmp_path), data_path)
        assert not any("sneaky" in f for f in files)

    def test_empty_directory_returns_empty_list(self, tmp_path):
        data_path = "snapshot_exports/fingerprints/fp1/generations/gen1/data"
        os.makedirs(tmp_path / data_path)
        assert list_data_files(str(tmp_path), data_path) == []


# ---------------------------------------------------------------------------
# Archive manifest builder / validator
# ---------------------------------------------------------------------------

class TestBuildArchiveManifest:
    def test_includes_archive_metadata(self):
        manifest = build_archive_manifest(
            _export_manifest(), archive_key="snapshot_archives/fingerprints/fp1/snapshot.tar.zst",
            archive_bytes=42, archive_sha256="abc123", file_count=2,
        )
        assert manifest["archive"]["path"] == "snapshot_archives/fingerprints/fp1/snapshot.tar.zst"
        assert manifest["archive"]["bytes"] == 42
        assert manifest["archive"]["sha256"] == "abc123"
        assert manifest["archive"]["file_count"] == 2
        assert manifest["export_fingerprint"] == "fp1"

    def test_does_not_mutate_input_manifest(self):
        export_manifest = _export_manifest()
        build_archive_manifest(
            export_manifest, archive_key="k", archive_bytes=1, archive_sha256="x", file_count=1,
        )
        assert "archive" not in export_manifest


class TestLoadArchiveManifest:
    def _write(self, tmp_path, manifest, manifest_key, archive_key, archive_bytes,
               archive_content=None):
        full = tmp_path / manifest_key
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(json.dumps(manifest))
        archive_full = tmp_path / archive_key
        archive_full.parent.mkdir(parents=True, exist_ok=True)
        archive_full.write_bytes(
            archive_content if archive_content is not None else b"x" * archive_bytes
        )

    def test_missing_manifest_returns_none(self, tmp_path):
        assert load_archive_manifest(str(tmp_path), "nope/archive_manifest.json", "fp1") is None

    def test_valid_manifest_returns_manifest(self, tmp_path):
        import hashlib
        content = b"xxx"
        manifest = build_archive_manifest(
            _export_manifest(), archive_key="archives/fp1/snapshot.tar.zst",
            archive_bytes=len(content), archive_sha256=hashlib.sha256(content).hexdigest(),
            file_count=1,
        )
        self._write(tmp_path, manifest, "archives/fp1/archive_manifest.json",
                    "archives/fp1/snapshot.tar.zst", len(content), archive_content=content)
        result = load_archive_manifest(str(tmp_path), "archives/fp1/archive_manifest.json", "fp1")
        assert result == manifest

    def test_checksum_mismatch_with_matching_size_treated_as_miss(self, tmp_path):
        """A same-size-but-corrupted archive object (recorded sha256 no
        longer matches the real bytes) must never be trusted as a valid
        cache hit just because the manifest JSON still looks complete and
        the size happens to match."""
        manifest = build_archive_manifest(
            _export_manifest(), archive_key="archives/fp1/snapshot.tar.zst",
            archive_bytes=3, archive_sha256="not-the-real-hash", file_count=1,
        )
        self._write(tmp_path, manifest, "archives/fp1/archive_manifest.json",
                    "archives/fp1/snapshot.tar.zst", 3, archive_content=b"xxx")
        result = load_archive_manifest(str(tmp_path), "archives/fp1/archive_manifest.json", "fp1")
        assert result is None

    def test_fingerprint_mismatch_treated_as_miss(self, tmp_path):
        manifest = build_archive_manifest(
            _export_manifest(fingerprint="fp1"), archive_key="archives/fp1/snapshot.tar.zst",
            archive_bytes=3, archive_sha256="abc", file_count=1,
        )
        self._write(tmp_path, manifest, "archives/fp1/archive_manifest.json",
                    "archives/fp1/snapshot.tar.zst", 3)
        result = load_archive_manifest(str(tmp_path), "archives/fp1/archive_manifest.json", "fp2")
        assert result is None

    def test_size_mismatch_treated_as_miss(self, tmp_path):
        """A recorded archive.bytes that no longer matches the actual object
        size on disk (e.g. it was truncated/corrupted) must never be trusted
        as a valid cache hit."""
        manifest = build_archive_manifest(
            _export_manifest(), archive_key="archives/fp1/snapshot.tar.zst",
            archive_bytes=999, archive_sha256="abc", file_count=1,
        )
        self._write(tmp_path, manifest, "archives/fp1/archive_manifest.json",
                    "archives/fp1/snapshot.tar.zst", 3)  # actual file is only 3 bytes
        result = load_archive_manifest(str(tmp_path), "archives/fp1/archive_manifest.json", "fp1")
        assert result is None

    def test_missing_archive_object_treated_as_miss(self, tmp_path):
        manifest = build_archive_manifest(
            _export_manifest(), archive_key="archives/fp1/snapshot.tar.zst",
            archive_bytes=3, archive_sha256="abc", file_count=1,
        )
        full = tmp_path / "archives/fp1/archive_manifest.json"
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(json.dumps(manifest))
        # Deliberately do not write the archive object itself.
        result = load_archive_manifest(str(tmp_path), "archives/fp1/archive_manifest.json", "fp1")
        assert result is None

    def test_incomplete_archive_metadata_treated_as_miss(self, tmp_path):
        manifest = _export_manifest()
        manifest["archive_cache_schema_version"] = 1
        manifest["archive"] = {"path": None, "bytes": None, "sha256": None}
        full = tmp_path / "archives/fp1/archive_manifest.json"
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(json.dumps(manifest))
        result = load_archive_manifest(str(tmp_path), "archives/fp1/archive_manifest.json", "fp1")
        assert result is None


# ---------------------------------------------------------------------------
# package_snapshot_archive — end-to-end local-fixture packaging
# ---------------------------------------------------------------------------

class TestPackageSnapshotArchive:
    def test_packages_and_publishes_archive(self, tmp_path):
        data_path = _seed_data(tmp_path)
        manifest = _export_manifest(data_path=data_path)

        result = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1",
            archive_prefix="snapshot_archives",
        )

        assert result.ok
        assert result.cache_hit is False
        assert result.cache_action == "computed"
        assert result.archive_bytes > 0
        assert result.archive_sha256
        assert result.file_count == 2
        archive_full = tmp_path / result.archive_key
        assert archive_full.exists()
        assert archive_full.stat().st_size == result.archive_bytes
        manifest_full = tmp_path / result.archive_manifest_key
        assert manifest_full.exists()

    def test_deterministic_sha256_across_two_builds(self, tmp_path):
        _seed_data(tmp_path, "data_a")
        manifest = _export_manifest(data_path="data_a")
        result_a = package_snapshot_archive(
            str(tmp_path), "data_a", manifest, "fpA", archive_prefix="archives_a",
        )

        tmp_path_b = tmp_path / "other_root"
        data_path_b = _seed_data(tmp_path_b, "data_a")
        result_b = package_snapshot_archive(
            str(tmp_path_b), "data_a", manifest, "fpA", archive_prefix="archives_a",
        )

        assert result_a.archive_sha256 == result_b.archive_sha256
        assert result_a.archive_bytes == result_b.archive_bytes
        assert data_path_b == "data_a"

    def test_reuse_archive_cache_skips_repackaging(self, tmp_path, mocker):
        data_path = _seed_data(tmp_path)
        manifest = _export_manifest(data_path=data_path)
        first = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
        )
        assert first.ok

        spy = mocker.spy(
            __import__(
                "archiver.processors.lake_snapshot_archive", fromlist=["build_archive_tar_zst"]
            ),
            "build_archive_tar_zst",
        )
        second = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
            reuse_archive_cache=True,
        )
        assert second.ok
        assert second.cache_hit is True
        assert second.cache_action == "reused"
        assert second.archive_sha256 == first.archive_sha256
        assert not spy.called

    def test_repeat_without_reuse_flag_is_a_harmless_noop(self, tmp_path):
        """Packaging is a pure function of the materialized data, so building
        it again (without reuse_archive_cache) produces identical bytes and
        is treated as a dedup no-op, not a conflict."""
        data_path = _seed_data(tmp_path)
        manifest = _export_manifest(data_path=data_path)
        first = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
        )
        second = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
        )
        assert first.ok and second.ok
        assert second.cache_action == "reused"

    def test_checksum_mismatch_refuses_to_overwrite_without_refresh(self, tmp_path):
        """If the materialized data changes under a fingerprint that already
        has a published archive (which should not normally happen — the
        fingerprint is supposed to be a pure function of the data — but must
        be defended against), packaging must refuse to silently overwrite the
        existing archive."""
        data_path = _seed_data(tmp_path)
        manifest = _export_manifest(data_path=data_path)
        first = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
        )
        assert first.ok

        # Mutate the underlying data without changing the fingerprint.
        (tmp_path / data_path / "silver_normalized/observations/source=detail/part-0.parquet"
         ).write_bytes(b"CHANGED")

        second = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
        )
        assert not second.ok
        assert "refusing to overwrite" in second.error

        # The original archive on disk must be untouched.
        archive_full = tmp_path / first.archive_key
        assert archive_full.stat().st_size == first.archive_bytes

    def test_refresh_archive_cache_overwrites_conflicting_archive(self, tmp_path):
        data_path = _seed_data(tmp_path)
        manifest = _export_manifest(data_path=data_path)
        first = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
        )
        assert first.ok

        (tmp_path / data_path / "silver_normalized/observations/source=detail/part-0.parquet"
         ).write_bytes(b"CHANGED")

        second = package_snapshot_archive(
            str(tmp_path), data_path, manifest, "fp1", archive_prefix="snapshot_archives",
            refresh_archive_cache=True,
        )
        assert second.ok
        assert second.cache_action == "refreshed"
        assert second.archive_sha256 != first.archive_sha256


# ---------------------------------------------------------------------------
# promote_snapshot_pointers
# ---------------------------------------------------------------------------

class TestPromoteSnapshotPointers:
    def test_writes_alias_and_latest(self, tmp_path):
        archive_result = ArchiveResult(
            ok=True, archive_key="snapshot_archives/fingerprints/fp1/snapshot.tar.zst",
            archive_manifest_key="snapshot_archives/fingerprints/fp1/archive_manifest.json",
            archive_bytes=3, archive_sha256="abc",
        )
        promotion = promote_snapshot_pointers(
            str(tmp_path), "adaptive-refresh-2026-07-15", "fp1", archive_result,
            alias_prefix="ci_snapshots/adaptive_refresh",
        )
        assert promotion["ok"] is True
        latest = json.loads(
            (tmp_path / "ci_snapshots/adaptive_refresh/latest.json").read_text()
        )
        alias = json.loads(
            (tmp_path / "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-2026-07-15.json")
            .read_text()
        )
        assert latest["export_fingerprint"] == "fp1"
        assert latest["archive_sha256"] == "abc"
        assert alias == latest

    def test_refuses_to_promote_a_failed_archive_result(self, tmp_path):
        with pytest.raises(LakeSnapshotArchiveError):
            promote_snapshot_pointers(
                str(tmp_path), "adaptive-refresh-x", "fp1",
                ArchiveResult(ok=False, error="boom"),
            )

    def test_alias_write_failure_reports_not_ok_and_skips_latest(self, tmp_path, mocker):
        """If the alias write fails, latest.json must never be attempted —
        and the caller must be able to tell promotion did not fully
        succeed, rather than silently reporting the export as complete."""
        mocker.patch(
            "archiver.processors.lake_snapshot_archive._write_json_object",
            return_value=False,
        )
        archive_result = ArchiveResult(
            ok=True, archive_key="k", archive_manifest_key="mk",
            archive_bytes=1, archive_sha256="x",
        )
        promotion = promote_snapshot_pointers(
            str(tmp_path), "adaptive-refresh-x", "fp1", archive_result,
            alias_prefix="ci_snapshots/adaptive_refresh",
        )
        assert promotion["ok"] is False
        assert "alias" in promotion["error"]
        assert not (tmp_path / "ci_snapshots/adaptive_refresh/latest.json").exists()

    def test_latest_write_failure_reports_not_ok(self, tmp_path, mocker):
        """If alias succeeds but latest.json fails, promotion must still be
        reported as not-ok (a caller checking only "did we raise" would
        otherwise miss this)."""
        real_write = __import__(
            "archiver.processors.lake_snapshot_archive", fromlist=["_write_json_object"]
        )._write_json_object

        def _fail_on_latest(base_path, key, obj):
            if key.endswith("latest.json"):
                return False
            return real_write(base_path, key, obj)

        mocker.patch(
            "archiver.processors.lake_snapshot_archive._write_json_object",
            side_effect=_fail_on_latest,
        )
        archive_result = ArchiveResult(
            ok=True, archive_key="k", archive_manifest_key="mk",
            archive_bytes=1, archive_sha256="x",
        )
        promotion = promote_snapshot_pointers(
            str(tmp_path), "adaptive-refresh-x", "fp1", archive_result,
            alias_prefix="ci_snapshots/adaptive_refresh",
        )
        assert promotion["ok"] is False
        assert "latest" in promotion["error"]
        assert (tmp_path / "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-x.json").exists()

    def test_alias_written_before_latest(self, tmp_path, mocker):
        """latest.json must be the last thing promoted so a mid-failure
        never leaves it pointing at a snapshot with no alias file."""
        write_order = []
        real_write = __import__(
            "archiver.processors.lake_snapshot_archive", fromlist=["_write_json_object"]
        )._write_json_object

        def _tracking_write(base_path, key, obj):
            write_order.append(key)
            return real_write(base_path, key, obj)

        mocker.patch(
            "archiver.processors.lake_snapshot_archive._write_json_object",
            side_effect=_tracking_write,
        )
        archive_result = ArchiveResult(
            ok=True, archive_key="k", archive_manifest_key="mk",
            archive_bytes=1, archive_sha256="x",
        )
        promote_snapshot_pointers(
            str(tmp_path), "adaptive-refresh-x", "fp1", archive_result,
            alias_prefix="ci_snapshots/adaptive_refresh",
        )
        assert write_order == [
            "ci_snapshots/adaptive_refresh/aliases/adaptive-refresh-x.json",
            "ci_snapshots/adaptive_refresh/latest.json",
        ]
