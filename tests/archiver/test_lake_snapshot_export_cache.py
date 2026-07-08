"""Unit tests for archiver/processors/lake_snapshot_export_cache.py
(Plan 120 Gate D).

Covers export fingerprint stability/sensitivity and the load/write cache
helpers. Storage is exercised against shared.minio.read_json/write_json via
mocker, never real MinIO.
"""
from archiver.processors.lake_snapshot_export_cache import (
    EXPORT_CACHE_SCHEMA_VERSION,
    build_export_manifest,
    compute_export_fingerprint,
    export_data_prefix,
    export_manifest_path,
    load_export_manifest,
    write_export_manifest,
)

# ---------------------------------------------------------------------------
# Fingerprint stability / sensitivity
# ---------------------------------------------------------------------------

class TestComputeExportFingerprint:
    def test_same_planning_fingerprint_same_export_fingerprint(self):
        fp_a, _ = compute_export_fingerprint("planning-abc")
        fp_b, _ = compute_export_fingerprint("planning-abc")
        assert fp_a == fp_b

    def test_different_planning_fingerprint_different_export_fingerprint(self):
        fp_a, _ = compute_export_fingerprint("planning-abc")
        fp_b, _ = compute_export_fingerprint("planning-xyz")
        assert fp_a != fp_b

    def test_payload_includes_planning_fingerprint(self):
        _, payload = compute_export_fingerprint("planning-abc")
        assert payload["planning_fingerprint"] == "planning-abc"

    def test_payload_includes_writer_semantics(self):
        _, payload = compute_export_fingerprint("planning-abc")
        assert "included_tables" in payload
        assert "export_algorithm_version" in payload
        assert "output_schema_version" in payload
        assert "partition_layout_version" in payload
        assert "parquet_compression" in payload


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

class TestExportPaths:
    def test_manifest_path_builds_deterministic_path(self):
        path = export_manifest_path("snapshot_exports", "abc123")
        assert path == "snapshot_exports/fingerprints/abc123/manifest.json"

    def test_manifest_path_strips_trailing_slash_on_prefix(self):
        path = export_manifest_path("snapshot_exports/", "abc123")
        assert path == "snapshot_exports/fingerprints/abc123/manifest.json"

    def test_data_prefix_builds_deterministic_path(self):
        path = export_data_prefix("snapshot_exports", "abc123")
        assert path == "snapshot_exports/fingerprints/abc123/data"


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------

class TestBuildExportManifest:
    def test_includes_schema_version_and_fingerprints(self):
        manifest = build_export_manifest(
            fingerprint="export-abc",
            planning_fingerprint="planning-abc",
            export_fingerprint_payload={"a": 1},
            snapshot_id="adaptive-refresh-2026-07-08",
            tier="edge",
            source_window={"start": None, "end": None},
            counts={"closed_vins": 5},
            coverage={},
            tables={},
        )
        assert manifest["export_cache_schema_version"] == EXPORT_CACHE_SCHEMA_VERSION
        assert manifest["export_fingerprint"] == "export-abc"
        assert manifest["planning_fingerprint"] == "planning-abc"
        assert manifest["snapshot_id"] == "adaptive-refresh-2026-07-08"
        assert manifest["counts"] == {"closed_vins": 5}


# ---------------------------------------------------------------------------
# load/write helpers
# ---------------------------------------------------------------------------

class TestLoadExportManifest:
    def test_miss_returns_none(self, mocker):
        mocker.patch(
            "archiver.processors.lake_snapshot_export_cache.read_json", return_value=None
        )
        assert load_export_manifest("some/path") is None

    def test_hit_returns_manifest(self, mocker):
        manifest = {
            "export_cache_schema_version": EXPORT_CACHE_SCHEMA_VERSION,
            "export_fingerprint": "abc",
        }
        mocker.patch(
            "archiver.processors.lake_snapshot_export_cache.read_json", return_value=manifest
        )
        assert load_export_manifest("some/path") == manifest

    def test_load_failure_returns_none(self, mocker):
        mocker.patch(
            "archiver.processors.lake_snapshot_export_cache.read_json",
            side_effect=RuntimeError("boom"),
        )
        assert load_export_manifest("some/path") is None

    def test_schema_mismatch_treated_as_miss(self, mocker):
        manifest = {
            "export_cache_schema_version": EXPORT_CACHE_SCHEMA_VERSION + 1,
            "export_fingerprint": "abc",
        }
        mocker.patch(
            "archiver.processors.lake_snapshot_export_cache.read_json", return_value=manifest
        )
        assert load_export_manifest("some/path") is None


class TestWriteExportManifest:
    def test_write_calls_write_json(self, mocker):
        mock_write = mocker.patch(
            "archiver.processors.lake_snapshot_export_cache.write_json"
        )
        write_export_manifest("some/path", {"a": 1})
        mock_write.assert_called_once_with("some/path", {"a": 1})

    def test_write_failure_does_not_raise(self, mocker):
        mocker.patch(
            "archiver.processors.lake_snapshot_export_cache.write_json",
            side_effect=RuntimeError("boom"),
        )
        write_export_manifest("some/path", {"a": 1})
