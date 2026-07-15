"""
Plan 112 Gate A4: unit tests for scripts/preflight_local_lakehouse_snapshot.py.

No live Docker, MinIO, Lakekeeper, DuckDB, or Spark -- HTTP and S3 clients
are monkeypatched/faked, filesystem checks use tmp_path. These run in the
regular `unit-tests` CI job.
"""
import hashlib
import json
from pathlib import Path

import pytest

from scripts import preflight_local_lakehouse_snapshot as preflight
from scripts.preflight_local_lakehouse_snapshot import (
    FAIL,
    PASS,
    SKIP,
    CheckResult,
    check_analytics_duckdb,
    check_feature_tables,
    check_lakekeeper,
    check_minio_endpoint,
    check_required_files,
    check_snapshot_archive,
    check_snapshot_seeded,
    check_warehouse_registered,
    find_snapshot_manifest,
    format_results,
    run_preflight,
)

_REPO_ROOT = Path(__file__).parent.parent.parent


def _write_snapshot_pair(directory: Path, payload: bytes = b"archive-bytes") -> Path:
    """Write a manifest.json + snapshot.tar.zst pair the way
    scripts/download_lake_snapshot.py lays them out; return the manifest path."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "snapshot.tar.zst").write_bytes(payload)
    manifest = {
        "snapshot_id": "adaptive-refresh-test",
        "archive": {
            "path": "snapshot.tar.zst",
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        },
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path


class TestRequiredFiles:
    def test_passes_on_real_repo_root(self):
        result = check_required_files(_REPO_ROOT)
        assert result.status == PASS

    def test_fails_with_missing_files_listed(self, tmp_path):
        result = check_required_files(tmp_path)
        assert result.status == FAIL
        assert "docker-compose.lakehouse.local.yml" in result.message


class TestSnapshotArchive:
    def test_no_manifest_found(self, tmp_path):
        result = check_snapshot_archive(tmp_path, None, verify_checksum=False)
        assert result.status == FAIL
        assert "download_lake_snapshot" in result.message

    def test_finds_newest_manifest_in_snapshot_dir(self, tmp_path):
        old = _write_snapshot_pair(tmp_path / "older")
        new = _write_snapshot_pair(tmp_path / "newer")
        import os
        os.utime(old, (1, 1))
        assert find_snapshot_manifest(tmp_path) == new

    def test_size_verified_pass(self, tmp_path):
        manifest_path = _write_snapshot_pair(tmp_path / "snap")
        result = check_snapshot_archive(tmp_path, manifest_path, verify_checksum=False)
        assert result.status == PASS
        assert "size verified" in result.message

    def test_size_mismatch_fails(self, tmp_path):
        manifest_path = _write_snapshot_pair(tmp_path / "snap")
        (tmp_path / "snap" / "snapshot.tar.zst").write_bytes(b"different-length-payload!")
        result = check_snapshot_archive(tmp_path, manifest_path, verify_checksum=False)
        assert result.status == FAIL
        assert "re-download" in result.message

    def test_missing_archive_beside_manifest_fails(self, tmp_path):
        manifest_path = _write_snapshot_pair(tmp_path / "snap")
        (tmp_path / "snap" / "snapshot.tar.zst").unlink()
        result = check_snapshot_archive(tmp_path, manifest_path, verify_checksum=False)
        assert result.status == FAIL
        assert "no snapshot.tar.zst" in result.message

    def test_checksum_verified_pass(self, tmp_path):
        manifest_path = _write_snapshot_pair(tmp_path / "snap")
        result = check_snapshot_archive(tmp_path, manifest_path, verify_checksum=True)
        assert result.status == PASS
        assert "sha256 verified" in result.message

    def test_checksum_mismatch_fails(self, tmp_path):
        manifest_path = _write_snapshot_pair(tmp_path / "snap")
        # Same length, different content -- passes the size check, fails sha256.
        (tmp_path / "snap" / "snapshot.tar.zst").write_bytes(b"archive-bytez")
        result = check_snapshot_archive(tmp_path, manifest_path, verify_checksum=True)
        assert result.status == FAIL


class TestMinioEndpoint:
    def test_production_like_endpoint_refused_without_any_network_call(self):
        result = check_minio_endpoint("https://cartracker.info", "bronze")
        assert result.status == FAIL
        assert "production-like" in result.message

    def test_production_like_bucket_refused(self):
        result = check_minio_endpoint("http://localhost:19000", "prod-bronze")
        assert result.status == FAIL

    def test_reachable(self, monkeypatch):
        import httpx

        class FakeResponse:
            def raise_for_status(self):
                pass

        monkeypatch.setattr(httpx, "get", lambda url, timeout: FakeResponse())
        result = check_minio_endpoint("http://localhost:19000", "bronze")
        assert result.status == PASS

    def test_unreachable_mentions_compose_command(self, monkeypatch):
        import httpx

        def boom(url, timeout):
            raise httpx.ConnectError("refused")

        monkeypatch.setattr(httpx, "get", boom)
        result = check_minio_endpoint("http://localhost:19000", "bronze")
        assert result.status == FAIL
        assert "docker-compose.lakehouse.local.yml" in result.message


class _FakeS3Client:
    def __init__(self, counts_by_prefix):
        self.counts_by_prefix = counts_by_prefix

    def list_objects_v2(self, Bucket, Prefix, MaxKeys):
        return {"KeyCount": self.counts_by_prefix.get(Prefix, 0)}


class TestSnapshotSeeded:
    def test_all_prefixes_seeded(self):
        client = _FakeS3Client({p: 1 for p in preflight.FIXTURE_PREFIXES})
        result = check_snapshot_seeded("http://localhost:19000", "bronze", client=client)
        assert result.status == PASS

    def test_empty_prefix_fails_with_seed_hint(self):
        counts = {p: 1 for p in preflight.FIXTURE_PREFIXES}
        counts["silver_normalized/"] = 0
        client = _FakeS3Client(counts)
        result = check_snapshot_seeded("http://localhost:19000", "bronze", client=client)
        assert result.status == FAIL
        assert "silver_normalized/" in result.message
        assert "seed_lake_snapshot" in result.message

    def test_listing_error_fails(self):
        class BrokenClient:
            def list_objects_v2(self, **kwargs):
                raise RuntimeError("connection reset")

        result = check_snapshot_seeded(
            "http://localhost:19000", "bronze", client=BrokenClient()
        )
        assert result.status == FAIL


class TestAnalyticsDuckdb:
    def test_missing_file_fails_with_dbt_hint(self, tmp_path):
        result = check_analytics_duckdb(tmp_path / "analytics.duckdb")
        assert result.status == FAIL
        assert "dbt" in result.message

    def test_empty_file_fails(self, tmp_path):
        path = tmp_path / "analytics.duckdb"
        path.touch()
        result = check_analytics_duckdb(path)
        assert result.status == FAIL

    def test_non_empty_file_passes(self, tmp_path):
        path = tmp_path / "analytics.duckdb"
        path.write_bytes(b"not-really-a-db-but-non-empty")
        result = check_analytics_duckdb(path)
        assert result.status == PASS


class _FakeDuckdbConnection:
    def __init__(self, tables):
        self.tables = tables

    def execute(self, sql):
        assert "information_schema.tables" in sql
        outer = self

        class Cursor:
            def fetchall(self):
                return [(t,) for t in outer.tables]

        return Cursor()

    def close(self):
        pass


class TestFeatureTables:
    @pytest.fixture
    def fake_duckdb(self, monkeypatch, tmp_path):
        """Install a fake `duckdb` module so this test never needs the real
        package (it is not a unit-test dependency)."""
        import sys
        import types

        module = types.ModuleType("duckdb")
        module._tables = ["int_listing_volatility_features", "other_table"]
        module.connect = lambda path, read_only: _FakeDuckdbConnection(module._tables)
        monkeypatch.setitem(sys.modules, "duckdb", module)
        db_path = tmp_path / "analytics.duckdb"
        db_path.write_bytes(b"x")
        return module, db_path

    def test_required_table_present(self, fake_duckdb):
        _, db_path = fake_duckdb
        result = check_feature_tables(db_path, ["int_listing_volatility_features"])
        assert result.status == PASS

    def test_missing_table_fails_with_stale_hint(self, fake_duckdb):
        module, db_path = fake_duckdb
        module._tables = ["something_else"]
        result = check_feature_tables(db_path, ["int_listing_volatility_features"])
        assert result.status == FAIL
        assert "int_listing_volatility_features" in result.message
        assert "stale" in result.message

    def test_unopenable_file_fails(self, fake_duckdb, monkeypatch):
        module, db_path = fake_duckdb

        def broken_connect(path, read_only):
            raise RuntimeError("not a duckdb file")

        module.connect = broken_connect
        result = check_feature_tables(db_path, ["int_listing_volatility_features"])
        assert result.status == FAIL


class TestLakekeeperChecks:
    def test_lakekeeper_reachable(self, monkeypatch):
        import httpx

        class FakeResponse:
            def raise_for_status(self):
                pass

        monkeypatch.setattr(httpx, "get", lambda url, timeout: FakeResponse())
        result = check_lakekeeper("http://localhost:18181")
        assert result.status == PASS
        assert "/management/v1/info" in result.message

    def test_lakekeeper_unreachable_mentions_compose_command(self, monkeypatch):
        import httpx

        def boom(url, timeout):
            raise httpx.ConnectError("refused")

        monkeypatch.setattr(httpx, "get", boom)
        result = check_lakekeeper("http://localhost:18181")
        assert result.status == FAIL
        assert "docker-compose.lakehouse.local.yml" in result.message

    def test_warehouse_registered(self, monkeypatch):
        import httpx

        class FakeResponse:
            status_code = 200

        monkeypatch.setattr(
            httpx, "get", lambda url, params, timeout: FakeResponse()
        )
        result = check_warehouse_registered("http://localhost:18181", "cartracker_experiments")
        assert result.status == PASS

    def test_warehouse_not_registered_mentions_register_script(self, monkeypatch):
        import httpx

        class FakeResponse:
            status_code = 400

        monkeypatch.setattr(
            httpx, "get", lambda url, params, timeout: FakeResponse()
        )
        result = check_warehouse_registered("http://localhost:18181", "cartracker_experiments")
        assert result.status == FAIL
        assert "register_lakehouse_warehouse" in result.message


class TestRunPreflight:
    def _args(self, tmp_path, **overrides):
        argv = [
            "--snapshot-dir", str(tmp_path / "snapshots"),
            "--analytics-path", str(tmp_path / "analytics.duckdb"),
        ]
        return preflight._parse_args(argv)

    def test_dependent_checks_skip_when_prerequisite_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            preflight, "check_minio_endpoint",
            lambda endpoint, bucket: CheckResult("minio-endpoint", FAIL, "down"),
        )
        monkeypatch.setattr(
            preflight, "check_lakekeeper",
            lambda url: CheckResult("lakekeeper", FAIL, "down"),
        )
        results = run_preflight(self._args(tmp_path), _REPO_ROOT)
        by_name = {r.name: r for r in results}
        assert by_name["snapshot-seeded"].status == SKIP
        assert by_name["feature-tables"].status == SKIP
        assert by_name["warehouse"].status == SKIP

    def test_all_checks_reported_in_order(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            preflight, "check_minio_endpoint",
            lambda endpoint, bucket: CheckResult("minio-endpoint", FAIL, "down"),
        )
        monkeypatch.setattr(
            preflight, "check_lakekeeper",
            lambda url: CheckResult("lakekeeper", FAIL, "down"),
        )
        results = run_preflight(self._args(tmp_path), _REPO_ROOT)
        names = [r.name for r in results]
        assert names == [
            "required-files", "snapshot-archive", "minio-endpoint",
            "snapshot-seeded", "analytics-duckdb", "feature-tables",
            "lakekeeper", "warehouse",
        ]

    def test_never_writes_anything(self, tmp_path, monkeypatch):
        """The preflight is read-only: running it against an empty tmp dir
        must not create any file or directory."""
        monkeypatch.setattr(
            preflight, "check_minio_endpoint",
            lambda endpoint, bucket: CheckResult("minio-endpoint", FAIL, "down"),
        )
        monkeypatch.setattr(
            preflight, "check_lakekeeper",
            lambda url: CheckResult("lakekeeper", FAIL, "down"),
        )
        run_preflight(self._args(tmp_path), _REPO_ROOT)
        assert list(tmp_path.iterdir()) == []


class TestFormatResults:
    def test_failure_summary_and_exit_semantics(self):
        results = [
            CheckResult("a", PASS, "ok"),
            CheckResult("b", FAIL, "broken"),
        ]
        output = format_results(results)
        assert "1 check(s) FAILED" in output
        assert any(r.failed for r in results)

    def test_all_pass_summary(self):
        results = [CheckResult("a", PASS, "ok")]
        output = format_results(results)
        assert "All checks passed" in output
