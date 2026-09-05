"""Unit tests for scripts/seed_lake_snapshot.py (Plan 120, Phase 4)."""
from __future__ import annotations

import io
import json
import tarfile
from unittest.mock import MagicMock, PropertyMock

import pytest
import zstandard as zstd

from scripts.lake_snapshot_common import (
    ChecksumMismatchError,
    LakeSnapshotError,
    ProductionTargetError,
    sha256_file,
)
from scripts.seed_lake_snapshot import ensure_bucket, main, seed_lake_snapshot
from shared.lake_snapshot_postgres import (
    POSTGRES_SNAPSHOT_TABLES,
    UnknownSnapshotTableError,
)
from shared.queries import REPLACE_POSTGRES_SNAPSHOT_TABLE

LOCAL_POSTGRES_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"


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


def _build_snapshot(tmp_path, files=None, raw_members=None, tables=None):
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
    if tables is not None:
        manifest["tables"] = tables
    (snapshot_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return archive, manifest


# The four lake tables, all non-empty, as the archive manifest records them.
# Only `rows` is read by --require-non-empty.
_FULL_TABLES = {
    "silver_observations": {"rows": 16847},
    "price_observation_events": {"rows": 16615},
    "vin_to_listing_events": {"rows": 4036},
    "blocked_cooldown_events": {"rows": 26},
}

_SEARCH_CONFIGS_JSON = json.dumps(
    [{"search_key": "toyota-camry-60614", "enabled": True, "params": {"zip": "60614"}}]
).encode("utf-8")
_TRACKED_MODELS_JSON = json.dumps(
    [{"search_key": "toyota-camry-60614", "make": "toyota", "model": "camry"}]
).encode("utf-8")


def _files_with_postgres(**overrides):
    """The default archive contents plus the two Postgres dimension members."""
    files = {
        "silver_normalized/observations/source=detail/obs_year=2026/obs_month=7/part-000.parquet":
            b"a" * 10,
        "ops_normalized/price_observation_events/year=2026/month=7/part-000.parquet": b"b" * 20,
        "ops_normalized/vin_to_listing_events/year=2026/month=7/part-000.parquet": b"c" * 5,
        "ops_normalized/blocked_cooldown_events/year=2026/month=7/part-000.parquet": b"d" * 5,
        "expected/feature_audit_summary.json": b"{}",
        "postgres/public.search_configs.json": _SEARCH_CONFIGS_JSON,
        "postgres/ops.tracked_models.json": _TRACKED_MODELS_JSON,
    }
    files.update(overrides)
    return files


def _mock_conn(rowcounts=(1, 1)):
    """A psycopg2-shaped connection whose cursor reports *rowcounts* in order.

    `with conn:` is the transaction and `with conn.cursor()` the cursor, so both
    context managers have to resolve to something -- a bare MagicMock returns a
    new child for `__enter__` and the assertions would read the wrong object.
    """
    cur = MagicMock()
    cur.rowcount = rowcounts[0]
    type(cur).rowcount = PropertyMock(side_effect=list(rowcounts))
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    return conn, cur


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
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["archive"]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
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

    def test_main_cli_smoke(self, tmp_path, capsys, mocker):
        archive, _ = _build_snapshot(tmp_path)
        client = _mock_client()
        mocker.patch(
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


class TestPostgresHalf:
    """Plan 162 Stage P: the two dbt sources that resolve through
    postgres_scan() and so cannot be objects in a bucket."""

    def test_postgres_members_go_to_postgres_and_never_to_minio(self, tmp_path):
        """The routing property, which is the whole reason `postgres/` is its own
        top-level prefix rather than a subdirectory of an existing one."""
        archive, _ = _build_snapshot(tmp_path, files=_files_with_postgres())
        client = _mock_client()
        conn, _ = _mock_conn()

        result = seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False,
            postgres_url=LOCAL_POSTGRES_URL, client=client, conn=conn,
        )

        uploaded = {call.args[2] for call in client.upload_file.call_args_list}
        assert not any(key.startswith("postgres/") for key in uploaded)
        assert result["postgres_rows_by_table"] == {
            "public.search_configs": 1, "ops.tracked_models": 1,
        }
        assert result["postgres_skipped"] == []

    def test_applies_the_tables_in_allowlist_order(self, tmp_path):
        """Filesystem order is alphabetical, which puts ops before public. The
        seeder follows POSTGRES_SNAPSHOT_TABLES instead, so the order a future
        foreign key would need is the one declared rather than the one the tar
        happened to produce."""
        archive, _ = _build_snapshot(tmp_path, files=_files_with_postgres())
        conn, cur = _mock_conn()

        seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False,
            postgres_url=LOCAL_POSTGRES_URL, client=_mock_client(), conn=conn,
        )

        # Compared against production's own template rather than a regex over
        # its text: reading the relation back out with `DELETE FROM (\S+);`
        # asserted the seeder's *order* while checking almost nothing about the
        # statements themselves, and it was the last SQL-shaped literal in this
        # file. Rendering the template per table checks both at once.
        assert [call.args[0] for call in cur.execute.call_args_list] == [
            REPLACE_POSTGRES_SNAPSHOT_TABLE.format(schema=schema, table=table)
            for schema, table in POSTGRES_SNAPSHOT_TABLES
        ]

    def test_the_json_payload_is_bound_not_formatted(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path, files=_files_with_postgres())
        conn, cur = _mock_conn()

        seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False,
            postgres_url=LOCAL_POSTGRES_URL, client=_mock_client(), conn=conn,
        )

        payloads = [call.args[1][0] for call in cur.execute.call_args_list]
        assert json.loads(payloads[0])[0]["search_key"] == "toyota-camry-60614"
        assert json.loads(payloads[1])[0]["make"] == "toyota"

    def test_an_unknown_postgres_member_is_refused_not_skipped(self, tmp_path):
        """A snapshot carrying a table this seeder cannot place is a snapshot the
        seed would silently under-apply -- exactly the empty-world failure the
        job reading it exists to rule out."""
        files = _files_with_postgres(**{"postgres/public.users.json": b"[]"})
        archive, _ = _build_snapshot(tmp_path, files=files)
        conn, _ = _mock_conn()

        with pytest.raises(UnknownSnapshotTableError):
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False,
                postgres_url=LOCAL_POSTGRES_URL, client=_mock_client(), conn=conn,
            )

    def test_without_a_url_the_tables_are_reported_skipped(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path, files=_files_with_postgres())

        result = seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False,
            client=_mock_client(),
        )

        assert result["postgres_rows_by_table"] == {}
        assert result["postgres_skipped"] == [
            "public.search_configs", "ops.tracked_models",
        ]

    def test_refuses_a_postgres_host_that_is_not_loopback_or_private(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path, files=_files_with_postgres())
        client = _mock_client()
        conn, cur = _mock_conn()

        with pytest.raises(ProductionTargetError):
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False,
                postgres_url="postgresql://u:p@postgres:5432/cartracker",
                client=client, conn=conn,
            )
        cur.execute.assert_not_called()
        client.upload_file.assert_not_called()

    def test_the_refusal_does_not_echo_the_password(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path, files=_files_with_postgres())
        conn, _ = _mock_conn()

        with pytest.raises(ProductionTargetError) as excinfo:
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False,
                postgres_url="postgresql://u:hunter2@db.example.com:5432/cartracker",
                client=_mock_client(), conn=conn,
            )
        assert "hunter2" not in str(excinfo.value)
        assert "db.example.com" in str(excinfo.value)


class TestRequireNonEmpty:
    """The flag that turns a short snapshot from a green build over an empty
    world into a failure at the seed."""

    def test_passes_when_all_six_sources_have_rows(self, tmp_path):
        archive, _ = _build_snapshot(
            tmp_path, files=_files_with_postgres(), tables=_FULL_TABLES,
        )
        conn, _ = _mock_conn()

        result = seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False,
            postgres_url=LOCAL_POSTGRES_URL, require_non_empty=True,
            client=_mock_client(), conn=conn,
        )
        assert result["postgres_rows_by_table"]["public.search_configs"] == 1

    def test_fails_when_a_postgres_source_seeded_nothing(self, tmp_path):
        """The specific case the flag exists for: a snapshot exported before the
        Postgres half existed uploads four healthy Parquet tables and leaves
        stg_search_configs reading nothing."""
        files = _files_with_postgres()
        del files["postgres/public.search_configs.json"]
        del files["postgres/ops.tracked_models.json"]
        archive, _ = _build_snapshot(tmp_path, files=files, tables=_FULL_TABLES)
        conn, _ = _mock_conn()

        with pytest.raises(LakeSnapshotError) as excinfo:
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False,
                postgres_url=LOCAL_POSTGRES_URL, require_non_empty=True,
                client=_mock_client(), conn=conn,
            )
        assert "public.search_configs" in str(excinfo.value)
        assert "ops.tracked_models" in str(excinfo.value)

    def test_fails_on_a_lake_table_with_files_but_no_rows(self, tmp_path):
        """Read from the manifest, not the upload plan. A zero-row Parquet file
        is still a file, so a file-count check would pass exactly here."""
        tables = dict(_FULL_TABLES, vin_to_listing_events={"rows": 0})
        archive, _ = _build_snapshot(
            tmp_path, files=_files_with_postgres(), tables=tables,
        )
        conn, _ = _mock_conn()

        with pytest.raises(LakeSnapshotError) as excinfo:
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False,
                postgres_url=LOCAL_POSTGRES_URL, require_non_empty=True,
                client=_mock_client(), conn=conn,
            )
        assert "vin_to_listing_events" in str(excinfo.value)

    def test_a_manifest_with_no_tables_key_fails_rather_than_passing_vacuously(
        self, tmp_path,
    ):
        """An older manifest records no per-table counts. Reading that as "no
        table is empty" would make the flag assert nothing on exactly the
        snapshots most likely to be short."""
        archive, _ = _build_snapshot(tmp_path, files=_files_with_postgres())
        conn, _ = _mock_conn()

        with pytest.raises(LakeSnapshotError):
            seed_lake_snapshot(
                snapshot_path=archive, manifest_path=None,
                minio_endpoint="http://localhost:9000", bucket="bronze",
                clear_prefixes=False, allow_production_target=False,
                postgres_url=LOCAL_POSTGRES_URL, require_non_empty=True,
                client=_mock_client(), conn=conn,
            )

    def test_off_by_default_so_a_partial_local_seed_still_works(self, tmp_path):
        archive, _ = _build_snapshot(tmp_path)
        result = seed_lake_snapshot(
            snapshot_path=archive, manifest_path=None,
            minio_endpoint="http://localhost:9000", bucket="bronze",
            clear_prefixes=False, allow_production_target=False,
            client=_mock_client(),
        )
        assert result["uploaded_files"] == 5
