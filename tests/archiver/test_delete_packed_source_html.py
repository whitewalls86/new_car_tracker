"""Unit tests for archiver/processors/delete_packed_source_html.py (Plan 131 Stage 4).

This is the only job in the repo that deletes bronze data, against an
un-versioned bucket, so the tests that matter are the ones asserting it does
**not** delete. An in-memory fake stands in for MinIO: real packs are written,
really read back over ranged GETs, and really verified — a MagicMock would
assert away exactly the behaviour worth testing.

No MinIO, no DuckDB, no dictionary registry.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import shared.minio as minio
from archiver.processors import delete_packed_source_html as pruner
from shared.compression import compress_frame
from shared.packfile import (
    PackMember,
    build_pack,
    index_key,
    pack_key,
    write_index_parquet,
)

_YEAR, _MONTH, _TYPE = 2026, 4, "detail_page"
_BUCKET = "bronze"
_HTML_PREFIX = f"html/year={_YEAR}/month={_MONTH}/artifact_type={_TYPE}/"


# ---------------------------------------------------------------------------
# Fake object store
# ---------------------------------------------------------------------------

class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _Paginator:
    def __init__(self, store: "FakeS3"):
        self._store = store

    def paginate(self, Bucket=None, Prefix="", Delimiter=None):  # noqa: N803 - boto3 API
        self._store.calls["list"] += 1
        keys = sorted(k for k in self._store.objects if k.startswith(Prefix))
        yield {
            "Contents": [
                {
                    "Key": k,
                    "Size": len(self._store.objects[k]),
                    "LastModified": self._store.modified.get(k),
                }
                for k in keys
            ]
        }


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.modified: dict[str, datetime] = {}
        self.deleted: list[str] = []
        self.calls: Counter = Counter()

    def _missing(self, operation: str):
        from botocore.exceptions import ClientError

        return ClientError({"Error": {"Code": "NoSuchKey"}}, operation)

    def get_object(self, Bucket=None, Key=None, Range=None, **kwargs):  # noqa: N803
        self.calls["get_object"] += 1
        if Key not in self.objects:
            raise self._missing("GetObject")
        data = self.objects[Key]
        if Range:
            start, end = Range.replace("bytes=", "").split("-")
            data = data[int(start): int(end) + 1]
        return {"Body": _Body(data)}

    def head_object(self, Bucket=None, Key=None, **kwargs):  # noqa: N803
        if Key not in self.objects:
            raise self._missing("HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def delete_object(self, Bucket=None, Key=None, **kwargs):  # noqa: N803
        self.calls["delete_object"] += 1
        self.deleted.append(Key)
        self.objects.pop(Key, None)
        return {}

    def get_paginator(self, name):
        return _Paginator(self)


def _html(i: int) -> bytes:
    return (
        f"<html><head><title>vehicle {i}</title></head><body>"
        + f"<p>{'specification ' * 60}</p>" * 4
        + f"<span id='vin'>{i:08d}</span></body></html>"
    ).encode("utf-8")


def _source_key(i: int) -> str:
    return f"{_HTML_PREFIX}uuid-{i:03d}.html.zst"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("HTML_COMPRESSION_DICT_ID", raising=False)
    minio.clear_pack_caches()
    yield
    minio.clear_pack_caches()


@pytest.fixture
def store(mocker):
    fake = FakeS3()
    mocker.patch.object(pruner, "get_boto3_client", return_value=fake)
    mocker.patch.object(minio, "get_boto3_client", return_value=fake)
    return fake


def _seed(
    store: FakeS3,
    count: int = 6,
    *,
    seq: int = 0,
    start: int = 0,
    write_sidecar: bool = True,
    sidecar_age_days: float = 30.0,
    artifact_ids: bool = True,
):
    """Write *count* source objects and pack them, exactly as Stage 2 would."""
    keys = [_source_key(i) for i in range(start, start + count)]
    for i, key in zip(range(start, start + count), keys):
        store.objects[key] = compress_frame(_html(i), level=1)

    members = [
        PackMember(
            source_key=key,
            content=_html(i),
            artifact_id=(1000 + i) if artifact_ids else None,
            listing_id=f"L{i // 2:03d}",
        )
        for i, key in zip(range(start, start + count), keys)
    ]
    pack = build_pack(members, frame_target_bytes=4096)
    key = pack_key(_TYPE, _YEAR, _MONTH, seq)
    store.objects[key] = pack.data
    store.modified[key] = datetime.now(timezone.utc) - timedelta(days=sidecar_age_days)
    if write_sidecar:
        store.objects[index_key(key)] = write_index_parquet(pack.entries)
        store.modified[index_key(key)] = store.modified[key]
    return keys, pack, key


def _run(**kwargs):
    params = {
        "artifact_type": _TYPE,
        "year": _YEAR,
        "month": _MONTH,
        "max_objects": 1000,
        "max_packs": 0,
        "status_breakdown": False,
        "bucket": _BUCKET,
    }
    params.update(kwargs)
    return pruner.delete_packed_source_html(**params)


# ---------------------------------------------------------------------------
# Dry-run is the default
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_apply_absent_deletes_nothing(self, store):
        keys, _, _ = _seed(store, 6)

        result = _run()

        assert result["mode"] == "dry_run"
        assert result["objects_deleted"] == 0
        assert store.deleted == []
        assert all(key in store.objects for key in keys)

    def test_dry_run_still_verifies_every_member(self, store):
        _seed(store, 6)

        result = _run()

        assert result["objects_verified"] == 6
        assert result["objects_refused"] == 0

    def test_dry_run_reports_what_apply_would_free(self, store):
        keys, _, _ = _seed(store, 4)
        expected = sum(len(store.objects[key]) for key in keys)

        dry = _run()
        applied = _run(apply=True)

        assert dry["objects_deleted"] == 0
        assert applied["objects_deleted"] == 4
        assert applied["bytes_freed"] == expected


# ---------------------------------------------------------------------------
# The safety property
# ---------------------------------------------------------------------------

class TestRefusesToDelete:
    def test_a_key_with_no_sidecar_entry_is_never_deleted(self, store):
        keys, _, _ = _seed(store, 4)
        unpacked = _source_key(99)
        store.objects[unpacked] = compress_frame(_html(99), level=1)

        result = _run(apply=True)

        assert unpacked in store.objects
        assert unpacked not in store.deleted
        assert result["objects_deleted"] == 4
        assert set(store.deleted) == set(keys)

    def test_an_orphan_pack_never_has_its_sources_deleted(self, store):
        keys, _, orphan = _seed(store, 4, write_sidecar=False)

        result = _run(apply=True)

        assert store.deleted == []
        assert result["orphan_packs"] == [orphan]
        assert result["packs_considered"] == 0
        assert all(key in store.objects for key in keys)

    def test_a_sidecar_that_disagrees_with_its_pack_blocks_the_whole_pack(self, store):
        keys, pack, key = _seed(store, 4)
        # A sidecar describing more members than the pack header claims.
        extra = replace(pack.entries[0], source_key=_source_key(500))
        store.objects[index_key(key)] = write_index_parquet([*pack.entries, extra])

        result = _run(apply=True)

        assert store.deleted == []
        assert result["objects_refused"] == 4
        assert "sidecar has 5 members" in result["failures"][0]["error"]

    def test_a_sha256_mismatch_refuses_that_member_only(self, store):
        keys, pack, key = _seed(store, 4)
        tampered = [
            replace(e, raw_sha256="0" * 64) if e.source_key == keys[1] else e
            for e in pack.entries
        ]
        store.objects[index_key(key)] = write_index_parquet(tampered)

        result = _run(apply=True)

        assert keys[1] in store.objects
        assert keys[1] not in store.deleted
        assert result["objects_refused"] == 1
        assert result["objects_deleted"] == 3

    def test_an_object_whose_bytes_differ_from_the_pack_is_refused(self, store):
        keys, _, _ = _seed(store, 4)
        # The pack is self-consistent; the object is not what was packed. Only
        # comparing the two can catch this.
        store.objects[keys[2]] = compress_frame(b"<html>something else</html>", level=1)

        result = _run(apply=True)

        assert keys[2] in store.objects
        assert result["objects_refused"] == 1
        assert result["objects_deleted"] == 3
        assert "refusing to delete" in result["failures"][0]["error"]

    def test_objects_the_read_path_could_not_resolve_are_refused(self, store, monkeypatch):
        """Objects living where the production resolver does not look.

        ``read_html``'s fallback derives its search prefix from the key's own
        hive partition and only ever looks under ``html_packs/``. So a
        deployment whose objects and packs sit under some other root is one
        where a packed artifact is unreadable once its object is gone — the
        bytes exist and nothing can find them.

        This is the shape a namespaced test run has, which is why the Stage 3
        integration tests isolate by artifact_type rather than by prefix. Here
        it is asserted directly: the job refuses rather than deletes.
        """
        monkeypatch.setattr(pruner, "_HTML_PREFIX", "sandbox/html")
        monkeypatch.setattr(pruner, "_PACK_PREFIX", "sandbox/html_packs")

        keys = []
        members = []
        for i in range(3):
            key = f"sandbox/html/year={_YEAR}/month={_MONTH}/artifact_type={_TYPE}/u{i}.html.zst"
            store.objects[key] = compress_frame(_html(i), level=1)
            keys.append(key)
            members.append(PackMember(source_key=key, content=_html(i), listing_id="L000"))
        pack = build_pack(members, frame_target_bytes=4096)
        pack_object = f"sandbox/html_packs/{_TYPE}/{_YEAR:04d}/{_MONTH:02d}/pack-00000.zpack"
        store.objects[pack_object] = pack.data
        store.objects[index_key(pack_object)] = write_index_parquet(pack.entries)

        result = _run(apply=True)

        assert store.deleted == []
        assert result["objects_refused"] == 3
        assert result["objects_deleted"] == 0
        assert "could not find it" in result["failures"][0]["error"]
        assert all(key in store.objects for key in keys)

    def test_year_and_month_are_required(self, store):
        result = pruner.delete_packed_source_html(apply=True, year=None, month=None)

        assert result["error"]
        assert store.deleted == []


# ---------------------------------------------------------------------------
# Caps and resume
# ---------------------------------------------------------------------------

class TestCapsAndResume:
    def test_per_run_cap_is_honoured_exactly(self, store):
        _seed(store, 10)

        result = _run(apply=True, max_objects=3)

        assert result["objects_deleted"] == 3
        assert len(store.deleted) == 3
        assert result["capped"] is True

    def test_cap_of_zero_deletes_nothing(self, store):
        _seed(store, 5)

        result = _run(apply=True, max_objects=0)

        assert result["objects_deleted"] == 0
        assert store.deleted == []

    def test_resume_deletes_each_object_at_most_once(self, store):
        keys, _, _ = _seed(store, 9)

        first = _run(apply=True, max_objects=4)
        second = _run(apply=True, max_objects=4)
        third = _run(apply=True, max_objects=4)

        assert first["objects_deleted"] == 4
        assert second["objects_deleted"] == 4
        assert third["objects_deleted"] == 1
        assert sorted(store.deleted) == sorted(keys)
        assert len(store.deleted) == len(set(store.deleted))

    def test_resume_costs_no_read_for_an_already_deleted_object(self, store):
        _seed(store, 8)
        _run(apply=True, max_objects=8)
        store.calls.clear()

        again = _run(apply=True)

        # The surviving-object listing is the checkpoint: a drained pack is
        # recognised from the listing alone, with no per-object request.
        assert again["objects_deleted"] == 0
        assert store.calls["get_object"] == 0
        assert store.calls["delete_object"] == 0

    def test_max_packs_limits_work_to_one_pack(self, store):
        first, _, _ = _seed(store, 4, seq=0, start=0)
        second, _, _ = _seed(store, 4, seq=1, start=100)

        result = _run(apply=True, max_packs=1)

        assert result["packs_drained"] == 1
        assert set(store.deleted) == set(first)
        assert all(key in store.objects for key in second)


# ---------------------------------------------------------------------------
# The grace period
# ---------------------------------------------------------------------------

class TestGracePeriod:
    def test_default_is_zero_and_deletes_a_pack_written_seconds_ago(self, store):
        _seed(store, 4, sidecar_age_days=0)

        result = _run(apply=True)

        assert pruner.DELETE_GRACE_DAYS == 0
        assert result["objects_deleted"] == 4
        assert result["packs_skipped_grace"] == 0

    def test_a_grace_period_defers_a_recent_pack(self, store):
        keys, _, _ = _seed(store, 4, sidecar_age_days=2)

        result = _run(apply=True, grace_days=14)

        assert result["objects_deleted"] == 0
        assert result["packs_skipped_grace"] == 1
        assert all(key in store.objects for key in keys)

    def test_a_grace_period_admits_an_older_pack(self, store):
        _seed(store, 4, sidecar_age_days=30)

        result = _run(apply=True, grace_days=14)

        assert result["objects_deleted"] == 4
        assert result["packs_skipped_grace"] == 0


# ---------------------------------------------------------------------------
# Reporting — status never blocks, inodes are reported
# ---------------------------------------------------------------------------

class TestReporting:
    def test_inodes_are_reported_not_just_objects_and_bytes(self, store):
        _seed(store, 5)

        result = _run(apply=True)

        assert result["objects_deleted"] == 5
        assert result["bytes_freed"] > 0
        assert result["inodes_freed_estimated"] == pytest.approx(5 * 2.24)
        assert "inodes_freed_measured" in result

    def test_members_with_no_event_row_are_deleted_and_counted(self, store):
        # A sidecar entry with no artifact_id is exactly how the packer records
        # an artifact that had no artifacts_queue_events row: April's 42,276.
        keys, _, _ = _seed(store, 4, artifact_ids=False)

        result = _run(apply=True)

        assert result["objects_deleted"] == 4
        assert result["by_status"] == {"no_event_row": 4}
        assert store.deleted == sorted(keys, key=store.deleted.index)

    def test_status_is_reported_and_never_blocks(self, store, mocker):
        _seed(store, 4)
        mocker.patch.object(
            pruner, "_load_statuses",
            return_value={1000: "complete", 1001: "ok", 1002: "skip", 1003: "pending"},
        )

        result = _run(apply=True, status_breakdown=True)

        # Every class is deleted, including `ok` (success) and `pending`.
        assert result["objects_deleted"] == 4
        assert result["by_status"] == {
            "complete": 1, "ok": 1, "pending": 1, "skip": 1,
        }

    def test_an_unavailable_status_breakdown_does_not_stop_deletion(self, store, mocker):
        _seed(store, 3)
        mocker.patch(
            "shared.duckdb_s3.get_duckdb_s3_connection",
            side_effect=RuntimeError("duckdb unavailable"),
        )

        result = _run(apply=True, status_breakdown=True)

        assert result["objects_deleted"] == 3
        assert result["by_status"] == {"no_event_row": 3}


# ---------------------------------------------------------------------------
# The read path is what is verified
# ---------------------------------------------------------------------------

class TestReadPathVerification:
    def test_the_full_resolver_is_exercised_per_pack(self, store, mocker):
        _seed(store, 8)
        spy = mocker.spy(minio, "read_packed_html")

        _run(apply=True, sample_full_reads=3)

        assert spy.call_count == 3

    def test_a_resolver_failure_refuses_that_member(self, store, mocker):
        keys, _, _ = _seed(store, 4)
        mocker.patch.object(minio, "read_packed_html", return_value=None)

        result = _run(apply=True, sample_full_reads=1)

        assert result["objects_refused"] == 1
        assert result["objects_deleted"] == 3

    def test_verification_reads_the_object_not_the_pack_fallback(self, store, mocker):
        """The comparison must not be the pack against itself.

        With Stage 3 live, read_html answers from the pack once an object is
        missing. If verification used it, a deleted-then-reverified object
        would compare the pack to the pack and always agree.
        """
        _seed(store, 3)
        spy = mocker.spy(minio, "read_html")

        _run(apply=True)

        assert spy.call_count == 0

    def test_every_deleted_object_is_still_readable_afterwards(self, store):
        keys, _, _ = _seed(store, 6)

        _run(apply=True)
        minio.clear_pack_caches()

        assert store.deleted
        for i, key in enumerate(keys):
            assert key not in store.objects
            assert minio.read_html(f"s3://{_BUCKET}/{key}") == _html(i)
