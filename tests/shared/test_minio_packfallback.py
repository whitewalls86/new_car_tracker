"""Plan 131 Stage 3: ``read_html`` falls back to the pack index.

An in-memory object store stands in for MinIO so a real pack is written, stored
and read back over real ranged GETs — the parts worth testing here are exactly
the ones a MagicMock would assert away. No MinIO, no DuckDB, no dictionary
registry except in the test that covers dictionary frames.

The property under test is the one Stage 4 will delete source objects on the
strength of: for an artifact that is inside a verified pack, ``read_html``
returns its exact bytes whether or not the source object still exists.
"""
from __future__ import annotations

from collections import Counter

import pytest
import zstandard as zstd

import shared.minio as minio
from shared.compression import compress_frame
from shared.packfile import (
    PackIndexMismatchError,
    PackMember,
    PackVerificationError,
    build_pack,
    index_key,
    pack_key,
    write_index_parquet,
)

_YEAR, _MONTH, _TYPE = 2026, 4, "detail_page"
_BUCKET = "bronze"


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
        yield {"Contents": [{"Key": k, "Size": len(self._store.objects[k])} for k in keys]}


class FakeS3:
    """Enough of the boto3 S3 client for the read path, including ranged GETs."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.calls: Counter = Counter()

    def _missing(self, key: str, operation: str):
        from botocore.exceptions import ClientError

        return ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
            operation,
        )

    def get_object(self, Bucket=None, Key=None, Range=None, **kwargs):  # noqa: N803
        self.calls["get_object"] += 1
        if Key.endswith(".idx.parquet"):
            self.calls["get_index"] += 1
        if Key not in self.objects:
            raise self._missing(Key, "GetObject")
        data = self.objects[Key]
        if Range:
            start, end = Range.replace("bytes=", "").split("-")
            data = data[int(start): int(end) + 1]
        return {"Body": _Body(data)}

    def head_object(self, Bucket=None, Key=None, **kwargs):  # noqa: N803
        self.calls["head_object"] += 1
        if Key not in self.objects:
            raise self._missing(Key, "HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def get_paginator(self, name):
        return _Paginator(self)


def _html(i: int) -> bytes:
    return (
        f"<html><head><title>vehicle {i}</title></head><body>"
        + f"<p>{'specification ' * 60}</p>" * 4
        + f"<span id='vin'>{i:08d}</span></body></html>"
    ).encode("utf-8")


def _source_key(i: int, *, year: int = _YEAR, month: int = _MONTH) -> str:
    return (
        f"html/year={year}/month={month}/artifact_type={_TYPE}/uuid-{i:03d}.html.zst"
    )


@pytest.fixture(autouse=True)
def _clean_caches(monkeypatch):
    """Caches are process-wide by design; no test may inherit another's."""
    monkeypatch.delenv("HTML_COMPRESSION_DICT_ID", raising=False)
    minio.clear_pack_caches()
    yield
    minio.clear_pack_caches()


@pytest.fixture
def store(mocker):
    fake = FakeS3()
    mocker.patch.object(minio, "get_boto3_client", return_value=fake)
    return fake


@pytest.fixture
def dictionary(mocker):
    """A trained dictionary resolved without Postgres or MinIO.

    Packed frames carry a dictionary (Stage 0d: without one the first member of
    every frame grows 8,578 -> 32,952 bytes), and readers resolve it from each
    frame's own header — never from object metadata, which a copy can rewrite.
    """
    import shared.compression as compression

    samples = [
        (
            f"<html><head><title>vehicle {i % 7}</title></head><body>"
            + "specification " * 40
            + "</body></html>"
        ).encode()
        for i in range(100)
    ]
    trained = zstd.train_dictionary(4096, samples)
    compression.clear_dictionary_cache()
    mocker.patch.object(
        compression,
        "_load_registered",
        lambda dict_id: compression.RegisteredDictionary(
            dict_id=dict_id,
            raw=trained.as_bytes(),
            data=zstd.ZstdCompressionDict(trained.as_bytes()),
            source="test",
        ),
    )
    yield trained
    compression.clear_dictionary_cache()


def _write_sources(store: FakeS3, count: int, *, level: int = 1, dict_id=None) -> list[str]:
    keys = []
    for i in range(count):
        key = _source_key(i)
        store.objects[key] = compress_frame(_html(i), level=level, dict_id=dict_id)
        keys.append(key)
    return keys


def _pack_sources(
    store: FakeS3,
    keys: list[str],
    *,
    seq: int = 0,
    frame_target_bytes: int = 4096,
    delete_sources: bool = True,
):
    """Pack *keys* into one stored pack + sidecar, exactly as Stage 2 does."""
    members = [
        PackMember(
            source_key=key,
            content=_html(int(key.rsplit("uuid-", 1)[1].split(".", 1)[0])),
            artifact_id=1000 + i,
            listing_id=f"L{i // 2:03d}",
        )
        for i, key in enumerate(keys)
    ]
    pack = build_pack(members, frame_target_bytes=frame_target_bytes)
    key = pack_key(_TYPE, _YEAR, _MONTH, seq)
    store.objects[key] = pack.data
    store.objects[index_key(key)] = write_index_parquet(pack.entries)
    if delete_sources:
        for source in keys:
            store.objects.pop(source, None)
    return key, pack


# ---------------------------------------------------------------------------
# pack_lookup_prefix — the bounded search
# ---------------------------------------------------------------------------

class TestPackLookupPrefix:
    def test_maps_hive_partition_to_pack_prefix(self):
        assert minio.pack_lookup_prefix(_source_key(0)) == "html_packs/detail_page/2026/04/"

    def test_zero_pads_the_month(self):
        key = "html/year=2026/month=9/artifact_type=results_page/a.html.zst"
        assert minio.pack_lookup_prefix(key) == "html_packs/results_page/2026/09/"

    @pytest.mark.parametrize(
        "key",
        [
            "silver_normalized/observations/part-0.parquet",
            "html/no-partitions.html.zst",
            "html/year=2026/artifact_type=detail_page/a.html.zst",
            "html/year=2026/month=13/artifact_type=detail_page/a.html.zst",
            "html/year=notayear/month=4/artifact_type=detail_page/a.html.zst",
        ],
    )
    def test_unpackable_keys_have_no_prefix(self, key):
        assert minio.pack_lookup_prefix(key) is None


# ---------------------------------------------------------------------------
# The object path is unchanged
# ---------------------------------------------------------------------------

class TestObjectStillPresent:
    def test_returns_the_object_without_touching_any_pack(self, store):
        keys = _write_sources(store, 3)
        _pack_sources(store, keys, delete_sources=False)
        store.calls.clear()

        assert minio.read_html(f"s3://{_BUCKET}/{keys[1]}") == _html(1)
        assert store.calls["get_object"] == 1
        assert store.calls["list"] == 0
        assert store.calls["get_index"] == 0

    def test_bare_key_uses_the_default_bucket(self, store):
        keys = _write_sources(store, 1)
        assert minio.read_html(keys[0]) == _html(0)

    def test_corrupt_object_raises_rather_than_serving_the_pack(self, store):
        keys = _write_sources(store, 2)
        _pack_sources(store, keys, delete_sources=False)
        store.objects[keys[0]] = b"not a zstd frame at all"

        with pytest.raises(zstd.ZstdError):
            minio.read_html(keys[0])


# ---------------------------------------------------------------------------
# The fallback
# ---------------------------------------------------------------------------

class TestPackFallback:
    def test_deleted_source_is_served_from_the_pack_byte_identically(self, store):
        keys = _write_sources(store, 6)
        _pack_sources(store, keys)

        for i, key in enumerate(keys):
            assert minio.read_html(f"s3://{_BUCKET}/{key}") == _html(i)

    def test_identical_bytes_whether_read_from_object_or_pack(self, store):
        keys = _write_sources(store, 4)
        from_objects = [minio.read_html(k) for k in keys]

        _pack_sources(store, keys)
        minio.clear_pack_caches()
        from_pack = [minio.read_html(k) for k in keys]

        assert from_pack == from_objects

    def test_second_pack_in_the_month_is_found(self, store):
        first = _write_sources(store, 2)
        second = [_source_key(i) for i in range(2, 5)]
        for i, key in enumerate(second, start=2):
            store.objects[key] = compress_frame(_html(i), level=1)

        _pack_sources(store, first, seq=0)
        _pack_sources(store, second, seq=1)

        assert minio.read_html(second[-1]) == _html(4)

    def test_a_pack_written_after_the_listing_was_cached_is_still_found(self, store):
        keys = _write_sources(store, 4)
        early, late = keys[:2], keys[2:]
        _pack_sources(store, early, seq=0)

        assert minio.read_html(early[0]) == _html(0)  # caches the listing

        _pack_sources(store, late, seq=1)
        assert minio.read_html(late[0]) == _html(2)

    def test_dictionary_and_plain_objects_and_packs_are_all_readable(self, store, dictionary):
        plain = _source_key(0)
        dicted = _source_key(1)
        store.objects[plain] = compress_frame(_html(0), level=3, dict_id=None)
        store.objects[dicted] = compress_frame(_html(1), level=9, dict_id=dictionary.dict_id())

        packed_keys = [_source_key(i) for i in (2, 3)]
        _pack_sources(store, packed_keys)

        assert minio.read_html(plain) == _html(0)
        assert minio.read_html(dicted) == _html(1)
        assert minio.read_html(packed_keys[0]) == _html(2)

    def test_frames_inside_a_pack_may_use_a_dictionary(self, store, dictionary):
        keys = [_source_key(i) for i in range(3)]
        members = [
            PackMember(source_key=k, content=_html(i), listing_id="L000")
            for i, k in enumerate(keys)
        ]
        pack = build_pack(members, dict_id=dictionary.dict_id(), frame_target_bytes=4096)
        key = pack_key(_TYPE, _YEAR, _MONTH, 0)
        store.objects[key] = pack.data
        store.objects[index_key(key)] = write_index_parquet(pack.entries)

        assert pack.dict_id == dictionary.dict_id()
        assert minio.read_html(keys[2]) == _html(2)


# ---------------------------------------------------------------------------
# Logical existence follows the same loose-or-packed boundary as reads
# ---------------------------------------------------------------------------

class TestArtifactExists:
    def test_loose_object_exists_without_searching_packs(self, store):
        keys = _write_sources(store, 1)

        assert minio.artifact_exists(keys[0]) is True
        assert store.calls["list"] == 0
        assert store.calls["get_index"] == 0

    def test_pruned_source_exists_through_its_pack(self, store):
        keys = _write_sources(store, 2)
        _pack_sources(store, keys)

        assert minio.object_exists(keys[0]) is False
        assert minio.artifact_exists(keys[0]) is True

    def test_missing_loose_and_packed_artifact_does_not_exist(self, store):
        assert minio.artifact_exists(_source_key(99)) is False


# ---------------------------------------------------------------------------
# Absent from both places
# ---------------------------------------------------------------------------

class TestMissingEverywhere:
    def _client_error(self):
        from botocore.exceptions import ClientError

        return ClientError

    def test_raises_the_same_error_shape_as_today(self, store):
        _write_sources(store, 2)
        with pytest.raises(self._client_error()) as exc_info:
            minio.read_html(_source_key(99))
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"
        assert exc_info.value.operation_name == "GetObject"

    def test_unpackable_key_shape_does_not_search(self, store):
        with pytest.raises(self._client_error()):
            minio.read_html("some/other/prefix/file.bin")
        assert store.calls["list"] == 0

    def test_missing_in_an_unpacked_month_lists_once_and_gives_up(self, store):
        with pytest.raises(self._client_error()):
            minio.read_html(_source_key(7))
        # No sidecars exist, so re-listing could only return the same nothing.
        assert store.calls["list"] == 1

    def test_non_404_errors_propagate_without_a_pack_search(self, store, mocker):
        from botocore.exceptions import ClientError

        denied = ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
        mocker.patch.object(store, "get_object", side_effect=denied)

        with pytest.raises(ClientError) as exc_info:
            minio.read_html(_source_key(0))
        assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
        assert store.calls["list"] == 0

    def test_fallback_can_be_switched_off(self, store, mocker):
        keys = _write_sources(store, 2)
        _pack_sources(store, keys)
        mocker.patch.object(minio, "PACK_READ_FALLBACK", False)

        with pytest.raises(self._client_error()):
            minio.read_html(keys[0])
        assert store.calls["list"] == 0


# ---------------------------------------------------------------------------
# Disagreement is an error, not a silent preference
# ---------------------------------------------------------------------------

class TestIndexAndPackMustAgree:
    def test_sidecar_from_a_different_pack_is_rejected(self, store):
        keys = _write_sources(store, 4)
        key, _ = _pack_sources(store, keys)

        other = build_pack(
            [PackMember(source_key=_source_key(9), content=_html(9), listing_id="L009")],
            frame_target_bytes=4096,
        )
        store.objects[index_key(key)] = write_index_parquet(other.entries)
        minio.clear_pack_caches()

        # The key is not in the replaced sidecar, so it resolves to nothing and
        # the original 404 stands.
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            minio.read_html(keys[0])

        # ...but a key the sidecar *does* claim exposes the disagreement.
        with pytest.raises(PackIndexMismatchError):
            minio.read_html(_source_key(9))

    def test_index_entry_pointing_outside_its_frame_is_an_error(self, store):
        keys = _write_sources(store, 4)
        key, pack = _pack_sources(store, keys)

        from dataclasses import replace

        broken = [replace(e, length=e.length + 10_000_000) for e in pack.entries]
        store.objects[index_key(key)] = write_index_parquet(broken)
        minio.clear_pack_caches()

        with pytest.raises(PackIndexMismatchError):
            minio.read_html(keys[0])

    def test_sha256_mismatch_is_reported_not_returned(self, store):
        keys = _write_sources(store, 4)
        key, pack = _pack_sources(store, keys)

        from dataclasses import replace

        tampered = [
            replace(e, raw_sha256="0" * 64) if e.source_key == keys[0] else e
            for e in pack.entries
        ]
        store.objects[index_key(key)] = write_index_parquet(tampered)
        minio.clear_pack_caches()

        with pytest.raises(PackVerificationError):
            minio.read_html(keys[0])
        assert minio.read_html(keys[1]) == _html(1)


# ---------------------------------------------------------------------------
# Caching — the reason a reparse is not 32 GETs per artifact
# ---------------------------------------------------------------------------

class TestCaching:
    def test_a_second_member_reuses_keys_and_fetches_only_its_sidecar(self, store):
        keys = _write_sources(store, 8)
        _pack_sources(store, keys)

        minio.read_html(keys[0])
        store.calls.clear()
        minio.read_html(keys[1])

        assert store.calls["list"] == 0
        assert store.calls["get_index"] == 1
        assert store.calls["head_object"] == 0

    def test_clearing_the_caches_makes_the_next_read_cold_again(self, store):
        keys = _write_sources(store, 4)
        _pack_sources(store, keys)

        minio.read_html(keys[0])
        minio.clear_pack_caches()
        store.calls.clear()
        minio.read_html(keys[1])

        assert store.calls["get_index"] == 1
        assert store.calls["head_object"] == 1

    def test_index_cache_is_bounded(self, store, mocker):
        mocker.patch.object(minio, "PACK_INDEX_CACHE_PACKS", 1)
        keys = _write_sources(store, 4)
        _pack_sources(store, keys[:2], seq=0)
        _pack_sources(store, keys[2:], seq=1)

        minio.read_html(keys[2])   # loads both sidecars, keeps one
        minio.read_html(keys[0])
        assert len(minio._pack_index_cache) == 1

    def test_month_sized_scan_stays_resident_as_source_keys_only(self, store, mocker):
        pack_count = 5
        mocker.patch.object(minio, "PACK_INDEX_CACHE_PACKS", pack_count)
        keys = [_source_key(i) for i in range(pack_count)]
        for seq, key in enumerate(keys):
            _pack_sources(store, [key], seq=seq)

        minio.read_html(keys[-1])

        assert len(minio._pack_index_cache) == pack_count
        assert all(
            table.schema.names == ["source_key"]
            for table in minio._pack_index_cache.values()
        )

        store.calls.clear()
        minio.read_html(keys[0])

        # The month-wide key scan is warm. Only the one matching sidecar is
        # fetched for the remaining entry fields; no LRU restart occurs.
        assert store.calls["list"] == 0
        assert store.calls["get_index"] == 1
        assert len(minio._pack_index_cache) == pack_count

    def test_default_cache_covers_every_current_packed_month(self):
        # April-May-June-July have 32, 41, 38 and 33 sidecars respectively.
        assert minio.PACK_INDEX_CACHE_PACKS >= 41
