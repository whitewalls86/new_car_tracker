"""Unit tests for archiver/processors/verify_pack_read_path.py (Plan 131 Stage 3).

This script is what authorizes Stage 4's deletions, so the case that matters is
the one where it must **not** report a match. A verifier that says "identical"
about bytes that differ is worse than no verifier, and that is what these tests
are for.

The same in-memory object store the read-path tests use stands in for MinIO, so
a real pack is written, stored, and read back over real ranged GETs.
"""
from __future__ import annotations

import pytest

import shared.minio as minio
from archiver.processors import verify_pack_read_path as verifier
from shared.compression import compress_frame
from shared.packfile import (
    PackMember,
    build_pack,
    index_key,
    pack_key,
    write_index_parquet,
)
from tests.shared.test_minio_packfallback import FakeS3, _html, _source_key

_BUCKET = "bronze"
_YEAR, _MONTH, _TYPE = 2026, 4, "detail_page"


@pytest.fixture(autouse=True)
def _clean_caches():
    minio.clear_pack_caches()
    yield
    minio.clear_pack_caches()


@pytest.fixture
def store(mocker):
    fake = FakeS3()
    mocker.patch.object(minio, "get_boto3_client", return_value=fake)
    return fake


def _seed_packed(store: FakeS3, count: int = 6, *, keep_sources: bool = True):
    keys = [_source_key(i) for i in range(count)]
    for i, key in enumerate(keys):
        store.objects[key] = compress_frame(_html(i), level=1)
    members = [
        PackMember(source_key=key, content=_html(i), listing_id=f"L{i // 2:03d}")
        for i, key in enumerate(keys)
    ]
    pack = build_pack(members, frame_target_bytes=4096)
    key = pack_key(_TYPE, _YEAR, _MONTH, 0)
    store.objects[key] = pack.data
    store.objects[index_key(key)] = write_index_parquet(pack.entries)
    if not keep_sources:
        for source in keys:
            store.objects.pop(source)
    return keys, pack


def _run(**kwargs):
    params = {
        "artifact_type": _TYPE,
        "year": _YEAR,
        "month": _MONTH,
        "per_pack": 100,
        "seed": 1,
        "warm_reads": 0,
        "bucket": _BUCKET,
    }
    params.update(kwargs)
    return verifier.verify_pack_read_path(**params)


class TestPercentiles:
    def test_empty(self):
        assert verifier.percentiles([]) == {"n": 0, "p50": None, "p95": None, "max": None}

    def test_ignores_missing_readings(self):
        assert verifier.percentiles([1.0, None, 3.0])["n"] == 2

    def test_p95_is_a_real_reading_not_an_interpolation(self):
        out = verifier.percentiles([float(i) for i in range(1, 101)])
        assert out["p50"] == 50.5
        assert out["p95"] == 95.0
        assert out["max"] == 100.0


class TestVerification:
    def test_every_member_verifies_while_sources_still_exist(self, store):
        keys, _ = _seed_packed(store, 6)

        result = _run()

        assert result["sampled"] == 6
        assert result["verified"] == 6
        assert result["failed"] == 0
        assert result["sources_already_deleted"] == 0
        assert result["failures"] == []
        assert result["latency_ms"]["pack_cold"]["n"] == 6
        assert result["latency_ms"]["object"]["n"] == 6
        # Every source object is still exactly where it was: this script reads.
        assert all(key in store.objects for key in keys)

    def test_a_deleted_source_is_counted_and_still_verified(self, store):
        keys, _ = _seed_packed(store, 4)
        store.objects.pop(keys[0])

        result = _run()

        assert result["verified"] == 4
        assert result["failed"] == 0
        assert result["sources_already_deleted"] == 1
        assert result["latency_ms"]["object"]["n"] == 3

    def test_object_and_pack_disagreement_fails_loudly(self, store):
        keys, _ = _seed_packed(store, 4)
        # An object whose bytes are not what the pack holds. Only a comparison
        # of the two reads can catch this — both halves hash consistently with
        # themselves.
        store.objects[keys[1]] = compress_frame(b"<html>different</html>", level=1)

        result = _run()

        assert result["failed"] == 1
        assert result["verified"] == 3
        failure = result["failures"][0]
        assert failure["source_key"] == keys[1]
        assert "disagree" in failure["error"]

    def test_a_member_missing_from_every_index_fails(self, store, mocker):
        _seed_packed(store, 3)
        records = verifier.sample_members(
            store, _BUCKET,
            [index_key(pack_key(_TYPE, _YEAR, _MONTH, 0))], 100, 1,
        )
        for record in records:
            record["source_key"] = _source_key(99)
        mocker.patch.object(verifier, "sample_members", return_value=records)

        result = _run()

        assert result["verified"] == 0
        assert result["failed"] == 3
        assert all("not found in any pack index" in f["error"] for f in result["failures"])

    def test_sidecar_hash_mismatch_fails_rather_than_returning_bytes(self, store):
        from dataclasses import replace

        keys, pack = _seed_packed(store, 4)
        tampered = [
            replace(e, raw_sha256="0" * 64) if e.source_key == keys[2] else e
            for e in pack.entries
        ]
        store.objects[index_key(pack_key(_TYPE, _YEAR, _MONTH, 0))] = write_index_parquet(
            tampered
        )

        result = _run()

        assert result["failed"] == 1
        assert result["verified"] == 3
        assert result["failures"][0]["source_key"] == keys[2]

    def test_no_packs_reports_nothing_verified(self, store):
        result = _run()
        assert result["sidecars"] == 0
        assert result["verified"] == 0
        assert result["sampled"] == 0

    def test_warm_reads_are_measured_separately(self, store):
        _seed_packed(store, 6)

        result = _run(warm_reads=3)

        assert result["latency_ms"]["pack_warm"]["n"] == 3
        assert result["failed"] == 0

    def test_sampling_is_deterministic_for_a_seed(self, store):
        _seed_packed(store, 8)
        sidecars = [index_key(pack_key(_TYPE, _YEAR, _MONTH, 0))]

        first = verifier.sample_members(store, _BUCKET, sidecars, 3, seed=7)
        second = verifier.sample_members(store, _BUCKET, sidecars, 3, seed=7)
        other = verifier.sample_members(store, _BUCKET, sidecars, 3, seed=8)

        assert [r["source_key"] for r in first] == [r["source_key"] for r in second]
        assert len(first) == 3
        assert {r["source_key"] for r in first} != {r["source_key"] for r in other}
