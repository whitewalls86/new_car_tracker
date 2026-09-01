"""Integration tests for pack_bronze_html (Plan 131 Stage 2).

Seeds real HTML objects into MinIO through the production write path, packs
them, and reads every member back out of the *stored* pack over real ranged
GETs. Requires real MinIO (MINIO_ENDPOINT must be set).

Each test patches ``_HTML_PREFIX``/``_PACK_PREFIX`` to a unique throwaway
namespace — the same isolation compact_silver's integration tests use — so a
run can neither touch production objects nor be displaced by them.

The silver-side ordering query is not exercised here: it is covered against a
real DuckDB over local Parquet in ``tests/archiver/test_pack_bronze_html.py``.
Seeding production-shaped silver *and* artifact-queue events just to re-derive
an ordering the test already knows would test the fixture, not the packer.
"""
import hashlib
import os
import uuid
from datetime import datetime, timezone

import boto3
import pytest

from archiver.processors import pack_bronze_html as packer
from shared.minio import read_html, write_html
from shared.packfile import PackReader, read_index_parquet

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT"),
        reason="MINIO_ENDPOINT not set — MinIO not available",
    ),
]

_BUCKET = os.environ.get("MINIO_BUCKET", "bronze")
_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "")
_ACCESS = os.environ.get("MINIO_ROOT_USER", "cartracker")
_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "")

_YEAR, _MONTH = 2026, 5
_TYPE = "detail_page"


@pytest.fixture(scope="module")
def s3_client():
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=_ENDPOINT,
        aws_access_key_id=_ACCESS,
        aws_secret_access_key=_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@pytest.fixture()
def ns(s3_client, monkeypatch, mocker):
    """A unique html/ + html_packs/ namespace, removed after the test."""
    run_id = uuid.uuid4().hex[:8]
    root = f"test-pack-{run_id}"
    mocker.patch.object(packer, "_HTML_PREFIX", f"{root}/html")
    mocker.patch.object(packer, "_PACK_PREFIX", f"{root}/html_packs")
    # Seeded objects are written with whatever dictionary the environment names,
    # and this environment has no registered dictionary to resolve.
    monkeypatch.delenv("HTML_COMPRESSION_DICT_ID", raising=False)
    monkeypatch.delenv("PACK_BRONZE_DICT_ID", raising=False)

    yield root

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=root):
        for obj in page.get("Contents", []):
            s3_client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


def _page(i: int) -> bytes:
    """A page with real within-listing repetition, so a frame has work to do."""
    return (
        "<html><head><title>2024 Ford F-150</title></head><body>"
        + "<div class='spec'>Engine 3.5L V6 EcoBoost</div>" * 60
        + f"<span id='price'>${30000 + i * 25}</span>"
        + f"<span id='vin'>1FTFW1E5{i:03d}NFA00000</span>"
        + "</body></html>"
    ).encode("utf-8")


def _seed(root: str, n: int) -> list[tuple[str, bytes]]:
    """Write *n* artifacts through the production write path."""
    seeded = []
    for i in range(n):
        key = (
            f"{root}/html/year={_YEAR}/month={_MONTH}/artifact_type={_TYPE}/"
            f"{uuid.uuid4()}.html.zst"
        )
        content = _page(i)
        write_html(key, content)
        seeded.append((key, content))
    return seeded


def _metadata_for(seeded):
    """Stand in for the silver query, in cluster order.

    Identity and cluster key are the same value here, which is the ordinary
    case. They are separate columns because the bronze packer's are not always
    the same thing — see Plan 145 Stage 5b — and the unit suite covers where
    they diverge.
    """
    def _fetch(con, bucket, artifact_type, year, month):
        for i, (key, _) in enumerate(seeded):
            listing = f"L{i // 3:03d}"
            yield (
                key,
                1000 + i,
                listing,
                listing,
                datetime(year, month, 1 + (i % 27), 12, tzinfo=timezone.utc),
            )
    return _fetch


def _run(mocker, seeded, **kwargs):
    from unittest.mock import MagicMock

    mocker.patch.object(packer, "fetch_member_metadata", _metadata_for(seeded))
    # The stubbed query ignores its connection, so there is nothing to gain
    # from opening a real one.
    mocker.patch(
        "shared.duckdb_s3.get_duckdb_s3_connection", MagicMock(return_value=MagicMock())
    )
    params = {
        "apply": True,
        "artifact_type": _TYPE,
        "year": _YEAR,
        "month": _MONTH,
        "allow_no_dictionary": True,
        "min_free_bytes": 0,
    }
    params.update(kwargs)
    return packer.pack_bronze_html(**params)


def _keys_under(s3_client, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]


# ---------------------------------------------------------------------------


def test_pack_round_trips_through_minio_and_deletes_nothing(ns, s3_client, mocker):
    seeded = _seed(ns, 12)

    result = _run(mocker, seeded)

    assert result["error"] is None
    assert result["packs_written"] == 1
    assert result["members_packed"] == 12
    assert result["members_verified"] == 12

    # Every source object is still exactly where it was.
    surviving = set(_keys_under(s3_client, f"{ns}/html/"))
    assert surviving == {key for key, _ in seeded}
    for key, content in seeded:
        assert read_html(key) == content

    pack = result["buckets"][0]["packs"][0]
    stored = s3_client.get_object(Bucket=_BUCKET, Key=pack["pack_key"])["Body"].read()
    entries = read_index_parquet(
        s3_client.get_object(Bucket=_BUCKET, Key=pack["index_key"])["Body"].read()
    )

    reader = PackReader.from_bytes(stored)
    reader.check_index(entries)
    by_key = dict(seeded)
    assert len(entries) == len(seeded)
    for entry in entries:
        extracted = reader.read_member(entry)
        assert extracted == by_key[entry.source_key]
        assert hashlib.sha256(extracted).hexdigest() == entry.raw_sha256


def test_pack_is_smaller_than_the_objects_it_packs(ns, s3_client, mocker):
    seeded = _seed(ns, 24)

    result = _run(mocker, seeded)
    bucket = result["buckets"][0]

    assert bucket["packs"][0]["pack_bytes"] < bucket["source_bytes"]
    # 24 objects became 2 (pack + sidecar). Object count is the constraint this
    # plan exists for, so it is asserted rather than assumed.
    assert len(_keys_under(s3_client, f"{ns}/html_packs/")) == 2


def test_resume_over_real_objects_packs_nothing_twice(ns, s3_client, mocker):
    seeded = _seed(ns, 8)
    _run(mocker, seeded)

    second = _run(mocker, seeded)

    assert second["packs_written"] == 0
    assert second["buckets"][0]["already_packed"] == 8
    assert second["buckets"][0]["pending"] == 0
    assert len(_keys_under(s3_client, f"{ns}/html_packs/")) == 2


def test_ranged_reads_fetch_one_frame_not_the_whole_pack(ns, s3_client, mocker):
    seeded = _seed(ns, 24)
    result = _run(mocker, seeded, frame_target_bytes=4096)
    pack = result["buckets"][0]["packs"][0]
    assert pack["frames"] > 1

    fetched = []

    def fetch(offset: int, length: int) -> bytes:
        fetched.append(length)
        return s3_client.get_object(
            Bucket=_BUCKET,
            Key=pack["pack_key"],
            Range=f"bytes={offset}-{offset + length - 1}",
        )["Body"].read()

    entries = read_index_parquet(
        s3_client.get_object(Bucket=_BUCKET, Key=pack["index_key"])["Body"].read()
    )
    last = max(entries, key=lambda e: (e.frame_ordinal, e.offset_in_frame))
    extracted = PackReader(fetch, pack["pack_bytes"]).read_member(last)

    assert extracted == dict(seeded)[last.source_key]
    assert sum(fetched) < pack["pack_bytes"]


def test_dry_run_against_real_objects_writes_nothing(ns, s3_client, mocker):
    seeded = _seed(ns, 6)

    result = _run(mocker, seeded, apply=False)

    assert result["packs_written"] == 0
    assert _keys_under(s3_client, f"{ns}/html_packs/") == []
    assert len(_keys_under(s3_client, f"{ns}/html/")) == len(seeded)
