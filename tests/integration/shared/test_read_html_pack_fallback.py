"""Plan 131 Stage 3 against real MinIO: packed and unpacked both readable.

This is the end-to-end version of the property Stage 4 will delete source
objects on the strength of. Artifacts are written through the production
``write_html``, packed by the production packer, and then their source objects
are removed — after which ``read_html`` must return the same bytes it returned
before, for the same key, with no signature change and no caller aware of it.

Isolation is by **artifact_type**, not by a patched prefix. The pack fallback
derives its search prefix from the source key's own hive partition — that is
the mechanism under test — so a test that redirected ``html/`` somewhere else
would be testing a different resolver than the one production runs. A unique
artifact_type gives a throwaway namespace inside the real prefixes instead,
and both are deleted afterwards.
"""
import hashlib
import os
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import boto3
import pytest

import shared.minio as minio
from archiver.processors import pack_bronze_html as packer
from shared.minio import read_html, write_html

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
def artifact_type(s3_client, monkeypatch):
    """A unique artifact_type, and every object under it removed afterwards."""
    name = f"test_pack_read_{uuid.uuid4().hex[:8]}"
    monkeypatch.delenv("HTML_COMPRESSION_DICT_ID", raising=False)
    monkeypatch.delenv("PACK_BRONZE_DICT_ID", raising=False)
    minio.clear_pack_caches()

    yield name

    minio.clear_pack_caches()
    paginator = s3_client.get_paginator("list_objects_v2")
    for prefix in (
        f"html/year={_YEAR}/month={_MONTH}/artifact_type={name}/",
        f"html_packs/{name}/",
    ):
        for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                s3_client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


def _page(i: int) -> bytes:
    return (
        "<html><head><title>2024 Ford F-150</title></head><body>"
        + "<div class='spec'>Engine 3.5L V6 EcoBoost</div>" * 60
        + f"<span id='price'>${30000 + i * 25}</span>"
        + f"<span id='vin'>1FTFW1E5{i:03d}NFA00000</span>"
        + "</body></html>"
    ).encode("utf-8")


def _seed(artifact_type: str, n: int) -> list[tuple[str, bytes]]:
    seeded = []
    for i in range(n):
        key = (
            f"html/year={_YEAR}/month={_MONTH}/artifact_type={artifact_type}/"
            f"{uuid.uuid4()}.html.zst"
        )
        content = _page(i)
        write_html(key, content)
        seeded.append((key, content))
    return seeded


def _pack(mocker, artifact_type: str, seeded, **kwargs):
    """Run the production packer over exactly the seeded artifacts."""
    def _fetch(con, bucket, a_type, year, month):
        # (source_key, artifact_id, listing_id, cluster_key, fetched_at) —
        # identity and cluster key hold the same value here, the ordinary case.
        for i, (key, _) in enumerate(seeded):
            listing = f"L{i // 3:03d}"
            yield (
                key,
                1000 + i,
                listing,
                listing,
                datetime(year, month, 1 + (i % 27), 12, tzinfo=timezone.utc),
            )

    mocker.patch.object(packer, "fetch_member_metadata", _fetch)
    mocker.patch(
        "shared.duckdb_s3.get_duckdb_s3_connection", MagicMock(return_value=MagicMock())
    )
    params = {
        "apply": True,
        "artifact_type": artifact_type,
        "year": _YEAR,
        "month": _MONTH,
        "allow_no_dictionary": True,
        "min_free_bytes": 0,
    }
    params.update(kwargs)
    result = packer.pack_bronze_html(**params)
    assert result["error"] is None, result["error"]
    return result


def _delete(s3_client, keys):
    for key in keys:
        s3_client.delete_object(Bucket=_BUCKET, Key=key)


def test_packed_artifacts_read_identically_before_and_after_their_sources_go(
    artifact_type, s3_client, mocker
):
    seeded = _seed(artifact_type, 12)
    before = {key: read_html(key) for key, _ in seeded}
    assert before == dict(seeded)

    result = _pack(mocker, artifact_type, seeded, frame_target_bytes=4096)
    assert result["members_verified"] == 12

    # Still present: the object path must win, and the pack must not be touched.
    assert read_html(seeded[0][0]) == seeded[0][1]

    _delete(s3_client, [key for key, _ in seeded])
    minio.clear_pack_caches()

    for key, content in seeded:
        served = read_html(key)
        assert served == content
        assert hashlib.sha256(served).hexdigest() == hashlib.sha256(before[key]).hexdigest()


def test_unpacked_artifacts_in_the_same_month_still_read_from_their_objects(
    artifact_type, s3_client, mocker
):
    packed = _seed(artifact_type, 6)
    _pack(mocker, artifact_type, packed, frame_target_bytes=4096)

    later = _seed(artifact_type, 3)          # arrived after the pack was written
    _delete(s3_client, [key for key, _ in packed])
    minio.clear_pack_caches()

    for key, content in packed:
        assert read_html(key) == content     # from the pack
    for key, content in later:
        assert read_html(key) == content     # from the object


def test_a_key_in_neither_place_raises_the_same_404(artifact_type, mocker):
    from botocore.exceptions import ClientError

    seeded = _seed(artifact_type, 3)
    _pack(mocker, artifact_type, seeded, frame_target_bytes=4096)

    missing = (
        f"html/year={_YEAR}/month={_MONTH}/artifact_type={artifact_type}/"
        f"{uuid.uuid4()}.html.zst"
    )
    with pytest.raises(ClientError) as exc_info:
        read_html(missing)
    assert exc_info.value.response["Error"]["Code"] in ("NoSuchKey", "404")


def test_dictionary_compressed_objects_and_packed_members_are_both_readable(
    artifact_type, s3_client, mocker, monkeypatch
):
    """Both frame types, through one code path.

    The environment supplies the dictionary if it has one; where it does not,
    the no-dictionary half is still a real assertion and the dictionary half is
    covered by the unit tests, which can register one without Postgres.
    """
    dict_id = os.environ.get("INTEGRATION_HTML_DICT_ID", "").strip()
    seeded = _seed(artifact_type, 4)
    _pack(mocker, artifact_type, seeded, frame_target_bytes=4096)
    _delete(s3_client, [key for key, _ in seeded])
    minio.clear_pack_caches()

    for key, content in seeded:
        assert read_html(key) == content

    if not dict_id:
        pytest.skip("INTEGRATION_HTML_DICT_ID not set — no registered dictionary here")

    monkeypatch.setenv("HTML_COMPRESSION_DICT_ID", dict_id)
    key = (
        f"html/year={_YEAR}/month={_MONTH}/artifact_type={artifact_type}/"
        f"{uuid.uuid4()}.html.zst"
    )
    content = _page(99)
    write_html(key, content)
    assert read_html(key) == content
