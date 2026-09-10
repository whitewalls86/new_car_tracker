"""The S3 error codes ``shared/minio.py`` names, asked of a real store.

Plan 162 Stage AB / CAR-106.

``shared/minio.py`` decides an object is absent by reading
``ClientError.response["Error"]["Code"]`` and testing it against a tuple of
four strings. Those strings belong to the S3 API and to whatever
implementation is answering -- here MinIO, which CI runs from
``docker-compose.yml`` rather than from a ``services:`` block, so the store
this test asks is configured the way production's is (Plan 162 Stage Q).

**Why this is worth a test rather than a comment.** The set going stale is
not the loud kind of wrong. ``object_exists`` returns ``False`` on a code it
recognises and re-raises on one it does not, so a renamed code turns a
routine absence into an exception on a path whose whole job is to answer
"is it there?" -- and the callers that treat absence as ordinary are the pack
fallback and the cleanup sweep, both of which run unattended.

This is the cheapest true replay in the census: the corpus is four strings
and the real thing is already running in the job.
"""
from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).parents[3]
CENSUS_PATH = REPO_ROOT / "tests" / "external_vocabulary_census.py"

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

_WHAT = "the ClientError codes that mean 'no such object or bucket'"


def _declared() -> tuple:
    """The codes the census declares, loaded by path for the census's reason."""
    spec = importlib.util.spec_from_file_location("external_vocabulary_census", CENSUS_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    rows = [row for row in module.CENSUS if row["what"] == _WHAT]
    assert len(rows) == 1, f"{CENSUS_PATH} declares {len(rows)} rows for {_WHAT!r}"
    return rows[0]["members"]


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


def test_a_missing_object_still_reports_a_code_this_repository_knows(s3_client):
    """A key that does not exist comes back with a code the tuple contains."""
    key = f"plan162-stage-ab/{uuid.uuid4()}.nothing"

    with pytest.raises(ClientError) as raised:
        s3_client.head_object(Bucket=_BUCKET, Key=key)

    code = raised.value.response["Error"]["Code"]
    declared = _declared()
    assert code in declared, (
        f"MinIO reports a missing object as {code!r} and shared/minio.py only "
        f"recognises {declared}. object_exists() would re-raise instead of "
        "returning False, and the pack fallback and the cleanup sweep both "
        "treat absence as ordinary. Add the code to shared/minio.py and to the "
        "census row together."
    )


def test_a_missing_bucket_still_reports_a_code_this_repository_knows(s3_client):
    """And the bucket case, which ``ensure_bucket`` reads separately."""
    bucket = f"plan162-stage-ab-{uuid.uuid4().hex[:12]}"

    with pytest.raises(ClientError) as raised:
        s3_client.head_bucket(Bucket=bucket)

    code = raised.value.response["Error"]["Code"]
    declared = _declared()
    assert code in declared, (
        f"MinIO reports a missing bucket as {code!r} and shared/minio.py only "
        f"recognises {declared}."
    )


def test_the_declared_codes_are_the_ones_shared_minio_holds():
    """The other end of the pin: the census and the module name the same set.

    Neither rule works alone. The two above ask MinIO whether the census is
    still right; this asks whether the census still describes the code. A
    census that had drifted away from ``shared/minio.py`` would pass both of
    them while the module carried a set nothing checked.
    """
    from shared.minio import _NOT_FOUND_CODES

    declared = set(_declared())
    held = set(_NOT_FOUND_CODES)
    # `ensure_bucket` adds the bucket-level code to the object-level tuple, so
    # the census is the union and `_NOT_FOUND_CODES` is a subset of it rather
    # than equal to it.
    assert held <= declared, (
        "shared/minio.py recognises codes the census does not declare: "
        f"{sorted(held - declared)}"
    )
    assert "NoSuchBucket" in declared, (
        "the census dropped NoSuchBucket, which shared/minio.py's ensure_bucket "
        "reads at line 135 outside _NOT_FOUND_CODES."
    )
