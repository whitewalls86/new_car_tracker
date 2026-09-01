"""Unit tests for scripts/train_html_dictionary.py (Plan 129, Stage 1).

This script registers the artifact whose loss makes every dictionary-compressed
object permanently unreadable, so the tests concentrate on the two ways it can
do lasting damage — losing a long training run to one bad object, and breaking
the immutability of an already-registered dictionary — rather than on the
training itself, which is zstd's job.

  A - sample fetching survives individual unreadable objects, but not a flood
  B - registration is immutable: never overwrite, never double-register
  C - provenance is recorded and reproducible
  D - main() exit codes
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from scripts.train_html_dictionary import fetch_samples, main, register_dictionary


def _rows(n: int, prefix: str = "s3://bronze/html/") -> list[dict]:
    return [{"minio_path": f"{prefix}{i}.html.zst"} for i in range(n)]


def _fake_db(mocker, existing_row=None, insert_error=None):
    """Patch shared.db.db_cursor; SELECT returns *existing_row*, INSERT may raise."""
    cursor = MagicMock()
    cursor.fetchone.return_value = existing_row
    if insert_error is not None:
        cursor.execute.side_effect = [None, insert_error]

    @contextmanager
    def db_cursor(*_args, **_kwargs):
        yield cursor

    mocker.patch("shared.db.db_cursor", db_cursor)
    return cursor


# ── A. Sample fetching ────────────────────────────────────────────────────────

def test_one_unreadable_object_does_not_kill_the_run(mocker):
    """A 10,000-object training run must not die on a single deleted artifact."""
    rows = _rows(20)

    def read(path):
        # Anchored on the separator: "7.html.zst" alone also matches 17.
        if path.endswith("/7.html.zst"):
            raise OSError("NoSuchKey")
        return b"<html>page</html>"

    mocker.patch("shared.minio.read_html", side_effect=read)

    samples, keys = fetch_samples(rows, progress_every=0)

    assert len(samples) == 19
    assert len(keys) == 19
    assert not any(k.endswith("/7.html.zst") for k in keys)


def test_keys_line_up_with_samples_after_a_skip(mocker):
    """sample_keys is the provenance record; a misaligned key list is a lie."""
    rows = _rows(5)

    def read(path):
        if path.endswith("/2.html.zst"):
            raise OSError("gone")
        return path.encode()

    mocker.patch("shared.minio.read_html", side_effect=read)

    samples, keys = fetch_samples(rows, progress_every=0)

    assert [s.decode() for s in samples] == keys


def test_widespread_read_failure_aborts_rather_than_training_on_survivors(mocker):
    """A dictionary trained on whatever happened to survive is not auditable."""
    rows = _rows(100)

    mocker.patch("shared.minio.read_html", side_effect=OSError("store unreachable"))

    with pytest.raises(RuntimeError, match="aborting"):
        fetch_samples(rows, progress_every=0)


def test_a_few_failures_below_the_threshold_are_tolerated(mocker):
    rows = _rows(500)

    def read(path):
        if path.endswith(("/13.html.zst", "/207.html.zst")):
            raise OSError("gone")
        return b"<html>page</html>"

    mocker.patch("shared.minio.read_html", side_effect=read)

    samples, _ = fetch_samples(rows, progress_every=0)

    # 2 failures in 500 is 0.4% -- well under the 5% abort threshold.
    assert len(samples) == 498


def test_rows_without_a_path_are_skipped_not_fetched(mocker):
    rows = [{"minio_path": None}, {"minio_path": "s3://bronze/a.zst"}, {}]

    read = mocker.patch("shared.minio.read_html", return_value=b"x")

    samples, keys = fetch_samples(rows, progress_every=0)

    assert len(samples) == 1
    assert read.call_count == 1
    assert keys == ["s3://bronze/a.zst"]


# ── B. Registration immutability ──────────────────────────────────────────────

def _register(**overrides):
    kwargs = dict(
        dict_id=123,
        version=1,
        zstd_level=9,
        parameters={"dict_size_kb": 768},
        sample_keys=["s3://bronze/a.zst"],
        sample_sha256="abc123",
    )
    kwargs.update(overrides)
    return register_dictionary(b"dictionary-bytes", **kwargs)


def test_already_registered_id_refuses_before_touching_minio(mocker):
    """Order matters: the DB check must gate the immutable object write."""
    client = MagicMock()
    _fake_db(mocker, existing_row={"dict_id": 123, "version": 1})

    mocker.patch("shared.minio.get_boto3_client", return_value=client)

    with pytest.raises(RuntimeError, match="already registered"):
        _register()

    client.put_object.assert_not_called()
    client.get_object.assert_not_called()


def test_existing_object_with_different_bytes_is_never_overwritten(mocker):
    """Overwriting would silently corrupt every frame already written against it."""
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = b"different-bytes"
    client.get_object.return_value = {"Body": body}

    _fake_db(mocker, existing_row=None)

    mocker.patch("shared.minio.get_boto3_client", return_value=client)

    with pytest.raises(RuntimeError, match="refusing to overwrite"):
        _register()

    client.put_object.assert_not_called()


def test_identical_orphaned_object_is_reused(mocker):
    """A crashed prior run leaves the object without a row; that is recoverable."""
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = b"dictionary-bytes"
    client.get_object.return_value = {"Body": body}

    _fake_db(mocker, existing_row=None)

    mocker.patch("shared.minio.get_boto3_client", return_value=client)

    path = _register()

    assert path.endswith("dictionaries/zstd/v1.dict")
    client.put_object.assert_not_called()


def test_missing_object_is_uploaded_with_if_none_match(mocker):
    """IfNoneMatch is what makes the upload itself refuse to clobber."""
    client = MagicMock()
    error = Exception("missing")
    error.response = {"Error": {"Code": "NoSuchKey"}}
    client.get_object.side_effect = error

    _fake_db(mocker, existing_row=None)

    mocker.patch("shared.minio.get_boto3_client", return_value=client)

    _register()

    assert client.put_object.call_args.kwargs["IfNoneMatch"] == "*"


def test_an_unexpected_head_error_is_not_treated_as_absent(mocker):
    """A permissions error must not be read as 'the object is not there'."""
    client = MagicMock()
    error = Exception("denied")
    error.response = {"Error": {"Code": "AccessDenied"}}
    client.get_object.side_effect = error

    _fake_db(mocker, existing_row=None)

    mocker.patch("shared.minio.get_boto3_client", return_value=client)

    with pytest.raises(RuntimeError, match="could not check immutable"):
        _register()

    client.put_object.assert_not_called()


# ── C. Provenance ─────────────────────────────────────────────────────────────

def test_registration_records_sample_keys_and_parameters(mocker):
    client = MagicMock()
    error = Exception("missing")
    error.response = {"Error": {"Code": "NoSuchKey"}}
    client.get_object.side_effect = error

    cursor = _fake_db(mocker, existing_row=None)

    mocker.patch("shared.minio.get_boto3_client", return_value=client)

    _register(sample_keys=["s3://bronze/a.zst", "s3://bronze/b.zst"])

    insert_params = cursor.execute.call_args.args[1]
    assert json.loads(insert_params[7]) == ["s3://bronze/a.zst", "s3://bronze/b.zst"]
    assert json.loads(insert_params[6]) == {"dict_size_kb": 768}


# ── D. main() exit codes ──────────────────────────────────────────────────────

def test_no_samples_exits_two_without_registering(mocker):
    mocker.patch("scripts.train_html_dictionary.load_rows", return_value=[])

    register = mocker.patch("scripts.train_html_dictionary.register_dictionary")

    code = main(["--sample-in", "/nonexistent"] if False else [])

    assert code == 2
    register.assert_not_called()


def test_dry_run_trains_but_does_not_register(tmp_path, capsys, mocker):
    rows = _rows(200)
    page = b"<html><head>shared shell</head><body>listing data here</body></html>" * 4

    mocker.patch("scripts.train_html_dictionary.load_rows", return_value=rows)
    mocker.patch("shared.minio.read_html", return_value=page)
    register = mocker.patch("scripts.train_html_dictionary.register_dictionary")

    code = main(["--dry-run", "--dict-size-kb", "1", "--progress-every", "0"])

    assert code == 0
    register.assert_not_called()
    report = json.loads(capsys.readouterr().out)
    assert report["dry_run"] is True
    assert report["minio_path"] is None
    assert report["samples"] == 200
    assert report["dict_id"] > 0


def test_untrainable_sample_exits_two(mocker):
    mocker.patch("scripts.train_html_dictionary.load_rows", return_value=_rows(2))
    mocker.patch("shared.minio.read_html", return_value=b"x")
    register = mocker.patch("scripts.train_html_dictionary.register_dictionary")

    code = main(["--dry-run", "--progress-every", "0"])

    assert code == 2
    register.assert_not_called()
