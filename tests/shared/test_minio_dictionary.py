"""Plan 129 dictionary compression and registry tests."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
import zstandard as zstd


def _trained_dictionary() -> zstd.ZstdCompressionDict:
    samples = [
        (
            f"<html><head>common shell {i % 7}</head><body>listing={i} "
            + "vehicle details and shared navigation " * 20
            + "</body></html>"
        ).encode()
        for i in range(100)
    ]
    return zstd.train_dictionary(4096, samples)


@pytest.fixture
def dictionary(mocker):
    import shared.compression as compression

    trained = _trained_dictionary()
    compression.clear_dictionary_cache()

    def load(dict_id):
        return compression.RegisteredDictionary(
            dict_id=dict_id,
            raw=trained.as_bytes(),
            data=zstd.ZstdCompressionDict(trained.as_bytes()),
            source="test",
        )

    mocker.patch.object(compression, "_load_registered", load)
    yield trained
    compression.clear_dictionary_cache()


def _fake_db(mocker, row):
    """Patch shared.db.db_cursor to yield a cursor returning *row* from fetchone."""
    cursor = MagicMock()
    cursor.fetchone.return_value = row

    @contextmanager
    def db_cursor(*_args, **_kwargs):
        yield cursor

    mocker.patch("shared.db.db_cursor", db_cursor)
    return cursor


def test_dictionary_round_trip(dictionary):
    from shared.compression import compress_frame, decompress_frame

    original = b"<html>shared navigation vehicle details</html>" * 20
    frame = compress_frame(original, level=9, dict_id=dictionary.dict_id())
    assert zstd.get_frame_parameters(frame).dict_id == dictionary.dict_id()
    assert decompress_frame(frame) == original


@pytest.mark.parametrize("level", [3, 9])
def test_legacy_frames_still_decode(level):
    from shared.compression import decompress_frame

    original = b"legacy frame" * 100
    frame = zstd.ZstdCompressor(level=level).compress(original)
    assert zstd.get_frame_parameters(frame).dict_id == 0
    assert decompress_frame(frame) == original


def test_unknown_dictionary_id_raises_dedicated_error(dictionary, mocker):
    import shared.compression as compression

    frame = zstd.ZstdCompressor(level=9, dict_data=dictionary).compress(b"payload" * 100)
    compression.clear_dictionary_cache()

    def missing(dict_id):
        raise compression.UnknownDictionaryError(dict_id)

    mocker.patch.object(compression, "_load_registered", missing)
    with pytest.raises(compression.UnknownDictionaryError) as exc_info:
        compression.decompress_frame(frame)
    assert exc_info.value.dict_id == dictionary.dict_id()


def test_registry_cache_does_not_refetch(dictionary, mocker):
    import shared.compression as compression

    calls = 0

    def fetch(dict_id):
        nonlocal calls
        calls += 1
        return compression.RegisteredDictionary(
            dict_id=dict_id,
            raw=dictionary.as_bytes(),
            data=zstd.ZstdCompressionDict(dictionary.as_bytes()),
            source="test",
        )

    compression.clear_dictionary_cache()
    mocker.patch.object(compression, "_load_registered", fetch)
    first = compression.get_dictionary(dictionary.dict_id())
    second = compression.get_dictionary(dictionary.dict_id())
    assert first is second
    assert calls == 1


def test_read_html_uses_frame_id_not_object_metadata(dictionary, mocker):
    import shared.minio as minio

    original = b"dictionary frame" * 100
    frame = zstd.ZstdCompressor(level=9, dict_data=dictionary).compress(original)
    body = MagicMock()
    body.read.return_value = frame
    client = MagicMock()
    client.get_object.return_value = {
        "Body": body,
        "Metadata": {"dictionary-id": "999999"},
    }
    mocker.patch.object(minio, "get_boto3_client", return_value=client)
    assert minio.read_html("html/test.html.zst") == original


def test_write_flag_unset_matches_plain_zstd(monkeypatch):
    from shared.compression import compress_frame, configured_dictionary_id

    monkeypatch.delenv("HTML_COMPRESSION_DICT_ID", raising=False)
    content = b"plain frame" * 100
    actual = compress_frame(content, level=9, dict_id=configured_dictionary_id())
    expected = zstd.ZstdCompressor(level=9).compress(content)
    assert actual == expected


def test_configured_dictionary_id_validation(monkeypatch):
    from shared.compression import CompressionDictionaryError, configured_dictionary_id

    monkeypatch.setenv("HTML_COMPRESSION_DICT_ID", "not-an-id")
    with pytest.raises(CompressionDictionaryError):
        configured_dictionary_id()


def test_backfill_reads_legacy_and_writes_dictionary_frame(dictionary):
    from scripts.recompress_bronze_html import ObjectInfo, Summary, process_object

    original = b"<html>vehicle details and shared navigation</html>" * 100
    old_frame = zstd.ZstdCompressor(level=3).compress(original)
    body = MagicMock()
    body.read.return_value = old_frame
    client = MagicMock()
    client.get_object.return_value = {"Body": body}
    summary = Summary()

    process_object(
        client,
        "bronze",
        ObjectInfo(key="html/test.html.zst", size=len(old_frame)),
        apply=True,
        force=True,
        checkpoint_keys=set(),
        checkpoint_path=None,
        summary=summary,
        dictionary_id=dictionary.dict_id(),
    )

    output = client.put_object.call_args.kwargs["Body"]
    assert zstd.get_frame_parameters(output).dict_id == dictionary.dict_id()
    assert zstd.ZstdDecompressor(dict_data=dictionary).decompress(output) == original
    assert summary.recompressed == 1


# ── Recovery: the Postgres copy must cover the case it exists for ─────────────

_ROW_PATH = "s3://bronze/dictionaries/zstd/v1.dict"


def _row(dictionary_bytes):
    return {"minio_path": _ROW_PATH, "dictionary_bytes": dictionary_bytes}


@pytest.fixture(autouse=True)
def _clean_registry_cache():
    import shared.compression as compression

    compression.clear_dictionary_cache()
    yield
    compression.clear_dictionary_cache()


@pytest.mark.parametrize(
    "minio_bytes, label",
    [(b"", "zero-length"), (b"not-a-dictionary-at-all", "corrupt")],
    ids=["zero_length", "corrupt"],
)
def test_unusable_minio_copy_falls_back_to_postgres(minio_bytes, label, mocker):
    """The failure the second store exists for: MinIO *reads fine* but is useless.

    An earlier version fell back only when the read raised, so a truncated or
    zero-length object -- which reads back perfectly happily -- bricked every
    dictionary frame while the intact recovery copy sat untouched.
    """
    import shared.compression as compression

    trained = _trained_dictionary()
    _fake_db(mocker, _row(trained.as_bytes()))

    mocker.patch("shared.minio.read_bytes", return_value=minio_bytes)

    registered = compression.get_dictionary(trained.dict_id())

    assert registered.source == "postgres", label
    assert registered.dict_id == trained.dict_id()


def test_minio_read_failure_falls_back_to_postgres(mocker):
    import shared.compression as compression

    trained = _trained_dictionary()
    _fake_db(mocker, _row(trained.as_bytes()))

    mocker.patch("shared.minio.read_bytes", side_effect=OSError("connection refused"))

    registered = compression.get_dictionary(trained.dict_id())

    assert registered.source == "postgres"


def test_healthy_minio_copy_is_preferred_and_postgres_untouched(mocker):
    import shared.compression as compression

    trained = _trained_dictionary()
    _fake_db(mocker, _row(b"corrupt-recovery-copy"))

    mocker.patch("shared.minio.read_bytes", return_value=trained.as_bytes())

    registered = compression.get_dictionary(trained.dict_id())

    assert registered.source == "minio"


def test_both_copies_unusable_raises_and_names_both_attempts(mocker):
    """The operator reading this error is mid-incident; it must say what it tried."""
    import shared.compression as compression

    trained = _trained_dictionary()
    _fake_db(mocker, _row(b"also-corrupt"))

    mocker.patch("shared.minio.read_bytes", return_value=b"corrupt")

    with pytest.raises(compression.DictionaryMismatchError) as exc_info:
        compression.get_dictionary(trained.dict_id())

    message = str(exc_info.value)
    assert _ROW_PATH in message
    assert "Postgres dictionary_bytes" in message


def test_bytes_carrying_a_different_dictionary_id_are_rejected(mocker):
    """Right shape, wrong dictionary -- silently accepting it corrupts every read."""
    import shared.compression as compression

    trained = _trained_dictionary()
    other = zstd.train_dictionary(
        4096, [f"unrelated corpus {i} ".encode() * 50 for i in range(100)]
    )
    assert other.dict_id() != trained.dict_id()

    _fake_db(mocker, _row(other.as_bytes()))

    mocker.patch("shared.minio.read_bytes", return_value=other.as_bytes())

    with pytest.raises(compression.DictionaryMismatchError):
        compression.get_dictionary(trained.dict_id())


def test_missing_row_raises_unknown_not_mismatch(mocker):
    import shared.compression as compression

    _fake_db(mocker, None)

    with pytest.raises(compression.UnknownDictionaryError):
        compression.get_dictionary(4242)


# ── Negative caching: a bad ID must not re-query per frame ────────────────────

def test_unresolvable_id_is_not_re_queried_per_call(mocker):
    """Finding 7: without this a corpus scan opens a DB connection per object."""
    import shared.compression as compression

    calls = 0

    @contextmanager
    def counting_cursor(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        yield cursor

    mocker.patch("shared.db.db_cursor", counting_cursor)

    for _ in range(5):
        with pytest.raises(compression.UnknownDictionaryError):
            compression.get_dictionary(4242)

    assert calls == 1


def test_transient_failures_are_not_negatively_cached(mocker):
    """A database blip must not become a process-lifetime outage."""
    import shared.compression as compression

    trained = _trained_dictionary()
    boom = MagicMock(side_effect=RuntimeError("connection pool exhausted"))
    mocker.patch("shared.db.db_cursor", boom)

    with pytest.raises(RuntimeError):
        compression.get_dictionary(trained.dict_id())

    # Infrastructure recovered: the next call must try again, not replay the error.
    _fake_db(mocker, _row(trained.as_bytes()))

    mocker.patch("shared.minio.read_bytes", return_value=trained.as_bytes())

    assert compression.get_dictionary(trained.dict_id()).source == "minio"


def test_clear_cache_resets_a_negative_entry(mocker):
    import shared.compression as compression

    trained = _trained_dictionary()
    _fake_db(mocker, None)

    with pytest.raises(compression.UnknownDictionaryError):
        compression.get_dictionary(trained.dict_id())

    compression.clear_dictionary_cache()
    _fake_db(mocker, _row(trained.as_bytes()))

    mocker.patch("shared.minio.read_bytes", return_value=trained.as_bytes())

    assert compression.get_dictionary(trained.dict_id()).dict_id == trained.dict_id()


# ── Precomputed compression form ──────────────────────────────────────────────

def test_precomputed_form_is_built_once_per_level_and_round_trips(mocker):
    """Precompute is a 65x speedup on a 768 KB dictionary; it must stay correct."""
    import shared.compression as compression

    trained = _trained_dictionary()
    _fake_db(mocker, _row(trained.as_bytes()))

    mocker.patch("shared.minio.read_bytes", return_value=trained.as_bytes())

    registered = compression.get_dictionary(trained.dict_id())

    first = registered.compress_form(9)
    assert registered.compress_form(9) is first
    assert registered.compress_form(3) is not first

    original = b"<html>shared navigation vehicle details</html>" * 30
    frame = compression.compress_frame(original, level=9, dict_id=trained.dict_id())
    assert zstd.get_frame_parameters(frame).dict_id == trained.dict_id()
    assert compression.decompress_frame(frame) == original
