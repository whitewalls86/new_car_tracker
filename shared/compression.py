"""Dictionary-aware zstd compression for bronze HTML frames."""
from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from typing import Any

from shared.queries import SELECT_COMPRESSION_DICTIONARY

logger = logging.getLogger(__name__)


class CompressionDictionaryError(RuntimeError):
    """Base error for dictionary registry failures."""


class UnknownDictionaryError(CompressionDictionaryError):
    """A frame references a dictionary that is not registered."""

    def __init__(self, dict_id: int):
        self.dict_id = dict_id
        super().__init__(f"zstd frame references unknown dictionary ID {dict_id}")


class DictionaryMismatchError(CompressionDictionaryError):
    """Registered bytes do not carry the expected zstd dictionary ID."""


@dataclass
class RegisteredDictionary:
    """An immutable registered dictionary, plus per-level compression forms.

    ``data`` is the dictionary as loaded, used directly for decompression.
    Compression needs more: building a compressor with a raw dictionary
    re-digests it into a CDict on *every* call, which for a ~768 KB dictionary
    measured 1.27 ms against 0.019 ms once precomputed -- a 65x difference, or
    ~1.4 hours of pure overhead across a 3.9M-object backfill. So a precomputed
    form is cached per level.

    ``precompute_compress`` mutates the dictionary object it is called on and
    binds it to one level, so each level gets its own instance built from the
    same bytes rather than sharing one.
    """

    dict_id: int
    raw: bytes
    data: Any
    source: str
    _compress_forms: dict[int, Any] = field(default_factory=dict, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def compress_form(self, level: int) -> Any:
        """Return the dictionary precomputed for compression at *level*."""
        import zstandard as zstd

        form = self._compress_forms.get(level)
        if form is not None:
            return form
        with self._lock:
            form = self._compress_forms.get(level)
            if form is None:
                form = zstd.ZstdCompressionDict(self.raw)
                form.precompute_compress(level=level)
                self._compress_forms[level] = form
            return form


_cache: dict[int, RegisteredDictionary] = {}
#: Deterministic resolution failures, cached so a bad ID cannot re-open a
#: Postgres connection and a MinIO GET for every frame in a corpus scan.
#: **Only** outcomes that cannot change without a registry write are stored
#: here -- a missing row or unusable bytes. Transient infrastructure errors
#: propagate uncached, because caching those would turn a momentary database
#: blip into a process-lifetime outage.
_negative_cache: dict[int, CompressionDictionaryError] = {}
_cache_lock = threading.Lock()


def configured_dictionary_id() -> int | None:
    """Return HTML_COMPRESSION_DICT_ID, or None when dictionary writes are disabled."""
    value = os.environ.get("HTML_COMPRESSION_DICT_ID")
    if value is None or not value.strip():
        return None
    try:
        dict_id = int(value)
    except ValueError as exc:
        raise CompressionDictionaryError(
            f"HTML_COMPRESSION_DICT_ID must be a positive integer, got {value!r}"
        ) from exc
    if dict_id <= 0:
        raise CompressionDictionaryError("HTML_COMPRESSION_DICT_ID must be positive")
    return dict_id


def _build_dictionary(raw: bytes | None, dict_id: int) -> Any | None:
    """Return a usable dictionary object for *dict_id*, or None if these bytes are not.

    Returns rather than raises: the caller has a second copy to try, and
    "these bytes are unusable" is a routine answer during recovery, not an
    error condition on its own.
    """
    import zstandard as zstd

    if not raw:
        return None
    try:
        data = zstd.ZstdCompressionDict(bytes(raw))
        if data.dict_id() != dict_id:
            return None
        return data
    except Exception:  # noqa: BLE001 - unusable bytes are a result; the caller has a fallback.
        return None


def _load_registered(dict_id: int) -> RegisteredDictionary:
    """Load a dictionary, preferring the MinIO working copy over the DB recovery copy.

    The Postgres ``dictionary_bytes`` column exists precisely so that losing
    the MinIO object does not make every frame written against this dictionary
    unreadable. It is therefore consulted whenever the MinIO copy fails to
    *produce a usable dictionary* -- not merely when reading it raises. A
    truncated or zero-length object reads back perfectly happily, and an
    earlier version of this function fell through to the recovery copy only on
    an exception, so exactly the corruption the second store exists for was the
    one case it could not repair.
    """
    from shared.db import db_cursor

    with db_cursor(error_context="Load compression dictionary", dict_cursor=True) as cur:
        cur.execute(SELECT_COMPRESSION_DICTIONARY, (dict_id,))
        row = cur.fetchone()
    if row is None:
        raise UnknownDictionaryError(dict_id)

    minio_path = row["minio_path"]
    attempts: list[str] = []

    raw: bytes | None = None
    try:
        from shared.minio import read_bytes

        raw = read_bytes(minio_path)
    except Exception as exc:  # noqa: BLE001 - the recovery copy is the whole point.
        attempts.append(f"MinIO {minio_path}: read failed ({exc})")
    else:
        data = _build_dictionary(raw, dict_id)
        if data is not None:
            return RegisteredDictionary(
                dict_id=dict_id, raw=bytes(raw), data=data, source="minio"
            )
        attempts.append(
            f"MinIO {minio_path}: {len(raw)} bytes did not yield dictionary ID {dict_id}"
        )

    recovery = row["dictionary_bytes"]
    data = _build_dictionary(recovery, dict_id)
    if data is not None:
        logger.warning(
            "Compression dictionary %d recovered from Postgres; the MinIO copy at %s is "
            "unusable and should be restored. Attempts: %s",
            dict_id,
            minio_path,
            "; ".join(attempts),
        )
        return RegisteredDictionary(
            dict_id=dict_id, raw=bytes(recovery), data=data, source="postgres"
        )

    attempts.append(
        "Postgres dictionary_bytes: "
        + (
            f"{len(recovery)} bytes did not yield dictionary ID {dict_id}"
            if recovery
            else "empty"
        )
    )
    raise DictionaryMismatchError(
        f"dictionary ID {dict_id} is registered but no stored copy is usable. "
        + "; ".join(attempts)
    )


def get_dictionary(dict_id: int) -> RegisteredDictionary:
    """Resolve and cache an immutable registered dictionary by zstd ID."""
    cached = _cache.get(dict_id)
    if cached is not None:
        return cached
    with _cache_lock:
        cached = _cache.get(dict_id)
        if cached is not None:
            return cached
        failure = _negative_cache.get(dict_id)
        if failure is not None:
            raise failure
        try:
            registered = _load_registered(dict_id)
        except (UnknownDictionaryError, DictionaryMismatchError) as exc:
            # Deterministic: cannot change until someone writes to the registry,
            # and clear_dictionary_cache() is the reset if they do.
            _negative_cache[dict_id] = exc
            raise
        _cache[dict_id] = registered
        return registered


def decompress_frame(frame: bytes) -> bytes:
    """Decompress a legacy or dictionary-backed zstd frame using its own header ID."""
    import zstandard as zstd

    dict_id = zstd.get_frame_parameters(frame).dict_id
    if dict_id == 0:
        return zstd.ZstdDecompressor().decompress(frame)
    dictionary = get_dictionary(dict_id)
    return zstd.ZstdDecompressor(dict_data=dictionary.data).decompress(frame)


def compress_frame(content: bytes, *, level: int, dict_id: int | None = None) -> bytes:
    """Compress bytes, optionally with a registered dictionary."""
    import zstandard as zstd

    if dict_id is None:
        return zstd.ZstdCompressor(level=level).compress(content)
    dictionary = get_dictionary(dict_id)
    return zstd.ZstdCompressor(
        level=level, dict_data=dictionary.compress_form(level)
    ).compress(content)


def clear_dictionary_cache() -> None:
    """Clear the process caches (tests, and after a registry write)."""
    with _cache_lock:
        _cache.clear()
        _negative_cache.clear()
