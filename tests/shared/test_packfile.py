"""Unit tests for shared/packfile.py (Plan 131 Stage 1).

Pure in-memory: no MinIO, no DuckDB, no dictionary registry. Packs are built
with ``dict_id=None`` so ``shared.compression`` never needs to resolve a
registered dictionary — the dictionary is a compression-ratio question, and
nothing in the format depends on which one (if any) a frame used.
"""
import hashlib
import struct
from datetime import datetime, timezone

import pytest

import shared.compression as compression
from shared.packfile import (
    FORMAT_VERSION,
    FRAME_ENTRY_SIZE,
    HEADER_SIZE,
    MAGIC,
    TRAILER_SIZE,
    PackError,
    PackFormatError,
    PackIndexEntry,
    PackIndexMismatchError,
    PackMember,
    PackReader,
    PackVerificationError,
    PackWriter,
    UnsupportedPackVersionError,
    build_pack,
    index_key,
    pack_key,
    read_index_parquet,
    verify_pack,
    write_index_parquet,
)

_FRAME_TARGET = 4096


def _html(i: int, *, size: int = 900) -> bytes:
    """A page whose bytes are deterministic but not trivially compressible."""
    body = f"<html><body><h1>listing {i}</h1><p>{hashlib.sha256(str(i).encode()).hexdigest()}</p>"
    filler = (f"row-{i}-" * 40)[: max(0, size - len(body) - 14)]
    return (body + filler + "</body></html>").encode("utf-8")


def _members(n: int, *, size: int = 900) -> list[PackMember]:
    return [
        PackMember(
            source_key=f"html/year=2026/month=5/artifact_type=detail_page/uuid-{i:04d}.html.zst",
            content=_html(i, size=size),
            artifact_id=1000 + i,
            listing_id=f"L{i // 3:04d}",
            fetched_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        )
        for i in range(n)
    ]


def _build(n: int = 25, **kwargs):
    members = _members(n)
    pack = build_pack(members, dict_id=None, frame_target_bytes=_FRAME_TARGET, **kwargs)
    return members, pack


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------

def test_every_member_extracts_byte_identically():
    members, pack = _build(25)
    assert pack.frame_count > 1, "test needs multiple frames to be meaningful"
    assert pack.member_count == len(members)

    reader = PackReader.from_bytes(pack.data)
    by_key = {m.source_key: m.content for m in members}
    for entry in pack.entries:
        assert reader.read_member(entry) == by_key[entry.source_key]
        assert hashlib.sha256(by_key[entry.source_key]).hexdigest() == entry.raw_sha256


def test_last_member_in_each_frame_extracts_byte_identically():
    members, pack = _build(25)
    by_key = {m.source_key: m.content for m in members}
    reader = PackReader.from_bytes(pack.data)

    last_in_frame = {}
    for entry in pack.entries:
        current = last_in_frame.get(entry.frame_ordinal)
        if current is None or entry.offset_in_frame > current.offset_in_frame:
            last_in_frame[entry.frame_ordinal] = entry

    assert len(last_in_frame) == pack.frame_count
    for entry in last_in_frame.values():
        assert reader.read_member(entry) == by_key[entry.source_key]


def test_single_member_pack_round_trips():
    members = _members(1)
    pack = build_pack(members, dict_id=None, frame_target_bytes=_FRAME_TARGET)

    assert pack.frame_count == 1
    assert pack.member_count == 1
    reader = PackReader.from_bytes(pack.data)
    assert reader.read_member(pack.entries[0]) == members[0].content


def test_member_larger_than_the_frame_target_still_round_trips():
    """One oversized page must not be silently split or dropped."""
    members = _members(3, size=_FRAME_TARGET * 3)
    pack = build_pack(members, dict_id=None, frame_target_bytes=_FRAME_TARGET)

    reader = PackReader.from_bytes(pack.data)
    for member, entry in zip(members, pack.entries):
        assert reader.read_member(entry) == member.content


def test_pack_with_no_members_is_refused():
    with pytest.raises(PackError, match="no members"):
        build_pack([], dict_id=None)


def test_members_concatenate_with_no_separator():
    members, pack = _build(25)
    reader = PackReader.from_bytes(pack.data)
    for ordinal in range(pack.frame_count):
        in_frame = sorted(
            (e for e in pack.entries if e.frame_ordinal == ordinal),
            key=lambda e: e.offset_in_frame,
        )
        expected = b"".join(
            next(m.content for m in members if m.source_key == e.source_key) for e in in_frame
        )
        assert reader.read_frame(ordinal) == expected


def test_frames_are_independently_decodable_without_the_reader():
    """A frame's byte range is a complete zstd frame on its own."""
    import zstandard as zstd

    _, pack = _build(25)
    reader = PackReader.from_bytes(pack.data)
    for ordinal, frame in enumerate(reader.frames):
        raw = pack.data[frame.offset:frame.offset + frame.compressed_length]
        assert zstd.ZstdDecompressor().decompress(raw) == reader.read_frame(ordinal)


# ---------------------------------------------------------------------------
# Frame boundaries and caching
# ---------------------------------------------------------------------------

def test_reading_one_member_decompresses_exactly_one_frame(mocker):
    _, pack = _build(25)
    assert pack.frame_count >= 3

    spy = mocker.patch(
        "shared.compression.decompress_frame", side_effect=compression.decompress_frame
    )
    reader = PackReader.from_bytes(pack.data)
    last = max(pack.entries, key=lambda e: (e.frame_ordinal, e.offset_in_frame))
    reader.read_member(last)

    assert spy.call_count == 1


def test_repeat_reads_of_a_cached_frame_do_not_decompress_again(mocker):
    _, pack = _build(25)
    spy = mocker.patch(
        "shared.compression.decompress_frame", side_effect=compression.decompress_frame
    )

    reader = PackReader.from_bytes(pack.data)
    first_frame = [e for e in pack.entries if e.frame_ordinal == 0]
    for entry in first_frame:
        reader.read_member(entry)

    assert spy.call_count == 1


def test_reader_fetches_only_the_frame_it_needs():
    _, pack = _build(25)
    reads: list[tuple[int, int]] = []

    def fetch(offset: int, length: int) -> bytes:
        reads.append((offset, length))
        return pack.data[offset:offset + length]

    reader = PackReader(fetch, len(pack.data))
    entry = max(pack.entries, key=lambda e: e.frame_ordinal)
    reader.read_member(entry)

    frame = reader.frames[entry.frame_ordinal]
    assert (frame.offset, frame.compressed_length) in reads
    assert sum(length for _, length in reads) < len(pack.data)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def test_a_member_failing_its_sha256_fails_the_pack():
    _, pack = _build(5)
    tampered = list(pack.entries)
    tampered[2] = PackIndexEntry(
        **{**vars(tampered[2]), "raw_sha256": "0" * 64}
    )

    with pytest.raises(PackVerificationError) as exc:
        verify_pack(pack.data, tampered)
    assert tampered[2].source_key in str(exc.value)
    assert len(exc.value.failures) == 1


def test_verification_names_every_failing_member():
    _, pack = _build(5)
    tampered = [
        PackIndexEntry(**{**vars(e), "raw_sha256": "0" * 64}) for e in pack.entries
    ]
    with pytest.raises(PackVerificationError) as exc:
        verify_pack(pack.data, tampered)
    assert len(exc.value.failures) == 5


def test_corrupted_frame_bytes_fail_verification_rather_than_escaping():
    _, pack = _build(5)
    corrupt = bytearray(pack.data)
    frame_start = HEADER_SIZE
    corrupt[frame_start + 10] ^= 0xFF

    with pytest.raises(PackVerificationError):
        verify_pack(bytes(corrupt), pack.entries)


def test_finish_verifies_before_returning(mocker):
    """A pack whose members do not extract byte-identically is never finalized."""
    mocker.patch(
        "shared.packfile.verify_pack",
        side_effect=PackVerificationError(["synthetic failure"]),
    )
    writer = PackWriter(dict_id=None, frame_target_bytes=_FRAME_TARGET)
    for member in _members(3):
        writer.add(member)

    with pytest.raises(PackVerificationError):
        writer.finish()


# ---------------------------------------------------------------------------
# Header / format version
# ---------------------------------------------------------------------------

def _rewrite_header(data: bytes, **overrides) -> bytes:
    from shared.packfile import _HEADER

    fields = list(_HEADER.unpack_from(data, 0))
    names = ["magic", "version", "flags", "dict_id", "frame_count", "member_count", "reserved"]
    for name, value in overrides.items():
        fields[names.index(name)] = value
    out = bytearray(data)
    _HEADER.pack_into(out, 0, *fields)
    return bytes(out)


def test_future_format_version_raises_rather_than_misreading():
    _, pack = _build(3)
    future = _rewrite_header(pack.data, version=FORMAT_VERSION + 1)

    with pytest.raises(UnsupportedPackVersionError) as exc:
        PackReader.from_bytes(future).read_frame(0)
    assert exc.value.version == FORMAT_VERSION + 1


def test_bad_header_magic_raises():
    _, pack = _build(3)
    broken = _rewrite_header(pack.data, magic=b"NOTAPACK")

    with pytest.raises(PackFormatError, match="header magic"):
        PackReader.from_bytes(broken).frame_count


def test_bad_trailer_magic_raises():
    _, pack = _build(3)
    broken = bytearray(pack.data)
    broken[-8:] = b"NOTAPACK"

    with pytest.raises(PackFormatError, match="trailer magic"):
        PackReader.from_bytes(bytes(broken)).frame_count


def test_header_and_trailer_frame_count_disagreement_raises():
    _, pack = _build(25)
    broken = _rewrite_header(pack.data, frame_count=99)

    with pytest.raises(PackFormatError, match="frame count"):
        PackReader.from_bytes(broken).frame_count


def test_truncated_pack_raises():
    _, pack = _build(3)
    with pytest.raises(PackFormatError):
        PackReader.from_bytes(pack.data[: HEADER_SIZE + TRAILER_SIZE - 1]).frame_count


def test_a_non_pack_object_raises():
    payload = b"x" * 4096
    with pytest.raises(PackFormatError):
        PackReader.from_bytes(payload).frame_count


def test_trailer_layout_is_readable_from_the_end():
    """A reader with only the object size can find the footer in two ranged GETs."""
    _, pack = _build(25)
    footer_offset, frame_count, magic = struct.unpack("<QI8s", pack.data[-TRAILER_SIZE:])
    assert magic == MAGIC
    assert frame_count == pack.frame_count
    assert footer_offset + frame_count * FRAME_ENTRY_SIZE + TRAILER_SIZE == len(pack.data)


# ---------------------------------------------------------------------------
# Index / footer agreement
# ---------------------------------------------------------------------------

def test_index_and_footer_agree_on_a_good_pack():
    _, pack = _build(25)
    PackReader.from_bytes(pack.data).check_index(pack.entries)


def test_missing_index_entry_is_an_error():
    _, pack = _build(25)
    reader = PackReader.from_bytes(pack.data)
    with pytest.raises(PackIndexMismatchError, match="members"):
        reader.check_index(pack.entries[:-1])


def test_shifted_index_offset_is_an_error():
    _, pack = _build(25)
    entries = list(pack.entries)
    entries[1] = PackIndexEntry(
        **{**vars(entries[1]), "offset_in_frame": entries[1].offset_in_frame + 1}
    )
    with pytest.raises(PackIndexMismatchError):
        PackReader.from_bytes(pack.data).check_index(entries)


def test_index_entry_pointing_at_a_missing_frame_is_an_error():
    _, pack = _build(25)
    entries = list(pack.entries)
    entries[0] = PackIndexEntry(**{**vars(entries[0]), "frame_ordinal": 99})
    with pytest.raises(PackIndexMismatchError, match="frame"):
        PackReader.from_bytes(pack.data).check_index(entries)


def test_index_member_longer_than_its_frame_is_an_error():
    _, pack = _build(25)
    entries = list(pack.entries)
    last = max(entries, key=lambda e: (e.frame_ordinal, e.offset_in_frame))
    entries[entries.index(last)] = PackIndexEntry(
        **{**vars(last), "length": last.length + 1}
    )
    with pytest.raises(PackIndexMismatchError):
        PackReader.from_bytes(pack.data).check_index(entries)


# ---------------------------------------------------------------------------
# Sidecar index
# ---------------------------------------------------------------------------

def test_index_parquet_round_trips():
    _, pack = _build(25)
    restored = read_index_parquet(write_index_parquet(pack.entries))

    assert len(restored) == len(pack.entries)
    for original, entry in zip(pack.entries, restored):
        assert entry.source_key == original.source_key
        assert entry.frame_ordinal == original.frame_ordinal
        assert entry.offset_in_frame == original.offset_in_frame
        assert entry.length == original.length
        assert entry.raw_sha256 == original.raw_sha256
        assert entry.artifact_id == original.artifact_id
        assert entry.listing_id == original.listing_id
        assert entry.fetched_at == original.fetched_at


def test_index_parquet_round_trip_still_verifies_the_pack():
    _, pack = _build(25)
    verify_pack(pack.data, read_index_parquet(write_index_parquet(pack.entries)))


def test_index_parquet_accepts_members_without_silver_metadata():
    """Objects with no surviving silver row are still packable and still indexed."""
    members = [
        PackMember(source_key="html/year=2026/month=5/x.html.zst", content=_html(1))
    ]
    pack = build_pack(members, dict_id=None)
    restored = read_index_parquet(write_index_parquet(pack.entries))

    assert restored[0].artifact_id is None
    assert restored[0].listing_id is None
    assert restored[0].fetched_at is None
    assert restored[0].source_key == members[0].source_key


def test_index_missing_a_required_column_is_an_error():
    import io

    import pyarrow.parquet as pq

    from shared.packfile import index_to_table

    _, pack = _build(3)
    table = index_to_table(pack.entries).drop_columns(["raw_sha256"])
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    with pytest.raises(PackIndexMismatchError, match="raw_sha256"):
        read_index_parquet(buffer.getvalue())


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------

def test_pack_and_index_keys():
    key = pack_key("detail_page", 2026, 5, 3)
    assert key == "html_packs/detail_page/2026/05/pack-00003.zpack"
    assert index_key(key) == "html_packs/detail_page/2026/05/pack-00003.idx.parquet"


def test_index_key_rejects_a_non_pack_key():
    with pytest.raises(ValueError):
        index_key("html_packs/detail_page/2026/05/pack-00003.parquet")


# ---------------------------------------------------------------------------
# Writer bookkeeping
# ---------------------------------------------------------------------------

def test_compressed_bytes_counts_sealed_frames_only():
    writer = PackWriter(dict_id=None, frame_target_bytes=_FRAME_TARGET)
    assert writer.compressed_bytes == 0

    for member in _members(2):
        writer.add(member)
    assert writer.compressed_bytes == 0, "nothing sealed yet"

    for member in _members(25)[2:]:
        writer.add(member)
    assert writer.compressed_bytes > 0

    pack = writer.finish()
    assert writer.compressed_bytes <= pack.size


def test_writer_refuses_further_members_after_finish():
    writer = PackWriter(dict_id=None, frame_target_bytes=_FRAME_TARGET)
    for member in _members(2):
        writer.add(member)
    writer.finish()

    with pytest.raises(PackError, match="finished"):
        writer.add(_members(1)[0])


def test_member_order_is_preserved():
    """The caller owns ordering (listing_id, fetched_at); the writer must not resort."""
    members = _members(25)
    pack = build_pack(members, dict_id=None, frame_target_bytes=_FRAME_TARGET)
    assert [e.source_key for e in pack.entries] == [m.source_key for m in members]
