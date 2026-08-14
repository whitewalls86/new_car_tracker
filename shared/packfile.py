"""Plan 131 Stage 1: the bronze HTML pack format — writer, indexed reader, sidecar.

A pack holds many HTML artifacts in one object. `bronze` is running out of
**inodes**, not bytes, and compression cannot move that number: Plan 129 cut
bronze bytes ~73% and changed object count by zero. Packing many artifacts into
few objects is the only lever that touches the constraint.

Layout, all integers little-endian::

    header   32 bytes   magic, format version, dict_id, frame count, member count
    frame 0             an independent zstd frame: members concatenated, then
    frame 1             compressed as one unit (~8 MiB uncompressed by default)
    ...
    footer              one 28-byte entry per frame: offset, compressed length,
                        uncompressed length, member count
    trailer  20 bytes   footer offset, frame count, magic

Four properties the format exists for:

* **Frames are independently decodable.** Reading one member decompresses one
  frame, never the whole pack, and a ranged GET fetches only that frame.
* **Members concatenate with no separator.** The index carries offsets, so a
  separator would spend bytes the format does not need.
* **The format version is in the header from day one.** Packs are immutable and
  will outlive this module's assumptions.
* **Every member carries a `raw_sha256`**, which is the verification anchor for
  Stage 2 and for any later audit. A pack whose members do not all extract
  byte-identically is never finalized — see :func:`PackWriter.finish`.

The index is a **sidecar Parquet object**, not a Postgres table: 4.5M index rows
are historical record, not hot operational state, so they belong in object
storage per the MinIO-first architecture. DuckDB can glob every `.idx.parquet`
to resolve an artifact without opening a single pack.

The trained zstd dictionary stays enabled inside frames. Stage 0d measured that
the frame window does *not* subsume it: without the dictionary the first member
of a frame grows from 8,578 to 32,952 bytes while subsequent members are
unchanged. Its whole contribution is the head of each frame, which is still
~10 GiB across 441,448 groups.
"""
from __future__ import annotations

import hashlib
import struct
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Sequence

from shared.minio import ZSTD_LEVEL

MAGIC = b"CTZPACK\x00"
FORMAT_VERSION = 1

#: magic, format_version, flags, dict_id, frame_count, member_count, reserved
_HEADER = struct.Struct("<8sHHIIIQ")
#: offset, compressed_length, uncompressed_length, member_count
_FRAME_ENTRY = struct.Struct("<QQQI")
#: footer_offset, frame_count, magic — read from the end of the object
_TRAILER = struct.Struct("<QI8s")

HEADER_SIZE = _HEADER.size
FRAME_ENTRY_SIZE = _FRAME_ENTRY.size
TRAILER_SIZE = _TRAILER.size

#: Uncompressed bytes per frame — a *soft* target. A frame is sealed at the
#: first listing boundary at or after this, never in the middle of a listing.
#:
#: 16 MiB rather than 8: the first production pack (2026-08-14) held listings
#: averaging 31.8 captures at ~190 KB raw, about 6 MB each, and 8 MiB frames
#: split half of them. At 16 MiB roughly 90% of listings fit whole — p90 there
#: was 59 captures, ~11 MB. The cost is that reading one member decompresses
#: 16 MiB instead of 8, which is ~32 ms and the trade the plan accepts.
DEFAULT_FRAME_TARGET_BYTES = 16 * 1024 * 1024

#: Hard ceiling, as a multiple of the target. One listing must not be able to
#: create an unbounded frame — the corpus contains a listing with 4,467
#: captures, which at ~190 KB raw would be ~850 MB. Past this a frame is sealed
#: mid-listing, paying one extra base copy rather than blowing up the cost of
#: every single-member read in that frame.
DEFAULT_FRAME_MAX_MULTIPLE = 2

#: Frames decompressed and held in a reader. Two is enough to keep a sequential
#: walk across a frame boundary from decompressing the same frame twice.
DEFAULT_FRAME_CACHE = 2


class PackError(RuntimeError):
    """Base error for pack format failures."""


class PackFormatError(PackError):
    """The bytes are not a pack, or are structurally inconsistent."""


class UnsupportedPackVersionError(PackFormatError):
    """The pack was written by a format version this reader does not understand."""

    def __init__(self, version: int):
        self.version = version
        super().__init__(
            f"pack format version {version} is not supported by this reader "
            f"(supported: {FORMAT_VERSION})"
        )


class PackVerificationError(PackError):
    """One or more members did not extract byte-identically to their raw_sha256."""

    def __init__(self, failures: Sequence[str]):
        self.failures = list(failures)
        shown = "; ".join(self.failures[:5])
        super().__init__(
            f"{len(self.failures)} member(s) failed byte-identical verification: {shown}"
            + (" ..." if len(self.failures) > 5 else "")
        )


class PackIndexMismatchError(PackError):
    """The sidecar index and the pack footer describe different packs."""


@dataclass(frozen=True)
class PackMember:
    """One artifact offered to a pack: raw (uncompressed) HTML plus its identity."""

    source_key: str
    content: bytes
    artifact_id: int | None = None
    listing_id: str | None = None
    fetched_at: datetime | None = None


@dataclass(frozen=True)
class PackIndexEntry:
    """Where one member lives inside a pack, and what it must extract to."""

    source_key: str
    frame_ordinal: int
    offset_in_frame: int
    length: int
    raw_sha256: str
    artifact_id: int | None = None
    listing_id: str | None = None
    fetched_at: datetime | None = None


@dataclass(frozen=True)
class FrameEntry:
    """Footer record for one frame."""

    offset: int
    compressed_length: int
    uncompressed_length: int
    member_count: int


@dataclass(frozen=True)
class Pack:
    """A finalized pack: its bytes, and the index describing every member."""

    data: bytes
    entries: tuple[PackIndexEntry, ...]
    dict_id: int
    frame_count: int

    @property
    def member_count(self) -> int:
        return len(self.entries)

    @property
    def size(self) -> int:
        return len(self.data)


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------

PACK_PREFIX = "html_packs"


def pack_key(
    artifact_type: str, year: int, month: int, seq: int, *, prefix: str = PACK_PREFIX
) -> str:
    """Object key for pack *seq* of one monthly capture bucket (no bucket prefix)."""
    return f"{prefix}/{artifact_type}/{year:04d}/{month:02d}/pack-{seq:05d}.zpack"


def index_key(key: str) -> str:
    """Sidecar index key for a pack key."""
    if not key.endswith(".zpack"):
        raise ValueError(f"not a pack key: {key}")
    return key[: -len(".zpack")] + ".idx.parquet"


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class PackWriter:
    """Accumulate members into frames and finalize them into one pack object.

    Members are written in the order they are added; the caller owns the
    ordering decision. Stage 0d measured that the ordering that matters is
    ``listing_id, fetched_at`` — a vehicle's repeat captures must be adjacent
    inside one frame, because that is where the within-listing redundancy is
    (30.4% by whole-section hash, against 0.65% across listings).

    **Frames are sealed at listing boundaries, not at a fixed byte count.**
    Adjacency is worth nothing if a frame boundary lands in the middle of a
    listing: a zstd frame is an independent compression window, so the
    continuation frame re-pays that listing's full base cost instead of a
    ~2 KB delta. Measured on the first production pack (2026-08-14): fixed
    8 MiB frames split 270 of 544 listings and inflated the marginal cost per
    capture from the 2,142 B Stage 0d measured to 3,732 B — which is the whole
    difference between the projected 67.8% logical saving and the 57.8%
    actually achieved.

    So ``frame_target_bytes`` is a *soft* target: the frame is sealed at the
    first listing boundary at or after it. ``frame_max_bytes`` is the hard
    ceiling that keeps one enormous listing from producing one enormous frame.
    Passing ``frame_max_bytes == frame_target_bytes`` restores the old
    fixed-size behaviour, which is how the two are compared in tests.
    """

    def __init__(
        self,
        *,
        dict_id: int | None = None,
        level: int = ZSTD_LEVEL,
        frame_target_bytes: int = DEFAULT_FRAME_TARGET_BYTES,
        frame_max_bytes: int | None = None,
    ):
        if frame_target_bytes <= 0:
            raise ValueError("frame_target_bytes must be positive")
        if frame_max_bytes is None:
            frame_max_bytes = frame_target_bytes * DEFAULT_FRAME_MAX_MULTIPLE
        if frame_max_bytes < frame_target_bytes:
            raise ValueError("frame_max_bytes must be >= frame_target_bytes")
        self._dict_id = dict_id
        self._level = level
        self._frame_target_bytes = frame_target_bytes
        self._frame_max_bytes = frame_max_bytes

        self._frames: list[bytes] = []
        self._frame_meta: list[tuple[int, int]] = []  # (uncompressed_length, member_count)
        self._entries: list[PackIndexEntry] = []
        self._pending: list[PackMember] = []
        self._pending_bytes = 0
        self._compressed_bytes = 0
        self._finished = False

    @property
    def member_count(self) -> int:
        return len(self._entries) + len(self._pending)

    @property
    def compressed_bytes(self) -> int:
        """Compressed bytes in sealed frames.

        Members still buffered for the open frame are not counted — they have
        not been compressed yet, so their cost is not known. Callers rolling to
        a new pack at a size threshold are therefore accurate to within one
        frame, which is the granularity the threshold has anyway.
        """
        return self._compressed_bytes

    def add(self, member: PackMember) -> None:
        if self._finished:
            raise PackError("cannot add to a finished pack")

        # Seal *before* accepting a member that opens a new listing, so the
        # listing that just ended keeps all of its captures in one window.
        if (
            self._pending
            and self._pending_bytes >= self._frame_target_bytes
            and member.listing_id != self._pending[-1].listing_id
        ):
            self._seal_frame()

        self._pending.append(member)
        self._pending_bytes += len(member.content)

        # The hard ceiling is the only thing that can split a listing.
        if self._pending_bytes >= self._frame_max_bytes:
            self._seal_frame()

    def _seal_frame(self) -> None:
        if not self._pending:
            return
        from shared.compression import compress_frame

        ordinal = len(self._frames)
        payload = bytearray()
        for member in self._pending:
            offset = len(payload)
            payload += member.content
            self._entries.append(
                PackIndexEntry(
                    source_key=member.source_key,
                    frame_ordinal=ordinal,
                    offset_in_frame=offset,
                    length=len(member.content),
                    raw_sha256=hashlib.sha256(member.content).hexdigest(),
                    artifact_id=member.artifact_id,
                    listing_id=member.listing_id,
                    fetched_at=member.fetched_at,
                )
            )
        frame = compress_frame(bytes(payload), level=self._level, dict_id=self._dict_id)
        self._frames.append(frame)
        self._frame_meta.append((len(payload), len(self._pending)))
        self._compressed_bytes += len(frame)
        self._pending = []
        self._pending_bytes = 0

    def finish(self) -> Pack:
        """Seal the open frame, assemble the pack, and verify every member.

        Verification is not optional and not sampled: the pack is read back
        through :class:`PackReader` and every member is compared against its
        own ``raw_sha256``. A pack whose members do not all extract
        byte-identically is never returned, so a caller cannot upload one.
        """
        if self._finished:
            raise PackError("pack is already finished")
        self._seal_frame()
        if not self._frames:
            raise PackError("refusing to finalize a pack with no members")

        header = _HEADER.pack(
            MAGIC,
            FORMAT_VERSION,
            0,
            self._dict_id or 0,
            len(self._frames),
            len(self._entries),
            0,
        )
        parts: list[bytes] = [header]
        footer = bytearray()
        offset = HEADER_SIZE
        for frame, (uncompressed_length, member_count) in zip(self._frames, self._frame_meta):
            footer += _FRAME_ENTRY.pack(offset, len(frame), uncompressed_length, member_count)
            parts.append(frame)
            offset += len(frame)
        parts.append(bytes(footer))
        parts.append(_TRAILER.pack(offset, len(self._frames), MAGIC))

        data = b"".join(parts)
        entries = tuple(self._entries)
        verify_pack(data, entries)

        self._finished = True
        return Pack(
            data=data,
            entries=entries,
            dict_id=self._dict_id or 0,
            frame_count=len(self._frames),
        )


def build_pack(
    members: Iterable[PackMember],
    *,
    dict_id: int | None = None,
    level: int = ZSTD_LEVEL,
    frame_target_bytes: int = DEFAULT_FRAME_TARGET_BYTES,
    frame_max_bytes: int | None = None,
) -> Pack:
    """Build and verify one pack from *members*, in the order given."""
    writer = PackWriter(
        dict_id=dict_id,
        level=level,
        frame_target_bytes=frame_target_bytes,
        frame_max_bytes=frame_max_bytes,
    )
    for member in members:
        writer.add(member)
    return writer.finish()


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class PackReader:
    """Read members out of a pack, decompressing one frame at a time.

    *fetch_range(offset, length) -> bytes* is the only I/O this needs, so the
    same reader serves a local bytes object and a ranged S3 GET.
    """

    def __init__(
        self,
        fetch_range: Callable[[int, int], bytes],
        size: int,
        *,
        max_cached_frames: int = DEFAULT_FRAME_CACHE,
    ):
        self._fetch_range = fetch_range
        self._size = size
        self._max_cached_frames = max(1, max_cached_frames)
        self._cache: "OrderedDict[int, bytes]" = OrderedDict()
        self._frames: list[FrameEntry] | None = None
        self._dict_id = 0
        self._member_count = 0

    @classmethod
    def from_bytes(cls, data: bytes, **kwargs: Any) -> "PackReader":
        return cls(lambda offset, length: data[offset:offset + length], len(data), **kwargs)

    # -- header/footer ------------------------------------------------------

    def _load(self) -> list[FrameEntry]:
        if self._frames is not None:
            return self._frames
        if self._size < HEADER_SIZE + TRAILER_SIZE:
            raise PackFormatError(f"too small to be a pack: {self._size} bytes")

        trailer = self._fetch_range(self._size - TRAILER_SIZE, TRAILER_SIZE)
        footer_offset, trailer_frame_count, trailer_magic = _TRAILER.unpack(trailer)
        if trailer_magic != MAGIC:
            raise PackFormatError("trailer magic mismatch — not a pack, or truncated")

        header = self._fetch_range(0, HEADER_SIZE)
        (
            magic,
            version,
            _flags,
            dict_id,
            frame_count,
            member_count,
            _reserved,
        ) = _HEADER.unpack(header)
        if magic != MAGIC:
            raise PackFormatError("header magic mismatch — not a pack")
        if version != FORMAT_VERSION:
            raise UnsupportedPackVersionError(version)
        if frame_count != trailer_frame_count:
            raise PackFormatError(
                f"header frame count {frame_count} disagrees with trailer "
                f"{trailer_frame_count}"
            )

        footer_length = frame_count * FRAME_ENTRY_SIZE
        if footer_offset + footer_length + TRAILER_SIZE != self._size:
            raise PackFormatError(
                f"footer at {footer_offset} + {footer_length} does not end where the "
                f"trailer begins ({self._size - TRAILER_SIZE})"
            )

        raw_footer = self._fetch_range(footer_offset, footer_length)
        frames = [
            FrameEntry(*_FRAME_ENTRY.unpack_from(raw_footer, i * FRAME_ENTRY_SIZE))
            for i in range(frame_count)
        ]
        if sum(f.member_count for f in frames) != member_count:
            raise PackFormatError(
                "header member count disagrees with the sum of the footer's frames"
            )

        self._dict_id = dict_id
        self._member_count = member_count
        self._frames = frames
        return frames

    @property
    def frames(self) -> list[FrameEntry]:
        return list(self._load())

    @property
    def frame_count(self) -> int:
        return len(self._load())

    @property
    def member_count(self) -> int:
        self._load()
        return self._member_count

    @property
    def dict_id(self) -> int:
        self._load()
        return self._dict_id

    # -- reads --------------------------------------------------------------

    def read_frame(self, ordinal: int) -> bytes:
        """Return the decompressed bytes of one frame, from cache when possible."""
        from shared.compression import decompress_frame

        frames = self._load()
        if ordinal < 0 or ordinal >= len(frames):
            raise PackFormatError(f"frame {ordinal} out of range (0..{len(frames) - 1})")

        cached = self._cache.get(ordinal)
        if cached is not None:
            self._cache.move_to_end(ordinal)
            return cached

        entry = frames[ordinal]
        payload = decompress_frame(
            self._fetch_range(entry.offset, entry.compressed_length)
        )
        if len(payload) != entry.uncompressed_length:
            raise PackFormatError(
                f"frame {ordinal} decompressed to {len(payload)} bytes, footer says "
                f"{entry.uncompressed_length}"
            )
        self._cache[ordinal] = payload
        while len(self._cache) > self._max_cached_frames:
            self._cache.popitem(last=False)
        return payload

    def read_member(self, entry: PackIndexEntry) -> bytes:
        """Extract one member's raw bytes. Decompresses exactly one frame."""
        frame = self.read_frame(entry.frame_ordinal)
        end = entry.offset_in_frame + entry.length
        if entry.offset_in_frame < 0 or end > len(frame):
            raise PackIndexMismatchError(
                f"index entry for {entry.source_key} spans "
                f"{entry.offset_in_frame}..{end} of a {len(frame)}-byte frame"
            )
        return frame[entry.offset_in_frame:end]

    # -- cross-checks -------------------------------------------------------

    def check_index(self, entries: Sequence[PackIndexEntry]) -> None:
        """Assert the sidecar index and the pack footer describe the same pack.

        Disagreement is an error, never a silent preference for one of them:
        the index is what Stage 4 would consult before deleting a source
        object, and the footer is what the bytes actually say.
        """
        frames = self._load()
        if len(entries) != self._member_count:
            raise PackIndexMismatchError(
                f"index has {len(entries)} members, pack header says {self._member_count}"
            )

        by_frame: dict[int, list[PackIndexEntry]] = {}
        for entry in entries:
            if entry.frame_ordinal < 0 or entry.frame_ordinal >= len(frames):
                raise PackIndexMismatchError(
                    f"index entry for {entry.source_key} references frame "
                    f"{entry.frame_ordinal}; pack has {len(frames)}"
                )
            by_frame.setdefault(entry.frame_ordinal, []).append(entry)

        for ordinal, frame in enumerate(frames):
            members = by_frame.get(ordinal, [])
            if len(members) != frame.member_count:
                raise PackIndexMismatchError(
                    f"frame {ordinal}: index has {len(members)} members, footer says "
                    f"{frame.member_count}"
                )
            # Members concatenate with no separator, so the index offsets must
            # tile the frame exactly — no gaps, no overlaps, no slack at the end.
            cursor = 0
            for entry in sorted(members, key=lambda e: e.offset_in_frame):
                if entry.offset_in_frame != cursor:
                    raise PackIndexMismatchError(
                        f"frame {ordinal}: index entry for {entry.source_key} starts at "
                        f"{entry.offset_in_frame}, expected {cursor}"
                    )
                cursor += entry.length
            if cursor != frame.uncompressed_length:
                raise PackIndexMismatchError(
                    f"frame {ordinal}: index members total {cursor} bytes, footer says "
                    f"{frame.uncompressed_length}"
                )


def verify_pack(data: bytes, entries: Sequence[PackIndexEntry]) -> None:
    """Extract every member and compare it against its own ``raw_sha256``.

    Raises :class:`PackVerificationError` naming the members that failed.
    100% is the only acceptable rate — anything less is a bug in the extractor,
    not a tolerance.
    """
    reader = PackReader.from_bytes(data, max_cached_frames=1)
    reader.check_index(entries)

    failures: list[str] = []
    # Frame-ordered so each frame is decompressed exactly once even with a
    # single-frame cache.
    for entry in sorted(entries, key=lambda e: (e.frame_ordinal, e.offset_in_frame)):
        try:
            extracted = reader.read_member(entry)
        except Exception as exc:  # noqa: BLE001 - verification reports, it does not crash.
            # A corrupt frame raises out of zstd, not out of this module. That
            # is still "this member did not extract", and the caller needs it
            # named alongside every other failure rather than as a bare
            # ZstdError from whichever member happened to be first.
            failures.append(f"{entry.source_key}: {type(exc).__name__}: {exc}")
            continue
        actual = hashlib.sha256(extracted).hexdigest()
        if actual != entry.raw_sha256:
            failures.append(
                f"{entry.source_key}: sha256 {actual[:12]} != {entry.raw_sha256[:12]}"
            )
    if failures:
        raise PackVerificationError(failures)


# ---------------------------------------------------------------------------
# Sidecar index (Parquet)
# ---------------------------------------------------------------------------

_INDEX_FIELDS = (
    "artifact_id",
    "listing_id",
    "fetched_at",
    "source_key",
    "frame_ordinal",
    "offset_in_frame",
    "length",
    "raw_sha256",
)


def _as_utc(value: datetime | None) -> datetime | None:
    """Normalize to a UTC-aware datetime; naive values are read as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def index_schema() -> Any:
    import pyarrow as pa

    return pa.schema([
        pa.field("artifact_id", pa.int64()),
        pa.field("listing_id", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("source_key", pa.string()),
        pa.field("frame_ordinal", pa.int32()),
        pa.field("offset_in_frame", pa.int64()),
        pa.field("length", pa.int64()),
        pa.field("raw_sha256", pa.string()),
    ])


def index_to_table(entries: Sequence[PackIndexEntry]) -> Any:
    import pyarrow as pa

    columns = {
        "artifact_id": [e.artifact_id for e in entries],
        "listing_id": [e.listing_id for e in entries],
        "fetched_at": [_as_utc(e.fetched_at) for e in entries],
        "source_key": [e.source_key for e in entries],
        "frame_ordinal": [e.frame_ordinal for e in entries],
        "offset_in_frame": [e.offset_in_frame for e in entries],
        "length": [e.length for e in entries],
        "raw_sha256": [e.raw_sha256 for e in entries],
    }
    return pa.table(columns, schema=index_schema())


def write_index_parquet(entries: Sequence[PackIndexEntry]) -> bytes:
    """Serialize the sidecar index to Parquet bytes, ready to PUT."""
    import io

    import pyarrow.parquet as pq

    buffer = io.BytesIO()
    pq.write_table(index_to_table(entries), buffer, compression="zstd")
    return buffer.getvalue()


def read_index_parquet(data: bytes) -> list[PackIndexEntry]:
    """Parse sidecar index bytes back into entries."""
    import io

    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(data))
    missing = [name for name in _INDEX_FIELDS if name not in table.schema.names]
    if missing:
        raise PackIndexMismatchError(f"index is missing column(s): {', '.join(missing)}")
    rows = table.select(_INDEX_FIELDS).to_pylist()
    return [
        PackIndexEntry(
            source_key=row["source_key"],
            frame_ordinal=int(row["frame_ordinal"]),
            offset_in_frame=int(row["offset_in_frame"]),
            length=int(row["length"]),
            raw_sha256=row["raw_sha256"],
            artifact_id=row["artifact_id"],
            listing_id=row["listing_id"],
            fetched_at=row["fetched_at"],
        )
        for row in rows
    ]
