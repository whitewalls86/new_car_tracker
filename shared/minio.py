"""
Shared MinIO / S3-compatible object store helpers.

Provides two client factories (one per access pattern) and the HTML-layer
convenience functions used by the scraper and processing service:

  get_boto3_client() — singleton boto3 S3 client; thread-safe.
                       Used for raw-object get/put (bronze HTML).

  get_s3fs()         — s3fs.S3FileSystem instance.
                       Used for hive-partitioned Parquet dataset writes (silver).

Both factory functions lazy-import their library so each service only needs
to install the dependency it actually calls.  Configuration is read once from
environment variables, mirroring the pattern in shared/db.py.
"""
import logging
import os
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENDPOINT = os.environ.get("MINIO_ENDPOINT",      "http://minio:9000")
ACCESS   = os.environ.get("MINIO_ROOT_USER",     "cartracker")
SECRET   = os.environ.get("MINIO_ROOT_PASSWORD", "")
BUCKET   = os.environ.get("MINIO_BUCKET",        "bronze")

ZSTD_LEVEL = 9  # ~8-10% smaller than level 3 (Plan 116 evidence)


def _env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() not in ("0", "false", "no", "off")


#: Plan 131 Stage 3: resolve a missing HTML object through the pack index.
#: On by default — a bucket with no packs simply never finds a sidecar and the
#: fallback costs one prefix listing per missing key. Set to 0 to prove that a
#: read failure is the pack path's doing rather than the object path's.
PACK_READ_FALLBACK = _env_flag("PACK_READ_FALLBACK", True)

#: Sidecar ``source_key`` columns held as Arrow tables. Measured 2026-08-14 on
#: April-shaped sidecars (17,291 rows, 1.17 MB stored): 1.3 ms to parse and
#: 1.69 MB in Arrow, against 1.7 ms and 3.78 MB for every column. Forty-eight
#: entries cover the largest currently packed month (May: 41) with headroom,
#: while keeping the resident index near 81 MB instead of ~181 MB.
PACK_INDEX_CACHE_PACKS = int(os.environ.get("PACK_INDEX_CACHE_PACKS", "48"))

#: Open pack readers held, each with its own frame LRU. A reader holds up to
#: two decompressed frames (~16 MiB each), so this is the memory knob: 1 pack
#: is ~32 MiB. One is enough for a sequential walk, which is what reprocessing
#: does; a random-access workload gets no benefit from more.
PACK_READER_CACHE_PACKS = int(os.environ.get("PACK_READER_CACHE_PACKS", "1"))

#: Seconds a month's sidecar listing is trusted. A miss against a *cached*
#: listing re-lists once before giving up, so a newly written pack is never
#: invisible for longer than one read — the TTL only bounds how often an
#: unchanged month pays for a LIST.
PACK_INDEX_LIST_TTL = float(os.environ.get("PACK_INDEX_LIST_TTL_SECONDS", "300"))

# ---------------------------------------------------------------------------
# boto3 singleton client (raw object get/put)
# ---------------------------------------------------------------------------

_boto3_lock   = threading.Lock()
_boto3_client = None


def get_boto3_client():
    """Return the process-wide boto3 S3 client (thread-safe singleton)."""
    global _boto3_client
    if _boto3_client is None:
        with _boto3_lock:
            if _boto3_client is None:
                import boto3
                from botocore.client import Config
                _boto3_client = boto3.client(
                    "s3",
                    endpoint_url=ENDPOINT,
                    aws_access_key_id=ACCESS,
                    aws_secret_access_key=SECRET,
                    config=Config(signature_version="s3v4"),
                    region_name="us-east-1",  # required by boto3; value ignored by MinIO
                )
    return _boto3_client


# ---------------------------------------------------------------------------
# s3fs filesystem factory (Parquet dataset writes)
# ---------------------------------------------------------------------------

def get_s3fs():
    """Return a new s3fs.S3FileSystem configured for MinIO."""
    import s3fs
    return s3fs.S3FileSystem(
        key=ACCESS,
        secret=SECRET,
        use_ssl=False,
        client_kwargs={"endpoint_url": ENDPOINT},
    )


# ---------------------------------------------------------------------------
# Bucket bootstrap (boto3 path — checked once per process)
# ---------------------------------------------------------------------------

_bucket_checked    = False
_bucket_check_lock = threading.Lock()


def ensure_bucket() -> None:
    """Ensure BUCKET exists. Uses the boto3 client. Checked at most once per process."""
    global _bucket_checked
    if _bucket_checked:
        return
    with _bucket_check_lock:
        if _bucket_checked:
            return
        from botocore.exceptions import ClientError
        client = get_boto3_client()
        try:
            client.head_bucket(Bucket=BUCKET)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code not in ("404", "NoSuchBucket"):
                raise
            client.create_bucket(Bucket=BUCKET)
            logger.info("Created MinIO bucket: %s", BUCKET)
        _bucket_checked = True


# ---------------------------------------------------------------------------
# HTML object helpers (bronze layer)
# ---------------------------------------------------------------------------

def make_key(artifact_type: str, fetched_at, file_id: str | None = None) -> str:
    """
    Build the object key (without bucket prefix).

    fetched_at: datetime or ISO string — used for the hive partition.
    file_id:    UUID string to use as the filename stem; generated if omitted.
    """
    from datetime import datetime

    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at)

    fid = file_id or str(uuid.uuid4())
    return (
        f"html/year={fetched_at.year}/month={fetched_at.month}"
        f"/artifact_type={artifact_type}/{fid}.html.zst"
    )


def write_html(key: str, content: bytes) -> str:
    """
    Compress *content* with zstd and upload to MinIO at *key*.
    Returns the full S3 URI: ``s3://<bucket>/<key>``.
    """
    import hashlib

    from shared.compression import compress_frame, configured_dictionary_id

    dict_id = configured_dictionary_id()
    compressed = compress_frame(content, level=ZSTD_LEVEL, dict_id=dict_id)

    client = get_boto3_client()
    ensure_bucket()
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=compressed,
        ContentEncoding="zstd",
        ContentType="text/html",
    )
    uri = f"s3://{BUCKET}/{key}"

    if "/artifact_type=" in key:
        artifact_type = key.split("/artifact_type=")[1].split("/")[0]
    else:
        artifact_type = "unknown"
    logger.info(
        "write_html: artifact_type=%s raw_bytes=%d compressed_bytes=%d "
        "raw_sha256=%.12s dictionary_id=%s minio_path=%s key=%s",
        artifact_type, len(content), len(compressed),
        hashlib.sha256(content).hexdigest(), dict_id or 0, uri, key,
    )
    return uri


def write_json(key: str, obj) -> str:
    """
    Serialize *obj* as canonical JSON and upload to MinIO at *key*.
    Returns the full S3 URI: ``s3://<bucket>/<key>``.
    """
    import json

    body = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")

    client = get_boto3_client()
    ensure_bucket()
    client.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    return f"s3://{BUCKET}/{key}"


def read_json(minio_path: str):
    """
    Fetch and parse a JSON object.

    *minio_path* may be a full S3 URI (``s3://bucket/key``) or a bare key.
    Returns None if the object does not exist.
    """
    import json

    from botocore.exceptions import ClientError

    if minio_path.startswith("s3://"):
        remainder = minio_path[len("s3://"):]
        bucket, key = remainder.split("/", 1)
    else:
        bucket = BUCKET
        key = minio_path

    client = get_boto3_client()
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return None
        raise
    return json.loads(response["Body"].read())


def write_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    """
    Upload raw *data* to MinIO at *key* with no compression/encoding applied
    (unlike write_html, which always zstd-compresses). Returns the full S3
    URI: ``s3://<bucket>/<key>``.
    """
    client = get_boto3_client()
    ensure_bucket()
    client.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=content_type)
    return f"s3://{BUCKET}/{key}"


def read_bytes(minio_path: str) -> bytes:
    """
    Fetch a raw (uncompressed) object's bytes.

    *minio_path* may be a full S3 URI (``s3://bucket/key``) or a bare key.
    """
    if minio_path.startswith("s3://"):
        remainder = minio_path[len("s3://"):]
        bucket, key = remainder.split("/", 1)
    else:
        bucket = BUCKET
        key = minio_path

    client = get_boto3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def open_stream(minio_path: str):
    """
    Return an iterator of byte chunks for a stored object, without loading
    the whole object into memory (unlike read_bytes). Raises the underlying
    ClientError (e.g. NoSuchKey) if the object does not exist — callers that
    want a clean 404 should check object_size() first.

    *minio_path* may be a full S3 URI (``s3://bucket/key``) or a bare key.
    """
    if minio_path.startswith("s3://"):
        remainder = minio_path[len("s3://"):]
        bucket, key = remainder.split("/", 1)
    else:
        bucket = BUCKET
        key = minio_path

    client = get_boto3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].iter_chunks(chunk_size=1 << 20)


def object_size(minio_path: str) -> "int | None":
    """
    Return an object's byte size via HEAD (no body download), or None if it
    does not exist.

    *minio_path* may be a full S3 URI (``s3://bucket/key``) or a bare key.
    """
    from botocore.exceptions import ClientError

    if minio_path.startswith("s3://"):
        remainder = minio_path[len("s3://"):]
        bucket, key = remainder.split("/", 1)
    else:
        bucket = BUCKET
        key = minio_path

    client = get_boto3_client()
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return response["ContentLength"]


def _split_s3_path(minio_path: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` (or a bare key) into (bucket, key)."""
    if minio_path.startswith("s3://"):
        bucket, key = minio_path[len("s3://"):].split("/", 1)
        return bucket, key
    return BUCKET, minio_path


def object_exists(minio_path: str) -> bool:
    """Return True if the object exists in MinIO, False on a 404.

    *minio_path* may be a full S3 URI (``s3://bucket/key``) or a bare key.
    Non-404 errors propagate — a missing object and an unreachable store are
    different outcomes, and callers must not treat a network blip as "gone".
    """
    from botocore.exceptions import ClientError

    bucket, key = _split_s3_path(minio_path)
    client = get_boto3_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# ---------------------------------------------------------------------------
# Pack fallback (Plan 131 Stage 3)
# ---------------------------------------------------------------------------
#
# Stage 4 deletes source objects that are provably inside a verified pack. This
# is the read path that must be live and proven first, so that deletion is a
# latency change rather than an outage.
#
# Three design decisions, each measured on April-shaped sidecars 2026-08-14
# rather than chosen by preference:
#
# * **The search is bounded by the source key's own hive partition.** A key
#   under ``html/year=2026/month=4/artifact_type=detail_page/`` can only be in
#   a pack under ``html_packs/detail_page/2026/04/``, so one month's 32
#   sidecars are searched, never every month's. Globbing everything was the
#   plan's stated approach and is fine for an audit; this is the parse path.
# * **pyarrow, not DuckDB.** The one production caller of ``read_html`` is the
#   processing service, whose image has pyarrow and boto3 but no DuckDB, so a
#   DuckDB resolver would mean a new dependency on the hot path. It would not
#   buy anything either: a cold 32-sidecar scan measured 41 ms with pyarrow
#   against 19 ms for a DuckDB glob of the same files, and a *warm* lookup is
#   0.04 ms because the Arrow table is already in memory.
# * **The index cache matters more than the frame LRU for resolution.** Parsing
#   a sidecar costs 1.7 ms and 3.8 MB; a lookup in a cached one costs 0.04 ms.
#   Reprocessing walks many members of the same pack, so both caches earn their
#   keep, but they earn it at different steps.
#
# Every packed read verifies the extracted bytes against the sidecar's own
# ``raw_sha256``. That is ~0.15 ms against a ~16 MiB frame decompress, and it
# makes this function self-verifying — which is precisely the claim Stage 4
# deletes source objects on the strength of.

_NOT_FOUND_CODES = ("404", "NoSuchKey", "NotFound")

_pack_lock = threading.Lock()
#: index key -> Arrow table containing only that pack's ``source_key`` column
_pack_index_cache: "OrderedDict[str, Any]" = OrderedDict()
#: pack key -> (PackReader, per-reader lock). PackReader mutates a frame LRU,
#: so concurrent reads of one pack are serialised; different packs are not.
_pack_reader_cache: "OrderedDict[str, tuple[Any, threading.Lock]]" = OrderedDict()
#: month prefix -> (monotonic timestamp, sidecar keys)
_pack_listing_cache: dict = {}


def clear_pack_caches() -> None:
    """Drop every cached sidecar, reader and listing (tests, and after packing)."""
    with _pack_lock:
        _pack_index_cache.clear()
        _pack_reader_cache.clear()
        _pack_listing_cache.clear()


def pack_lookup_prefix(key: str) -> "str | None":
    """Map a bronze HTML key to the pack prefix that could hold it.

    ``html/year=2026/month=4/artifact_type=detail_page/<uuid>.html.zst``
    -> ``html_packs/detail_page/2026/04/``

    Returns None for anything that is not a hive-partitioned bronze HTML key,
    which is the signal that this key was never packable and no search should
    be attempted for it.
    """
    from shared.packfile import PACK_PREFIX

    if not key.startswith("html/"):
        return None
    fields = {}
    for segment in key.split("/")[1:-1]:
        name, sep, value = segment.partition("=")
        if sep:
            fields[name] = value
    try:
        year = int(fields["year"])
        month = int(fields["month"])
        artifact_type = fields["artifact_type"]
    except (KeyError, ValueError):
        return None
    if not 1 <= month <= 12 or not artifact_type:
        return None
    return f"{PACK_PREFIX}/{artifact_type}/{year:04d}/{month:02d}/"


def _list_sidecars(bucket: str, prefix: str, *, refresh: bool = False) -> tuple:
    """Sidecar index keys under a month's pack prefix, cached for a TTL.

    Returns ``(keys, from_cache)``. The caller needs to know whether the answer
    could be stale, because a miss against a cached listing is worth re-listing
    for and a miss against a fresh one is not.
    """
    cache_key = f"{bucket}/{prefix}"
    now = time.monotonic()
    if not refresh:
        with _pack_lock:
            cached = _pack_listing_cache.get(cache_key)
        if cached is not None and now - cached[0] < PACK_INDEX_LIST_TTL:
            return cached[1], True

    client = get_boto3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys = [
        entry["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for entry in page.get("Contents", [])
        if entry["Key"].endswith(".idx.parquet")
    ]
    keys.sort()
    with _pack_lock:
        _pack_listing_cache[cache_key] = (now, keys)
    return keys, False


def _sidecar_source_keys(bucket: str, sidecar_key: str):
    """Return a sidecar's cached ``source_key`` table and any fresh bytes.

    The raw bytes are returned only on a cache miss so a matching cold lookup
    can parse its full entry without fetching the sidecar twice. Only the
    projected key column is retained across lookups.
    """
    import io

    import pyarrow.parquet as pq

    from shared.packfile import PackIndexMismatchError

    cache_key = f"{bucket}/{sidecar_key}"
    with _pack_lock:
        table = _pack_index_cache.get(cache_key)
        if table is not None:
            _pack_index_cache.move_to_end(cache_key)
            return table, None

    client = get_boto3_client()
    body = client.get_object(Bucket=bucket, Key=sidecar_key)["Body"].read()
    parquet = pq.ParquetFile(io.BytesIO(body))
    if "source_key" not in parquet.schema_arrow.names:
        raise PackIndexMismatchError(
            f"{sidecar_key} has no source_key column — not a pack index"
        )
    table = parquet.read(columns=["source_key"])

    with _pack_lock:
        _pack_index_cache[cache_key] = table
        while len(_pack_index_cache) > max(1, PACK_INDEX_CACHE_PACKS):
            _pack_index_cache.popitem(last=False)
    return table, body


def _sidecar_entry(bucket: str, sidecar_key: str, row: int, body: "bytes | None"):
    """Parse one matching entry, reusing sidecar bytes from a cold key scan."""
    import io

    import pyarrow.parquet as pq

    from shared.packfile import PackIndexEntry, PackIndexMismatchError

    if body is None:
        client = get_boto3_client()
        body = client.get_object(Bucket=bucket, Key=sidecar_key)["Body"].read()

    columns = (
        "artifact_id",
        "listing_id",
        "fetched_at",
        "source_key",
        "frame_ordinal",
        "offset_in_frame",
        "length",
        "raw_sha256",
    )
    parquet = pq.ParquetFile(io.BytesIO(body))
    missing = [name for name in columns if name not in parquet.schema_arrow.names]
    if missing:
        raise PackIndexMismatchError(
            f"{sidecar_key} is missing column(s): {', '.join(missing)}"
        )
    record = parquet.read(columns=list(columns)).slice(row, 1).to_pylist()[0]
    return PackIndexEntry(
        source_key=record["source_key"],
        frame_ordinal=int(record["frame_ordinal"]),
        offset_in_frame=int(record["offset_in_frame"]),
        length=int(record["length"]),
        raw_sha256=record["raw_sha256"],
        artifact_id=record.get("artifact_id"),
        listing_id=record.get("listing_id"),
        fetched_at=record.get("fetched_at"),
    )


def _find_index_entry(bucket: str, key: str):
    """Locate *key* in one month's sidecars.

    Returns ``(pack_key, PackIndexEntry, sidecar_row_count)``, or None when no
    pack in the month claims it.

    Sidecars are searched in sequence order, which is also capture order, so a
    reprocessing walk tends to hit the same sidecar repeatedly and the cache
    holds. A miss re-lists the prefix once before giving up: a pack written
    since the listing was cached must not be invisible.
    """
    import pyarrow.compute as pc

    from shared.packfile import index_key

    prefix = pack_lookup_prefix(key)
    if prefix is None:
        return None

    def search(sidecar_keys, skip: set):
        for sidecar_key in sidecar_keys:
            if sidecar_key in skip:
                continue
            skip.add(sidecar_key)
            table, body = _sidecar_source_keys(bucket, sidecar_key)
            row = pc.index(table.column("source_key"), key).as_py()
            if row is None or row < 0:
                continue
            entry = _sidecar_entry(bucket, sidecar_key, row, body)
            pack = sidecar_key[: -len(".idx.parquet")] + ".zpack"
            # index_key() is the inverse and is the definition of the pairing;
            # checking it here keeps the two together if either ever changes.
            if index_key(pack) != sidecar_key:  # pragma: no cover - defensive
                continue
            return pack, entry, table.num_rows
        return None

    searched: set = set()
    sidecars, from_cache = _list_sidecars(bucket, prefix)
    found = search(sidecars, searched)
    if found is None and from_cache:
        # The listing could be stale, and a pack written since it was cached
        # must not be invisible. A fresh listing is re-searched for whatever it
        # adds; sidecars already searched are not read twice.
        fresh, _ = _list_sidecars(bucket, prefix, refresh=True)
        found = search(fresh, searched)
    return found


def artifact_exists(minio_path: str) -> bool:
    """Return whether an HTML artifact is available loose or from a pack.

    Unlike :func:`object_exists`, this answers the logical read-path question:
    a source object pruned after packing still exists as an artifact. Non-404
    object-store and malformed-index errors propagate rather than being
    mistaken for absence.
    """
    if object_exists(minio_path):
        return True
    bucket, key = _split_s3_path(minio_path)
    return _find_index_entry(bucket, key) is not None


def _pack_reader(bucket: str, pack_key: str, index_rows: int):
    """Open (or reuse) a reader for one pack, with its own frame LRU.

    The reader is created against the *stored* object over ranged GETs, and its
    header member count is checked against the sidecar's row count on creation.
    A sidecar that describes a different pack is an error here, not a silent
    preference for one of them — the sidecar is what Stage 4 consults before
    deleting, and the footer is what the bytes actually say.
    """
    from shared.packfile import PackIndexMismatchError, PackReader

    cache_key = f"{bucket}/{pack_key}"
    with _pack_lock:
        cached = _pack_reader_cache.get(cache_key)
        if cached is not None:
            _pack_reader_cache.move_to_end(cache_key)
            return cached

    client = get_boto3_client()
    size = client.head_object(Bucket=bucket, Key=pack_key)["ContentLength"]

    def fetch(offset: int, length: int) -> bytes:
        end = offset + length - 1
        response = client.get_object(
            Bucket=bucket, Key=pack_key, Range=f"bytes={offset}-{end}"
        )
        return response["Body"].read()

    reader = PackReader(fetch, size)
    if reader.member_count != index_rows:
        raise PackIndexMismatchError(
            f"sidecar for {pack_key} has {index_rows} members, the pack header "
            f"says {reader.member_count}"
        )
    entry = (reader, threading.Lock())

    with _pack_lock:
        _pack_reader_cache[cache_key] = entry
        while len(_pack_reader_cache) > max(1, PACK_READER_CACHE_PACKS):
            _pack_reader_cache.popitem(last=False)
    return entry


def read_packed_html(minio_path: str) -> "bytes | None":
    """Return an artifact's bytes from its pack, or None if no pack holds it.

    Exposed for Stage 4 and for verification tooling, which need to exercise
    exactly this path rather than something that resembles it.
    """
    import hashlib

    from shared.packfile import PackVerificationError

    bucket, key = _split_s3_path(minio_path)
    located = _find_index_entry(bucket, key)
    if located is None:
        return None
    pack_key, entry, index_rows = located

    reader, lock = _pack_reader(bucket, pack_key, index_rows)
    with lock:
        content = reader.read_member(entry)

    actual = hashlib.sha256(content).hexdigest()
    if actual != entry.raw_sha256:
        raise PackVerificationError(
            [f"{key} in {pack_key}: sha256 {actual[:12]} != {entry.raw_sha256[:12]}"]
        )
    logger.info(
        "read_html: served %s from pack %s frame=%d bytes=%d",
        key, pack_key, entry.frame_ordinal, len(content),
    )
    return content


def read_html(minio_path: str) -> bytes:
    """
    Fetch and decompress a zstd-compressed HTML object.

    *minio_path* may be a full S3 URI (``s3://bucket/key``) or a bare key.
    Returns the raw (uncompressed) HTML bytes.

    An object that is **gone** is resolved through the pack index instead
    (Plan 131 Stage 3), transparently and with no signature change. A key that
    is in neither place raises the same 404 it raises today, from the original
    GET rather than from the pack search, because "this artifact does not
    exist" is the same answer it has always been.
    """
    from botocore.exceptions import ClientError

    from shared.compression import decompress_frame

    bucket, key = _split_s3_path(minio_path)

    client = get_boto3_client()
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        compressed = response["Body"].read()
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in _NOT_FOUND_CODES or not PACK_READ_FALLBACK:
            raise
        content = read_packed_html(f"s3://{bucket}/{key}")
        if content is None:
            raise
        return content

    # Deliberately outside the except: a stored object that fails to decompress
    # is a corrupt object and must say so. Falling back to the pack there would
    # hide exactly the corruption the sha256 verification exists to catch.
    return decompress_frame(compressed)
