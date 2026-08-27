"""Plan 145: the April detail-page reconciliation command.

Plan 145 deletes the 1,172 legacy April ``detail_page`` Parquet objects, but
only after every distinct successful capture is either already represented in
silver or rebuilt as a real current artifact. That is six stages of work over
one frozen population, so this is one command with one mode per stage rather
than six scripts that each re-derive the census.

Four modes live here, one per early stage of the plan's third revision, which
*flattens* the population before parsing it:

* ``census`` (Stage 1) -- read-only. Enumerates the exact legacy prefix,
  streams every row occurrence, recomputes hashes, and writes local manifests
  plus fingerprints. Never writes to MinIO, never touches Postgres.
* ``materialize`` (Stage 2) -- writes every surviving successful legacy body as
  a normal ``.html.zst`` object under a content-derived key, one Parquet
  manifest shard per source file under ``recovery/plan145/materialized/``.
* ``dedupe`` (Stage 3a) -- deletes each materialized object whose ``raw_sha256``
  already appears in an April pack sidecar. A join of two written-down columns;
  no bytes are re-hashed. The per-shard deletion manifest under
  ``recovery/plan145/dedupe/`` is written before that shard's first delete,
  which is by exact key in capped batches with one receipt per key.
* ``unpack`` (Stage 3b) -- writes every April pack member back as a loose
  object **under its original ``source_key``**, verifying each member against
  its sidecar ``raw_sha256`` first, one manifest shard per pack under
  ``recovery/plan145/unpacked/``.

``dedupe`` and ``unpack`` default to a dry run and take an explicit ``--apply``;
between them the population becomes one flat prefix of distinct captures that
Stage 4 parses. No mode writes Postgres.

What the legacy rows actually are
---------------------------------
The population was written by the Plan 72 archiver
(``archiver/processors/archive_artifacts.py`` at commit 1798a99), which paired
``raw_artifacts`` metadata from Postgres with the HTML read off local disk::

    html = b""
    if filepath and os.path.exists(filepath):
        html = open(filepath, "rb").read()

Three consequences drive this script's design, and each one contradicts a
convenient reading of the plan:

1. **Empty bodies carry a non-empty stored hash.** When the disk file was
   already gone at archive time, the writer stored ``b""`` for ``html`` while
   still copying ``sha256`` from the database row -- the hash of the *original*
   page. So for empty rows the stored hash is real and the bytes are not, and
   ``sha256(b"") != stored_sha256`` is the expected state, not corruption.
   Empty rows are therefore excluded from hash verification and from the
   recovery population: there are no bytes to recover. They are still counted,
   because the plan's census invariant is over occurrences.

2. **A row's status says nothing about whether its bytes survived.** An empty
   row is typically ``http_status = 200``: the fetch succeeded, the file was
   just cleaned up before the archiver reached it. So "successful capture" and
   "recoverable capture" are different populations, and this script reports the
   status census and the empty census as a cross-tab rather than as one number.

3. **There is no ``listing_id`` column.** The legacy schema carries
   ``artifact_id, run_id, source, artifact_type, search_key, search_scope, url,
   fetched_at, http_status, content_bytes, sha256, error, page_num, html``.
   The plan's ``(listing_id, fetched_at)`` identity is recovered from ``url``
   using the same UUID extraction the production parser applies
   (``processing/processors/parse_detail_page.py``), so legacy and current
   identity are derived by identical rules.

Two hashes, deliberately kept apart
-----------------------------------
``stored_sha256`` is what the old system recorded. ``recomputed_sha256`` is
what the surviving bytes actually hash to. Plan 145 leans on "the SHA" in
several places, but the two answer different questions and Stage 2 needs the
second one specifically:

* the pack join is content-based, so it must use ``recomputed_sha256``; a
  stored hash whose bytes are gone would join a pack member this Parquet row
  cannot actually supply.
* the distinct-observation count is an identity question, so it tolerates
  either -- but the report emits both distinct counts so the Stage 1 gate can
  be checked against whichever the reviewer means.

Fail-closed behaviour
---------------------
The run stops, non-zero, without writing manifests when a non-empty body
disagrees with its stored hash, or when two occurrences sharing
``(listing_id, fetched_at)`` disagree on recomputed hash. Both are donor
selection problems, and Plan 145 is explicit that this run must not choose a
donor. Disagreements are reported with bounded examples so the delta can be
explained before a rerun.

Where it runs
-------------
Needs pyarrow, s3fs and boto3 plus network reach to MinIO, so it runs inside
the compose network (or through a tunnel with ``MINIO_ENDPOINT`` pointed at
it), not from a laptop against nothing.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

logger = logging.getLogger("reconcile_april_detail")

# The Plan 72 archiver wrote s3://<bucket>/html partitioned by year, month and
# artifact_type. pyarrow renders int32 partition values unpadded ("month=4"),
# but the exact spelling is discovered by listing rather than assumed -- a
# hardcoded guess that matches nothing would look like an empty population
# instead of a failure. See _discover_prefix.
ARCHIVE_ROOT = "html"
TARGET_YEAR = 2026
TARGET_MONTH = 4
TARGET_ARTIFACT_TYPE = "detail_page"

# Plan 145's baseline, restated so drift is a stop rather than a shrug. These
# are assertions about production as measured 2026-08-21, not configuration.
BASELINE_OBJECTS = 1172
BASELINE_ROWS = 951821
BASELINE_STATUS_CENSUS = {"200": 847785, "403": 104025, "5xx": 11}

# Columns stored inside each Parquet file. `year`, `month` and `artifact_type`
# are *partition* columns: pyarrow encodes them in the key path and omits them
# from the file, so asking for them by name fails the schema check. The prefix
# already pins all three, which is why nothing is lost by dropping them here.
METADATA_COLUMNS = [
    "artifact_id", "run_id", "source", "search_key",
    "search_scope", "url", "fetched_at", "http_status", "content_bytes",
    "sha256", "error", "page_num",
]
PARTITION_COLUMNS = ("year", "month", "artifact_type")
HTML_COLUMN = "html"

# Same extraction the production detail parser uses, so legacy and current
# listing identity cannot diverge by rule.
_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()

OCCURRENCE_FIELDS = [
    "legacy_object_key", "row_group", "row_offset",
    "legacy_artifact_id", "run_id", "source", "search_key", "search_scope",
    "url", "listing_id", "fetched_at", "http_status", "page_num", "error",
    "content_bytes", "html_len", "stored_sha256", "recomputed_sha256",
]

OBSERVATION_FIELDS = [
    "listing_id", "fetched_at", "recomputed_sha256", "stored_sha256",
    "html_len", "http_status", "url", "run_id", "source",
    "search_key", "search_scope", "page_num",
    "donor_legacy_object_key", "donor_row_group", "donor_row_offset",
    "occurrence_count",
]


class ReconcileError(RuntimeError):
    """A gate failed. The run stops and writes no manifests."""


def extract_listing_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = _UUID_RE.search(url)
    return m.group(0) if m else None


def status_bucket(http_status: Optional[int]) -> str:
    """Collapse a status into the plan's three census buckets.

    Plan 145 censuses 200 / 403 / 5xx and nothing else. Anything outside those
    is bucketed under its own label rather than silently folded into one of
    them: a fourth bucket appearing is exactly the kind of drift the gate is
    supposed to catch.
    """
    if http_status is None:
        return "null"
    if http_status == 200:
        return "200"
    if http_status == 403:
        return "403"
    if 500 <= http_status <= 599:
        return "5xx"
    return str(http_status)


def _normalize_fetched_at(value: Any) -> Optional[str]:
    """Render a capture time as a stable UTC ISO-8601 string.

    Plan 145 matches silver on *exact normalized* ``(listing_id, fetched_at)``,
    so the normalization has to be one rule applied everywhere rather than
    whatever repr pyarrow hands back for a given chunk type.
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    raise ReconcileError(f"unhandled fetched_at type: {type(value)!r}")


# --------------------------------------------------------------------------
# Object enumeration
# --------------------------------------------------------------------------

def _s3_client():
    import boto3

    from shared.minio import ACCESS, ENDPOINT, SECRET

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
    )


def _list_prefix(client, bucket: str, prefix: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    token: Optional[str] = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for entry in page.get("Contents", []):
            objects.append(entry)
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    return objects


def _list_common_prefixes(client, bucket: str, prefix: str) -> list[str]:
    """List only the immediate child "directories" of a prefix.

    Delimiter listing returns partition prefixes instead of every key beneath
    them. The archive root holds the whole HTML population, so walking it key
    by key to find one partition would list millions of objects to learn
    something three cheap requests can answer.
    """
    prefixes: list[str] = []
    token: Optional[str] = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for entry in page.get("CommonPrefixes", []):
            prefixes.append(entry["Prefix"])
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    return prefixes


def _discover_prefix(client, bucket: str) -> str:
    """Find the real detail_page partition prefix by listing, not by guessing.

    pyarrow writes int partition values unpadded ("month=4"), but this
    population is four months old and has been touched by later plans, so the
    spelling is confirmed against the bucket rather than assumed: a hardcoded
    guess that matched nothing would look like an empty population instead of a
    failure.

    The walk is level by level -- year, then month, then artifact_type -- so it
    costs three delimiter listings rather than a full enumeration of the
    archive.
    """
    root = f"{ARCHIVE_ROOT}/"

    def _match(prefixes: Sequence[str], field: str, want: Any) -> list[str]:
        matched = []
        for candidate in prefixes:
            leaf = candidate.rstrip("/").rsplit("/", 1)[-1]
            if not leaf.startswith(f"{field}="):
                continue
            value = leaf.split("=", 1)[1]
            if isinstance(want, int):
                try:
                    if int(value) != want:
                        continue
                except ValueError:
                    continue
            elif value != want:
                continue
            matched.append(candidate)
        return matched

    years = _match(_list_common_prefixes(client, bucket, root), "year", TARGET_YEAR)
    months: list[str] = []
    for year_prefix in years:
        months += _match(
            _list_common_prefixes(client, bucket, year_prefix), "month", TARGET_MONTH,
        )
    seen: list[str] = []
    for month_prefix in months:
        seen += _match(
            _list_common_prefixes(client, bucket, month_prefix),
            "artifact_type", TARGET_ARTIFACT_TYPE,
        )

    if not seen:
        raise ReconcileError(
            f"no {TARGET_ARTIFACT_TYPE} partition found for "
            f"{TARGET_YEAR}-{TARGET_MONTH:02d} under s3://{bucket}/{root}"
        )
    if len(set(seen)) > 1:
        raise ReconcileError(f"ambiguous target prefix: {sorted(set(seen))}")
    return seen[0]


def enumerate_objects(client, bucket: str, prefix: str) -> list[dict[str, Any]]:
    """Census the Parquet objects under the frozen prefix.

    Records the immutable source metadata Stage 6 regenerates its deletion
    manifest from: key, size, ETag and last-modified. Non-Parquet keys are
    excluded rather than counted, so a stray marker object cannot inflate the
    object count past its baseline.
    """
    objects = []
    for entry in _list_prefix(client, bucket, prefix):
        key = entry["Key"]
        if not key.endswith(".parquet"):
            continue
        objects.append({
            "legacy_object_key": key,
            "size_bytes": int(entry["Size"]),
            "etag": entry["ETag"].strip('"'),
            "last_modified": entry["LastModified"].astimezone(timezone.utc).isoformat(),
        })
    objects.sort(key=lambda row: row["legacy_object_key"])
    return objects


# --------------------------------------------------------------------------
# Row streaming
# --------------------------------------------------------------------------

def _s3_opener(bucket: str):
    """Return an opener that resolves a legacy object key to a binary stream.

    Uses boto3 into an in-memory buffer rather than ``s3fs``. The images this
    runs in ship different s3fs versions -- the processing image's constructor
    rejects ``endpoint_url`` outright -- and boto3 is present and identical in
    all of them. The whole object is buffered because ``ParquetFile`` needs a
    seekable stream and ``get_object`` bodies are not; at ~12.5 MB average that
    is cheaper than the row groups it is about to decompress anyway.
    """
    import io

    client = _s3_client()

    def _open(key: str):
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        return io.BytesIO(body)

    return _open


def iter_rows(
    bucket: str,
    objects: Sequence[dict[str, Any]],
    *,
    progress_every: int = 50,
    opener: Optional[Any] = None,
) -> Iterator[dict[str, Any]]:
    """Stream every row occurrence, one row group at a time.

    The HTML column is large_binary and the whole population is ~13.66 GiB, so
    row groups are read and released individually. Each row carries its
    ``(legacy_object_key, row_group, row_offset)`` locator, which is the
    coordinate Plan 145 uses to account for occurrences and to name a donor.

    ``opener`` exists so fixture Parquet on local disk drives this same loop in
    tests. The schema check and the row-group walk are the parts most likely to
    be wrong against a four-month-old population, so they should not be
    reachable only through a live MinIO.
    """
    import pyarrow.parquet as pq

    if opener is None:
        opener = _s3_opener(bucket)
    columns = METADATA_COLUMNS + [HTML_COLUMN]

    for index, obj in enumerate(objects, start=1):
        key = obj["legacy_object_key"]
        if progress_every and index % progress_every == 0:
            logger.info("scanning object %d/%d: %s", index, len(objects), key)
        with opener(key) as handle:
            parquet_file = pq.ParquetFile(handle)
            available = set(parquet_file.schema_arrow.names)
            missing = [c for c in columns if c not in available]
            if missing:
                raise ReconcileError(f"{key}: missing expected columns {missing}")
            for row_group in range(parquet_file.num_row_groups):
                table = parquet_file.read_row_group(row_group, columns=columns)
                batch = table.to_pydict()
                for offset in range(table.num_rows):
                    row = {name: batch[name][offset] for name in columns}
                    row["legacy_object_key"] = key
                    row["row_group"] = row_group
                    row["row_offset"] = offset
                    yield row
                del table, batch


def classify_row(row: dict[str, Any]) -> dict[str, Any]:
    """Turn one raw Parquet row into a manifest occurrence.

    Hashing is the expensive part and only non-empty bodies are hashed: an
    empty body's recomputed hash is a constant that carries no information and
    would join pack sidecars spuriously if it reached Stage 2.
    """
    html = row.get(HTML_COLUMN) or b""
    html_len = len(html)
    is_empty = html_len == 0
    stored = row.get("sha256")
    recomputed = None if is_empty else hashlib.sha256(html).hexdigest()

    return {
        "legacy_object_key": row["legacy_object_key"],
        "row_group": row["row_group"],
        "row_offset": row["row_offset"],
        "legacy_artifact_id": row.get("artifact_id"),
        "run_id": row.get("run_id"),
        "source": row.get("source"),
        "search_key": row.get("search_key"),
        "search_scope": row.get("search_scope"),
        "url": row.get("url"),
        "listing_id": extract_listing_id(row.get("url")),
        "fetched_at": _normalize_fetched_at(row.get("fetched_at")),
        "http_status": row.get("http_status"),
        "page_num": row.get("page_num"),
        "error": row.get("error"),
        "content_bytes": row.get("content_bytes"),
        "html_len": html_len,
        "stored_sha256": stored,
        "recomputed_sha256": recomputed,
        "is_empty": is_empty,
        "status_bucket": status_bucket(row.get("http_status")),
    }


# --------------------------------------------------------------------------
# Stage 1 aggregation and gates
# --------------------------------------------------------------------------

class CensusAccumulator:
    """Accumulates the Stage 1 census, manifests and gate evidence.

    Kept as a class so the fixture tests can drive it with synthetic rows and
    assert on the same gate logic the production scan runs, without a MinIO.

    Memory: one entry per HTTP 200 occurrence is retained (~848k), which is the
    working set Stage 1 has to hold to collapse duplicates and prove the
    identity invariant. Non-200 rows are counted and released.
    """

    def __init__(self, *, max_examples: int = 20) -> None:
        self.max_examples = max_examples
        self.total_rows = 0
        self.status_census: Counter[str] = Counter()
        # status x empty, because "successful" and "has bytes" are different
        # populations in this dataset and the plan reads as if they are not.
        self.empty_by_status: Counter[str] = Counter()
        self.missing_listing_id = 0
        self.missing_fetched_at = 0

        self.occurrences: list[dict[str, Any]] = []
        self.hash_mismatches: list[dict[str, Any]] = []
        self.empty_with_stored_hash = 0

    def add(self, occurrence: dict[str, Any]) -> None:
        self.total_rows += 1
        bucket = occurrence["status_bucket"]
        self.status_census[bucket] += 1
        if occurrence["is_empty"]:
            self.empty_by_status[bucket] += 1
            # Expected for this population: the Plan 72 writer stored the
            # database hash of a page whose bytes it could no longer read.
            if occurrence["stored_sha256"]:
                self.empty_with_stored_hash += 1
        elif occurrence["stored_sha256"] and (
            occurrence["stored_sha256"] != occurrence["recomputed_sha256"]
        ):
            if len(self.hash_mismatches) < self.max_examples:
                self.hash_mismatches.append({
                    "legacy_object_key": occurrence["legacy_object_key"],
                    "row_group": occurrence["row_group"],
                    "row_offset": occurrence["row_offset"],
                    "stored_sha256": occurrence["stored_sha256"],
                    "recomputed_sha256": occurrence["recomputed_sha256"],
                })
            else:
                self.hash_mismatches.append({"truncated": True})

        if bucket != "200":
            return

        if not occurrence["listing_id"]:
            self.missing_listing_id += 1
        if not occurrence["fetched_at"]:
            self.missing_fetched_at += 1
        self.occurrences.append(occurrence)

    # -- gates ------------------------------------------------------------

    def check_hashes(self) -> None:
        if self.hash_mismatches:
            shown = [m for m in self.hash_mismatches if not m.get("truncated")]
            raise ReconcileError(
                f"{len(self.hash_mismatches)} non-empty rows disagree with their "
                f"stored hash; first {len(shown)}: {json.dumps(shown, indent=2)}"
            )

    def collapse(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Collapse HTTP 200 occurrences to distinct observations.

        Identity is ``(listing_id, fetched_at)``. Occurrences sharing an
        identity must agree on recomputed hash; if they disagree there is no
        rule here that picks a donor, so the run stops. Empty-bodied rows
        collapse alongside their identity but can never become the donor: a
        donor has to supply bytes.
        """
        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        unidentified = 0
        for occurrence in self.occurrences:
            listing_id = occurrence["listing_id"]
            fetched_at = occurrence["fetched_at"]
            if not listing_id or not fetched_at:
                unidentified += 1
                continue
            groups[(listing_id, fetched_at)].append(occurrence)

        conflicts: list[dict[str, Any]] = []
        observations: list[dict[str, Any]] = []
        empty_only_identities = 0

        for (listing_id, fetched_at), members in groups.items():
            hashes = {m["recomputed_sha256"] for m in members if not m["is_empty"]}
            if len(hashes) > 1:
                if len(conflicts) < self.max_examples:
                    conflicts.append({
                        "listing_id": listing_id,
                        "fetched_at": fetched_at,
                        "recomputed_sha256": sorted(hashes),
                        "locators": [
                            [m["legacy_object_key"], m["row_group"], m["row_offset"]]
                            for m in members
                        ],
                    })
                else:
                    conflicts.append({"truncated": True})
                continue

            # Deterministic donor: the lowest locator among rows that actually
            # have bytes. Sorting makes reruns byte-identical.
            with_bytes = sorted(
                (m for m in members if not m["is_empty"]),
                key=lambda m: (m["legacy_object_key"], m["row_group"], m["row_offset"]),
            )
            if not with_bytes:
                # Successful capture whose bytes the archiver never captured.
                # Counted, never recovered -- Stage 4 would have nothing to write.
                empty_only_identities += 1
                continue

            donor = with_bytes[0]
            observations.append({
                "listing_id": listing_id,
                "fetched_at": fetched_at,
                "recomputed_sha256": donor["recomputed_sha256"],
                "stored_sha256": donor["stored_sha256"],
                "html_len": donor["html_len"],
                "http_status": donor["http_status"],
                "url": donor["url"],
                "run_id": donor["run_id"],
                "source": donor["source"],
                "search_key": donor["search_key"],
                "search_scope": donor["search_scope"],
                "page_num": donor["page_num"],
                "donor_legacy_object_key": donor["legacy_object_key"],
                "donor_row_group": donor["row_group"],
                "donor_row_offset": donor["row_offset"],
                "occurrence_count": len(members),
            })

        if conflicts:
            shown = [c for c in conflicts if not c.get("truncated")]
            raise ReconcileError(
                f"{len(conflicts)} (listing_id, fetched_at) identities disagree on "
                f"recomputed hash; this run does not select a donor. First "
                f"{len(shown)}: {json.dumps(shown, indent=2)}"
            )

        observations.sort(key=lambda row: (row["listing_id"], row["fetched_at"]))

        non_empty = [o for o in self.occurrences if not o["is_empty"]]
        stats = {
            "http_200_occurrences": len(self.occurrences),
            "http_200_occurrences_empty": self.empty_by_status.get("200", 0),
            "http_200_occurrences_with_bytes": len(non_empty),
            "unidentified_occurrences": unidentified,
            "distinct_identities": len(groups),
            "identities_with_bytes": len(observations),
            "identities_empty_only": empty_only_identities,
            "distinct_recomputed_sha256": len(
                {o["recomputed_sha256"] for o in non_empty}
            ),
            "distinct_stored_sha256": len(
                {o["stored_sha256"] for o in self.occurrences if o["stored_sha256"]}
            ),
            "duplicate_occurrences_collapsed": (
                len(self.occurrences) - unidentified - len(groups)
            ),
        }
        return observations, stats


def check_baseline(objects: Sequence[dict], accumulator: CensusAccumulator,
                   *, strict: bool) -> list[str]:
    """Compare the scan against Plan 145's published baseline.

    Returns the list of drifts. Under ``strict`` (the default) any drift stops
    the run, which is the Stage 1 gate: "all baseline counts reproduce exactly
    or the plan stops". ``--allow-drift`` downgrades it to a reported delta so
    a reviewer can see the whole picture before deciding.
    """
    drifts = []
    if len(objects) != BASELINE_OBJECTS:
        drifts.append(f"objects: expected {BASELINE_OBJECTS}, found {len(objects)}")
    if accumulator.total_rows != BASELINE_ROWS:
        drifts.append(f"rows: expected {BASELINE_ROWS}, found {accumulator.total_rows}")
    for bucket, expected in BASELINE_STATUS_CENSUS.items():
        found = accumulator.status_census.get(bucket, 0)
        if found != expected:
            drifts.append(f"status {bucket}: expected {expected}, found {found}")
    unexpected = set(accumulator.status_census) - set(BASELINE_STATUS_CENSUS)
    if unexpected:
        drifts.append(
            "unexpected status buckets: "
            + ", ".join(f"{b}={accumulator.status_census[b]}" for b in sorted(unexpected))
        )
    if drifts and strict:
        raise ReconcileError(
            "baseline drift; Stage 1 stops until each difference is explained:\n  - "
            + "\n  - ".join(drifts)
        )
    return drifts


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def _fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_csv(path: Path, fields: Sequence[str], rows: Sequence[dict]) -> str:
    """Write a manifest deterministically.

    ``\\n`` line endings and a fixed field order are what make the fingerprint
    reproducible across reruns and machines, which is the Stage 3 gate
    ("rerunning against the same inputs produces identical fingerprints").
    """
    with path.open("w", newline="\n", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(fields), extrasaction="ignore", lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return _fingerprint(path)


def write_outputs(
    out_dir: Path,
    *,
    objects: Sequence[dict],
    accumulator: CensusAccumulator,
    observations: Sequence[dict],
    stats: dict,
    drifts: Sequence[str],
    context: dict,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    occurrences = sorted(
        accumulator.occurrences,
        key=lambda row: (row["legacy_object_key"], row["row_group"], row["row_offset"]),
    )

    fingerprints = {
        "object_census.csv": _write_csv(
            out_dir / "object_census.csv",
            ["legacy_object_key", "size_bytes", "etag", "last_modified"],
            objects,
        ),
        "occurrences_http200.csv": _write_csv(
            out_dir / "occurrences_http200.csv", OCCURRENCE_FIELDS, occurrences,
        ),
        "observations_distinct.csv": _write_csv(
            out_dir / "observations_distinct.csv", OBSERVATION_FIELDS, observations,
        ),
    }

    report = {
        "stage": "plan_145_stage_1_census",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "context": context,
        "objects": {
            "count": len(objects),
            "stored_bytes": sum(o["size_bytes"] for o in objects),
            "baseline_count": BASELINE_OBJECTS,
        },
        "rows": {
            "total": accumulator.total_rows,
            "baseline_total": BASELINE_ROWS,
            "status_census": dict(sorted(accumulator.status_census.items())),
            "baseline_status_census": BASELINE_STATUS_CENSUS,
            "empty_by_status": dict(sorted(accumulator.empty_by_status.items())),
            "empty_rows_carrying_stored_hash": accumulator.empty_with_stored_hash,
        },
        "identity": {
            **stats,
            "http_200_missing_listing_id": accumulator.missing_listing_id,
            "http_200_missing_fetched_at": accumulator.missing_fetched_at,
        },
        "gates": {
            "hash_mismatches": len(accumulator.hash_mismatches),
            "baseline_drifts": list(drifts),
        },
        "fingerprints": fingerprints,
    }

    report_path = out_dir / "stage1_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n",
                           encoding="utf-8")
    report["fingerprints"]["stage1_report.json"] = _fingerprint(report_path)
    return report


def print_report(report: dict) -> None:
    rows = report["rows"]
    identity = report["identity"]
    print()
    print("Plan 145 Stage 1 — April detail-page census")
    print("=" * 62)
    print(f"objects            {report['objects']['count']:>12,}"
          f"  (baseline {report['objects']['baseline_count']:,})")
    print(f"stored bytes       {report['objects']['stored_bytes']:>12,}")
    print(f"row occurrences    {rows['total']:>12,}"
          f"  (baseline {rows['baseline_total']:,})")
    print()
    print("status census            rows      of which empty")
    for bucket in sorted(rows["status_census"]):
        print(f"  {bucket:<10} {rows['status_census'][bucket]:>12,}"
              f"  {rows['empty_by_status'].get(bucket, 0):>12,}")
    print(f"\nempty rows carrying a stored hash: "
          f"{rows['empty_rows_carrying_stored_hash']:,}")
    print("  (expected: the Plan 72 writer stored the DB hash of bytes it could")
    print("   not read; these are counted but are not recovery candidates)")
    print()
    print("identity")
    for key in ("http_200_occurrences", "http_200_occurrences_empty",
                "http_200_occurrences_with_bytes", "distinct_identities",
                "identities_with_bytes", "identities_empty_only",
                "distinct_recomputed_sha256", "distinct_stored_sha256",
                "duplicate_occurrences_collapsed", "unidentified_occurrences",
                "http_200_missing_listing_id", "http_200_missing_fetched_at"):
        print(f"  {key:<34} {identity[key]:>12,}")
    print()
    if report["gates"]["baseline_drifts"]:
        print("BASELINE DRIFT")
        for drift in report["gates"]["baseline_drifts"]:
            print(f"  - {drift}")
        print()
    print("fingerprints")
    for name, digest in sorted(report["fingerprints"].items()):
        print(f"  {name:<28} {digest}")
    print()


# --------------------------------------------------------------------------
# Stage 2 -- materialize successful bodies as normal HTML objects
# --------------------------------------------------------------------------

#: Where the per-source-file manifest shards land. One shard per legacy Parquet
#: object, so a crash loses only the in-flight file and a completed shard is
#: proof that source is done.
MATERIALIZE_PREFIX = "recovery/plan145/materialized"

#: Why a legacy row produced no object. Every HTTP 200 row is either `written`
#: or `exists`; the rest are recorded so all 951,821 occurrences stay accounted
#: for rather than silently vanishing from the population.
DISPOSITIONS = ("written", "exists", "skipped_empty", "skipped_non_success")

MANIFEST_FIELDS = [
    "object_key", "raw_sha256", "html_len", "compressed_len", "disposition",
    "legacy_object_key", "row_group", "row_offset",
    "listing_id", "fetched_at", "url", "http_status",
    "run_id", "source", "search_key", "search_scope", "page_num",
    "legacy_artifact_id",
]


def sha_to_file_id(sha256: str) -> str:
    """Derive a stable UUID-shaped object stem from a content hash.

    ``make_key`` generates a random UUID when none is given, which would make
    every re-run of this job write a second copy of the entire population. A
    content-derived stem makes the job idempotent instead: a rerun recomputes
    the same key, ``object_exists`` skips it, and two rows with identical bytes
    can never produce two objects.

    This is a deliberate departure from how the scraper names objects, and it
    is visible in the store permanently. Nothing reads meaning from the stem --
    lookups go through the queue and pack sidecars -- so the only effect is
    that these keys are reproducible and the scraper's are not.
    """
    if not sha256 or len(sha256) < 32:
        raise ReconcileError(f"cannot derive a file id from sha {sha256!r}")
    h = sha256[:32]
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def plan_row(occurrence: dict[str, Any]) -> dict[str, Any]:
    """Decide what, if anything, this legacy row should become.

    Pure and side-effect free so the disposition rules are testable without a
    MinIO. The actual write happens in :func:`materialize_row`.
    """
    from shared.minio import make_key

    record = {field: occurrence.get(field) for field in MANIFEST_FIELDS
              if field in occurrence}
    record.update({
        "object_key": None,
        "raw_sha256": occurrence.get("recomputed_sha256"),
        "compressed_len": None,
        "legacy_artifact_id": occurrence.get("legacy_artifact_id"),
    })

    if occurrence["is_empty"]:
        # The Plan 72 writer archived b"" for a page whose file was already
        # gone. There are no bytes to materialize; the row survives only as
        # metadata.
        record["disposition"] = "skipped_empty"
        return record

    if occurrence["status_bucket"] != "200":
        # Challenge pages and error bodies parse to a blocked/failed state and
        # yield no vehicle observation, so carrying them into the parse stage
        # costs storage and CPU for nothing. Recorded, not written.
        record["disposition"] = "skipped_non_success"
        return record

    record["object_key"] = make_key(
        TARGET_ARTIFACT_TYPE,
        occurrence["fetched_at"],
        file_id=sha_to_file_id(occurrence["recomputed_sha256"]),
    )
    record["disposition"] = "written"
    return record


def materialize_row(
    record: dict[str, Any],
    html: bytes,
    *,
    apply: bool,
    verify: bool = True,
) -> dict[str, Any]:
    """Write one body and prove it reads back byte-identically.

    Verification is a read-back through ``read_html`` -- the same path
    production uses -- rather than a comparison against the bytes still in
    memory. Checking what we just compressed against itself would prove
    nothing about what actually landed in the store.
    """
    from shared.minio import (
        BUCKET,
        object_exists,
        object_size,
        read_html,
        write_html,
    )

    if record["disposition"] != "written":
        return record

    key = record["object_key"]
    if object_exists(key):
        record["disposition"] = "exists"
        return record

    if not apply:
        return record

    write_html(key, html)

    if verify:
        readback = read_html(f"s3://{BUCKET}/{key}")
        actual = hashlib.sha256(readback).hexdigest()
        if actual != record["raw_sha256"]:
            raise ReconcileError(
                f"read-back mismatch for {key}: wrote {record['raw_sha256']}, "
                f"read {actual}"
            )
    record["compressed_len"] = object_size(f"s3://{BUCKET}/{key}")
    return record


def _shard_key(legacy_object_key: str) -> str:
    stem = legacy_object_key.rsplit("/", 1)[-1]
    if stem.endswith(".parquet"):
        stem = stem[: -len(".parquet")]
    return f"{MATERIALIZE_PREFIX}/{stem}.parquet"


def write_manifest_shard(records: Sequence[dict[str, Any]], shard_key: str) -> None:
    """Persist one source file's manifest to MinIO as Parquet.

    Parquet rather than CSV because the parse and compare stages read these
    with DuckDB; MinIO rather than a local directory because a disposable
    worker container's filesystem does not outlive the run.
    """
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    from shared.minio import write_bytes

    schema = pa.schema([
        pa.field("object_key", pa.string()),
        pa.field("raw_sha256", pa.string()),
        pa.field("html_len", pa.int64()),
        pa.field("compressed_len", pa.int64()),
        pa.field("disposition", pa.string()),
        pa.field("legacy_object_key", pa.string()),
        pa.field("row_group", pa.int32()),
        pa.field("row_offset", pa.int32()),
        pa.field("listing_id", pa.string()),
        pa.field("fetched_at", pa.string()),
        pa.field("url", pa.string()),
        pa.field("http_status", pa.int32()),
        pa.field("run_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("search_key", pa.string()),
        pa.field("search_scope", pa.string()),
        pa.field("page_num", pa.int32()),
        pa.field("legacy_artifact_id", pa.int64()),
    ])
    rows = [{f: r.get(f) for f in schema.names} for r in records]
    table = pa.Table.from_pylist(rows, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    write_bytes(shard_key, buf.getvalue(), content_type="application/octet-stream")


def run_materialize(args: argparse.Namespace) -> int:
    from shared.minio import BUCKET as DEFAULT_BUCKET
    from shared.minio import object_exists

    bucket = args.bucket or DEFAULT_BUCKET
    client = _s3_client()
    prefix = args.prefix or _discover_prefix(client, bucket)
    logger.info("frozen prefix: s3://%s/%s", bucket, prefix)

    objects = enumerate_objects(client, bucket, prefix)
    logger.info("enumerated %d Parquet objects", len(objects))
    if args.max_objects:
        objects = objects[: args.max_objects]
        logger.warning("--max-objects set: processing %d objects", len(objects))

    if not args.apply:
        logger.warning("DRY RUN: planning only, no object will be written. "
                       "Pass --apply to write.")

    totals: Counter[str] = Counter()
    done_shards = 0

    for index, obj in enumerate(objects, start=1):
        legacy_key = obj["legacy_object_key"]
        shard_key = _shard_key(legacy_key)

        if not args.force and object_exists(shard_key):
            done_shards += 1
            logger.debug("shard exists, skipping source %s", legacy_key)
            continue

        records: list[dict[str, Any]] = []
        for row in iter_rows(bucket, [obj], progress_every=0):
            occurrence = classify_row(row)
            record = plan_row(occurrence)
            html = row.get(HTML_COLUMN) or b""
            record = materialize_row(record, html, apply=args.apply,
                                     verify=not args.no_verify)
            totals[record["disposition"]] += 1
            records.append(record)

        if args.apply:
            write_manifest_shard(records, shard_key)

        if args.progress_every and index % args.progress_every == 0:
            logger.info("source %d/%d  %s", index, len(objects),
                        "  ".join(f"{k}={v:,}" for k, v in sorted(totals.items())))

    print()
    print("Plan 145 -- materialize April detail bodies")
    print("=" * 52)
    print(f"mode                {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"source objects      {len(objects):>12,}")
    print(f"shards already done {done_shards:>12,}")
    for disposition in DISPOSITIONS:
        print(f"  {disposition:<22} {totals.get(disposition, 0):>12,}")
    print(f"  {'total rows':<22} {sum(totals.values()):>12,}")
    print()
    return 0


# --------------------------------------------------------------------------
# Stage 3 helpers -- one flat prefix, shared by 3a and 3b
# --------------------------------------------------------------------------

def _april_pack_prefix() -> str:
    """The one pack prefix that could hold an April legacy detail capture.

    ``pack_lookup_prefix`` maps every ``html/year=2026/month=4/
    artifact_type=detail_page/`` key to exactly this prefix, so a content match
    against a sidecar here is a match against the store that would serve the
    read after a materialized twin is deleted.
    """
    from shared.packfile import PACK_PREFIX

    return (
        f"{PACK_PREFIX}/{TARGET_ARTIFACT_TYPE}/"
        f"{TARGET_YEAR:04d}/{TARGET_MONTH:02d}/"
    )


def _read_parquet_rows(
    client, bucket: str, key: str, *, columns: Optional[Sequence[str]] = None,
) -> list[dict[str, Any]]:
    import io

    import pyarrow.parquet as pq

    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(
        io.BytesIO(body), columns=list(columns) if columns else None,
    )
    return table.to_pylist()


def _write_parquet_shard(
    shard_key: str, schema: Any, records: Sequence[dict[str, Any]],
) -> None:
    """Persist one unit's manifest as Parquet, straight to MinIO.

    Parquet rather than CSV because the later stages read these with DuckDB;
    MinIO rather than local disk because a disposable worker container's
    filesystem does not outlive the run. Matches :func:`write_manifest_shard`.
    """
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    from shared.minio import write_bytes

    rows = [{name: r.get(name) for name in schema.names} for r in records]
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buf, compression="zstd")
    write_bytes(shard_key, buf.getvalue(), content_type="application/octet-stream")


# --------------------------------------------------------------------------
# Stage 3a -- delete materialized objects the April packs already hold
# --------------------------------------------------------------------------

#: Deletion manifest + receipts. The manifest for a source shard is written
#: before that shard's first delete, so an interrupted run still leaves a
#: complete record of what it intended to remove.
DEDUPE_PREFIX = "recovery/plan145/dedupe"

#: The only two dispositions that ever produced an object. ``skipped_empty`` and
#: ``skipped_non_success`` rows carry no ``object_key`` and can never be a
#: deletion candidate.
MATERIALIZED_DISPOSITIONS = ("written", "exists")

#: S3 ``DeleteObjects`` accepts at most this many keys per request.
MAX_DELETE_BATCH = 1000


def _deletion_schema() -> Any:
    import pyarrow as pa

    return pa.schema([
        pa.field("object_key", pa.string()),
        pa.field("raw_sha256", pa.string()),
        pa.field("legacy_object_key", pa.string()),
        pa.field("row_group", pa.int32()),
        pa.field("row_offset", pa.int32()),
        pa.field("claimed_by_sidecar", pa.string()),
        pa.field("claimed_by_source_key", pa.string()),
    ])


def _receipt_schema() -> Any:
    import pyarrow as pa

    return pa.schema([
        pa.field("object_key", pa.string()),
        pa.field("raw_sha256", pa.string()),
        pa.field("result", pa.string()),
    ])


def _list_keys(client, bucket: str, prefix: str, suffix: str) -> list[str]:
    return sorted(
        entry["Key"]
        for entry in _list_prefix(client, bucket, prefix)
        if entry["Key"].endswith(suffix)
    )


def load_sidecar_hashes(
    client, bucket: str, sidecar_keys: Sequence[str],
) -> dict[str, tuple[str, str]]:
    """Map every packed member's ``raw_sha256`` to the sidecar that holds it.

    Projects only ``raw_sha256`` and ``source_key`` -- ~557k members across 32
    sidecars, about a minute. No pack bytes are read and nothing is re-hashed:
    the sidecar ``raw_sha256`` is the value ``read_packed_html`` verifies every
    served read against, so its presence here is proof the content sits in a
    verified pack.

    When two members share a hash the lowest ``(sidecar_key, source_key)`` wins,
    so the deletion manifest names one claimant deterministically.
    """
    hashes: dict[str, tuple[str, str]] = {}
    for sidecar_key in sidecar_keys:
        rows = _read_parquet_rows(
            client, bucket, sidecar_key, columns=["raw_sha256", "source_key"],
        )
        for row in rows:
            sha = row.get("raw_sha256")
            if not sha:
                continue
            claim = (sidecar_key, row.get("source_key") or "")
            current = hashes.get(sha)
            if current is None or claim < current:
                hashes[sha] = claim
    return hashes


def plan_deletions(
    manifest_rows: Sequence[dict[str, Any]],
    sidecar_hashes: dict[str, tuple[str, str]],
) -> list[dict[str, Any]]:
    """Pick the materialized objects whose content an April pack already holds.

    Pure: a join of two written-down ``raw_sha256`` columns, nothing re-hashed.
    Only ``written`` / ``exists`` rows carry an ``object_key``; the two skipped
    dispositions are structurally ineligible and are never considered. The
    result is sorted and de-duplicated on ``object_key`` so identical bytes
    across two legacy rows plan exactly one deletion.
    """
    planned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in sorted(
        manifest_rows,
        key=lambda r: (
            r.get("object_key") or "",
            r.get("legacy_object_key") or "",
            r.get("row_group") or 0,
            r.get("row_offset") or 0,
        ),
    ):
        if row.get("disposition") not in MATERIALIZED_DISPOSITIONS:
            continue
        object_key = row.get("object_key")
        sha = row.get("raw_sha256")
        if not object_key or not sha or object_key in seen:
            continue
        if sha not in sidecar_hashes:
            continue
        seen.add(object_key)
        sidecar_key, source_key = sidecar_hashes[sha]
        planned.append({
            "object_key": object_key,
            "raw_sha256": sha,
            "legacy_object_key": row.get("legacy_object_key"),
            "row_group": row.get("row_group"),
            "row_offset": row.get("row_offset"),
            "claimed_by_sidecar": sidecar_key,
            "claimed_by_source_key": source_key,
        })
    return planned


def _dedupe_shard_key(materialize_shard_key: str) -> str:
    stem = materialize_shard_key.rsplit("/", 1)[-1]
    return f"{DEDUPE_PREFIX}/{stem}"


def _dedupe_receipt_key(materialize_shard_key: str) -> str:
    stem = materialize_shard_key.rsplit("/", 1)[-1]
    return f"{DEDUPE_PREFIX}/receipts/{stem}"


def delete_objects_in_batches(
    client,
    bucket: str,
    records: Sequence[dict[str, Any]],
    *,
    apply: bool,
    batch_size: int,
    verified_hashes: dict[str, Any],
) -> list[dict[str, Any]]:
    """Delete each planned key by exact name, in capped batches, one receipt per key.

    A record whose ``raw_sha256`` is not in ``verified_hashes`` is refused and
    the run stops: the manifest and the join share a source, but the guard
    means a hand-edited manifest cannot widen the blast radius. Nothing is ever
    deleted by prefix.
    """
    cap = max(1, min(batch_size, MAX_DELETE_BATCH))
    for record in records:
        if record.get("raw_sha256") not in verified_hashes:
            raise ReconcileError(
                f"refusing to delete {record.get('object_key')!r}: its content "
                f"{str(record.get('raw_sha256'))[:12]} is not in any April pack "
                f"sidecar"
            )

    receipts: list[dict[str, Any]] = []
    for start in range(0, len(records), cap):
        batch = records[start:start + cap]
        if not apply:
            receipts.extend(
                {"object_key": r["object_key"], "raw_sha256": r["raw_sha256"],
                 "result": "planned"}
                for r in batch
            )
            continue
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": r["object_key"]} for r in batch],
                    "Quiet": False},
        )
        errors = {
            e["Key"]: e.get("Code", "Unknown") for e in response.get("Errors", [])
        }
        deleted = {d["Key"] for d in response.get("Deleted", [])}
        for record in batch:
            key = record["object_key"]
            if key in errors:
                result = f"error:{errors[key]}"
            elif key in deleted:
                result = "deleted"
            else:
                result = "absent"
            receipts.append({
                "object_key": key,
                "raw_sha256": record["raw_sha256"],
                "result": result,
            })
    return receipts


def run_dedupe(args: argparse.Namespace) -> int:
    from shared.minio import BUCKET as DEFAULT_BUCKET
    from shared.minio import object_exists

    bucket = args.bucket or DEFAULT_BUCKET
    client = _s3_client()

    pack_prefix = args.pack_prefix or _april_pack_prefix()
    sidecar_keys = _list_keys(client, bucket, pack_prefix, ".idx.parquet")
    if not sidecar_keys:
        raise ReconcileError(f"no sidecars under s3://{bucket}/{pack_prefix}")
    logger.info("loading %d April sidecars from %s", len(sidecar_keys), pack_prefix)
    sidecar_hashes = load_sidecar_hashes(client, bucket, sidecar_keys)
    logger.info("indexed %d distinct packed content hashes", len(sidecar_hashes))

    shard_keys = _list_keys(
        client, bucket, MATERIALIZE_PREFIX + "/", ".parquet",
    )
    shard_keys = [k for k in shard_keys if "/" not in k[len(MATERIALIZE_PREFIX) + 1:]]
    if not shard_keys:
        raise ReconcileError(
            f"no materialize manifest shards under s3://{bucket}/{MATERIALIZE_PREFIX}/"
        )
    if args.max_shards:
        shard_keys = shard_keys[: args.max_shards]
        logger.warning("--max-shards set: %d shards; the rate gate is disabled",
                       len(shard_keys))

    if not args.apply:
        logger.warning("DRY RUN: planning only, nothing will be deleted. "
                       "Pass --apply to delete.")

    # -- plan phase: write every deletion manifest shard, delete nothing -------
    #: shard key -> the minimal (object_key, raw_sha256) pairs the delete phase
    #: needs. The full locator/claim columns live in the written manifest, not
    #: here -- ~371k rows held whole would be a few hundred MB for no reason.
    plan_by_shard: dict[str, list[dict[str, Any]]] = {}
    candidates = 0
    done_shards = 0
    for index, shard_key in enumerate(shard_keys, start=1):
        checkpoint = (
            _dedupe_receipt_key(shard_key) if args.apply
            else _dedupe_shard_key(shard_key)
        )
        if not args.force and object_exists(checkpoint):
            done_shards += 1
            continue

        rows = _read_parquet_rows(client, bucket, shard_key)
        candidates += len({
            r["object_key"] for r in rows
            if r.get("disposition") in MATERIALIZED_DISPOSITIONS and r.get("object_key")
        })
        planned = plan_deletions(rows, sidecar_hashes)
        _write_parquet_shard(_dedupe_shard_key(shard_key), _deletion_schema(), planned)
        plan_by_shard[shard_key] = [
            {"object_key": p["object_key"], "raw_sha256": p["raw_sha256"]}
            for p in planned
        ]

        if args.progress_every and index % args.progress_every == 0:
            logger.info("planned %d/%d shards, %d deletions so far",
                        index, len(shard_keys), sum(len(v) for v in plan_by_shard.values()))

    planned_total = sum(len(v) for v in plan_by_shard.values())
    rate = (planned_total / candidates) if candidates else 0.0
    lo = args.expect_rate - args.rate_tolerance
    hi = args.expect_rate + args.rate_tolerance
    gated = bool(candidates) and not args.max_shards
    off_band = gated and not (lo <= rate <= hi)

    print()
    print("Plan 145 Stage 3a -- delete materialized twins the packs already hold")
    print("=" * 70)
    print(f"mode                   {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"source shards          {len(shard_keys):>12,}")
    print(f"shards already done    {done_shards:>12,}")
    print(f"packed content hashes  {len(sidecar_hashes):>12,}")
    print(f"deletion candidates    {candidates:>12,}")
    print(f"planned for deletion   {planned_total:>12,}"
          f"   ({rate:.1%} of candidates)")
    print(f"expected rate          {args.expect_rate:.1%}"
          f" +/- {args.rate_tolerance:.1%}")
    print()

    if off_band:
        message = (
            f"deletion rate {rate:.1%} is outside the expected "
            f"{lo:.1%}-{hi:.1%} band; the sidecar join may be wrong -- "
            f"stopping before any delete"
        )
        if not args.allow_rate_drift:
            raise ReconcileError(message)
        logger.warning("%s (continuing: --allow-rate-drift)", message)

    if not args.apply:
        print(f"wrote {len(plan_by_shard)} deletion manifest shard(s); deleted nothing.")
        print()
        return 0

    # -- delete phase: exact keys, capped batches, a receipt shard per source --
    receipt_results: Counter[str] = Counter()
    deleted_total = 0
    for index, shard_key in enumerate(sorted(plan_by_shard), start=1):
        planned = plan_by_shard[shard_key]
        receipts = delete_objects_in_batches(
            client, bucket, planned,
            apply=True, batch_size=args.batch_size, verified_hashes=sidecar_hashes,
        )
        _write_parquet_shard(
            _dedupe_receipt_key(shard_key), _receipt_schema(), receipts,
        )
        for receipt in receipts:
            receipt_results[receipt["result"]] += 1
        deleted_total += sum(1 for r in receipts if r["result"] == "deleted")
        if args.progress_every and index % args.progress_every == 0:
            logger.info("deleted %d/%d shards, %d objects removed",
                        index, len(plan_by_shard), deleted_total)

    print("Plan 145 Stage 3a -- deletions applied")
    print("=" * 70)
    print(f"objects deleted        {deleted_total:>12,}")
    for result, count in sorted(receipt_results.items()):
        print(f"  receipt {result:<13} {count:>12,}")
    print()
    return 0


# --------------------------------------------------------------------------
# Stage 3b -- unpack every April pack member back to a loose object
# --------------------------------------------------------------------------

#: One manifest shard per source pack: an interrupted unpack loses only the
#: pack in flight, and a completed shard proves that pack is done.
UNPACK_PREFIX = "recovery/plan145/unpacked"

#: An unpacked member was either written now or already present.
UNPACK_DISPOSITIONS = ("written", "exists")


def _unpack_schema() -> Any:
    import pyarrow as pa

    return pa.schema([
        pa.field("source_key", pa.string()),
        pa.field("raw_sha256", pa.string()),
        pa.field("html_len", pa.int64()),
        pa.field("disposition", pa.string()),
        pa.field("pack_key", pa.string()),
        pa.field("frame_ordinal", pa.int32()),
        pa.field("offset_in_frame", pa.int64()),
        pa.field("artifact_id", pa.int64()),
        pa.field("listing_id", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ])


def _unpack_shard_key(pack_key: str) -> str:
    stem = pack_key.rsplit("/", 1)[-1]
    if stem.endswith(".zpack"):
        stem = stem[: -len(".zpack")]
    return f"{UNPACK_PREFIX}/{stem}.parquet"


def _ranged_pack_reader(client, bucket: str, pack_key: str):
    """A ``PackReader`` over the stored pack via ranged GETs, one frame cached.

    ``max_cached_frames=1`` is deliberate: members are walked in frame order,
    so a single cached frame turns a frame's whole run of ~1,000 members into
    one decompress. A larger cache would only retain frames already finished
    with.
    """
    from shared.packfile import PackReader

    size = client.head_object(Bucket=bucket, Key=pack_key)["ContentLength"]

    def fetch(offset: int, length: int) -> bytes:
        end = offset + length - 1
        return client.get_object(
            Bucket=bucket, Key=pack_key, Range=f"bytes={offset}-{end}"
        )["Body"].read()

    return PackReader(fetch, size, max_cached_frames=1)


def iter_members_by_frame(entries: Sequence[Any]) -> list[Any]:
    """Sidecar entries in ``(frame_ordinal, offset_in_frame)`` order.

    Reading members in this order means each ~16 MiB frame is decompressed
    exactly once for the members it serves. Sidecar order is usually already
    this; sorting makes the property hold regardless and makes a resumed run
    byte-for-byte deterministic.
    """
    return sorted(entries, key=lambda e: (e.frame_ordinal, e.offset_in_frame))


def unpack_member(
    reader, entry: Any, pack_key: str, *, apply: bool, verify: bool = True,
) -> dict[str, Any]:
    """Extract one member, prove its bytes, and write it under its original key.

    The sha256 is checked against the sidecar's ``raw_sha256`` before any write.
    The packer verified these at finalize time and ``read_packed_html`` verifies
    them on every read, so a disagreement here means the store moved under the
    run -- it stops, it is not a per-member skip.

    An existing key is left untouched, which is what makes the mode idempotent
    and resumable.
    """
    from shared.minio import object_exists, write_html

    record = {
        "source_key": entry.source_key,
        "raw_sha256": entry.raw_sha256,
        "html_len": entry.length,
        "disposition": None,
        "pack_key": pack_key,
        "frame_ordinal": entry.frame_ordinal,
        "offset_in_frame": entry.offset_in_frame,
        "artifact_id": entry.artifact_id,
        "listing_id": entry.listing_id,
        "fetched_at": entry.fetched_at,
    }

    if object_exists(entry.source_key):
        record["disposition"] = "exists"
        return record

    content = reader.read_member(entry)
    if verify:
        actual = hashlib.sha256(content).hexdigest()
        if actual != entry.raw_sha256:
            raise ReconcileError(
                f"pack member {entry.source_key} hashes to {actual[:12]}, its "
                f"sidecar says {entry.raw_sha256[:12]} -- the store moved; stopping"
            )
    record["html_len"] = len(content)

    if apply:
        write_html(entry.source_key, content)
    record["disposition"] = "written"
    return record


def run_unpack(args: argparse.Namespace) -> int:
    from shared.minio import BUCKET as DEFAULT_BUCKET
    from shared.minio import object_exists
    from shared.packfile import index_key, read_index_parquet

    bucket = args.bucket or DEFAULT_BUCKET
    client = _s3_client()

    pack_prefix = args.pack_prefix or _april_pack_prefix()
    pack_keys = _list_keys(client, bucket, pack_prefix, ".zpack")
    if not pack_keys:
        raise ReconcileError(f"no .zpack objects under s3://{bucket}/{pack_prefix}")
    if args.max_packs:
        pack_keys = pack_keys[: args.max_packs]
        logger.warning("--max-packs set: processing %d packs", len(pack_keys))

    if not args.apply:
        logger.warning("DRY RUN: reading and verifying every member, writing "
                       "nothing. Pass --apply to write.")

    totals: Counter[str] = Counter()
    members_total = 0
    done_packs = 0

    for pindex, pack_key in enumerate(pack_keys, start=1):
        shard_key = _unpack_shard_key(pack_key)
        if not args.force and object_exists(shard_key):
            done_packs += 1
            logger.debug("shard exists, skipping pack %s", pack_key)
            continue

        body = client.get_object(
            Bucket=bucket, Key=index_key(pack_key),
        )["Body"].read()
        entries = read_index_parquet(body)

        reader = _ranged_pack_reader(client, bucket, pack_key)
        if reader.member_count != len(entries):
            raise ReconcileError(
                f"{pack_key}: sidecar has {len(entries)} members, the pack header "
                f"says {reader.member_count}"
            )

        records: list[dict[str, Any]] = []
        for entry in iter_members_by_frame(entries):
            record = unpack_member(
                reader, entry, pack_key,
                apply=args.apply, verify=not args.no_verify,
            )
            totals[record["disposition"]] += 1
            members_total += 1
            records.append(record)

        if args.apply:
            _write_parquet_shard(shard_key, _unpack_schema(), records)

        if args.progress_every and pindex % args.progress_every == 0:
            logger.info(
                "pack %d/%d  %s", pindex, len(pack_keys),
                "  ".join(f"{k}={v:,}" for k, v in sorted(totals.items())),
            )

    print()
    print("Plan 145 Stage 3b -- unpack April pack members to loose objects")
    print("=" * 64)
    print(f"mode                {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"packs               {len(pack_keys):>12,}")
    print(f"packs already done  {done_packs:>12,}")
    for disposition in UNPACK_DISPOSITIONS:
        print(f"  {disposition:<16} {totals.get(disposition, 0):>12,}")
    print(f"  {'members total':<16} {members_total:>12,}")
    print()
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def run_census(args: argparse.Namespace) -> int:
    from shared.minio import BUCKET

    bucket = args.bucket or BUCKET
    client = _s3_client()

    prefix = args.prefix or _discover_prefix(client, bucket)
    logger.info("frozen prefix: s3://%s/%s", bucket, prefix)

    objects = enumerate_objects(client, bucket, prefix)
    logger.info("enumerated %d Parquet objects", len(objects))
    if args.max_objects:
        objects = objects[: args.max_objects]
        logger.warning("--max-objects set: scanning %d objects; counts will not "
                       "reproduce the baseline", len(objects))

    accumulator = CensusAccumulator(max_examples=args.max_examples)
    for row in iter_rows(bucket, objects, progress_every=args.progress_every):
        accumulator.add(classify_row(row))

    accumulator.check_hashes()
    drifts = check_baseline(
        objects, accumulator, strict=not (args.allow_drift or args.max_objects),
    )
    observations, stats = accumulator.collapse()

    report = write_outputs(
        Path(args.out_dir),
        objects=objects,
        accumulator=accumulator,
        observations=observations,
        stats=stats,
        drifts=drifts,
        context={
            "bucket": bucket,
            "prefix": prefix,
            "max_objects": args.max_objects,
            "allow_drift": bool(args.allow_drift),
            "git_commit": os.environ.get("GIT_COMMIT"),
        },
    )
    print_report(report)
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan 145 April detail-page reconciliation.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    census = sub.add_parser(
        "census",
        help="Stage 1: freeze the successful-capture manifest (read-only).",
    )
    census.add_argument("--out-dir", type=Path, required=True,
                        help="Local directory for manifests, report and fingerprints.")
    census.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    census.add_argument("--prefix", default=None,
                        help="Override the discovered detail_page prefix.")
    census.add_argument("--max-objects", type=int, default=0,
                        help="Scan only the first N objects (smoke test; "
                             "disables the baseline gate).")
    census.add_argument("--max-examples", type=int, default=20,
                        help="Bounded examples reported per gate failure.")
    census.add_argument("--progress-every", type=int, default=50,
                        help="Log progress every N objects.")
    census.add_argument("--allow-drift", action="store_true",
                        help="Report baseline drift instead of stopping. Use to "
                             "produce a delta report, never to proceed past a gate.")
    census.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    census.set_defaults(func=run_census)

    mat = sub.add_parser(
        "materialize",
        help="Write every successful legacy body as a normal .html.zst object.",
    )
    mat.add_argument("--apply", action="store_true",
                     help="Actually write objects. Without it the run plans "
                          "and reports only.")
    mat.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    mat.add_argument("--prefix", default=None,
                     help="Override the discovered detail_page prefix.")
    mat.add_argument("--max-objects", type=int, default=0,
                     help="Process only the first N source Parquet objects.")
    mat.add_argument("--force", action="store_true",
                     help="Reprocess source files whose manifest shard exists.")
    mat.add_argument("--no-verify", action="store_true",
                     help="Skip the read-back hash check. Not recommended.")
    mat.add_argument("--progress-every", type=int, default=10,
                     help="Log cumulative totals every N source objects.")
    mat.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    mat.set_defaults(func=run_materialize)

    ded = sub.add_parser(
        "dedupe",
        help="Stage 3a: delete materialized objects whose content an April pack "
             "already holds.",
    )
    ded.add_argument("--apply", action="store_true",
                     help="Actually delete. Without it the run plans, writes the "
                          "deletion manifests, and deletes nothing.")
    ded.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    ded.add_argument("--pack-prefix", default=None,
                     help="Override the derived April pack prefix.")
    ded.add_argument("--max-shards", type=int, default=0,
                     help="Process only the first N materialize manifest shards "
                          "(smoke test; disables the rate gate).")
    ded.add_argument("--batch-size", type=int, default=MAX_DELETE_BATCH,
                     help="Keys per delete request (S3 caps this at 1000).")
    ded.add_argument("--expect-rate", type=float, default=0.456,
                     help="Expected share of candidates already in the packs.")
    ded.add_argument("--rate-tolerance", type=float, default=0.10,
                     help="Half-width of the accepted band around --expect-rate.")
    ded.add_argument("--allow-rate-drift", action="store_true",
                     help="Report an out-of-band deletion rate instead of "
                          "stopping. Never use it to proceed past a broken join.")
    ded.add_argument("--force", action="store_true",
                     help="Reprocess shards whose receipt (apply) or deletion "
                          "manifest (dry run) already exists.")
    ded.add_argument("--progress-every", type=int, default=50,
                     help="Log progress every N shards.")
    ded.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    ded.set_defaults(func=run_dedupe)

    unp = sub.add_parser(
        "unpack",
        help="Stage 3b: write every April pack member back as a loose .html.zst "
             "object under its original key.",
    )
    unp.add_argument("--apply", action="store_true",
                     help="Actually write objects. Without it the run reads and "
                          "verifies every member but writes nothing.")
    unp.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    unp.add_argument("--pack-prefix", default=None,
                     help="Override the derived April pack prefix.")
    unp.add_argument("--max-packs", type=int, default=0,
                     help="Process only the first N packs.")
    unp.add_argument("--force", action="store_true",
                     help="Reprocess packs whose manifest shard already exists.")
    unp.add_argument("--no-verify", action="store_true",
                     help="Skip the per-member sha256 check. Not recommended.")
    unp.add_argument("--progress-every", type=int, default=1,
                     help="Log cumulative totals every N packs.")
    unp.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    unp.set_defaults(func=run_unpack)

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        return args.func(args)
    except ReconcileError as exc:
        logger.error("STOP: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
