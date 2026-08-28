"""Plan 145: the April detail-page reconciliation command.

Plan 145 deletes the 1,172 legacy April ``detail_page`` Parquet objects, but
only after every distinct successful capture is either already represented in
silver or rebuilt as a real current artifact. That is six stages of work over
one frozen population, so this is one command with one mode per stage rather
than six scripts that each re-derive the census.

Seven modes live here, one per stage of the plan's third revision (Stage 5
takes three), which *flattens* the population before parsing it:

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
* ``parse`` (Stage 4) -- parses the flattened population with the production
  parser into recovery-only Parquet, with identity provenance and one input
  audit row per object. It never writes Postgres or production silver.
* ``compare`` (Stage 5, slice 1) -- classifies every parsed observation against
  the deployed March-May silver on ``same listing_id AND abs(dt) <= 300s`` into
  ``already_represented`` / ``to_import`` / ``unclassifiable`` under
  ``recovery/plan145/compared/<run_id>/``, freezes the input inventory under
  ``recovery/plan145/inventory/<run_id>.json``, and writes one read-only VIN
  snapshot under ``recovery/plan145/vin_snapshot/<run_id>.parquet``. The only
  database statement it issues is that ``SELECT``; it writes no Postgres row and
  no production object. Slice 3 (canary and the live-state proof) is separate.
* ``assign`` (Stage 5, slice 2) -- gives every import-bearing source object one
  ``artifact_id``, preserved from its queue event where the normalized path has
  exactly one and otherwise allocated with
  ``nextval('ops.artifacts_queue_artifact_id_seq')``, and records it in an
  immutable per-batch shard under ``recovery/plan145/assigned/`` *before* any
  database insertion. ``nextval`` is the only statement it issues.
* ``apply`` (Stage 5, slice 2) -- writes one batch's silver rows, its historical
  ``detail`` price events at the legacy capture time, one ``recovered``
  artifact event per artifact, and the durable receipt, in **one transaction**
  on **one connection**. It never inserts into ``ops.artifacts_queue`` and never
  touches ``ops.price_observations``, ``ops.vin_to_listing``,
  ``ops.blocked_cooldown``, ``ops.detail_scrape_claims`` or live event emission.

Every mode from ``dedupe`` on defaults to a dry run and takes an explicit
``--apply``; between 3a and 3b the population becomes one flat prefix of distinct
captures that Stage 4 parses and Stage 5 compares. Only ``assign`` and ``apply``
write Postgres: every earlier mode is confined to MinIO, bar ``compare``'s one
read-only VIN ``SELECT``.

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
import concurrent.futures
import csv
import hashlib
import json
import logging
import multiprocessing
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, NamedTuple, Optional, Sequence

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
# Stage 4 -- parse the flattened population into recovery-only silver rows
# --------------------------------------------------------------------------

PARSED_PREFIX = "recovery/plan145/parsed"
QUEUE_EVENTS_PREFIX = "ops_normalized/artifacts_queue_events"
QUEUE_EVENT_MONTHS = (3, 4, 5)
_NOT_FOUND_CODES = ("404", "NoSuchKey", "NotFound")
EXPECTED_MATERIALIZED_SHARDS = 1172
EXPECTED_UNPACK_SHARDS = 32
EXPECTED_UNPACK_MEMBERS = 557065
EXPECTED_FLATTENED_INPUTS = 983043

SILVER_FIELDS = [
    "listing_id", "vin", "canonical_detail_url", "source", "listing_state",
    "fetched_at", "price", "make", "model", "trim", "year", "mileage",
    "msrp", "stock_type", "fuel_type", "body_style", "dealer_name",
    "dealer_zip", "customer_id", "seller_id", "dealer_street", "dealer_city",
    "dealer_state", "dealer_phone", "dealer_website", "dealer_cars_com_url",
    "dealer_rating", "financing_type", "seller_zip", "seller_customer_id",
    "page_number", "position_on_page", "trid", "isa_context", "body",
    "condition",
]

DEALER_FIELDS = [
    "dealer_name", "dealer_zip", "customer_id", "seller_id", "dealer_street",
    "dealer_city", "dealer_state", "dealer_phone", "dealer_website",
    "dealer_cars_com_url", "dealer_rating",
]

def _parsed_rows_schema() -> Any:
    import pyarrow as pa

    fields = [
        pa.field("listing_id", pa.string()),
        pa.field("vin", pa.string()),
        pa.field("canonical_detail_url", pa.string()),
        pa.field("source", pa.string()),
        pa.field("listing_state", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("price", pa.int32()),
        pa.field("make", pa.string()),
        pa.field("model", pa.string()),
        pa.field("trim", pa.string()),
        pa.field("year", pa.int16()),
        pa.field("mileage", pa.int32()),
        pa.field("msrp", pa.int32()),
        pa.field("stock_type", pa.string()),
        pa.field("fuel_type", pa.string()),
        pa.field("body_style", pa.string()),
        pa.field("dealer_name", pa.string()),
        pa.field("dealer_zip", pa.string()),
        pa.field("customer_id", pa.string()),
        pa.field("seller_id", pa.string()),
        pa.field("dealer_street", pa.string()),
        pa.field("dealer_city", pa.string()),
        pa.field("dealer_state", pa.string()),
        pa.field("dealer_phone", pa.string()),
        pa.field("dealer_website", pa.string()),
        pa.field("dealer_cars_com_url", pa.string()),
        pa.field("dealer_rating", pa.float32()),
        pa.field("financing_type", pa.string()),
        pa.field("seller_zip", pa.string()),
        pa.field("seller_customer_id", pa.string()),
        pa.field("page_number", pa.int16()),
        pa.field("position_on_page", pa.int16()),
        pa.field("trid", pa.string()),
        pa.field("isa_context", pa.string()),
        pa.field("body", pa.string()),
        pa.field("condition", pa.string()),
        pa.field("content_sha256", pa.string()),
        pa.field("object_key", pa.string()),
        pa.field("listing_id_source", pa.string()),
        pa.field("fetched_at_source", pa.string()),
        pa.field("legacy_object_key", pa.string()),
        pa.field("row_group", pa.int32()),
        pa.field("row_offset", pa.int32()),
        pa.field("legacy_artifact_id", pa.int64()),
    ]
    return pa.schema(fields)


def _parsed_inputs_schema() -> Any:
    import pyarrow as pa

    return pa.schema([
        pa.field("object_key", pa.string()),
        pa.field("raw_sha256", pa.string()),
        pa.field("html_len", pa.int64()),
        pa.field("size_band", pa.string()),
        pa.field("input_kind", pa.string()),
        pa.field("listing_id", pa.string()),
        pa.field("listing_id_source", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("fetched_at_source", pa.string()),
        pa.field("importable", pa.bool_()),
        pa.field("identity_disagreement", pa.bool_()),
        pa.field("outcome", pa.string()),
        pa.field("carousel_seen", pa.int32()),
        pa.field("carousel_emitted", pa.int32()),
        pa.field("carousel_drop_listing_id", pa.int32()),
        pa.field("carousel_drop_price", pa.int32()),
        pa.field("carousel_drop_body", pa.int32()),
        pa.field("error", pa.string()),
        pa.field("legacy_object_key", pa.string()),
        pa.field("row_group", pa.int32()),
        pa.field("row_offset", pa.int32()),
        pa.field("legacy_artifact_id", pa.int64()),
        pa.field("pack_key", pa.string()),
        pa.field("frame_ordinal", pa.int32()),
    ])


def size_band(size: Optional[int]) -> str:
    """Stable byte bands used to measure the short block-page cohort."""
    if size is None:
        return "unknown"
    if size < 512:
        return "000000-000511"
    if size < 1024:
        return "000512-001023"
    if size < 4096:
        return "001024-004095"
    if size < 16384:
        return "004096-016383"
    if size < 65536:
        return "016384-065535"
    return "065536+"


def _as_utc_datetime(value: Any) -> Optional[datetime]:
    """Convert manifest ISO strings to the timestamp type used by Parquet."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime):
        raise ReconcileError(f"unhandled parse fetched_at type: {type(value)!r}")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _bare_object_key(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if value.startswith("s3://"):
        rest = value[len("s3://"):]
        _, slash, key = rest.partition("/")
        return key if slash else None
    return value.lstrip("/")


def build_queue_identity(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Reduce queue events to the scraper identity recorded for each object key."""
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.get("artifact_type") != TARGET_ARTIFACT_TYPE:
            continue
        key = _bare_object_key(row.get("minio_path"))
        if not key:
            continue
        current = grouped.setdefault(key, {"listing_id": None, "fetched_at": None})
        listing_id = row.get("listing_id")
        if current["listing_id"] is None and listing_id is not None:
            current["listing_id"] = str(listing_id)
        fetched_at = row.get("fetched_at")
        if fetched_at is not None:
            normalized = _normalize_fetched_at(fetched_at)
            if current["fetched_at"] is None or normalized < current["fetched_at"]:
                current["fetched_at"] = normalized
    return grouped


def build_legacy_identity(
    materialized_rows: Sequence[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Index the trusted legacy identity by verified content hash."""
    result: dict[str, dict[str, Any]] = {}
    for row in materialized_rows:
        if row.get("disposition") not in MATERIALIZED_DISPOSITIONS:
            continue
        sha = row.get("raw_sha256")
        if not sha or not row.get("listing_id"):
            continue
        identity = {
            "listing_id": str(row["listing_id"]),
            "fetched_at": _normalize_fetched_at(row.get("fetched_at")),
        }
        previous = result.get(sha)
        if previous is not None and previous != identity:
            raise ReconcileError(
                f"legacy hash {sha[:12]} maps to conflicting identities: "
                f"{previous!r} and {identity!r}"
            )
        result[sha] = identity
    return result


def resolve_manifest_identity(
    record: dict[str, Any],
    legacy_identity: dict[str, dict[str, Any]],
    queue_identity: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Resolve tiers 1 and 2, preserving evidence of every disagreement."""
    legacy = legacy_identity.get(record.get("raw_sha256"))
    queue = queue_identity.get(record["object_key"])
    disagreement = bool(legacy and queue and (
        (
            queue.get("listing_id") is not None
            and legacy.get("listing_id") != queue.get("listing_id")
        ) or (
            queue.get("fetched_at") is not None
            and legacy.get("fetched_at") != queue.get("fetched_at")
        )
    ))
    if legacy:
        return {
            "listing_id": legacy.get("listing_id"),
            "listing_id_source": "legacy_manifest",
            "fetched_at": legacy.get("fetched_at"),
            "fetched_at_source": (
                "legacy_manifest" if legacy.get("fetched_at") else "none"
            ),
            "identity_disagreement": disagreement,
        }
    if queue:
        return {
            "listing_id": queue.get("listing_id"),
            "listing_id_source": "queue_events" if queue.get("listing_id") else "none",
            "fetched_at": queue.get("fetched_at"),
            "fetched_at_source": "queue_events" if queue.get("fetched_at") else "none",
            "identity_disagreement": False,
        }
    return {
        "listing_id": None,
        "listing_id_source": "none",
        "fetched_at": None,
        "fetched_at_source": "none",
        "identity_disagreement": False,
    }


def build_parse_units(
    materialized: Sequence[tuple[str, Sequence[dict[str, Any]]]],
    deleted_keys: set[str],
    unpacked: Sequence[tuple[str, Sequence[dict[str, Any]]]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Build materialized-minus-deleted union unpacked without listing HTML."""
    units: list[tuple[str, list[dict[str, Any]]]] = []
    seen: set[str] = set()
    for shard_key, rows in [*materialized, *unpacked]:
        unit_rows: list[dict[str, Any]] = []
        kind = "unpacked" if shard_key.startswith(UNPACK_PREFIX + "/") else "materialized"
        key_field = "source_key" if kind == "unpacked" else "object_key"
        for row in rows:
            key = row.get(key_field)
            if not key or key in seen:
                continue
            if kind == "materialized":
                if row.get("disposition") not in MATERIALIZED_DISPOSITIONS:
                    continue
                if key in deleted_keys:
                    continue
            elif row.get("disposition") not in UNPACK_DISPOSITIONS:
                continue
            seen.add(key)
            unit_rows.append({
                "object_key": key,
                "raw_sha256": row.get("raw_sha256"),
                "html_len": row.get("html_len"),
                "input_kind": kind,
                "legacy_object_key": row.get("legacy_object_key"),
                "row_group": row.get("row_group"),
                "row_offset": row.get("row_offset"),
                "legacy_artifact_id": row.get("legacy_artifact_id"),
                "pack_key": row.get("pack_key"),
                "frame_ordinal": row.get("frame_ordinal"),
            })
        stem = shard_key.rsplit("/", 1)[-1].removesuffix(".parquet")
        units.append((f"{kind}-{stem}", unit_rows))
    return units


def _base_input_record(record: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    return {
        **{name: record.get(name) for name in (
            "object_key", "raw_sha256", "html_len", "input_kind",
            "legacy_object_key", "row_group", "row_offset", "legacy_artifact_id",
            "pack_key", "frame_ordinal",
        )},
        "size_band": size_band(record.get("html_len")),
        "listing_id": identity["listing_id"],
        "listing_id_source": identity["listing_id_source"],
        "fetched_at": _as_utc_datetime(identity["fetched_at"]),
        "fetched_at_source": identity["fetched_at_source"],
        "importable": bool(identity["listing_id"] and identity["fetched_at"]),
        "identity_disagreement": identity["identity_disagreement"],
        "outcome": None,
        "carousel_seen": 0,
        "carousel_emitted": 0,
        "carousel_drop_listing_id": 0,
        "carousel_drop_price": 0,
        "carousel_drop_body": 0,
        "error": None,
    }


def _row_provenance(record: dict[str, Any], input_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "content_sha256": record.get("raw_sha256"),
        "object_key": record.get("object_key"),
        "listing_id_source": input_row["listing_id_source"],
        "fetched_at_source": input_row["fetched_at_source"],
        "legacy_object_key": record.get("legacy_object_key"),
        "row_group": record.get("row_group"),
        "row_offset": record.get("row_offset"),
        "legacy_artifact_id": record.get("legacy_artifact_id"),
    }


def build_observation_rows(
    record: dict[str, Any],
    identity: dict[str, Any],
    primary: dict[str, Any],
    carousel: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, Any]]:
    """Mirror detail_writer's silver rows, including all three carousel drops."""
    resolved = dict(identity)
    if not resolved["listing_id"] and primary.get("listing_id"):
        resolved["listing_id"] = str(primary["listing_id"])
        resolved["listing_id_source"] = "parsed_page"
    input_row = _base_input_record(record, resolved)
    input_row["importable"] = bool(resolved["listing_id"] and resolved["fetched_at"])
    provenance = _row_provenance(record, input_row)
    fetched_at = _as_utc_datetime(resolved.get("fetched_at"))
    listing_id = resolved.get("listing_id")
    canonical = (
        f"https://www.cars.com/vehicledetail/{listing_id}/" if listing_id else None
    )
    dealer = {name: primary.get(name) for name in DEALER_FIELDS}
    detail = {name: None for name in SILVER_FIELDS}
    detail.update({name: primary.get(name) for name in (
        "vin", "price", "make", "model", "trim", "year", "mileage", "msrp",
        "stock_type", "fuel_type", "body_style",
    )})
    detail.update({
        "listing_id": listing_id,
        "canonical_detail_url": canonical,
        "source": "detail",
        "listing_state": primary.get("listing_state") or "active",
        "fetched_at": fetched_at,
        **dealer,
        **provenance,
    })
    if detail["listing_state"] == "unlisted":
        detail["price"] = None
        detail["mileage"] = None

    rows = [detail]
    drops = {"listing_id": 0, "price": 0, "body": 0}
    for hint in carousel:
        if not hint.get("listing_id"):
            drops["listing_id"] += 1
            continue
        if hint.get("price") is None:
            drops["price"] += 1
            continue
        if not hint.get("body"):
            drops["body"] += 1
            continue
        hint_id = str(hint["listing_id"])
        row = {name: None for name in SILVER_FIELDS}
        row.update({name: hint.get(name) for name in (
            "price", "mileage", "body", "condition", "year",
        )})
        row.update({
            "listing_id": hint_id,
            "vin": None,
            "canonical_detail_url": hint.get("canonical_detail_url")
            or f"https://www.cars.com/vehicledetail/{hint_id}/",
            "source": "carousel",
            "listing_state": "active",
            "fetched_at": fetched_at,
            **dealer,
            **provenance,
        })
        rows.append(row)
    return rows, drops, input_row


def parse_one_input(
    record: dict[str, Any], identity: dict[str, Any], *, reader: Optional[Any] = None,
    parser: Optional[Any] = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read, verify and parse one object; a hash disagreement always escapes."""
    if reader is None:
        from shared.minio import read_html
        reader = read_html
    if parser is None:
        from processing.processors.parse_detail_page import parse_cars_detail_page_html_v1
        parser = parse_cars_detail_page_html_v1

    input_row = _base_input_record(record, identity)
    try:
        body = reader(record["object_key"])
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if isinstance(exc, (FileNotFoundError, KeyError)) or code in _NOT_FOUND_CODES:
            input_row["outcome"] = "missing_object"
            input_row["error"] = str(exc)
            return [], input_row
        input_row["outcome"] = "failed"
        input_row["error"] = f"read: {type(exc).__name__}: {exc}"
        return [], input_row

    actual = hashlib.sha256(body).hexdigest()
    if actual != record.get("raw_sha256"):
        raise ReconcileError(
            f"{record['object_key']} hashes to {actual[:12]}, manifest says "
            f"{str(record.get('raw_sha256'))[:12]} -- the store moved; stopping"
        )
    input_row["html_len"] = len(body)
    input_row["size_band"] = size_band(len(body))
    text = body.decode("utf-8", errors="replace")
    url = (
        f"https://www.cars.com/vehicledetail/{identity['listing_id']}/"
        if identity.get("listing_id_source") in ("legacy_manifest", "queue_events")
        and identity.get("listing_id") else None
    )
    try:
        primary, carousel, _meta = parser(text, url)
    except Exception as exc:
        input_row["outcome"] = "failed"
        input_row["error"] = f"parse: {type(exc).__name__}: {exc}"
        return [], input_row

    if primary.get("listing_state") == "blocked":
        input_row["outcome"] = "blocked_cloudflare"
        return [], input_row
    if primary.get("listing_state") == "active" and all(
        primary.get(name) is None for name in ("listing_id", "vin", "price", "make")
    ):
        input_row["outcome"] = "blocked_other"
        return [], input_row

    rows, drops, final_input = build_observation_rows(
        record, identity, primary, carousel,
    )
    final_input.update({
        "outcome": "parsed",
        "carousel_seen": len(carousel),
        "carousel_emitted": len(rows) - 1,
        "carousel_drop_listing_id": drops["listing_id"],
        "carousel_drop_price": drops["price"],
        "carousel_drop_body": drops["body"],
    })
    return rows, final_input


_PARSE_LEGACY_IDENTITY: dict[str, dict[str, Any]] = {}
_PARSE_QUEUE_IDENTITY: dict[str, dict[str, Any]] = {}


def _parse_worker_init() -> None:
    import shared.minio as minio

    minio._boto3_client = None
    minio.clear_pack_caches()


def _parse_output_key(kind: str, unit_name: str) -> str:
    return f"{PARSED_PREFIX}/{kind}/{unit_name}.parquet"


def _parse_unit(unit: tuple[str, list[dict[str, Any]]]) -> dict[str, Any]:
    unit_name, records = unit
    rows: list[dict[str, Any]] = []
    inputs: list[dict[str, Any]] = []
    for record in records:
        identity = resolve_manifest_identity(
            record, _PARSE_LEGACY_IDENTITY, _PARSE_QUEUE_IDENTITY,
        )
        parsed_rows, input_row = parse_one_input(record, identity)
        rows.extend(parsed_rows)
        inputs.append(input_row)
    _write_parquet_shard(_parse_output_key("rows", unit_name), _parsed_rows_schema(), rows)
    _write_parquet_shard(
        _parse_output_key("inputs", unit_name), _parsed_inputs_schema(), inputs,
    )
    return summarize_parse_unit(unit_name, inputs, row_count=len(rows))


def _load_manifest_family(
    client, bucket: str, prefix: str, *, columns: Optional[Sequence[str]] = None,
) -> list[tuple[str, list[dict[str, Any]]]]:
    keys = _list_keys(client, bucket, prefix + "/", ".parquet")
    keys = [key for key in keys if "/" not in key[len(prefix) + 1:]]
    return [
        (key, _read_parquet_rows(client, bucket, key, columns=columns))
        for key in keys
    ]


def _load_deleted_keys(client, bucket: str) -> set[str]:
    keys = _list_keys(client, bucket, DEDUPE_PREFIX + "/", ".parquet")
    keys = [key for key in keys if "/receipts/" not in key]
    deleted: set[str] = set()
    for key in keys:
        deleted.update(
            row["object_key"] for row in _read_parquet_rows(
                client, bucket, key, columns=["object_key"],
            ) if row.get("object_key")
        )
    return deleted


def _parquet_num_rows(client, bucket: str, key: str) -> int:
    import io

    import pyarrow.parquet as pq

    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.ParquetFile(io.BytesIO(body)).metadata.num_rows


def _load_queue_events(client, bucket: str) -> dict[str, dict[str, Any]]:
    identities: dict[str, dict[str, Any]] = {}
    year_prefix = f"{QUEUE_EVENTS_PREFIX}/year={TARGET_YEAR}/"
    month_prefixes = []
    for prefix in _list_common_prefixes(client, bucket, year_prefix):
        leaf = prefix.rstrip("/").rsplit("/", 1)[-1]
        if not leaf.startswith("month="):
            continue
        try:
            month = int(leaf.split("=", 1)[1])
        except ValueError:
            continue
        if month in QUEUE_EVENT_MONTHS:
            month_prefixes.append(prefix)
    if len(month_prefixes) != len(QUEUE_EVENT_MONTHS):
        raise ReconcileError(
            f"expected queue-event months {QUEUE_EVENT_MONTHS}, found {month_prefixes}"
        )
    for prefix in sorted(month_prefixes):
        for key in _list_keys(client, bucket, prefix, ".parquet"):
            shard = build_queue_identity(_read_parquet_rows(
                client, bucket, key,
                columns=["minio_path", "listing_id", "fetched_at", "artifact_type"],
            ))
            for object_key, identity in shard.items():
                current = identities.setdefault(
                    object_key, {"listing_id": None, "fetched_at": None},
                )
                if current["listing_id"] is None and identity["listing_id"] is not None:
                    current["listing_id"] = identity["listing_id"]
                fetched_at = identity["fetched_at"]
                if fetched_at is not None and (
                    current["fetched_at"] is None or fetched_at < current["fetched_at"]
                ):
                    current["fetched_at"] = fetched_at
    return identities


def check_parse_apply_gate(
    materialized_shards: int,
    unpacked: Sequence[tuple[str, Sequence[dict[str, Any]]]],
    input_count: int,
) -> None:
    """Refuse --apply until Stage 3b and the flattened census are complete."""
    unpack_members = sum(
        1 for _, rows in unpacked for row in rows
        if row.get("disposition") in UNPACK_DISPOSITIONS
    )
    failures = []
    if materialized_shards != EXPECTED_MATERIALIZED_SHARDS:
        failures.append(
            f"materialized shards {materialized_shards:,} "
            f"!= {EXPECTED_MATERIALIZED_SHARDS:,}"
        )
    if len(unpacked) != EXPECTED_UNPACK_SHARDS:
        failures.append(
            f"unpack shards {len(unpacked):,} != {EXPECTED_UNPACK_SHARDS:,}"
        )
    if unpack_members != EXPECTED_UNPACK_MEMBERS:
        failures.append(
            f"unpack members {unpack_members:,} != {EXPECTED_UNPACK_MEMBERS:,}"
        )
    if input_count != EXPECTED_FLATTENED_INPUTS:
        failures.append(
            f"flattened inputs {input_count:,} != {EXPECTED_FLATTENED_INPUTS:,}"
        )
    if failures:
        raise ReconcileError(
            "Stage 4 --apply gate failed (Stage 3b may still be running): "
            + "; ".join(failures)
        )


def aggregate_parse_report(
    results: Sequence[dict[str, Any]], *, planned_units: int,
) -> dict[str, Any]:
    outcomes: Counter[str] = Counter()
    listing_sources: Counter[str] = Counter()
    fetched_sources: Counter[str] = Counter()
    size_x_outcome: Counter[tuple[str, str]] = Counter()
    totals: Counter[str] = Counter()
    disagreements = 0
    for result in results:
        totals.update(result["totals"])
        outcomes.update(result["outcomes"])
        listing_sources.update(result["listing_id_sources"])
        fetched_sources.update(result["fetched_at_sources"])
        disagreements += result["identity_disagreements"]
        for band, by_outcome in result["size_band_by_outcome"].items():
            for outcome, count in by_outcome.items():
                size_x_outcome[(band, outcome)] += count
    return {
        "plan": 145,
        "stage": 4,
        "identity_lookup": "pyarrow in-process over queue-event months 3-5",
        "planned_units": planned_units,
        "completed_units": len(results),
        "totals": dict(totals),
        "outcomes": dict(sorted(outcomes.items())),
        "listing_id_sources": dict(sorted(listing_sources.items())),
        "fetched_at_sources": dict(sorted(fetched_sources.items())),
        "identity_disagreements": disagreements,
        "block_pages": {
            "cloudflare": outcomes["blocked_cloudflare"],
            "other": outcomes["blocked_other"],
            "total": outcomes["blocked_cloudflare"] + outcomes["blocked_other"],
        },
        "size_band_by_outcome": {
            band: {
                outcome: size_x_outcome[(band, outcome)]
                for outcome in sorted(outcomes)
                if size_x_outcome[(band, outcome)]
            }
            for band in sorted({band for band, _ in size_x_outcome})
        },
    }


def summarize_parse_unit(
    unit_name: str, inputs: Sequence[dict[str, Any]], *, row_count: int,
) -> dict[str, Any]:
    outcomes: Counter[str] = Counter()
    listing_sources: Counter[str] = Counter()
    fetched_sources: Counter[str] = Counter()
    size_x_outcome: Counter[tuple[str, str]] = Counter()
    totals: Counter[str] = Counter({"rows": row_count})
    disagreements = 0
    for row in inputs:
        outcomes[row["outcome"]] += 1
        listing_sources[row["listing_id_source"]] += 1
        fetched_sources[row["fetched_at_source"]] += 1
        size_x_outcome[(row["size_band"], row["outcome"])] += 1
        totals["inputs"] += 1
        totals["importable"] += int(row["importable"])
        totals["carousel_seen"] += row["carousel_seen"]
        totals["carousel_emitted"] += row["carousel_emitted"]
        totals["carousel_drop_listing_id"] += row["carousel_drop_listing_id"]
        totals["carousel_drop_price"] += row["carousel_drop_price"]
        totals["carousel_drop_body"] += row["carousel_drop_body"]
        disagreements += int(row["identity_disagreement"])
    return {
        "unit": unit_name,
        "totals": dict(totals),
        "outcomes": dict(outcomes),
        "listing_id_sources": dict(listing_sources),
        "fetched_at_sources": dict(fetched_sources),
        "identity_disagreements": disagreements,
        "size_band_by_outcome": {
            band: {
                outcome: size_x_outcome[(band, outcome)]
                for outcome in outcomes if size_x_outcome[(band, outcome)]
            }
            for band in {band for band, _ in size_x_outcome}
        },
    }


def run_parse(args: argparse.Namespace) -> int:
    global _PARSE_LEGACY_IDENTITY, _PARSE_QUEUE_IDENTITY

    from shared.minio import BUCKET as DEFAULT_BUCKET
    from shared.minio import object_exists, write_bytes

    bucket = args.bucket or DEFAULT_BUCKET
    client = _s3_client()
    materialized = _load_manifest_family(
        client, bucket, MATERIALIZE_PREFIX,
        columns=[
            "object_key", "raw_sha256", "html_len", "disposition",
            "legacy_object_key", "row_group", "row_offset", "legacy_artifact_id",
            "listing_id", "fetched_at",
        ],
    )
    unpacked = _load_manifest_family(
        client, bucket, UNPACK_PREFIX,
        columns=[
            "source_key", "raw_sha256", "html_len", "disposition", "pack_key",
            "frame_ordinal",
        ],
    )
    if not materialized or not unpacked:
        raise ReconcileError(
            f"parse requires both manifest families; found {len(materialized)} "
            f"materialized and {len(unpacked)} unpacked shards"
        )
    deleted_keys = _load_deleted_keys(client, bucket)
    units = build_parse_units(materialized, deleted_keys, unpacked)
    full_input_count = sum(len(rows) for _, rows in units)
    if args.apply:
        check_parse_apply_gate(len(materialized), unpacked, full_input_count)
    all_materialized_rows = [row for _, rows in materialized for row in rows]
    legacy_identity = build_legacy_identity(all_materialized_rows)
    queue_identity = _load_queue_events(client, bucket)
    if args.max_units:
        units = units[:args.max_units]

    planned_listing_sources: Counter[str] = Counter()
    planned_fetched_sources: Counter[str] = Counter()
    planned_disagreements = 0
    for _, records in units:
        for record in records:
            identity = resolve_manifest_identity(
                record, legacy_identity, queue_identity,
            )
            planned_listing_sources[identity["listing_id_source"]] += 1
            planned_fetched_sources[identity["fetched_at_source"]] += 1
            planned_disagreements += int(identity["identity_disagreement"])

    pending = [unit for unit in units if args.force or not (
        object_exists(_parse_output_key("rows", unit[0]))
        and object_exists(_parse_output_key("inputs", unit[0]))
    )]
    input_count = sum(len(rows) for _, rows in units)
    print()
    print("Plan 145 Stage 4 -- parse flattened April detail artifacts")
    print("=" * 66)
    print(f"mode                 {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"materialized shards  {len(materialized):>12,}")
    print(f"unpacked shards      {len(unpacked):>12,}")
    print(f"deleted keys         {len(deleted_keys):>12,}")
    print(f"input objects        {input_count:>12,}")
    print(f"work units           {len(units):>12,}")
    print(f"pending units        {len(pending):>12,}")
    print(f"queue identities     {len(queue_identity):>12,}")
    print(f"tier disagreements   {planned_disagreements:>12,}")
    for source, count in sorted(planned_listing_sources.items()):
        print(f"  listing {source:<19} {count:>12,}")
    for source, count in sorted(planned_fetched_sources.items()):
        print(f"  fetched {source:<19} {count:>12,}")
    if not args.apply:
        print("\nDRY RUN: manifests were measured; no HTML was read and nothing was written.")
        return 0

    _PARSE_LEGACY_IDENTITY = legacy_identity
    _PARSE_QUEUE_IDENTITY = queue_identity
    workers = args.workers or max(1, multiprocessing.cpu_count() - 2)
    results: list[dict[str, Any]] = []
    if pending:
        context = multiprocessing.get_context("fork")
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=workers, mp_context=context, initializer=_parse_worker_init,
        ) as executor:
            futures = {executor.submit(_parse_unit, unit): unit[0] for unit in pending}
            for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    results.append(future.result())
                except Exception as exc:
                    for other in futures:
                        other.cancel()
                    raise ReconcileError(
                        f"parse unit {futures[future]} failed; stopping: {exc}"
                    ) from exc
                if args.progress_every and index % args.progress_every == 0:
                    logger.info("completed %d/%d parse units", index, len(pending))

    # Include already-complete input shards in the aggregate report on resume.
    completed_names = {result["unit"] for result in results}
    for unit_name, _ in units:
        if unit_name in completed_names:
            continue
        input_key = _parse_output_key("inputs", unit_name)
        row_key = _parse_output_key("rows", unit_name)
        if object_exists(input_key) and object_exists(row_key):
            prior_inputs = _read_parquet_rows(client, bucket, input_key)
            results.append(
                summarize_parse_unit(
                    unit_name, prior_inputs,
                    row_count=_parquet_num_rows(client, bucket, row_key),
                )
            )
    report = aggregate_parse_report(results, planned_units=len(units))
    write_bytes(
        f"{PARSED_PREFIX}/parse_report.json",
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
        content_type="application/json",
    )
    print(f"\nparsed inputs        {report['totals'].get('inputs', 0):>12,}")
    print(f"observation rows     {report['totals'].get('rows', 0):>12,}")
    print(f"report               {PARSED_PREFIX}/parse_report.json")
    return 0


# --------------------------------------------------------------------------
# Stage 5 slice 1 -- compare parsed observations to the deployed silver
# --------------------------------------------------------------------------
#
# This slice writes no Postgres row and no production object. Its outputs land
# only under recovery/plan145/{compared,inventory,vin_snapshot}/. Classification
# is an *existence test*, not a join that picks a silver row: the plan's
# predicate is
#
#     same listing_id AND abs(silver.fetched_at - parsed.fetched_at) <= 300 s
#
# and source / vin / artifact_id / every parsed business value are explicitly
# *not* match keys. A parsed observation is:
#
#   * unclassifiable      -- fetched_at_source == 'none' (tier-3 identity, no
#                            capture time): it cannot be windowed and cannot be
#                            imported (silver.fetched_at is NOT NULL);
#   * already_represented -- at least one silver observation for the same
#                            listing lies within +/-300 s, from any source;
#   * to_import           -- zero candidates.
#
# The three families partition the parsed rows exactly -- that sum is what makes
# the plan's "classified exactly once" gate enforceable.

COMPARED_PREFIX = "recovery/plan145/compared"
INVENTORY_PREFIX = "recovery/plan145/inventory"
VIN_SNAPSHOT_PREFIX = "recovery/plan145/vin_snapshot"
#: --probe writes here instead; probe output is never promoted.
PROBE_SUFFIX = "_probe"

SILVER_PREFIX = "silver_normalized/observations"
SILVER_MONTHS = (3, 4, 5)

#: The plan's window, as microseconds. `<=` -- exactly 300 s is a match.
COMPARE_WINDOW_US = 300 * 1_000_000

#: The authoritative run refuses to start until Stage 4 reports all of these.
COMPARE_PLANNED_UNITS = 1204

#: Tier-3 identity yields ~760 pages with no capture time. Materially more than
#: this means something upstream is wrong; stop rather than proceed to slice 2.
UNCLASSIFIABLE_EXPECTATION = 760
MAX_UNCLASSIFIABLE = 2000

#: listing_ids per read-only ops.vin_to_listing SELECT.
VIN_BATCH = 1000

#: The business columns a recovery-duplicate collapse compares. Provenance,
#: object_key and content_sha256 are not in SILVER_FIELDS so they are already
#: out; vin is excluded because the parse stage deliberately leaves it NULL on
#: carousel rows.
FINGERPRINT_FIELDS = tuple(f for f in SILVER_FIELDS if f != "vin")


class DuplicateFingerprintConflict(ReconcileError):
    """Two parsed rows share ``(listing_id, fetched_at)`` but differ in business
    content. The plan says stop and let a human rule; do not pick a winner."""

    def __init__(self, conflicts: list[dict[str, Any]]) -> None:
        self.conflicts = conflicts
        super().__init__(
            f"{len(conflicts)} (listing_id, fetched_at) group(s) share the key but "
            f"differ in business fingerprint; compare stops without choosing a winner"
        )


class LiteDup(NamedTuple):
    """The minimum a global duplicate pass needs -- so the full rows can stay on
    local disk while the key/fingerprint grouping runs in memory."""

    uid: int
    listing_id: Optional[str]
    fetched_at: Optional[str]
    ts_us: Optional[int]
    fingerprint: str
    source: Optional[str]
    object_key: Optional[str]
    content_sha256: Optional[str]


# -- time --------------------------------------------------------------------

def _epoch_us(value: Any) -> Optional[int]:
    """Whole microseconds since the Unix epoch, by exact integer arithmetic.

    The comparison window is +/-300 s and the probe reported distances to the
    millisecond, so float epoch seconds are avoided: two ~1.75e15 microsecond
    values subtracted in float would lose the low bits.
    """
    if value is None:
        return None
    dt = value if isinstance(value, datetime) else _as_utc_datetime(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86_400_000_000
            + delta.seconds * 1_000_000
            + delta.microseconds)


# -- classification (existence test only) ----------------------------------

def match_within_window(target_us: Optional[int],
                        series: Sequence[tuple[Optional[int], str]]
                        ) -> list[tuple[int, str]]:
    """Silver events for one listing that fall within the +/-300 s window.

    Returns ``(abs_distance_us, source)`` pairs -- measurements, never a chosen
    row. The predicate is ``abs(delta) <= 300 s`` so an event exactly 300 s away
    is included.
    """
    if target_us is None:
        return []
    out: list[tuple[int, str]] = []
    for event_us, source in series:
        if event_us is None:
            continue
        dist = abs(event_us - target_us)
        if dist <= COMPARE_WINDOW_US:
            out.append((dist, source))
    return out


def classify_from_summary(parsed_row: dict[str, Any],
                          summary: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Decide the family from a match summary. One decision path for both the
    DuckDB scan and the pure-Python test helper.

    ``summary`` is ``{"match_count", "nearest_us", "match_sources"}`` or ``None``.
    No candidate identity or value is ever consulted -- only count and distance.

    ``unclassifiable`` covers *both* halves of a missing identity: a NULL
    ``listing_id`` (tier-2 resolved a capture time but no listing) cannot be
    windowed and ``staging.silver_observations.listing_id`` is NOT NULL, so such
    a row is no more importable than a tier-3 page with no capture time. The two
    are reasoned apart -- ``no_listing_id`` vs ``no_capture_time`` -- because the
    ~760 expectation was sized for the latter only. This mirrors
    ``build_observation_rows``'s ``importable = bool(listing_id and fetched_at)``.
    """
    if parsed_row.get("listing_id") is None:
        return {"family": "unclassifiable", "reason": "no_listing_id",
                "match_count": 0, "nearest_distance_s": None, "match_sources": []}
    if (parsed_row.get("fetched_at_source") == "none"
            or parsed_row.get("fetched_at") is None):
        return {"family": "unclassifiable", "reason": "no_capture_time",
                "match_count": 0, "nearest_distance_s": None, "match_sources": []}
    if not summary or not summary.get("match_count"):
        return {"family": "to_import", "reason": None,
                "match_count": 0, "nearest_distance_s": None, "match_sources": []}
    return {
        "family": "already_represented",
        "reason": "silver_candidate",
        "match_count": int(summary["match_count"]),
        "nearest_distance_s": round(int(summary["nearest_us"]) / 1_000_000, 6),
        "match_sources": sorted(summary["match_sources"]),
    }


def classify_parsed_observation(parsed_row: dict[str, Any],
                                silver_index: dict[str, Sequence[tuple[Optional[int], str]]]
                                ) -> dict[str, Any]:
    """Pure classifier over an in-memory silver index -- the shape the tests use.

    ``silver_index`` maps ``listing_id`` to a sequence of ``(event_us, source)``.
    """
    lid = parsed_row.get("listing_id")
    if (lid is None
            or parsed_row.get("fetched_at_source") == "none"
            or parsed_row.get("fetched_at") is None):
        return classify_from_summary(parsed_row, None)
    series = silver_index.get(str(lid), ())
    hits = match_within_window(_epoch_us(parsed_row.get("fetched_at")), series)
    if not hits:
        return classify_from_summary(parsed_row, None)
    return classify_from_summary(parsed_row, {
        "match_count": len(hits),
        "nearest_us": min(d for d, _ in hits),
        "match_sources": sorted({s for _, s in hits}),
    })


# -- global duplicate resolution (across shard boundaries) ----------------

def _fp_json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def business_fingerprint(row: dict[str, Any]) -> str:
    """A hash over the silver business columns, excluding provenance and vin."""
    payload = {name: row.get(name) for name in FINGERPRINT_FIELDS}
    blob = json.dumps(payload, sort_keys=True, default=_fp_json_default)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _dup_sort_key(rec: LiteDup) -> tuple[int, str, str]:
    # detail before carousel, then lowest object_key, then lowest content hash.
    return (0 if rec.source == "detail" else 1,
            rec.object_key or "", rec.content_sha256 or "")


def resolve_global_duplicates(
    records: Sequence[LiteDup],
) -> tuple[set[int], set[int], dict[str, Any]]:
    """Collapse identical unrepresented ``(listing_id, fetched_at)`` duplicates.

    Stage 4's work unit is one manifest shard, so the same key can appear in two
    shards; collapsing inside a shard would let a pair through and break the
    plan's zero-duplicate-write criterion. Resolution is therefore global.

    Returns ``(winner_uids, loser_uids, report)``. Raises
    :class:`DuplicateFingerprintConflict` when a key's members disagree on
    business content -- the plan forbids picking a winner there.
    """
    groups: dict[tuple[Optional[str], Optional[str]], list[LiteDup]] = defaultdict(list)
    for rec in records:
        groups[(rec.listing_id, rec.fetched_at)].append(rec)

    winner_uids: set[int] = set()
    loser_uids: set[int] = set()
    conflicts: list[dict[str, Any]] = []
    collapsed = 0
    for (listing_id, fetched_at), members in groups.items():
        if len(members) == 1:
            winner_uids.add(members[0].uid)
            continue
        fingerprints = {m.fingerprint for m in members}
        if len(fingerprints) > 1:
            conflicts.append({
                "listing_id": listing_id,
                "fetched_at": fetched_at,
                "fingerprints": sorted(fingerprints),
                "members": [
                    {"source": m.source, "object_key": m.object_key,
                     "content_sha256": m.content_sha256}
                    for m in sorted(members, key=lambda m: (m.object_key or "",
                                                            m.content_sha256 or ""))
                ],
            })
            continue
        collapsed += 1
        ordered = sorted(members, key=_dup_sort_key)
        winner_uids.add(ordered[0].uid)
        loser_uids.update(m.uid for m in ordered[1:])

    if conflicts:
        raise DuplicateFingerprintConflict(conflicts)

    return winner_uids, loser_uids, {
        "groups_collapsed": collapsed,
        "rows_moved_to_already_represented": len(loser_uids),
        "conflicting_fingerprint_groups": 0,
    }


def near_duplicate_window(
    pairs_source: Iterator[tuple[str, int]],
) -> dict[str, int]:
    """Count unrepresented same-listing captures within 300 s of the previous one.

    Representation uses a +/-300 s window but duplicate collapse uses an *exact*
    ``(listing_id, fetched_at)``, so two real captures of one listing 200 s
    apart both survive into ``to_import``. That is correct -- they are two real
    captures -- but the maintainer needs the number before slice 2 writes
    anything, so this measures it without collapsing anything.

    ``adjacent_pairs_within_300s`` is over *adjacent* captures in time order (the
    gap to the predecessor is ``0 < gap <= 300 s``), which is linear in a
    listing's capture count -- a listing with up to 980 captures, some bursting
    inside one window, must not cost a quadratic scan. A cluster of *k* captures
    inside one window contributes *k-1*, so this is a lower bound on
    ``captures_with_a_neighbour`` -- the count of captures with another within
    300 s on either side, which is the quantity the maintainer rules on.
    ``run_compare`` computes the same definitions with a window query over the
    temp shards rather than re-accumulating the population in memory.
    """
    by_listing: dict[str, list[int]] = defaultdict(list)
    for listing_id, ts_us in pairs_source:
        by_listing[listing_id].append(ts_us)
    pairs = 0
    listings = 0
    with_neighbour = 0
    for series in by_listing.values():
        series.sort()
        n = len(series)
        hit = False
        for i in range(n):
            prev_near = i > 0 and 0 < series[i] - series[i - 1] <= COMPARE_WINDOW_US
            next_near = i < n - 1 and 0 < series[i + 1] - series[i] <= COMPARE_WINDOW_US
            if prev_near:
                pairs += 1
                hit = True
            if prev_near or next_near:
                with_neighbour += 1
        if hit:
            listings += 1
    return {"adjacent_pairs_within_300s": pairs, "listings_involved": listings,
            "captures_with_a_neighbour": with_neighbour}


# -- the input freeze -----------------------------------------------------

def compute_run_id(inventory_core: dict[str, Any]) -> str:
    """A run id that changes iff any frozen input object changes.

    A changed inventory requires a new ``run_id`` and a complete re-compare;
    never a patch of one family in place.
    """
    blob = json.dumps(inventory_core, sort_keys=True, separators=(",", ":"))
    return "cmp-" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def check_compare_apply_gate(parse_report: dict[str, Any],
                             real_observation_total: int) -> None:
    """Refuse the authoritative run until Stage 4 is complete.

    This refusal is a feature of the slice, not an operational note: it reads
    ``parse_report.json`` but reproduces the observation total from the real
    ``rows/*.parquet`` row counts rather than trusting the report.
    """
    failures: list[str] = []
    completed = parse_report.get("completed_units")
    planned = parse_report.get("planned_units")
    if completed != COMPARE_PLANNED_UNITS or planned != COMPARE_PLANNED_UNITS:
        failures.append(
            f"parse units completed={completed} planned={planned}; both must "
            f"equal {COMPARE_PLANNED_UNITS}"
        )
    totals = parse_report.get("totals") or {}
    if totals.get("inputs") != EXPECTED_FLATTENED_INPUTS:
        failures.append(
            f"parsed inputs {totals.get('inputs')} != {EXPECTED_FLATTENED_INPUTS}"
        )
    if totals.get("rows") != real_observation_total:
        failures.append(
            f"parse_report rows total {totals.get('rows')} disagrees with the "
            f"summed rows/*.parquet row counts {real_observation_total}"
        )
    if failures:
        raise ReconcileError(
            "Stage 5 compare --apply gate failed (is Stage 4 complete?): "
            + "; ".join(failures)
        )


def _object_inventory(client, bucket: str, prefix: str, suffix: str
                      ) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entry in _list_prefix(client, bucket, prefix):
        key = entry["Key"]
        if not key.endswith(suffix):
            continue
        out.append({
            "key": key,
            "size": int(entry["Size"]),
            "etag": entry["ETag"].strip('"'),
        })
    out.sort(key=lambda r: r["key"])
    return out


def _silver_source(key: str) -> Optional[str]:
    m = re.search(r"/source=([^/]+)/", key)
    return m.group(1) if m else None


def _discover_silver_objects(client, bucket: str,
                             silver_prefix: Optional[str] = None
                             ) -> list[dict[str, Any]]:
    """The March-May silver objects, read as named objects -- never a dbt view.

    ``source`` is a hive partition column, encoded in the key path and absent
    from the file, so it is recorded here per object.
    """
    root = (silver_prefix or SILVER_PREFIX).rstrip("/") + "/"
    out: list[dict[str, Any]] = []
    for source_prefix in _list_common_prefixes(client, bucket, root):
        leaf = source_prefix.rstrip("/").rsplit("/", 1)[-1]
        if not leaf.startswith("source="):
            continue
        for month_prefix in _list_common_prefixes(
            client, bucket, f"{source_prefix}obs_year={TARGET_YEAR}/",
        ):
            m_leaf = month_prefix.rstrip("/").rsplit("/", 1)[-1]
            if not m_leaf.startswith("obs_month="):
                continue
            try:
                month = int(m_leaf.split("=", 1)[1])
            except ValueError:
                continue
            if month not in SILVER_MONTHS:
                continue
            for entry in _list_prefix(client, bucket, month_prefix):
                if not entry["Key"].endswith(".parquet"):
                    continue
                out.append({
                    "key": entry["Key"],
                    "size": int(entry["Size"]),
                    "etag": entry["ETag"].strip('"'),
                    "source": _silver_source(entry["Key"]),
                })
    out.sort(key=lambda r: r["key"])
    if not out:
        raise ReconcileError(
            f"no silver objects under s3://{bucket}/{root} for months {SILVER_MONTHS}"
        )
    return out


def _discover_queue_event_objects(client, bucket: str) -> list[dict[str, Any]]:
    """The March-May artifact-event objects -- frozen for slice 2's identity
    step, not consumed here."""
    out: list[dict[str, Any]] = []
    for prefix in _list_common_prefixes(
        client, bucket, f"{QUEUE_EVENTS_PREFIX}/year={TARGET_YEAR}/",
    ):
        leaf = prefix.rstrip("/").rsplit("/", 1)[-1]
        if not leaf.startswith("month="):
            continue
        try:
            month = int(leaf.split("=", 1)[1])
        except ValueError:
            continue
        if month not in QUEUE_EVENT_MONTHS:
            continue
        for entry in _list_prefix(client, bucket, prefix):
            if entry["Key"].endswith(".parquet"):
                out.append({
                    "key": entry["Key"],
                    "size": int(entry["Size"]),
                    "etag": entry["ETag"].strip('"'),
                })
    out.sort(key=lambda r: r["key"])
    return out


def _compare_unit_name(row_shard_key: str) -> str:
    return row_shard_key.rsplit("/", 1)[-1].removesuffix(".parquet")


def _compare_run_complete(run_dir: str, inv_key: str) -> bool:
    """A run_id is finished once its inventory freeze and its report both exist.

    Per-family per-unit shards are not a completeness signal: a unit can
    legitimately contribute zero rows to a family.
    """
    from shared.minio import object_exists

    return object_exists(inv_key) and object_exists(f"{run_dir}/compare_report.json")


# -- output schemas -----------------------------------------------------

def _compared_schema(family: str) -> Any:
    import pyarrow as pa

    fields = list(_parsed_rows_schema())
    fields.append(pa.field("reason", pa.string()))
    if family == "already_represented":
        fields.append(pa.field("match_count", pa.int32()))
        fields.append(pa.field("nearest_distance_s", pa.float64()))
        fields.append(pa.field("match_sources", pa.list_(pa.string())))
    return pa.schema(fields)


def _toimport_temp_schema() -> Any:
    import pyarrow as pa

    fields = list(_parsed_rows_schema())
    fields.append(pa.field("reason", pa.string()))
    fields.append(pa.field("_uid", pa.int64()))
    fields.append(pa.field("_fp", pa.string()))
    return pa.schema(fields)


def _emit_compare_shard(run_dir: str, family: str, unit: str, schema: Any,
                        rows: Sequence[dict[str, Any]], *, apply: bool) -> None:
    if not apply or not rows:
        return
    from shared.minio import object_exists

    key = f"{run_dir}/{family}/{unit}.parquet"
    if object_exists(key):
        return
    _write_parquet_shard(key, schema, rows)


# -- the read-only VIN snapshot --------------------------------------------

def _valid_uuid_ids(values: Iterator[Any]) -> list[str]:
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        text = str(value)
        if _UUID_RE.fullmatch(text):
            seen.add(text)
    return sorted(seen)


def snapshot_vin_lookup(listing_ids: Sequence[Any], cursor, *,
                        batch_size: int = VIN_BATCH) -> dict[str, Any]:
    """Read ``ops.vin_to_listing`` for the distinct parsed listing ids.

    Read-only, by design: the only statement issued is the ``SELECT`` from
    ``processing/sql/batch_lookup_vin_to_listing.sql``. A parsed VIN that
    collides with current hot state is reported by the caller and never causes a
    delete, a remap or an exclusion.
    """
    ids = _valid_uuid_ids(iter(listing_ids))
    out: dict[str, Any] = {}
    for start in range(0, len(ids), max(1, batch_size)):
        batch = ids[start:start + max(1, batch_size)]
        cursor.execute(
            "SELECT listing_id, vin FROM ops.vin_to_listing "
            "WHERE listing_id = ANY(%(listing_ids)s::uuid[])",
            {"listing_ids": batch},
        )
        for row in cursor.fetchall():
            out[str(row[0])] = row[1]
    return out


def _write_vin_snapshot(key: str, vin_map: dict[str, Any], *, apply: bool
                        ) -> dict[str, Any]:
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    from shared.minio import write_bytes

    schema = pa.schema([pa.field("listing_id", pa.string()),
                        pa.field("vin", pa.string())])
    rows = [{"listing_id": k, "vin": v} for k, v in sorted(vin_map.items())]
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buf, compression="zstd")
    data = buf.getvalue()
    info = {"key": key, "rows": len(rows), "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest()}
    if apply:
        write_bytes(key, data, content_type="application/octet-stream")
    return info


# -- the silver index (DuckDB, thread- and memory-bounded, disk-backed) ----

def _open_compare_duckdb(db_path: Path, *, threads: int = 1,
                         memory_limit: Optional[str] = "2GB"):
    """A disk-backed DuckDB connection with an explicit thread and memory cap.

    The silver side filters to only the listings the parsed population asks
    about, but with ~1M distinct parsed listings against 20.7M silver rows that
    still materialises most of the table plus a ``listing_id`` index. An
    in-memory connection on a 4-core host production is also using is a real
    risk, so the connection is a file with a spill path and a ceiling.
    """
    import duckdb

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={max(1, threads)}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    return con


def _load_silver_index(con, source_paths: Sequence[tuple[str, Optional[str]]],
                       wanted: set[str]) -> None:
    """Materialise ``silver(listing_id, ts_us, source)`` on ``con``, filtered to
    the wanted listings, with an index on ``listing_id`` for the per-unit
    probes."""
    import pyarrow as pa

    selects = []
    for path, source in source_paths:
        literal_path = path.replace("'", "''")
        literal_source = (source or "").replace("'", "''")
        selects.append(
            "SELECT CAST(listing_id AS VARCHAR) AS listing_id, "
            "epoch_us(fetched_at) AS ts_us, "
            f"'{literal_source}' AS source "
            f"FROM read_parquet('{literal_path}')"
        )
    union_sql = " UNION ALL ".join(selects)
    con.register("wanted", pa.table({"listing_id": pa.array(sorted(wanted), pa.string())}))
    con.execute(
        f"CREATE TABLE silver AS SELECT s.* FROM ({union_sql}) s "
        "WHERE s.ts_us IS NOT NULL AND s.listing_id IS NOT NULL "
        "AND s.listing_id IN (SELECT listing_id FROM wanted)"
    )
    con.unregister("wanted")
    con.execute("CREATE INDEX silver_listing_idx ON silver(listing_id)")


def _summarise_unit(con, prows: Sequence[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Per parsed row: silver candidate count, nearest distance, and the
    distinct candidate sources -- as measurements, never a chosen row."""
    import pyarrow as pa

    ids = [str(r["listing_id"]) if r.get("listing_id") else None for r in prows]
    ts = [_epoch_us(r.get("fetched_at")) for r in prows]
    con.register("parsed_unit", pa.table({
        "rid": pa.array(range(len(prows)), pa.int64()),
        "listing_id": pa.array(ids, pa.string()),
        "ts_us": pa.array(ts, pa.int64()),
    }))
    try:
        rows = con.execute(
            "SELECT p.rid, count(*) AS mc, "
            "       min(abs(s.ts_us - p.ts_us)) AS nearest_us, "
            "       list(DISTINCT s.source) AS srcs "
            "FROM parsed_unit p JOIN silver s ON s.listing_id = p.listing_id "
            "WHERE p.ts_us IS NOT NULL "
            f"  AND abs(s.ts_us - p.ts_us) <= {COMPARE_WINDOW_US} "
            "GROUP BY p.rid"
        ).fetchall()
    finally:
        con.unregister("parsed_unit")
    return {
        rid: {"match_count": mc, "nearest_us": nearest_us,
              "match_sources": list(srcs or [])}
        for rid, mc, nearest_us, srcs in rows
    }


def _toimport_glob(toimport_dir: Path) -> Optional[str]:
    """The read_parquet glob for the temp to_import shards, or None if empty."""
    if not any(toimport_dir.glob("*.parquet")):
        return None
    return str(toimport_dir / "*.parquet").replace("'", "''")


def _resolve_toimport_duplicates(
    con, toimport_dir: Path,
) -> tuple[set[int], dict[str, Any]]:
    """Collapse identical unrepresented ``(listing_id, fetched_at)`` duplicates
    across shard boundaries, reading the temp shards rather than an in-memory
    list.

    DuckDB groups the shards and hands only the multi-member groups to the pure,
    tested :func:`resolve_global_duplicates`; singletons never leave the scan.
    Returns ``(loser_uids, report)``; ``report["conflicts"]`` is populated when a
    key's members disagree on business fingerprint.
    """
    glob = _toimport_glob(toimport_dir)
    if glob is None:
        return set(), {"groups_collapsed": 0,
                       "rows_moved_to_already_represented": 0, "conflicts": []}
    # One `list(struct_pack(...))` rather than five parallel `list()` aggregates
    # zipped positionally: DuckDB does not document that separate list aggregates
    # order their elements identically within a group, and the scan can be
    # parallel (--duckdb-threads). A mis-zip would silently pick the wrong
    # duplicate winner; a struct keeps the columns bound to their row.
    groups = con.execute(
        "SELECT listing_id, fetched_at, "
        "  list(struct_pack(uid := _uid, fp := _fp, src := source, "
        "                   ok := object_key, h := content_sha256)) AS members "
        f"FROM read_parquet('{glob}') "
        "GROUP BY listing_id, fetched_at HAVING count(*) > 1"
    ).fetchall()
    multi: list[LiteDup] = []
    for listing_id, fetched_at, members in groups:
        key_time = _normalize_fetched_at(fetched_at) if fetched_at is not None else None
        for m in members:
            multi.append(LiteDup(int(m["uid"]), listing_id, key_time, None,
                                 m["fp"], m["src"], m["ok"], m["h"]))
    try:
        _, loser_uids, report = resolve_global_duplicates(multi)
    except DuplicateFingerprintConflict as exc:
        return set(), {"groups_collapsed": 0,
                       "rows_moved_to_already_represented": 0,
                       "conflicts": exc.conflicts}
    report["conflicts"] = []
    return loser_uids, report


def _measure_near_duplicates(
    con, toimport_dir: Path, loser_uids: set[int],
) -> dict[str, int]:
    """The near-duplicate window, as a linear gap scan over the temp shards.

    ``adjacent_pairs_within_300s`` counts consecutive captures whose gap is
    ``0 < gap <= 300 s`` -- a cluster of *k* captures inside one window is *k-1*,
    not ``k(k-1)/2``. ``captures_with_a_neighbour`` is the count of unrepresented
    captures that have another within 300 s on either side; that is the quantity
    the maintainer rules on. Recovery duplicates are excluded so a collapsed
    pair is not counted as a near one.
    """
    import pyarrow as pa

    empty = {"adjacent_pairs_within_300s": 0, "listings_involved": 0,
             "captures_with_a_neighbour": 0}
    glob = _toimport_glob(toimport_dir)
    if glob is None:
        return empty
    exclude = ""
    if loser_uids:
        con.register(
            "_losers",
            pa.table({"uid": pa.array(sorted(loser_uids), pa.int64())}),
        )
        exclude = " WHERE _uid NOT IN (SELECT uid FROM _losers)"
    win = COMPARE_WINDOW_US
    try:
        pairs, listings, with_neighbour = con.execute(
            "SELECT "
            f"  count(*) FILTER (WHERE gap_prev > 0 AND gap_prev <= {win}), "
            "  count(DISTINCT listing_id) FILTER "
            f"    (WHERE gap_prev > 0 AND gap_prev <= {win}), "
            f"  count(*) FILTER (WHERE (gap_prev > 0 AND gap_prev <= {win}) "
            f"                      OR (gap_next > 0 AND gap_next <= {win})) "
            "FROM ("
            "  SELECT listing_id, "
            "    epoch_us(fetched_at) - lag(epoch_us(fetched_at)) OVER w AS gap_prev, "
            "    lead(epoch_us(fetched_at)) OVER w - epoch_us(fetched_at) AS gap_next "
            f"  FROM read_parquet('{glob}'){exclude} "
            "  WINDOW w AS (PARTITION BY listing_id ORDER BY fetched_at)"
            ")"
        ).fetchone()
    finally:
        if loser_uids:
            con.unregister("_losers")
    return {"adjacent_pairs_within_300s": int(pairs or 0),
            "listings_involved": int(listings or 0),
            "captures_with_a_neighbour": int(with_neighbour or 0)}


def run_compare(args: argparse.Namespace) -> int:
    import tempfile

    import pyarrow as pa
    import pyarrow.parquet as pq

    from shared.minio import BUCKET as DEFAULT_BUCKET
    from shared.minio import read_json, write_bytes

    bucket = args.bucket or DEFAULT_BUCKET
    client = _s3_client()
    probe = bool(args.probe)
    apply = bool(args.apply)

    compared_root = COMPARED_PREFIX + (PROBE_SUFFIX if probe else "")
    inventory_root = INVENTORY_PREFIX + (PROBE_SUFFIX if probe else "")
    vin_root = VIN_SNAPSHOT_PREFIX + (PROBE_SUFFIX if probe else "")

    # -- freeze the inputs ------------------------------------------------
    parsed_rows = _object_inventory(client, bucket, PARSED_PREFIX + "/rows/", ".parquet")
    parsed_inputs = _object_inventory(
        client, bucket, PARSED_PREFIX + "/inputs/", ".parquet",
    )
    if not parsed_rows:
        raise ReconcileError(
            f"no parsed row shards under s3://{bucket}/{PARSED_PREFIX}/rows/"
        )
    silver = _discover_silver_objects(client, bucket, args.silver_prefix)
    queue_events = _discover_queue_event_objects(client, bucket)

    inventory_core = {
        "parsed_rows": parsed_rows,
        "parsed_inputs": parsed_inputs,
        "silver": [{k: o[k] for k in ("key", "size", "etag")} for o in silver],
        "queue_events": queue_events,
    }
    run_id = compute_run_id(inventory_core)
    run_dir = f"{compared_root}/{run_id}"
    inv_key = f"{inventory_root}/{run_id}.json"

    row_shards = parsed_rows[: args.max_units] if args.max_units else parsed_rows
    unit_names = [_compare_unit_name(o["key"]) for o in row_shards]
    refusals: list[dict[str, Any]] = []
    if len(silver) != 9:
        detail = (
            f"expected 9 compacted objects (one per source/month), found "
            f"{len(silver)}; the silver side of the comparison is not the shape "
            f"the design was validated against"
        )
        refusals.append({"kind": "silver_object_count", "detail": detail,
                         "enforced": apply and not probe
                         and not args.allow_silver_shape_drift})
        if apply and not probe and not args.allow_silver_shape_drift:
            raise ReconcileError(
                detail + ". The maintainer should rule on the shape change before "
                "an authoritative run; --allow-silver-shape-drift to proceed."
            )
        logger.warning("%s (advisory: %s)", detail,
                       "probe/dry run" if not apply
                       else "--allow-silver-shape-drift")

    logger.info("run_id %s  (%d parsed row shards, %d silver objects, %d queue-event "
                "objects)", run_id, len(row_shards), len(silver), len(queue_events))

    if apply and not probe and not args.force and _compare_run_complete(run_dir, inv_key):
        print(f"\ncompare: run {run_id} is already complete and the inventory is "
              f"unchanged; nothing to do.\n")
        return 0

    tmp = tempfile.TemporaryDirectory(prefix="p145compare-")
    tmpdir = Path(tmp.name)
    for sub in ("rows", "silver", "toimport"):
        (tmpdir / sub).mkdir()
    con = None

    try:
        # -- Pass A: localise the parsed row shards, count rows, collect ids --
        # `all_objects` is the distinct source-object count for the carousel
        # fan-out denominator. Accumulating it here (like `wanted`) counts a
        # content-derived key that legitimately appears in two source manifests
        # exactly once -- a per-unit `+= len(set)` would double it (cf. 051f7d0).
        nrows: dict[str, int] = {}
        wanted: set[str] = set()
        all_objects: set[str] = set()
        for obj, unit in zip(row_shards, unit_names):
            dest = tmpdir / "rows" / f"{unit}.parquet"
            dest.write_bytes(
                client.get_object(Bucket=bucket, Key=obj["key"])["Body"].read()
            )
            nrows[unit] = pq.ParquetFile(dest).metadata.num_rows
            table = pq.read_table(dest, columns=["listing_id", "object_key"])
            for lid in table.column("listing_id").to_pylist():
                if lid:
                    wanted.add(str(lid))
            for okey in table.column("object_key").to_pylist():
                if okey:
                    all_objects.add(str(okey))
        processed_total = sum(nrows.values())

        parse_report = read_json(
            f"s3://{bucket}/{PARSED_PREFIX}/parse_report.json"
        ) or {}
        if not probe:
            if not parse_report:
                raise ReconcileError(
                    f"s3://{bucket}/{PARSED_PREFIX}/parse_report.json is missing; "
                    f"Stage 4 has not finished. Use --probe to run against whatever "
                    f"units exist."
                )
            check_compare_apply_gate(parse_report, processed_total)
        elif not args.max_units and len(row_shards) != COMPARE_PLANNED_UNITS:
            logger.warning("PROBE: %d/%d parsed row shards present; results are partial",
                           len(row_shards), COMPARE_PLANNED_UNITS)

        # -- build the silver index ------------------------------------------
        source_paths: list[tuple[str, Optional[str]]] = []
        for index, obj in enumerate(silver):
            dest = tmpdir / "silver" / f"{index}.parquet"
            dest.write_bytes(
                client.get_object(Bucket=bucket, Key=obj["key"])["Body"].read()
            )
            source_paths.append((str(dest), obj.get("source")))
        con = _open_compare_duckdb(
            tmpdir / "compare.duckdb", threads=args.duckdb_threads,
            memory_limit=args.duckdb_memory_limit or None,
        )
        _load_silver_index(con, source_paths, wanted)
        silver_obs = con.execute("SELECT count(*) FROM silver").fetchone()[0]
        logger.info("silver index: %d observations for %d wanted listings "
                    "(duckdb threads=%d memory_limit=%s)",
                    silver_obs, len(wanted), max(1, args.duckdb_threads),
                    args.duckdb_memory_limit or "unset")

        # -- Pass B: classify every parsed row -----------------------------
        # to_import candidate rows are streamed straight to per-unit temp
        # shards carrying `_uid` and `_fp`; the global duplicate pass and the
        # near-duplicate measurement both run over those files, so nothing
        # per-row is retained in memory here.
        uid = 0
        parsed_vin: dict[str, Any] = {}
        fam_counts: Counter[str] = Counter()
        match_hist: Counter[int] = Counter()
        multi_candidate = 0
        detail_rows = carousel_rows = 0
        represented_written = 0
        unc_no_listing_id = unc_no_capture_time = 0

        for uindex, unit in enumerate(unit_names, start=1):
            prows = pq.read_table(tmpdir / "rows" / f"{unit}.parquet").to_pylist()
            summaries = _summarise_unit(con, prows)
            rep_rows: list[dict[str, Any]] = []
            unc_rows: list[dict[str, Any]] = []
            ti_rows: list[dict[str, Any]] = []
            for rid, row in enumerate(prows):
                src = row.get("source")
                if src == "detail":
                    detail_rows += 1
                    if row.get("vin") and row.get("listing_id"):
                        parsed_vin.setdefault(str(row["listing_id"]), row.get("vin"))
                elif src == "carousel":
                    carousel_rows += 1
                verdict = classify_from_summary(row, summaries.get(rid))
                fam_counts[verdict["family"]] += 1
                match_hist[verdict["match_count"]] += 1
                if verdict["match_count"] > 1:
                    multi_candidate += 1
                out = dict(row)
                out["reason"] = verdict["reason"]
                if verdict["family"] == "already_represented":
                    out["match_count"] = verdict["match_count"]
                    out["nearest_distance_s"] = verdict["nearest_distance_s"]
                    out["match_sources"] = verdict["match_sources"]
                    rep_rows.append(out)
                elif verdict["family"] == "unclassifiable":
                    if verdict["reason"] == "no_listing_id":
                        unc_no_listing_id += 1
                    else:
                        unc_no_capture_time += 1
                    unc_rows.append(out)
                else:
                    out["_uid"] = uid
                    out["_fp"] = business_fingerprint(out)
                    ti_rows.append(out)
                    uid += 1

            _emit_compare_shard(run_dir, "already_represented", unit,
                                _compared_schema("already_represented"), rep_rows,
                                apply=apply)
            _emit_compare_shard(run_dir, "unclassifiable", unit,
                                _compared_schema("unclassifiable"), unc_rows,
                                apply=apply)
            represented_written += len(rep_rows)
            if ti_rows:
                pq.write_table(
                    pa.Table.from_pylist(
                        [{k: r.get(k) for k in _toimport_temp_schema().names}
                         for r in ti_rows],
                        schema=_toimport_temp_schema(),
                    ),
                    tmpdir / "toimport" / f"{unit}.parquet",
                )
            if args.progress_every and uindex % args.progress_every == 0:
                logger.info("classified %d/%d units", uindex, len(unit_names))

        unclassifiable_written = unc_no_listing_id + unc_no_capture_time

        # -- distinct carousel fan-out, over the whole localised population --
        # Grouping by object_key across all row shards keeps a content-derived
        # key that appears in two units bound to one object -- the same case
        # `all_objects` fixes for the denominator (cf. 051f7d0).
        row_glob = str(tmpdir / "rows" / "*.parquet").replace("'", "''")
        max_carousel_per_object = int(con.execute(
            "SELECT coalesce(max(c), 0) FROM (SELECT count(*) AS c FROM "
            f"read_parquet('{row_glob}') WHERE source = 'carousel' "
            "AND object_key IS NOT NULL GROUP BY object_key)"
        ).fetchone()[0])

        # -- the unclassifiable magnitude gates --------------------------
        # The ~760 expectation was sized for tier-3 pages with no capture time.
        # The NULL-listing_id cohort is a distinct population whose size is not
        # known ahead of time; its ceiling defaults to 0, so any non-zero count
        # stops the first authoritative run for a maintainer ruling, after which
        # --max-no-listing-id is set to the measured number.
        #
        # Like the silver-shape refusal above, this only *stops* an --apply run:
        # a dry run and a probe write nothing and cannot advance slice 2, and
        # they are exactly the runs whose job is to measure these cohorts, so
        # there they warn and let the report carry the counts.
        gate_failures = []
        if unc_no_capture_time > args.max_unclassifiable:
            gate_failures.append(
                f"no_capture_time rows {unc_no_capture_time:,} exceed the "
                f"~{UNCLASSIFIABLE_EXPECTATION} expectation and the "
                f"{args.max_unclassifiable:,} ceiling"
            )
        if unc_no_listing_id > args.max_no_listing_id:
            gate_failures.append(
                f"no_listing_id rows {unc_no_listing_id:,} exceed the "
                f"{args.max_no_listing_id:,} ceiling; a large cohort means "
                f"compare drops that share as unimportable and slice 2 imports "
                f"less than the plan's arithmetic expects"
            )
        if gate_failures:
            message = ("; ".join(gate_failures)
                       + "; stopping so the maintainer rules on it before slice 2")
            if apply and not probe and not args.allow_unclassifiable_drift:
                raise ReconcileError(message)
            logger.warning("%s (%s)", message,
                           "continuing: --allow-unclassifiable-drift" if apply and not probe
                           else "advisory: probe/dry run measures, does not gate")

        # -- global duplicate resolution, over the temp shards ----------
        loser_uids, dup_report = _resolve_toimport_duplicates(
            con, tmpdir / "toimport",
        )
        if dup_report.get("conflicts"):
            if apply:
                stopped = {
                    "plan": 145, "stage": 5, "slice": 1, "mode": "compare",
                    "run_id": run_id, "status": "stopped_fingerprint_conflict",
                    "conflicting_fingerprint_groups": len(dup_report["conflicts"]),
                    "conflicts": dup_report["conflicts"][:50],
                }
                write_bytes(
                    f"{run_dir}/compare_report.stopped.json",
                    (json.dumps(stopped, indent=2, sort_keys=True, default=str)
                     + "\n").encode(),
                    content_type="application/json",
                )
            raise DuplicateFingerprintConflict(dup_report["conflicts"])

        # -- Pass C: split to_import into winners and recovery duplicates --
        to_import_final = 0
        dedupe_losers = 0
        for unit in unit_names:
            src_path = tmpdir / "toimport" / f"{unit}.parquet"
            if not src_path.exists():
                continue
            keep: list[dict[str, Any]] = []
            lose: list[dict[str, Any]] = []
            for row in pq.read_table(src_path).to_pylist():
                if row.get("_uid") in loser_uids:
                    row["reason"] = "recovery_duplicate"
                    row["match_count"] = 0
                    row["nearest_distance_s"] = None
                    row["match_sources"] = []
                    lose.append(row)
                else:
                    row["reason"] = None
                    keep.append(row)
            _emit_compare_shard(run_dir, "to_import", unit,
                                _compared_schema("to_import"), keep, apply=apply)
            _emit_compare_shard(run_dir, "already_represented", f"{unit}.dedup",
                                _compared_schema("already_represented"), lose,
                                apply=apply)
            to_import_final += len(keep)
            dedupe_losers += len(lose)

        already_total = represented_written + dedupe_losers
        family_sum = already_total + to_import_final + unclassifiable_written
        if family_sum != processed_total:
            raise ReconcileError(
                f"the three families sum to {family_sum} but {processed_total} "
                f"parsed rows were processed; classification is not a partition"
            )

        near = _measure_near_duplicates(con, tmpdir / "toimport", loser_uids)
        near["note"] = (
            "adjacent_pairs_within_300s counts consecutive captures; "
            "captures_with_a_neighbour is the count of unrepresented captures "
            "with another within 300 s -- the quantity to rule on"
        )

        # -- the read-only VIN snapshot --------------------------------
        vin_info = {"key": None, "rows": 0, "size": 0, "sha256": None}
        vin_collisions: dict[str, Any] = {"count": 0, "examples": []}
        if apply:
            from shared.db import get_conn

            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    vin_map = snapshot_vin_lookup(sorted(wanted), cur,
                                                  batch_size=args.vin_batch)
                conn.rollback()
            finally:
                conn.close()
            vin_info = _write_vin_snapshot(
                f"{vin_root}/{run_id}.parquet", vin_map, apply=True,
            )
            for listing_id, parsed_value in parsed_vin.items():
                hot = vin_map.get(listing_id)
                if hot and parsed_value and hot != parsed_value:
                    vin_collisions["count"] += 1
                    if len(vin_collisions["examples"]) < 50:
                        vin_collisions["examples"].append({
                            "listing_id": listing_id,
                            "parsed_vin": parsed_value,
                            "hot_vin": hot,
                        })

        # -- report -----------------------------------------------------
        report = {
            "plan": 145, "stage": 5, "slice": 1, "mode": "compare",
            "run_id": run_id,
            "probe": probe,
            "apply": apply,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "predicate": "same listing_id AND abs(silver.fetched_at - "
                         "parsed.fetched_at) <= 300 seconds",
            "inventory": {
                "key": inv_key,
                "digest": hashlib.sha256(
                    json.dumps(inventory_core, sort_keys=True,
                               separators=(",", ":")).encode()
                ).hexdigest(),
                "parsed_rows": {"objects": len(parsed_rows),
                                "bytes": sum(o["size"] for o in parsed_rows)},
                "parsed_inputs": {"objects": len(parsed_inputs),
                                  "bytes": sum(o["size"] for o in parsed_inputs)},
                "silver": {"objects": len(silver),
                           "bytes": sum(o["size"] for o in silver),
                           "observations": silver_obs},
                "queue_events": {"objects": len(queue_events),
                                 "bytes": sum(o["size"] for o in queue_events)},
                "vin_snapshot": vin_info,
            },
            "units_processed": len(unit_names),
            "planned_units": COMPARE_PLANNED_UNITS,
            "parsed_row_total": processed_total,
            "families": {
                "already_represented": already_total,
                "to_import": to_import_final,
                "unclassifiable": unclassifiable_written,
                "sum": family_sum,
                "pre_dedupe": {
                    "already_represented": fam_counts["already_represented"],
                    "to_import": fam_counts["to_import"],
                    "unclassifiable": fam_counts["unclassifiable"],
                },
            },
            "carousel_fan_out": {
                "detail_rows": detail_rows,
                "carousel_rows": carousel_rows,
                "objects": len(all_objects),
                "carousel_per_detail_row": (
                    round(carousel_rows / detail_rows, 4) if detail_rows else None
                ),
                "carousel_per_object": (
                    round(carousel_rows / len(all_objects), 4)
                    if all_objects else None
                ),
                "max_carousel_per_object": max_carousel_per_object,
            },
            "multiple_candidate_share": {
                "observations": processed_total,
                "with_multiple_candidates": multi_candidate,
                "share": (round(multi_candidate / processed_total, 4)
                          if processed_total else None),
            },
            "match_count_distribution": {
                str(k): match_hist[k] for k in sorted(match_hist)
            },
            "duplicates": {
                "groups_collapsed": dup_report["groups_collapsed"],
                "rows_moved_to_already_represented":
                    dup_report["rows_moved_to_already_represented"],
                "conflicting_fingerprint_groups": 0,
            },
            "near_duplicate_window": near,
            "unclassifiable": {
                "count": unclassifiable_written,
                "no_capture_time": unc_no_capture_time,
                "no_listing_id": unc_no_listing_id,
                "expectation_no_capture_time": UNCLASSIFIABLE_EXPECTATION,
                "ceiling_no_capture_time": args.max_unclassifiable,
                "ceiling_no_listing_id": args.max_no_listing_id,
                "materially_larger": (
                    unc_no_capture_time > args.max_unclassifiable
                    or unc_no_listing_id > args.max_no_listing_id
                ),
            },
            "vin_collisions": vin_collisions,
            "refusals": refusals,
        }

        if apply:
            inventory = dict(inventory_core)
            inventory.update({
                "run_id": run_id,
                "generated_at": report["generated_at"],
                "digest": report["inventory"]["digest"],
                "silver_observations": silver_obs,
                "vin_snapshot": vin_info,
            })
            write_bytes(
                inv_key,
                (json.dumps(inventory, indent=2, sort_keys=True) + "\n").encode(),
                content_type="application/json",
            )
            write_bytes(
                f"{run_dir}/compare_report.json",
                (json.dumps(report, indent=2, sort_keys=True, default=str)
                 + "\n").encode(),
                content_type="application/json",
            )
    finally:
        if con is not None:
            con.close()
        tmp.cleanup()

    _print_compare_report(report, apply=apply, probe=probe)
    return 0


def _print_compare_report(report: dict[str, Any], *, apply: bool, probe: bool) -> None:
    fam = report["families"]
    fan = report["carousel_fan_out"]
    print()
    print("Plan 145 Stage 5 slice 1 -- compare parsed observations to silver")
    print("=" * 66)
    print(f"mode                 {'APPLY' if apply else 'DRY RUN'}"
          f"{'  (PROBE)' if probe else ''}")
    print(f"run_id               {report['run_id']}")
    print(f"units processed      {report['units_processed']:>12,}"
          f"  (planned {report['planned_units']:,})")
    print(f"parsed row total     {report['parsed_row_total']:>12,}")
    print()
    unc = report["unclassifiable"]
    print(f"  already_represented {fam['already_represented']:>12,}")
    print(f"  to_import           {fam['to_import']:>12,}")
    print(f"  unclassifiable      {fam['unclassifiable']:>12,}"
          f"  (no_capture_time {unc['no_capture_time']:,}/~{unc['expectation_no_capture_time']}"
          f", no_listing_id {unc['no_listing_id']:,}/ceil {unc['ceiling_no_listing_id']:,})")
    print(f"  sum                 {fam['sum']:>12,}"
          f"  {'OK' if fam['sum'] == report['parsed_row_total'] else 'MISMATCH'}")
    print()
    print(f"carousel fan-out     {fan['carousel_per_object']} per object over "
          f"{fan['objects']:,} objects, max {fan['max_carousel_per_object']}")
    print(f"multi-candidate      {report['multiple_candidate_share']['with_multiple_candidates']:,}"
          f"  ({report['multiple_candidate_share']['share']})")
    print(f"recovery duplicates  {report['duplicates']['rows_moved_to_already_represented']:,}"
          f" in {report['duplicates']['groups_collapsed']:,} groups")
    nd = report["near_duplicate_window"]
    print(f"near-dup <=300s      {nd['adjacent_pairs_within_300s']:,} adjacent pairs, "
          f"{nd['captures_with_a_neighbour']:,} captures with a neighbour, over "
          f"{nd['listings_involved']:,} listings")
    print(f"vin collisions       {report['vin_collisions']['count']:,}")
    print(f"inventory digest     {report['inventory']['digest']}")
    if apply:
        print(f"\nwrote compared/{report['run_id']}/, inventory/{report['run_id']}.json"
              f" and the VIN snapshot")
    else:
        print("\nDRY RUN: classified and measured only; no object was written and "
              "no VIN query was issued.")
    print()


# --------------------------------------------------------------------------
# Stage 5 slice 2 -- assign identity, then write the historical import set
# --------------------------------------------------------------------------
#
# This is the first Plan 145 mode that writes to Postgres. Two modes:
#
#   assign  reads compared/<run_id>/to_import/ and writes one immutable
#           assignment shard per batch under recovery/plan145/assigned/. The
#           only database statement it issues is `nextval` on
#           ops.artifacts_queue_artifact_id_seq.
#   apply   reads those shards back and writes four things per batch in one
#           transaction: staging.silver_observations, the historical
#           staging.price_observation_events for detail rows,
#           staging.artifacts_queue_events with status 'recovered', and the
#           durable receipt row.
#
# Both default to a dry run.
#
# Three deployed-contract details shape the code, and each one is a trap:
#
# 1. `shared.db.db_cursor` opens its own connection and commits on exit, so
#    three calls are three transactions. `write_silver_observations_postgres`
#    catches every exception and returns 0, so a failed batch would be logged
#    as a warning and the run would carry on believing it committed. Neither
#    can be reused. One connection is opened here, all four writes run on it,
#    it commits once, and exceptions propagate. The *column list* is reused --
#    `_POSTGRES_COLS` and `_INSERT_SQL` are imported so a schema change cannot
#    drift silently -- but not the function.
# 2. `staging.price_observation_events.listing_id` is `uuid NOT NULL` while
#    `staging.silver_observations.listing_id` is `text`. Every listing id is
#    validated against `_UUID_RE`, and a failure is a stop, not a skip.
# 3. The staging tables are asynchronously flushed and then *deleted*
#    (`flush_silver_observations.py` deletes the rows it flushed), so querying
#    Postgres after an ambiguous client response cannot tell "never committed"
#    from "committed and already flushed away". The receipt written inside the
#    same transaction is the only durable evidence, which is why retry is
#    keyed on it rather than on the rows.
#
# What is never touched: `ops.artifacts_queue` (an inserted row would be
# claimed by `processing/sql/claim_artifacts.sql` within seconds and run the
# whole hot-state path this plan forbids), `ops.price_observations`,
# `ops.vin_to_listing`, `ops.blocked_cooldown`, `ops.detail_scrape_claims`,
# and live event emission.

ASSIGNED_PREFIX = "recovery/plan145/assigned"

#: `bigserial`. `nextval` never returns a value twice and does not roll back on
#: abort, so concurrent live inserts are safe. `max(artifact_id) + 1` races
#: them and is forbidden.
ARTIFACT_ID_SEQUENCE = "ops.artifacts_queue_artifact_id_seq"

#: A batch never splits an artifact: all of an object's rows share one identity
#: and one transaction. Whichever cap binds first closes the batch.
MAX_BATCH_ARTIFACTS = 5000
MAX_BATCH_SILVER_ROWS = 50000

#: Silver rows one `apply --apply` may write before it needs a named maintainer
#: approval. The plan sizes the canary at ~500 `to_import` observations, so this
#: leaves headroom for one while refusing a full batch -- which, at the caps
#: above, is 50,000 rows. The gate has to be measured in rows: a batch count
#: says nothing about how much history a run writes.
CANARY_ROW_BUDGET = 1000

#: `staging.artifacts_queue_events` has no status CHECK and no FK, so this
#: value with no hot queue row is valid by construction.
RECOVERED_STATUS = "recovered"

ID_PRESERVED = "preserved_queue_event"
ID_ALLOCATED = "allocated_sequence"

#: April pack members with no historical event row (2026-08-27 audit). They may
#: stay unattributed only while they contribute no row to `to_import`; one that
#: does gets a sequence value like any other artifact.
UNATTRIBUTED_PACK_MEMBERS = 42276

RECEIPT_TABLE = "public.plan145_recovery_batch_receipts"

_RECEIPT_SELECT_SQL = (
    f"SELECT manifest_sha256 FROM {RECEIPT_TABLE} WHERE batch_name = %s"
)

_RECEIPT_INSERT_SQL = f"""
    INSERT INTO {RECEIPT_TABLE} (
        batch_name, manifest_sha256, artifact_count, silver_count,
        price_event_count, queue_event_count
    ) VALUES (%s, %s, %s, %s, %s, %s)
"""

#: `event_at` is listed explicitly because the column defaults to `now()` and
#: this is a historical capture, not news. The uuid cast is explicit because
#: the parsed listing id arrives as text.
_PRICE_EVENT_INSERT_SQL = """
    INSERT INTO staging.price_observation_events (
        listing_id, vin, price, make, model, artifact_id,
        event_type, source, event_at
    ) VALUES %s
"""
_PRICE_EVENT_TEMPLATE = "(%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)"
_PRICE_EVENT_COLS = (
    "listing_id", "vin", "price", "make", "model", "artifact_id",
    "event_type", "source", "event_at",
)

#: `fetched_at` is the April capture time; `event_at` is left to its `now()`
#: default because it records when the recovery ran, not when the page was
#: captured. Mirrors `processing/sql/insert_artifact_event.sql` plus that split.
_QUEUE_EVENT_INSERT_SQL = """
    INSERT INTO staging.artifacts_queue_events (
        artifact_id, status, minio_path, artifact_type,
        fetched_at, listing_id, run_id
    ) VALUES %s
"""
_QUEUE_EVENT_COLS = (
    "artifact_id", "status", "minio_path", "artifact_type",
    "fetched_at", "listing_id", "run_id",
)

#: One `nextval` per identity, in one round trip. `generate_series` is the
#: whole point: it cannot be written as an arithmetic offset from a `max()`.
_ALLOCATE_SQL = (
    f"SELECT nextval('{ARTIFACT_ID_SEQUENCE}') FROM generate_series(1, %s)"
)


class ReceiptConflict(ReconcileError):
    """The same batch name already carries a *different* assignment digest.

    Not a retry: either the assignment set changed under a committed batch or
    two different populations were given one name. Both digests are surfaced
    and nothing is written.
    """


class ViolationLog:
    """Counts every invariant violation; keeps only a bounded sample of them.

    The stop path exists to report a *cohort* before refusing, rather than
    dying on row three. That needs the counts and a handful of examples, and
    nothing else -- so a systematic upstream defect (every row missing
    ``fetched_at``, say) is described in constant space instead of
    accumulating one dict per row across a ~1.1M-row population.
    """

    def __init__(self, max_examples: int = 20) -> None:
        self.counts: Counter[str] = Counter()
        self.examples: list[dict[str, Any]] = []
        self.max_examples = max_examples

    def add(self, reason: str, **detail: Any) -> None:
        self.counts[reason] += 1
        if len(self.examples) < self.max_examples:
            self.examples.append({"reason": reason, **detail})

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    def __bool__(self) -> bool:
        return bool(self.counts)


class ImportSetInvalid(ReconcileError):
    """The import set violates an invariant that must hold before any write.

    Slice 1 enforced "every ``to_import`` row has a non-NULL ``listing_id``"
    through a path no real data has exercised -- both probes ran on a
    materialized-only population where tier-1 identity always resolves. So the
    invariant is re-validated here rather than assumed, and a violation stops
    the run instead of skipping the row.
    """


# -- schema ----------------------------------------------------------------

def _assigned_schema() -> Any:
    import pyarrow as pa

    return pa.schema([
        pa.field("batch_name", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("object_key", pa.string()),
        pa.field("artifact_id", pa.int64()),
        pa.field("id_source", pa.string()),
        pa.field("listing_id", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("input_kind", pa.string()),
        pa.field("source_unit", pa.string()),
        pa.field("silver_rows", pa.int32()),
        pa.field("detail_rows", pa.int32()),
        pa.field("assigned_at", pa.timestamp("us", tz="UTC")),
    ])


def assign_batch_name(run_id: str, index: int) -> str:
    """Batch names are run-scoped so a receipt names one population forever."""
    return f"{run_id}-b{index:05d}"


def _assigned_key(batch_name: str) -> str:
    return f"{ASSIGNED_PREFIX}/{batch_name}.parquet"


def _assign_report_key(run_id: str) -> str:
    return f"{ASSIGNED_PREFIX}/{run_id}-assign_report.json"


# -- validation ------------------------------------------------------------

def validate_import_listing_id(value: Any) -> Optional[str]:
    """Return why this listing id cannot be imported, or None.

    Both failures are stops. A NULL id cannot reach
    ``staging.silver_observations`` (NOT NULL) and a non-UUID id cannot reach
    ``staging.price_observation_events`` (uuid NOT NULL); either one arriving
    here means an upstream classification is wrong, not that one row should be
    dropped quietly.
    """
    if value is None or value == "":
        return "null_listing_id"
    if not _UUID_RE.fullmatch(str(value)):
        return "non_uuid_listing_id"
    return None


# -- batching --------------------------------------------------------------

def plan_import_batches(
    objects: Sequence[dict[str, Any]], *,
    max_artifacts: int = MAX_BATCH_ARTIFACTS,
    max_silver_rows: int = MAX_BATCH_SILVER_ROWS,
) -> list[dict[str, Any]]:
    """Order by ``object_key`` and cut on whichever cap binds first.

    An artifact is never split: every row of one source object shares one
    identity and one transaction, so an object larger than ``max_silver_rows``
    becomes a batch of its own rather than being cut in half.
    """
    batches: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    rows = 0
    for obj in sorted(objects, key=lambda o: o["object_key"]):
        obj_rows = int(obj["silver_rows"])
        if current:
            over_artifacts = len(current) + 1 > max_artifacts
            over_rows = rows + obj_rows > max_silver_rows
            if over_artifacts or over_rows:
                batches.append({
                    "objects": current, "silver_rows": rows,
                    "bound_by": "artifacts" if over_artifacts else "silver_rows",
                })
                current, rows = [], 0
        current.append(obj)
        rows += obj_rows
    if current:
        batches.append({"objects": current, "silver_rows": rows, "bound_by": "end"})
    return batches


# -- identity --------------------------------------------------------------

def build_queue_artifact_ids(
    rows: Sequence[dict[str, Any]], wanted: Optional[set[str]] = None,
) -> dict[str, int]:
    """Map a normalized object path to its one historical ``artifact_id``.

    The March-May artifact-event lake holds 4,906,595 detail event rows
    reducing to 1,536,055 distinct object paths with **zero** paths mapped to
    conflicting artifact ids, so this is a strict lookup. A conflict would
    falsify that audit, so it stops the run rather than picking a side.

    ``legacy_artifact_id`` is never consulted: ``raw_artifacts`` and
    ``ops.artifacts_queue`` are separate sequences and the same integer names
    two different artifacts across the cutover.
    """
    out: dict[str, int] = {}
    for row in rows:
        if row.get("artifact_type") != TARGET_ARTIFACT_TYPE:
            continue
        key = _bare_object_key(row.get("minio_path"))
        if not key:
            continue
        if wanted is not None and key not in wanted:
            continue
        artifact_id = row.get("artifact_id")
        if artifact_id is None:
            continue
        artifact_id = int(artifact_id)
        previous = out.get(key)
        if previous is None:
            out[key] = artifact_id
        elif previous != artifact_id:
            raise ReconcileError(
                f"object path {key} maps to conflicting queue-event artifact "
                f"ids {previous} and {artifact_id}; the identity audit says no "
                f"such path exists, so this is a stop rather than a choice"
            )
    return out


def _load_queue_artifact_ids(client, bucket: str, wanted: set[str]) -> dict[str, int]:
    """Read the frozen March-May artifact-event objects for identity only."""
    identities: dict[str, int] = {}
    month_prefixes = []
    for prefix in _list_common_prefixes(
        client, bucket, f"{QUEUE_EVENTS_PREFIX}/year={TARGET_YEAR}/",
    ):
        leaf = prefix.rstrip("/").rsplit("/", 1)[-1]
        if not leaf.startswith("month="):
            continue
        try:
            month = int(leaf.split("=", 1)[1])
        except ValueError:
            continue
        if month in QUEUE_EVENT_MONTHS:
            month_prefixes.append(prefix)
    if len(month_prefixes) != len(QUEUE_EVENT_MONTHS):
        raise ReconcileError(
            f"expected queue-event months {QUEUE_EVENT_MONTHS}, found {month_prefixes}"
        )
    for prefix in sorted(month_prefixes):
        for key in _list_keys(client, bucket, prefix, ".parquet"):
            shard = build_queue_artifact_ids(
                _read_parquet_rows(
                    client, bucket, key,
                    columns=["minio_path", "artifact_id", "artifact_type"],
                ),
                wanted,
            )
            for object_key, artifact_id in shard.items():
                previous = identities.get(object_key)
                if previous is None:
                    identities[object_key] = artifact_id
                elif previous != artifact_id:
                    raise ReconcileError(
                        f"object path {object_key} maps to conflicting "
                        f"queue-event artifact ids {previous} and {artifact_id} "
                        f"across shards"
                    )
    return identities


def allocate_artifact_ids(cursor, count: int) -> list[int]:
    """``count`` fresh sequence values, in one round trip.

    Never ``max(artifact_id) + 1``: that races live inserts. ``nextval`` never
    returns a value twice and does not roll back on abort, so a value lost
    before the assignment shard is written is a harmless gap in a `bigserial`.
    """
    if count <= 0:
        return []
    cursor.execute(_ALLOCATE_SQL, (count,))
    ids = [int(row[0]) for row in cursor.fetchall()]
    if len(ids) != count:
        raise ReconcileError(
            f"{ARTIFACT_ID_SEQUENCE} returned {len(ids)} values for {count} artifacts"
        )
    return ids


def assign_identities(
    objects: Sequence[dict[str, Any]],
    queue_ids: dict[str, int],
    allocate,
) -> list[dict[str, Any]]:
    """Preserve where the path has a historical id, allocate otherwise.

    ``allocate(n) -> list[int]`` is called exactly once, for the objects with
    no preserved id, so a batch costs one sequence round trip.
    """
    needing = [o for o in objects if o["object_key"] not in queue_ids]
    allocated = list(allocate(len(needing)))
    if len(allocated) != len(needing):
        raise ReconcileError(
            f"allocator returned {len(allocated)} ids for {len(needing)} artifacts"
        )
    pending = iter(allocated)
    out: list[dict[str, Any]] = []
    for obj in objects:
        preserved = queue_ids.get(obj["object_key"])
        record = dict(obj)
        if preserved is not None:
            record["artifact_id"] = preserved
            record["id_source"] = ID_PRESERVED
        else:
            record["artifact_id"] = next(pending)
            record["id_source"] = ID_ALLOCATED
        out.append(record)
    return out


# -- the four writes -------------------------------------------------------

def build_recovery_silver_row(
    row: dict[str, Any], artifact_id: int, vin_map: dict[str, Any],
) -> dict[str, Any]:
    """One ``staging.silver_observations`` row, from one ``to_import`` row.

    The VIN snapshot only *fills* -- a parsed VIN always wins, and the snapshot
    is never written back. Carousel rows are the reason it exists: the parse
    stage deliberately leaves their ``vin`` NULL.

    The listing id is validated *before* it is cast, because the cast would
    otherwise defeat the column that is supposed to catch this:
    ``staging.silver_observations.listing_id`` is ``text NOT NULL``, and
    ``str(None)`` is the perfectly acceptable four-character string ``"None"``.
    A carousel row never reaches ``build_recovery_price_event``, so this is the
    only check standing between it and the INSERT.
    """
    from processing.writers.silver_writer import _POSTGRES_COLS

    problem = validate_import_listing_id(row.get("listing_id"))
    if problem:
        raise ImportSetInvalid(
            f"{problem}: {row.get('listing_id')!r} on object "
            f"{row.get('object_key')!r} ({row.get('source')} row) cannot be "
            f"imported; a stop, not a skip"
        )
    out = {name: row.get(name) for name in _POSTGRES_COLS}
    out["artifact_id"] = int(artifact_id)
    out["listing_id"] = str(row["listing_id"])
    if not out.get("vin"):
        out["vin"] = vin_map.get(out["listing_id"])
    out["listing_state"] = row.get("listing_state") or "active"
    out["fetched_at"] = _as_utc_datetime(row.get("fetched_at"))
    return out


def build_recovery_price_event(silver_row: dict[str, Any]) -> Optional[dict[str, Any]]:
    """The historical price event for one imported **detail** row, or None.

    No carousel price events. Production mints those only for carousel hints
    passing the search configuration active at capture time, and that April
    configuration is not recoverable: applying today's filter, or writing all
    carousel rows, would manufacture history. Carousel rows stay first-class
    silver coverage.
    """
    if silver_row.get("source") != "detail":
        return None
    listing_id = silver_row.get("listing_id")
    problem = validate_import_listing_id(listing_id)
    if problem:
        raise ImportSetInvalid(
            f"{problem}: {listing_id!r} cannot mint a price event "
            f"(staging.price_observation_events.listing_id is uuid NOT NULL)"
        )
    fetched_at = silver_row.get("fetched_at")
    if fetched_at is None:
        raise ImportSetInvalid(
            f"listing {listing_id} has no capture time; a price event at "
            f"now() would date an April capture as today"
        )
    return {
        "listing_id": str(listing_id),
        "vin": silver_row.get("vin"),
        "price": silver_row.get("price"),
        "make": silver_row.get("make"),
        "model": silver_row.get("model"),
        "artifact_id": silver_row["artifact_id"],
        "event_type": (
            "deleted" if silver_row.get("listing_state") == "unlisted" else "upserted"
        ),
        "source": "detail",
        "event_at": fetched_at,
    }


def build_recovery_queue_event(
    assignment: dict[str, Any], batch_name: str, bucket: str,
) -> dict[str, Any]:
    """One ``recovered`` artifact event per artifact -- never a queue row."""
    return {
        "artifact_id": int(assignment["artifact_id"]),
        "status": RECOVERED_STATUS,
        "minio_path": f"s3://{bucket}/{assignment['object_key']}",
        "artifact_type": TARGET_ARTIFACT_TYPE,
        "fetched_at": assignment["fetched_at"],
        "listing_id": assignment["listing_id"],
        "run_id": batch_name,
    }


def check_batch_receipt(cursor, batch_name: str, manifest_sha256: str) -> str:
    """``"absent"`` or ``"committed"``; a different digest raises.

    The staging tables are flushed and deleted, so this receipt is the only
    thing that can answer "did this batch commit?" after an ambiguous client
    response.
    """
    cursor.execute(_RECEIPT_SELECT_SQL, (batch_name,))
    digests = {str(row[0]) for row in cursor.fetchall()}
    if not digests:
        return "absent"
    if digests == {manifest_sha256}:
        return "committed"
    raise ReceiptConflict(
        f"batch {batch_name} already has receipt digest(s) "
        f"{sorted(digests)} but this assignment manifest hashes to "
        f"{manifest_sha256}; refusing to write and refusing to overwrite the "
        f"receipt"
    )


def write_import_batch(
    conn, batch_name: str, manifest_sha256: str,
    silver_rows: Sequence[dict[str, Any]],
    price_events: Sequence[dict[str, Any]],
    queue_events: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """All four writes on one connection, one commit, exceptions propagating.

    Deliberately not ``shared.db.db_cursor`` (a connection and a commit per
    call) and deliberately not ``write_silver_observations_postgres`` (returns
    0 on failure and logs a warning). Either would turn a half-written batch
    into a success.
    """
    from psycopg2.extras import execute_values

    from processing.writers.silver_writer import _INSERT_SQL as _SILVER_INSERT_SQL
    from processing.writers.silver_writer import _POSTGRES_COLS

    try:
        with conn.cursor() as cur:
            if check_batch_receipt(cur, batch_name, manifest_sha256) == "committed":
                conn.rollback()
                return {"batch_name": batch_name, "skipped": True,
                        "silver": 0, "price_events": 0, "queue_events": 0,
                        "artifacts": 0}
            if silver_rows:
                execute_values(cur, _SILVER_INSERT_SQL, [
                    tuple(row.get(col) for col in _POSTGRES_COLS)
                    for row in silver_rows
                ])
            if price_events:
                execute_values(cur, _PRICE_EVENT_INSERT_SQL, [
                    tuple(event.get(col) for col in _PRICE_EVENT_COLS)
                    for event in price_events
                ], template=_PRICE_EVENT_TEMPLATE)
            if queue_events:
                execute_values(cur, _QUEUE_EVENT_INSERT_SQL, [
                    tuple(event.get(col) for col in _QUEUE_EVENT_COLS)
                    for event in queue_events
                ])
            cur.execute(_RECEIPT_INSERT_SQL, (
                batch_name, manifest_sha256, len(queue_events), len(silver_rows),
                len(price_events), len(queue_events),
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"batch_name": batch_name, "skipped": False,
            "silver": len(silver_rows), "price_events": len(price_events),
            "queue_events": len(queue_events), "artifacts": len(queue_events)}


# -- reading slice 1's outputs --------------------------------------------

def _discover_compare_run(client, bucket: str) -> str:
    """The one complete ``compare`` run, or a demand for ``--run-id``."""
    candidates = []
    for prefix in _list_common_prefixes(client, bucket, COMPARED_PREFIX + "/"):
        run_id = prefix.rstrip("/").rsplit("/", 1)[-1]
        if _compare_run_complete(f"{COMPARED_PREFIX}/{run_id}",
                                 f"{INVENTORY_PREFIX}/{run_id}.json"):
            candidates.append(run_id)
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise ReconcileError(
            f"no complete compare run under s3://{bucket}/{COMPARED_PREFIX}/; "
            f"slice 1 must finish before slice 2 can assign"
        )
    raise ReconcileError(
        f"{len(candidates)} complete compare runs ({', '.join(sorted(candidates))}); "
        f"name one with --run-id"
    )


def _scan_to_import(client, bucket: str, run_dir: str
                    ) -> tuple[dict[str, dict[str, Any]], ViolationLog, int]:
    """Stream every ``to_import`` shard into per-object aggregates.

    Only four columns are read and nothing per-row is retained -- not even a
    violation, beyond its count and a bounded sample: at the probe's measured
    ratio the population is ~1.1M rows over ~190k artifacts, and holding those
    rows to plan batches, or to describe a systematic failure, would cost
    gigabytes for a count.
    """
    objects: dict[str, dict[str, Any]] = {}
    violations = ViolationLog()
    total_rows = 0
    keys = _list_keys(client, bucket, f"{run_dir}/to_import/", ".parquet")
    if not keys:
        raise ReconcileError(
            f"no to_import shards under s3://{bucket}/{run_dir}/to_import/"
        )
    for key in keys:
        unit = key.rsplit("/", 1)[-1].removesuffix(".parquet")
        rows = _read_parquet_rows(
            client, bucket, key,
            columns=["object_key", "listing_id", "source", "fetched_at"],
        )
        for row in rows:
            total_rows += 1
            object_key = row.get("object_key")
            if not object_key:
                violations.add("null_object_key", unit=unit)
                continue
            problem = validate_import_listing_id(row.get("listing_id"))
            if problem:
                violations.add(problem, unit=unit, object_key=object_key,
                               listing_id=row.get("listing_id"))
            if row.get("fetched_at") is None:
                violations.add("null_fetched_at", unit=unit, object_key=object_key)
            record = objects.get(object_key)
            if record is None:
                objects[object_key] = record = {
                    "object_key": object_key, "source_unit": unit,
                    "silver_rows": 0, "detail_rows": 0,
                }
            elif record["source_unit"] != unit:
                # Stage 4 dedupes object keys across work units, so one object's
                # rows live in exactly one shard. Two shards would mean a batch
                # could not hold one artifact whole.
                violations.add("object_split_across_units", object_key=object_key,
                               units=[record["source_unit"], unit])
            record["silver_rows"] += 1
            if row.get("source") == "detail":
                record["detail_rows"] += 1
    return objects, violations, total_rows


def _load_import_identity(client, bucket: str, wanted: set[str]
                          ) -> dict[str, dict[str, Any]]:
    """Per-object page identity, from Stage 4's frozen ``inputs`` shards.

    The *page's* listing id is needed even for an object whose own detail row
    landed in ``already_represented`` while its carousel rows did not: the
    recovered queue event carries the primary listing, and a carousel row's
    ``listing_id`` is the hint's, not the page's.
    """
    out: dict[str, dict[str, Any]] = {}
    for key in _list_keys(client, bucket, PARSED_PREFIX + "/inputs/", ".parquet"):
        for row in _read_parquet_rows(
            client, bucket, key,
            columns=["object_key", "listing_id", "fetched_at", "input_kind"],
        ):
            object_key = row.get("object_key")
            if not object_key or object_key not in wanted:
                continue
            out[object_key] = {
                "listing_id": (
                    str(row["listing_id"]) if row.get("listing_id") else None
                ),
                "fetched_at": _as_utc_datetime(row.get("fetched_at")),
                "input_kind": row.get("input_kind"),
            }
    return out


def _load_vin_snapshot(client, bucket: str, run_id: str) -> dict[str, Any]:
    """Slice 1's read-only ``ops.vin_to_listing`` snapshot. Never written back."""
    key = f"{VIN_SNAPSHOT_PREFIX}/{run_id}.parquet"
    rows = _read_parquet_rows(client, bucket, key, columns=["listing_id", "vin"])
    return {str(r["listing_id"]): r.get("vin") for r in rows if r.get("listing_id")}


def _read_assignment_shard(client, bucket: str, batch_name: str
                           ) -> tuple[list[dict[str, Any]], str]:
    """The assignment shard's rows and the SHA-256 of its exact bytes.

    The digest is over the stored object, so the receipt is keyed to the bytes
    a retry will read rather than to a re-derived summary of them.
    """
    import io

    import pyarrow.parquet as pq

    body = client.get_object(
        Bucket=bucket, Key=_assigned_key(batch_name),
    )["Body"].read()
    rows = pq.read_table(io.BytesIO(body)).to_pylist()
    return rows, hashlib.sha256(body).hexdigest()


# -- assign ----------------------------------------------------------------

def _no_allocation(count: int) -> list[int]:
    """A dry run's allocator: it must not advance the sequence.

    The sentinel is negative so that a dry-run record reaching a shard or a
    database would be obviously wrong rather than plausibly right.
    """
    return [-1] * count


def run_assign(args: argparse.Namespace) -> int:
    from shared.minio import BUCKET as DEFAULT_BUCKET
    from shared.minio import object_exists, write_bytes

    bucket = args.bucket or DEFAULT_BUCKET
    client = _s3_client()
    apply = bool(args.apply)

    run_id = args.run_id or _discover_compare_run(client, bucket)
    run_dir = f"{COMPARED_PREFIX}/{run_id}"
    if not _compare_run_complete(run_dir, f"{INVENTORY_PREFIX}/{run_id}.json"):
        detail = (
            f"compare run {run_id} has no inventory freeze and/or no "
            f"compare_report.json, so its to_import family is not final"
        )
        if apply:
            raise ReconcileError(detail + "; finish slice 1 before assigning")
        logger.warning("%s (advisory: dry run measures, does not gate)", detail)

    objects, violations, to_import_rows = _scan_to_import(client, bucket, run_dir)
    identity = _load_import_identity(client, bucket, set(objects))
    for object_key in sorted(objects):
        context = identity.get(object_key)
        if context is None:
            violations.add("no_parsed_input_row", object_key=object_key)
            continue
        problem = validate_import_listing_id(context["listing_id"])
        if problem:
            violations.add(f"page_{problem}", object_key=object_key,
                           listing_id=context["listing_id"])
        if context["fetched_at"] is None:
            violations.add("page_null_fetched_at", object_key=object_key)
        objects[object_key].update(context)

    # Reported first, then refused: a run that dies on row three cannot tell
    # the maintainer how large the problem is.
    if violations:
        print()
        print("Plan 145 Stage 5 slice 2 -- assign STOPPED: the import set is invalid")
        print("=" * 70)
        for reason, count in sorted(violations.counts.items()):
            print(f"  {reason:<28} {count:>10,}")
        for example in violations.examples:
            print(f"    e.g. {example}")
        print()
        raise ImportSetInvalid(
            f"{violations.total:,} invalid to_import row(s) across "
            f"{len(violations.counts)} reason(s): "
            f"{dict(sorted(violations.counts.items()))}"
        )

    queue_ids = _load_queue_artifact_ids(client, bucket, set(objects))
    batches = plan_import_batches(
        list(objects.values()),
        max_artifacts=args.max_artifacts, max_silver_rows=args.max_silver_rows,
    )

    # The caps decide batch membership, so re-assigning a run under different
    # caps would silently reuse shards whose contents no longer match their
    # name. The recorded caps make that a stop.
    report_key = _assign_report_key(run_id)
    if object_exists(report_key):
        from shared.minio import read_json

        previous = read_json(f"s3://{bucket}/{report_key}") or {}
        prior_caps = previous.get("caps") or {}
        if prior_caps and prior_caps != {"max_artifacts": args.max_artifacts,
                                         "max_silver_rows": args.max_silver_rows}:
            raise ReconcileError(
                f"run {run_id} was already assigned under caps {prior_caps}; "
                f"re-assigning under "
                f"{{'max_artifacts': {args.max_artifacts}, "
                f"'max_silver_rows': {args.max_silver_rows}}} would change every "
                f"batch's membership without changing its name"
            )

    conn = None
    cursor = None
    census = Counter()
    existing_batches = 0
    written_batches = 0
    batch_rows: list[dict[str, Any]] = []
    try:
        for index, batch in enumerate(batches, start=1):
            batch_name = assign_batch_name(run_id, index)
            key = _assigned_key(batch_name)
            planned = {o["object_key"] for o in batch["objects"]}
            if object_exists(key):
                # Immutable: every retry reuses the recorded value. That is
                # what makes apply idempotent at the identity level.
                recorded, _digest = _read_assignment_shard(client, bucket, batch_name)
                if {r["object_key"] for r in recorded} != planned:
                    raise ReconcileError(
                        f"assignment shard {key} holds a different object set "
                        f"than batch {batch_name} now plans; the compare run or "
                        f"the caps changed under an immutable assignment"
                    )
                for row in recorded:
                    census[row["id_source"]] += 1
                    if (row.get("input_kind") == "unpacked"
                            and row["id_source"] == ID_ALLOCATED):
                        census["unattributed_pack_member_now_import_bearing"] += 1
                existing_batches += 1
                batch_rows.append({"batch_name": batch_name, "existing": True,
                                   "artifacts": len(recorded),
                                   "silver_rows": batch["silver_rows"],
                                   "bound_by": batch["bound_by"]})
                continue

            if apply and conn is None:
                from shared.db import get_conn

                conn = get_conn()
                cursor = conn.cursor()

            assigned = assign_identities(
                batch["objects"], queue_ids,
                (lambda n: allocate_artifact_ids(cursor, n)) if apply else _no_allocation,
            )
            now = datetime.now(timezone.utc)
            records = [{
                "batch_name": batch_name, "run_id": run_id,
                "object_key": row["object_key"], "artifact_id": row["artifact_id"],
                "id_source": row["id_source"], "listing_id": row["listing_id"],
                "fetched_at": row["fetched_at"], "input_kind": row.get("input_kind"),
                "source_unit": row["source_unit"],
                "silver_rows": row["silver_rows"], "detail_rows": row["detail_rows"],
                "assigned_at": now,
            } for row in assigned]
            for row in records:
                census[row["id_source"]] += 1
                if (row.get("input_kind") == "unpacked"
                        and row["id_source"] == ID_ALLOCATED):
                    census["unattributed_pack_member_now_import_bearing"] += 1
            if apply:
                # Before any database insertion, create-if-absent. A sequence
                # value lost before this write is a harmless bigserial gap;
                # after it, every retry reuses the recorded value.
                _write_parquet_shard(key, _assigned_schema(), records)
                if conn is not None:
                    conn.commit()
                written_batches += 1
            batch_rows.append({"batch_name": batch_name, "existing": False,
                               "artifacts": len(records),
                               "silver_rows": batch["silver_rows"],
                               "bound_by": batch["bound_by"]})
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.commit()
            conn.close()

    bound = Counter(b["bound_by"] for b in batch_rows)
    report = {
        "plan": 145, "stage": 5, "slice": 2, "mode": "assign",
        "run_id": run_id,
        "apply": apply,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "caps": {"max_artifacts": args.max_artifacts,
                 "max_silver_rows": args.max_silver_rows},
        "to_import": {"rows": to_import_rows, "artifacts": len(objects)},
        "identity_census": {
            ID_PRESERVED: census[ID_PRESERVED],
            ID_ALLOCATED: census[ID_ALLOCATED],
            "unattributed_pack_members_now_import_bearing":
                census["unattributed_pack_member_now_import_bearing"],
            "unattributed_pack_members_total": UNATTRIBUTED_PACK_MEMBERS,
        },
        "batches": {
            "total": len(batches),
            "written": written_batches,
            "already_present": existing_batches,
            "bound_by": dict(sorted(bound.items())),
        },
        "batch_list": batch_rows,
    }
    if apply:
        write_bytes(
            report_key,
            (json.dumps(report, indent=2, sort_keys=True, default=str) + "\n").encode(),
            content_type="application/json",
        )
    _print_assign_report(report, apply=apply)
    return 0


def _print_assign_report(report: dict[str, Any], *, apply: bool) -> None:
    census = report["identity_census"]
    batches = report["batches"]
    print()
    print("Plan 145 Stage 5 slice 2 -- assign artifact identity")
    print("=" * 66)
    print(f"mode                 {'APPLY' if apply else 'DRY RUN'}")
    print(f"run_id               {report['run_id']}")
    print(f"to_import rows       {report['to_import']['rows']:>12,}")
    print(f"artifacts            {report['to_import']['artifacts']:>12,}")
    print()
    print(f"  {ID_PRESERVED:<24}{census[ID_PRESERVED]:>12,}")
    print(f"  {ID_ALLOCATED:<24}{census[ID_ALLOCATED]:>12,}")
    print(f"  pack members newly attributed "
          f"{census['unattributed_pack_members_now_import_bearing']:,}"
          f" of {census['unattributed_pack_members_total']:,}")
    print()
    print(f"batches              {batches['total']:>12,}"
          f"  (written {batches['written']:,}, already present "
          f"{batches['already_present']:,})")
    print(f"caps                 {report['caps']['max_artifacts']:,} artifacts / "
          f"{report['caps']['max_silver_rows']:,} rows; bound by "
          f"{batches['bound_by']}")
    if apply:
        print(f"\nwrote {ASSIGNED_PREFIX}/{report['run_id']}-b*.parquet "
              f"(create-if-absent) and the assign report")
    else:
        print(f"\nDRY RUN: planned only. No shard was written and "
              f"{ARTIFACT_ID_SEQUENCE} was not touched.")
    print()


# -- apply -----------------------------------------------------------------

def run_apply(args: argparse.Namespace) -> int:
    import tempfile

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from shared.minio import BUCKET as DEFAULT_BUCKET
    from shared.minio import object_exists

    bucket = args.bucket or DEFAULT_BUCKET
    client = _s3_client()
    apply = bool(args.apply)

    run_id = args.run_id or _discover_compare_run(client, bucket)
    all_names = sorted(
        key.rsplit("/", 1)[-1].removesuffix(".parquet")
        for key in _list_keys(client, bucket, f"{ASSIGNED_PREFIX}/{run_id}-b", ".parquet")
    )
    if not all_names:
        raise ReconcileError(
            f"no assignment shards for run {run_id}; run `assign --apply` first"
        )
    selected = list(args.batch) if args.batch else all_names
    unknown = [name for name in selected if name not in all_names]
    if unknown:
        raise ReconcileError(
            f"unknown batch name(s) {unknown}; this run has {len(all_names)} batches "
            f"from {all_names[0]} to {all_names[-1]}"
        )
    # The blast radius, from the assignment shards, before anything is written.
    plan_rows: list[dict[str, Any]] = []
    for name in selected:
        records, digest = _read_assignment_shard(client, bucket, name)
        plan_rows.append({
            "batch_name": name, "digest": digest, "records": records,
            "artifacts": len(records),
            "silver_rows": sum(int(r["silver_rows"]) for r in records),
            "detail_rows": sum(int(r["detail_rows"]) for r in records),
        })
    _print_apply_plan(run_id, plan_rows, apply=apply,
                      approval=args.maintainer_approval)

    # The canary gate, measured in rows rather than in batches. Counting
    # batches would have let one default-cap batch -- 5,000 artifacts and up to
    # 50,000 silver rows -- through unapproved, two orders of magnitude past
    # the ~500 observations the plan sizes the canary at. The write set is the
    # thing being approved, so it is the thing that is measured.
    selected_rows = sum(entry["silver_rows"] for entry in plan_rows)
    if apply and selected_rows > args.max_unapproved_rows \
            and not args.maintainer_approval:
        raise ReconcileError(
            f"--apply selected {selected_rows:,} silver rows across "
            f"{len(plan_rows)} batch(es), over the {args.max_unapproved_rows:,}-row "
            f"canary budget. Plan 145 allows nothing beyond the canary until "
            f"slice 3 closes the live-state proof and the maintainer approves by "
            f"name: select a canary-sized assignment, or pass "
            f"--maintainer-approval <name>"
        )

    vin_map = _load_vin_snapshot(client, bucket, run_id)
    run_dir = f"{COMPARED_PREFIX}/{run_id}"

    # Batches are ordered by object_key while a to_import shard is one Stage 4
    # work unit, and materialized keys are content-derived -- so the two orders
    # interleave and almost every batch touches almost every shard. Fetching
    # per batch would cost batches x shards requests; localising once costs
    # shards, and every batch then reads from disk. Same shape as compare.
    tmp = tempfile.TemporaryDirectory(prefix="p145apply-")
    tmpdir = Path(tmp.name)
    local_units: dict[str, Path] = {}

    conn = None
    results: list[dict[str, Any]] = []
    totals = Counter()
    try:
        for unit in sorted({r["source_unit"] for e in plan_rows for r in e["records"]}):
            key = f"{run_dir}/to_import/{unit}.parquet"
            if not object_exists(key):
                raise ReconcileError(
                    f"assignment names to_import unit {unit} but "
                    f"s3://{bucket}/{key} is missing"
                )
            dest = tmpdir / f"{unit}.parquet"
            dest.write_bytes(client.get_object(Bucket=bucket, Key=key)["Body"].read())
            local_units[unit] = dest

        if apply:
            from shared.db import get_conn

            conn = get_conn()
        for entry in plan_rows:
            batch_name = entry["batch_name"]
            by_object = {r["object_key"]: r for r in entry["records"]}
            units = sorted({r["source_unit"] for r in entry["records"]})
            silver_rows: list[dict[str, Any]] = []
            price_events: list[dict[str, Any]] = []
            seen_rows: Counter[str] = Counter()
            wanted_keys = pa.array(sorted(by_object))
            for unit in units:
                # Filtered in Arrow, not in Python. A unit shard holds every
                # to_import row of one Stage 4 work unit while a batch keeps
                # only the objects assigned to it, and the two orders
                # interleave -- so materialising the shard and discarding most
                # of it would build tens of millions of dicts across a full run
                # to keep about a million.
                table = pq.read_table(local_units[unit])
                keep = table.filter(
                    pc.is_in(table["object_key"], value_set=wanted_keys),
                )
                for row in keep.to_pylist():
                    object_key = row["object_key"]
                    assignment = by_object[object_key]
                    seen_rows[object_key] += 1
                    silver = build_recovery_silver_row(
                        row, assignment["artifact_id"], vin_map,
                    )
                    silver_rows.append(silver)
                    event = build_recovery_price_event(silver)
                    if event is not None:
                        price_events.append(event)
            drift = [
                {"object_key": k, "assigned": int(by_object[k]["silver_rows"]),
                 "found": seen_rows.get(k, 0)}
                for k in by_object
                if seen_rows.get(k, 0) != int(by_object[k]["silver_rows"])
            ]
            if drift:
                raise ReconcileError(
                    f"batch {batch_name}: {len(drift)} artifact(s) no longer "
                    f"carry the row count their assignment recorded, e.g. "
                    f"{drift[:5]}; the compare output changed under an immutable "
                    f"assignment"
                )
            queue_events = [
                build_recovery_queue_event(by_object[k], batch_name, bucket)
                for k in sorted(by_object)
            ]

            if not apply:
                receipt = "unknown"
                results.append({
                    "batch_name": batch_name, "receipt": receipt, "skipped": False,
                    "artifacts": len(queue_events), "silver": len(silver_rows),
                    "price_events": len(price_events),
                    "queue_events": len(queue_events),
                })
            else:
                outcome = write_import_batch(
                    conn, batch_name, entry["digest"],
                    silver_rows, price_events, queue_events,
                )
                results.append({**outcome, "receipt":
                                "already_committed" if outcome["skipped"] else "written"})
                logger.info(
                    "batch %s: %s (%d artifacts, %d silver, %d price events)",
                    batch_name, "skipped -- receipt already present"
                    if outcome["skipped"] else "committed",
                    outcome["artifacts"], outcome["silver"], outcome["price_events"],
                )
            totals["silver"] += results[-1]["silver"]
            totals["price_events"] += results[-1]["price_events"]
            totals["queue_events"] += results[-1]["queue_events"]
            totals["artifacts"] += results[-1]["artifacts"]
    finally:
        if conn is not None:
            conn.close()
        tmp.cleanup()

    _print_apply_report(run_id, results, totals, apply=apply)
    return 0


_APPLY_WRITES = (
    "staging.silver_observations",
    "staging.price_observation_events",
    "staging.artifacts_queue_events",
    RECEIPT_TABLE,
)
_APPLY_NEVER_TOUCHES = (
    "ops.artifacts_queue",
    "ops.price_observations",
    "ops.vin_to_listing",
    "ops.blocked_cooldown",
    "ops.detail_scrape_claims",
)


def _print_apply_plan(run_id: str, plan_rows: Sequence[dict[str, Any]], *,
                      apply: bool, approval: Optional[str]) -> None:
    """The blast radius, printed before the first write of a production run."""
    print()
    print("Plan 145 Stage 5 slice 2 -- apply the historical write set")
    print("=" * 66)
    print(f"mode                 {'APPLY' if apply else 'DRY RUN'}")
    print(f"run_id               {run_id}")
    if approval:
        print(f"maintainer approval  {approval}")
    print(f"batches selected     {len(plan_rows):>12,}")
    print(f"artifacts            {sum(r['artifacts'] for r in plan_rows):>12,}")
    print(f"silver rows          {sum(r['silver_rows'] for r in plan_rows):>12,}")
    print(f"price events (max)   {sum(r['detail_rows'] for r in plan_rows):>12,}")
    print(f"queue events         {sum(r['artifacts'] for r in plan_rows):>12,}")
    print()
    print("writes               " + "\n                     ".join(_APPLY_WRITES))
    print("never touched        " + "\n                     ".join(_APPLY_NEVER_TOUCHES))
    print()


def _print_apply_report(run_id: str, results: Sequence[dict[str, Any]],
                        totals: Counter, *, apply: bool) -> None:
    skipped = sum(1 for r in results if r.get("skipped"))
    print()
    print(f"batches processed    {len(results):>12,}"
          f"  ({skipped:,} skipped on an existing receipt)")
    print(f"silver rows          {totals['silver']:>12,}")
    print(f"price events         {totals['price_events']:>12,}")
    print(f"queue events         {totals['queue_events']:>12,}")
    if apply:
        print(f"\ncommitted {len(results) - skipped:,} batch(es) of run {run_id}, "
              f"one transaction each, receipt included.")
    else:
        print("\nDRY RUN: built and validated the write set only; no statement "
              "was issued and no row was written.")
    print()


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

    prs = sub.add_parser(
        "parse",
        help="Stage 4: parse flattened April objects to recovery-only Parquet.",
    )
    prs.add_argument("--apply", action="store_true",
                     help="Read and parse objects and write recovery shards. "
                          "Without it, only plan and measure the manifest input set.")
    prs.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    prs.add_argument("--workers", type=int, default=0,
                     help="Worker processes (default: cpu_count - 2, minimum 1).")
    prs.add_argument("--max-units", type=int, default=0,
                     help="Process only the first N manifest work units.")
    prs.add_argument("--force", action="store_true",
                     help="Reprocess units whose two output shards already exist.")
    prs.add_argument("--progress-every", type=int, default=10,
                     help="Log progress every N completed units.")
    prs.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    prs.set_defaults(func=run_parse)

    cmp_ = sub.add_parser(
        "compare",
        help="Stage 5 slice 1: classify parsed observations against deployed "
             "March-May silver into already_represented / to_import / "
             "unclassifiable. Writes no Postgres row and no production object.",
    )
    cmp_.add_argument("--apply", action="store_true",
                      help="Write the compared shards, the input inventory freeze "
                           "and the read-only VIN snapshot. Without it, classify "
                           "and measure only -- no object written, no VIN query.")
    cmp_.add_argument("--probe", action="store_true",
                      help="Run against whatever parsed units exist and write to a "
                           "disposable *_probe prefix; skips the Stage 4 gate. "
                           "Probe output is never promoted.")
    cmp_.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    cmp_.add_argument("--silver-prefix", default=None,
                      help="Override silver_normalized/observations.")
    cmp_.add_argument("--allow-silver-shape-drift", action="store_true",
                      help="Proceed under --apply even when silver is not the "
                           "frozen 9 objects (one per source/month). Never use it "
                           "to skip a maintainer's ruling on the shape change.")
    cmp_.add_argument("--max-units", type=int, default=0,
                      help="Process only the first N parsed row shards (smoke "
                           "test; the gate still runs unless --probe).")
    cmp_.add_argument("--force", action="store_true",
                      help="Re-run even when this run_id's outputs already exist.")
    cmp_.add_argument("--duckdb-threads", type=int, default=1,
                      help="DuckDB thread cap for the silver scan (default 1; the "
                           "host has 4 cores production also needs).")
    cmp_.add_argument("--duckdb-memory-limit", default="2GB",
                      help="DuckDB memory ceiling for the disk-backed silver "
                           "index (default 2GB; empty string disables).")
    cmp_.add_argument("--vin-batch", type=int, default=VIN_BATCH,
                      help="listing_ids per read-only ops.vin_to_listing SELECT.")
    cmp_.add_argument("--max-unclassifiable", type=int, default=MAX_UNCLASSIFIABLE,
                      help="Stop if more no_capture_time rows than this land in "
                           f"unclassifiable (expectation ~{UNCLASSIFIABLE_EXPECTATION}).")
    cmp_.add_argument("--max-no-listing-id", type=int, default=0,
                      help="Stop if more no_listing_id rows than this land in "
                           "unclassifiable (default 0: any non-zero count needs a "
                           "maintainer ruling; set to the measured number after).")
    cmp_.add_argument("--allow-unclassifiable-drift", action="store_true",
                      help="Report an oversized unclassifiable cohort (either "
                           "reason) instead of stopping. Never use it to proceed "
                           "past the gate.")
    cmp_.add_argument("--progress-every", type=int, default=50,
                      help="Log progress every N classified units.")
    cmp_.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    cmp_.set_defaults(func=run_compare)

    asg = sub.add_parser(
        "assign",
        help="Stage 5 slice 2: assign one artifact_id per source object and "
             "write the immutable per-batch assignment shards. The only "
             "database statement it issues is nextval on the sequence.",
    )
    asg.add_argument("--apply", action="store_true",
                     help="Allocate sequence values and write the assignment "
                          "shards. Without it the run plans and reports only, "
                          "and the sequence is never touched.")
    asg.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    asg.add_argument("--run-id", default=None,
                     help="The compare run to assign. Default: the one complete "
                          "run under recovery/plan145/compared/.")
    asg.add_argument("--max-artifacts", type=int, default=MAX_BATCH_ARTIFACTS,
                     help=f"Artifacts per batch (default {MAX_BATCH_ARTIFACTS}). "
                          f"Changing it changes every batch's membership, so a "
                          f"run already assigned under other caps is refused.")
    asg.add_argument("--max-silver-rows", type=int, default=MAX_BATCH_SILVER_ROWS,
                     help=f"Silver rows per batch (default {MAX_BATCH_SILVER_ROWS}). "
                          f"An artifact is never split, so one object larger than "
                          f"this becomes a batch of its own.")
    asg.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    asg.set_defaults(func=run_assign)

    app = sub.add_parser(
        "apply",
        help="Stage 5 slice 2: write one batch's silver rows, historical price "
             "events, recovered queue events and receipt in one transaction. "
             "Never inserts into ops.artifacts_queue.",
    )
    app.add_argument("--apply", action="store_true",
                     help="Actually write. Without it the run builds and "
                          "validates the whole write set, prints the blast "
                          "radius and issues no statement.")
    app.add_argument("--bucket", default=None, help="Override MINIO_BUCKET.")
    app.add_argument("--run-id", default=None,
                     help="The compare run whose assignment shards to apply.")
    app.add_argument("--batch", action="append", default=[],
                     help="Batch name to apply; repeatable. Default: every batch "
                          "of the run, which --apply refuses without "
                          "--maintainer-approval.")
    app.add_argument("--max-unapproved-rows", type=int,
                     default=CANARY_ROW_BUDGET,
                     help=f"Silver rows an --apply run may write without a named "
                          f"maintainer approval (default {CANARY_ROW_BUDGET}). "
                          f"Sized for the plan's ~500-observation canary; one "
                          f"default-cap batch is 50,000.")
    app.add_argument("--maintainer-approval", default=None, metavar="NAME",
                     help="Named maintainer approval for writing more than the "
                          "canary row budget. Plan 145 allows nothing beyond the "
                          "canary until slice 3 closes the live-state proof.")
    app.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING")
    app.set_defaults(func=run_apply)

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
