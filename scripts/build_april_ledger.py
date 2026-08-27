# ruff: noqa: E501
"""Build the immutable, row-complete Plan 145 April reconciliation ledger.

This command is intentionally read-only with respect to production: it lists and
reads legacy Parquet, pack indexes, and lake Parquet only.  The result is the
control plane for CAR-19 through CAR-22; it never derives a recovery manifest or
writes a recovered observation.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Sequence

APRIL_LEGACY_PREFIX = "artifacts/year=2026/month=4/"
APRIL_SIDECAR_PREFIX = "html_packs/detail_page/2026/04/"
APRIL_START = datetime(2026, 4, 1, tzinfo=timezone.utc)
APRIL_END = datetime(2026, 5, 1, tzinfo=timezone.utc)
SILVER_WINDOW_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
SILVER_WINDOW_END = datetime(2026, 6, 1, tzinfo=timezone.utc)

# These are census facts from Plan 145 / superseded Plan 137, not tolerances.
BASELINE = {
    "objects": 1299,
    "rows": 954817,
    "empty": 43019,
    "pack_matches": 423304,
    "pack_absent": 488494,
    "sidecars": 32,
    "sidecar_members": 557065,
    "orphan_members": 42276,
    "orphan_legacy_rows": 42976,
    "recover_car19": 37715,
    "cohort_no_priced_observation": 11199,
    "cohort_listing_absent": 138,
    "cohort_bracketed_price_changed": 270,
}

LEDGER_COLUMNS = (
    "legacy_key",
    "row_group",
    "row_offset",
    "legacy_artifact_type",
    "artifact_id",
    "run_id",
    "source",
    "search_key",
    "search_scope",
    "listing_id",
    "url",
    "fetched_at",
    "http_status",
    "content_length",
    "legacy_object_size",
    "legacy_etag",
    "legacy_last_modified",
    "stored_sha256",
    "sha256",
    "hash_matches_stored",
    "source_key",
    "pack_key",
    "sidecar_key",
    "sidecar_row",
    "raw_sha256",
    "sidecar_match_count",
    "sidecar_artifact_id",
    "queue_event_count",
    "silver_match_count",
    "information_value_cohorts",
    "preservation_reasons",
    "disposition",
    "unresolved_reason",
)


class LedgerDriftError(RuntimeError):
    """A production population differs from the frozen Plan 145 census."""


def _json_default(value: Any) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return (
        result.replace(tzinfo=timezone.utc)
        if result.tzinfo is None
        else result.astimezone(timezone.utc)
    )


def _time_text(value: Any) -> str | None:
    parsed = _time(value)
    return parsed.isoformat() if parsed else None


def _content(source: Mapping[str, Any]) -> bytes:
    value = source.get("content_bytes")
    if value is None:
        value = source.get("html", b"")
    return value.encode() if isinstance(value, str) else (value or b"")


def canonical_fingerprint(rows: Iterable[Mapping[str, Any]]) -> str:
    """Fingerprint canonical rows in their required deterministic order."""
    digest = hashlib.sha256()
    for row in rows:
        digest.update(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=_json_default).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _sidecars_by_hash(sidecars: Iterable[Mapping[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for ordinal, source in enumerate(sidecars):
        raw_hash = str(source.get("raw_sha256") or "")
        if not raw_hash:
            continue
        candidate = dict(source)
        candidate.setdefault("sidecar_row", ordinal)
        result[raw_hash].append(candidate)
    for candidates in result.values():
        candidates.sort(
            key=lambda row: (
                str(row.get("source_key") or ""),
                str(row.get("pack_key") or ""),
                str(row.get("artifact_id") or ""),
            )
        )
    return result


def _silver_index(silver_rows: Iterable[Mapping[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for source in silver_rows:
        listing_id = source.get("listing_id")
        fetched_at = _time(source.get("fetched_at"))
        if listing_id not in (None, "") and fetched_at:
            index[str(listing_id)].append({**source, "_fetched_at": fetched_at})
    for rows in index.values():
        rows.sort(key=lambda row: row["_fetched_at"])
    return index


def _information_cohorts(
    listing_id: Any, fetched_at: Any, silver_by_listing: Mapping[str, List[Dict[str, Any]]]
) -> List[str]:
    """Encode Stage 0f exactly: no-priced, absent, and changed brackets only."""
    if listing_id in (None, "") or not (when := _time(fetched_at)):
        return []
    observations = silver_by_listing.get(str(listing_id), [])
    cohorts: List[str] = []
    if not observations:
        return ["listing_absent_from_silver", "no_priced_observation"]
    priced = [row for row in observations if row.get("price") is not None]
    if not priced:
        return ["no_priced_observation"]
    if min(abs((row["_fetched_at"] - when).total_seconds()) for row in priced) <= 60:
        return cohorts
    before = [row for row in priced if row["_fetched_at"] < when]
    after = [row for row in priced if row["_fetched_at"] > when]
    prior = before[-1] if before else None
    following = after[0] if after else None

    def within_day(row: Dict[str, Any] | None) -> bool:
        return bool(row) and abs((row["_fetched_at"] - when).total_seconds()) <= 86400

    if within_day(prior) and within_day(following) and prior.get("price") != following.get("price"):
        cohorts.append("bracketed_price_changed")
    return cohorts


def _disposition(row: Mapping[str, Any]) -> str:
    if row.get("unresolved_reason"):
        return "unresolved"
    if not row.get("content_length"):
        return "empty"
    status = int(row.get("http_status") or 0)
    if status == 403:
        return "challenge_page"
    if status >= 500:
        return "preserve_car21"
    if status != 200:
        return "unresolved"
    if (
        row.get("sidecar_artifact_id") is None
        and row.get("source_key")
        and not row.get("silver_match_count")
    ):
        return "recover_car19"
    if row.get("information_value_cohorts"):
        return "recover_car20"
    return "redundant"


def iter_ledger(
    legacy_rows: Iterable[Mapping[str, Any]],
    sidecars: Iterable[Mapping[str, Any]],
    *,
    queue_events: Iterable[Mapping[str, Any]] = (),
    silver_rows: Iterable[Mapping[str, Any]] = (),
) -> Iterator[Dict[str, Any]]:
    """Yield row-complete ledger entries; legacy IDs are never a join key."""
    sidecar_by_hash = _sidecars_by_hash(sidecars)
    current_ids = {
        entry.get("artifact_id")
        for candidates in sidecar_by_hash.values()
        for entry in candidates
        if entry.get("artifact_id") is not None
    }
    queue_counts = Counter(
        event.get("artifact_id")
        for event in queue_events
        if event.get("artifact_id") in current_ids
    )
    silver_by_listing = _silver_index(silver_rows)
    for source in legacy_rows:
        content = _content(source)
        computed_hash = hashlib.sha256(content).hexdigest() if content else None
        stored_hash = source.get("sha256")
        matches = sidecar_by_hash.get(computed_hash or "", [])
        match = matches[0] if matches else None
        listing_id = source.get("listing_id")
        fetched_at = _time_text(source.get("fetched_at"))
        exact_silver = [
            row
            for row in silver_by_listing.get(str(listing_id), [])
            if fetched_at and _time_text(row["_fetched_at"]) == fetched_at
        ]
        entry: Dict[str, Any] = {name: source.get(name) for name in LEDGER_COLUMNS}
        entry.update(
            stored_sha256=stored_hash,
            sha256=computed_hash,
            hash_matches_stored=(computed_hash == stored_hash if computed_hash else None),
            content_length=len(content),
            fetched_at=fetched_at,
            legacy_last_modified=_time_text(source.get("legacy_last_modified")),
            listing_id=str(listing_id) if listing_id not in (None, "") else None,
            information_value_cohorts=[],
            preservation_reasons=[],
            queue_event_count=0,
            sidecar_match_count=len(matches),
            silver_match_count=len(exact_silver),
            unresolved_reason=None,
        )
        if computed_hash and stored_hash and computed_hash != stored_hash:
            entry["unresolved_reason"] = "stored_sha256_mismatch"
        if content and not computed_hash:
            entry["unresolved_reason"] = "missing_recomputed_sha256"
        if match:
            entry.update(
                source_key=match.get("source_key"),
                pack_key=match.get("pack_key"),
                sidecar_key=match.get("sidecar_key"),
                sidecar_row=match.get("sidecar_row"),
                raw_sha256=match.get("raw_sha256"),
                sidecar_artifact_id=match.get("artifact_id"),
            )
            entry["queue_event_count"] = queue_counts.get(match.get("artifact_id"), 0)
            artifact_ids = {candidate.get("artifact_id") for candidate in matches}
            if len(artifact_ids) > 1:
                entry["unresolved_reason"] = "ambiguous_sidecar_artifact_id"
        status = int(entry.get("http_status") or 0)
        if content and status == 200 and not entry["listing_id"]:
            entry["unresolved_reason"] = entry["unresolved_reason"] or "missing_listing_id"
        entry["information_value_cohorts"] = (
            _information_cohorts(entry["listing_id"], entry["fetched_at"], silver_by_listing)
            if status == 200
            else []
        )
        if status >= 500:
            entry["preservation_reasons"].append("five_xx_body")
        if "listing_absent_from_silver" in entry["information_value_cohorts"]:
            entry["preservation_reasons"].append("listing_absent_from_silver")
        entry["disposition"] = _disposition(entry)
        yield entry


def build_ledger(
    legacy_rows: Iterable[Mapping[str, Any]], sidecars: Iterable[Mapping[str, Any]], **kwargs: Any
) -> List[Dict[str, Any]]:
    """Fixture-friendly materialized wrapper around :func:`iter_ledger`."""
    return sorted(
        iter_ledger(legacy_rows, sidecars, **kwargs),
        key=lambda row: (
            str(row.get("sha256") or ""),
            str(row.get("legacy_key") or ""),
            int(row.get("row_group") or 0),
            int(row.get("row_offset") or 0),
        ),
    )


def _listed_keys(client: Any, bucket: str, prefix: str) -> Iterator[Dict[str, Any]]:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from sorted(page.get("Contents", []), key=lambda item: item["Key"])


def iter_legacy_parquet_rows(
    client: Any, bucket: str, prefix: str = APRIL_LEGACY_PREFIX
) -> Iterator[Dict[str, Any]]:
    """Read one object and row group at a time, retaining no corpus-sized HTML."""
    import pyarrow.parquet as pq

    for meta in _listed_keys(client, bucket, prefix):
        key = meta["Key"]
        if not key.endswith(".parquet"):
            continue
        parquet = pq.ParquetFile(
            io.BytesIO(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        )
        for group in range(parquet.num_row_groups):
            for offset, row in enumerate(parquet.read_row_group(group).to_pylist()):
                yield {
                    **row,
                    "legacy_key": key,
                    "row_group": group,
                    "row_offset": offset,
                    "legacy_object_size": meta.get("Size"),
                    "legacy_etag": meta.get("ETag"),
                    "legacy_last_modified": meta.get("LastModified"),
                    "content_bytes": row.get("html") or b"",
                }


def load_sidecars(
    client: Any, bucket: str, prefix: str = APRIL_SIDECAR_PREFIX
) -> List[Dict[str, Any]]:
    """Read the April sidecars once; a sidecar artifact ID is current-system metadata."""
    import pyarrow.parquet as pq

    entries: List[Dict[str, Any]] = []
    for meta in _listed_keys(client, bucket, prefix):
        key = meta["Key"]
        if key.endswith(".idx.parquet"):
            table = pq.read_table(
                io.BytesIO(client.get_object(Bucket=bucket, Key=key)["Body"].read())
            )
            pack_key = key[: -len(".idx.parquet")] + ".zpack"
            entries.extend(
                {**row, "sidecar_key": key, "pack_key": pack_key} for row in table.to_pylist()
            )
    return entries


def iter_legacy_identities(client: Any, bucket: str) -> Iterator[Dict[str, Any]]:
    """Read only identity columns for the silver join's bounded March--May scan."""
    import pyarrow.parquet as pq

    for meta in _listed_keys(client, bucket, APRIL_LEGACY_PREFIX):
        key = meta["Key"]
        if not key.endswith(".parquet"):
            continue
        parquet = pq.ParquetFile(
            io.BytesIO(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        )
        required = {"listing_id", "fetched_at"}
        missing = required.difference(parquet.schema.names)
        if missing:
            raise LedgerDriftError(f"{key} is missing required identity columns: {sorted(missing)}")
        for group in range(parquet.num_row_groups):
            yield from parquet.read_row_group(
                group, columns=["listing_id", "fetched_at"]
            ).to_pylist()


def load_lake_joins(
    *,
    bucket: str,
    sidecars: Sequence[Mapping[str, Any]],
    legacy_identities: Iterable[Mapping[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Load only valid queue and silver joins from the historical lake.

    The temporary ``current_sidecars`` relation is deliberately the only route
    into queue events.  No legacy artifact ID is registered or referenced.
    Silver is constrained to legacy listing identities and the documented
    March--May window, then joined by exact ``(listing_id, fetched_at)`` in the
    ledger core.
    """
    import pyarrow as pa

    from shared.duckdb_s3 import get_duckdb_s3_connection

    current_sidecars = [
        {"artifact_id": row.get("artifact_id")}
        for row in sidecars
        if row.get("artifact_id") is not None
    ]
    identities = [
        {"listing_id": str(row["listing_id"])}
        for row in legacy_identities
        if row.get("listing_id") not in (None, "")
    ]
    if not identities:
        raise LedgerDriftError("legacy identity scan found no listing_id values")
    con = get_duckdb_s3_connection()
    try:
        con.register("current_sidecars", pa.Table.from_pylist(current_sidecars))
        con.register("legacy_listing_ids", pa.Table.from_pylist(identities))
        queue_events = (
            con.execute(
                """
            SELECT e.artifact_id
            FROM read_parquet(
                ?, hive_partitioning=true, union_by_name=true
            ) AS e
            INNER JOIN (SELECT DISTINCT artifact_id FROM current_sidecars) AS s
                ON s.artifact_id = e.artifact_id
            """,
                [f"s3://{bucket}/ops_normalized/artifacts_queue_events/**/*.parquet"],
            )
            .fetch_arrow_table()
            .to_pylist()
        )
        silver_rows = (
            con.execute(
                """
            SELECT s.listing_id, s.fetched_at, s.price
            FROM read_parquet(
                ?, hive_partitioning=true, union_by_name=true
            ) AS s
            INNER JOIN (SELECT DISTINCT listing_id FROM legacy_listing_ids) AS l
                ON l.listing_id = CAST(s.listing_id AS VARCHAR)
            WHERE s.fetched_at >= ? AND s.fetched_at < ?
            """,
                [
                    f"s3://{bucket}/silver_normalized/observations/**/*.parquet",
                    SILVER_WINDOW_START,
                    SILVER_WINDOW_END,
                ],
            )
            .fetch_arrow_table()
            .to_pylist()
        )
        return queue_events, silver_rows
    finally:
        con.close()


def write_ledger(
    rows: Sequence[Mapping[str, Any]],
    output: Path,
    *,
    report_extra: Mapping[str, Any] | None = None,
) -> str:
    """Write deterministic Parquet plus machine- and human-readable reconciliation reports."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output.parent.mkdir(parents=True, exist_ok=True)
    normalized = [{name: row.get(name) for name in LEDGER_COLUMNS} for row in rows]
    pq.write_table(
        pa.Table.from_pylist(normalized), output, compression="zstd", use_dictionary=False
    )
    fingerprint = canonical_fingerprint(normalized)
    dispositions = Counter(row["disposition"] for row in normalized)
    cohorts = Counter(cohort for row in normalized for cohort in row["information_value_cohorts"])
    report: Dict[str, Any] = {
        "rows": len(normalized),
        "dispositions": dict(sorted(dispositions.items())),
        "information_value_cohorts": dict(sorted(cohorts.items())),
        "fingerprint": fingerprint,
    }
    if report_extra:
        report.update(report_extra)
    output.with_suffix(".report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=_json_default) + "\n"
    )
    output.with_suffix(".report.md").write_text(
        "# Plan 145 April ledger\n\n"
        + f"Fingerprint: `{fingerprint}`\n\n"
        + "| disposition | rows |\n|---|---:|\n"
        + "".join(f"| {key} | {value} |\n" for key, value in sorted(dispositions.items()))
        + "\n| Stage 0f cohort | rows |\n|---|---:|\n"
        + "".join(f"| {key} | {value} |\n" for key, value in sorted(cohorts.items())),
    )
    output.with_suffix(".sha256").write_text(fingerprint + "\n")
    return fingerprint


def assert_baseline(
    rows: Sequence[Mapping[str, Any]], *, object_count: int, sidecars: Sequence[Mapping[str, Any]]
) -> None:
    counts = Counter(row["disposition"] for row in rows)
    packed = sum(1 for row in rows if row.get("raw_sha256"))
    orphan_hashes = {
        str(row["raw_sha256"])
        for row in sidecars
        if row.get("raw_sha256") and row.get("artifact_id") is None
    }
    orphan = sum(1 for row in rows if row.get("sha256") in orphan_hashes)
    cohorts = Counter(cohort for row in rows for cohort in row["information_value_cohorts"])
    observed = {
        "objects": object_count,
        "rows": len(rows),
        "empty": counts["empty"],
        "pack_matches": packed,
        "pack_absent": len(rows) - packed - counts["empty"],
        "sidecars": len({row.get("sidecar_key") for row in sidecars}),
        "sidecar_members": len(sidecars),
        "orphan_members": len(orphan_hashes),
        "orphan_legacy_rows": orphan,
        "recover_car19": counts["recover_car19"],
        "cohort_no_priced_observation": cohorts["no_priced_observation"],
        "cohort_listing_absent": cohorts["listing_absent_from_silver"],
        "cohort_bracketed_price_changed": cohorts["bracketed_price_changed"],
    }
    drift = {
        key: {"expected": value, "actual": observed[key]}
        for key, value in BASELINE.items()
        if observed[key] != value
    }
    if drift:
        raise LedgerDriftError("Plan 145 baseline drift: " + json.dumps(drift, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--fixture",
        type=Path,
        help="JSON object with legacy_rows, sidecars, queue_events, and silver_rows",
    )
    source.add_argument(
        "--production", action="store_true", help="scan the exact immutable April prefixes"
    )
    parser.add_argument("--bucket", help="object-store bucket (defaults to shared.minio.BUCKET)")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.fixture:
        fixture = json.loads(args.fixture.read_text())
        rows = build_ledger(
            fixture["legacy_rows"],
            fixture["sidecars"],
            queue_events=fixture.get("queue_events", []),
            silver_rows=fixture.get("silver_rows", []),
        )
        print(write_ledger(rows, args.output))
        return 0
    # Production context deliberately cannot accept arbitrary prefixes.  A drift
    # must stop CAR-13, not silently create a ledger for a smaller population.
    from shared.minio import BUCKET, get_boto3_client

    client, bucket = get_boto3_client(), args.bucket or BUCKET
    object_meta = [
        item
        for item in _listed_keys(client, bucket, APRIL_LEGACY_PREFIX)
        if item["Key"].endswith(".parquet")
    ]
    sidecars = load_sidecars(client, bucket)
    queue_events, silver_rows = load_lake_joins(
        bucket=bucket, sidecars=sidecars, legacy_identities=iter_legacy_identities(client, bucket)
    )
    rows = build_ledger(
        iter_legacy_parquet_rows(client, bucket),
        sidecars,
        queue_events=queue_events,
        silver_rows=silver_rows,
    )
    assert_baseline(rows, object_count=len(object_meta), sidecars=sidecars)
    print(
        write_ledger(
            rows,
            args.output,
            report_extra={
                "objects": len(object_meta),
                "sidecars": len({row.get("sidecar_key") for row in sidecars}),
            },
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
