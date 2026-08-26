"""Construct a deterministic, row-complete Plan 145 ledger from legacy rows.

The production adapter may stream Parquet row groups into ``build_ledger``;
the core is deliberately pure so fixture corpora exercise the same invariants.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


LEDGER_COLUMNS = (
    "legacy_key", "row_group", "row_offset", "artifact_id", "run_id", "source",
    "search_key", "search_scope", "listing_id", "url", "fetched_at", "http_status",
    "content_length", "legacy_object_size", "legacy_etag", "legacy_last_modified",
    "stored_sha256", "sha256",
    "hash_matches_stored", "source_key", "pack_key", "sidecar_key", "raw_sha256",
    "disposition", "unresolved_reason",
)


def canonical_fingerprint(rows: Sequence[Dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def _disposition(row: Dict[str, Any]) -> str:
    if row.get("unresolved_reason"):
        return "unresolved"
    if not row.get("content_bytes"):
        return "empty"
    if int(row.get("http_status") or 0) == 403:
        return "challenge_page"
    if row.get("orphan") and int(row.get("http_status") or 0) == 200:
        return "recover_car19"
    if row.get("information_value"):
        return "recover_car20"
    return "redundant"


def build_ledger(legacy_rows: Iterable[Dict[str, Any]], sidecars: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Join legacy rows to sidecars by content hash, never by artifact ID."""
    sidecar_by_hash = {str(row["raw_sha256"]): row for row in sidecars if row.get("raw_sha256")}
    ledger: List[Dict[str, Any]] = []
    for source in legacy_rows:
        content = source.get("content_bytes") or source.get("html") or b""
        if isinstance(content, str):
            content = content.encode()
        digest = hashlib.sha256(content).hexdigest() if content else None
        stored = source.get("sha256")
        entry: Dict[str, Any] = {name: source.get(name) for name in LEDGER_COLUMNS}
        entry["stored_sha256"] = stored
        entry["sha256"] = digest
        entry["content_length"] = len(content)
        entry["hash_matches_stored"] = digest == stored if digest else None
        if digest and stored and digest != stored:
            entry["unresolved_reason"] = "stored_sha256_mismatch"
        match = sidecar_by_hash.get(digest or "")
        if match:
            entry.update({key: match.get(key) for key in ("source_key", "pack_key", "sidecar_key", "raw_sha256")})
        entry["disposition"] = _disposition({**source, **entry})
        ledger.append(entry)
    return sorted(ledger, key=lambda row: tuple(str(row.get(key) or "") for key in ("legacy_key", "row_group", "row_offset")))


def _listed_keys(client: Any, bucket: str, prefix: str) -> Iterable[Dict[str, Any]]:
    """Yield object metadata deterministically without any object mutation."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from sorted(page.get("Contents", []), key=lambda item: item["Key"])


def iter_legacy_parquet_rows(client: Any, bucket: str, prefix: str) -> Iterable[Dict[str, Any]]:
    """Stream each Parquet row group; never materialize the legacy corpus at once."""
    import pyarrow.parquet as pq

    for meta in _listed_keys(client, bucket, prefix):
        key = meta["Key"]
        if not key.endswith(".parquet"):
            continue
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        parquet = pq.ParquetFile(io.BytesIO(body))
        for group in range(parquet.num_row_groups):
            for offset, row in enumerate(parquet.read_row_group(group).to_pylist()):
                row.update({"legacy_key": key, "row_group": group, "row_offset": offset,
                            "legacy_object_size": meta.get("Size"), "legacy_etag": meta.get("ETag"),
                            "legacy_last_modified": meta.get("LastModified")})
                # The historical source column is HTML bytes, not its length.
                row["content_bytes"] = row.get("html") or b""
                yield row


def load_sidecars(client: Any, bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """Read each pack sidecar once and retain its source/hash coordinates."""
    import pyarrow.parquet as pq

    entries: List[Dict[str, Any]] = []
    for meta in _listed_keys(client, bucket, prefix):
        key = meta["Key"]
        if not key.endswith(".idx.parquet"):
            continue
        table = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key=key)["Body"].read()))
        pack_key = key[: -len(".idx.parquet")] + ".zpack"
        for row in table.to_pylist():
            entries.append({**row, "sidecar_key": key, "pack_key": pack_key})
    return entries


def write_ledger(rows: Sequence[Dict[str, Any]], output: Path) -> str:
    """Write Parquet ledger, JSON count report, and its content fingerprint."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output.parent.mkdir(parents=True, exist_ok=True)
    normalized = [{key: row.get(key) for key in LEDGER_COLUMNS} for row in rows]
    pq.write_table(pa.Table.from_pylist(normalized), output, compression="zstd")
    fingerprint = canonical_fingerprint(normalized)
    report = {"rows": len(normalized), "dispositions": {}}
    for row in normalized:
        report["dispositions"][row["disposition"]] = report["dispositions"].get(row["disposition"], 0) + 1
    output.with_suffix(".report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    output.with_suffix(".sha256").write_text(fingerprint + "\n")
    return fingerprint


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--legacy-json", type=Path, help="fixture legacy rows")
    source.add_argument("--legacy-prefix", help="production legacy Parquet prefix")
    parser.add_argument("--sidecars-json", type=Path, help="fixture sidecar rows")
    parser.add_argument("--sidecar-prefix", help="production pack sidecar prefix")
    parser.add_argument("--bucket", help="object-store bucket (defaults to shared.minio.BUCKET)")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if bool(args.legacy_json) != bool(args.sidecars_json) or bool(args.legacy_prefix) != bool(args.sidecar_prefix):
        parser.error("provide both fixture inputs or both production prefixes")
    if args.legacy_json:
        legacy_rows = json.loads(args.legacy_json.read_text())
        sidecars = json.loads(args.sidecars_json.read_text())
    else:
        from shared.minio import BUCKET, get_boto3_client
        client = get_boto3_client()
        bucket = args.bucket or BUCKET
        legacy_rows = iter_legacy_parquet_rows(client, bucket, args.legacy_prefix)
        sidecars = load_sidecars(client, bucket, args.sidecar_prefix)
    rows = build_ledger(legacy_rows, sidecars)
    print(write_ledger(rows, args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
