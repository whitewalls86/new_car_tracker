"""Dry-run-first replay of reviewed Plan 145 recovery manifests.

This module deliberately has no queue, pack, or sidecar write capability.  A
manifest is an immutable projection of the April ledger; replaying it only
reads source HTML and, with explicit approval, appends historical rows.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from processing.processors import parse_cars_detail_page_html_v1
from processing.writers.detail_writer import write_detail_active, write_detail_unlisted
from shared.minio import read_html

ALLOWED_DISPOSITIONS = {"recover_car19", "recover_car20"}


class BackfillRefusal(ValueError):
    """A manifest row is not safe to write."""


def sha256_json(value: Any) -> str:
    """Fingerprint canonical JSON, independent of source formatting."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def load_manifest(path: Path, expected_fingerprint: str) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data["rows"] if isinstance(data, dict) else data
    actual = data.get("fingerprint") if isinstance(data, dict) else sha256_json(rows)
    if actual != expected_fingerprint:
        raise BackfillRefusal("manifest fingerprint does not match --manifest-fingerprint")
    if actual != sha256_json(rows):
        raise BackfillRefusal("manifest content does not match its fingerprint")
    return rows


def _parse_time(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


def validate_row(row: Dict[str, Any], ledger_fingerprint: str) -> None:
    if row.get("ledger_fingerprint") != ledger_fingerprint:
        raise BackfillRefusal("ledger fingerprint mismatch")
    if row.get("disposition") not in ALLOWED_DISPOSITIONS:
        raise BackfillRefusal("disposition is not an approved recovery disposition")
    if int(row.get("http_status", 0)) != 200:
        raise BackfillRefusal("only HTTP 200 rows may be recovered")
    for field in ("source_key", "sha256", "listing_id", "fetched_at", "artifact_id"):
        if row.get(field) in (None, ""):
            raise BackfillRefusal(f"missing required field: {field}")


def process_row(row: Dict[str, Any], ledger_fingerprint: str, *, apply: bool) -> Dict[str, Any]:
    """Read, verify and parse one row; only write when ``apply`` is true."""
    validate_row(row, ledger_fingerprint)
    raw = read_html(row["source_key"])
    actual_hash = hashlib.sha256(raw).hexdigest()
    if actual_hash != row["sha256"]:
        raise BackfillRefusal("source bytes sha256 does not match manifest")
    primary, _carousel, _meta = parse_cars_detail_page_html_v1(
        raw.decode("utf-8", errors="replace"), row.get("url"),
    )
    if primary.get("listing_id") != str(row["listing_id"]):
        raise BackfillRefusal("parsed listing_id does not match manifest identity")
    state = primary.get("listing_state", "active")
    if state not in {"active", "unlisted"}:
        raise BackfillRefusal(f"parsed listing state is not recoverable: {state}")
    outcome = {"listing_id": str(row["listing_id"]), "state": state, "written": False}
    if apply:
        writer = write_detail_unlisted if state == "unlisted" else write_detail_active
        result = writer(
            primary, int(row["artifact_id"]), _parse_time(row["fetched_at"]),
            str(row["listing_id"]), None, backfill=True,
        ) if state == "unlisted" else writer(
            primary, [], int(row["artifact_id"]), _parse_time(row["fetched_at"]),
            str(row["listing_id"]), None, backfill=True,
        )
        outcome.update(result)
        outcome["written"] = True
    return outcome


def run_manifest(
    rows: Iterable[Dict[str, Any]], *, ledger_fingerprint: str, apply: bool, cap: int,
) -> Dict[str, Any]:
    if cap < 1:
        raise ValueError("cap must be at least one")
    counters: Counter[str] = Counter()
    results: List[Dict[str, Any]] = []
    for row in list(rows)[:cap]:
        counters["attempted"] += 1
        try:
            result = process_row(row, ledger_fingerprint, apply=apply)
        except BackfillRefusal as exc:
            counters["refused"] += 1
            results.append({"row": row.get("sha256"), "refusal": str(exc)})
        except Exception as exc:  # preserve a safe, reviewable resume point
            counters["failed"] += 1
            results.append({"row": row.get("sha256"), "error": f"{type(exc).__name__}: {exc}"})
        else:
            counters["written" if apply else "dry_run"] += 1
            results.append(result)
    return {**counters, "apply": apply, "results": results}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-fingerprint", required=True)
    parser.add_argument("--ledger-fingerprint", required=True)
    parser.add_argument("--cap", type=int, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="append historical rows")
    args = parser.parse_args(argv)
    rows = load_manifest(args.manifest, args.manifest_fingerprint)
    receipt = run_manifest(rows, ledger_fingerprint=args.ledger_fingerprint, apply=args.apply, cap=args.cap)
    receipt.update({"ledger_fingerprint": args.ledger_fingerprint,
                    "manifest_fingerprint": args.manifest_fingerprint})
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps({key: receipt.get(key, 0) for key in ("attempted", "written", "dry_run", "refused", "failed")}, sort_keys=True))
    return 1 if receipt.get("failed") or receipt.get("refused") else 0


if __name__ == "__main__":
    raise SystemExit(main())
