"""Estimate duplicate compressed HTML storage in MinIO.

This is an operational probe for Plan 110. It scans bronze HTML objects in
chronological order, logs progress by day, and computes exact compressed-byte
duplicates while avoiding unnecessary object downloads:

* every object is listed via MinIO metadata
* objects with unique compressed sizes are not downloaded
* only objects in same-size groups are read and SHA-256 hashed

The duplicate estimate is therefore exact for compressed-byte duplicates and a
lower bound for raw-HTML duplicates.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from shared.minio import BUCKET, get_s3fs

LOG = logging.getLogger("html_duplicate_estimator")


@dataclass
class ObjectInfo:
    path: str
    size: int
    modified: datetime


@dataclass
class SizeBucket:
    count: int = 0
    first_path: str | None = None
    hashes: dict[str, int] = field(default_factory=dict)


@dataclass
class Totals:
    listed_objects: int = 0
    listed_bytes: int = 0
    processed_objects: int = 0
    processed_bytes: int = 0
    same_size_candidate_objects: int = 0
    same_size_candidate_bytes: int = 0
    hashed_objects: int = 0
    hashed_bytes: int = 0
    duplicate_objects: int = 0
    duplicate_bytes: int = 0
    hash_failures: int = 0

    def as_dict(self) -> dict[str, Any]:
        total = self.processed_objects or 1
        total_bytes = self.processed_bytes or 1
        return {
            "listed_objects": self.listed_objects,
            "listed_gib": round(gib(self.listed_bytes), 4),
            "processed_objects": self.processed_objects,
            "processed_gib": round(gib(self.processed_bytes), 4),
            "same_size_candidate_objects": self.same_size_candidate_objects,
            "same_size_candidate_pct": round(
                100 * self.same_size_candidate_objects / total, 4
            ),
            "same_size_candidate_gib": round(gib(self.same_size_candidate_bytes), 4),
            "hashed_objects": self.hashed_objects,
            "hashed_gib": round(gib(self.hashed_bytes), 4),
            "duplicate_objects": self.duplicate_objects,
            "duplicate_object_pct": round(100 * self.duplicate_objects / total, 4),
            "duplicate_gib": round(gib(self.duplicate_bytes), 4),
            "duplicate_storage_pct": round(100 * self.duplicate_bytes / total_bytes, 4),
            "hash_failures": self.hash_failures,
        }


def gib(value: int) -> float:
    return value / 1024 / 1024 / 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate duplicate compressed HTML storage in MinIO."
    )
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--artifact-type", default="detail_page")
    parser.add_argument("--start", help="Inclusive YYYY-MM. Defaults to earliest found.")
    parser.add_argument("--end", help="Inclusive YYYY-MM. Defaults to latest found.")
    parser.add_argument("--log-every", type=int, default=1000)
    parser.add_argument("--limit", type=int, default=0, help="Stop after N objects.")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only list object sizes; do not read/hash same-size candidates.",
    )
    return parser.parse_args()


def month_tuple(value: str) -> tuple[int, int]:
    year, month = value.split("-", 1)
    return int(year), int(month)


def parse_modified(raw: Any, fallback_year: int, fallback_month: int) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime(fallback_year, fallback_month, 1, tzinfo=UTC)


def discover_months(fs, bucket: str, artifact_type: str) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    html_root = f"{bucket}/html"
    for year_entry in fs.ls(html_root, detail=True):
        year_name = str(year_entry.get("name", ""))
        if "/year=" not in year_name:
            continue
        year = int(year_name.rsplit("year=", 1)[1].split("/", 1)[0])
        for month_entry in fs.ls(year_name, detail=True):
            month_name = str(month_entry.get("name", ""))
            if "/month=" not in month_name:
                continue
            month = int(month_name.rsplit("month=", 1)[1].split("/", 1)[0])
            prefix = f"{bucket}/html/year={year}/month={month}/artifact_type={artifact_type}/"
            if fs.exists(prefix):
                months.append((year, month))
    return sorted(set(months))


def selected_months(
    fs, bucket: str, artifact_type: str, start: str | None, end: str | None
) -> list[tuple[int, int]]:
    months = discover_months(fs, bucket, artifact_type)
    if start:
        start_tuple = month_tuple(start)
        months = [m for m in months if m >= start_tuple]
    if end:
        end_tuple = month_tuple(end)
        months = [m for m in months if m <= end_tuple]
    return months


def list_month(fs, bucket: str, artifact_type: str, year: int, month: int) -> list[ObjectInfo]:
    prefix = f"{bucket}/html/year={year}/month={month}/artifact_type={artifact_type}/"
    LOG.info("Listing %s", prefix)
    try:
        entries = fs.ls(prefix, detail=True)
    except FileNotFoundError:
        return []

    objects: list[ObjectInfo] = []
    for entry in entries:
        if entry.get("type") == "directory":
            continue
        path = str(entry.get("name", ""))
        if not path.endswith(".html.zst"):
            continue
        size = int(entry.get("size") or entry.get("Size") or 0)
        modified = parse_modified(
            entry.get("LastModified") or entry.get("last_modified") or entry.get("updated"),
            year,
            month,
        )
        objects.append(ObjectInfo(path=path, size=size, modified=modified))
    objects.sort(key=lambda obj: (obj.modified, obj.path))
    return objects


def hash_object(fs, path: str) -> str:
    digest = hashlib.sha256()
    with fs.open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record_hash(
    hashes: dict[str, int], digest: str, obj: ObjectInfo, totals: Totals
) -> None:
    if hashes.get(digest, 0) > 0:
        totals.duplicate_objects += 1
        totals.duplicate_bytes += obj.size
    hashes[digest] = hashes.get(digest, 0) + 1


def process_object(
    fs,
    obj: ObjectInfo,
    buckets: dict[int, SizeBucket],
    totals: Totals,
    metadata_only: bool,
) -> None:
    totals.processed_objects += 1
    totals.processed_bytes += obj.size

    bucket = buckets[obj.size]
    if bucket.count == 0:
        bucket.count = 1
        bucket.first_path = obj.path
        return

    if bucket.count == 1:
        totals.same_size_candidate_objects += 2
        totals.same_size_candidate_bytes += obj.size * 2
    else:
        totals.same_size_candidate_objects += 1
        totals.same_size_candidate_bytes += obj.size

    bucket.count += 1
    if metadata_only:
        return

    try:
        if bucket.first_path and not bucket.hashes:
            first_obj = ObjectInfo(
                path=bucket.first_path,
                size=obj.size,
                modified=obj.modified,
            )
            first_hash = hash_object(fs, bucket.first_path)
            totals.hashed_objects += 1
            totals.hashed_bytes += obj.size
            record_hash(bucket.hashes, first_hash, first_obj, totals)

        digest = hash_object(fs, obj.path)
        totals.hashed_objects += 1
        totals.hashed_bytes += obj.size
        record_hash(bucket.hashes, digest, obj, totals)
    except Exception as exc:  # noqa: BLE001 - operational script should continue.
        totals.hash_failures += 1
        LOG.warning("Hash failed for %s: %s", obj.path, exc)


def log_summary(label: str, totals: Totals) -> None:
    summary = totals.as_dict()
    LOG.info(
        "%s | processed=%s (%.2f GiB) duplicates=%s (%.4f GiB, %.3f%%) "
        "same_size_candidates=%s (%.2f%%) hashed=%s failures=%s",
        label,
        summary["processed_objects"],
        summary["processed_gib"],
        summary["duplicate_objects"],
        summary["duplicate_gib"],
        summary["duplicate_storage_pct"],
        summary["same_size_candidate_objects"],
        summary["same_size_candidate_pct"],
        summary["hashed_objects"],
        summary["hash_failures"],
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    fs = get_s3fs()

    months = selected_months(fs, args.bucket, args.artifact_type, args.start, args.end)
    if not months:
        LOG.error("No months found for artifact_type=%s", args.artifact_type)
        return 1

    LOG.info(
        "Scanning artifact_type=%s bucket=%s months=%s metadata_only=%s",
        args.artifact_type,
        args.bucket,
        ", ".join(f"{year}-{month:02d}" for year, month in months),
        args.metadata_only,
    )

    totals = Totals()
    buckets: dict[int, SizeBucket] = defaultdict(SizeBucket)
    current_day: date | None = None

    for year, month in months:
        objects = list_month(fs, args.bucket, args.artifact_type, year, month)
        totals.listed_objects += len(objects)
        totals.listed_bytes += sum(obj.size for obj in objects)
        LOG.info(
            "Listed %s objects for %s-%02d (%.2f GiB)",
            len(objects),
            year,
            month,
            gib(sum(obj.size for obj in objects)),
        )

        for obj in objects:
            obj_day = obj.modified.date()
            if current_day is None:
                current_day = obj_day
                LOG.info("Starting day %s", current_day.isoformat())
            elif obj_day != current_day:
                log_summary(f"Completed day {current_day.isoformat()}", totals)
                current_day = obj_day
                LOG.info("Starting day %s", current_day.isoformat())

            process_object(fs, obj, buckets, totals, args.metadata_only)

            if args.log_every and totals.processed_objects % args.log_every == 0:
                log_summary("Progress", totals)

            if args.limit and totals.processed_objects >= args.limit:
                LOG.info("Stopping at --limit=%s", args.limit)
                log_summary("Final", totals)
                if args.json_out:
                    args.json_out.write_text(json.dumps(totals.as_dict(), indent=2))
                return 0

    if current_day is not None:
        log_summary(f"Completed day {current_day.isoformat()}", totals)
    log_summary("Final", totals)

    if args.json_out:
        args.json_out.write_text(json.dumps(totals.as_dict(), indent=2))
        LOG.info("Wrote %s", args.json_out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
