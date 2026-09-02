"""Plan 131 Stage 3: prove the pack read path, and measure what it costs.

Stage 4 deletes source objects on one claim: **that the production read path
returns an artifact's exact bytes after its object is gone.** This verifier tests
that claim while the safety net is still in place — every source object still
exists, so a failure here is a finding rather than a loss.

For a sample of packed keys it reads each artifact **twice**:

* through the source object, exactly as ``read_html`` does today; and
* through ``read_packed_html``, which is the fallback ``read_html`` will take
  once the object is gone,

and asserts the two are byte-identical and that both match the sidecar's own
``raw_sha256``. Anything else is reported, loudly, per key.

It also reports single-artifact extraction latency p50/p95, which Plan 131's
success criteria require **measured and accepted, not assumed**. Three numbers,
because they are three different questions:

* ``object``      — today's read, the baseline the regression is against.
* ``pack_cold``   — every cache dropped first: the single-artifact random-access
                    case, and the honest worst case.
* ``pack_warm``   — caches left alone: the reprocessing case, which walks many
                    members of the same pack.

**Read-only.** It fetches, decompresses and hashes. It writes nothing, deletes
nothing, and needs no dictionary configured — frames name their own dictionary.

Run it in the archiver container, whose app root is ``/app``::

    docker exec -w /app cartracker-archiver \\
        python -m archiver.processors.verify_pack_read_path \\
        --year 2026 --month 4 --per-pack 5
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import random
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

LOG = logging.getLogger("verify_pack_read_path")

DEFAULT_ARTIFACT_TYPE = "detail_page"


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def list_sidecars(client: Any, bucket: str, prefix: str) -> List[str]:
    paginator = client.get_paginator("list_objects_v2")
    return sorted(
        entry["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for entry in page.get("Contents", [])
        if entry["Key"].endswith(".idx.parquet")
    )


def sample_members(
    client: Any, bucket: str, sidecars: Sequence[str], per_pack: int, seed: int
) -> List[Dict[str, Any]]:
    """Take *per_pack* members from every sidecar, deterministically.

    Sampling every pack rather than N members overall is deliberate: a pack is
    the unit that can be individually wrong, and the April run's own
    measurements varied pack by pack.
    """
    import pyarrow.parquet as pq

    rng = random.Random(seed)
    out: List[Dict[str, Any]] = []
    for sidecar in sidecars:
        body = client.get_object(Bucket=bucket, Key=sidecar)["Body"].read()
        table = pq.read_table(
            io.BytesIO(body), columns=["source_key", "raw_sha256", "length"]
        )
        rows = table.num_rows
        if not rows:
            LOG.warning("sidecar %s has no rows", sidecar)
            continue
        picks = rng.sample(range(rows), min(per_pack, rows))
        picked = table.take(picks).to_pylist()
        for record in picked:
            record["index_key"] = sidecar
            record["pack_key"] = sidecar[: -len(".idx.parquet")] + ".zpack"
            out.append(record)
    return out


# ---------------------------------------------------------------------------
# The two reads
# ---------------------------------------------------------------------------

def read_source_object(client: Any, bucket: str, key: str) -> bytes:
    """Today's read path, with the pack fallback deliberately not involved."""
    from shared.compression import decompress_frame

    return decompress_frame(client.get_object(Bucket=bucket, Key=key)["Body"].read())


def verify_one(
    client: Any, bucket: str, record: Dict[str, Any], *, cold: bool
) -> Dict[str, Any]:
    from shared.minio import clear_pack_caches, read_packed_html

    key = record["source_key"]
    outcome: Dict[str, Any] = {
        "source_key": key,
        "pack_key": record["pack_key"],
        "object_present": True,
        "match": False,
        "error": None,
        "object_ms": None,
        "pack_ms": None,
        "bytes": None,
    }

    from botocore.exceptions import ClientError

    try:
        started = time.perf_counter()
        from_object = read_source_object(client, bucket, key)
        outcome["object_ms"] = (time.perf_counter() - started) * 1000
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            outcome["error"] = f"object read failed: {exc}"
            return outcome
        # Stage 4 has already removed this one. The pack read below is then the
        # only reader, which is exactly what it is here to prove.
        from_object = None
        outcome["object_present"] = False

    if cold:
        clear_pack_caches()

    try:
        started = time.perf_counter()
        from_pack = read_packed_html(f"s3://{bucket}/{key}")
        outcome["pack_ms"] = (time.perf_counter() - started) * 1000
    except Exception as exc:  # noqa: BLE001 - one bad member must not end the run.
        outcome["error"] = f"pack read failed: {type(exc).__name__}: {exc}"
        return outcome

    if from_pack is None:
        outcome["error"] = "not found in any pack index"
        return outcome

    outcome["bytes"] = len(from_pack)
    digest = hashlib.sha256(from_pack).hexdigest()
    if digest != record["raw_sha256"]:
        outcome["error"] = (
            f"pack bytes hash {digest[:12]} != sidecar raw_sha256 "
            f"{str(record['raw_sha256'])[:12]}"
        )
        return outcome
    if len(from_pack) != int(record["length"]):
        outcome["error"] = (
            f"pack bytes {len(from_pack)} != sidecar length {record['length']}"
        )
        return outcome
    if from_object is not None and from_object != from_pack:
        outcome["error"] = (
            f"object and pack disagree: object sha256 "
            f"{hashlib.sha256(from_object).hexdigest()[:12]} != {digest[:12]}"
        )
        return outcome

    outcome["match"] = True
    return outcome


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def percentiles(values: Sequence[float]) -> Dict[str, Optional[float]]:
    xs = sorted(v for v in values if v is not None)
    if not xs:
        return {"n": 0, "p50": None, "p95": None, "max": None}
    return {
        "n": len(xs),
        "p50": round(statistics.median(xs), 2),
        "p95": round(xs[min(len(xs) - 1, int(round(0.95 * (len(xs) - 1))))], 2),
        "max": round(xs[-1], 2),
    }


def verify_pack_read_path(
    *,
    artifact_type: str,
    year: int,
    month: int,
    per_pack: int,
    seed: int,
    warm_reads: int,
    bucket: str,
) -> Dict[str, Any]:
    from shared.minio import clear_pack_caches, get_boto3_client
    from shared.packfile import PACK_PREFIX

    client = get_boto3_client()
    prefix = f"{PACK_PREFIX}/{artifact_type}/{year:04d}/{month:02d}/"
    sidecars = list_sidecars(client, bucket, prefix)
    LOG.info("%s — %d sidecar index(es)", prefix, len(sidecars))

    result: Dict[str, Any] = {
        "bucket": bucket,
        "prefix": prefix,
        "sidecars": len(sidecars),
        "sampled": 0,
        "verified": 0,
        "failed": 0,
        "sources_already_deleted": 0,
        "latency_ms": {},
        "failures": [],
    }
    if not sidecars:
        LOG.error("no sidecar indexes under %s — nothing is packed here", prefix)
        return result

    records = sample_members(client, bucket, sidecars, per_pack, seed)
    result["sampled"] = len(records)
    LOG.info("sampled %d member(s) across %d pack(s)", len(records), len(sidecars))

    cold_ms: List[float] = []
    object_ms: List[float] = []
    started = time.monotonic()
    for i, record in enumerate(records, start=1):
        outcome = verify_one(client, bucket, record, cold=True)
        if outcome["match"]:
            result["verified"] += 1
        else:
            result["failed"] += 1
            result["failures"].append(outcome)
            LOG.error("FAIL %s — %s", outcome["source_key"], outcome["error"])
        if not outcome["object_present"]:
            result["sources_already_deleted"] += 1
        if outcome["pack_ms"] is not None:
            cold_ms.append(outcome["pack_ms"])
        if outcome["object_ms"] is not None:
            object_ms.append(outcome["object_ms"])
        if i % 50 == 0:
            LOG.info(
                "verified %d/%d (%.1f/s)", i, len(records),
                i / max(time.monotonic() - started, 1e-9),
            )

    # Warm: one pack, caches kept, which is the reprocessing shape.
    warm_ms: List[float] = []
    if warm_reads and records:
        clear_pack_caches()
        first_pack = records[0]["pack_key"]
        same_pack = [r for r in records if r["pack_key"] == first_pack][:warm_reads]
        for record in same_pack:
            outcome = verify_one(client, bucket, record, cold=False)
            if outcome["pack_ms"] is not None:
                warm_ms.append(outcome["pack_ms"])
            if not outcome["match"]:
                result["failed"] += 1
                result["failures"].append(outcome)

    result["latency_ms"] = {
        "object": percentiles(object_ms),
        "pack_cold": percentiles(cold_ms),
        "pack_warm": percentiles(warm_ms),
    }
    LOG.info(
        "verified=%d failed=%d — object p50=%s p95=%s | pack cold p50=%s p95=%s | "
        "pack warm p50=%s p95=%s (ms)",
        result["verified"], result["failed"],
        result["latency_ms"]["object"]["p50"], result["latency_ms"]["object"]["p95"],
        result["latency_ms"]["pack_cold"]["p50"], result["latency_ms"]["pack_cold"]["p95"],
        result["latency_ms"]["pack_warm"]["p50"], result["latency_ms"]["pack_warm"]["p95"],
    )
    return result


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify Plan 131 Stage 3's pack read path against real packs, and "
            "measure single-artifact extraction latency. Read-only."
        )
    )
    parser.add_argument("--artifact-type", default=DEFAULT_ARTIFACT_TYPE)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument(
        "--per-pack", type=int, default=5,
        help="Members sampled from each pack [default: %(default)s]",
    )
    parser.add_argument(
        "--warm-reads", type=int, default=50,
        help="Reads of one pack with caches kept, for the warm latency figure",
    )
    parser.add_argument("--seed", type=int, default=131)
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    from shared.minio import BUCKET

    result = verify_pack_read_path(
        artifact_type=args.artifact_type,
        year=args.year,
        month=args.month,
        per_pack=args.per_pack,
        seed=args.seed,
        warm_reads=args.warm_reads,
        bucket=BUCKET,
    )
    print(json.dumps(result, indent=2, default=str))
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, default=str)
    return 1 if result["failed"] or not result["verified"] else 0


if __name__ == "__main__":
    sys.exit(main())
