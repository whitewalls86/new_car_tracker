"""Train and register an immutable bronze HTML zstd dictionary (Plan 129 Stage 1).

The dictionary is written to both MinIO and Postgres.  The sampled object-key
list and training parameters are retained so the training input is auditable.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Sequence

from scripts.estimate_dictionary_savings import (
    DEFAULT_ZSTD_LEVEL,
    fetch_available_months,
    fetch_corpus_sample,
)

LOG = logging.getLogger("train_html_dictionary")
DEFAULT_DICTIONARY_SIZE_KB = 768


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and register a bronze HTML dictionary.")
    parser.add_argument("--version", type=int, default=1)
    parser.add_argument("--dict-size-kb", type=int, default=DEFAULT_DICTIONARY_SIZE_KB)
    parser.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL)
    parser.add_argument("--sample-in", type=Path)
    parser.add_argument("--sample-size", type=int, default=10000)
    parser.add_argument("--months", nargs="+", default=None)
    parser.add_argument("--source-pattern", default="%detail%")
    parser.add_argument("--optimize-cover", action="store_true")
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true", help="Train and report; do not register.")
    parser.add_argument("--dictionary-out", type=Path)
    return parser.parse_args(argv)


def load_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.sample_in:
        return list(json.loads(args.sample_in.read_text()))
    from shared.duckdb_s3 import get_duckdb_s3_connection

    con = get_duckdb_s3_connection()
    months = args.months or fetch_available_months(con, args.source_pattern)
    return fetch_corpus_sample(
        con, months=months, sample_size=args.sample_size, source_pattern=args.source_pattern
    )


def fetch_samples(
    rows: Sequence[dict[str, Any]],
    *,
    progress_every: int = 100,
    max_failure_pct: float = 5.0,
) -> tuple[list[bytes], list[str]]:
    """Fetch training samples, tolerating individual unreadable objects.

    A training run samples thousands of objects and takes a long time. Letting
    one deleted artifact abort it -- with no partial state -- wastes the whole
    run for a sample the dictionary would not have missed. So per-object
    failures are counted and skipped, as the sibling sampler in
    ``estimate_dictionary_savings.collect_documents`` already does.

    They are not, however, ignored. Above *max_failure_pct* the run stops:
    widespread read failures mean something is wrong with the store or the
    sample, and a dictionary trained on whatever happened to survive is not a
    dictionary anyone can reason about later.
    """
    from shared.minio import read_html

    samples: list[bytes] = []
    keys: list[str] = []
    failures: list[str] = []
    considered = 0

    for row in rows:
        path = row.get("minio_path")
        if not path:
            continue
        considered += 1
        try:
            payload = read_html(str(path))
        except Exception as exc:  # noqa: BLE001 - one unreadable object must not end the run.
            failures.append(f"{path}: {exc}")
            if 100.0 * len(failures) / considered > max_failure_pct and len(failures) > 10:
                raise RuntimeError(
                    f"aborting: {len(failures)} of {considered} sampled objects failed to "
                    f"read (>{max_failure_pct}%); last error {exc}"
                ) from exc
            continue
        samples.append(payload)
        keys.append(str(path))
        if progress_every and len(samples) % progress_every == 0:
            LOG.info("PROGRESS | fetched=%d failures=%d", len(samples), len(failures))

    if failures:
        LOG.warning(
            "Skipped %d of %d sampled objects that could not be read; first: %s",
            len(failures), considered, failures[0],
        )
    return samples, keys


def train(samples: Sequence[bytes], size_bytes: int, *, optimize_cover: bool):
    import zstandard as zstd

    kwargs = {"steps": 4, "threads": -1} if optimize_cover else {}
    return zstd.train_dictionary(size_bytes, list(samples), **kwargs)


def register_dictionary(
    dictionary_bytes: bytes,
    *,
    dict_id: int,
    version: int,
    zstd_level: int,
    parameters: dict[str, Any],
    sample_keys: Sequence[str],
    sample_sha256: str,
) -> str:
    from shared.db import db_cursor
    from shared.minio import BUCKET, get_boto3_client

    key = f"dictionaries/zstd/v{version}.dict"
    minio_path = f"s3://{BUCKET}/{key}"
    # Refuse before touching the immutable MinIO key. Re-running a version
    # must never replace bytes needed by frames already written against it.
    with db_cursor(error_context="Check compression dictionary", dict_cursor=True) as cur:
        cur.execute(
            "SELECT dict_id, version FROM ops.compression_dictionaries "
            "WHERE dict_id = %s OR version = %s",
            (dict_id, version),
        )
        if cur.fetchone() is not None:
            raise RuntimeError(
                f"dictionary ID {dict_id} or version {version} is already registered"
            )
    client = get_boto3_client()
    try:
        existing = client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code not in ("404", "NoSuchKey", "NotFound"):
            raise RuntimeError(f"could not check immutable dictionary object: {key}") from exc
        try:
            client.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=dictionary_bytes,
                ContentType="application/octet-stream",
                IfNoneMatch="*",
            )
        except Exception as upload_exc:
            raise RuntimeError(f"immutable dictionary upload failed: {key}") from upload_exc
    else:
        if existing != dictionary_bytes:
            raise RuntimeError(
                f"refusing to overwrite immutable dictionary object with different bytes: {key}"
            )
        LOG.info("Reusing identical orphaned MinIO dictionary object at %s", minio_path)
    try:
        with db_cursor(error_context="Register compression dictionary") as cur:
            cur.execute(
                """
                INSERT INTO ops.compression_dictionaries (
                    dict_id, version, minio_path, dictionary_bytes,
                    dictionary_size_bytes, zstd_level, training_parameters,
                    sample_keys, sample_sha256
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    dict_id,
                    version,
                    minio_path,
                    dictionary_bytes,
                    len(dictionary_bytes),
                    zstd_level,
                    json.dumps(parameters, sort_keys=True),
                    json.dumps(list(sample_keys)),
                    sample_sha256,
                ),
            )
    except Exception:
        LOG.error(
            "Postgres registration failed; MinIO recovery copy remains at s3://%s/%s",
            BUCKET,
            key,
        )
        raise
    return minio_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.version <= 0 or args.dict_size_kb <= 0:
        raise SystemExit("--version and --dict-size-kb must be positive")

    rows = load_rows(args)
    samples, sample_keys = fetch_samples(rows, progress_every=args.progress_every)
    if not samples:
        LOG.error("No training samples were fetched")
        return 2

    try:
        dictionary = train(
            samples, args.dict_size_kb * 1024, optimize_cover=args.optimize_cover
        )
    except Exception as exc:
        LOG.error("Dictionary training failed (sample may be too small): %s", exc)
        return 2

    import zstandard as zstd

    dictionary_bytes = dictionary.as_bytes()
    dict_id = dictionary.dict_id()
    plain = zstd.ZstdCompressor(level=args.zstd_level)
    with_dict = zstd.ZstdCompressor(level=args.zstd_level, dict_data=dictionary)
    baseline_bytes = sum(len(plain.compress(sample)) for sample in samples)
    dictionary_bytes_total = sum(len(with_dict.compress(sample)) for sample in samples)
    ratio = dictionary_bytes_total / baseline_bytes if baseline_bytes else 0.0
    sample_manifest = json.dumps(list(sample_keys), separators=(",", ":")).encode()
    sample_sha256 = hashlib.sha256(sample_manifest).hexdigest()
    parameters = {
        "dict_size_kb": args.dict_size_kb,
        "optimize_cover": args.optimize_cover,
        "sample_size_requested": args.sample_size,
        "samples_fetched": len(samples),
        "months": args.months,
        "source_pattern": args.source_pattern,
    }

    if args.dictionary_out:
        args.dictionary_out.write_bytes(dictionary_bytes)
    minio_path = None
    if not args.dry_run:
        minio_path = register_dictionary(
            dictionary_bytes,
            dict_id=dict_id,
            version=args.version,
            zstd_level=args.zstd_level,
            parameters=parameters,
            sample_keys=sample_keys,
            sample_sha256=sample_sha256,
        )

    print(
        json.dumps(
            {
                "version": args.version,
                "dict_id": dict_id,
                "dictionary_size_bytes": len(dictionary_bytes),
                "samples": len(samples),
                "sample_sha256": sample_sha256,
                "training_corpus_compressed_pct_of_plain": round(ratio * 100, 4),
                "training_corpus_saving_pct": round((1 - ratio) * 100, 4),
                "minio_path": minio_path,
                "dry_run": args.dry_run,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
