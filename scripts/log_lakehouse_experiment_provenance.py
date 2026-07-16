"""
Plan 112 Gate B: log one lakehouse/backtesting input-snapshot provenance run
to MLflow.

This records enough metadata to trace an experiment back to its Plan 120
snapshot archive and its Gate A Iceberg table -- BEFORE any model training or
backtest scheduling. It trains nothing and schedules nothing.

Inputs are layered (each later source overrides earlier ones for the same
field), so this can be driven either by hand or from the A4 rehearsal flow:

  1. --metadata-json PATH   flat JSON object of provenance fields
  2. --iceberg-info-json PATH   output of
        `export_volatility_features_to_iceberg info`
        (catalog/table/current_snapshot_id/row_count/distinct_vin17/...)
  3. --manifest PATH        a Plan 120 archive_manifest.json; archive fields
        (snapshot_id, export_fingerprint, archive_sha256, archive_key) are
        extracted from it AND the file itself is logged as a run artifact
  4. individual --field flags (highest precedence)

Examples:

  # Dry run: build + print the exact params/tags/artifact, log nothing.
  python -m scripts.log_lakehouse_experiment_provenance \\
      --manifest .cache/lake_snapshots/<id>/manifest.json \\
      --iceberg-info-json /tmp/iceberg_info.json \\
      --feature-table-name int_listing_volatility_features \\
      --dry-run

  # Log to a local file store (no server, no Docker needed):
  python -m scripts.log_lakehouse_experiment_provenance \\
      --manifest .cache/lake_snapshots/<id>/manifest.json \\
      --iceberg-info-json /tmp/iceberg_info.json \\
      --feature-table-name int_listing_volatility_features \\
      --tracking-uri file:./.cache/mlruns

  # Log to the standalone MLflow server (docker-compose.mlflow.yml):
  python -m scripts.log_lakehouse_experiment_provenance ... \\
      --tracking-uri http://localhost:15000

See docs/plan_112_refresh_policy_backtesting.md ("Gate B provenance smoke") for the full flow.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any, Dict, Optional

from shared.mlflow_provenance import (
    DEFAULT_EXPERIMENT,
    build_provenance_payload,
    log_provenance_run,
    provenance_fields_from_iceberg_info,
    provenance_fields_from_manifest,
)

# Default tracking URI: a local file store under the repo cache, so a bare
# host with just `pip install mlflow` can run the smoke without a server.
DEFAULT_TRACKING_URI = "file:./.cache/mlruns"

# Provenance fields settable via an individual CLI flag. Kept aligned with
# shared.mlflow_provenance's REQUIRED/OPTIONAL field names.
_FLAG_FIELDS = (
    "snapshot_id",
    "iceberg_catalog",
    "iceberg_table",
    "iceberg_snapshot_id",
    "feature_table_name",
    "row_count",
    "distinct_vin17",
    "max_latest_fetched_at",
    "export_fingerprint",
    "archive_sha256",
    "archive_key",
    "archive_manifest_key",
)

_INT_FLAG_FIELDS = {"row_count", "distinct_vin17"}


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-json", help="Path to a flat provenance-fields JSON object")
    parser.add_argument(
        "--iceberg-info-json",
        help="Path to `export_volatility_features_to_iceberg info` output JSON",
    )
    parser.add_argument(
        "--manifest",
        help="Path to a Plan 120 archive_manifest.json (extracted + logged as an artifact)",
    )
    parser.add_argument(
        "--manifest-key",
        help="Object/download key of the manifest (recorded as archive_manifest_key)",
    )
    for name in _FLAG_FIELDS:
        parser.add_argument(
            f"--{name.replace('_', '-')}",
            dest=name,
            help=f"Override the {name} provenance field",
        )
    parser.add_argument("--experiment", default=DEFAULT_EXPERIMENT)
    parser.add_argument("--run-name", default=None)
    parser.add_argument(
        "--env", default=os.environ.get("PROVENANCE_ENV", "local"),
        help="Environment tag: local | vm | ci (default: local)",
    )
    parser.add_argument(
        "--code-sha", default=None,
        help="Code SHA tag; defaults to `git rev-parse --short HEAD` when available",
    )
    parser.add_argument(
        "--tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Build and print the payload; do not log anything to MLflow",
    )
    return parser.parse_args(argv)


class InputError(Exception):
    """A user-facing input problem (missing/unreadable/invalid input file or
    field), reported as a clean nonzero exit rather than a raw traceback."""


def _load_json(path: str, *, label: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise InputError(f"{label} not found at {path}")
    except OSError as e:
        raise InputError(f"could not read {label} at {path}: {e}")
    except json.JSONDecodeError as e:
        raise InputError(f"{label} at {path} is not valid JSON: {e}")


def _detect_code_sha() -> Optional[str]:
    """Best-effort short code SHA for provenance. Falls back to env vars and
    then None -- never fails the run just because git is unavailable (e.g.
    inside a container with no .git)."""
    for env_var in ("CODE_SHA", "GIT_SHA", "GIT_COMMIT"):
        value = os.environ.get(env_var)
        if value:
            return value[:12]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.strip() or None
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return None


def _collect_fields(args: argparse.Namespace) -> Dict[str, Any]:
    """Layer the input sources into a single flat provenance-fields dict,
    later sources overriding earlier ones for the same key."""
    fields: Dict[str, Any] = {}

    if args.metadata_json:
        fields.update(_load_json(args.metadata_json, label="--metadata-json"))

    if args.iceberg_info_json:
        info = _load_json(args.iceberg_info_json, label="--iceberg-info-json")
        fields.update(provenance_fields_from_iceberg_info(info))

    if args.manifest:
        manifest = _load_json(args.manifest, label="--manifest")
        fields.update(
            provenance_fields_from_manifest(manifest, manifest_key=args.manifest_key)
        )

    # Individual flags win over everything above.
    for name in _FLAG_FIELDS:
        value = getattr(args, name)
        if value is not None:
            fields[name] = int(value) if name in _INT_FLAG_FIELDS else value

    return fields


def main(argv: Optional[list] = None) -> int:
    args = _parse_args(argv)

    # All user-input problems (missing/invalid input files, missing/invalid
    # provenance fields) surface as a clean nonzero exit, never a raw
    # traceback -- this is a user-facing smoke CLI.
    try:
        fields = _collect_fields(args)
        code_sha = args.code_sha or _detect_code_sha()
        payload = build_provenance_payload(
            fields,
            experiment=args.experiment,
            run_name=args.run_name,
            env=args.env,
            code_sha=code_sha,
            manifest_artifact_path=args.manifest,
        )
    except (InputError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(json.dumps(
            {
                "experiment": payload.experiment,
                "run_name": payload.run_name,
                "params": payload.params,
                "tags": payload.tags,
                "manifest_artifact_path": payload.manifest_artifact_path,
            },
            indent=2,
        ))
        return 0

    run_id = log_provenance_run(payload, tracking_uri=args.tracking_uri)
    print(f"Logged provenance run {run_id} to experiment {payload.experiment!r} "
          f"({args.tracking_uri}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
