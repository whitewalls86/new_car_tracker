"""
Plan 112 Gate B: lakehouse experiment-provenance bridge.

This module is the engine-agnostic provenance bridge described in
`docs/plan_112_refresh_policy_backtesting.md`: it turns a plain,
JSON-serializable metadata dict (from the Plan 120 snapshot archive manifest
and the Gate A Iceberg metadata capture) into the exact set of MLflow
params/tags/artifacts that make an experiment traceable back to its input
snapshot and Iceberg table.

Deliberately split so the payload construction/validation is pure and unit
testable **without MLflow installed**: `build_provenance_payload` and the
`provenance_fields_from_*` helpers import nothing beyond the stdlib (and
`scripts.lake_snapshot_common` for the shared Plan 120 manifest shape).
`log_provenance_run` is the only function that touches MLflow, and it imports
it lazily inside the call -- mirroring the deferred-import convention the rest
of the lakehouse stack uses so the unit tests and a bare host degrade
gracefully.

This bridge does NOT train a model and does NOT schedule a backtest -- it only
records provenance for a lakehouse/backtesting input snapshot.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

# Default experiment for provenance runs -- isolated from the eventual
# `adaptive_refresh_backtest` experiment (plan Sec 3.4) so provenance records
# never mix with real backtest runs.
DEFAULT_EXPERIMENT = "adaptive_refresh_provenance"

# The fields every provenance run must carry to be traceable at all: the Plan
# 120 snapshot identity, the Iceberg table it was materialized into, the dbt
# feature table it came from, and its row count.
REQUIRED_FIELDS = (
    "snapshot_id",
    "iceberg_catalog",
    "iceberg_table",
    "feature_table_name",
    "row_count",
)

# Optional provenance fields, logged only when present (plan Sec 3.5). Kept
# explicit so `build_provenance_payload` never silently swallows an unknown
# key and the unit tests can assert the exact contract.
OPTIONAL_FIELDS = (
    "export_fingerprint",
    "archive_sha256",
    "archive_key",
    "archive_manifest_key",
    "iceberg_snapshot_id",
    "distinct_vin17",
    "max_latest_fetched_at",
)


@dataclass(frozen=True)
class ProvenancePayload:
    """The fully-resolved, MLflow-ready provenance record.

    `params` and `tags` are both str->str (MLflow requires string values);
    `manifest_artifact_path` is the local path of the Plan 120
    `archive_manifest.json` to log as a run artifact, or None when no manifest
    was provided.
    """

    experiment: str
    run_name: Optional[str]
    params: Dict[str, str]
    tags: Dict[str, str]
    manifest_artifact_path: Optional[str] = None
    # The raw resolved field values (pre-stringification), handy for --dry-run
    # output and debugging. Not sent to MLflow directly.
    resolved: Dict[str, Any] = field(default_factory=dict)


def _stringify(value: Any) -> str:
    """Normalize a value to the string form MLflow stores. Booleans become
    'true'/'false' (not 'True'/'False') for consistency with the rest of the
    stack's env-var conventions; everything else uses str()."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def provenance_fields_from_manifest(
    manifest: Dict[str, Any], *, manifest_key: Optional[str] = None
) -> Dict[str, Any]:
    """Extract the archive-side provenance fields from a Plan 120 archive
    manifest dict (the checksum-bearing manifest CI/local downloaders read --
    see `archiver.processors.lake_snapshot_archive.build_archive_manifest`).

    Returns only the keys actually present; never raises on a partial
    manifest. `manifest_key` is the object/download key of the manifest
    itself, which is not stored inside the manifest, so callers pass it
    explicitly when known.
    """
    # Reuse the exact same archive-metadata reader the downloader uses so this
    # bridge tolerates both the rich (`archive.sha256`) and flatter
    # (`archive_sha256`) manifest shapes without duplicating that logic.
    from scripts.lake_snapshot_common import get_archive_meta

    fields: Dict[str, Any] = {}
    if manifest.get("snapshot_id") is not None:
        fields["snapshot_id"] = manifest["snapshot_id"]
    if manifest.get("export_fingerprint") is not None:
        fields["export_fingerprint"] = manifest["export_fingerprint"]
    try:
        archive_meta = get_archive_meta(manifest)
    except Exception:
        archive_meta = None
    if archive_meta:
        if archive_meta.get("sha256"):
            fields["archive_sha256"] = archive_meta["sha256"]
        if archive_meta.get("path"):
            fields["archive_key"] = archive_meta["path"]
    if manifest_key:
        fields["archive_manifest_key"] = manifest_key
    return fields


def provenance_fields_from_iceberg_info(info: Dict[str, Any]) -> Dict[str, Any]:
    """Map the `scripts.export_volatility_features_to_iceberg info` output
    (the Gate A/A3 metadata-capture dict) onto provenance field names.

    That dict uses `catalog`/`table`/`current_snapshot_id`; this bridge uses
    `iceberg_catalog`/`iceberg_table`/`iceberg_snapshot_id`. Only present keys
    are returned.
    """
    mapping = {
        "catalog": "iceberg_catalog",
        "table": "iceberg_table",
        "current_snapshot_id": "iceberg_snapshot_id",
        "row_count": "row_count",
        "distinct_vin17": "distinct_vin17",
        "max_latest_fetched_at": "max_latest_fetched_at",
    }
    fields: Dict[str, Any] = {}
    for src, dest in mapping.items():
        if info.get(src) is not None:
            fields[dest] = info[src]
    return fields


def build_provenance_payload(
    fields: Dict[str, Any],
    *,
    experiment: str = DEFAULT_EXPERIMENT,
    run_name: Optional[str] = None,
    env: str = "local",
    code_sha: Optional[str] = None,
    manifest_artifact_path: Optional[str] = None,
) -> ProvenancePayload:
    """Validate and normalize a flat provenance `fields` dict into a
    ProvenancePayload.

    Required keys (REQUIRED_FIELDS) must be present and non-None or a
    ValueError is raised naming every missing field. Optional keys
    (OPTIONAL_FIELDS) are included only when present and non-None. All values
    are stringified for MLflow. Unknown keys are rejected so a typo can never
    silently drop a provenance field.
    """
    known = set(REQUIRED_FIELDS) | set(OPTIONAL_FIELDS)
    unknown = sorted(k for k in fields if k not in known)
    if unknown:
        raise ValueError(f"unknown provenance field(s): {', '.join(unknown)}")

    missing = [
        name
        for name in REQUIRED_FIELDS
        if fields.get(name) is None
    ]
    if missing:
        raise ValueError(f"missing required provenance field(s): {', '.join(missing)}")

    # Resolved (pre-stringification) view: required first, then present optionals.
    resolved: Dict[str, Any] = {name: fields[name] for name in REQUIRED_FIELDS}
    for name in OPTIONAL_FIELDS:
        if fields.get(name) is not None:
            resolved[name] = fields[name]

    # Params: the full provenance field set, stringified. Params are immutable
    # per run and searchable (`params.snapshot_id = '...'`) -- the right home
    # for input-identity facts.
    params = {name: _stringify(value) for name, value in resolved.items()}

    # Tags: cross-run grouping/search dimensions (plan Sec 3.4). The two
    # highest-value trace keys (snapshot_id, iceberg table) are mirrored here
    # because tags are MLflow's primary grouping axis; the rest of the
    # identity lives in params to avoid wholesale duplication.
    tags: Dict[str, str] = {
        "plan": "112",
        "gate": "B",
        "kind": "lakehouse_provenance",
        "entity_grain": "vin17",
        "env": env,
        "snapshot_id": _stringify(resolved["snapshot_id"]),
        "iceberg.table": _stringify(resolved["iceberg_table"]),
    }
    if code_sha:
        tags["code_sha"] = _stringify(code_sha)

    artifact_path = None
    if manifest_artifact_path is not None:
        if not Path(manifest_artifact_path).is_file():
            raise ValueError(
                f"manifest artifact not found at {manifest_artifact_path}"
            )
        artifact_path = str(manifest_artifact_path)

    return ProvenancePayload(
        experiment=experiment,
        run_name=run_name,
        params=params,
        tags=tags,
        manifest_artifact_path=artifact_path,
        resolved=resolved,
    )


def log_provenance_run(payload: ProvenancePayload, *, tracking_uri: str) -> str:
    """Log `payload` as a single MLflow run and return its run id.

    MLflow is imported lazily here (never at module load) so payload
    construction/validation stays fully testable without MLflow installed.
    This function is intentionally thin -- all business logic lives in
    `build_provenance_payload`.
    """
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(payload.experiment)
    with mlflow.start_run(run_name=payload.run_name) as run:
        mlflow.set_tags(payload.tags)
        mlflow.log_params(payload.params)
        if payload.manifest_artifact_path:
            mlflow.log_artifact(payload.manifest_artifact_path)
        return run.info.run_id
