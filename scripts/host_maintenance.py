#!/usr/bin/env python3
"""Operator-side host-maintenance lifecycle client for Plan 142 Stage 2."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_API_URL = "http://localhost:5050"
DEFAULT_CHECKPOINT = Path("/var/lib/cartracker/maintenance/history.jsonl")
CHECKPOINT_PHASES = frozenset({"requested", "draining", "active", "validating"})
REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNING_SET_POLICY = REPO_ROOT / "maintenance-running-set.txt"
logger = logging.getLogger(__name__)
CLI_LOG_FIELDS = (
    "generation",
    "prior_phase",
    "phase",
    "kind",
    "drained",
    "blockers",
    "method",
    "route",
)

# Projects that must still be rendered when they have no containers to supply
# Compose labels.  That absence is meaningful for the two Plan 125 projects.
KNOWN_COMPOSE_PROJECTS = (
    ("cartracker", REPO_ROOT, (REPO_ROOT / "docker-compose.yml",)),
    (
        "cartracker-lakehouse",
        REPO_ROOT,
        (REPO_ROOT / "docker-compose.lakehouse.yml",),
    ),
    ("cartracker-mlflow", REPO_ROOT, (REPO_ROOT / "docker-compose.mlflow.yml",)),
)

# Preflight is deliberately observation-only. Package refresh/download belongs
# to a later, separately confirmed preparation command; stop/update/reboot do
# not belong in this list at all.
PREFLIGHT_COMMANDS = (
    ("git_status", ("git", "status", "--short", "--branch"), frozenset({0})),
    ("git_revision", ("git", "rev-parse", "HEAD"), frozenset({0})),
    ("kernel", ("uname", "-r"), frozenset({0})),
    ("os_release", ("cat", "/etc/os-release"), frozenset({0})),
    (
        "reboot_required",
        ("cat", "/var/run/reboot-required"),
        frozenset({0, 1}),
    ),
    (
        "reboot_required_packages",
        ("cat", "/var/run/reboot-required.pkgs"),
        frozenset({0, 1}),
    ),
    (
        "apt_list_age",
        ("stat", "-c", "%y", "/var/lib/apt/lists"),
        frozenset({0}),
    ),
    ("upgradable_packages", ("apt", "list", "--upgradable"), frozenset({0})),
    ("package_holds", ("apt-mark", "showhold"), frozenset({0})),
    (
        "apt_locks",
        (
            "sudo",
            "fuser",
            "/var/lib/dpkg/lock-frontend",
            "/var/lib/apt/lists/lock",
            "/var/cache/apt/archives/lock",
        ),
        frozenset({0, 1}),
    ),
    ("dpkg_audit", ("sudo", "dpkg", "--audit"), frozenset({0})),
    (
        "unattended_enabled",
        ("systemctl", "is-enabled", "unattended-upgrades"),
        frozenset({0, 1, 3, 4}),
    ),
    (
        "unattended_active",
        ("systemctl", "is-active", "unattended-upgrades"),
        frozenset({0, 1, 3, 4}),
    ),
    ("failed_units", ("systemctl", "--failed", "--no-legend"), frozenset({0})),
    ("disk_bytes", ("df", "-B1", "/", "/boot", "/mnt/data"), frozenset({0})),
    ("disk_inodes", ("df", "-i", "/", "/boot", "/mnt/data"), frozenset({0})),
    ("mounts", ("findmnt", "--json"), frozenset({0})),
    ("mount_verify", ("findmnt", "--verify", "--verbose"), frozenset({0})),
    ("sshd_config", ("sudo", "sshd", "-t"), frozenset({0})),
    ("netplan_config", ("sudo", "netplan", "generate"), frozenset({0})),
    ("docker_active", ("systemctl", "is-active", "docker"), frozenset({0})),
    ("docker_version", ("docker", "version"), frozenset({0})),
    ("docker_info", ("docker", "info"), frozenset({0})),
    ("compose_version", ("docker", "compose", "version"), frozenset({0})),
    (
        "maintenance_pool",
        (
            "docker",
            "exec",
            "cartracker-airflow-scheduler",
            "airflow",
            "pools",
            "list",
        ),
        frozenset({0}),
    ),
)


class MaintenanceError(RuntimeError):
    """A fail-closed operator error suitable for printing without a traceback."""


class CliJsonFormatter(logging.Formatter):
    """Preserve reviewed structured fields in operator-visible CLI logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {"level": record.levelname, "message": record.getMessage()}
        payload.update(
            {field: getattr(record, field) for field in CLI_LOG_FIELDS if hasattr(record, field)}
        )
        return json.dumps(payload, sort_keys=True)


def _run_command(
    command: tuple[str, ...],
    *,
    allowed_returncodes: frozenset[int] = frozenset({0}),
    cwd: Path = REPO_ROOT,
) -> dict[str, Any]:
    """Run one argv-only command and retain auditable output."""
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise MaintenanceError(f"unable to run: {' '.join(command)}") from exc
    result = {
        "command": list(command),
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    if completed.returncode not in allowed_returncodes:
        raise MaintenanceError(
            f"preflight command failed ({completed.returncode}): {' '.join(command)}"
        )
    return result


def _safe_json_write(path: Path, payload: Any) -> None:
    """Write reviewed evidence without following a caller-controlled symlink."""
    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    if path.parent.is_symlink() or (path.exists() and path.is_symlink()):
        raise MaintenanceError("evidence path must not traverse a symlink")
    path.parent.chmod(0o755)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(temporary, flags, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o644)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise MaintenanceError(f"unable to write evidence at {path}") from exc


def _parse_running_set_policy(path: Path = RUNNING_SET_POLICY) -> dict[str, str]:
    """Return the exceptions-only restore policy as ``key -> class``."""
    entries: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith(("#", " ", "\t")):
            continue
        fields = line.split()
        if len(fields) < 2:
            raise MaintenanceError(f"malformed running-set policy line: {line!r}")
        entries[fields[0]] = fields[1]
    return entries


def _split_compose_paths(raw: str, working_dir: Path) -> tuple[Path, ...]:
    paths = []
    for value in raw.split(","):
        value = value.strip()
        if not value:
            continue
        candidate = Path(value)
        paths.append(candidate if candidate.is_absolute() else working_dir / candidate)
    return tuple(paths)


def _log_rotation_config(config: dict[str, Any]) -> dict[str, Any]:
    """Keep the log driver and rotation knobs, never arbitrary driver secrets."""
    option_key = "Config" if "Config" in config else "options"
    options = config.get(option_key) or {}
    safe_options = {
        key: value
        for key, value in options.items()
        if key in {"compress", "max-file", "max-size", "mode"}
    }
    driver_key = "Type" if "Type" in config else "driver"
    return {
        "driver": config.get(driver_key),
        "options": safe_options,
    }


def _compose_sources(inspections: list[dict[str, Any]]) -> dict[str, tuple[Path, tuple[Path, ...]]]:
    """Discover exact active project inputs, with known stopped-project fallbacks."""
    sources = {name: (directory, files) for name, directory, files in KNOWN_COMPOSE_PROJECTS}
    for container in inspections:
        labels = (container.get("Config") or {}).get("Labels") or {}
        project = labels.get("com.docker.compose.project")
        working_dir_raw = labels.get("com.docker.compose.project.working_dir")
        config_files_raw = labels.get("com.docker.compose.project.config_files")
        if not (project and working_dir_raw and config_files_raw):
            continue
        working_dir = Path(working_dir_raw)
        config_files = _split_compose_paths(config_files_raw, working_dir)
        if config_files:
            sources[project] = (working_dir, config_files)
    return sources


def _render_compose_projects(
    inspections: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    rendered: dict[str, dict[str, Any]] = {}
    for project, (working_dir, config_files) in sorted(_compose_sources(inspections).items()):
        command = ["docker", "compose", "--project-name", project]
        for config_file in config_files:
            command.extend(("--file", str(config_file)))
        command.extend(("--profile", "*", "config", "--format", "json"))
        result = _run_command(tuple(command), cwd=working_dir)
        try:
            config = json.loads(result["stdout"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise MaintenanceError(f"Compose rendered malformed JSON for {project}") from exc
        if not isinstance(config, dict) or not isinstance(config.get("services"), dict):
            raise MaintenanceError(f"Compose rendered no services for {project}")
        rendered[project] = {
            "working_directory": str(working_dir),
            "config_files": [str(path) for path in config_files],
            "rendered_sha256": hashlib.sha256(result["stdout"].encode()).hexdigest(),
            # Compose interpolation can contain credentials. Keep only the
            # reviewed restore fields; the full render is validation input,
            # never an evidence artifact.
            "services": {
                name: {
                    "profiles": sorted(service.get("profiles") or []),
                    "image": service.get("image"),
                    "restart": service.get("restart"),
                    "logging": _log_rotation_config(service.get("logging") or {}),
                }
                for name, service in sorted(config["services"].items())
            },
        }
    return rendered


def capture_running_set() -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Capture Compose identity and intended state before any stop is allowed."""
    ids_result = _run_command(("docker", "ps", "--all", "--quiet"))
    container_ids = [line.strip() for line in ids_result["stdout"].splitlines() if line.strip()]
    if not container_ids:
        raise MaintenanceError("no containers exist; refusing to capture an empty running set")
    inspections: list[dict[str, Any]] = []
    inspected = _run_command(("docker", "inspect", *container_ids))
    try:
        inspections = json.loads(inspected["stdout"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise MaintenanceError("Docker returned malformed container inspection JSON") from exc
    if not isinstance(inspections, list):
        raise MaintenanceError("Docker returned a non-list container inspection")

    rendered = _render_compose_projects(inspections)
    image_ids = sorted(
        {container.get("Image") for container in inspections if container.get("Image")}
    )
    image_digests: dict[str, list[str]] = {}
    if image_ids:
        images_result = _run_command(("docker", "image", "inspect", *image_ids))
        try:
            images = json.loads(images_result["stdout"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise MaintenanceError("Docker returned malformed image inspection JSON") from exc
        image_digests = {
            image.get("Id", ""): sorted(image.get("RepoDigests") or [])
            for image in images
            if image.get("Id")
        }

    policy = _parse_running_set_policy()
    containers = []
    for container in inspections:
        config = container.get("Config") or {}
        labels = config.get("Labels") or {}
        state = container.get("State") or {}
        host_config = container.get("HostConfig") or {}
        project = labels.get("com.docker.compose.project")
        service = labels.get("com.docker.compose.service")
        policy_key = f"{project}/{service}" if project and project != "cartracker" else service
        rendered_services = (rendered.get(project) or {}).get("services", {})
        rendered_service = rendered_services.get(service, {})
        containers.append(
            {
                "name": str(container.get("Name", "")).lstrip("/"),
                "container_id": container.get("Id"),
                "project": project,
                "service": service,
                "working_directory": labels.get("com.docker.compose.project.working_dir"),
                "config_files": [
                    value.strip()
                    for value in labels.get("com.docker.compose.project.config_files", "").split(
                        ","
                    )
                    if value.strip()
                ],
                "declared_profiles": sorted(rendered_service.get("profiles") or []),
                "policy_class": policy.get(policy_key),
                "image_reference": config.get("Image"),
                "image_id": container.get("Image"),
                "image_repo_digests": image_digests.get(container.get("Image"), []),
                "state": {
                    "status": state.get("Status"),
                    "running": state.get("Running"),
                    "exit_code": state.get("ExitCode"),
                    "health": (state.get("Health") or {}).get("Status"),
                },
                "restart_policy": host_config.get("RestartPolicy") or {},
                "log_config": _log_rotation_config(host_config.get("LogConfig") or {}),
            }
        )

    policy_bytes = RUNNING_SET_POLICY.read_bytes()
    manifest = {
        "schema_version": 1,
        "captured_at": _utc_now(),
        "git_revision": _git_revision(),
        "running_kernel": _running_kernel(),
        "running_set_policy": {
            "path": str(RUNNING_SET_POLICY),
            "sha256": hashlib.sha256(policy_bytes).hexdigest(),
            "classes": policy,
        },
        "compose_projects": {
            project: {
                "working_directory": data["working_directory"],
                "config_files": data["config_files"],
                "rendered_sha256": data["rendered_sha256"],
            }
            for project, data in rendered.items()
        },
        "containers": sorted(containers, key=lambda row: row["name"]),
    }
    return manifest, rendered


def collect_preflight() -> dict[str, Any]:
    """Collect the read-only host baseline and enforce known unsafe findings."""
    observations = {
        name: _run_command(command, allowed_returncodes=returncodes)
        for name, command, returncodes in PREFLIGHT_COMMANDS
    }
    if observations["apt_locks"]["returncode"] == 0:
        raise MaintenanceError("apt/dpkg lock is held; inspect the owning process")
    if observations["dpkg_audit"]["stdout"].strip():
        raise MaintenanceError("dpkg --audit reported an inconsistent package database")
    holds = {
        line.strip()
        for line in observations["package_holds"]["stdout"].splitlines()
        if line.strip()
    }
    if holds != {"docker.io"}:
        raise MaintenanceError(
            "package holds differ from reviewed policy: "
            f"expected ['docker.io'], got {sorted(holds)}"
        )
    return {
        "schema_version": 1,
        "captured_at": _utc_now(),
        "console_access_verified": True,
        "observations": observations,
    }


def run_preflight(output_dir: Path, *, console_access_verified: bool) -> dict[str, Any]:
    if not console_access_verified:
        raise MaintenanceError("verify Oracle Cloud console access before preflight")
    manifest, rendered = capture_running_set()
    preflight = collect_preflight()
    compose_dir = output_dir / "compose"
    for project, data in rendered.items():
        _safe_json_write(compose_dir / f"{project}.json", data)
    manifest_path = output_dir / "running-set.json"
    _safe_json_write(manifest_path, manifest)
    _safe_json_write(output_dir / "preflight.json", preflight)
    return {
        "phase": "preflight",
        "manifest_location": str(manifest_path),
        "containers": len(manifest["containers"]),
        "compose_projects": sorted(rendered),
    }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git_revision() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise MaintenanceError("unable to read the deployed Git revision") from exc


def _running_kernel() -> str:
    return os.uname().release


def checkpoint_record(phase: str, manifest: str) -> dict[str, str]:
    """Return the deliberately small, non-secret offline breadcrumb."""
    if phase not in CHECKPOINT_PHASES:
        raise MaintenanceError(f"unsupported checkpoint phase: {phase}")
    if not manifest.strip():
        raise MaintenanceError("manifest location must not be empty")
    return {
        "phase": phase,
        "timestamp": _utc_now(),
        "git_revision": _git_revision(),
        "running_kernel": _running_kernel(),
        "manifest_location": manifest,
    }


def append_checkpoint(path: Path, phase: str, manifest: str) -> dict[str, str]:
    """Append one durable transition breadcrumb with reviewed permissions."""
    record = checkpoint_record(phase, manifest)
    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    if path.parent.is_symlink() or (path.exists() and path.is_symlink()):
        raise MaintenanceError("checkpoint path must not traverse a symlink")
    path.parent.chmod(0o755)

    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o644)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        path.chmod(0o644)
    except OSError as exc:
        raise MaintenanceError(f"unable to append checkpoint at {path}") from exc
    return record


def api_request(
    base_url: str, method: str, route: str, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(
        f"{base_url.rstrip('/')}{route}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310 - operator URL
            result = json.load(response)
    except HTTPError as exc:
        try:
            detail = json.load(exc)
        except (json.JSONDecodeError, UnicodeDecodeError):
            detail = {"detail": exc.reason}
        raise MaintenanceError(
            f"coordination API returned HTTP {exc.code}: {json.dumps(detail)}"
        ) from exc
    except TimeoutError as exc:
        logger.warning(
            "coordination API request timed out",
            extra={"method": method, "route": route},
        )
        raise MaintenanceError("coordination API unavailable or malformed") from exc
    except URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            logger.warning(
                "coordination API request timed out",
                extra={"method": method, "route": route},
            )
        raise MaintenanceError("coordination API unavailable or malformed") from exc
    except json.JSONDecodeError as exc:
        raise MaintenanceError("coordination API unavailable or malformed") from exc
    if not isinstance(result, dict):
        raise MaintenanceError("coordination API returned a non-object response")
    return result


def transition(
    args: argparse.Namespace,
    route: str,
    expected_phase: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = api_request(args.api_url, "GET", "/coordination/status")
    if current.get("phase") == expected_phase:
        if current.get("kind") != "host_maintenance":
            raise MaintenanceError(f"phase {expected_phase!r} belongs to another coordination kind")
        recorded_manifest = current.get("manifest_location")
        if recorded_manifest and recorded_manifest != args.manifest:
            raise MaintenanceError("manifest does not match the active coordination")
        append_checkpoint(args.checkpoint, expected_phase, args.manifest)
        return current

    result = api_request(args.api_url, "POST", route, payload)
    if result.get("phase") != expected_phase:
        raise MaintenanceError(f"coordination API did not confirm phase {expected_phase!r}")
    append_checkpoint(args.checkpoint, expected_phase, args.manifest)
    return result


def wait_until_active(args: argparse.Namespace) -> dict[str, Any]:
    """Wait without a short deadline, logging drain evidence at a bounded rate."""
    current = api_request(args.api_url, "GET", "/coordination/status")
    if current.get("phase") == "active":
        if current.get("kind") != "host_maintenance":
            raise MaintenanceError("active coordination belongs to another kind")
        recorded_manifest = current.get("manifest_location")
        if recorded_manifest and recorded_manifest != args.manifest:
            raise MaintenanceError("manifest does not match the active coordination")
        append_checkpoint(args.checkpoint, "active", args.manifest)
        return current
    if current.get("kind") != "host_maintenance" or current.get("phase") != "draining":
        raise MaintenanceError("host maintenance is not draining")

    next_progress_at = 0.0
    while True:
        evidence = api_request(args.api_url, "GET", "/coordination/drain-status")
        now = time.monotonic()
        if now >= next_progress_at:
            logger.info(
                "host maintenance drain progress",
                extra={
                    "generation": current.get("generation"),
                    "prior_phase": "requested",
                    "phase": "draining",
                    "kind": "host_maintenance",
                    "drained": evidence.get("drained"),
                    "blockers": evidence.get("blockers", []),
                },
            )
            next_progress_at = now + args.progress_seconds
        if evidence.get("drained") is True:
            return transition(args, "/coordination/authorize", "active")
        time.sleep(args.poll_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    parser.add_argument("--manifest", help="running-set manifest path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    request = subparsers.add_parser("request")
    request.add_argument("--requested-by", required=True)
    request.add_argument("--reason", required=True)
    request.add_argument("--expected-work", action="append", default=[])

    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--output-dir", type=Path, required=True)
    preflight.add_argument(
        "--console-access-verified",
        action="store_true",
        help="Attest that Oracle Cloud console access was tested for this window",
    )

    subparsers.add_parser("status")
    subparsers.add_parser("begin-drain")
    subparsers.add_parser("drain-status")
    subparsers.add_parser("authorize")
    wait_active = subparsers.add_parser("wait-active")
    wait_active.add_argument("--poll-seconds", type=float, default=5.0)
    wait_active.add_argument("--progress-seconds", type=float, default=60.0)
    subparsers.add_parser("begin-validation")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "preflight":
        return run_preflight(
            args.output_dir,
            console_access_verified=args.console_access_verified,
        )

    if args.command in {
        "request",
        "begin-drain",
        "authorize",
        "wait-active",
        "begin-validation",
    }:
        if not args.manifest:
            raise MaintenanceError("--manifest is required for state transitions")

    if args.command == "status":
        return api_request(args.api_url, "GET", "/coordination/status")
    if args.command == "drain-status":
        return api_request(args.api_url, "GET", "/coordination/drain-status")
    if args.command == "request":
        return transition(
            args,
            "/coordination/request",
            "requested",
            {
                "kind": "host_maintenance",
                "targets": ["host"],
                "requested_by": args.requested_by,
                "reason": args.reason,
                "expected_work": args.expected_work,
                "manifest_location": args.manifest,
            },
        )
    if args.command == "wait-active":
        if args.poll_seconds <= 0 or args.progress_seconds <= 0:
            raise MaintenanceError("wait intervals must be positive")
        return wait_until_active(args)
    routes = {
        "begin-drain": ("/coordination/begin-drain", "draining"),
        "authorize": ("/coordination/authorize", "active"),
        "begin-validation": ("/coordination/begin-validation", "validating"),
    }
    route, phase = routes[args.command]
    return transition(args, route, phase)


def main(argv: list[str] | None = None) -> int:
    handler = logging.StreamHandler()
    handler.setFormatter(CliJsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    args = build_parser().parse_args(argv)
    try:
        result = run(args)
    except MaintenanceError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
