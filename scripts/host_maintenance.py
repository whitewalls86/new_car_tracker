#!/usr/bin/env python3
"""Operator-side host-maintenance lifecycle client for Plan 142 Stage 2."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# The coordination API is ops on 8060, which is what ``scripts/redeploy.sh``
# already uses. The previous default of 5050 is pgAdmin (``"5050:80"`` in
# docker-compose.yml), so every command run without ``--api-url`` answered 500
# against the wrong service -- found 2026-08-29 while scoping the Stage 4 window.
DEFAULT_API_URL = "http://localhost:8060"
DEFAULT_CHECKPOINT = Path("/var/lib/cartracker/maintenance/history.jsonl")
CHECKPOINT_PHASES = frozenset(
    {
        "requested",
        "preflight",
        "draining",
        "active",
        "stopped",
        "updated",
        "rebooting",
        "rebooted",
        "started",
        "validating",
        "complete",
    }
)
REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNING_SET_POLICY = REPO_ROOT / "maintenance-running-set.txt"
logger = logging.getLogger(__name__)
NON_RESTORABLE_POLICY_CLASSES = frozenset({"oneshot", "on-demand", "aux-paused", "aux-foreign"})
PACKAGE_BOUNDARIES = {
    "container_runtime": ("docker", "containerd", "runc"),
    "kernel": ("linux-image", "linux-headers", "linux-modules", "linux-oracle"),
    "ssh": ("openssh",),
    "network": ("netplan", "network-manager", "systemd", "dnsmasq"),
}
APT_CONTROL_UNITS = (
    "apt-daily.timer",
    "apt-daily-upgrade.timer",
    "unattended-upgrades.service",
)
_APT_INSTALLED_RE = re.compile(r"^Inst\s+(\S+)(?:\s+\[[^]]+\])?\s+\((\S+)")
# A versioned kernel package names a kernel GRUB can select; the meta-packages
# (`linux-image-generic`, `-virtual`, `-oracle`) and the `-unsigned-` flavours do
# not, and `GRUB_DEFAULT=0` boots the highest signed versioned one.
_VERSIONED_KERNEL_RE = re.compile(r"^linux-image-(\d[\w.+-]*)$")
# `docker compose run` containers carry the project and service labels but are
# one-shots, not members of the intended running set. Plan 140's collector skips
# the same label (`container_health/collector.py`); this capture did not, so a
# live `run` made two containers share one (project, service) identity.
COMPOSE_ONEOFF_LABEL = "com.docker.compose.oneoff"
# Where a checkpointed boot target came from: a `linux-image-*` in the confirmed
# transaction, or the highest kernel already installed on the host.
KERNEL_TARGET_SOURCES = frozenset({"transaction", "installed"})
HOST_MAINTENANCE_PROCEDURE = (
    "preflight",
    "prepare-update",
    "request",
    "drain",
    "wait-active",
    "stop",
    "update",
    "reboot",
    "start",
    "begin-validation",
    "validate-host",
    "complete",
    "restore-apt-automation",
)
HOST_DISK_FLOORS = {
    "bytes_available": 10 * 1024 * 1024 * 1024,
    "inodes_available": 100_000,
}
REQUIRED_HOST_UNITS = ("docker", "ssh")
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
    ("docker_info", ("docker", "info", "--format", "{{json .}}"), frozenset({0})),
    (
        "docker_daemon_config",
        ("sudo", "cat", "/etc/docker/daemon.json"),
        frozenset({0, 1}),
    ),
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

# Host facts are captured behind the command seam so gates and checkpoint
# records consume data rather than reaching back into the host themselves.
# Identity is split out from the rest: checkpoints and the running-set manifest
# only ever need these three cheap fields, not the full Stage 3 gate sweep.
HOST_IDENTITY_COMMANDS = (
    ("git_revision", ("git", "rev-parse", "HEAD"), frozenset({0})),
    ("kernel", ("uname", "-r"), frozenset({0})),
    ("boot_id", ("cat", "/proc/sys/kernel/random/boot_id"), frozenset({0})),
)
HOST_FACT_COMMANDS = (
    ("reboot_required", ("test", "-e", "/var/run/reboot-required"), frozenset({0, 1})),
    ("mounts", ("findmnt", "--json"), frozenset({0})),
    ("disk_bytes", ("df", "-B1", "/", "/boot", "/mnt/data"), frozenset({0})),
    ("disk_inodes", ("df", "-i", "/", "/boot", "/mnt/data"), frozenset({0})),
    ("failed_units", ("systemctl", "--failed", "--no-legend"), frozenset({0})),
    ("docker_active", ("systemctl", "is-active", "docker"), frozenset({0, 1, 3, 4})),
    ("ssh_active", ("systemctl", "is-active", "ssh"), frozenset({0, 1, 3, 4})),
    ("dns", ("getent", "hosts", "archive.ubuntu.com"), frozenset({0})),
    (
        "clock_synchronised",
        ("timedatectl", "show", "--property=NTPSynchronized", "--value"),
        frozenset({0}),
    ),
    ("sshd_config", ("sudo", "sshd", "-t"), frozenset({0})),
    ("docker_info", ("docker", "info", "--format", "{{json .}}"), frozenset({0})),
    (
        "docker_daemon_config",
        ("sudo", "cat", "/etc/docker/daemon.json"),
        frozenset({0, 1}),
    ),
    ("dpkg_audit", ("sudo", "dpkg", "--audit"), frozenset({0})),
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
    ("package_holds", ("apt-mark", "showhold"), frozenset({0})),
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
        raise MaintenanceError(f"command failed ({completed.returncode}): {' '.join(command)}")
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


def load_running_set_manifest(path: Path) -> dict[str, Any]:
    """Load one preflight manifest without following a replaceable symlink."""
    if path.is_symlink():
        raise MaintenanceError("running-set manifest must not be a symlink")
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MaintenanceError(f"unable to read running-set manifest at {path}") from exc
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise MaintenanceError("running-set manifest has an unsupported schema")
    if not isinstance(manifest.get("containers"), list) or not isinstance(
        manifest.get("compose_projects"), dict
    ):
        raise MaintenanceError("running-set manifest is missing restore records")
    return manifest


def build_running_set_plan(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Derive the exact Compose stop/start plan from captured running state."""
    grouped: dict[str, dict[str, set[str]]] = {}
    seen: set[tuple[str, str]] = set()
    for container in manifest["containers"]:
        if not isinstance(container, dict):
            raise MaintenanceError("running-set manifest contains a malformed container")
        if not (container.get("state") or {}).get("running"):
            continue
        project = container.get("project")
        service = container.get("service")
        if (
            not isinstance(project, str)
            or not project
            or not isinstance(service, str)
            or not service
        ):
            raise MaintenanceError("running container lacks Compose project/service identity")
        identity = (project, service)
        if identity in seen:
            raise MaintenanceError(f"running-set manifest duplicates {project}/{service}")
        seen.add(identity)
        policy_class = container.get("policy_class")
        if policy_class in NON_RESTORABLE_POLICY_CLASSES:
            raise MaintenanceError(
                f"{project}/{service} is running despite policy class {policy_class}"
            )
        profiles = container.get("declared_profiles") or []
        if not isinstance(profiles, list) or not all(
            isinstance(profile, str) and profile for profile in profiles
        ):
            raise MaintenanceError(f"{project}/{service} has malformed Compose profiles")
        group = grouped.setdefault(project, {"services": set(), "profiles": set()})
        group["services"].add(service)
        group["profiles"].update(profiles)

    plan = []
    for project, selected in sorted(grouped.items()):
        project_record = manifest["compose_projects"].get(project)
        if not isinstance(project_record, dict):
            raise MaintenanceError(f"running project {project} has no Compose source record")
        working_directory = project_record.get("working_directory")
        config_files = project_record.get("config_files")
        if not isinstance(working_directory, str) or not working_directory:
            raise MaintenanceError(f"running project {project} has no working directory")
        if (
            not isinstance(config_files, list)
            or not config_files
            or not all(isinstance(config_file, str) and config_file for config_file in config_files)
        ):
            raise MaintenanceError(f"running project {project} has no Compose files")
        command_prefix = ["docker", "compose", "--project-name", project]
        for config_file in config_files:
            command_prefix.extend(("--file", config_file))
        for profile in sorted(selected["profiles"]):
            command_prefix.extend(("--profile", profile))
        services = sorted(selected["services"])
        plan.append(
            {
                "project": project,
                "working_directory": working_directory,
                "config_files": config_files,
                "profiles": sorted(selected["profiles"]),
                "services": services,
                "stop_command": [*command_prefix, "stop", *services],
                "start_command": [*command_prefix, "start", *services],
            }
        )
    if not plan:
        raise MaintenanceError("running-set manifest contains no running Compose services")
    return plan


def latest_checkpoint(path: Path) -> dict[str, Any]:
    """Read the last complete offline breadcrumb without following symlinks."""
    if path.is_symlink():
        raise MaintenanceError("checkpoint path must not be a symlink")
    try:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
    except OSError as exc:
        raise MaintenanceError(f"unable to read checkpoint at {path}") from exc
    for line in reversed(lines):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("phase") in CHECKPOINT_PHASES:
            return record
        raise MaintenanceError("checkpoint contains an unsupported phase")
    raise MaintenanceError(f"unable to read checkpoint at {path}")


def checkpoint_for_phase(path: Path, phase: str) -> dict[str, Any]:
    """Return the newest valid checkpoint for one prior lifecycle phase."""
    if path.is_symlink():
        raise MaintenanceError("checkpoint path must not be a symlink")
    try:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
    except OSError as exc:
        raise MaintenanceError(f"unable to read checkpoint at {path}") from exc
    for line in reversed(lines):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("phase") == phase:
            return record
    raise MaintenanceError(f"checkpoint has no {phase!r} record")


def _selected_container_ids(manifest: dict[str, Any], plan: list[dict[str, Any]]) -> list[str]:
    selected = {(entry["project"], service) for entry in plan for service in entry["services"]}
    ids = []
    for container in manifest["containers"]:
        if (container.get("project"), container.get("service")) not in selected:
            continue
        container_id = container.get("container_id")
        if not isinstance(container_id, str) or not container_id:
            raise MaintenanceError("selected running-set service has no container id")
        ids.append(container_id)
    if len(ids) != len(selected):
        raise MaintenanceError("running-set plan does not map one-to-one to containers")
    return ids


def verify_running_set_state(
    manifest: dict[str, Any], plan: list[dict[str, Any]], *, running: bool
) -> None:
    """Confirm every selected preflight container reached the requested state."""
    container_ids = _selected_container_ids(manifest, plan)
    result = _run_command(("docker", "inspect", *container_ids))
    try:
        inspections = json.loads(result["stdout"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise MaintenanceError("Docker returned malformed restore verification JSON") from exc
    observed = {
        item.get("Id"): bool((item.get("State") or {}).get("Running"))
        for item in inspections
        if isinstance(item, dict)
    }
    mismatched = [
        container_id for container_id in container_ids if observed.get(container_id) is not running
    ]
    if mismatched:
        target = "running" if running else "stopped"
        raise MaintenanceError(
            f"running-set verification did not reach {target}: {len(mismatched)} container(s)"
        )


def run_running_set_action(args: argparse.Namespace, action: str) -> dict[str, Any]:
    """Replay one manifest-scoped Compose stop/start and verify its postcondition."""
    checkpoint = latest_checkpoint(args.checkpoint)
    allowed_phases = {
        "stop": {"active", "stopped"},
        "start": {"stopped", "updated", "rebooted", "started"},
    }
    if checkpoint["phase"] not in allowed_phases[action]:
        raise MaintenanceError(f"cannot {action} from checkpoint phase {checkpoint['phase']!r}")
    if checkpoint.get("manifest_location") != args.manifest:
        raise MaintenanceError("checkpoint manifest does not match --manifest")
    manifest = load_running_set_manifest(Path(args.manifest))
    plan = build_running_set_plan(manifest)
    command_key = f"{action}_command"
    for entry in plan:
        _run_command(tuple(entry[command_key]), cwd=Path(entry["working_directory"]))
    running = action == "start"
    verify_running_set_state(manifest, plan, running=running)
    phase = "started" if running else "stopped"
    append_checkpoint(args.checkpoint, phase, args.manifest)
    return {
        "phase": phase,
        "projects": [entry["project"] for entry in plan],
        "services": sum((entry["services"] for entry in plan), []),
    }


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
    facts = host_identity()
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
        if labels.get(COMPOSE_ONEOFF_LABEL) == "True":
            # A one-off is work in flight, not intended running state. The drain
            # contract already counts it (`ops/coordination_drain.py`'s
            # `_container_processes`), which is where a live `run` must block the
            # window -- before coordination is gated, not at `stop` afterwards.
            continue
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
        "boot_id": facts["boot_id"],
        "git_revision": facts["git_revision"],
        "running_kernel": facts["kernel"],
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
        "observations": observations,
    }


def host_identity() -> dict[str, str]:
    """Capture the cheap identity fields checkpoints and the running-set
    manifest need, without the heavier Stage 3 gate sweep."""
    observations = {
        name: _run_command(command, allowed_returncodes=returncodes)
        for name, command, returncodes in HOST_IDENTITY_COMMANDS
    }
    boot_id = observations["boot_id"]["stdout"].strip()
    if not boot_id:
        raise MaintenanceError("host boot id is empty")
    return {
        "git_revision": observations["git_revision"]["stdout"].strip(),
        "kernel": observations["kernel"]["stdout"].strip(),
        "boot_id": boot_id,
    }


def host_facts() -> dict[str, Any]:
    """Capture Stage 3 host evidence as data through the command seam."""
    identity = host_identity()
    observations = {
        name: _run_command(command, allowed_returncodes=returncodes)
        for name, command, returncodes in HOST_FACT_COMMANDS
    }
    return {
        **identity,
        "reboot_required": observations["reboot_required"]["returncode"] == 0,
        "mounts": observations["mounts"],
        "disk": {
            "bytes": observations["disk_bytes"],
            "inodes": observations["disk_inodes"],
        },
        "units": {
            "failed": observations["failed_units"],
            "docker": observations["docker_active"],
            "ssh": observations["ssh_active"],
        },
        "dns": observations["dns"],
        "clock_synchronised": observations["clock_synchronised"],
        "sshd": observations["sshd_config"],
        "docker": observations["docker_info"],
        "docker_daemon_config": observations["docker_daemon_config"],
        "dpkg": observations["dpkg_audit"],
        "apt_locks": observations["apt_locks"],
        "package_holds": observations["package_holds"],
    }


def _command_stdout(value: Any) -> str | None:
    """Return a captured command's text, or mark malformed evidence unknown."""
    if not isinstance(value, dict) or not isinstance(value.get("stdout"), str):
        return None
    return value["stdout"].strip()


def _command_returncode(value: Any) -> int | None:
    if not isinstance(value, dict) or not isinstance(value.get("returncode"), int):
        return None
    return value["returncode"]


def _mount_devices(value: Any) -> dict[str, str] | None:
    """Extract source devices for the mounts the maintenance contract owns."""
    stdout = _command_stdout(value)
    if stdout is None:
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    filesystems = payload.get("filesystems") if isinstance(payload, dict) else None
    if not isinstance(filesystems, list):
        return None
    # `findmnt --json` returns a tree, not a flat list: every mount below `/`
    # arrives as a descendant of the root entry, so `/mnt/data` is never a
    # top-level element. Reading only the top level made `mounts_expected`
    # report the data mount missing on every host in every state.
    mounts = {}
    pending = list(filesystems)
    while pending:
        filesystem = pending.pop()
        if not isinstance(filesystem, dict):
            continue
        children = filesystem.get("children")
        if isinstance(children, list):
            pending.extend(children)
        target = filesystem.get("target")
        source = filesystem.get("source")
        if target in {"/", "/mnt/data"} and isinstance(source, str) and source:
            mounts[target] = source
    return mounts


def _disk_availability(value: Any) -> dict[str, int] | None:
    """Read available bytes/inodes per target from portable ``df`` output."""
    stdout = _command_stdout(value)
    if stdout is None:
        return None
    lines = stdout.splitlines()
    if len(lines) < 2:
        return None
    availability: dict[str, int] = {}
    for line in lines[1:]:
        fields = line.split()
        if len(fields) < 6:
            return None
        target = fields[-1]
        try:
            availability[target] = int(fields[-3])
        except ValueError:
            return None
    return availability


def _docker_settings(value: Any) -> dict[str, str] | None:
    stdout = _command_stdout(value)
    if stdout is None:
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    root = payload.get("DockerRootDir")
    logging_driver = payload.get("LoggingDriver")
    if (
        not isinstance(root, str)
        or not root
        or not isinstance(logging_driver, str)
        or not logging_driver
    ):
        return None
    return {"DockerRootDir": root, "LoggingDriver": logging_driver}


def _docker_daemon_config(value: Any) -> dict[str, Any] | None:
    stdout = _command_stdout(value)
    if stdout is None:
        return None
    returncode = _command_returncode(value)
    if returncode == 1 and not stdout:
        stderr = value.get("stderr") if isinstance(value, dict) else None
        if isinstance(stderr, str) and "No such file or directory" in stderr:
            return {}
        return None
    if returncode != 0:
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return {
        "data-root": payload.get("data-root"),
        "log-opts": payload.get("log-opts"),
    }


def gate_kernel_expected(facts: dict[str, Any], preflight: dict[str, Any]) -> tuple[str, str]:
    expected = preflight.get("updated_kernel")
    kernel = facts.get("kernel")
    if not isinstance(expected, str) or not expected or not isinstance(kernel, str) or not kernel:
        return "unknown", "kernel target is missing from validation evidence"
    if kernel != expected:
        return "fail", f"running kernel {kernel!r} does not match expected {expected!r}"
    return "pass", "running kernel matches the updated target"


def gate_no_reboot_required(facts: dict[str, Any], preflight: dict[str, Any]) -> tuple[str, str]:
    value = facts.get("reboot_required")
    if not isinstance(value, bool):
        return "unknown", "reboot-required evidence is missing"
    if value:
        return "fail", "a reboot is still required"
    return "pass", "no reboot is required"


def gate_mounts_expected(facts: dict[str, Any], preflight: dict[str, Any]) -> tuple[str, str]:
    observed = _mount_devices(facts.get("mounts"))
    baseline = _mount_devices((preflight.get("observations") or {}).get("mounts"))
    if observed is None or baseline is None:
        return "unknown", "mount evidence is unreadable"
    for target in ("/", "/mnt/data"):
        if target not in observed or target not in baseline:
            return "fail", f"required mount {target} is missing"
        if observed[target] != baseline[target]:
            return "fail", f"mount source changed for {target}"
    return "pass", "root and data mounts match preflight"


def gate_disk_headroom(facts: dict[str, Any], preflight: dict[str, Any]) -> tuple[str, str]:
    disk = facts.get("disk")
    if not isinstance(disk, dict):
        return "unknown", "disk evidence is missing"
    bytes_available = _disk_availability(disk.get("bytes"))
    inodes_available = _disk_availability(disk.get("inodes"))
    if bytes_available is None or inodes_available is None:
        return "unknown", "disk evidence is unreadable"
    for target in ("/", "/mnt/data"):
        if target not in bytes_available or target not in inodes_available:
            return "fail", f"disk evidence has no {target} row"
        if bytes_available[target] < HOST_DISK_FLOORS["bytes_available"]:
            return "fail", f"available bytes below reviewed floor on {target}"
        if inodes_available[target] < HOST_DISK_FLOORS["inodes_available"]:
            return "fail", f"available inodes below reviewed floor on {target}"
    return "pass", "disk byte and inode headroom meet reviewed floors"


def gate_host_services(facts: dict[str, Any], preflight: dict[str, Any]) -> tuple[str, str]:
    units = facts.get("units")
    if not isinstance(units, dict):
        return "unknown", "systemd evidence is missing"
    failed = _command_stdout(units.get("failed"))
    if failed is None:
        return "unknown", "failed-unit evidence is unreadable"
    if failed:
        return "fail", "systemd reports failed units"
    for unit in REQUIRED_HOST_UNITS:
        state = _command_stdout(units.get(unit))
        if state is None:
            return "unknown", f"required unit {unit} evidence is unreadable"
        if state != "active":
            return "fail", f"required unit {unit} is not active"
    dns = _command_stdout(facts.get("dns"))
    if dns is None:
        return "unknown", "DNS evidence is unreadable"
    if not dns:
        return "fail", "DNS lookup returned no addresses"
    clock = _command_stdout(facts.get("clock_synchronised"))
    if clock is None:
        return "unknown", "clock synchronisation evidence is unreadable"
    if clock != "yes":
        return "fail", "clock is not synchronised"
    sshd = _command_returncode(facts.get("sshd"))
    if sshd is None:
        return "unknown", "sshd evidence is unreadable"
    if sshd != 0:
        return "fail", "sshd configuration check failed"
    return "pass", "required host services are healthy"


def gate_docker_daemon(facts: dict[str, Any], preflight: dict[str, Any]) -> tuple[str, str]:
    observed = _docker_settings(facts.get("docker"))
    baseline = _docker_settings((preflight.get("observations") or {}).get("docker_info"))
    observed_config = _docker_daemon_config(facts.get("docker_daemon_config"))
    baseline_config = _docker_daemon_config(
        (preflight.get("observations") or {}).get("docker_daemon_config")
    )
    if observed is None or baseline is None or observed_config is None or baseline_config is None:
        return "unknown", "Docker daemon evidence is unreadable"
    if observed != baseline or observed_config != baseline_config:
        return "fail", "Docker storage path or log limits drifted from preflight"
    return "pass", "Docker storage path and log limits match preflight"


def gate_package_state(facts: dict[str, Any], preflight: dict[str, Any]) -> tuple[str, str]:
    audit = _command_stdout(facts.get("dpkg"))
    locks = _command_returncode(facts.get("apt_locks"))
    if audit is None or locks is None:
        return "unknown", "apt/dpkg evidence is unreadable"
    if locks == 0:
        return "fail", "apt/dpkg lock is held"
    if locks != 1:
        return "unknown", "apt/dpkg lock probe returned an unexpected status"
    if audit:
        return "fail", "dpkg audit reports unfinished configuration"
    return "pass", "apt/dpkg is idle and configured"


HOST_VALIDATION_GATES = {
    "kernel_expected": gate_kernel_expected,
    "no_reboot_required": gate_no_reboot_required,
    "mounts_expected": gate_mounts_expected,
    "disk_headroom": gate_disk_headroom,
    "host_services": gate_host_services,
    "docker_daemon": gate_docker_daemon,
    "package_state": gate_package_state,
}


def collect_host_validation(
    facts: dict[str, Any], preflight: dict[str, Any]
) -> dict[str, dict[str, str]]:
    """Evaluate every host gate; missing or malformed evidence fails closed."""
    return {
        name: {"verdict": verdict, "reason": reason}
        for name, gate in HOST_VALIDATION_GATES.items()
        for verdict, reason in [gate(facts, preflight)]
    }


def host_validation_passes(results: dict[str, dict[str, str]]) -> bool:
    """Only a complete all-pass registry can release the next slice."""
    return set(results) == set(HOST_VALIDATION_GATES) and all(
        result.get("verdict") == "pass" for result in results.values()
    )


def _parse_apt_simulation(output: str) -> dict[str, str]:
    packages = {}
    for line in output.splitlines():
        match = _APT_INSTALLED_RE.match(line)
        if match:
            packages[match.group(1)] = match.group(2)
    return packages


def _package_boundaries(package: str) -> list[str]:
    base_name = package.split(":", 1)[0]
    return [
        boundary
        for boundary, prefixes in PACKAGE_BOUNDARIES.items()
        if base_name.startswith(prefixes)
    ]


def prepare_package_plan(output_dir: Path, *, included_holds: list[str]) -> dict[str, Any]:
    """Refresh, resolve, and download an exact transaction without installing it."""
    refresh = _run_command(("sudo", "apt-get", "update"))
    holds_result = _run_command(("apt-mark", "showhold"))
    holds = sorted(line.strip() for line in holds_result["stdout"].splitlines() if line.strip())
    if sorted(set(included_holds)) != holds:
        raise MaintenanceError(
            "--include-held must name every held package exactly: "
            f"expected {holds}, got {sorted(set(included_holds))}"
        )

    ordinary = _run_command(("apt-get", "--simulate", "upgrade"))
    packages = _parse_apt_simulation(ordinary["stdout"])
    for package in holds:
        held = _run_command(
            (
                "apt-get",
                "--simulate",
                "--allow-change-held-packages",
                "install",
                package,
            )
        )
        packages.update(_parse_apt_simulation(held["stdout"]))

    pins = [f"{name}={version}" for name, version in sorted(packages.items())]
    exact_simulation_command = (
        "apt-get",
        "--simulate",
        "--allow-change-held-packages",
        "--no-remove",
        "install",
        *pins,
    )
    apply_command = (
        "sudo",
        "apt-get",
        "--yes",
        "--allow-change-held-packages",
        "--no-remove",
        "install",
        *pins,
    )
    if pins:
        exact = _run_command(exact_simulation_command)
        exact_packages = _parse_apt_simulation(exact["stdout"])
        if exact_packages != packages:
            raise MaintenanceError("exact package transaction differs from the reviewed resolution")
        _run_command(
            (
                "sudo",
                "apt-get",
                "--download-only",
                "--yes",
                "--allow-change-held-packages",
                "--no-remove",
                "install",
                *pins,
            )
        )

    package_rows = [
        {
            "name": name,
            "version": version,
            "boundaries": _package_boundaries(name),
            "held": name in holds,
        }
        for name, version in sorted(packages.items())
    ]
    plan = {
        "schema_version": 1,
        "prepared_at": _utc_now(),
        "apt_update_stdout_sha256": hashlib.sha256(refresh["stdout"].encode()).hexdigest(),
        "holds": holds,
        "packages": package_rows,
        "apply_command": list(apply_command) if pins else [],
        "requires_reboot_review": any(
            "kernel" in package["boundaries"] for package in package_rows
        ),
        "compatibility_boundaries": sorted(
            {boundary for package in package_rows for boundary in package["boundaries"]}
        ),
    }
    plan_path = output_dir / "package-plan.json"
    _safe_json_write(plan_path, plan)
    digest = hashlib.sha256(plan_path.read_bytes()).hexdigest()
    return {
        "phase": "package-prepared",
        "package_plan": str(plan_path),
        "package_plan_sha256": digest,
        "packages": len(package_rows),
        "compatibility_boundaries": plan["compatibility_boundaries"],
    }


def load_package_plan(path: Path, expected_sha256: str) -> tuple[dict[str, Any], str]:
    """Load exactly the package plan whose digest the operator confirmed."""
    if path.is_symlink():
        raise MaintenanceError("package plan must not be a symlink")
    try:
        raw = path.read_bytes()
        plan = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise MaintenanceError(f"unable to read package plan at {path}") from exc
    digest = hashlib.sha256(raw).hexdigest()
    if not hmac.compare_digest(digest, expected_sha256.lower()):
        raise MaintenanceError("package plan SHA-256 does not match --confirm-plan")
    if not isinstance(plan, dict) or plan.get("schema_version") != 1:
        raise MaintenanceError("package plan has an unsupported schema")
    packages = plan.get("packages")
    apply_command = plan.get("apply_command")
    if not isinstance(packages, list) or not isinstance(apply_command, list):
        raise MaintenanceError("package plan is missing transaction records")
    if apply_command and (
        apply_command[:2] != ["sudo", "apt-get"]
        or not all(isinstance(argument, str) and argument for argument in apply_command)
    ):
        raise MaintenanceError("package plan contains an unsupported apply command")
    return plan, digest


def _read_existing_update_evidence(path: Path, plan_sha256: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if path.is_symlink():
        raise MaintenanceError("update evidence must not be a symlink")
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MaintenanceError(f"unable to read update evidence at {path}") from exc
    if not isinstance(evidence, dict) or evidence.get("package_plan_sha256") != plan_sha256:
        raise MaintenanceError("update evidence belongs to another package plan")
    return evidence


def assert_package_manager_idle() -> None:
    """Fail closed rather than stopping automation during an apt/dpkg transaction."""
    locks = _run_command(
        (
            "sudo",
            "fuser",
            "/var/lib/dpkg/lock-frontend",
            "/var/lib/apt/lists/lock",
            "/var/cache/apt/archives/lock",
        ),
        allowed_returncodes=frozenset({0, 1}),
    )
    if locks["returncode"] == 0:
        raise MaintenanceError("apt/dpkg lock is held; refusing to interrupt its owner")
    audit = _run_command(("sudo", "dpkg", "--audit"))
    if audit["stdout"].strip():
        raise MaintenanceError("dpkg --audit reported an inconsistent package database")


def _kernel_sort_key(kernel: str) -> tuple[tuple[int, int, str], ...]:
    """Order kernel names by their numeric ABI, not lexically.

    ``6.8.0-1060-oracle`` has to sort above ``6.8.0-999-oracle``. A plain string
    sort puts ``999`` last and would name a kernel GRUB is not going to select,
    which matters now that the fallback below chooses among every kernel the
    host has installed rather than the one or two in a single transaction.
    """
    return tuple(
        (0, int(chunk), "") if chunk.isdigit() else (1, 0, chunk)
        for chunk in re.split(r"(\d+)", kernel)
        if chunk
    )


def _versioned_kernels(names: Iterable[str]) -> list[str]:
    """Return the kernel names of versioned ``linux-image-*`` packages, ordered."""
    matches = [_VERSIONED_KERNEL_RE.match(name) for name in names]
    return sorted(
        (match.group(1) for match in matches if match is not None),
        key=_kernel_sort_key,
    )


def installed_kernels() -> list[str]:
    """Return every versioned kernel currently installed on the host, ordered.

    ``unattended-upgrades`` installs kernels on its own schedule and defers only
    the reboot, so the window that boots a kernel is routinely not the window
    that installed it. Reading the host's installed set is what lets such a
    window name a boot target at all.

    Only fully installed packages count. ``dpkg-query`` also reports removed
    packages whose config files remain, and a kernel in that state has no
    ``/boot`` image for GRUB to select.
    """
    result = _run_command(
        (
            "dpkg-query",
            "--show",
            "--showformat=${db:Status-Status} ${Package}\n",
            "linux-image-*",
        ),
        allowed_returncodes=frozenset({0, 1}),
    )
    installed = []
    for line in result["stdout"].splitlines():
        status, separator, name = line.partition(" ")
        if separator and status == "installed":
            installed.append(name.strip())
    return _versioned_kernels(installed)


def apply_package_plan(args: argparse.Namespace) -> dict[str, Any]:
    """Apply one confirmed offline transaction and leave apt automation masked."""
    checkpoint = latest_checkpoint(args.checkpoint)
    if checkpoint["phase"] not in {"stopped", "updated"}:
        raise MaintenanceError(f"cannot update from checkpoint phase {checkpoint['phase']!r}")
    if checkpoint.get("manifest_location") != args.manifest:
        raise MaintenanceError("checkpoint manifest does not match --manifest")
    if not args.confirm_apply:
        raise MaintenanceError("--confirm-apply is required for package installation")

    plan_path = Path(args.package_plan)
    plan, digest = load_package_plan(plan_path, args.confirm_plan)
    packages = plan["packages"]
    boundaries = plan.get("compatibility_boundaries") or []
    if packages and not args.release_notes_reviewed:
        raise MaintenanceError("--release-notes-reviewed is required")
    if boundaries and not args.compatibility_reviewed:
        raise MaintenanceError("--compatibility-reviewed is required")

    evidence_path = plan_path.with_name("update-result.json")
    existing = _read_existing_update_evidence(evidence_path, digest)
    unit_states = (existing or {}).get("apt_unit_states_before", {})
    assert_package_manager_idle()
    if not unit_states:
        for unit in APT_CONTROL_UNITS:
            state = _run_command(
                ("systemctl", "is-enabled", unit),
                allowed_returncodes=frozenset({0, 1, 3, 4}),
            )
            unit_states[unit] = state["stdout"].strip()

    _run_command(("sudo", "systemctl", "mask", "--now", *APT_CONTROL_UNITS))
    if plan["apply_command"]:
        _run_command(tuple(plan["apply_command"]))
    audit = _run_command(("sudo", "dpkg", "--audit"))
    if audit["stdout"].strip():
        raise MaintenanceError("dpkg --audit reported an inconsistent package database")

    package_names = [package["name"] for package in packages]
    installed_versions = {}
    if package_names:
        installed = _run_command(
            (
                "dpkg-query",
                "--show",
                "--showformat=${Package}\t${Version}\n",
                *package_names,
            )
        )
        for line in installed["stdout"].splitlines():
            name, separator, version = line.partition("\t")
            if separator:
                installed_versions[name] = version
        expected_versions = {package["name"]: package["version"] for package in packages}
        if installed_versions != expected_versions:
            raise MaintenanceError("installed package versions differ from the confirmed plan")

    holds_result = _run_command(("apt-mark", "showhold"))
    holds_after = sorted(
        line.strip() for line in holds_result["stdout"].splitlines() if line.strip()
    )
    if holds_after != sorted(plan.get("holds") or []):
        raise MaintenanceError("package holds changed during the confirmed transaction")

    # The kernel running before reboot is deliberately not the validation target.
    # A versioned linux-image package names the kernel that bootloader selection
    # will start after this transaction and the explicitly confirmed reboot.
    #
    # The transaction is not the only source, and on this host it is usually not
    # the source at all: `unattended-upgrades` installs kernels on its own
    # schedule and only the reboot is deferred, so a window that boots
    # `6.8.0-1060-oracle` routinely carries no `linux-image-*` of its own. Read
    # from the transaction first, because a kernel this operator confirmed
    # outranks one that merely happens to be present, and fall back to the
    # highest installed kernel otherwise. Record which source answered: after the
    # reboot the checkpoint is the only evidence an operator has offline, and
    # "confirmed in this transaction" and "already on the host" are not the same
    # claim about how the target was chosen.
    transaction_kernels = _versioned_kernels(installed_versions)
    if transaction_kernels:
        boot_kernel_target = transaction_kernels[-1]
        kernel_target_source = "transaction"
    else:
        host_kernels = installed_kernels()
        boot_kernel_target = host_kernels[-1] if host_kernels else None
        kernel_target_source = "installed"
    if "kernel" in boundaries and kernel_target_source != "transaction":
        raise MaintenanceError("confirmed kernel update has no versioned installed boot target")
    if not boot_kernel_target:
        # `validate-host` requires this target unconditionally, and it runs after
        # the reboot and after `start`. Refusing here costs a paused window that
        # has not yet mutated the boot path; discovering it there strands the
        # window at `validating` with no path to `complete`.
        raise MaintenanceError("no versioned kernel is installed to validate the boot against")

    _run_command(("sync",))
    facts = host_identity()
    evidence = {
        "schema_version": 1,
        "applied_at": _utc_now(),
        "package_plan": str(plan_path),
        "package_plan_sha256": digest,
        "apt_unit_states_before": unit_states,
        "apt_automation_masked": True,
        "installed_versions": installed_versions,
        "holds_after": holds_after,
        "running_kernel": facts["kernel"],
        "boot_kernel_target": boot_kernel_target,
        "boot_kernel_target_source": kernel_target_source,
    }
    _safe_json_write(evidence_path, evidence)
    append_checkpoint(
        args.checkpoint,
        "updated",
        args.manifest,
        facts=facts,
        kernel_target=boot_kernel_target,
        kernel_target_source=kernel_target_source,
    )
    return {
        "phase": "updated",
        "package_plan_sha256": digest,
        "packages": len(packages),
        "update_evidence": str(evidence_path),
        "boot_kernel_target": boot_kernel_target,
        "boot_kernel_target_source": kernel_target_source,
        "apt_automation_masked": True,
    }


def restore_apt_automation(args: argparse.Namespace) -> dict[str, Any]:
    """Restore the apt controls only after the resume gate has completed."""
    checkpoint = latest_checkpoint(args.checkpoint)
    if checkpoint.get("phase") != "complete":
        raise MaintenanceError(
            "cannot restore apt automation before the resume gate has passed"
        )
    if checkpoint.get("manifest_location") != args.manifest:
        raise MaintenanceError("checkpoint manifest does not match --manifest")

    plan_path = Path(args.package_plan)
    plan, digest = load_package_plan(plan_path, args.confirm_plan)
    evidence = _read_existing_update_evidence(plan_path.with_name("update-result.json"), digest)
    if evidence is None or evidence.get("apt_automation_masked") is not True:
        raise MaintenanceError("package plan has no masked apt automation evidence")
    states = evidence.get("apt_unit_states_before")
    if not isinstance(states, dict) or set(states) != set(APT_CONTROL_UNITS):
        raise MaintenanceError("update evidence has incomplete apt unit enablement states")
    if any(state not in {"enabled", "disabled"} for state in states.values()):
        raise MaintenanceError("update evidence has unsupported apt unit enablement states")

    _run_command(("sudo", "systemctl", "unmask", *APT_CONTROL_UNITS))
    for unit in APT_CONTROL_UNITS:
        action = "enable" if states[unit] == "enabled" else "disable"
        _run_command(("sudo", "systemctl", action, unit))

    verified_states = {}
    for unit in APT_CONTROL_UNITS:
        observed = _run_command(
            ("systemctl", "is-enabled", unit),
            allowed_returncodes=frozenset({0, 1, 3, 4}),
        )["stdout"].strip()
        verified_states[unit] = observed
        if observed != states[unit]:
            raise MaintenanceError(f"apt automation unit did not restore: {unit}")

    holds = sorted(
        line.strip()
        for line in _run_command(("apt-mark", "showhold"))["stdout"].splitlines()
        if line.strip()
    )
    if holds != sorted(plan.get("holds") or []):
        raise MaintenanceError("package holds changed before apt automation restoration")
    return {
        "phase": "complete",
        "apt_automation_restored": True,
        "apt_unit_states": verified_states,
        "holds": holds,
    }


def run_reboot_boundary(args: argparse.Namespace) -> dict[str, Any]:
    """Initiate an explicitly confirmed reboot or prove it completed on replay."""
    checkpoint = latest_checkpoint(args.checkpoint)
    if checkpoint["phase"] not in {"updated", "rebooting", "rebooted"}:
        raise MaintenanceError(f"cannot reboot from checkpoint phase {checkpoint['phase']!r}")
    if checkpoint.get("manifest_location") != args.manifest:
        raise MaintenanceError("checkpoint manifest does not match --manifest")
    manifest = load_running_set_manifest(Path(args.manifest))
    captured_boot_id = manifest.get("boot_id")
    if not isinstance(captured_boot_id, str) or not captured_boot_id:
        raise MaintenanceError("running-set manifest has no preflight boot id")
    facts = host_identity()
    current_boot_id = facts["boot_id"]

    if current_boot_id != captured_boot_id:
        append_checkpoint(args.checkpoint, "rebooted", args.manifest, facts=facts)
        return {
            "phase": "rebooted",
            "boot_id_changed": True,
            "running_kernel": facts["kernel"],
        }
    if checkpoint["phase"] == "rebooted":
        raise MaintenanceError("rebooted checkpoint conflicts with the current boot id")
    if not args.confirm_reboot:
        raise MaintenanceError("--confirm-reboot is required to reboot the host")

    _run_command(("sync",))
    append_checkpoint(args.checkpoint, "rebooting", args.manifest, facts=facts)
    _run_command(("sudo", "systemctl", "reboot"))
    return {
        "phase": "rebooting",
        "boot_id_changed": False,
        "running_kernel": facts["kernel"],
    }


def run_preflight(output_dir: Path) -> dict[str, Any]:
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


def load_preflight_bundle(path: Path) -> dict[str, Any]:
    """Read the pre-window evidence bundle without following a mutable link."""
    if path.is_symlink():
        raise MaintenanceError("preflight evidence must not be a symlink")
    try:
        bundle = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MaintenanceError(f"unable to read preflight evidence at {path}") from exc
    if not isinstance(bundle, dict) or bundle.get("schema_version") != 1:
        raise MaintenanceError("preflight evidence has an unsupported schema")
    if not isinstance(bundle.get("observations"), dict):
        raise MaintenanceError("preflight evidence is missing observations")
    return bundle


def run_validate_host(args: argparse.Namespace) -> dict[str, Any]:
    """Collect and record host validation without changing coordination state."""
    checkpoint = latest_checkpoint(args.checkpoint)
    if checkpoint.get("phase") != "validating":
        raise MaintenanceError(
            f"cannot validate host from checkpoint phase {checkpoint.get('phase')!r}"
        )
    if checkpoint.get("manifest_location") != args.manifest:
        raise MaintenanceError("checkpoint manifest does not match --manifest")
    preflight = load_preflight_bundle(args.preflight)
    updated = checkpoint_for_phase(args.checkpoint, "updated")
    expected_kernel = updated.get("kernel_target")
    if not isinstance(expected_kernel, str) or not expected_kernel:
        raise MaintenanceError("updated checkpoint has no kernel target")
    facts = host_facts()
    gates = collect_host_validation(facts, {**preflight, "updated_kernel": expected_kernel})
    result = {
        "schema_version": 1,
        "validated_at": _utc_now(),
        "preflight": str(args.preflight),
        "manifest_location": args.manifest,
        "gates": gates,
        "passed": host_validation_passes(gates),
    }
    _safe_json_write(args.output_dir / "validate-host.json", result)
    if not result["passed"]:
        failed = [name for name, gate in gates.items() if gate["verdict"] != "pass"]
        raise MaintenanceError(f"host validation did not pass: {', '.join(failed)}")
    state = api_request(args.api_url, "GET", "/coordination/status")
    if state.get("phase") != "validating" or state.get("kind") != "host_maintenance":
        raise MaintenanceError("coordination is not validating host maintenance")
    generation = state.get("generation")
    if not isinstance(generation, int) or generation < 1:
        raise MaintenanceError("coordination status has no valid generation")
    evidence_digests = {
        "preflight": hashlib.sha256(args.preflight.read_bytes()).hexdigest(),
        "manifest": hashlib.sha256(Path(args.manifest).read_bytes()).hexdigest(),
    }
    submitted = api_request(
        args.api_url,
        "POST",
        "/coordination/host-evidence",
        {
            "generation": generation,
            "gates": gates,
            "evidence_digests": evidence_digests,
        },
    )
    result["submission"] = submitted
    return result


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def checkpoint_record(phase: str, manifest: str, facts: dict[str, Any]) -> dict[str, str]:
    """Return the deliberately small, non-secret offline breadcrumb."""
    if phase not in CHECKPOINT_PHASES:
        raise MaintenanceError(f"unsupported checkpoint phase: {phase}")
    if not manifest.strip():
        raise MaintenanceError("manifest location must not be empty")
    git_revision = facts.get("git_revision")
    kernel = facts.get("kernel")
    if not isinstance(git_revision, str) or not git_revision:
        raise MaintenanceError("host facts are missing git_revision")
    if not isinstance(kernel, str) or not kernel:
        raise MaintenanceError("host facts are missing kernel")
    return {
        "phase": phase,
        "timestamp": _utc_now(),
        "git_revision": git_revision,
        "running_kernel": kernel,
        "manifest_location": manifest,
    }


def append_checkpoint(
    path: Path,
    phase: str,
    manifest: str,
    *,
    facts: dict[str, Any] | None = None,
    kernel_target: str | None = None,
    kernel_target_source: str | None = None,
) -> dict[str, str]:
    """Append one durable transition breadcrumb with reviewed permissions."""
    # Reject invalid callers before collecting from the host.
    if phase not in CHECKPOINT_PHASES:
        raise MaintenanceError(f"unsupported checkpoint phase: {phase}")
    if not manifest.strip():
        raise MaintenanceError("manifest location must not be empty")
    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    if path.parent.is_symlink() or (path.exists() and path.is_symlink()):
        raise MaintenanceError("checkpoint path must not traverse a symlink")
    path.parent.chmod(0o755)
    record = checkpoint_record(phase, manifest, facts if facts is not None else host_identity())
    if kernel_target is not None:
        if not kernel_target:
            raise MaintenanceError("kernel target must not be empty")
        record["kernel_target"] = kernel_target
    if kernel_target_source is not None:
        if kernel_target is None:
            raise MaintenanceError("kernel target source without a kernel target")
        if kernel_target_source not in KERNEL_TARGET_SOURCES:
            raise MaintenanceError(f"unsupported kernel target source: {kernel_target_source}")
        record["kernel_target_source"] = kernel_target_source

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
    checkpoint_phase: str | None = None,
) -> dict[str, Any]:
    current = api_request(args.api_url, "GET", "/coordination/status")
    if current.get("phase") == expected_phase:
        if current.get("kind") != "host_maintenance":
            raise MaintenanceError(f"phase {expected_phase!r} belongs to another coordination kind")
        recorded_manifest = current.get("manifest_location")
        if recorded_manifest and recorded_manifest != args.manifest:
            raise MaintenanceError("manifest does not match the active coordination")
        append_checkpoint(args.checkpoint, checkpoint_phase or expected_phase, args.manifest)
        return current

    result = api_request(args.api_url, "POST", route, payload)
    if result.get("phase") != expected_phase:
        raise MaintenanceError(f"coordination API did not confirm phase {expected_phase!r}")
    append_checkpoint(args.checkpoint, checkpoint_phase or expected_phase, args.manifest)
    return result


def complete_transition(args: argparse.Namespace) -> dict[str, Any]:
    """Complete, or repair a completion checkpoint after an ambiguous response."""
    current = api_request(args.api_url, "GET", "/coordination/status")
    generation = current.get("generation")
    if not isinstance(generation, int) or generation < 1:
        raise MaintenanceError("coordination status has no valid generation")
    payload = {
        "confirm_complete": True,
        "generation": generation,
        "manifest_sha256": hashlib.sha256(Path(args.manifest).read_bytes()).hexdigest(),
    }
    if current.get("phase") == "validating":
        if current.get("kind") != "host_maintenance":
            raise MaintenanceError("validating coordination belongs to another kind")
        if current.get("manifest_location") != args.manifest:
            raise MaintenanceError("manifest does not match the active coordination")
    elif current.get("phase") != "none":
        raise MaintenanceError("host maintenance is not ready to complete")
    result = api_request(args.api_url, "POST", "/coordination/complete", payload)
    if result.get("phase") != "none" or result.get("generation") != generation:
        raise MaintenanceError("coordination API did not confirm completion")
    append_checkpoint(args.checkpoint, "complete", args.manifest)
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
    subparsers.add_parser("plan")

    request = subparsers.add_parser("request")
    request.add_argument("--requested-by", required=True)
    request.add_argument("--reason", required=True)
    request.add_argument("--expected-work", action="append", default=[])

    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--output-dir", type=Path, required=True)

    validate_host = subparsers.add_parser("validate-host")
    validate_host.add_argument("--preflight", type=Path, required=True)
    validate_host.add_argument("--output-dir", type=Path, required=True)

    prepare_update = subparsers.add_parser("prepare-update")
    prepare_update.add_argument("--output-dir", type=Path, required=True)
    prepare_update.add_argument("--include-held", action="append", default=[])

    update = subparsers.add_parser("update")
    update.add_argument("--package-plan", type=Path, required=True)
    update.add_argument("--confirm-plan", required=True)
    update.add_argument("--confirm-apply", action="store_true")
    update.add_argument("--release-notes-reviewed", action="store_true")
    update.add_argument("--compatibility-reviewed", action="store_true")

    reboot = subparsers.add_parser("reboot")
    reboot.add_argument("--confirm-reboot", action="store_true")

    subparsers.add_parser("status")
    subparsers.add_parser("begin-drain")
    subparsers.add_parser("drain")
    subparsers.add_parser("drain-status")
    subparsers.add_parser("authorize")
    wait_active = subparsers.add_parser("wait-active")
    wait_active.add_argument("--poll-seconds", type=float, default=5.0)
    wait_active.add_argument("--progress-seconds", type=float, default=60.0)
    subparsers.add_parser("stop")
    subparsers.add_parser("start")
    subparsers.add_parser("begin-validation")
    complete = subparsers.add_parser("complete")
    complete.add_argument("--confirm-complete", action="store_true")
    restore_apt = subparsers.add_parser("restore-apt-automation")
    restore_apt.add_argument("--package-plan", type=Path, required=True)
    restore_apt.add_argument("--confirm-plan", required=True)
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "plan":
        return {
            "phase": "dry-run",
            "commands": list(HOST_MAINTENANCE_PROCEDURE),
            "complete_implicit": False,
        }
    if args.command == "preflight":
        result = run_preflight(args.output_dir)
        append_checkpoint(
            args.checkpoint,
            "preflight",
            result["manifest_location"],
        )
        return result
    if args.command == "prepare-update":
        return prepare_package_plan(
            args.output_dir,
            included_holds=args.include_held,
        )

    if args.command in {
        "request",
        "begin-drain",
        "drain",
        "authorize",
        "wait-active",
        "stop",
        "update",
        "reboot",
        "start",
        "begin-validation",
        "validate-host",
        "complete",
        "restore-apt-automation",
    }:
        if not args.manifest:
            raise MaintenanceError("--manifest is required for state transitions")

    if args.command == "validate-host":
        return run_validate_host(args)
    if args.command == "restore-apt-automation":
        return restore_apt_automation(args)

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
    if args.command in {"stop", "start"}:
        return run_running_set_action(args, args.command)
    if args.command == "update":
        return apply_package_plan(args)
    if args.command == "reboot":
        return run_reboot_boundary(args)
    routes = {
        "begin-drain": ("/coordination/begin-drain", "draining"),
        "drain": ("/coordination/begin-drain", "draining"),
        "authorize": ("/coordination/authorize", "active"),
        "begin-validation": ("/coordination/begin-validation", "validating"),
        "complete": ("/coordination/complete", "none"),
    }
    route, phase = routes[args.command]
    payload = (
        {"confirm_complete": True}
        if args.command == "complete" and args.confirm_complete
        else None
    )
    if args.command == "complete" and payload is None:
        raise MaintenanceError("--confirm-complete is required")
    if args.command == "complete":
        return complete_transition(args)
    return transition(
        args,
        route,
        phase,
        payload,
        checkpoint_phase="complete" if args.command == "complete" else None,
    )


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
