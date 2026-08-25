#!/usr/bin/env python3
"""Operator-side host-maintenance lifecycle client for Plan 142 Stage 2."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_API_URL = "http://localhost:5050"
DEFAULT_CHECKPOINT = Path("/var/lib/cartracker/maintenance/history.jsonl")
CHECKPOINT_PHASES = frozenset({"requested", "draining", "active", "validating"})


class MaintenanceError(RuntimeError):
    """A fail-closed operator error suitable for printing without a traceback."""


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
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
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
            raise MaintenanceError(
                f"phase {expected_phase!r} belongs to another coordination kind"
            )
        recorded_manifest = current.get("manifest_location")
        if recorded_manifest and recorded_manifest != args.manifest:
            raise MaintenanceError("manifest does not match the active coordination")
        append_checkpoint(args.checkpoint, expected_phase, args.manifest)
        return current

    result = api_request(args.api_url, "POST", route, payload)
    if result.get("phase") != expected_phase:
        raise MaintenanceError(
            f"coordination API did not confirm phase {expected_phase!r}"
        )
    append_checkpoint(args.checkpoint, expected_phase, args.manifest)
    return result


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

    subparsers.add_parser("status")
    subparsers.add_parser("begin-drain")
    subparsers.add_parser("drain-status")
    subparsers.add_parser("authorize")
    subparsers.add_parser("begin-validation")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command in {"request", "begin-drain", "authorize", "begin-validation"}:
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
    routes = {
        "begin-drain": ("/coordination/begin-drain", "draining"),
        "authorize": ("/coordination/authorize", "active"),
        "begin-validation": ("/coordination/begin-validation", "validating"),
    }
    route, phase = routes[args.command]
    return transition(args, route, phase)


def main(argv: list[str] | None = None) -> int:
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
