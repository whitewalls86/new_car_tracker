"""Plan 142 Stage 2 operator client and offline checkpoint contract."""

import json
import logging
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import host_maintenance


def _args(tmp_path, command, **kwargs):
    values = {
        "api_url": "http://ops",
        "checkpoint": tmp_path / "maintenance" / "history.jsonl",
        "manifest": "/var/lib/cartracker/maintenance/running-set.json",
        "command": command,
        "requested_by": "operator",
        "reason": "monthly patching",
        "expected_work": ["install reviewed packages", "reboot"],
    }
    values.update(kwargs)
    return Namespace(**values)


def test_checkpoint_is_append_only_five_field_jsonl(mocker, tmp_path):
    mocker.patch.object(host_maintenance, "_git_revision", return_value="abc123")
    mocker.patch.object(host_maintenance, "_running_kernel", return_value="6.8.0-test")
    mocker.patch.object(host_maintenance, "_utc_now", side_effect=["t1", "t2"])
    chmod = mocker.patch.object(Path, "chmod", autospec=True, side_effect=Path.chmod)
    path = tmp_path / "maintenance" / "history.jsonl"

    host_maintenance.append_checkpoint(path, "requested", "/tmp/manifest.json")
    host_maintenance.append_checkpoint(path, "draining", "/tmp/manifest.json")

    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["phase"] for row in rows] == ["requested", "draining"]
    assert {row["running_kernel"] for row in rows} == {"6.8.0-test"}
    assert set(rows[0]) == {
        "phase",
        "timestamp",
        "git_revision",
        "running_kernel",
        "manifest_location",
    }
    # Assert the modes the client requests; only POSIX hosts store them verbatim.
    assert chmod.call_args_list == [
        mocker.call(path.parent, 0o755),
        mocker.call(path, 0o644),
        mocker.call(path.parent, 0o755),
        mocker.call(path, 0o644),
    ]


@pytest.mark.parametrize("phase", ["none", "complete", "offline", "secret=value"])
def test_checkpoint_rejects_unreviewed_phases(phase, tmp_path):
    with pytest.raises(host_maintenance.MaintenanceError, match="unsupported"):
        host_maintenance.append_checkpoint(tmp_path / "history", phase, "/tmp/m")


def test_checkpoint_refuses_symlink(mocker, tmp_path):
    mocker.patch.object(host_maintenance, "checkpoint_record", return_value={})
    link = tmp_path / "history"
    link.write_text("")

    # Creating a real symlink requires elevated privileges on Windows. The
    # behavior owned here is refusing a path classified as a symlink, not
    # pathlib's OS integration.
    real_is_symlink = Path.is_symlink

    def _is_symlink(path):
        return path == link or real_is_symlink(path)

    mocker.patch.object(Path, "is_symlink", autospec=True, side_effect=_is_symlink)

    with pytest.raises(host_maintenance.MaintenanceError, match="symlink"):
        host_maintenance.append_checkpoint(link, "active", "/tmp/m")


def test_request_is_always_host_scoped_and_checkpoints_after_confirmation(mocker, tmp_path):
    api = mocker.patch.object(
        host_maintenance,
        "api_request",
        side_effect=[{"phase": "none"}, {"phase": "requested"}],
    )
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")
    args = _args(tmp_path, "request")

    assert host_maintenance.run(args) == {"phase": "requested"}

    assert api.call_args_list == [
        mocker.call("http://ops", "GET", "/coordination/status"),
        mocker.call(
            "http://ops",
            "POST",
            "/coordination/request",
            {
                "kind": "host_maintenance",
                "targets": ["host"],
                "requested_by": "operator",
                "reason": "monthly patching",
                "expected_work": ["install reviewed packages", "reboot"],
                "manifest_location": args.manifest,
            },
        ),
    ]
    checkpoint.assert_called_once_with(args.checkpoint, "requested", args.manifest)


@pytest.mark.parametrize(
    ("command", "route", "phase"),
    [
        ("begin-drain", "/coordination/begin-drain", "draining"),
        ("drain", "/coordination/begin-drain", "draining"),
        ("authorize", "/coordination/authorize", "active"),
        ("begin-validation", "/coordination/begin-validation", "validating"),
    ],
)
def test_mutating_commands_checkpoint_only_confirmed_phase(mocker, tmp_path, command, route, phase):
    api = mocker.patch.object(
        host_maintenance,
        "api_request",
        side_effect=[{"phase": "prior"}, {"phase": phase}],
    )
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")
    args = _args(tmp_path, command)

    host_maintenance.run(args)

    assert api.call_args_list == [
        mocker.call("http://ops", "GET", "/coordination/status"),
        mocker.call("http://ops", "POST", route, None),
    ]
    checkpoint.assert_called_once_with(args.checkpoint, phase, args.manifest)


def test_transition_does_not_checkpoint_ambiguous_response(mocker, tmp_path):
    mocker.patch.object(
        host_maintenance,
        "api_request",
        side_effect=[{"phase": "draining"}, {"phase": "draining"}],
    )
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")

    with pytest.raises(host_maintenance.MaintenanceError, match="did not confirm"):
        host_maintenance.run(_args(tmp_path, "authorize"))

    checkpoint.assert_not_called()


def test_replay_repairs_checkpoint_without_repeating_transition(mocker, tmp_path):
    current = {
        "kind": "host_maintenance",
        "phase": "active",
        "manifest_location": "/var/lib/cartracker/maintenance/running-set.json",
    }
    api = mocker.patch.object(host_maintenance, "api_request", return_value=current)
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")
    args = _args(tmp_path, "authorize")

    assert host_maintenance.run(args) == current

    api.assert_called_once_with("http://ops", "GET", "/coordination/status")
    checkpoint.assert_called_once_with(args.checkpoint, "active", args.manifest)


@pytest.mark.parametrize(
    ("command", "route"),
    [
        ("status", "/coordination/status"),
        ("drain-status", "/coordination/drain-status"),
    ],
)
def test_read_commands_never_write_checkpoint(mocker, tmp_path, command, route):
    api = mocker.patch.object(host_maintenance, "api_request", return_value={"phase": "none"})
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")

    host_maintenance.run(_args(tmp_path, command, manifest=None))

    api.assert_called_once_with("http://ops", "GET", route)
    checkpoint.assert_not_called()


def test_wait_active_logs_progress_at_bounded_interval(mocker, tmp_path, caplog):
    caplog.set_level("INFO")
    api = mocker.patch.object(
        host_maintenance,
        "api_request",
        side_effect=[
            {"phase": "draining", "kind": "host_maintenance", "generation": 7},
            {"drained": False, "blockers": ["running_detail_claims"]},
            {"drained": False, "blockers": ["running_detail_claims"]},
            {"drained": True, "blockers": []},
        ],
    )
    transition = mocker.patch.object(
        host_maintenance,
        "transition",
        return_value={"phase": "active"},
    )
    sleep = mocker.patch.object(host_maintenance.time, "sleep")
    mocker.patch.object(host_maintenance.time, "monotonic", side_effect=[0.0, 5.0, 10.0])
    args = _args(tmp_path, "wait-active", poll_seconds=5.0, progress_seconds=60.0)

    assert host_maintenance.run(args) == {"phase": "active"}

    assert api.call_count == 4
    assert sleep.call_args_list == [mocker.call(5.0), mocker.call(5.0)]
    transition.assert_called_once_with(args, "/coordination/authorize", "active")
    progress = [
        record for record in caplog.records if record.msg == "host maintenance drain progress"
    ]
    assert len(progress) == 1
    assert progress[0].generation == 7
    assert progress[0].blockers == ["running_detail_claims"]


def test_api_timeout_is_logged_with_request_identity(mocker, caplog):
    mocker.patch.object(host_maintenance, "urlopen", side_effect=TimeoutError("slow"))

    with pytest.raises(host_maintenance.MaintenanceError, match="unavailable"):
        host_maintenance.api_request("http://ops", "GET", "/coordination/status")

    timeout = caplog.records[-1]
    assert timeout.levelname == "WARNING"
    assert timeout.method == "GET"
    assert timeout.route == "/coordination/status"


def test_cli_formatter_preserves_reviewed_structured_fields():
    record = logging.LogRecord(
        "host-maintenance",
        logging.INFO,
        __file__,
        1,
        "drain progress",
        (),
        None,
    )
    record.generation = 7
    record.phase = "draining"
    record.blockers = ["running_detail_claims"]

    rendered = json.loads(host_maintenance.CliJsonFormatter().format(record))

    assert rendered == {
        "level": "INFO",
        "message": "drain progress",
        "generation": 7,
        "phase": "draining",
        "blockers": ["running_detail_claims"],
    }


def test_parser_intentionally_exposes_no_complete_command():
    parser = host_maintenance.build_parser()
    choices = next(action.choices for action in parser._actions if action.dest == "command")
    assert set(choices) == {
        "plan",
        "preflight",
        "prepare-update",
        "request",
        "status",
        "begin-drain",
        "drain",
        "drain-status",
        "authorize",
        "wait-active",
        "stop",
        "update",
        "reboot",
        "start",
        "begin-validation",
    }


def test_dry_run_plan_has_canonical_order_and_never_completes(mocker, tmp_path):
    run_command = mocker.patch.object(host_maintenance, "_run_command")

    result = host_maintenance.run(_args(tmp_path, "plan", manifest=None))

    assert result == {
        "phase": "dry-run",
        "commands": [
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
        ],
        "complete_implicit": False,
    }
    assert "complete" not in result["commands"]
    run_command.assert_not_called()


def test_preflight_command_contract_is_observation_only():
    mutation_words = {
        "down",
        "install",
        "reboot",
        "restart",
        "stop",
        "unmask",
        "update",
        "upgrade",
    }

    for _, command, _ in host_maintenance.PREFLIGHT_COMMANDS:
        assert mutation_words.isdisjoint(command)


def test_prepare_package_plan_pins_downloads_and_classifies_boundaries(mocker, tmp_path):
    ordinary = "Inst openssl [1.0] (1.1 jammy-updates [amd64])\n"
    held = "\n".join(
        [
            "Inst docker.io [28.0] (29.1 jammy-security [amd64])",
            "Inst containerd [1.6] (1.7 jammy-security [amd64])",
        ]
    )
    exact = ordinary + held + "\n"
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        stdout = ""
        if command == ("sudo", "apt-get", "update"):
            stdout = "indexes refreshed"
        elif command == ("apt-mark", "showhold"):
            stdout = "docker.io\n"
        elif command == ("apt-get", "--simulate", "upgrade"):
            stdout = ordinary
        elif command[-2:] == ("install", "docker.io"):
            stdout = held
        elif command[:2] == ("apt-get", "--simulate"):
            stdout = exact
        return {"stdout": stdout, "stderr": "", "returncode": 0}

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)
    mocker.patch.object(host_maintenance, "_utc_now", return_value="timestamp")

    result = host_maintenance.prepare_package_plan(
        tmp_path,
        included_holds=["docker.io"],
    )

    plan_path = tmp_path / "package-plan.json"
    plan = json.loads(plan_path.read_text())
    assert (
        result["package_plan_sha256"]
        == host_maintenance.hashlib.sha256(plan_path.read_bytes()).hexdigest()
    )
    assert [package["name"] for package in plan["packages"]] == [
        "containerd",
        "docker.io",
        "openssl",
    ]
    assert plan["compatibility_boundaries"] == ["container_runtime"]
    assert "docker.io=29.1" in plan["apply_command"]
    assert any("--download-only" in command for command in calls)


def test_prepare_package_plan_requires_exact_held_package_review(mocker, tmp_path):
    def fake_run(command, **kwargs):
        stdout = "docker.io\n" if command == ("apt-mark", "showhold") else ""
        return {"stdout": stdout, "stderr": "", "returncode": 0}

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)

    with pytest.raises(host_maintenance.MaintenanceError, match="every held package"):
        host_maintenance.prepare_package_plan(tmp_path, included_holds=[])


def _write_package_plan(tmp_path):
    plan_path = tmp_path / "package-plan.json"
    plan = {
        "schema_version": 1,
        "packages": [
            {
                "name": "docker.io",
                "version": "29.1",
                "boundaries": ["container_runtime"],
                "held": True,
            }
        ],
        "compatibility_boundaries": ["container_runtime"],
        "holds": ["docker.io"],
        "apply_command": [
            "sudo",
            "apt-get",
            "--yes",
            "--allow-change-held-packages",
            "--no-remove",
            "install",
            "docker.io=29.1",
        ],
    }
    plan_path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    return plan_path, host_maintenance.hashlib.sha256(plan_path.read_bytes()).hexdigest()


def test_apply_package_plan_requires_digest_reviews_masks_and_audits(mocker, tmp_path):
    plan_path, digest = _write_package_plan(tmp_path)
    args = _args(
        tmp_path,
        "update",
        package_plan=plan_path,
        confirm_plan=digest,
        confirm_apply=True,
        release_notes_reviewed=True,
        compatibility_reviewed=True,
    )
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "stopped", "manifest_location": args.manifest},
    )
    mocker.patch.object(host_maintenance, "_utc_now", return_value="timestamp")
    mocker.patch.object(host_maintenance, "_running_kernel", return_value="6.8.0-test")
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        stdout = ""
        returncode = 0
        if command[:2] == ("systemctl", "is-enabled"):
            stdout = "enabled\n"
        elif command[:2] == ("dpkg-query", "--show"):
            stdout = "docker.io\t29.1\n"
        elif command == ("apt-mark", "showhold"):
            stdout = "docker.io\n"
        elif command[:2] == ("sudo", "fuser"):
            returncode = 1
        return {"stdout": stdout, "stderr": "", "returncode": returncode}

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)

    result = host_maintenance.run(args)

    assert result["phase"] == "updated"
    assert result["package_plan_sha256"] == digest
    mask = ("sudo", "systemctl", "mask", "--now", *host_maintenance.APT_CONTROL_UNITS)
    assert mask in calls
    assert tuple(json.loads(plan_path.read_text())["apply_command"]) in calls
    assert ("sudo", "dpkg", "--audit") in calls
    assert calls[-1] == ("sync",)
    evidence = json.loads((tmp_path / "update-result.json").read_text())
    assert evidence["apt_automation_masked"] is True
    assert set(evidence["apt_unit_states_before"].values()) == {"enabled"}
    assert evidence["installed_versions"] == {"docker.io": "29.1"}
    assert evidence["holds_after"] == ["docker.io"]
    checkpoint.assert_called_once_with(args.checkpoint, "updated", args.manifest)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"confirm_apply": False}, "confirm-apply"),
        ({"release_notes_reviewed": False}, "release-notes-reviewed"),
        ({"compatibility_reviewed": False}, "compatibility-reviewed"),
        ({"confirm_plan": "0" * 64}, "SHA-256"),
    ],
)
def test_apply_package_plan_refuses_missing_authority(mocker, tmp_path, overrides, message):
    plan_path, digest = _write_package_plan(tmp_path)
    values = {
        "package_plan": plan_path,
        "confirm_plan": digest,
        "confirm_apply": True,
        "release_notes_reviewed": True,
        "compatibility_reviewed": True,
    }
    values.update(overrides)
    args = _args(tmp_path, "update", **values)
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "stopped", "manifest_location": args.manifest},
    )
    run_command = mocker.patch.object(host_maintenance, "_run_command")

    with pytest.raises(host_maintenance.MaintenanceError, match=message):
        host_maintenance.run(args)

    run_command.assert_not_called()


def test_apply_package_plan_refuses_live_package_manager_lock(mocker, tmp_path):
    plan_path, digest = _write_package_plan(tmp_path)
    args = _args(
        tmp_path,
        "update",
        package_plan=plan_path,
        confirm_plan=digest,
        confirm_apply=True,
        release_notes_reviewed=True,
        compatibility_reviewed=True,
    )
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "stopped", "manifest_location": args.manifest},
    )
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return {"stdout": "apt-owner", "stderr": "", "returncode": 0}

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)

    with pytest.raises(host_maintenance.MaintenanceError, match="lock is held"):
        host_maintenance.run(args)

    assert commands == [
        (
            "sudo",
            "fuser",
            "/var/lib/dpkg/lock-frontend",
            "/var/lib/apt/lists/lock",
            "/var/cache/apt/archives/lock",
        )
    ]


def _reboot_args(tmp_path, *, confirm_reboot):
    manifest_path = tmp_path / "running-set.json"
    manifest_path.write_text(json.dumps(_round_trip_manifest()), encoding="utf-8")
    return _args(
        tmp_path,
        "reboot",
        manifest=str(manifest_path),
        confirm_reboot=confirm_reboot,
    )


def test_reboot_requires_confirmation_and_checkpoints_before_command(mocker, tmp_path):
    args = _reboot_args(tmp_path, confirm_reboot=True)
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "updated", "manifest_location": args.manifest},
    )
    mocker.patch.object(host_maintenance, "_boot_id", return_value="boot-before")
    mocker.patch.object(host_maintenance, "_running_kernel", return_value="6.8.0-before")
    events = []
    mocker.patch.object(
        host_maintenance,
        "append_checkpoint",
        side_effect=lambda *call_args: events.append(("checkpoint", call_args[1])),
    )
    mocker.patch.object(
        host_maintenance,
        "_run_command",
        side_effect=lambda command, **kwargs: (
            events.append(("command", command)) or {"stdout": "", "stderr": "", "returncode": 0}
        ),
    )

    result = host_maintenance.run(args)

    assert result["phase"] == "rebooting"
    assert events == [
        ("command", ("sync",)),
        ("checkpoint", "rebooting"),
        ("command", ("sudo", "systemctl", "reboot")),
    ]


def test_reboot_replay_proves_changed_boot_before_checkpointing_rebooted(mocker, tmp_path):
    args = _reboot_args(tmp_path, confirm_reboot=False)
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "rebooting", "manifest_location": args.manifest},
    )
    mocker.patch.object(host_maintenance, "_boot_id", return_value="boot-after")
    mocker.patch.object(host_maintenance, "_running_kernel", return_value="6.8.0-after")
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")
    run_command = mocker.patch.object(host_maintenance, "_run_command")

    assert host_maintenance.run(args) == {
        "phase": "rebooted",
        "boot_id_changed": True,
        "running_kernel": "6.8.0-after",
    }

    checkpoint.assert_called_once_with(args.checkpoint, "rebooted", args.manifest)
    run_command.assert_not_called()


def test_reboot_without_confirmation_never_mutates_host(mocker, tmp_path):
    args = _reboot_args(tmp_path, confirm_reboot=False)
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "updated", "manifest_location": args.manifest},
    )
    mocker.patch.object(host_maintenance, "_boot_id", return_value="boot-before")
    run_command = mocker.patch.object(host_maintenance, "_run_command")

    with pytest.raises(host_maintenance.MaintenanceError, match="confirm-reboot"):
        host_maintenance.run(args)

    run_command.assert_not_called()


def test_capture_running_set_records_compose_and_container_evidence(mocker):
    container = {
        "Id": "container-1",
        "Name": "/cartracker-trawl",
        "Image": "sha256:image-1",
        "Config": {
            "Image": "cartracker-trawl:latest",
            "Labels": {
                "com.docker.compose.project": "cartracker",
                "com.docker.compose.service": "trawl",
                "com.docker.compose.project.working_dir": "/opt/cartracker",
                "com.docker.compose.project.config_files": ("/opt/cartracker/docker-compose.yml"),
            },
        },
        "State": {
            "Status": "running",
            "Running": True,
            "ExitCode": 0,
            "Health": {"Status": "healthy"},
        },
        "HostConfig": {
            "RestartPolicy": {"Name": "unless-stopped"},
            "LogConfig": {
                "Type": "json-file",
                "Config": {"max-size": "10m", "secret-token": "do-not-store"},
            },
        },
    }

    def fake_run(command, **kwargs):
        if command == ("docker", "ps", "--all", "--quiet"):
            return {"stdout": "container-1\n", "stderr": "", "returncode": 0}
        if command[:2] == ("docker", "inspect"):
            return {"stdout": json.dumps([container]), "stderr": "", "returncode": 0}
        if command[:3] == ("docker", "image", "inspect"):
            return {
                "stdout": json.dumps(
                    [
                        {
                            "Id": "sha256:image-1",
                            "RepoDigests": ["cartracker-trawl@sha256:digest-1"],
                        }
                    ]
                ),
                "stderr": "",
                "returncode": 0,
            }
        if command[:2] == ("docker", "compose"):
            project = command[command.index("--project-name") + 1]
            profiles = ["trawl"] if project == "cartracker" else []
            service = "trawl" if project == "cartracker" else "placeholder"
            return {
                "stdout": json.dumps({"services": {service: {"profiles": profiles}}}),
                "stderr": "",
                "returncode": 0,
            }
        raise AssertionError(command)

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)
    mocker.patch.object(host_maintenance, "_git_revision", return_value="abc123")
    mocker.patch.object(host_maintenance, "_running_kernel", return_value="6.8.0-test")
    mocker.patch.object(host_maintenance, "_boot_id", return_value="boot-before")
    mocker.patch.object(host_maintenance, "_utc_now", return_value="timestamp")

    manifest, rendered = host_maintenance.capture_running_set()

    assert set(rendered) == {
        "cartracker",
        "cartracker-lakehouse",
        "cartracker-mlflow",
    }
    assert manifest["git_revision"] == "abc123"
    assert manifest["boot_id"] == "boot-before"
    assert manifest["running_kernel"] == "6.8.0-test"
    row = manifest["containers"][0]
    assert row["name"] == "cartracker-trawl"
    assert row["declared_profiles"] == ["trawl"]
    assert row["policy_class"] == "profile-running"
    assert row["image_repo_digests"] == ["cartracker-trawl@sha256:digest-1"]
    assert row["state"] == {
        "status": "running",
        "running": True,
        "exit_code": 0,
        "health": "healthy",
    }
    assert row["restart_policy"] == {"Name": "unless-stopped"}
    assert row["log_config"] == {
        "driver": "json-file",
        "options": {"max-size": "10m"},
    }
    assert "do-not-store" not in json.dumps(manifest)


def test_capture_running_set_refuses_an_empty_host(mocker):
    mocker.patch.object(
        host_maintenance,
        "_run_command",
        return_value={"stdout": "", "stderr": "", "returncode": 0},
    )

    with pytest.raises(host_maintenance.MaintenanceError, match="no containers"):
        host_maintenance.capture_running_set()


def _round_trip_manifest():
    return {
        "schema_version": 1,
        "boot_id": "boot-before",
        "compose_projects": {
            "cartracker": {
                "working_directory": "/opt/cartracker",
                "config_files": ["/opt/cartracker/docker-compose.yml"],
            },
            "cartracker-mlflow": {
                "working_directory": "/opt/cartracker",
                "config_files": ["/opt/cartracker/docker-compose.mlflow.yml"],
            },
        },
        "containers": [
            {
                "project": "cartracker",
                "service": "ops",
                "container_id": "container-ops",
                "declared_profiles": [],
                "policy_class": None,
                "state": {"running": True},
            },
            {
                "project": "cartracker",
                "service": "trawl",
                "container_id": "container-trawl",
                "declared_profiles": ["trawl"],
                "policy_class": "profile-running",
                "state": {"running": True},
            },
            {
                "project": "cartracker",
                "service": "flyway",
                "container_id": "container-flyway",
                "declared_profiles": [],
                "policy_class": "oneshot",
                "state": {"running": False},
            },
            {
                "project": "cartracker",
                "service": "dbt",
                "container_id": "container-dbt",
                "declared_profiles": ["tools"],
                "policy_class": "on-demand",
                "state": {"running": False},
            },
            {
                "project": "cartracker-mlflow",
                "service": "mlflow",
                "container_id": "container-mlflow",
                "declared_profiles": [],
                "policy_class": "aux-paused",
                "state": {"running": False},
            },
        ],
    }


def test_running_set_plan_round_trips_default_profile_and_stopped_services():
    plan = host_maintenance.build_running_set_plan(_round_trip_manifest())

    assert len(plan) == 1
    assert plan[0]["project"] == "cartracker"
    assert plan[0]["services"] == ["ops", "trawl"]
    assert plan[0]["profiles"] == ["trawl"]
    assert plan[0]["stop_command"] == [
        "docker",
        "compose",
        "--project-name",
        "cartracker",
        "--file",
        "/opt/cartracker/docker-compose.yml",
        "--profile",
        "trawl",
        "stop",
        "ops",
        "trawl",
    ]
    assert plan[0]["start_command"][-3:] == ["start", "ops", "trawl"]
    assert "flyway" not in plan[0]["services"]
    assert "dbt" not in plan[0]["services"]
    assert all(item["project"] != "cartracker-mlflow" for item in plan)


@pytest.mark.parametrize("policy_class", sorted(host_maintenance.NON_RESTORABLE_POLICY_CLASSES))
def test_running_set_plan_refuses_non_restorable_service_marked_running(policy_class):
    manifest = _round_trip_manifest()
    container = next(
        (row for row in manifest["containers"] if row["policy_class"] == policy_class),
        None,
    )
    if container is None:
        container = {
            "project": "cartracker",
            "service": "foreign-test-service",
            "declared_profiles": [],
            "policy_class": policy_class,
            "state": {"running": True},
        }
        manifest["containers"].append(container)
    else:
        container["state"]["running"] = True

    with pytest.raises(host_maintenance.MaintenanceError, match="despite policy"):
        host_maintenance.build_running_set_plan(manifest)


def test_load_running_set_manifest_rejects_unknown_schema(tmp_path):
    manifest = tmp_path / "running-set.json"
    manifest.write_text(json.dumps({"schema_version": 2}), encoding="utf-8")
    with pytest.raises(host_maintenance.MaintenanceError, match="unsupported schema"):
        host_maintenance.load_running_set_manifest(manifest)


def test_load_running_set_manifest_rejects_symlink(mocker, tmp_path):
    manifest = tmp_path / "running-set.json"
    mocker.patch.object(Path, "is_symlink", autospec=True, return_value=True)

    with pytest.raises(host_maintenance.MaintenanceError, match="symlink"):
        host_maintenance.load_running_set_manifest(manifest)


def test_latest_checkpoint_recovers_from_torn_trailing_append(tmp_path):
    checkpoint = tmp_path / "history.jsonl"
    checkpoint.write_text(
        json.dumps({"phase": "active", "manifest_location": "/tmp/manifest"})
        + "\n"
        + '{"phase":"stopp',
        encoding="utf-8",
    )

    assert host_maintenance.latest_checkpoint(checkpoint)["phase"] == "active"


@pytest.mark.parametrize(
    ("action", "prior_phase", "expected_running", "result_phase"),
    [
        ("stop", "active", False, "stopped"),
        ("start", "stopped", True, "started"),
    ],
)
def test_running_set_action_is_manifest_scoped_and_verified(
    mocker, tmp_path, action, prior_phase, expected_running, result_phase
):
    manifest_path = tmp_path / "running-set.json"
    manifest_path.write_text(json.dumps(_round_trip_manifest()), encoding="utf-8")
    args = _args(tmp_path, action, manifest=str(manifest_path))
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": prior_phase, "manifest_location": str(manifest_path)},
    )
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        if command[:2] == ("docker", "inspect"):
            return {
                "stdout": json.dumps(
                    [
                        {"Id": "container-ops", "State": {"Running": expected_running}},
                        {
                            "Id": "container-trawl",
                            "State": {"Running": expected_running},
                        },
                    ]
                ),
                "stderr": "",
                "returncode": 0,
            }
        return {"stdout": "", "stderr": "", "returncode": 0}

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)

    result = host_maintenance.run(args)

    assert result == {
        "phase": result_phase,
        "projects": ["cartracker"],
        "services": ["ops", "trawl"],
    }
    compose = calls[0]
    assert compose[0][-3:] == (action, "ops", "trawl")
    assert compose[1]["cwd"] == Path("/opt/cartracker")
    assert calls[1][0] == ("docker", "inspect", "container-ops", "container-trawl")
    checkpoint.assert_called_once_with(args.checkpoint, result_phase, str(manifest_path))


def test_running_set_action_refuses_wrong_offline_phase(mocker, tmp_path):
    args = _args(tmp_path, "stop")
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "draining", "manifest_location": args.manifest},
    )
    run_command = mocker.patch.object(host_maintenance, "_run_command")

    with pytest.raises(host_maintenance.MaintenanceError, match="cannot stop"):
        host_maintenance.run(args)

    run_command.assert_not_called()


def test_running_set_action_refuses_failed_postcondition(mocker, tmp_path):
    manifest_path = tmp_path / "running-set.json"
    manifest_path.write_text(json.dumps(_round_trip_manifest()), encoding="utf-8")
    args = _args(tmp_path, "stop", manifest=str(manifest_path))
    mocker.patch.object(
        host_maintenance,
        "latest_checkpoint",
        return_value={"phase": "active", "manifest_location": str(manifest_path)},
    )

    def fake_run(command, **kwargs):
        if command[:2] == ("docker", "inspect"):
            return {
                "stdout": json.dumps(
                    [
                        {"Id": "container-ops", "State": {"Running": True}},
                        {"Id": "container-trawl", "State": {"Running": False}},
                    ]
                )
            }
        return {"stdout": "", "stderr": "", "returncode": 0}

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")

    with pytest.raises(host_maintenance.MaintenanceError, match="verification"):
        host_maintenance.run(args)

    checkpoint.assert_not_called()


def test_collect_preflight_enforces_lock_audit_and_hold_contract(mocker):
    def fake_run(command, **kwargs):
        stdout = "docker.io\n" if command == ("apt-mark", "showhold") else ""
        returncode = 1 if command[:2] == ("sudo", "fuser") else 0
        return {
            "command": list(command),
            "returncode": returncode,
            "stdout": stdout,
            "stderr": "",
        }

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)
    mocker.patch.object(host_maintenance, "_utc_now", return_value="timestamp")

    result = host_maintenance.collect_preflight()

    assert result["console_access_verified"] is True
    assert set(result["observations"]) == {
        name for name, _, _ in host_maintenance.PREFLIGHT_COMMANDS
    }


@pytest.mark.parametrize(
    ("lock_returncode", "audit", "holds", "message"),
    [
        (0, "", "docker.io\n", "lock is held"),
        (1, "broken package\n", "docker.io\n", "inconsistent"),
        (1, "", "", "holds differ"),
        (1, "", "docker.io\nlinux-image\n", "holds differ"),
    ],
)
def test_collect_preflight_fails_closed_on_package_findings(
    mocker, lock_returncode, audit, holds, message
):
    def fake_run(command, **kwargs):
        stdout = ""
        returncode = 0
        if command[:2] == ("sudo", "fuser"):
            returncode = lock_returncode
        elif command == ("sudo", "dpkg", "--audit"):
            stdout = audit
        elif command == ("apt-mark", "showhold"):
            stdout = holds
        return {
            "command": list(command),
            "returncode": returncode,
            "stdout": stdout,
            "stderr": "",
        }

    mocker.patch.object(host_maintenance, "_run_command", side_effect=fake_run)

    with pytest.raises(host_maintenance.MaintenanceError, match=message):
        host_maintenance.collect_preflight()


def test_run_preflight_writes_manifest_config_and_observations(mocker, tmp_path):
    manifest = {"containers": [{"name": "ops"}]}
    rendered = {
        "cartracker": {
            "working_directory": "/opt/cartracker",
            "config_files": ["/opt/cartracker/docker-compose.yml"],
            "rendered_sha256": "digest",
            "services": {"ops": {"profiles": [], "image": "ops:latest"}},
        }
    }
    mocker.patch.object(host_maintenance, "capture_running_set", return_value=(manifest, rendered))
    mocker.patch.object(
        host_maintenance,
        "collect_preflight",
        return_value={"observations": {"git_revision": {"stdout": "abc"}}},
    )

    with pytest.raises(host_maintenance.MaintenanceError, match="console"):
        host_maintenance.run_preflight(tmp_path, console_access_verified=False)

    result = host_maintenance.run_preflight(tmp_path, console_access_verified=True)

    assert result == {
        "phase": "preflight",
        "manifest_location": str(tmp_path / "running-set.json"),
        "containers": 1,
        "compose_projects": ["cartracker"],
    }
    assert json.loads((tmp_path / "running-set.json").read_text()) == manifest
    assert json.loads((tmp_path / "preflight.json").read_text())["observations"]
    compose = json.loads((tmp_path / "compose" / "cartracker.json").read_text())
    assert compose["services"] == {"ops": {"profiles": [], "image": "ops:latest"}}
    assert "environment" not in json.dumps(compose)


def test_preflight_command_checkpoints_only_after_evidence_is_written(mocker, tmp_path):
    manifest = tmp_path / "running-set.json"
    run_preflight = mocker.patch.object(
        host_maintenance,
        "run_preflight",
        return_value={"phase": "preflight", "manifest_location": str(manifest)},
    )
    checkpoint = mocker.patch.object(host_maintenance, "append_checkpoint")
    args = _args(
        tmp_path,
        "preflight",
        manifest=None,
        output_dir=tmp_path,
        console_access_verified=True,
    )

    result = host_maintenance.run(args)

    assert result["phase"] == "preflight"
    run_preflight.assert_called_once_with(tmp_path, console_access_verified=True)
    checkpoint.assert_called_once_with(args.checkpoint, "preflight", str(manifest))
