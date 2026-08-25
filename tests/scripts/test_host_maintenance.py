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


def test_parser_intentionally_exposes_no_complete_or_destructive_commands():
    parser = host_maintenance.build_parser()
    choices = next(action.choices for action in parser._actions if action.dest == "command")
    assert set(choices) == {
        "preflight",
        "request",
        "status",
        "begin-drain",
        "drain-status",
        "authorize",
        "wait-active",
        "begin-validation",
    }


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
    mocker.patch.object(host_maintenance, "_utc_now", return_value="timestamp")

    manifest, rendered = host_maintenance.capture_running_set()

    assert set(rendered) == {
        "cartracker",
        "cartracker-lakehouse",
        "cartracker-mlflow",
    }
    assert manifest["git_revision"] == "abc123"
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
