"""Plan 142 Stage 2 operator client and offline checkpoint contract."""

import json
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


def test_request_is_always_host_scoped_and_checkpoints_after_confirmation(
    mocker, tmp_path
):
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
def test_mutating_commands_checkpoint_only_confirmed_phase(
    mocker, tmp_path, command, route, phase
):
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


def test_parser_intentionally_exposes_no_complete_or_destructive_commands():
    parser = host_maintenance.build_parser()
    choices = next(
        action.choices for action in parser._actions if action.dest == "command"
    )
    assert set(choices) == {
        "request",
        "status",
        "begin-drain",
        "drain-status",
        "authorize",
        "begin-validation",
    }
