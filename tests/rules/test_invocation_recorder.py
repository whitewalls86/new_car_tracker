"""The invocation recorder loads everywhere, and its gate reads every job.

Plan 162 Stage R. The plugin and the gate are two halves of one instrument and
each fails silently on its own: a recorder nobody registered writes nothing and
looks exactly like a run in which every step selected everything, and a gate
that waits on five of six jobs reports a whole-repository reading from a
fraction of the record. That second failure is not hypothetical -- it is how
the Stage X gate landed, and ``test_every_job_that_runs_pytest_has_its_record_
read_by_the_gate`` exists because nothing noticed.

So the wiring is asserted here in the same derived shape: the owing set is
*jobs that upload a record*, never a list, so a job added next year is covered
by the derivation rather than by anyone remembering this file exists.
"""
from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
import yaml

from tests.plugins import invocation_recorder

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"
INVOCATION_GATE_SCRIPT = "scripts/check_test_invocation_coverage.py"
RECORDER_MODULE = "tests.plugins.invocation_recorder"
_RECORD_ARTIFACT = "ci-run-records-"


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _invocation_wiring() -> tuple[set[str], dict]:
    """``(jobs uploading a record, the invocation gate job)``."""
    uploading: set[str] = set()
    gate: dict = {}
    for key, job in _workflow()["jobs"].items():
        for step in job.get("steps", []) or []:
            run = str(step.get("run", ""))
            if INVOCATION_GATE_SCRIPT in run:
                gate = job | {"__key__": key, "__run__": run}
            with_ = step.get("with", {}) or {}
            if "upload-artifact" in str(step.get("uses", "")):
                if str(with_.get("name", "")).startswith(_RECORD_ARTIFACT):
                    uploading.add(key)
    return uploading, gate


def test_the_invocation_recorder_is_registered_for_every_pytest_run():
    """Through ``addopts``, for the reason Stage U's registration gives.

    ``docs-tests`` runs ``pytest --noconftest`` -- it installs three packages
    and cannot import ``tests/conftest.py`` -- so a conftest registration would
    leave the one job that most needs an invocation record producing none.
    ``-p`` loads through ``--noconftest``; ``pythonpath = ["."]`` makes the
    module importable at plugin-registration time.
    """
    addopts = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    registered = addopts["tool"]["pytest"]["ini_options"]["addopts"]
    assert f"-p {RECORDER_MODULE}" in registered, (
        f"{RECORDER_MODULE} is no longer registered in addopts, so no pytest "
        f"process records what it selected and "
        f"{INVOCATION_GATE_SCRIPT} measures nothing."
    )


def test_every_job_that_uploads_a_record_is_read_by_the_invocation_gate():
    """The gate waits on every job whose record it claims to have read.

    Derived rather than listed: a job that uploads a record owes the gate a
    ``needs``, and the gate reporting a reading without it is the Stage X
    failure repeated -- a number for the whole repository taken from part of
    the record, with nothing able to notice.
    """
    uploading, gate = _invocation_wiring()
    assert gate, f"{WORKFLOW} no longer runs {INVOCATION_GATE_SCRIPT} at all."

    unread = sorted(uploading - set(gate.get("needs", [])))
    assert not unread, (
        f"{gate['__key__']} does not wait on {unread}, which upload invocation "
        f"records. Their steps would be absent from the reading, and a step "
        f"that selected nothing is indistinguishable from a step that did not "
        f"report."
    )


def test_the_invocation_gate_is_not_merely_reporting():
    """The ratchet, and it is the one this stage argued about out loud.

    The Stage X gate landed with ``--report`` because it landed on master,
    where a red gate blocks work unrelated to it. This one landed red on a
    draft PR on purpose -- a green run with a documented hole is worth less
    than nothing when nothing consumes the gate yet. Putting a reporting flag
    back now costs a diff that touches this docstring.
    """
    _, gate = _invocation_wiring()
    assert gate, f"{WORKFLOW} no longer runs {INVOCATION_GATE_SCRIPT} at all."
    assert "--report" not in gate["__run__"], (
        f"{gate['__key__']} runs {INVOCATION_GATE_SCRIPT} in a reporting mode. "
        f"A gate that cannot fail is a decoration."
    )


@pytest.fixture()
def recorded(mocker, monkeypatch, tmp_path):
    """Drive the hooks and hand back what the plugin wrote.

    ``_selected`` and ``_deselected`` are *replaced* rather than cleared, for
    the reason Stage U's equivalent fixture gives: this file runs inside the
    session the plugin is watching, and clearing the real sets would leave the
    run it is part of with no record of itself.
    """
    monkeypatch.setenv("CI_RUN_RECORDS", str(tmp_path))
    monkeypatch.setenv("GITHUB_JOB", "some-job")

    def run(selected: tuple[str, ...], deselected: tuple[str, ...]) -> dict:
        mocker.patch.object(invocation_recorder, "_selected", set(selected))
        mocker.patch.object(invocation_recorder, "_deselected", set(deselected))
        invocation_recorder.pytest_unconfigure(None)
        written = sorted(tmp_path.glob("tests-*.json"))
        assert len(written) == 1, f"expected one record, got {written}"
        return json.loads(written[0].read_text(encoding="utf-8"))

    return run


def test_a_record_carries_what_was_selected_and_what_was_dropped(recorded):
    payload = recorded(("tests/a/test_one.py",), ("tests/a/test_two.py",))

    assert payload["selected"] == ["tests/a/test_one.py"]
    assert payload["deselected"] == ["tests/a/test_two.py"], (
        "the deselected half is what lets the gate say 'collected and "
        "deselected entirely' rather than 'absent', and those are different "
        "repairs -- a missing marker against a missing step"
    )
    assert payload["job"] == "some-job"


def test_nothing_is_written_when_no_directory_is_named(mocker, monkeypatch, tmp_path):
    """The mutation harness runs 176 pytest subprocesses and must stay out.

    Each is one node deep against a deliberately mutated tree. Recorded, every
    one would look like an invocation that ran almost nothing, and the gate
    would read a repository that does not exist.

    **``chdir`` is the assertion, not the setup.** The first version of this
    test asserted that ``tmp_path`` stayed empty with the variable unset, and
    the mutation harness reported it unnoticed: a recorder that falls back to a
    *relative* default writes beside the working directory and leaves
    ``tmp_path`` untouched, so the test passed while the plugin wrote a record.
    Moving the working directory into ``tmp_path`` first makes "nothing was
    written" a claim about anywhere a relative path could land, which is what
    the docstring above was always claiming.
    """
    monkeypatch.delenv("CI_RUN_RECORDS", raising=False)
    monkeypatch.chdir(tmp_path)
    mocker.patch.object(invocation_recorder, "_selected", {"tests/a/test_one.py"})

    invocation_recorder.pytest_unconfigure(None)

    assert not list(tmp_path.iterdir())
