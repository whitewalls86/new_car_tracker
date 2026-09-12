"""Which test files a pytest invocation actually selected, recorded at the run.

Plan 162 Stage R. The rule this exists to repair is
``test_every_integration_suite_is_invoked_by_a_ci_step``, which reads
``ci.yml`` as text and asks whether a *directory string* appears in a ``run:``
line. A directory is not a suite, and that question cannot tell apart three
things `Stage F
<../../docs/evidence/plan_162_stage_F_evidence.md>`_ had to separate by hand:
"this file runs in CI", "the directory containing this file is named in a
``run:`` line", and "this file sits in ``tests/integration/`` and needs nothing
that makes it one".

**Observation, not simulation, and that is the whole design.** The alternative
scoped for this stage was static: AST-parse ``pytestmark``, evaluate ``-m``
expressions, resolve ``--ignore``, and so reimplement pytest's own selection.
That reimplementation is then a second thing that can be wrong, with nothing
checking it against the first -- the same defect `Stage Q
<../../docs/plans/plan_162_testing_census_and_restructure.md#stage-q-cis-services-are-productions-in-definition-and-in-contents>`_
removed from the ``services:`` blocks by resolving Compose instead of
transcribing it. So this asks pytest what it did.

**The hook is Stage U's, deliberately.** ``tests/plugins/declared_skips.py``
established that ``pytest_collection_finish`` reads ``session.items`` *after*
deselection has happened, which is what makes it the selected set rather than
the collected one -- "a deselected test did not decline to skip; it was never
asked to". This records the same fact one level up, at file grain, and adds the
other half from ``pytest_deselected`` so a record can say *collected and
dropped* rather than merely *absent*.

**It stops at selection and does not look at outcomes**, because Stage U
already owns what happens after. A file whose tests were selected and then all
skipped is a declared-skip question, and answering it here too would be two
mechanisms for one fact -- which is the thing this plan keeps deleting. The
predicate here is: did this invocation ask this file for tests.

**Writes only when** :data:`_ARTIFACT_ENV` **names a directory**, which is what
keeps the mutation harness out of the record. That harness runs 176 pytest
subprocesses against a deliberately mutated tree, each one node deep; recorded,
every one of them would look like an invocation that ran almost nothing.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Shared with ``tests/plugins/sql_execution_recorder.py`` on purpose. One
#: variable names *where run records go*; the filename prefix says what kind
#: each record is, and each gate globs only its own. Two variables would be two
#: things for a job written next year to inherit and one to forget, and the
#: rule that keeps this set at workflow level derives its owing set from "jobs
#: that run pytest" -- which does not change when the number of recorders does.
_ARTIFACT_ENV = "CI_RUN_RECORDS"
_FILENAME_PREFIX = "tests"

_selected: set[str] = set()
_deselected: set[str] = set()


def _relative(item) -> str | None:
    """*item*'s file, repo-relative and posix, or ``None`` if it is outside.

    ``None`` rather than an absolute path: a record is read by a gate that
    compares against repository paths, and an entry it cannot match is worse
    than an entry that is not there -- it reads as a file that ran and cannot
    be found.
    """
    path = getattr(item, "path", None)
    if path is None:
        return None
    try:
        return Path(path).resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return None


def pytest_collection_finish(session):
    """What this invocation selected, after every deselection has happened."""
    for item in session.items:
        relative = _relative(item)
        if relative is not None:
            _selected.add(relative)


def pytest_deselected(items):
    """What it collected and dropped.

    Accumulated rather than assigned: pytest calls this once per deselecting
    mechanism, so the last caller would otherwise be the whole record.
    """
    for item in items:
        relative = _relative(item)
        if relative is not None:
            _deselected.add(relative)


def pytest_unconfigure(config) -> None:  # noqa: ARG001 - pytest hook signature
    """Write this invocation's slice of the record.

    Per *invocation* and not per job, for the reason the SQL recorder gives one
    file over: ``service-integration`` runs pytest five times and
    ``schema-contracts`` twice, and the question this record answers -- did
    *this* step's arguments actually reach these files -- is meaningless once
    five steps are unioned together.

    ``argv`` is recorded raw and parsed by the gate rather than here. The
    plugin's job is the observation; deciding which arguments name a suite is
    the rule, and keeping them apart means the rule can be corrected without a
    re-run to regenerate records taken under the old reading.
    """
    destination = os.environ.get(_ARTIFACT_ENV)
    if not destination:
        return
    payload = {
        "job": os.environ.get("GITHUB_JOB", ""),
        "argv": sys.argv[1:],
        "selected": sorted(_selected),
        "deselected": sorted(_deselected),
    }
    path = Path(destination)
    if path.suffix == ".json":
        # The single-file form belongs to the SQL recorder, whose Stage X
        # baseline recipe uses it. Two recorders cannot share one filename, so
        # this one writes beside it rather than over it.
        path = path.parent
    path.mkdir(parents=True, exist_ok=True)
    name = f"{_FILENAME_PREFIX}-{os.getpid()}-{len(_selected)}.json"
    (path / name).write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
