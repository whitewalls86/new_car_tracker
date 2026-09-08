"""Plan 162 Stage U / CAR-81: the declared-skips hook, exercised directly.

The rule this mechanism enforces is *a run that succeeds has done the work its
success implies*, so a hook that quietly stopped noticing would be the defect
it exists to prevent, wearing its own uniform. These drive
``pytest_terminal_summary`` against stub reports rather than through a nested
pytest session: the hook's whole contract is a set of reports in, an exit
status and some lines out, and a real subprocess run would test pytest's
plumbing instead.

The end-to-end direction is not asserted here at all. It is demonstrated --
Stage U's exit requires an undeclared skip failing a real CI run rather than a
test claiming it would.
"""
from __future__ import annotations

from datetime import date

import pytest

from tests.plugins import declared_skips
from tests.plugins.declared_skips import GATE, DeclaredSkip

DECLARED = DeclaredSkip(
    "tests/test_thing.py::test_declared",
    reason="the fixture it needs is deliberately absent in this job",
    condition="no widget here",
    since=date(2026, 9, 4),
)


class StubReport:
    """One skipped test as pytest reports it.

    ``longrepr`` is the ``(path, lineno, text)`` triple a skip carries, and the
    text is prefixed ``Skipped: `` by pytest itself -- reproduced here because
    the hook substring-matches against it and a stub that dropped the prefix
    would be testing a string this code never sees.
    """

    def __init__(self, nodeid: str, reason: str):
        self.nodeid = nodeid
        self.longrepr = ("tests/test_thing.py", 12, f"Skipped: {reason}")


class StubSession:
    exitstatus = 0


class StubReporter:
    """Enough ``TerminalReporter`` for the hook: stats, a section, and lines."""

    def __init__(self, skipped: list[StubReport]):
        self.stats = {"skipped": skipped, "passed": []}
        self._session = StubSession()
        self.lines: list[str] = []
        self.sections: list[tuple[str, bool]] = []

    def section(self, title, red=False, **_kwargs):
        self.sections.append((title, red))

    def write_line(self, line, **_kwargs):
        self.lines.append(line)

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


@pytest.fixture()
def gated(mocker, monkeypatch):
    """The gate on, the registry replaced, and the selected set controllable.

    ``_selected`` is *replaced* rather than mutated, and that is not a style
    preference. This file runs inside the same session the hook is watching, so
    a fixture that cleared the real set would leave the run it is part of with
    no record of what it selected -- and the drift direction, which is the one
    that matters most, would silently stop firing for every test collected
    after this one. The mechanism's own tests disabling the mechanism is the
    exact shape of defect this plan exists to catch.
    """
    monkeypatch.setenv(GATE, "1")
    mocker.patch.object(declared_skips, "DECLARED_SKIPS", (DECLARED,))

    def run(skipped: list[StubReport], selected: tuple[str, ...] = ()) -> StubReporter:
        mocker.patch.object(declared_skips, "_selected", set(selected))
        reporter = StubReporter(skipped)
        declared_skips.pytest_terminal_summary(reporter)
        return reporter

    return run


def test_an_undeclared_skip_fails_the_run(gated):
    reporter = gated([StubReport("tests/test_thing.py::test_new", "nobody said why")])

    assert reporter._session.exitstatus == 1
    assert "tests/test_thing.py::test_new" in reporter.text
    assert "nobody said why" in reporter.text, (
        "the reason pytest reported has to reach the log, or the person "
        "reading the failure has to go and find the skip themselves"
    )


def test_a_declared_skip_is_accepted_and_named(gated):
    reporter = gated([StubReport(DECLARED.nodeid, "no widget here, so nothing to read")])

    assert reporter._session.exitstatus == 0
    assert DECLARED.nodeid in reporter.text
    assert reporter.sections == [("Declared skips", False)], (
        "a green run still reports, because a job where the plugin never "
        "loaded is otherwise indistinguishable from one where it was satisfied"
    )


def test_a_declared_skip_that_stops_skipping_fails_the_run(gated):
    reporter = gated([], selected=(DECLARED.nodeid,))

    assert reporter._session.exitstatus == 1
    assert "ran instead of skipping" in reporter.text
    assert DECLARED.nodeid in reporter.text


def test_a_declared_skip_a_run_never_selected_is_not_a_failure(gated):
    """The other side of the direction above, and the reason it reads ``_selected``.

    Both real entries name tests in suites that most jobs do not run. If the
    hook read "did not skip" as "did not appear", every job would fail for the
    declarations belonging to the other jobs.
    """
    reporter = gated([], selected=("tests/test_thing.py::test_something_else",))

    assert reporter._session.exitstatus == 0
    assert "ran instead of skipping" not in reporter.text


def test_a_declared_skip_firing_for_a_different_cause_fails_the_run(gated):
    """The drift check: the entry describes a decision that is no longer the one taken."""
    reporter = gated([StubReport(DECLARED.nodeid, "the widget service timed out")])

    assert reporter._session.exitstatus == 1
    assert "no widget here" in reporter.text, "the declared condition"
    assert "the widget service timed out" in reporter.text, "and what happened instead"


def test_the_gate_is_off_by_default(mocker, monkeypatch):
    """Local runs are not CI, and one of the two declarations does not hold here.

    The recap check reads real git history and skips only on a shallow clone,
    so on a developer's full checkout it runs -- which the drift direction
    would read as a declaration that has stopped being true. The gate being
    opt-in is what keeps that from failing every local run.
    """
    monkeypatch.delenv(GATE, raising=False)
    mocker.patch.object(declared_skips, "DECLARED_SKIPS", (DECLARED,))
    reporter = StubReporter([StubReport("tests/test_thing.py::test_new", "whatever")])

    declared_skips.pytest_terminal_summary(reporter)

    assert reporter._session.exitstatus == 0
    assert reporter.lines == []


def test_an_unfamiliar_report_shape_is_read_rather_than_indexed():
    """``longrepr`` is a triple for every skip pytest produces today.

    ``_reason_of`` stringifies anything else instead of indexing it, so a shape
    this code has not seen surfaces as a declaration mismatch someone can read
    rather than an ``IndexError`` in a terminal summary.
    """
    class Odd:
        longrepr = "Skipped: some other shape entirely"

    assert declared_skips._reason_of(Odd()) == "Skipped: some other shape entirely"
    assert declared_skips._reason_of(object()) == "None"
