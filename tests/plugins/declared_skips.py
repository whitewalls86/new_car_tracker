"""Every skip observed in a CI run is declared, or the run fails.

Plan 162 Stage U / CAR-81.

``tests/integration/sql/conftest.py`` carried this hook first, and it was the
right mechanism at the wrong scope: it failed a run on any skip in one suite,
in one job. Measured across a whole run on 2026-09-04, the repository produced
**two skips, in two jobs, for two reasons, with zero mechanisms** -- a Layer 0
test in the unit job and a Layer 4 test in ``lake-integration``, both correct
decisions, both held in place by prose. A third would have arrived the same
way and nothing would have said so.

**The shape is** ``DORMANT_SUITES``, one level down. That tuple made a
deliberately unrun *suite* declare itself, and asserted both directions: an
undeclared one fails, and a declared one that starts running fails too. The
same two directions apply to a deliberately skipped *test*, and the second
matters more here -- a skip whose reason has stopped being true is exactly the
drift this plan exists against.

So this hook checks three things, and only when :data:`GATE` is set:

1. a skip with no entry in :data:`DECLARED_SKIPS` fails the run;
2. an entry that was *selected* in this run and did not skip fails it, because
   a reason that has stopped being true is a reason nobody is reading;
3. an entry that skipped for text its ``condition`` does not match fails it,
   because the same test skipping for a different cause is a new decision
   wearing an old declaration.

**It reports on a green run too**, listing what it accepted. A job where the
plugin silently failed to load is otherwise indistinguishable from a job where
it loaded and was satisfied, and ``docs-tests`` -- the one job that needs the
``-p`` registration rather than a conftest -- only ever runs on a docs-only
changeset, so its coverage is read out of a log rather than watched failing.

Which skips may be declared at all is not decided here. That rule is derived
from the contract's own layer table by
``test_no_declared_skip_sits_at_a_layer_that_admits_none``, which is what
retired ``REQUIRE_LAYER_2_EXECUTION`` without loosening Layer 2.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

#: Set once at workflow level in ``.github/workflows/ci.yml`` so that every job
#: inherits it, including jobs nobody has written yet. Off locally: a developer
#: on a full clone does not skip the recap check below, and direction 2 would
#: fail their run for it.
GATE = "REQUIRE_DECLARED_SKIPS"


@dataclass(frozen=True)
class DeclaredSkip:
    """One test deliberately skipped in CI, and why it is allowed to be.

    Modelled on ``Dormant`` and not on ``Waiver``, for the reason that class
    already gives: a waiver is debt -- it names a violation, an owner plan, and
    dies when that plan archives. Both entries below are decisions with no
    repair pending. ``actions/checkout@v4`` will clone at depth 1 for as long
    as it is what we call, and the dictionary job registers no dictionary on
    purpose.

    A skip that *is* waiting on a repair is debt and belongs in ``WAIVERS``,
    which already expires with its plan. That path is deliberately not built:
    nothing needs it today, and a tier with no members is a tier nobody has
    watched work.

    ``condition`` is the mechanical half of the declaration and ``reason`` the
    human one. ``condition`` must appear in the text pytest reports for the
    skip, so a test that starts skipping for a different cause fails rather
    than inheriting this entry.
    """

    nodeid: str
    reason: str
    condition: str
    since: date


DECLARED_SKIPS = (
    DeclaredSkip(
        "tests/test_planning_docs.py::TestWeeklyRecaps"
        "::test_every_sha_a_recap_names_is_a_real_commit",
        reason=(
            "the check reads real git history to prove a recap's shas exist, "
            "and every job clones with actions/checkout@v4, which is depth 1. "
            "Its value is local, in the run the plan-week skill makes "
            "immediately after writing a recap; a fetch-depth: 0 checkout "
            "would buy CI a check it already gets on the machine that wrote "
            "the file."
        ),
        condition="shallow clone",
        since=date(2026, 9, 4),
    ),
    DeclaredSkip(
        "tests/integration/shared/test_read_html_pack_fallback.py"
        "::test_dictionary_compressed_objects_and_packed_members_are_both_readable",
        reason=(
            "the lake-integration job leaves INTEGRATION_HTML_DICT_ID unset on "
            "purpose: it registers no dictionary, and the dictionary half is "
            "covered by unit tests that can register one without Postgres. The "
            "no-dictionary half of this test is a real assertion that runs "
            "before the skip, so the step is not empty."
        ),
        condition="INTEGRATION_HTML_DICT_ID not set",
        since=date(2026, 9, 4),
    ),
)

#: A ceiling, not a count -- the ``--cov-fail-under`` idiom pointed the other
#: way. It exists so that a third declaration cannot be a quiet tuple append:
#: the number has to move in the same diff, and the number is what review
#: argues about. Lower it when a stage removes a skip; never raise it to fit
#: one that could have been fixed instead.
DECLARED_SKIP_CEILING = 2

# Populated by ``pytest_collection_finish`` below. Direction 2 needs the set of
# tests this run actually selected: an entry naming a test in a suite this job
# does not run has not stopped being true, it simply was not asked.
_selected: set[str] = set()


def _reason_of(report) -> str:
    """The text pytest reports for a skip.

    ``longrepr`` is ``(path, lineno, "Skipped: <reason>")`` for a skipped test
    and for a collection-level skip alike. Anything else is stringified rather
    than indexed, so an unfamiliar report shape produces a declaration mismatch
    to read instead of an ``IndexError`` to debug.
    """
    longrepr = getattr(report, "longrepr", None)
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        return str(longrepr[2])
    return str(longrepr)


def pytest_collection_finish(session):
    """Record what this run selected, after every deselection has happened.

    ``pytest_collection_modifyitems`` is where ``-m "not integration"`` removes
    items, and reading ``session.items`` here rather than there is what makes
    this the *selected* set rather than the collected one. A deselected test
    did not decline to skip; it was never asked to.
    """
    _selected.clear()
    _selected.update(item.nodeid for item in session.items)


def pytest_terminal_summary(terminalreporter):
    """Report on the run's skips, and fail it when one is undeclared."""
    if not os.environ.get(GATE):
        return

    declared = {entry.nodeid: entry for entry in DECLARED_SKIPS}
    observed = {
        report.nodeid: _reason_of(report)
        for report in terminalreporter.stats.get("skipped", [])
    }

    undeclared = sorted(nodeid for nodeid in observed if nodeid not in declared)
    mismatched = sorted(
        f"{nodeid}\n      declared condition: {declared[nodeid].condition!r}"
        f"\n      skipped instead for: {reason!r}"
        for nodeid, reason in observed.items()
        if nodeid in declared and declared[nodeid].condition not in reason
    )
    silent = sorted(
        f"{nodeid} (declared {entry.since}: {entry.condition})"
        for nodeid, entry in declared.items()
        if nodeid in _selected and nodeid not in observed
    )
    accepted = sorted(
        nodeid
        for nodeid, reason in observed.items()
        if nodeid in declared and declared[nodeid].condition in reason
    )

    failed = bool(undeclared or mismatched or silent)
    terminalreporter.section("Declared skips", red=failed)

    if undeclared:
        terminalreporter.write_line(
            f"{len(undeclared)} skip(s) with no entry in DECLARED_SKIPS. A "
            "skipped test executes nothing, so this run proves nothing about "
            "what it names. Fix the cause, or declare it in "
            "tests/plugins/declared_skips.py with its reason and its "
            "condition:"
        )
        for nodeid in undeclared:
            terminalreporter.write_line(f"    {nodeid}")
            terminalreporter.write_line(f"      {observed[nodeid]}")

    if mismatched:
        terminalreporter.write_line(
            f"{len(mismatched)} declared skip(s) fired for something other "
            "than the condition they declare. The same test skipping for a "
            "new cause is a new decision, and the entry describes the old one:"
        )
        for line in mismatched:
            terminalreporter.write_line(f"    {line}")

    if silent:
        terminalreporter.write_line(
            f"{len(silent)} declared skip(s) ran instead of skipping. The "
            "reason has stopped being true, which is the drift this registry "
            "exists against; delete the entry:"
        )
        for line in silent:
            terminalreporter.write_line(f"    {line}")

    if accepted:
        terminalreporter.write_line(f"{len(accepted)} declared skip(s) accepted:")
        for nodeid in accepted:
            terminalreporter.write_line(f"    {nodeid}")
    elif not failed:
        terminalreporter.write_line(
            f"no skips, and none declared for what this run selected "
            f"({len(_selected)} test(s))."
        )

    if failed:
        terminalreporter._session.exitstatus = 1
