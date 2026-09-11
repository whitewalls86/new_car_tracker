"""Every test file was asked for tests by the step that is supposed to run it.

Plan 162 Stage R. The half of the invocation instrument that no single pytest
job can be: a file's home job is one of six, so no one job's record is a
reading and only something downstream of all of them is.

**What it replaces** is not a missing check but a weak one.
``test_every_integration_suite_is_invoked_by_a_ci_step`` reads ``ci.yml`` as
text and credits a file when the *directory containing it* appears in a
``run:`` line. Here the question is answered directly, from
``tests/plugins/invocation_recorder.py``'s observation of what pytest actually
selected.

Three checks, and the order matters -- the second is the one the static rule
could never make, and the third is the one no per-step assertion can.

* **Every test file under a path an invocation named was selected by that
  invocation.** This is the `Stage F
  <../docs/evidence/plan_162_stage_F_evidence.md>`_ case:
  ``tests/integration/airflow/test_scrape_listings.py`` carried no
  ``integration`` marker, so ``pytest tests/integration/airflow/ -m
  integration`` collected it and deselected all 7 of its tests. Only the
  coverage number caught it, at 0.18 percentage points, and the rule above
  stayed green throughout because the directory was named.

  **Scoped to arguments under** ``tests/integration/``, and that scope is a
  property of the argument rather than a list anyone maintains. The unit job
  names ``tests/`` and legitimately selects nothing from 62 integration files;
  keyed on paths under ``tests/integration/`` it is simply not asked, and no
  exemption has to exist for it.

  ``--ignore``d paths are excluded from the invocation that ignores them --
  they are somebody else's step, and check two is what proves that step ran.

* **Every test file was selected by some invocation, or is declared dormant.**
  This is `G1 <../docs/TESTING.md#the-gap-list>`_: 73 integration-marked tests
  in 11 files were written, reviewed, merged and maintained while no CI step
  ran them. ``DORMANT_SUITES`` is read rather than copied, so a suite that is
  deliberately not run says so in exactly one place and both directions of
  that declaration stay enforced where they already are.

* **Every pytest step of every job that reported produced a record.** A step
  that silently stopped running pytest asserts nothing, and a per-step check
  structurally cannot see it -- the check would be in the step that did not
  run. A job absent from the records altogether is *skipped*, not failing:
  ``docs-tests`` runs only on a docs-only changeset, and ``snapshot-dbt`` on
  its own trigger set, so a run scope cannot be inferred here. What is asserted
  is that a job which ran pytest at all ran all of it.

**No waiver list**, following the precedent ``check_sql_execution_coverage.py``
set when its own empty ledger was deleted rather than kept: the declaration a
deliberately-unrun suite needs already exists in ``DORMANT_SUITES``, and a
second ledger here would be a cheaper way to say the same thing, which is how
ledgers stop being read.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# Read out of the contract test rather than derived a second time -- two
# derivations are two answers, and the one nobody runs is the one that drifts.
# This is the import `check_sql_execution_coverage.py` already makes for
# `production_sql_files()`, for the same reason.
from tests.rules.test_testing_contract import (  # noqa: E402
    DORMANT_SUITES,
    UNMARKED_BY_DESIGN,
    pytest_invocations,
)

TESTS_DIR = REPO_ROOT / "tests"
_SCOPED_PREFIX = "tests/integration/"


def _load(records: Path) -> list[dict]:
    files = sorted(records.rglob("tests-*.json"))
    if not files:
        raise SystemExit(
            f"no invocation records under {records}. Every pytest step writes "
            f"one into $CI_RUN_RECORDS and every job uploads them; if none "
            f"arrived, the recorder did not load and this gate is measuring "
            f"nothing."
        )
    payloads = [json.loads(path.read_text(encoding="utf-8")) for path in files]
    print(f"read {len(payloads)} invocation record(s) from {len(files)} file(s)")
    return payloads


def _test_files() -> set[str]:
    return {
        path.relative_to(REPO_ROOT).as_posix()
        for path in TESTS_DIR.rglob("test_*.py")
    }


def _named_paths(argv: list[str]) -> tuple[list[str], set[str]]:
    """``(paths this invocation named, paths it ignored)``.

    Arguments are read from the record rather than from ``ci.yml`` on purpose:
    what a step *was written to* run and what the process *was given* differ
    the moment a step grows a shell variable, and this gate is about the
    second.
    """
    named, ignored = [], set()
    for argument in argv:
        if argument.startswith("--ignore="):
            ignored.add(argument.split("=", 1)[1].rstrip("/"))
        elif argument.startswith("tests"):
            named.append(argument.rstrip("/"))
    return named, ignored


def _files_under(path: str) -> set[str]:
    target = REPO_ROOT / path
    if target.is_dir():
        return {
            candidate.relative_to(REPO_ROOT).as_posix()
            for candidate in target.rglob("test_*.py")
        }
    return {path} if target.is_file() else set()


def _describe(record: dict) -> str:
    return f"{record.get('job') or '?'}: pytest {' '.join(record['argv'])}"


def _unasked(records: list[dict]) -> list[str]:
    """Check one: a file under a named integration path that was not selected.

    ``UNMARKED_BY_DESIGN`` is exempt here and **only** here. Such a file is
    declared as not this step's to run; it is still held to running somewhere
    by :func:`_unrun`, so the declaration cannot be used to hide a file that
    runs nowhere at all.
    """
    excused = {entry.subject for entry in UNMARKED_BY_DESIGN}
    failures = []
    for record in records:
        named, ignored = _named_paths(record["argv"])
        selected = set(record["selected"])
        deselected = set(record["deselected"])
        for path in named:
            if not path.startswith(_SCOPED_PREFIX):
                continue
            for candidate in sorted(_files_under(path) - ignored - excused):
                if candidate in selected:
                    continue
                how = (
                    "collected and deselected entirely"
                    if candidate in deselected
                    else "not collected at all"
                )
                failures.append(
                    f"{candidate}\n      {how} by `{_describe(record)}`"
                )
    return failures


def _contradicted(records: list[dict]) -> list[str]:
    """Check four: a declared file that the step naming its directory did run.

    The other direction, without which the declaration is an unread comment --
    the same pair ``test_no_dormant_suite_is_quietly_running`` holds one level
    up. A file that gains the marker, or a step that drops its ``-m``, has made
    the declaration's reason stop being true, and a reason that has stopped
    being true is a reason nobody is reading.
    """
    declared = {entry.subject: entry for entry in UNMARKED_BY_DESIGN}
    failures = []
    for record in records:
        named, _ = _named_paths(record["argv"])
        if not any(path.startswith(_SCOPED_PREFIX) for path in named):
            continue
        for candidate in sorted(set(record["selected"]) & declared.keys()):
            entry = declared[candidate]
            failures.append(
                f"{candidate}\n      declared {entry.since}: {entry.reason}"
                f"\n      selected anyway by `{_describe(record)}`"
            )
    return failures


def _unrun(records: list[dict]) -> list[str]:
    """Check two: a file no invocation anywhere selected."""
    selected = {path for record in records for path in record["selected"]}
    dormant = tuple(f"{entry.subject.rstrip('/')}/" for entry in DORMANT_SUITES)
    return sorted(
        path
        for path in _test_files() - selected
        if not path.startswith(dormant)
    )


def _silent_steps(records: list[dict]) -> list[str]:
    """Check three: a job that reported, but not for all of its pytest steps."""
    owed: dict[str, list[str]] = defaultdict(list)
    for key, _, step, _ in pytest_invocations():
        owed[key].append(step)
    produced: dict[str, int] = defaultdict(int)
    for record in records:
        produced[record.get("job") or ""] += 1
    return sorted(
        f"{key}: {len(steps)} pytest step(s) in ci.yml, {produced[key]} record(s)"
        for key, steps in owed.items()
        if produced.get(key) and produced[key] != len(steps)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--records", type=Path, default=REPO_ROOT / "ci-run-records"
    )
    arguments = parser.parse_args()
    records = _load(arguments.records)

    unasked = _unasked(records)
    unrun = _unrun(records)
    silent = _silent_steps(records)
    contradicted = _contradicted(records)

    if unasked:
        print(
            f"\n{len(unasked)} test file(s) sit under a path a CI step names "
            f"and were not selected by it. The step is credited with running "
            f"them and runs none:"
        )
        for line in unasked:
            print(f"    {line}")
    if unrun:
        print(
            f"\n{len(unrun)} test file(s) were selected by no invocation in "
            f"this run. They are maintained and prove nothing. Fix the step, "
            f"or declare the suite in DORMANT_SUITES with its reason:"
        )
        for path in unrun:
            print(f"    {path}")
    if silent:
        print(
            f"\n{len(silent)} job(s) produced fewer records than they have "
            f"pytest steps, so a step ran no pytest and nothing else can see it:"
        )
        for line in silent:
            print(f"    {line}")

    if contradicted:
        print(
            f"\n{len(contradicted)} file(s) declared UNMARKED_BY_DESIGN were "
            f"selected by the step that names their directory. The reason has "
            f"stopped being true; delete the entry:"
        )
        for line in contradicted:
            print(f"    {line}")

    if not (unasked or unrun or silent or contradicted):
        declared = len(UNMARKED_BY_DESIGN)
        print(
            f"every one of {len(_test_files())} test file(s) was selected by "
            f"the step that names it, with {declared} declared "
            f"UNMARKED_BY_DESIGN and running elsewhere."
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
