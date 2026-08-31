"""Plan 161 / CAR-34: prove ``tests/test_testing_contract.py`` can fail.

A contract test that nobody has watched fail is a contract test nobody knows
anything about. This applies one mutation per rule, runs the single assertion
that should notice, and restores the tree — so the claim "the test fails when
the contract and the repository disagree" is demonstrated rather than asserted.

    python scripts/verify_testing_contract_mutations.py

Exits non-zero if any mutation goes unnoticed, or if the tree does not come
back green. It writes to the working tree and restores from an in-memory
snapshot: it must never ``git checkout --`` anything, because the files it
mutates are exactly the ones a change in progress is editing.

This is not a CI step. It is the evidence run behind the contract, cheap enough
to repeat whenever a rule is added or reworded — which is the moment a rule
most often stops checking anything.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST = "tests/test_testing_contract.py"


def _pytest(node: str | None = None) -> tuple[int, str]:
    target = TEST if node is None else f"{TEST}::{node}"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", target, "-q", "--no-header",
         "-p", "no:cacheprovider"],
        cwd=REPO_ROOT, capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
    )
    return result.returncode, result.stdout


def _edit(relative: str, old: str, new: str) -> None:
    path = REPO_ROOT / relative
    text = path.read_text(encoding="utf-8")
    if old not in text:
        raise AssertionError(
            f"anchor not found in {relative}: {old!r}. The mutation has gone "
            f"stale, which means it has stopped testing anything."
        )
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def _write(relative: str, text: str) -> None:
    path = REPO_ROOT / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# (assertion that should notice, what changed, how to change it, files to
# snapshot before changing, files that only exist during the mutation)
MUTATIONS = [
    (
        "test_every_integration_suite_is_invoked_by_a_ci_step",
        "a CI step stops invoking tests/integration/archiver/",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "run: pytest tests/integration/archiver/ -v -m integration",
            "run: echo skipping the archiver suite",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_patching_is_mocker_everywhere",
        "a file that used mocker starts importing unittest.mock.patch",
        lambda: _edit(
            "tests/shared/test_db.py",
            "import pytest",
            "from unittest.mock import patch\n\nimport pytest",
        ),
        ["tests/shared/test_db.py"],
        [],
    ),
    (
        "test_every_route_is_reached_through_the_apps_routing_table",
        "a new route is added to ops with no test requesting it",
        lambda: _edit(
            "ops/app.py",
            '@app.get("/health")',
            '@app.get("/widgets")\ndef list_widgets():\n    return []\n\n\n'
            '@app.get("/health")',
        ),
        ["ops/app.py"],
        [],
    ),
    (
        "test_every_service_directory_has_a_row_in_the_enough_table",
        "a new service package appears with no row in the enough table",
        lambda: _write("notifier/__init__.py", ""),
        [],
        ["notifier/__init__.py"],
    ),
    (
        "test_every_production_sql_file_is_touched_by_a_layer_2_test",
        "a new production .sql file appears that no Layer 2 test names",
        lambda: _write("ops/sql/select_orphaned_widgets.sql", "SELECT 1;\n"),
        [],
        ["ops/sql/select_orphaned_widgets.sql"],
    ),
    (
        "test_every_test_directory_is_assigned_a_layer",
        "a new test directory appears that the contract places nowhere",
        lambda: _write(
            "tests/notifier/test_smoke.py",
            "def test_smoke():\n    assert True\n",
        ),
        [],
        ["tests/notifier/test_smoke.py"],
    ),
    (
        "test_every_layer_number_in_the_code_matches_the_contract",
        "a module claims a layer its directory does not have",
        lambda: _write(
            "tests/shared/test_widget_layer.py",
            '"""Layer 4 — widgets."""\n\n\ndef test_widget():\n    assert True\n',
        ),
        [],
        ["tests/shared/test_widget_layer.py"],
    ),
    (
        "test_every_pytest_invocation_in_ci_sets_pythonpath",
        "a pytest step loses its PYTHONPATH",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "        run: pytest tests/integration/sql/ -v -m integration\n"
            "        env:\n"
            "          PYTHONPATH: ${{ github.workspace }}\n",
            "        run: pytest tests/integration/sql/ -v -m integration\n"
            "        env:\n",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_every_waiver_names_a_gap_entry_that_exists",
        "a gap entry a waiver depends on is renamed in the contract",
        lambda: _edit(
            "docs/TESTING.md",
            "| G4 | **34 test files patch",
            "| G4x | **34 test files patch",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_every_integration_suite_is_invoked_by_a_ci_step",
        "a waiver survives the repair it was waiting for",
        lambda: _edit(
            TEST,
            "CI_INVOCATION_WAIVERS = ()",
            'CI_INVOCATION_WAIVERS = (\n'
            '    Waiver("tests/integration/ops", gap="G4", owner=162),\n'
            ')',
        ),
        [TEST],
        [],
    ),
    (
        "test_no_dormant_suite_is_quietly_running",
        "a suite declared dormant acquires a CI step and keeps the declaration",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "run: pytest tests/integration/archiver/ -v -m integration",
            "run: pytest tests/integration/lakehouse/ -v -m integration",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_no_gap_entry_outlives_the_plan_that_owns_it",
        "a gap entry's owner plan is archived and the entry stays behind",
        lambda: _edit(
            "docs/TESTING.md",
            "| G14 | **54 of 76 production `.sql` files",
            "| G14 | **PLACEHOLDER** | -- | Plan 84 |\n"
            "| G15 | **54 of 76 production `.sql` files",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_no_waiver_outlives_the_plan_that_owns_it",
        "a waiver's owner plan is archived and the waiver stays behind",
        lambda: _edit(
            TEST,
            'Waiver(subject, gap="G4", owner=162)',
            'Waiver(subject, gap="G4", owner=84)',
        ),
        [TEST],
        [],
    ),
    (
        "test_every_service_directory_is_measured_by_coverage",
        "a new service package appears that coverage is not pointed at",
        lambda: _write("notifier/__init__.py", ""),
        [],
        ["notifier/__init__.py"],
    ),
    (
        "test_every_service_directory_is_measured_by_coverage",
        "a coverage source is renamed and nothing measures it any more",
        lambda: _edit("pyproject.toml", '    "dashboard",', '    "dashboards",'),
        ["pyproject.toml"],
        [],
    ),
    (
        "test_the_coverage_number_the_unit_job_produces_is_consumed",
        "the unit job goes back to measuring coverage and discarding it",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "\n          --cov-fail-under=74",
            "",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_the_coverage_number_the_unit_job_produces_is_consumed",
        "a coverage threshold is left behind with nothing measuring coverage",
        lambda: _edit(
            ".github/workflows/ci.yml",
            "          --cov --cov-report=term-missing --cov-report=xml",
            "          --cov-report=term-missing --cov-report=xml",
        ),
        [".github/workflows/ci.yml"],
        [],
    ),
    (
        "test_every_asserted_rule_names_a_real_test",
        "the rules table names a check that does not exist",
        lambda: _edit(
            "docs/TESTING.md",
            "`test_every_pytest_invocation_in_ci_sets_pythonpath`",
            "`test_every_pytest_invocation_in_ci_sets_a_pythonpath`",
        ),
        ["docs/TESTING.md"],
        [],
    ),
    (
        "test_every_asserted_rule_names_a_real_test",
        "a rule is listed as checked with no test named against it",
        lambda: _edit(
            "docs/TESTING.md",
            "| `test_every_service_directory_has_a_row_in_the_enough_table` |",
            "| the enough table is compared to disk |",
        ),
        ["docs/TESTING.md"],
        [],
    ),
]


def main() -> int:
    code, output = _pytest()
    print("baseline:", output.strip().splitlines()[-1])
    if code != 0:
        print("the suite must be green before any mutation means anything")
        return 1

    missed = []
    for node, description, mutate, snapshot, created in MUTATIONS:
        saved = {rel: (REPO_ROOT / rel).read_text(encoding="utf-8") for rel in snapshot}
        try:
            mutate()
            code, _ = _pytest(node)
        finally:
            for rel, text in saved.items():
                (REPO_ROOT / rel).write_text(text, encoding="utf-8")
            for rel in created:
                path = REPO_ROOT / rel
                path.unlink(missing_ok=True)
                if path.parent != REPO_ROOT and path.parent.exists():
                    if not any(path.parent.iterdir()):
                        path.parent.rmdir()
        caught = code != 0
        missed += [] if caught else [description]
        print(f"{'CAUGHT' if caught else '*** MISSED ***':14} {description}")

    code, output = _pytest()
    print("\nrestored:", output.strip().splitlines()[-1])
    if missed:
        print("\nunnoticed mutations:\n  " + "\n  ".join(missed))
    return 1 if missed or code != 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
