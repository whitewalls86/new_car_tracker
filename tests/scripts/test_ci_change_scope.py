import subprocess
import sys

import pytest

from scripts.ci_change_scope import FULL, classify_from_nul


def _nul(paths):
    return b"".join(path.encode() + b"\0" for path in paths)


# The decision table, written once. Every case below is a row of it, and the
# column order is the order `main()` prints.
@pytest.mark.parametrize(
    ("label", "paths", "expected"),
    [
        (
            "docs only: the documentation suite substitutes for the unit run",
            ["docs/PLANS.md", "docs/plans/plan_148.md"],
            {"docs_tests": True, "unit": False, "heavy": False},
        ),
        (
            "oneoff only: lint and unit, nothing built and no database",
            [
                "scripts/oneoff/reconcile_april_detail.py",
                "tests/scripts/oneoff/test_reconcile_april_detail.py",
            ],
            {"docs_tests": False, "unit": True, "heavy": False},
        ),
        (
            "docs + oneoff: the union, still nothing heavy",
            ["docs/PLANS.md", "scripts/oneoff/reconcile_april_detail.py"],
            {"docs_tests": False, "unit": True, "heavy": False},
        ),
        (
            "one unclassified path drags the whole changeset to a full run",
            ["docs/PLANS.md", "ops/routers/deploy.py"],
            {"docs_tests": False, "unit": True, "heavy": True},
        ),
        (
            "a production script is not spent, so it takes the full workflow",
            ["scripts/ci_change_scope.py"],
            {"docs_tests": False, "unit": True, "heavy": True},
        ),
        (
            "prefix, not substring: a sibling with the same stem is not swept in",
            ["scripts/oneoff_helpers.py"],
            {"docs_tests": False, "unit": True, "heavy": True},
        ),
        (
            "an empty changeset is not a licence to skip anything",
            [],
            {"docs_tests": False, "unit": True, "heavy": True},
        ),
    ],
)
def test_classify_from_nul(label, paths, expected):
    assert classify_from_nul(_nul(paths)) == expected, label


def test_docs_and_oneoff_together_skip_exactly_what_each_skips_alone():
    """The composition property, asserted rather than left to the table.

    Before Stage 5b's second pass the two zones were mutually exclusive, so a
    branch that edited a spent script *and* wrote up why took the full 267s
    workflow -- the most ordinary shape of change in this repository, and the
    one the classifier was least useful for.
    """
    docs = classify_from_nul(_nul(["docs/PLANS.md"]))
    oneoff = classify_from_nul(_nul(["scripts/oneoff/reconcile_april_detail.py"]))
    both = classify_from_nul(
        _nul(["docs/PLANS.md", "scripts/oneoff/reconcile_april_detail.py"])
    )

    assert not (docs["heavy"] or oneoff["heavy"] or both["heavy"])
    # The union of what each zone needs, with one redundancy removed: the unit
    # job runs `pytest tests/`, which already contains the documentation tests.
    assert both["unit"] is True
    assert both["docs_tests"] is False


def test_paths_in_neither_zone_can_never_narrow_the_run():
    """Fail-open, stated as a property over every mixture.

    Whatever else is in the changeset, one unclassified path must produce
    exactly the full-workflow answer -- not merely `heavy`, the whole row.
    """
    unclassified = "airflow/dags/scrape_listings.py"
    for companions in (
        [],
        ["docs/PLANS.md"],
        ["scripts/oneoff/reconcile_april_detail.py"],
        ["docs/PLANS.md", "tests/scripts/oneoff/test_reconcile_april_detail.py"],
    ):
        assert classify_from_nul(_nul([*companions, unclassified])) == FULL


def test_docs_only_supports_spaces_in_paths():
    assert classify_from_nul(b"docs/a plan.md\0docs/another plan.md\0") == {
        "docs_tests": True,
        "unit": False,
        "heavy": False,
    }


@pytest.mark.parametrize(
    "data",
    [
        b"docs/PLANS.md",
        b"docs/PLANS.md\0\0",
    ],
)
def test_malformed_input_fails_closed(data):
    with pytest.raises(ValueError):
        classify_from_nul(data)


@pytest.mark.parametrize(
    ("changed", "expected"),
    [
        (
            "docs/PLANS.md\0docs/plans/plan_148.md\0",
            ["docs_tests=true", "unit=false", "heavy=false"],
        ),
        (
            "scripts/oneoff/reconcile_april_detail.py\0",
            ["docs_tests=false", "unit=true", "heavy=false"],
        ),
        (
            "docs/PLANS.md\0scripts/oneoff/reconcile_april_detail.py\0",
            ["docs_tests=false", "unit=true", "heavy=false"],
        ),
        (
            "ops/routers/deploy.py\0",
            ["docs_tests=false", "unit=true", "heavy=true"],
        ),
    ],
)
def test_command_line_contract(changed, expected):
    """One `name=true|false` line per job group, which is what ci.yml parses.

    The workflow matches each line against a literal `name=true|false` case and
    drops anything else, so the shape matters as much as the values.
    """
    completed = subprocess.run(
        [sys.executable, "scripts/ci_change_scope.py"],
        input=changed,
        capture_output=True,
        check=True,
        text=True,
        encoding="utf-8",
    )
    assert completed.stdout.splitlines() == expected


def test_malformed_input_exits_two_without_classifying():
    completed = subprocess.run(
        [sys.executable, "scripts/ci_change_scope.py"],
        input="docs/PLANS.md",
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert completed.returncode == 2
    assert completed.stdout == ""
