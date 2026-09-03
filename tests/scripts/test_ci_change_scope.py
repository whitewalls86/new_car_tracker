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
            {"docs_tests": True, "unit": False, "heavy": False, "snapshot_dbt": False},
        ),
        (
            "a plan document and the summary it renders into: still docs only",
            [
                "docs/plans/plan_138_public_surface_refresh.md",
                "ops/static_ops/generated/project-updates.json",
            ],
            {"docs_tests": True, "unit": False, "heavy": False, "snapshot_dbt": False},
        ),
        (
            "the generated projection alone is the docs zone, not a full run",
            ["ops/static_ops/generated/recaps/2026-08-30.html"],
            {"docs_tests": True, "unit": False, "heavy": False, "snapshot_dbt": False},
        ),
        (
            "the generator is not its own output: changing it costs a full run",
            ["scripts/build_public_roadmap.py"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": False},
        ),
        (
            "a sibling of the generated directory is not swept in",
            ["ops/static_ops/info.css"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": False},
        ),
        (
            "oneoff only: lint and unit, nothing built and no database",
            [
                "scripts/oneoff/reconcile_april_detail.py",
                "tests/scripts/oneoff/test_reconcile_april_detail.py",
            ],
            {"docs_tests": False, "unit": True, "heavy": False, "snapshot_dbt": False},
        ),
        (
            "docs + oneoff: the union, still nothing heavy",
            ["docs/PLANS.md", "scripts/oneoff/reconcile_april_detail.py"],
            {"docs_tests": False, "unit": True, "heavy": False, "snapshot_dbt": False},
        ),
        (
            "one unclassified path drags the whole changeset to a full run",
            ["docs/PLANS.md", "ops/routers/deploy.py"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": False},
        ),
        (
            "a production script is not spent, so it takes the full workflow",
            ["scripts/ci_change_scope.py"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": True},
        ),
        (
            "prefix, not substring: a sibling with the same stem is not swept in",
            ["scripts/oneoff_helpers.py"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": False},
        ),
        (
            "an empty changeset is not a licence to skip anything",
            [],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": True},
        ),
        (
            "a model change is the snapshot build's own subject",
            ["dbt/models/marts/mart_vehicle_snapshot.sql"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": True},
        ),
        (
            "a migration changes the schema the two postgres_scan() sources read",
            ["db/migrations/V037__something.sql"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": True},
        ),
        (
            "bumping the pin runs the build the bump is about",
            [".github/ci_lake_snapshot_pin.json"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": True},
        ),
        (
            "a service the snapshot build never reads does not trigger it",
            ["dashboard/queries.py"],
            {"docs_tests": False, "unit": True, "heavy": True, "snapshot_dbt": False},
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

    ``snapshot_dbt`` is excluded on purpose and is the one group this property
    does not hold for: it is an allowlist, so an unclassified path leaves it
    false rather than dragging it true. That direction is safe only because the
    job is net-new -- see the module docstring for why an untaken gate can
    suppress no evidence, and why the same argument does not license narrowing
    ``unit`` or ``heavy``.
    """
    unclassified = "airflow/dags/scrape_listings.py"
    expected = {name: value for name, value in FULL.items() if name != "snapshot_dbt"}
    for companions in (
        [],
        ["docs/PLANS.md"],
        ["scripts/oneoff/reconcile_april_detail.py"],
        ["docs/PLANS.md", "tests/scripts/oneoff/test_reconcile_april_detail.py"],
    ):
        classified = classify_from_nul(_nul([*companions, unclassified]))
        assert {
            name: value for name, value in classified.items() if name != "snapshot_dbt"
        } == expected
        assert classified["snapshot_dbt"] is False


def test_a_trigger_path_runs_the_snapshot_build_from_inside_any_zone():
    """The allowlist answers its own question regardless of the zone check.

    A changeset that is entirely docs still skips the unit suite, and still runs
    the snapshot build if one of those docs paths were ever a trigger -- the two
    decisions are independent, which is what stops a future zone widening from
    silently switching this gate off.
    """
    classified = classify_from_nul(
        _nul(["scripts/oneoff/reconcile_april_detail.py", "dbt/models/sources.yml"])
    )
    assert classified["snapshot_dbt"] is True
    # The trigger did not disturb the zone answer: the dbt path is unclassified
    # for the other two groups, so they take the full run.
    assert classified["heavy"] is True


def test_every_snapshot_trigger_is_a_path_that_exists():
    """A trigger naming a path that is not there gates on nothing.

    The rename that moves `scripts/seed_lake_snapshot.py` and leaves this tuple
    behind is silent otherwise: the job simply stops running for the change that
    should have run it, and no test fails.
    """
    from pathlib import Path

    from scripts.ci_change_scope import SNAPSHOT_DBT_TRIGGERS

    repo_root = Path(__file__).resolve().parents[2]
    missing = [
        trigger.decode()
        for trigger in SNAPSHOT_DBT_TRIGGERS
        if not (repo_root / trigger.decode().rstrip("/")).exists()
    ]
    assert not missing, f"snapshot_dbt triggers name paths that do not exist: {missing}"


def test_docs_only_supports_spaces_in_paths():
    assert classify_from_nul(b"docs/a plan.md\0docs/another plan.md\0") == {
        "docs_tests": True,
        "unit": False,
        "heavy": False,
        "snapshot_dbt": False,
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
            ["docs_tests=true", "unit=false", "heavy=false", "snapshot_dbt=false"],
        ),
        (
            "scripts/oneoff/reconcile_april_detail.py\0",
            ["docs_tests=false", "unit=true", "heavy=false", "snapshot_dbt=false"],
        ),
        (
            "docs/PLANS.md\0scripts/oneoff/reconcile_april_detail.py\0",
            ["docs_tests=false", "unit=true", "heavy=false", "snapshot_dbt=false"],
        ),
        (
            "ops/routers/deploy.py\0",
            ["docs_tests=false", "unit=true", "heavy=true", "snapshot_dbt=false"],
        ),
        (
            "dbt/models/sources.yml\0",
            ["docs_tests=false", "unit=true", "heavy=true", "snapshot_dbt=true"],
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
