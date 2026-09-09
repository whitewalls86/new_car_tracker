"""Plan 161 / CAR-34: ``docs/TESTING.md``, asserted rather than described.

``docs/ARCHITECTURE.md:179`` carried a Testing Strategy section that was
accurate in April 2026 and quietly false by August. Nothing could tell the
difference, so nothing did. This file is the mechanism that can.

It follows ``tests/test_planning_docs.py`` and
``tests/airflow/test_coordination_admission.py``: **coverage is asserted, not
enumerated.** Every subject below is derived from the repository -- the
services from the packages on disk, the routes from each app's real routing
table, the layers from the contract's own headings. There is no inventory here
that a new violation can simply be left out of.

The one sanctioned exception is :data:`WAIVERS`, and it is deliberately
uncomfortable to use. A waiver names the exact violating subject, the gap-list
entry that owns it and the plan that will fix it; the checks assert **both**
directions, so a waiver that no longer describes a violation fails just as
loudly as an unwaived violation does. That is what "the list only shrinks"
means when a test says it rather than a document.

The mechanical rules are asserted here, matching the table in
`docs/TESTING.md` under *What CI asserts*. **Three** are judgement -- whether
the thing under test is the thing being mocked, whether a failure branch
matters to another service, and whether an assertion is meaningful. Those
belong to ``.claude/skills/testing-contract/``, which flags them and refuses to
certify them. **Nothing in this file should grow to imply it checks them.**

There were four until Plan 162 Stage X, and the fourth left by being
mechanised: *whether a ``SELECT`` in a test file paraphrases production or
seeds a fixture*. It was judgement for an exact reason -- fixture seeds are SQL
in test files too, and a checker that cannot tell them apart fails on correct
code -- and that reason stopped applying when no SQL literal was left under
``tests/`` for the ambiguity to live in. See Rule 5g.

This docstring used to open "Eight rules are mechanical", which was already
false: the table it points at had eleven rows. Counts in prose drift, which is
the whole subject of this file, so the mechanical half is no longer stated as a
number here -- the table is the count, and
``test_every_asserted_rule_names_a_real_test`` is what keeps it honest.

One further check faces the other way. Every rule above compares the contract
to the repository; ``test_every_asserted_rule_names_a_real_test`` compares the
contract to this file, so the rules table cannot claim a check that was never
written. It was added by CAR-43 after the table was found doing exactly that.

Three of the eight checks report more violations than the contract's gap list
recorded on 2026-08-31, because CAR-33 measured some of them by hand and by
eye:

* routes reached through no routing table: **12**, not G6's 4;
* files patching with something other than ``mocker``: **34**, not G4's 20 --
  G4 counted ``unittest.mock.patch`` and did not count ``monkeypatch.setattr``;
* ``.sql`` files no Layer 2 test touches: **54 of 76**, which G5 did not
  measure at all -- it counted inline SQL, the opposite direction.

Those are recorded as G6, G4 and the new G14 in the contract. Finding them is
this file working, not this file disagreeing with the contract: the contract
said the repository would be found wrong, and named the mechanism that would
find it.
"""
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path

import pytest
import yaml

from archiver.processors.lake_snapshot_export_cache import INCLUDED_TABLES
from archiver.processors.lake_source_audit import SOURCE_TABLE_SPECS
from scripts.seed_lake_snapshot import dbt_source_tables
from shared.db_vocabularies import DB_VOCABULARIES
from shared.lake_snapshot_postgres import POSTGRES_SNAPSHOT_TABLES
from tests.plugins.declared_skips import (
    DECLARED_SKIP_CEILING,
    DECLARED_SKIPS,
    GATE,
)
from tests.sql_bindings import holds_a_placeholder, renderings

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT = "docs/TESTING.md"
WORKFLOW = ".github/workflows/ci.yml"
ARCHIVE = "docs/planning/completed_plans.md"
TESTS_DIR = REPO_ROOT / "tests"


@lru_cache(maxsize=None)
def _read(relative: str) -> str:
    return (REPO_ROOT / relative).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Waivers.
#
# Plan 161 is a decide-and-codify plan and its non-goals are explicit: it does
# not fix the violations. So today's violations are grandfathered here, each
# naming its gap entry and the plan that owns the repair, and the checks fail
# on anything else. ``since`` is the date the violation was measured, not the
# date it appeared -- several are much older than this file.
# ---------------------------------------------------------------------------
MEASURED = date(2026, 8, 31)


@dataclass(frozen=True)
class Waiver:
    """One grandfathered violation.

    ``subject`` must equal, character for character, the string the check
    reports. That is on purpose: a waiver you can write loosely is a waiver
    that silences the next violation too.
    """

    subject: str
    gap: str
    owner: int
    since: date = MEASURED


def _waived(waivers: tuple[Waiver, ...]) -> set[str]:
    return {waiver.subject for waiver in waivers}


def _assert_exactly(found: set[str], waivers: tuple[Waiver, ...], rule: str) -> None:
    """Both directions, because only one of them keeps the list shrinking.

    An unwaived violation is the obvious failure. A waiver with nothing left to
    waive is the one that matters over time: without it, repairs accumulate
    behind a list that still says they are outstanding, and the list stops
    describing anything.
    """
    waived = _waived(waivers)
    unwaived = sorted(found - waived)
    assert not unwaived, (
        f"{rule}\n\nNot waived:\n  " + "\n  ".join(unwaived) +
        "\n\nFix it, or add a Waiver naming the gap entry and the owner plan. "
        "Adding one is a decision, not a convenience."
    )
    stale = sorted(waived - found)
    assert not stale, (
        f"{rule}\n\nThese waivers no longer describe a violation and must be "
        "deleted:\n  " + "\n  ".join(stale)
    )


# ---------------------------------------------------------------------------
# The contract's own structure, parsed from the document.
#
# Everything below reads ``docs/TESTING.md`` rather than restating it. A
# heading renamed to make parsing easier would let this file edit its own
# input, which is the one thing a structural test must never be able to do --
# so the parsers assert what they expected to find and fail by name when it
# has moved.
# ---------------------------------------------------------------------------
_LAYER_HEADING = re.compile(r"^### Layer (\d) — (.+?)\s*$", re.M)
_LIVES_IN = re.compile(r"^\*\*Lives in:\*\* `([^`]+)`", re.M)
_SUITE_ROW = re.compile(r"^\| `(tests/[^`]+)` \| (\d) \|", re.M)
_SCRIPT_BUCKET_ROW = re.compile(r"^\| `(scripts/[^`]*)` \| (yes|\*\*no\*\*) \|", re.M)


@lru_cache(maxsize=None)
def contract_layers() -> dict[int, str]:
    """``{0: 'Config and contract tests', ...}`` from the ``###`` headings."""
    found = {int(n): title for n, title in _LAYER_HEADING.findall(_read(CONTRACT))}
    assert found, f"no '### Layer N — Title' headings in {CONTRACT}"
    assert sorted(found) == list(range(len(found))), (
        f"the layers in {CONTRACT} must be numbered from 0 with no gaps; "
        f"found {sorted(found)}"
    )
    return found


@lru_cache(maxsize=None)
def service_packages() -> frozenset[str]:
    """A service is a top-level Python package. Derived, never listed.

    This is exactly the eight the contract's "enough" table has rows for --
    ``tests`` excluded, being the suite rather than a service. ``airflow/``,
    ``dbt/`` and ``lakehouse/`` have Dockerfiles and are not packages; they own
    no importable Python that a service test could reach.
    """
    return frozenset(
        path.parent.name
        for path in REPO_ROOT.glob("*/__init__.py")
        if path.parent.name != "tests"
    )


@lru_cache(maxsize=None)
def declared_layer_homes() -> dict[int, str]:
    """Each layer's ``**Lives in:**`` path, keyed by layer number."""
    text = _read(CONTRACT)
    homes: dict[int, str] = {}
    for section in re.split(r"^### ", text, flags=re.M)[1:]:
        heading = section.partition("\n")[0]
        match = re.match(r"Layer (\d) — ", heading)
        if match is None:
            continue
        lives_in = _LIVES_IN.search(section)
        assert lives_in, f"'### {heading}' in {CONTRACT} has no '**Lives in:**' line"
        homes[int(match.group(1))] = lives_in.group(1)
    assert set(homes) == set(contract_layers()), (
        "every layer heading needs a '**Lives in:**' line: "
        f"{sorted(set(contract_layers()) - set(homes))} have none"
    )
    return homes


@lru_cache(maxsize=None)
def directory_layers() -> dict[str, int]:
    """Every test directory on disk, mapped to the layer the contract gives it.

    Two sources, both in ``docs/TESTING.md``: the ``**Lives in:**`` lines,
    whose ``<service>`` placeholder expands over the packages on disk, and the
    explicit rows of *Where the newer suites sit* for the directories that are
    named rather than patterned.
    """
    homes = declared_layer_homes()
    mapping: dict[str, int] = {}

    for layer, home in homes.items():
        if "<service>" in home:
            for service in service_packages():
                mapping[home.replace("<service>", service).rstrip("/")] = layer
        elif home.endswith("/"):
            mapping[home.rstrip("/")] = layer
        else:
            mapping["tests"] = layer  # ``tests/*.py`` -- the top level itself

    for path, layer in _SUITE_ROW.findall(_read(CONTRACT)):
        mapping[path.rstrip("/")] = int(layer)

    assert "tests" in mapping, (
        f"{CONTRACT} no longer says which layer the top level of tests/ is"
    )
    return mapping


def _test_directories() -> list[Path]:
    """Directories holding at least one test module, plus the top level.

    A directory with only an ``__init__.py`` is not a suite and is not asserted
    on -- ``tests/integration/observability/`` is one today. It is also not
    invisible: an empty package that never fills up is a different problem from
    a suite nothing runs, and conflating them would report the wrong one.
    """
    found = {
        path.parent
        for path in TESTS_DIR.rglob("test_*.py")
    }
    return sorted(found)


def _relative(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def _layer_of(directory: Path) -> int | None:
    """The layer of *directory*, inheriting from the nearest declared ancestor.

    ``tests/ops/routers/`` is Layer 1 because ``tests/ops/`` is; the contract
    does not enumerate sub-packages and should not have to.

    ``tests`` itself is deliberately *not* an ancestor anything inherits from.
    Layer 0's home is the top-level modules, and letting it catch every
    unplaced directory underneath would turn the gate below into a test that
    can never fail -- it would answer "Layer 0" for a suite nobody had placed.
    """
    mapping = directory_layers()
    relative = _relative(directory)
    while True:
        if relative in mapping:
            return mapping[relative]
        relative, _, tail = relative.rpartition("/")
        if not tail or relative in ("", "tests"):
            return None


# ---------------------------------------------------------------------------
# The workflow.
# ---------------------------------------------------------------------------
def _step_name(step: dict) -> str:
    """A step's ``name:``, or its first ``run:`` line when it has none.

    Many steps here are bare ``- run: pip install ...`` with no name, so the
    fallback is what makes a step addressable at all -- and it has to be the
    same string in both directions or :func:`_step_env` looks up nothing.
    """
    if step.get("name"):
        return step["name"]
    lines = str(step.get("run", "")).strip().splitlines()
    return lines[0] if lines else "?"


@lru_cache(maxsize=None)
def workflow_steps() -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    """``(job name, step name, run lines)`` for every step with a ``run:``."""
    document = yaml.safe_load(_read(WORKFLOW))
    steps: list[tuple[str, str, tuple[str, ...]]] = []
    for job in document["jobs"].values():
        for step in job.get("steps", []):
            if "run" in step:
                steps.append((
                    job.get("name", "?"),
                    _step_name(step),
                    tuple(str(step["run"]).splitlines()),
                ))
    assert steps, f"no run: steps parsed out of {WORKFLOW}"
    return tuple(steps)


_PYTEST_INVOCATION = re.compile(r"(?:^|[\s/])pytest\s+(?P<args>.+)$")


@lru_cache(maxsize=None)
def pytest_steps() -> tuple[tuple[str, str, str], ...]:
    """``(job, step, argument string)`` for every step that runs pytest.

    ``pip install pytest`` is not an invocation and must not be counted as one,
    which is why the pattern requires arguments that start with a path or a
    flag rather than matching the bare word.
    """
    found = []
    for job, step, lines in workflow_steps():
        for line in lines:
            match = _PYTEST_INVOCATION.search(line.strip())
            if match and not line.strip().startswith(("pip ", "python -m pip")):
                args = match.group("args")
                if args.startswith(("tests", "-", "--")):
                    found.append((job, step, args))
    assert found, f"no pytest invocations found in {WORKFLOW}"
    return tuple(found)


# ---------------------------------------------------------------------------
# Rule 1 -- every integration suite is invoked by CI, or waived.
# ---------------------------------------------------------------------------
CI_INVOCATION_WAIVERS = ()

_IGNORED_PATH = re.compile(r"--ignore=(\S+)")


def test_every_ignored_path_is_invoked_by_another_step_in_the_same_job():
    """A suite carved out of one invocation must be run by another one.

    **Rule 1 is directory-grained, and this is the hole that leaves.** It reads
    ``argument.split()[0]``, so a step invoking ``tests/integration/dbt/``
    satisfies it for every file in that directory -- including files that step
    explicitly ``--ignore``s. Delete the separate step that was supposed to run
    them and everything stays green while the gate silently stops running,
    which is precisely the failure rule 1 exists to prevent, reappearing one
    level down.

    It is not hypothetical. Plan 162 Stage S carved two suites out of
    ``tests/integration/dbt/`` because neither can share a process with the
    in-process dbt the other suites there use: ``test_branch_coverage.py`` for
    ``dbt compile`` overwriting ``target/`` under its neighbours, and
    ``test_constraint_mutation.py`` because ``attached_warehouse()`` opens the
    warehouse under a configuration DuckDB refuses while dbt-duckdb holds it.
    Both are load-bearing gates; both were, for one commit, deletable without
    turning anything red.

    The check is per job rather than per workflow deliberately. These suites
    need what their job built -- a warehouse, a compiled project -- so a step in
    some other job would satisfy a global check while running against a
    warehouse that does not exist.
    """
    ignored: dict[tuple[str, str], set[str]] = {}
    invoked: dict[str, set[str]] = {}
    for job, step, argument in pytest_steps():
        parts = argument.split()
        invoked.setdefault(job, set()).update(
            part for part in parts if part.startswith("tests")
        )
        for path in _IGNORED_PATH.findall(argument):
            ignored.setdefault((job, step), set()).add(path)

    orphaned = sorted(
        f"{job}: `{step}` ignores {path}, and no other step in that job runs it"
        for (job, step), paths in ignored.items()
        for path in paths
        if path not in invoked.get(job, set())
    )
    assert not orphaned, (
        "these paths are excluded from one pytest invocation and invoked by no "
        "other step in the same job, so the tests in them run nowhere -- and "
        "the rule above still passes, because it reads the directory the "
        "invocation names rather than the files it actually runs:\n  "
        + "\n  ".join(orphaned)
    )


@dataclass(frozen=True)
class Dormant:
    """One suite that is deliberately not run, and the reason it is not.

    Not a :class:`Waiver`, and the difference is the point. A waiver is debt: it
    names a violation, an owner plan, and dies when that plan archives -- which
    is exactly what ``test_no_waiver_outlives_the_plan_that_owns_it`` enforces.
    Dormancy is a decision with no repair pending and no plan to outlive. Held
    as a waiver, ``tests/integration/lakehouse/`` would have failed the moment
    Plan 162 archived, and the only way to quiet it would have been to delete
    the record of why the suite is not running -- losing precisely the fact G2
    asked to be written down.

    There is no ``gap`` field, deliberately. G2 asked for the declaration and
    was deleted once it had one; a pointer to it would dangle, and the contract
    already settled where that kind of history belongs -- the plan documents,
    not a cell here. ``reason`` carries what a reader actually needs.
    """

    subject: str
    reason: str
    since: date = MEASURED


DORMANT_SUITES = (
    Dormant(
        "tests/integration/lakehouse",
        reason=(
            "Plan 125 pulled the `lakehouse` job in 863a2f2 rather than patch "
            "its fixture problem. The 7 tests need Lakekeeper and PySpark "
            "services this workflow does not start; they are kept, not run, "
            "until Plan 125 Gate C brings the stack back."
        ),
    ),
)


def test_every_integration_suite_is_invoked_by_a_ci_step():
    """G1 is the reason this file exists at all.

    73 integration-marked tests in 11 files were written, reviewed, merged and
    maintained while no CI step ran them, and ``tests/integration/processing/``
    -- 58 of them -- had never appeared in ``ci.yml`` in its history. Nothing
    failed. No mechanism existed that could notice.

    Plan 162 Stage B ran them. 66 of the 73 passed; the 7 that did not were two
    defects in the tests themselves, both of the kind only running finds -- a
    cleanup naming a table that no migration has ever created, and a fixture
    seeding a timestamp that made the behaviour under test a no-op. Both are
    fixed and all three suites now have named steps, which is why
    :data:`CI_INVOCATION_WAIVERS` is empty and stays that way: an empty tuple
    still fails ``_assert_exactly`` the moment a new suite appears unrun.

    The remaining suite is dormant rather than orphaned, and says so through
    :data:`DORMANT_SUITES` rather than a waiver -- see :class:`Dormant` for why
    the distinction has to be structural.
    """
    invoked = {
        argument.split()[0].rstrip("/")
        for _, _, argument in pytest_steps()
        if argument.startswith("tests/integration/")
    }
    suites = {
        _relative(directory)
        for directory in _test_directories()
        if _relative(directory).startswith("tests/integration/")
    }
    dormant = {entry.subject for entry in DORMANT_SUITES}
    _assert_exactly(
        suites - invoked - dormant,
        CI_INVOCATION_WAIVERS,
        "Every tests/integration/<dir> is invoked by a named CI step, or is "
        "declared in DORMANT_SUITES with the reason it is not run "
        "(docs/TESTING.md, 'What CI asserts').",
    )


def test_no_dormant_suite_is_quietly_running():
    """The other direction, without which dormancy is just an unread comment.

    A suite that gets a CI step later and keeps its :class:`Dormant` entry
    would leave the contract asserting a reason that stopped being true --
    which is the failure mode this whole file exists to make impossible.
    """
    invoked = {
        argument.split()[0].rstrip("/")
        for _, _, argument in pytest_steps()
        if argument.startswith("tests/integration/")
    }
    contradicted = sorted(
        f"{entry.subject} (declared dormant {entry.since}: {entry.reason})"
        for entry in DORMANT_SUITES
        if entry.subject in invoked
    )
    assert not contradicted, (
        "these suites are declared dormant and are invoked by a named CI step "
        "anyway; delete the DORMANT_SUITES entry:\n  " + "\n  ".join(contradicted)
    )


# ---------------------------------------------------------------------------
# Rule 2 -- patching is ``mocker``.
# ---------------------------------------------------------------------------
# Empty since Plan 162 Stage F (CAR-49) converted all 34 on 2026-09-01 -- the
# 17 that imported ``unittest.mock.patch``, the 17 that reached for
# ``monkeypatch.setattr``, and the two that did both. The venv carve-out this
# rule's docstring argues against went with them: ``ci.yml`` now installs
# ``pytest-mock`` into the isolated Airflow venv, so ``mocker`` exists in every
# interpreter that runs a test here.
MOCKER_WAIVERS: tuple[Waiver, ...] = ()

_MONKEYPATCH_ALLOWED = frozenset({
    # monkeypatch owns process state and mocker is the wrong tool for it.
    "setenv", "delenv", "setitem", "delitem", "chdir", "syspath_prepend",
    "context",
})


def _patching_mechanisms(path: Path) -> set[str]:
    """Every non-``mocker`` patching mechanism *path* uses.

    ``from unittest.mock import MagicMock`` is not one of them and never was:
    it is a value constructor, not a patching mechanism, and 37 files import it
    legitimately. The violation is ``patch``.
    """
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    reasons: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "unittest.mock":
            if any(alias.name == "patch" for alias in node.names):
                reasons.add("unittest.mock.patch")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in {"unittest", "unittest.mock"}:
                    reasons.add(f"import {alias.name}")
        elif isinstance(node, ast.Attribute):
            target = node.value
            if (
                isinstance(target, ast.Name)
                and target.id == "monkeypatch"
                and node.attr not in _MONKEYPATCH_ALLOWED
            ):
                reasons.add(f"monkeypatch.{node.attr}")
    return reasons


def test_patching_is_mocker_everywhere():
    """One convention, and no venv carve-out.

    ``tests/integration/airflow/`` was the interesting pair. Its venv installed
    ``apache-airflow``, ``pytest``, ``psycopg2-binary`` and ``requests``, so
    ``mocker`` genuinely did not exist in that interpreter -- and that was an
    argument that the venv was built wrong, not that the convention forks.
    ``pytest-mock`` depends only on ``pytest``, which that venv already had, so
    Stage 5 added it to the ``pip install`` and converted both files rather
    than granting an exemption. That is what a waiver is for: it is how a
    defect waits its turn, and an exemption says the code is correct.
    """
    offenders = {
        _relative(path)
        for path in sorted(TESTS_DIR.rglob("*.py"))
        if _patching_mechanisms(path)
    }
    _assert_exactly(
        offenders,
        MOCKER_WAIVERS,
        "Patching is mocker (pytest-mock) everywhere. unittest.mock.patch and "
        "monkeypatch.setattr are the violations; `from unittest.mock import "
        "MagicMock` is not, and monkeypatch still owns process state "
        "(setenv/delenv/setitem/chdir).",
    )


# ---------------------------------------------------------------------------
# Rule 3 -- every route is reached through the app's routing table.
# ---------------------------------------------------------------------------
# Emptied by Stage 6 (CAR-50) on 2026-09-01, and it stays empty: an empty
# tuple still fails `_assert_exactly` the moment a route appears unreached.
#
# The twelve did not all mean the same thing, which is the finding worth
# keeping. Four `container_health` routes were a real gap -- the service had no
# `TestClient` anywhere and no test directory to put one in, which is why G6
# and G9 were one stage. Three more were real: `/coordination/status` and the
# two `/maintenance` routes were exercised only by calling their helpers.
#
# **The other five were never uncovered.** The three
# `/admin/snapshots/adaptive-refresh/` reads and the two safe-lifecycle
# coordination routes had tests going through the routing table and asserting
# status codes the whole time -- 200, 409 and 503 among them. The rule could
# not see them: it read only `ast.Constant` first arguments, so
# `f"{BASE}/latest"` and a `parametrize`-injected `path` both looked like no
# request at all. Stage 6 widened how the argument is read rather than
# rewriting five sound tests to suit the instrument, because the second option
# leaves the next f-string silently uncounted.
ROUTE_WAIVERS: tuple[Waiver, ...] = ()

# Run in a subprocess, one service at a time. Importing six FastAPI apps into
# this interpreter would register six sets of Prometheus collectors in one
# registry and put scraper/ on sys.path for everything downstream -- which is
# the harness deciding another test's outcome, the rule two sections below.
_HTTP_VERBS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})

# The enumerator is ``app.openapi()``, not a walk of ``app.routes``, and the
# difference is not cosmetic -- it is this rule's own worked example of the
# environment deciding the outcome.
#
# Up to FastAPI 0.128, ``include_router`` flattened a router's routes into
# ``app.routes`` with the prefix already applied, so a shallow walk saw all of
# them. By 0.141 it appends a single ``_IncludedRouter`` wrapper instead, which
# exposes neither ``routes`` nor ``prefix`` and resolves its children at match
# time. The walk still succeeds and still returns routes -- just four of them
# for ``ops`` instead of 54. Every requirements file here pins nothing, so the
# first CI run had 0.141 while this machine had 0.128, and the rule quietly
# stopped checking 50 routes without failing.
#
# ``openapi()`` is public, stable across both, applies prefixes, and drops the
# framework's own ``/docs``, ``/redoc`` and ``/openapi.json`` on its own, so the
# endpoint-module filter that used to do that by hand is gone with it.
_ROUTE_PROBE = """
import importlib, json, os, sys, tempfile
VERBS = {"get", "post", "put", "patch", "delete", "head", "options"}
repo, service = sys.argv[1], sys.argv[2]
os.environ.setdefault("LOG_PATH", os.path.join(tempfile.gettempdir(), "contract.log"))
if len(sys.argv) > 3:
    sys.path.insert(0, os.path.join(repo, service))
sys.path.insert(0, repo)
app = importlib.import_module(service + ".app").app
print(json.dumps(sorted(
    [method.upper(), path]
    for path, operations in app.openapi()["paths"].items()
    for method in operations
    if method.lower() in VERBS
)))
"""


@lru_cache(maxsize=None)
def app_routes(service: str) -> tuple[tuple[str, str], ...]:
    """``(METHOD, path)`` for every route *service* actually serves.

    Two import recipes, tried in order, because production has two: most
    services import as a package from the repo root, and ``scraper`` runs with
    its own directory as the root, so its modules import ``db`` and
    ``processors`` as top-level names. Trying the plain recipe first matters --
    putting ``ops/`` on ``sys.path`` shadows the standard library's ``email``
    with ``ops/email.py`` and the app never imports at all.
    """
    failures = []
    for extra in ([], ["--service-dir"]):
        result = subprocess.run(
            [sys.executable, "-c", _ROUTE_PROBE, str(REPO_ROOT), service] + extra,
            capture_output=True, text=True,
            encoding="utf-8",
        )
        if result.returncode == 0:
            found = tuple(
                (method, path) for method, path in json.loads(result.stdout)
            )
            # An empty table would satisfy every route rule by having nothing
            # to check, which is G1's failure in miniature: the rule passes
            # because nothing ran, not because everything is covered. A service
            # whose app.py constructs a FastAPI() serves at least one route.
            assert found, (
                f"{service}'s app imported but exposed no routes of its own. "
                f"Nothing can be proved about a service whose routing table is "
                f"empty, so this fails rather than passing vacuously."
            )
            return found
        failures.append(result.stderr.strip()[-600:])
    raise AssertionError(
        f"{service}'s routing table could not be loaded, so no test can prove "
        f"its routes exist. This is a failure, not a skip.\n\n"
        + "\n\n---\n\n".join(failures)
    )


def _module_constants(tree: ast.Module) -> dict[str, str]:
    """``NAME = "/some/prefix"`` at module scope, which tests build paths from."""
    constants = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        constants[target.id] = node.value.value
    return constants


def _parametrized_strings(function: ast.FunctionDef) -> dict[str, set[str]]:
    """Argument name -> the string values ``parametrize`` will inject.

    Needed because ``mock_client.post(path)`` inside a parametrized test is a
    real request through the routing table, and the path is a ``Name``. Reading
    only the call site sees a variable and concludes the route is untested,
    which is how four exemplary coordination tests came to sit under a waiver.
    """
    injected: dict[str, set[str]] = {}
    for decorator in function.decorator_list:
        if not (
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr == "parametrize"
            and len(decorator.args) >= 2
        ):
            continue
        names_node, values_node = decorator.args[0], decorator.args[1]
        if isinstance(names_node, ast.Constant) and isinstance(names_node.value, str):
            names = [part.strip() for part in names_node.value.split(",")]
        elif isinstance(names_node, (ast.Tuple, ast.List)):
            names = [
                element.value
                for element in names_node.elts
                if isinstance(element, ast.Constant) and isinstance(element.value, str)
            ]
        else:
            continue
        if not isinstance(values_node, (ast.List, ast.Tuple)):
            continue
        for row in values_node.elts:
            cells = row.elts if isinstance(row, (ast.Tuple, ast.List)) else [row]
            for name, cell in zip(names, cells):
                if isinstance(cell, ast.Constant) and isinstance(cell.value, str):
                    injected.setdefault(name, set()).add(cell.value)
    return injected


def _resolve_path(
    node: ast.AST, constants: dict[str, str], injected: dict[str, set[str]]
) -> set[str]:
    """Every string *node* can be at runtime, as far as reading can tell.

    Three forms beyond a bare literal, each of which the repository actually
    uses and each of which was silently uncounted before Stage 6:
    ``f"{BASE}/latest"``, ``BASE + "/latest"``, and a parametrized argument.
    An unresolvable expression yields nothing rather than a guess -- the rule
    must keep failing closed, because "named somewhere in tests/" is the weak
    reading docs/TESTING.md explicitly rejects.
    """
    if isinstance(node, ast.Constant):
        return {node.value} if isinstance(node.value, str) else set()
    if isinstance(node, ast.Name):
        if node.id in constants:
            return {constants[node.id]}
        return set(injected.get(node.id, ()))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _resolve_path(node.left, constants, injected)
        right = _resolve_path(node.right, constants, injected)
        return {a + b for a in left for b in right} if left and right else set()
    if isinstance(node, ast.JoinedStr):
        combined = {""}
        for part in node.values:
            if isinstance(part, ast.FormattedValue):
                pieces = _resolve_path(part.value, constants, injected)
            else:
                pieces = _resolve_path(part, constants, injected)
            if not pieces:
                return set()
            combined = {prefix + piece for prefix in combined for piece in pieces}
        return combined
    return set()


def _requested_routes(directories: list[Path]) -> set[tuple[str, str]]:
    """``(METHOD, path)`` the tests under *directories* request.

    Still only counts a path that reaches an HTTP verb call -- the rule is
    "reached through the routing table", and loosening it to any path-shaped
    literal would re-adopt the weakest reading on purpose. What Stage 6 widened
    is how the *argument* is read, not what counts as a request.
    """
    hits: set[tuple[str, str]] = set()
    for directory in directories:
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            constants = _module_constants(tree)
            scopes: list[tuple[ast.AST, dict[str, set[str]]]] = []
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    scopes.append((node, _parametrized_strings(node)))
            for function, injected in scopes + [(tree, {})]:
                for node in ast.walk(function):
                    if not (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr in _HTTP_VERBS
                        and node.args
                    ):
                        continue
                    for value in _resolve_path(node.args[0], constants, injected):
                        if value.startswith("/"):
                            hits.add((node.func.attr.upper(), value.split("?")[0]))
    return hits


def _matches(route: str, requested: str) -> bool:
    """``/project-status/{project}`` is reached by ``/project-status/acme``."""
    pattern = re.sub(r"\\\{[^}]+\\\}", "[^/]+", re.escape(route))
    return re.fullmatch(pattern, requested) is not None


def test_no_route_is_hidden_from_the_schema_this_rule_reads():
    """``include_in_schema=False`` would make a route invisible to the rule above.

    This is the price of enumerating from ``openapi()`` instead of walking
    ``app.routes``, and it is worth paying only while it costs nothing: no
    service uses the flag today. A route that opted out would be a route the
    contract requires a test for and this file cannot see -- silently, which is
    the one outcome the whole document is written against.

    If a route ever genuinely needs to be hidden, the answer is a different
    enumerator, not a quiet exemption. Failing here is how that conversation
    gets started rather than missed.
    """
    hidden = sorted(
        f"{_relative(path)}:{number}"
        for service in service_packages()
        for path in sorted((REPO_ROOT / service).rglob("*.py"))
        for number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        )
        if "include_in_schema" in line and "False" in line
    )
    assert not hidden, (
        "these routes are hidden from the OpenAPI schema, which is what "
        "test_every_route_is_reached_through_the_apps_routing_table reads:\n  "
        + "\n  ".join(hidden)
        + "\n\nA hidden route cannot be checked by that rule. Change the "
        "enumerator rather than accepting the blind spot."
    )


def test_every_route_is_reached_through_the_apps_routing_table():
    """Calling the handler proves the function works and nothing else.

    It proves nothing about the URL, the method, the prefix, or whether the
    router was included at all -- which is why ``container_health`` could 404
    for eleven hours with its endpoints under test the whole time.

    A test is attributed to a service by the directory it lives in, because
    ``GET /health`` exists six times and a request literal does not say whose
    it was. That attribution is why G6 and G9 were one stage: while
    ``test_container_health_app.py`` sat at the top level it could not have
    counted for ``container_health`` even once it grew a ``TestClient``, so
    the misfiling and the missing coverage were the same mistake seen from two
    sides. Both closed in Stage 6 -- the file now lives in
    ``tests/container_health/`` and the routes are reached from
    ``tests/integration/container_health/``.
    """
    uncovered = set()
    census = []
    for service in sorted(service_packages()):
        entrypoint = REPO_ROOT / service / "app.py"
        # ``dashboard`` has an ``app.py`` and no routing table: Streamlit owns
        # its URLs. What it owes instead is in the "enough" table, as G7.
        if not entrypoint.exists() or "FastAPI(" not in entrypoint.read_text(
            encoding="utf-8"
        ):
            census.append(f"{service}: skipped, no FastAPI app")
            continue
        requested = _requested_routes([
            TESTS_DIR / service,
            TESTS_DIR / "integration" / service,
        ])
        routes = app_routes(service)
        for method, path in routes:
            if not any(
                verb == method and _matches(path, target)
                for verb, target in requested
            ):
                uncovered.add(f"{service}: {method} {path}")
        census.append(
            f"{service}: {len(routes)} routes, {len(requested)} request literals"
        )

    # The census rides along in the failure message on purpose. A stale route
    # waiver is the hardest failure in this file to read without it -- the
    # message says a route is no longer uncovered, and the two ways that can
    # happen (someone tested it, or it stopped being enumerated) want opposite
    # responses. The counts tell them apart in one line.
    _assert_exactly(
        uncovered,
        ROUTE_WAIVERS,
        "Every route is reached through the app's routing table by at least "
        "one test in that service's own test directory. Health and readiness "
        "endpoints are not exempt -- they are what another service's drain "
        "logic reads.\n\nWhat was enumerated:\n  " + "\n  ".join(census),
    )


# ---------------------------------------------------------------------------
# Rule 4 -- every service has a row in the "enough" table.
# ---------------------------------------------------------------------------
_ENOUGH_ROW = re.compile(r"^\| `([a-z_]+)` \| ", re.M)


def test_every_service_directory_has_a_row_in_the_enough_table():
    """A new service with no row is a violation, not an omission from a list.

    Only the rows are asserted, not their numbers. The ``Src``/``Layer 1``/
    ``Layer 4`` counts are a dated measurement of how far each service sits
    from the floor; asserting them would fail on every file added and teach
    everyone to stop reading the table.
    """
    section = _read(CONTRACT).split('## What "enough" means, per service')[1]
    documented = set(_ENOUGH_ROW.findall(section.split("\n## ")[0]))
    assert documented, (
        f'{CONTRACT} has no parseable rows under \'What "enough" means, per '
        f"service'"
    )
    missing = sorted(service_packages() - documented)
    assert not missing, (
        f"service directories with no row in the \"enough\" table of "
        f"{CONTRACT}: {missing}. A service that ships without one has never "
        f"had anyone say what it owes."
    )
    phantom = sorted(documented - service_packages())
    assert not phantom, (
        f'the "enough" table in {CONTRACT} has rows for directories that do '
        f"not exist: {phantom}"
    )


# ---------------------------------------------------------------------------
# Rule 5 -- every .sql file is executed by a Layer 2 test.
# ---------------------------------------------------------------------------
# Empty since Plan 162 Stage L (CAR-51). Every production .sql file is
# executed by a Layer 2 test that imports the constant production imports.
#
# The last entry came off by deletion rather than by a test, which is allowed
# only because the statement that absorbed it can be named -- G16's rule, and
# the first case to exercise it. processing/sql/get_active_search_configs.sql
# read `params -> 'makes'` and `params -> 'models'` out of search_configs jsonb
# for carousel make/model filtering. That filtering still happens, in
# detail_writer._get_tracked_models() under a section header that still says
# "Carousel search_config filtering", but it reads ops.tracked_models joined to
# enabled search_configs instead -- a normalised (search_key, make, model)
# grain rather than paired jsonb arrays. Same question, same consumer, same
# `enabled = true` gate, different source. The pre-normalisation version had
# been dead since Plan 93 shipped it: no constant loaded it and `git log -S`
# found only the waiver naming it.
LAYER_2_WAIVERS: tuple[Waiver, ...] = ()

_SQL_SUITE_ROW = re.compile(r"^\| `(tests/integration/[^`]+)` \| ", re.M)


@lru_cache(maxsize=None)
def sql_executing_suites() -> frozenset[str]:
    """The suites the contract declares as executing production SQL.

    Read from ``docs/TESTING.md``'s Layer 2 section rather than listed here,
    for the reason every other derivation in this file gives: a list in the
    checker is a list nobody reviews. Adding a suite is an edit to the document
    that has to say *why* the suite executes production SQL, in the row itself.

    **This started as ``tests/integration/sql/`` alone and that was too narrow.**
    18 of the ``.sql`` files this rule reported as uncovered -- every
    ``archiver/sql/lake_snapshot_selectors/`` file -- are executed in CI against
    real Parquet in MinIO by ``tests/integration/archiver/``, which the rule
    could not see. That is a blind spot in the ruler, not a gap in the work,
    and it is the sharpest argument against location as a proxy for coverage:
    the tests were stronger than this check's own reading and it called them
    absent.

    **It is deliberately not a glob.** Measured 2026-09-01, reading all of
    ``tests/integration/`` would have credited 35 of 46 files on a name match
    alone, including matches from suites that mention a statement without
    executing it. The check reads a filename stem out of a test's *text*; it
    cannot tell execution from mention, so what it reads is a decision.
    """
    section = _read(CONTRACT).split("### Layer 2 — SQL smoke tests")[1]
    section = section.split("### Layer 3")[0]
    suites = {row.rstrip("/") for row in _SQL_SUITE_ROW.findall(section)}
    assert suites, (
        f"{CONTRACT}'s Layer 2 section no longer declares which suites execute "
        f"production SQL. See 'Executes production SQL from these suites.'"
    )
    missing = sorted(s for s in suites if not (REPO_ROOT / s).is_dir())
    assert not missing, (
        f"{CONTRACT} declares suites that do not exist: {missing}"
    )
    return frozenset(suites)


# Flyway owns db/migrations/ and dbt owns its models; both are named exemptions
# in the contract. tests/ holds fixture seeds, which are not production SQL.
_SQL_EXEMPT_ROOTS = ("db/migrations/", "dbt/", "tests/")

# Not exemptions -- these are not the repository. ``.claude/worktrees/`` is the
# one that matters and is easy to miss: a worktree is a full second checkout
# *inside* the tree, so a walk that descends into it finds every file twice and
# the count depends on how many branches someone happens to have open. That is
# the harness deciding the outcome, in a file that asserts it must not.
_NOT_THE_REPOSITORY = (".claude/", ".venv/", ".git/", "target/", "__pycache__/")


#: The floor under the production corpus, and the number is deliberately far
#: below the 161 it actually holds. A floor's job is to catch a corpus that
#: collapsed -- a broken glob, an exemption that swallowed a service -- not to
#: be a second count that has to be edited every time a statement lands.
_SQL_CORPUS_FLOOR = 100

_EXEMPT_ROOT_ROW = re.compile(r"^\| `([^`]+)` \| ", re.MULTILINE)


@lru_cache(maxsize=None)
def declared_sql_exemptions() -> frozenset[str]:
    """The roots the contract says are not production SQL, from its own table."""
    section = _read(CONTRACT).split(
        "**Which directories the production corpus excludes.**"
    )[1].split("\n\n**")[0]
    declared = frozenset(_EXEMPT_ROOT_ROW.findall(section))
    assert declared, (
        f"{CONTRACT}'s 'Which directories the production corpus excludes' "
        f"table no longer parses into rows"
    )
    return declared


def test_every_sql_corpus_exemption_is_declared():
    """Both directions, and the stale one is the reason this exists.

    ``production_sql_files()`` is the denominator of every coverage number in
    this contract, and ``_SQL_EXEMPT_ROOTS`` is the only thing deciding what it
    counts. Widening that tuple is the one edit that makes a gate read 100% by
    counting less: adding ``dashboard/`` drops 24 files and the execution gate
    still reports every file it counted as executing. Nothing noticed that
    until Plan 162 Stage X, which is late for a rule the whole stage's headline
    number rests on.

    A mechanical rule cannot judge whether an exemption is *legitimate* -- that
    is review's job. What it can do is make the edit visible: an exemption now
    costs a row in the contract, and a row costs a diff someone reads.
    """
    declared = declared_sql_exemptions()
    exempt = set(_SQL_EXEMPT_ROOTS)

    assert not exempt - declared, (
        f"these roots are exempt from production_sql_files() but not declared "
        f"in {CONTRACT}'s 'Which directories the production corpus excludes': "
        f"{sorted(exempt - declared)}. Every exemption shrinks the denominator "
        f"of every coverage number here, so it owes a row saying why."
    )
    assert not declared - exempt, (
        f"{CONTRACT} declares these roots excluded from the production SQL "
        f"corpus, but _SQL_EXEMPT_ROOTS does not exempt them: "
        f"{sorted(declared - exempt)}. The table has gone stale, which makes it "
        f"a description of a rule nobody is running."
    )

    empty = sorted(
        root for root in exempt
        if not any((REPO_ROOT / root.rstrip("/")).rglob("*.sql"))
    )
    assert not empty, (
        f"these roots are exempt but hold no .sql file: {empty}. An exemption "
        f"for a directory that no longer has SQL in it is a hole standing open "
        f"for whatever lands there next."
    )


def test_the_production_sql_corpus_is_not_empty():
    """A set difference over an empty corpus is empty, and empty passes.

    ``test_there_is_something_to_check`` puts exactly this guard on the *test*
    corpus, for exactly this reason, and the production side never had one --
    so a broken glob or an exemption that swallowed a service would have the
    execution gate print ``0 of 0`` and exit green. The floor is low on
    purpose: it catches a collapse, not a statement.
    """
    corpus = production_sql_files()
    assert len(corpus) > _SQL_CORPUS_FLOOR, (
        f"only {len(corpus)} production .sql files found, below the floor of "
        f"{_SQL_CORPUS_FLOOR}. The tree moved, the glob broke, or an entry in "
        f"_SQL_EXEMPT_ROOTS is swallowing a service -- and every coverage "
        f"number in this contract is a fraction of this number."
    )


@lru_cache(maxsize=None)
def production_sql_files() -> tuple[str, ...]:
    return tuple(
        relative
        for relative in sorted(
            path.relative_to(REPO_ROOT).as_posix()
            for path in REPO_ROOT.rglob("*.sql")
        )
        if not relative.startswith(_SQL_EXEMPT_ROOTS + _NOT_THE_REPOSITORY)
    )


#: Every production ``.sql`` file, one line each, generated by calling
#: :func:`production_sql_files` and never hand-typed. 163 paths on
#: 2026-09-06.
#:
#: ``_SQL_CORPUS_FLOOR`` is not this. The floor is deliberately loose and
#: catches a corpus that *collapsed*; it says nothing when one file leaves.
#: This constant is the other half: it names the population, so the diff a
#: reviewer reads shows exactly which statement went away.
PRODUCTION_SQL_MANIFEST: tuple[str, ...] = (
    "airflow/sql/delete_stale_emails.sql",
    "airflow/sql/deploy_intent_gate.sql",
    "airflow/sql/record_gate_observation.sql",
    "archiver/sql/delete_cleanup_candidates.sql",
    "archiver/sql/delete_silver_observations_up_to_id.sql",
    "archiver/sql/delete_staging_rows_up_to_pk.sql",
    "archiver/sql/get_queue_cleanup_candidates.sql",
    "archiver/sql/lake_snapshot/select_artifact_ids.sql",
    "archiver/sql/lake_snapshot/select_filtered_table_rows.sql",
    "archiver/sql/lake_snapshot/select_listing_ids_for_vins.sql",
    "archiver/sql/lake_snapshot/select_previous_listing_ids.sql",
    "archiver/sql/lake_snapshot/select_row_keys_for_candidates.sql",
    "archiver/sql/lake_snapshot/select_seed_vins_by_hash.sql",
    "archiver/sql/lake_snapshot/select_source_table_stats.sql",
    "archiver/sql/lake_snapshot/select_vins_for_listing_ids.sql",
    "archiver/sql/lake_snapshot/select_vins_ranked_within_make_model.sql",
    "archiver/sql/lake_snapshot/wrap_aggregate_query.sql",
    "archiver/sql/lake_snapshot/wrap_candidate_query.sql",
    "archiver/sql/lake_snapshot_selectors/active_to_unlisted.sql",
    "archiver/sql/lake_snapshot_selectors/benchmark_dense_make_model.sql",
    "archiver/sql/lake_snapshot_selectors/benchmark_sparse_make_model.sql",
    "archiver/sql/lake_snapshot_selectors/carousel_only_or_low_priority.sql",
    "archiver/sql/lake_snapshot_selectors/cooldown_events.sql",
    "archiver/sql/lake_snapshot_selectors/detail_beats_srp.sql",
    "archiver/sql/lake_snapshot_selectors/fresh_recent_listing.sql",
    "archiver/sql/lake_snapshot_selectors/invalid_or_null_vin.sql",
    "archiver/sql/lake_snapshot_selectors/no_price_history.sql",
    "archiver/sql/lake_snapshot_selectors/price_changed_30d_only.sql",
    "archiver/sql/lake_snapshot_selectors/price_changed_7d.sql",
    "archiver/sql/lake_snapshot_selectors/price_drop.sql",
    "archiver/sql/lake_snapshot_selectors/price_increase.sql",
    "archiver/sql/lake_snapshot_selectors/relisted_vin.sql",
    "archiver/sql/lake_snapshot_selectors/srp_fallback.sql",
    "archiver/sql/lake_snapshot_selectors/stable_state_run.sql",
    "archiver/sql/lake_snapshot_selectors/stale_listing.sql",
    "archiver/sql/lake_snapshot_selectors/state_change_run.sql",
    "archiver/sql/select_max_silver_observation_id.sql",
    "archiver/sql/select_silver_observations_up_to_id.sql",
    "archiver/sql/select_staging_max_pk.sql",
    "archiver/sql/select_staging_rows_up_to_pk.sql",
    "dashboard/sql/data_health_batch_outcomes.sql",
    "dashboard/sql/data_health_block_rate.sql",
    "dashboard/sql/data_health_cooldown_cohorts.sql",
    "dashboard/sql/data_health_inventory_coverage.sql",
    "dashboard/sql/data_health_price_freshness.sql",
    "dashboard/sql/data_health_scrape_volume.sql",
    "dashboard/sql/deals_days_on_market.sql",
    "dashboard/sql/deals_makes.sql",
    "dashboard/sql/deals_price_drops.sql",
    "dashboard/sql/deals_price_vs_msrp.sql",
    "dashboard/sql/deals_table.sql",
    "dashboard/sql/deals_tier_distribution.sql",
    "dashboard/sql/inventory_active_count.sql",
    "dashboard/sql/inventory_by_make_model.sql",
    "dashboard/sql/inventory_new_24h.sql",
    "dashboard/sql/inventory_new_30d.sql",
    "dashboard/sql/inventory_new_7d.sql",
    "dashboard/sql/inventory_new_over_time.sql",
    "dashboard/sql/inventory_top_dealers.sql",
    "dashboard/sql/inventory_unlisted_over_time.sql",
    "dashboard/sql/market_trends_days_on_market.sql",
    "dashboard/sql/market_trends_national_supply.sql",
    "dashboard/sql/market_trends_price_distribution.sql",
    "dashboard/sql/mart_freshness.sql",
    "dbt_runner/sql/analytics_metrics_snapshot.sql",
    "dbt_runner/sql/public_stats_snapshot.sql",
    "ops/sql/acquire_coordination_lock.sql",
    "ops/sql/advance_coordination_state.sql",
    "ops/sql/approve_access_request.sql",
    "ops/sql/authorize_coordination_state.sql",
    "ops/sql/cancel_coordination_state.sql",
    "ops/sql/claim_detail_scrape_batch.sql",
    "ops/sql/clear_deploy_intent.sql",
    "ops/sql/complete_coordination_state.sql",
    "ops/sql/count_blocked_cooldown_listings.sql",
    "ops/sql/delete_authorized_user.sql",
    "ops/sql/delete_detail_scrape_claims.sql",
    "ops/sql/deny_access_request.sql",
    "ops/sql/evict_delisted_cooldowns.sql",
    "ops/sql/expire_orphan_detail_claims.sql",
    "ops/sql/insert_access_request.sql",
    "ops/sql/insert_blocked_cooldown_events_batch.sql",
    "ops/sql/insert_completion_receipt.sql",
    "ops/sql/insert_coordination_release_evidence.sql",
    "ops/sql/insert_coordination_state_event.sql",
    "ops/sql/insert_machine_token.sql",
    "ops/sql/insert_search_config.sql",
    "ops/sql/mark_rotation_slot_queued.sql",
    "ops/sql/mark_search_config_queued.sql",
    "ops/sql/record_detail_fetches.sql",
    "ops/sql/release_coordination_state.sql",
    "ops/sql/release_deploy_coordination.sql",
    "ops/sql/request_coordination_state.sql",
    "ops/sql/request_deploy_coordination.sql",
    "ops/sql/retire_search_config.sql",
    "ops/sql/select_access_requests.sql",
    "ops/sql/select_active_machine_token_exists.sql",
    "ops/sql/select_airflow_gate_observations.sql",
    "ops/sql/select_airflow_task_instances.sql",
    "ops/sql/select_authorized_users.sql",
    "ops/sql/select_completion_receipt.sql",
    "ops/sql/select_coordination_state.sql",
    "ops/sql/select_coordination_state_actor.sql",
    "ops/sql/select_coordination_state_for_deploy.sql",
    "ops/sql/select_coordination_state_kind.sql",
    "ops/sql/select_coordination_state_metrics.sql",
    "ops/sql/select_deploy_intent_status.sql",
    "ops/sql/select_last_queued_at.sql",
    "ops/sql/select_legacy_search_config.sql",
    "ops/sql/select_live_cooldown_listings.sql",
    "ops/sql/select_machine_token.sql",
    "ops/sql/select_next_rotation_slot.sql",
    "ops/sql/select_pending_cleared_listings.sql",
    "ops/sql/select_pending_request_details.sql",
    "ops/sql/select_pending_request_for_email.sql",
    "ops/sql/select_pending_request_id_for_email.sql",
    "ops/sql/select_pending_request_notification_email.sql",
    "ops/sql/select_processing_artifacts_backlog.sql",
    "ops/sql/select_release_evidence.sql",
    "ops/sql/select_rotation_slot_configs.sql",
    "ops/sql/select_running_detail_claims.sql",
    "ops/sql/select_search_config_by_key.sql",
    "ops/sql/select_search_configs.sql",
    "ops/sql/select_stuck_processing_artifacts.sql",
    "ops/sql/select_user_role.sql",
    "ops/sql/set_deploy_intent.sql",
    "ops/sql/toggle_search_config_enabled.sql",
    "ops/sql/touch_machine_token_last_used.sql",
    "ops/sql/update_search_config.sql",
    "ops/sql/update_user_role.sql",
    "ops/sql/upsert_authorized_user.sql",
    "processing/sql/batch_lookup_vin_to_listing.sql",
    "processing/sql/claim_artifact.sql",
    "processing/sql/claim_artifacts.sql",
    "processing/sql/clear_blocked_cooldown.sql",
    "processing/sql/delete_price_observation.sql",
    "processing/sql/delete_price_observation_by_vin.sql",
    "processing/sql/delete_price_observations_for_missing_listings.sql",
    "processing/sql/get_tracked_models.sql",
    "processing/sql/insert_detail_claim_event.sql",
    "processing/sql/insert_price_observation_event.sql",
    "processing/sql/insert_silver_observations.sql",
    "processing/sql/insert_tracked_model_event.sql",
    "processing/sql/insert_vin_to_listing_event.sql",
    "processing/sql/lookup_vin_collision.sql",
    "processing/sql/release_detail_claims.sql",
    "processing/sql/upsert_price_observation.sql",
    "processing/sql/upsert_tracked_model.sql",
    "processing/sql/upsert_vin_to_listing.sql",
    "scraper/sql/enqueue_detail_artifact.sql",
    "scraper/sql/enqueue_results_artifact.sql",
    "scraper/sql/get_blocked_cooldown_attempts.sql",
    "scraper/sql/insert_blocked_cooldown_event.sql",
    "scraper/sql/insert_detail_artifact_event.sql",
    "scraper/sql/insert_results_artifact_event.sql",
    "scraper/sql/upsert_blocked_cooldown.sql",
    "scripts/sql/insert_fixture_tracked_models.sql",
    "scripts/sql/select_available_capture_months.sql",
    "scripts/sql/select_corpus_sample.sql",
    "scripts/sql/select_enabled_search_keys.sql",
    "shared/sql/insert_artifact_event.sql",
    "shared/sql/insert_blocked_cooldown_cleared_event.sql",
    "shared/sql/insert_compression_dictionary.sql",
    "shared/sql/mark_artifact_status.sql",
    "shared/sql/replace_postgres_snapshot_table.sql",
    "shared/sql/select_compression_dictionary.sql",
    "shared/sql/select_compression_dictionary_registration.sql",
    "shared/sql/select_deploy_intent_pause.sql",
    "shared/sql/select_postgres_snapshot_table.sql",
)


#: The ledger of departures, and it is empty on the day it is written.
#:
#: One entry per ``.sql`` file that left ``production_sql_files()`` because its
#: logic moved into a dbt model, paired with the model that absorbed it. That
#: pairing is the whole rule: ``dbt/`` is a named exemption from the Layer 2
#: census, so a statement moving from ``processing/sql/`` into a mart leaves a
#: counted surface for an uncounted one and every coverage fraction in this
#: file improves for a change that removed no risk. Naming the model does not
#: make the logic covered -- Layer 3 owes that -- but it makes the trade
#: legible, which is the difference between a migration and a silent shrink.
#:
#: **This is deliberately not in :data:`ALL_WAIVERS`, and the omission is a
#: decision rather than an oversight.** A waiver is a draining queue:
#: ``test_no_waiver_outlives_the_plan_that_owns_it`` fails any entry whose
#: owner plan has archived, on the argument that nobody is left to fix it.
#: There is nothing here to fix. An entry is permanent record of a move that
#: already happened, and filing it as a waiver would turn the entire ledger red
#: the day Plan 162 archives -- punishing the repository for having a history.
SQL_ABSORBED_BY_DBT: tuple[tuple[str, str], ...] = ()


def _is_a_dbt_model(model: str) -> bool:
    return any((REPO_ROOT / "dbt" / "models").rglob(f"{model}.sql"))


def test_the_sql_corpus_shrinks_only_by_naming_the_model_that_absorbed_it():
    """G16's own rule, and the failure it prevents is a number improving.

    Every coverage figure this file reports about SQL is a fraction whose
    denominator is ``production_sql_files()``. ``_SQL_CORPUS_FLOOR`` guards the
    denominator against collapsing and nothing guarded it against *eroding*:
    delete one ``.sql`` file and the corpus is 162, every rule that iterates it
    passes, and no assertion anywhere in this suite is worse off. That is the
    same shape as Stage F's substring bug -- the list shrinking for free -- and
    it is worse here, because ``dbt/`` is an exemption. Logic that moves out of
    ``processing/sql/`` and into a mart does not stop running; it stops being
    counted, and the census reads as if a statement were retired.

    So the manifest above is the population, stated once, and this check reads
    it against the live glob in both directions. A departure is legitimate only
    when the change says which dbt model absorbed the statement, and the model
    has to exist -- an entry naming ``mart_something_planned`` records an
    intention, not an absorption, and would waive exactly the case the rule is
    for.

    **There is no ``--update`` flag and no regeneration helper, on purpose.** A
    manifest that rewrites itself from the tree asserts that the tree equals
    itself; the reviewable diff *is* the mechanism, and a one-command refresh
    reduces it to a rubber stamp nobody reads. The messages below print the
    exact lines to add or delete so the edit is mechanical, but a person makes
    it, and that person is the check.
    """
    on_disk = set(production_sql_files())
    manifest = set(PRODUCTION_SQL_MANIFEST)
    absorbed = dict(SQL_ABSORBED_BY_DBT)

    still_here = sorted(path for path, _ in SQL_ABSORBED_BY_DBT if path in on_disk)
    assert not still_here, (
        "these SQL_ABSORBED_BY_DBT entries claim a departure that did not "
        "happen -- the file is still on disk:\n  " + "\n  ".join(still_here) +
        "\n\nDelete the entry and put the path back in "
        "PRODUCTION_SQL_MANIFEST, or the ledger is describing a repository "
        "that does not exist."
    )

    unlisted = sorted(on_disk - manifest)
    assert not unlisted, (
        "these production .sql files are on disk and not in "
        "PRODUCTION_SQL_MANIFEST, so nothing would notice if they left again. "
        "Add these lines to the constant, in sorted order:\n"
        + "\n".join(f'    "{path}",' for path in unlisted)
    )

    departed = sorted(manifest - on_disk)
    unexplained = [path for path in departed if path not in absorbed]
    assert not unexplained, (
        "these .sql files left production_sql_files() and no entry says where "
        "their logic went:\n  " + "\n  ".join(unexplained) + "\n\n"
        "The corpus is the denominator of every SQL coverage number here, and "
        "dbt/ is exempt from the Layer 2 census -- so a statement that moved "
        "into a model has left a counted surface for an uncounted one, and the "
        "count dropped for something that is not a repair. Name the dbt model "
        "that absorbed each one: delete its line from PRODUCTION_SQL_MANIFEST "
        "and add to SQL_ABSORBED_BY_DBT:\n"
        + "\n".join(f'    ("{path}", "<the dbt model>"),' for path in unexplained)
        + "\n\nIf nothing absorbed it -- the statement was dead, or it moved "
        "somewhere that is still counted -- that is a different change and this "
        "ledger is the wrong place for it."
    )

    imagined = sorted(
        f"{path} -> {absorbed[path]}"
        for path in departed
        if not _is_a_dbt_model(absorbed[path])
    )
    assert not imagined, (
        "these SQL_ABSORBED_BY_DBT entries name a model that is not a .sql "
        "file under dbt/models/:\n  " + "\n  ".join(imagined) + "\n\n"
        "A model that does not exist absorbed nothing. Either the name is a "
        "typo, or the entry records an intention and the statement is simply "
        "gone."
    )


def _names(stem: str, text: str) -> bool:
    """Does *text* name *stem*, as a whole word, in either case?

    ``re.escape`` because a stem is a filename and ``\\b`` because a substring
    match credits ``price_drop.sql`` for ``test_price_drops_no_filter``.
    """
    for candidate in (stem, stem.upper()):
        if re.search(rf"\b{re.escape(candidate)}\b", text):
            return True
    return False


def test_every_production_sql_file_is_touched_by_a_layer_2_test():
    """56 of 76 are not, at the *weakest* available reading.

    The reading is deliberately weak: a file counts as covered if some module
    under ``tests/integration/sql/`` so much as names it or its constant. The
    contract is clear that a weak reading is not the rule, and this one is
    chosen anyway for a specific reason -- 56 files fail it. A stricter check
    can only find more, so nothing is being hidden, and tightening it is worth
    doing when the number is small enough for the difference to be legible.
    Plan 162 owns both halves.

    **The match is on a word boundary, and that is not a detail.** Until Stage 5
    it was a bare substring test, which credited a ``.sql`` file whenever its
    stem appeared anywhere in a Layer 2 module -- including inside a longer
    identifier that had nothing to do with it.
    ``lake_snapshot_selectors/price_drop.sql`` was credited by the *test method
    name* ``test_price_drops_no_filter``, ``stale_listing.sql`` by
    ``test_price_stale_listing_is_also_held_by_the_backoff``, and
    ``cooldown_events.sql`` by the table name ``staging.blocked_cooldown_events``.
    None of the three is executed anywhere. A weak reading is a decision; a
    reading weaker than the one described is a checker that reports a number
    nobody chose.

    The failure this catches is already in the tree and is the sharpest example
    of what the contract calls worse than no test: ``test_ops_queries.py`` and
    ``test_processing_queries.py`` are named for the services whose statements
    they are supposed to execute, import nothing from either ``queries.py``,
    and paraphrase the SQL instead. A paraphrase passes forever. It is a copy
    that cannot notice the original changed.
    """
    layer_2 = "\n".join(
        path.read_text(encoding="utf-8")
        for suite in sql_executing_suites()
        for path in sorted((REPO_ROOT / suite).rglob("*.py"))
    )
    assert layer_2.strip(), (
        f"the suites {CONTRACT} declares as executing production SQL hold no "
        f"Python at all: {sorted(sql_executing_suites())}"
    )
    untouched = {
        relative
        for relative in production_sql_files()
        if not _names(Path(relative).stem, layer_2)
    }
    _assert_exactly(
        untouched,
        LAYER_2_WAIVERS,
        "Every production .sql file is executed by a Layer 2 test. Flyway's "
        "migrations and dbt's models are the two exemptions; a statement no "
        "layer ever runs against a real engine is the search_path incident "
        "waiting to happen again.",
    )


# The surface the two SQL rules below read. **Not** :func:`service_packages`,
# which answers a different question.
#
# Stage 7 wrote both rules against ``service_packages()`` and shipped a hole:
# ``airflow/`` and ``scripts/`` hold neither an ``__init__.py`` nor, therefore,
# any rule -- and they held 26 SQL sites, 22 of them in Plan 125's Iceberg and
# Spark scripts, which Gates C and D productionize. "Is this a service" and "is
# this production Python" coincided for the eight packages and stopped
# coinciding exactly at the boundary that mattered.
#
# **The fix is not an ``__init__.py``.** ``service_packages()`` drives seven
# rules -- the layer-home mapping, the hidden-route check, route coverage
# (which imports ``<service>.app``), the "enough" table's rows and the coverage
# ``source`` list. Making ``scripts`` a package would demand an "enough" row
# for something that is not a service and send the route rule looking for
# ``scripts.app``. The contract already says as much in
# ``test_every_service_directory_is_in_the_coverage_source``: scripts and
# airflow/dags "cannot be demanded by the same derivation -- neither is a
# package".
#
# So this is a second derivation, and it is derived rather than listed for the
# same reason as the first: ``scripts/oneoff/`` is excluded because Stage 5b
# declared it spent in the contract's own bucket table, not because a list here
# says so. A new bucket is covered by editing that table.
def production_python_roots() -> tuple[Path, ...]:
    """Every directory holding production Python, for the SQL rules only."""
    roots = [REPO_ROOT / package for package in sorted(service_packages())]
    roots.append(REPO_ROOT / "airflow" / "dags")
    roots += [
        REPO_ROOT / bucket
        for bucket, measured in sorted(script_buckets().items())
        if measured
    ]
    return tuple(root for root in roots if root.is_dir())


def _spent_script_buckets() -> tuple[str, ...]:
    """The buckets the contract declares spent, as posix prefixes."""
    return tuple(
        f"{bucket}/" for bucket, measured in sorted(script_buckets().items())
        if not measured
    )


def production_python_files() -> list[Path]:
    """Every ``.py`` file under :func:`production_python_roots`, spent excluded."""
    seen: dict[str, Path] = {}
    spent = _spent_script_buckets()
    for root in production_python_roots():
        for path in sorted(root.rglob("*.py")):
            relative = path.relative_to(REPO_ROOT).as_posix()
            if "__pycache__" in path.parts or relative.startswith(spent):
                continue
            seen[relative] = path
    return [seen[key] for key in sorted(seen)]


# ---------------------------------------------------------------------------
# Rule 5d -- a Layer 2 test asserts something about the result.
# ---------------------------------------------------------------------------
LAYER_2_ASSERTION_WAIVERS: tuple[Waiver, ...] = ()

# Names that carry an assertion without being an ``assert`` statement. Both are
# deliberate weakenings and both are narrow: ``pytest.raises`` asserts control
# flow, and a helper called ``_assert_columns`` has moved the assertion rather
# than dropped it. Widening this further -- crediting any helper call -- would
# make the rule unfalsifiable, because every test calls something.
_ASSERTING_CONTEXTS = frozenset({"raises", "warns", "deprecated_call"})


def _asserts_on_its_result(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Assert):
            return True
        if isinstance(child, ast.withitem):
            call = child.context_expr
            if isinstance(call, ast.Call):
                name = getattr(call.func, "attr", getattr(call.func, "id", ""))
                if name in _ASSERTING_CONTEXTS:
                    return True
        if isinstance(child, ast.Call):
            name = getattr(child.func, "attr", getattr(child.func, "id", ""))
            if name.lstrip("_").startswith("assert"):
                return True
    return False


def test_no_layer_2_test_executes_a_statement_without_asserting_on_the_result():
    """Layer 2 has two clauses and only the first was ever mechanised.

    The contract says a statement must execute against a real engine **and
    return the columns the caller expects**. ``test_every_production_sql_file_
    is_touched_by_a_layer_2_test`` is the first clause; a test that executes a
    statement and discards the result satisfies it while checking nothing, and
    nothing could tell the difference.

    That is not hypothetical. Until Plan 162 Stage M,
    ``tests/integration/sql/test_dashboard_queries.py`` was 25 tests and zero
    assertions -- the only Layer 2 suite with none -- and the rule this
    docstring belongs to found four more hiding in suites whose *other* tests
    assert: one each in the airflow-DAG and archiver suites and two in
    ``test_ops_queries.py``. A per-suite eye could not have seen those, which
    is the whole argument for a derived rule over a read-through.

    **This checks that an assertion exists, not that it is a good one.**
    Whether an assertion is *meaningful* is one of the four judgements
    ``docs/TESTING.md`` says are not mechanically checkable, and this rule must
    not be read as covering it -- ``assert True`` passes here. The skill owns
    that half and refuses to certify it.

    **Layer 2 only, and the scope is a decision.** The same shape exists
    elsewhere in ``tests/``, but most of it is not the same defect: a Layer 1
    test whose whole point is ``pytest.raises`` asserts perfectly well, and a
    sweep that reported those would be a number nobody chose. Layer 2 is where
    executing *is* the test, so executing and discarding is the failure. The
    layer comes from :func:`_layer_of`, which reads the contract's own
    headings, so a directory that becomes Layer 2 is covered without editing
    this file.
    """
    bare = set()
    for directory in _test_directories():
        if _layer_of(directory) != 2:
            continue
        for path in sorted(directory.glob("test_*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not node.name.startswith("test_"):
                    continue
                if not _asserts_on_its_result(node):
                    bare.add(f"{_relative(path)}::{node.name}")

    _assert_exactly(
        bare,
        LAYER_2_ASSERTION_WAIVERS,
        "A Layer 2 test that executes a statement and asserts nothing about "
        "the result is not a Layer 2 test. The contract's second clause is "
        "that a statement returns the columns the caller expects; assert them, "
        "or assert the rows when the fixture seeds any. "
        "tests/integration/sql/test_dashboard_queries.py is the pattern.",
    )


def test_the_assertionless_rule_sees_a_test_that_only_executes():
    """The rule's own worked example, so it cannot pass by finding nothing.

    An empty result set is what this rule reports on a healthy tree, which is
    exactly the state in which a broken checker and a working one look
    identical. These four shapes are the ones that decide whether it works.
    """
    executes_only = ast.parse(
        "def test_x(cur):\n    cur.execute(SQL)\n    cur.fetchall()\n"
    ).body[0]
    asserts = ast.parse(
        "def test_x(cur):\n    cur.execute(SQL)\n    assert cur.rowcount == 0\n"
    ).body[0]
    raises = ast.parse(
        "def test_x(cur):\n"
        "    with pytest.raises(ProgrammingError):\n        cur.execute(SQL)\n"
    ).body[0]
    delegates = ast.parse(
        "def test_x(cur):\n    _assert_columns(cur, ['a'])\n"
    ).body[0]

    assert not _asserts_on_its_result(executes_only)
    assert _asserts_on_its_result(asserts)
    assert _asserts_on_its_result(raises)
    assert _asserts_on_its_result(delegates)


# ---------------------------------------------------------------------------
# Rule 5e -- no .sql comment contains a parameter placeholder.
# ---------------------------------------------------------------------------
_PLACEHOLDER_IN_COMMENT = re.compile(r"^\s*--.*(%s|%\([a-z_]+\)s)", re.M)


def test_no_sql_comment_contains_a_parameter_placeholder():
    """A failure mode this plan created, found by CI on 2026-09-01.

    psycopg2 counts placeholders across the **whole statement string**, comments
    included. So a comment written to explain a parameter adds one, and the
    caller then passes too few.

    Both instances were written the same day, by the same well-meant impulse.
    ``ops/sql/set_deploy_intent.sql`` explained its ``interval '<placeholder>
    minutes'`` construct by quoting it, which made the statement expect four
    parameters where ``deploy.py`` passes three -- so ``/deploy/start`` raised,
    the router caught it, and returned 503. Seven Layer 4 tests failed on that
    alone, in a code path the extraction was not supposed to touch.
    ``ops/sql/insert_blocked_cooldown_events_batch.sql`` did the same to
    ``execute_values``, which refuses any statement carrying two placeholders
    and had exactly one job.

    **The named form is the one that will get someone later.**
    ``processing/sql/claim_artifacts.sql`` had ``%(limit)s`` in its first
    comment line and worked fine, because a named placeholder resolves from the
    same dict however many times it appears. It is a live trap that happens not
    to have sprung: rename the parameter and the comment raises ``KeyError``
    from a line that is not code. All three are fixed and this rule keeps them
    fixed.

    Worth stating plainly, because it is the argument for the whole exercise
    rather than a footnote to it: **this defect could only be found by
    executing.** Every static rule in this file passed on all three files. The
    statements were correctly extracted, correctly imported and correctly
    named, and two of them were broken.
    """
    found = {
        f"{relative}:{text[:match.start()].count(chr(10)) + 1}"
        for relative in production_sql_files()
        for text in [(REPO_ROOT / relative).read_text(encoding="utf-8")]
        for match in _PLACEHOLDER_IN_COMMENT.finditer(text)
    }
    assert not found, (
        "these .sql comments contain a parameter placeholder, which psycopg2 "
        "counts as part of the statement -- the caller will pass too few "
        "parameters, or a rename will raise KeyError from a comment:\n  "
        + "\n  ".join(sorted(found))
    )


# ---------------------------------------------------------------------------
# Rule 5d -- no statement is filed twice.
# ---------------------------------------------------------------------------
# One pair, and it is a decision rather than an oversight. `cancel` refuses
# anything past 'draining' and the deploy facade releases unconditionally --
# two policies that agree today, enforced in the Python around them rather than
# in the statements, which is why the statements match. Consolidating would
# couple two rules that are allowed to diverge.
DUPLICATE_SQL_WAIVERS: tuple[Waiver, ...] = (
    Waiver(
        "ops/sql/cancel_coordination_state.sql == "
        "ops/sql/release_deploy_coordination.sql",
        gap="G17",
        owner=162,
    ),
)


def _sql_body(relative: str) -> str:
    """A ``.sql`` file's statement, with comments and whitespace normalised."""
    text = (REPO_ROOT / relative).read_text(encoding="utf-8")
    body = "\n".join(
        line for line in text.splitlines() if not line.strip().startswith("--")
    )
    return re.sub(r"\s+", " ", body).strip().rstrip(";")


def test_no_two_production_sql_files_hold_the_same_statement():
    """A statement filed twice is two things to edit and one to forget.

    Found on 2026-09-01, while writing Layer 2 tests for the last of G14:
    ``mark_artifact_status``, ``insert_artifact_event`` and
    ``insert_blocked_cooldown_cleared_event`` each existed **byte-identically**
    under both ``ops/sql/`` and ``processing/sql/``. Both services issue them
    against the same tables, so the schema already coupled the two -- the copies
    decoupled nothing and only made a second place to edit, with nothing to
    notice when one moved. They are now one file each under ``shared/sql/``,
    re-exported by both services' ``queries.py`` so no call site changed.

    **The weak reading made this worse than it looks.** Rule 5 credits a
    ``.sql`` file when a Layer 2 module names its *stem*, and these pairs shared
    one -- so a test of ``processing``'s copy silently credited ``ops``'s. Three
    files were reported covered by a test that never executed them, which is
    the same defect as the paraphrase, arriving through the checker instead of
    the test.

    This rule is cheap and general: it compares every production statement to
    every other, so the next duplicate is a failure rather than a discovery.
    """
    bodies: dict[str, list[str]] = {}
    for relative in production_sql_files():
        bodies.setdefault(_sql_body(relative), []).append(relative)
    found = {
        " == ".join(sorted(group))
        for group in bodies.values()
        if len(group) > 1
    }
    _assert_exactly(
        found,
        DUPLICATE_SQL_WAIVERS,
        "these production .sql files hold the same statement, so one of them "
        "will be edited and the other will not:",
    )


# ---------------------------------------------------------------------------
# Rule 5b -- no production module holds SQL at its .execute() call site.
# ---------------------------------------------------------------------------
# The 66 sites in the eight service packages were seeded and drained on
# 2026-09-01. These 22 are what widening the scan surface to production
# Python exposed the same day -- all under scripts/, and 16 of them in Plan
# 125's Iceberg and Spark tooling, which Gates C and D productionize. They
# were never fixed and never waived; they were out of frame.
INLINE_SQL_WAIVERS: tuple[Waiver, ...] = tuple(
    Waiver(subject, gap="G5", owner=162)
    for subject in (
        "scripts/audit_adaptive_refresh_features.py:123",
        "scripts/audit_adaptive_refresh_features.py:148",
        "scripts/audit_adaptive_refresh_features.py:155",
        "scripts/audit_adaptive_refresh_features.py:164",
        "scripts/audit_adaptive_refresh_features.py:174",
        "scripts/compare_gate_a_parity.py:223",
        "scripts/compare_gate_b_parity.py:595",
        "scripts/export_volatility_features_to_iceberg.py:124",
        "scripts/export_volatility_features_to_iceberg.py:134",
        "scripts/export_volatility_features_to_iceberg.py:156",
        "scripts/preflight_local_lakehouse_snapshot.py:302",
        "scripts/run_dbt_spark.py:158",
        "scripts/spike_iceberg_lakehouse.py:134",
    )
)

# ``_SQL_CALL_NAMES`` lived here until Plan 162 Stage N (2026-09-02) and is
# deliberately not replaced. It was an inventory of database-client method
# names -- execute, executemany, spark.sql, read_sql and their kin -- and the
# rule that read it could be escaped by calling anything the list had not
# heard of, including a helper defined three lines up in the same file.
# Rule 5f below asks what the literal *is* instead, so there is no list to
# keep current. See its comment block for the three sites that proved the
# difference.

# The verb is what makes a generous name set safe. ``df.query("price > 100")``
# and ``resp.text`` reach the walk below and are rejected on content, so adding
# a name costs nothing while omitting one costs silence. It is also the part
# that is dialect-independent: SELECT and INSERT read the same in Spark SQL as
# in Postgres.
_SQL_VERB = re.compile(
    r"\s*(?:--[^\n]*\n\s*)*"
    r"\b(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|DROP|ALTER|TRUNCATE|COPY|CALL|"
    r"GRANT|REVOKE|VACUUM|ANALYZE|EXPLAIN|MERGE|UPSERT|REFRESH|REINDEX|"
    r"INSTALL|LOAD|SET|ATTACH|DETACH|PRAGMA|BEGIN|COMMIT|ROLLBACK)\b",
    re.I,
)

# Connection setup, not a query. ``INSTALL httpfs`` / ``LOAD httpfs`` /
# ``SET s3_url_style=?`` configure a session; they name no table and no column,
# so there is no schema for them to drift from -- which is the whole hazard the
# rule exists to catch. Extracting them into .sql files for a Layer 2 test to
# import would be ceremony, not coverage. Measured on 2026-09-01 this exempts
# exactly the seven sites in ``shared/duckdb_s3.py`` and nothing else; in
# particular no production module runs ``SET search_path`` through a cursor,
# which would be a schema statement wearing this shape and is not exempt.
_SESSION_SETUP_VERBS = frozenset({"INSTALL", "LOAD", "SET", "ATTACH", "DETACH", "PRAGMA"})

# DDL, which Plan 161 question 4 exempts by name: "DDL and one-shot
# maintenance, which Flyway and ``scripts/`` own". That exemption was written
# when nothing scanned ``scripts/`` and became load-bearing on 2026-09-01 when
# something did -- ``CREATE NAMESPACE IF NOT EXISTS`` and ``DROP TABLE IF
# EXISTS`` against a scratch Iceberg namespace are exactly the shape it
# describes.
#
# It is narrower than it looks. A ``.sql`` file earns its keep because a test
# can execute the statement production runs; this DDL creates and tears down
# the very namespace its script is about, so there is no production schema for
# it to drift from -- the same argument as session setup above.
# **Flyway's DDL is not covered by this.** ``db/migrations/`` is exempt one
# level up, in ``_SQL_EXEMPT_ROOTS``, and Flyway applies and checksums it.
_DDL_VERBS = frozenset({"CREATE", "DROP", "ALTER", "TRUNCATE"})

_EXEMPT_VERBS = _SESSION_SETUP_VERBS | _DDL_VERBS


# ---------------------------------------------------------------------------
# Rule 5c -- no production module keeps a SQL statement in a Python literal.
# ---------------------------------------------------------------------------
SQL_LITERAL_WAIVERS: tuple[Waiver, ...] = tuple(
    Waiver(subject, gap="G15", owner=162)
    for subject in (
        "archiver/processors/delete_packed_source_html.py:304",
        "archiver/processors/pack_bronze_html.py:440",
        "scripts/audit_adaptive_refresh_features.py:128",
        "scripts/audit_adaptive_refresh_features.py:136",
        "scripts/compare_gate_b_parity.py:510",
        "scripts/compare_gate_b_parity.py:527",
    )
)


# ---------------------------------------------------------------------------
# Rule 5f -- no production module holds a SQL statement at all.
#
# **This is Rules 5b and 5c, unified, and the unification is the point.** Both
# were keyed on the *shape* holding the statement -- 5b on a call site, 5c on
# an assignment or a return -- and a rule keyed on shape is an enumeration
# wearing a different coat. Stage 7 wrote the lesson down ("a denominator that
# is listed, or scoped to what exists when it is written, will be wrong") and
# then built 5b on ``_SQL_CALL_NAMES``, an inventory of client libraries.
#
# Three sites proved it, found 2026-09-02 by asking the question this rule asks
# instead:
#
# * ``ops/coordination_drain.py`` passes a production SELECT to
#   ``_database_count(...)`` -- a *project-local helper*. No inventory of
#   database libraries can ever contain your own function names, so 5b could
#   not have found this at any list length.
# * ``scripts/compare_gate_b_parity.py`` holds two statements as **dict values**
#   in ``TIE_QUERIES``. Not a call site and not an assignment: a third shape
#   neither rule had.
#
# So this asks only what the literal *is*, never where it sits. The set of ways
# to invoke SQL in Python is open and grows with every library and every helper
# somebody writes; the set of ways to write a string literal is closed. Keying
# on the closed one is the only version that cannot go stale.
#
# **And "not in Python" is exactly "in a .sql file", which is why one rule
# replaces two.** There is nowhere else for a statement to live. Paired with
# Rule 5 -- every .sql file is executed by a Layer 2 test -- the loop closes
# with no judgement in it: a statement cannot be in Python, so it is in a file,
# and the file is executed in CI.
#
# The detection heuristic is not novel and deliberately so. ``flake8-sql``
# independently arrived at the same one -- a string is SQL if it holds
# "select from", "insert into values", "update set" or "delete from" *in
# order*. What no linter ships is this rule: ``flake8-sql`` and ``sql_str_lint``
# style the SQL they find, and ruff's S608 flags only *interpolated* SQL, so it
# passes clean on the correctly-parameterised literal Stage 9 moved out of
# ``sensors.py``. Measured 2026-09-02.
# ---------------------------------------------------------------------------

# A clause keyword, required after the verb. This is SQL grammar, not an
# inventory: new database libraries appear every year, new SQL clauses do not.
# It is what separates a statement from the word -- `{"select": [...]}` in
# `dbt_build.py` and `hourly_analytics_refresh.py` is a dag_run.conf key, and a
# verb-only test reports 99 sites of which 62 are that. With the clause
# required it reports 34, and one of those is argparse help text.
_SQL_CLAUSE = re.compile(
    r"\b(FROM|INTO|SET|VALUES|WHERE|TABLE|VIEW|INDEX|SCHEMA|JOIN|USING)\b", re.I
)


# ``MERGE`` has exactly one form in SQL -- ``MERGE INTO target USING source``
# -- so the ``INTO`` is adjacent to the verb, not merely somewhere after it.
# Requiring that is grammar rather than a carve-out, and it is what stops the
# rule reading git's own vocabulary as SQL: "Merge origin/master into
# a-branch" and "Merge pull request #371 from whitewalls86/some-branch" are
# both a MERGE verb followed at a distance by a clause keyword, and both are
# commit subjects in ``tests/scripts/test_commit_msg_hook.py``'s exemption
# list. Found 2026-09-05 by pointing this rule at ``tests/`` for the first
# time, which is the hostile surface Stage X predicted it would meet.
_MERGE_INTO = re.compile(r"\s*INTO\b", re.I)


# DDL that *materialises a query* is not the DDL ``_DDL_VERBS`` exempts.
# ``CREATE TABLE u AS SELECT ...`` names the columns it reads, so it drifts
# from a schema exactly as a bare ``SELECT`` does -- the exemption's stated
# reason ("there is no production schema for it to drift from") does not reach
# it. Without this, a paraphrase can be written by putting ``CREATE TABLE x AS``
# in front of it, which is a hole in a rule whose whole claim is that it keys
# on the statement rather than its shape.
#
# Measured 2026-09-05 when Stage X first pointed this rule at ``tests/``:
# **zero** production sites and one test site, ``test_analytics_connection_
# guard``'s DuckDB scaffold. So this costs nothing today and closes the hole
# before something walks through it.
_MATERIALISING = re.compile(r"\bAS\s+(?:\(\s*)?(?:WITH|SELECT)\b", re.I)


def _is_sql_statement(text: str) -> bool:
    """Is *text* a SQL statement, judged only on its own content?"""
    match = _SQL_VERB.match(text)
    verb = match.group(1).upper() if match else ""
    if not match:
        return False
    rest = text[match.end():]
    if verb in _EXEMPT_VERBS and not (verb in _DDL_VERBS and _MATERIALISING.search(rest)):
        return False
    if verb == "MERGE" and not _MERGE_INTO.match(rest):
        return False
    return bool(rest.strip()) and bool(_SQL_CLAUSE.search(rest))


def _docstring_ids(tree: ast.AST) -> set[int]:
    """Every docstring node, which is documentation and not a statement.

    ``shared/db.py``'s ``db_cursor`` docstring carries a usage example with a
    real SELECT in it. Excluding it is exact rather than heuristic -- a
    docstring is the first statement of a module, class or function and nothing
    else is -- so this is not a judgement call smuggled into the rule.
    """
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        body = getattr(node, "body", None)
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
           and isinstance(body[0].value.value, str):
            found.add(id(body[0].value))
    return found


def _statement_text(node: ast.AST) -> str | None:
    """The full literal text of *node*, with interpolations reduced to markers.

    **The whole text, not the leading literal**, and the difference is a real
    hole rather than a refinement. Rule 5b's reader returned only the head;
    ``f"SELECT {col} FROM t"`` has ``"SELECT "`` at its head, so a head-only
    reader sees a verb with nothing after it and, under the clause grammar
    above, judges it not a statement. 5b got away with that because it tested
    the verb alone and let the callee's name carry the rest of the decision.
    With the name gone, the grammar has to see the clause -- so every segment
    is joined and each interpolation becomes a ``?`` marker, which keeps the
    f-string shape caught. It is both the most common way inline SQL is
    written and the most dangerous.
    """
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.JoinedStr):
        return "".join(
            value.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str)
            else " ? "
            for value in node.values
        )
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return (_statement_text(node.left) or "") + (_statement_text(node.right) or " ? ")
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr in {"format", "dedent", "strip", "lstrip"}:
            return _statement_text(node.func.value)
    return None


def _sql_statements_in_python(source: str, filename: str = "<canary>") -> set[int]:
    """Line numbers holding a SQL statement, wherever in the module it sits.

    Deduplication is by line rather than by node because the shapes nest: a
    ``BinOp`` and the ``Constant`` at its head report the same line, as does a
    ``JoinedStr`` and its first segment.
    """
    tree = ast.parse(source, filename=filename)
    docstrings = _docstring_ids(tree)
    found = set()
    for node in ast.walk(tree):
        if id(node) in docstrings:
            continue
        text = _statement_text(node)
        if text is not None and _is_sql_statement(text):
            found.add(node.lineno)
    return found


def test_the_sql_in_python_rule_sees_every_shape_that_can_hold_a_statement():
    """The detector, canaried on the shapes it exists to catch.

    The last four are the ones Rules 5b and 5c could not see between them, and
    they are here so that a future narrowing of this rule fails loudly rather
    than quietly restoring the blind spot.
    """
    caught = _sql_statements_in_python(
        'cur.execute("SELECT a FROM t")\n'                        # 1  call site
        'sql = "INSERT INTO t VALUES (%s)"\n'                     # 2  assignment
        'def f():\n    return "UPDATE t SET a = 1"\n'             # 3  return
        'cur.execute("SELECT a FROM t WHERE b = " + b)\n'         # 4  concatenated
        'cur.execute(f"SELECT {col} FROM t")\n'                   # 5  f-string
        'QUERIES = {"tie": "SELECT DISTINCT vin FROM events"}\n'  # 6  dict value
        '_count("x", "SELECT COUNT(*) FROM t")\n'                 # 7  local helper
        'def g():\n    return ("SELECT a FROM t", params)\n'      # 8  tuple return
        'CHOICES = ["DELETE FROM t WHERE id = %s"]\n'             # 9  list element
        'cur.execute("MERGE INTO t USING s ON t.id = s.id")\n'    # 10 merge
        'cur.execute("CREATE TABLE u AS SELECT a FROM t")\n'      # 11 DDL over a query
    )
    assert caught == {1, 2, 4, 5, 6, 7, 8, 10, 11, 12, 13}, (
        f"the SQL-in-Python rule no longer sees every shape: {sorted(caught)}"
    )

    clean = _sql_statements_in_python(
        'cur.execute(CLAIM_ARTIFACTS, (limit,))\n'   # a loaded constant is the fix
        'df.query("price > 100")\n'                  # not SQL: no leading verb
        'conf = {"select": ["model"]}\n'             # the word, not a statement
        'con.execute("INSTALL httpfs")\n'            # session setup, exempt
        'con.execute("SET s3_url_style=?", ["path"])\n'  # session setup, exempt
        'def h():\n    """Example: SELECT a FROM t."""\n    return 1\n'  # docstring
        'SUBJECTS = ["Merge origin/master into a-branch"]\n'  # git, not SQL
        'SUBJECTS = ["Merge pull request #371 from whitewalls86/b"]\n'  # git, not SQL
        'raw.execute("CREATE TABLE u (a int)")\n'     # scaffolding DDL, exempt
        'assert "CREATE TABLE public.gate_observations" in migration\n'  # migration text
    )
    assert not clean, (
        f"the SQL-in-Python rule fires on code that is already correct: {sorted(clean)}"
    )


def test_no_production_module_holds_a_sql_statement():
    """Rules 5b and 5c as one, keyed on the statement instead of its container.

    **What this buys over the two it replaces** is that it cannot be escaped by
    inventing a new place to put a string. Both predecessors could: 5b by
    calling a function it had never heard of, 5c by putting the statement
    anywhere that is not an assignment or a return. Three production sites were
    doing exactly that when this rule was written, and none of them was
    reachable by lengthening a list.

    **The waiver ledgers are kept separate on purpose.** They record which gap
    each site came from -- G5 for a call site, G15 for a binding -- and that
    attribution is history worth keeping even though one rule now reads both.
    Merging them would also break the mutation harness, which anchors on
    ``Waiver(subject, gap="G5", owner=162)`` as literal source text.

    **What this does not survive is PySpark**, unchanged from Rule 5b and still
    three named things rather than an open question: SQL *fragments*
    (``df.selectExpr("price > msrp")``, ``F.expr(...)``) start with no verb and
    the grammar guard is blind to them by construction; the DataFrame API is
    not text at all, so it can drift from a schema with nothing textual to see;
    and a ``.sql`` file only earns its keep if some engine executes it, which
    for Spark means the services ``tests/integration/lakehouse`` is
    :data:`DORMANT_SUITES`-declared against until Plan 125 Gate C returns them.
    Static reading stops at the first of those three; the other two are caught
    by executing them in CI or not at all.
    """
    found = {
        f"{_relative(path)}:{line}"
        for path in production_python_files()
        for line in _sql_statements_in_python(path.read_text(encoding="utf-8"), str(path))
    }
    _assert_exactly(
        found,
        INLINE_SQL_WAIVERS + SQL_LITERAL_WAIVERS,
        "these production modules hold a SQL statement in Python, so it is in "
        "no .sql file and the Layer 2 census cannot count it:",
    )


# ---------------------------------------------------------------------------
# Rule 5g -- no test module holds a SQL statement either.
#
# **This rule removes a judgement rule rather than adding a mechanical one**,
# which is the whole shape of Plan 162 Stage X.
#
# ``tests/`` was exempt from the rule above, and the exemption was reasoned.
# Plan 161 question 3 settled that telling a *paraphrase of production* from a
# *legitimate fixture seed* is judgement, for one stated reason: fixture seeds
# are SQL in test files too, and a checker that cannot tell them apart fails on
# correct code. That is true, and it was load-bearing for the whole
# judgement/mechanical split.
#
# **It stops applying once no SQL literal appears under ``tests/`` at all.**
# The ambiguity has nothing left to live in: any SQL-shaped literal here is a
# violation whichever kind it is, so the rule needs no judgement and the
# contract's split moves 7 mechanical / 4 judgement -> 8 / 3.
#
# It is deliberately :func:`_is_sql_statement` -- the *same* predicate the
# production rule uses, not a variant. A second copy of the grammar would be
# two rules that can disagree, which is the defect this file exists to catch.
# Pointing the one predicate at a second file set is what made it better: the
# hostile surface here found the two holes recorded at ``_MERGE_INTO`` and
# ``_MATERIALISING``, and both fixes apply to production too.
#
# Where the statements went is ``tests/sql/``, mirroring the test tree down to
# the module -- see :mod:`tests.sql_loader`. That root is **not** in
# :func:`production_sql_files`, so it inflates no production denominator and
# owes no Layer 2 test. A read-back assertion is still not a production
# statement; it is simply no longer a literal typed inside a test.
# ---------------------------------------------------------------------------
TEST_SQL_WAIVERS: tuple[Waiver, ...] = ()

SQL_ROOT = TESTS_DIR / "sql"


def all_test_modules() -> list[Path]:
    """Every ``.py`` under ``tests/``, which is this rule's whole surface.

    No package filter and no ``test_*`` filter: ``conftest.py`` held 17 of the
    statements this stage moved, and a rule that skipped it would have left the
    seeds most tests share behind.
    """
    return [
        path
        for path in sorted(TESTS_DIR.rglob("*.py"))
        if "__pycache__" not in path.parts
    ]


def test_no_test_module_holds_a_sql_statement():
    """505 sites on 2026-09-05, and the count is not what sized the stage.

    Stage T measured the same surface as *duplication* -- 96 ad-hoc ``INSERT``s
    and 161 distinct read-back ``SELECT``s, 43 of them written more than once
    for 145 total retypings, one of them seventeen times -- and reached for a
    shared Python helper. A helper removes the retyping and leaves the drift:
    one definition that still has to agree with a schema, with nothing
    asserting that it does. A file under ``tests/sql/`` is checked against the
    schema by ``PREPARE`` whether or not the test consuming it runs.

    **What this does not claim.** It says nothing about whether a mock is
    mocking the thing under test, or whether an assertion is meaningful. Those
    two stay judgement, they stay in ``docs/TESTING.md``'s judgement section,
    and the reviewer skill goes on refusing to certify them.
    """
    found = {
        f"{_relative(path)}:{line}"
        for path in all_test_modules()
        for line in _sql_statements_in_python(path.read_text(encoding="utf-8"), str(path))
    }
    _assert_exactly(
        found,
        TEST_SQL_WAIVERS,
        "these test modules hold a SQL statement in Python. Move it to "
        "tests/sql/ -- mirroring the module's own path -- and load it with "
        "`SQL(\"name\")`, so that PREPARE checks it against the schema whether "
        "or not this test runs. See tests/sql_loader.",
    )


def test_every_test_sql_file_is_named_by_the_module_it_mirrors():
    """The other direction: a statement nothing loads is a statement nobody reads.

    ``tests/sql/`` mirrors the test tree, so this needs no registry -- the
    module a file belongs to *is* its path. Two ways to fail, and the second is
    the one that bites over time: a directory whose module has been renamed or
    deleted, and a file whose module no longer names it. Without this, deleting
    a test leaves its statements behind to be PREPAREd forever against a schema
    nothing reads.
    """
    orphaned_directories, unnamed = [], []
    for path in sorted(SQL_ROOT.rglob("*.sql")):
        relative = path.relative_to(SQL_ROOT)
        parts = list(relative.parts)
        if parts[-2] in _ENGINE_DIRECTORIES:
            del parts[-2]
        module = TESTS_DIR.joinpath(*parts[:-1]).with_suffix(".py")
        if not module.is_file():
            orphaned_directories.append(
                f"{_relative(path)} mirrors {_relative(module)}, which does not exist"
            )
        elif not _names(path.stem, module.read_text(encoding="utf-8")):
            unnamed.append(f"{_relative(path)} is named by no line of {_relative(module)}")
    assert not orphaned_directories, (
        "these statements mirror a test module that is gone:\n  "
        + "\n  ".join(orphaned_directories)
    )
    assert not unnamed, (
        "these statements are loaded by nothing:\n  " + "\n  ".join(unnamed)
    )


# ---------------------------------------------------------------------------
# Rule 5h -- every test statement is schema-checked, whether or not it runs.
#
# The engine is **derived from the path**, not declared in a table: a statement
# sitting directly under its module's directory is Postgres's, and one under a
# ``duckdb/`` or ``airflow/`` segment names the engine that owns it instead.
#
# The default is what gives the rule its failure direction, and it is the whole
# reason there is no list. A statement for an engine nobody has thought about
# lands in the default bucket, is handed to ``PREPARE`` against a
# Flyway-migrated Postgres, and fails there until somebody files it. A table of
# engines would have to be remembered; a default that fails does not.
#
# ``airflow/`` is a second *Postgres* schema rather than a second engine:
# ``airflow.dag_run`` is created by ``airflow db migrate``, not by Flyway, so a
# Flyway-only database cannot plan it. It is named here rather than waived
# because it is a fact about where the schema comes from.
# ---------------------------------------------------------------------------
_ENGINE_DIRECTORIES = frozenset({"duckdb", "airflow"})

# One statement holds a ``{placeholder}`` no static reading can fill.
# ``insert_ops_price_observations`` builds ``{columns}`` and ``{values}`` with
# ``", ".join(...)`` over lists assembled per case, so the text that reaches
# Postgres exists only at run time. That is a real limit and it is the whole of
# this ledger.
#
# **It held 25 on 2026-09-05, and 24 of those were the rule's fault rather than
# the statements'.** The reading was ``"{" in text``, which is wrong twice
# over. Seven were not templates: ``'{"makes": ["test"]}'::jsonb`` is a JSONB
# literal whose braces sit inside a quoted string, beside a ``%s`` the driver
# binds -- they plan exactly as written and were being held out of the check
# they would have passed, which is a coverage hole wearing a known limit's
# clothes. The other seventeen were templates, but their bindings are stated at
# the call site in a module constant or a literal the module iterates
# (``RECEIPT_TABLE``, ``PROTECTED_TABLES``, ``POSTGRES_SNAPSHOT_TABLES``, and
# the ``for table in (...)`` loops). ``tests/sql_bindings.py`` reads the shape
# from the call's AST and the values from the imported module, and
# ``test_fixture_statements`` now PREPAREs every rendering rather than skipping
# the file.
#
# Four more were here on 2026-09-05 and are not waivers, because they were
# repaired instead: ``{claimed_at}``, ``{created_hours_ago}`` and
# ``{proc_event_hours_ago}`` interpolated a *value* into a statement where the
# driver would have bound one, which is interpolation wearing an identifier's
# clothes. They are ``now() - (%s || ' hours')::interval`` now, and they
# PREPARE.
TEST_SQL_TEMPLATE_WAIVERS: tuple[Waiver, ...] = tuple(
    Waiver(subject, gap="G19", owner=162, since=date(2026, 9, 5))
    for subject in (
        "tests/sql/integration/sql/test_ops_views/insert_ops_price_observations.sql",
    )
) + tuple(
    # Plan 162 Stage S's constraint-mutation gate. These three are the case
    # ``tests/sql_bindings.py`` cannot reach, and the reason is not effort: what
    # they interpolate is **a whole model's compiled SQL, and then that SQL with
    # one branch deleted**. There is no call-site constant to read the bindings
    # from, because the bindings are generated -- 23 model bodies and 216
    # mutants of them, none of which exists until dbt has compiled and the
    # enumerator has run. A statement whose renderings are enumerable at Layer 0
    # would not be here; these are not, and saying so is what the ledger is for.
    #
    # They still execute, against the in-memory database the gate mutates in,
    # and a rendering that will not run is what
    # ``test_no_mutant_failed_to_execute`` exists to catch -- so the schema
    # check ``PREPARE`` would give them is done by execution instead, on every
    # one of the 216.
    Waiver(subject, gap="G19", owner=162, since=date(2026, 9, 7))
    for subject in (
        "tests/sql/integration/dbt/test_constraint_mutation/count_failing_rows.sql",
        "tests/sql/integration/dbt/test_constraint_mutation/count_relation_rows.sql",
        "tests/sql/integration/dbt/test_constraint_mutation/materialize_relation.sql",
        # Same reason, one gate over: the non-vacuity gate walks every model
        # under dbt/models/ rather than naming any, so the relation is generated
        # and there is no call-site constant to read the renderings from. A list
        # of models beside the models is exactly the thing that goes stale and
        # lets a new one escape the obligation.
        "tests/sql/integration/dbt/test_models_are_not_vacuous/count_model_rows.sql",
    )
)


def postgres_test_statements() -> list[Path]:
    """Every ``tests/sql/`` statement a Flyway-migrated Postgres should plan."""
    return [
        path
        for path in sorted(SQL_ROOT.rglob("*.sql"))
        if not _ENGINE_DIRECTORIES & set(path.relative_to(SQL_ROOT).parts)
    ]


def test_every_test_statement_that_holds_a_template_is_waived():
    """The template ledger describes reality, in both directions.

    ``PREPARE`` is run by ``tests/integration/sql/test_fixture_statements.py``,
    which needs an engine and therefore cannot say which files it *declined* to
    check in a job that has no Postgres. This can, at Layer 0, with nothing:
    a statement that stops being a template must leave the ledger, and one that
    becomes a template must join it.

    **Holding a placeholder is no longer enough to be waived, and that is the
    repair.** The ledger seeded at 25 on the reading ``"{" in text``, which is
    two mistakes in one. Seven of those were not templates at all -- braces
    inside a ``::jsonb`` literal -- so they were excluded from the schema check
    they would have passed. The remaining eighteen were templates whose
    bindings the call site states out loud, in a module constant or a literal
    the module iterates. What is waived now is the intersection that is
    genuinely unplannable: a placeholder that is real, filled with something no
    static reading can produce.
    """
    templated = {
        _relative(path)
        for path in postgres_test_statements()
        if holds_a_placeholder(path.read_text(encoding="utf-8"))
        and renderings(path) is None
    }
    _assert_exactly(
        templated,
        TEST_SQL_TEMPLATE_WAIVERS,
        "a tests/sql statement holds a {placeholder} whose bindings cannot be "
        "read from its call site, so nothing plans it against the schema. Bind "
        "the value as a parameter if it is a value; name the relation in a "
        "module constant or a literal the module iterates if it is a relation, "
        "and tests/sql_bindings.py will render and PREPARE it. Waive against "
        "G19 only when the call site computes the text:",
    )


# ---------------------------------------------------------------------------
# Rule 5i -- a test may not invent the shape of a relation production defines.
#
# **The rule is not "a test may not create a table."** A scratch table standing
# for nothing is legitimate scaffolding -- ``create table t as select 1`` is how
# `test_analytics_connection_guard` gets something for its guard to refuse, and
# forbidding it would fail on correct code. What a test may not do is declare a
# schema for a relation *production already defines*, because that is a copy,
# and a copy drifts.
#
# **The name match is the signature, not a heuristic.** A test stands up
# ``int_listing_state_fingerprints`` precisely so that the code under test finds
# it -- `audit_adaptive_refresh_features` looks the relation up by the name in
# its own ``TABLE_SPECS``. Renaming the fixture to dodge this rule would stop
# the production code finding it, so the test would stop testing anything. The
# escape that does exist is a test passing an arbitrary relation name *into* the
# code under test; that is a weaker test to begin with, and it is recorded here
# rather than defended against.
#
# **Measured 2026-09-05, and the drift had already happened.** Three fixtures
# hand-declare stand-ins for real dbt models: `int_listing_state_fingerprints`
# declares 5 columns against the model's 8, `int_listing_state_runs` 1 against
# 11, `int_listing_observation_fingerprints` 1 against 10. Nothing noticed,
# because nothing compared them.
#
# **Subset, not equality, and that is the whole design.** Requiring equality
# would force an 11-column fixture where the test needs one column, which is
# ceremony. Subset catches the two failures that matter -- a renamed column and
# a dropped one both remove it from the model's declaration -- and lets a narrow
# fixture stay narrow. An *added* column does not fire, correctly: it cannot
# break a fixture that never mentioned it.
#
# **What this does not catch is a retype.** Until 2026-09-06 it could not:
# ``schema.yml`` carried column names and no ``data_type`` -- 0 of 187 on
# 2026-09-05 -- so nothing in the repository declared what the model's
# ``datediff_hours()`` actually returns. Stage S's contract work removed that
# obstacle: 307 of 307 columns now declare a type that a build enforces, and
# the answer is ``BIGINT`` against the fixture's ``run_duration_hours
# INTEGER``. The comparison this rule would have to make now has both sides;
# making it is the rest of CAR-90 and is stated in ``docs/TESTING.md`` as a
# limit rather than counted as coverage.
# ---------------------------------------------------------------------------
_CREATE_TABLE = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\w*\s+|UNLOGGED\s+)*TABLE\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][\w.\"]*)\s*\(",
    re.I,
)
_NOT_A_COLUMN = frozenset(
    {"primary", "foreign", "unique", "check", "constraint", "exclude", "like"}
)


@lru_cache(maxsize=None)
def dbt_model_columns() -> dict[str, frozenset[str]]:
    """``{model name: declared columns}``, read from dbt's own schema files.

    dbt's ``schema.yml`` is the only place in the repository that declares what
    a model's columns are without executing it, which is why it is the
    authority here rather than the model's final ``SELECT``. Since Plan 162
    Stage S it is a complete one -- 23 of 23 models document every column their
    final ``SELECT`` emits, 307 in all, each with a ``data_type`` under an
    enforced contract -- so a name missing from it is a name the model does not
    emit, and the rule below fails rather than this function guessing.
    """
    columns: dict[str, frozenset[str]] = {}
    for path in sorted((REPO_ROOT / "dbt" / "models").rglob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for model in document.get("models") or ():
            columns[model["name"]] = frozenset(
                column["name"] for column in model.get("columns") or ()
            )
    assert columns, "no dbt models parsed out of dbt/models/**/*.yml"
    return columns


@lru_cache(maxsize=None)
def production_relations() -> frozenset[str]:
    """Every relation production defines: dbt's models and Flyway's tables."""
    models = {path.stem for path in (REPO_ROOT / "dbt" / "models").rglob("*.sql")}
    flyway = {
        _bare_relation(match)
        for path in (REPO_ROOT / "db" / "migrations").glob("*.sql")
        for match in _CREATE_TABLE.findall(path.read_text(encoding="utf-8"))
    }
    return frozenset(models | flyway)


def _bare_relation(name: str) -> str:
    """``ops.artifacts_queue`` -> ``artifacts_queue``. Fixtures are unqualified."""
    return name.replace('"', "").rsplit(".", 1)[-1]


def _declared_columns(text: str, start: int) -> list[str]:
    """The column names in the parenthesised definition beginning at *start*."""
    depth, body, index = 0, [], start
    while index < len(text):
        character = text[index]
        if character == "(":
            depth += 1
            if depth == 1:
                index += 1
                continue
        elif character == ")":
            depth -= 1
            if depth == 0:
                break
        body.append(character)
        index += 1
    items, depth, current = [], 0, ""
    for character in "".join(body):
        if character in "([":
            depth += 1
        elif character in ")]":
            depth -= 1
        if character == "," and depth == 0:
            items.append(current)
            current = ""
        else:
            current += character
    items.append(current)
    names = []
    for item in items:
        first = item.strip().split()
        if first and first[0].lower() not in _NOT_A_COLUMN:
            names.append(first[0].strip('"').lower())
    return names


def _fixture_relations() -> list[tuple[str, str, list[str]]]:
    """``(where, relation, columns)`` for every table a test defines itself."""
    found = []
    for path in all_test_modules():
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        docstrings = _docstring_ids(tree)
        for node in ast.walk(tree):
            if id(node) in docstrings:
                continue
            text = _statement_text(node)
            if text is None:
                continue
            match = _CREATE_TABLE.search(text)
            if match:
                found.append((f"{_relative(path)}:{node.lineno}",
                              _bare_relation(match.group(1)),
                              _declared_columns(text, match.end() - 1)))
    for path in sorted(SQL_ROOT.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        match = _CREATE_TABLE.search(text)
        if match:
            found.append((_relative(path), _bare_relation(match.group(1)),
                          _declared_columns(text, match.end() - 1)))
    return found


def test_no_test_invents_the_shape_of_a_relation_production_defines():
    """A fixture may borrow production's relation name only on its own terms.

    Two ways to fail. A relation production defines but nothing declares the
    columns of -- every Flyway table today -- cannot be checked at all, so
    standing one up in a test is refused outright rather than passed silently.
    And a dbt model's fixture may name only columns that model declares, so a
    rename or a drop in the model takes the fixture's column with it.
    """
    unknown_shape, undeclared = [], []
    for where, relation, columns in _fixture_relations():
        if relation not in production_relations():
            continue
        declared = dbt_model_columns().get(relation)
        if declared is None:
            unknown_shape.append(f"{where} declares a shape for `{relation}`")
            continue
        missing = sorted(set(columns) - {name.lower() for name in declared})
        if missing:
            undeclared.append(f"{where} `{relation}`: {', '.join(missing)}")
    assert not unknown_shape, (
        "these tests declare a schema for a relation production defines, and "
        "nothing declares that relation's columns for them to be checked "
        "against -- Flyway owns it, so build the fixture by applying the "
        "migration rather than by retyping it:\n  " + "\n  ".join(unknown_shape)
    )
    assert not undeclared, (
        "these fixtures name columns the model does not declare. Every model "
        "declares every column it emits under an enforced contract, so the "
        "declaration is not the incomplete half: the model renamed or dropped "
        "them, the fixture is stale, and so is whatever reads it:\n  "
        + "\n  ".join(undeclared)
    )


def test_the_fixture_relation_corpus_is_not_empty():
    """The guard the rule above spent two commits without.

    A set difference over an empty corpus is empty, so a ``_CREATE_TABLE``
    that stops matching does not fail the rule above -- it makes it pass
    unconditionally, which is strictly worse than deleting it. That is what
    happened. ``815fcf2`` added the rule and a pattern ending at the opening
    paren; ``7825c0e`` added a **second** module-level ``_CREATE_TABLE`` for a
    different rule, requiring a terminating semicolon that no inline fixture
    carries, and the later binding won for every call site. The corpus read 0
    of 0 across 248 test modules and 385 statement files, and the rule went
    green on nothing.

    Two defects were masking each other, which is why this guard is worth more
    than the rename alone: ``production_relations`` calls ``.findall`` on the
    same name, and the shadowing pattern's second group made it return tuples.
    That ``AttributeError`` was unreachable only because the empty corpus meant
    the loop body never ran, so repairing the corpus without noticing would
    have swapped a rule that passes vacuously for one that crashes.

    The one thing that did notice was
    ``scripts/verify_testing_contract_mutations.py``, which reported this
    rule's mutation as MISSED for as long as it was broken.
    """
    assert _fixture_relations(), (
        "no test or statement file declares a CREATE TABLE anywhere. That is "
        "almost certainly _CREATE_TABLE having stopped matching rather than a "
        "repository with no fixtures, and the rule above cannot fail while it "
        "is true"
    )


# ---------------------------------------------------------------------------
# Rule 5j -- schema.yml is the model's shape, and dbt is what enforces it.
#
# The rule above can only be as good as the declaration it reads, and until
# Plan 162 Stage S that declaration was documentation: nothing made a model's
# `schema.yml` agree with the model. `int_latest_observation.sql` said so in
# its own prose -- *"a column added to stg_observations must be added here too,
# or it silently stops appearing downstream. Nothing currently catches that
# drift automatically ... this model's schema file documents only
# vin17/source/make, not the full column list, so it is not a backstop."*
#
# `contract: {enforced: true}` is what turns it into a backstop: dbt fails the
# build when a model's output stops matching its declared columns and types.
#
# The portability objection is already retired by measurement:
# `docs/reference/plan_125_portability_audit.md` verified that `varchar` is a
# hard Spark parse error and `string` is DuckDB's alias and Spark's native
# name, "verified on both". So a declared type can be spelled once for both
# engines, and Plan 125's migration is not a reason to leave this undeclared.
# ---------------------------------------------------------------------------
# Empty since Plan 162 Stage S (CAR-79). Seeded at 23 by Stage X on 2026-09-05
# -- one waiver per model, because 0 of 23 declared a contract and a ledger
# fails where a ticket does not -- and drained to 0 by Stage S on 2026-09-06.
#
# It drained in one operation rather than 23, because the thing that made it
# expensive was never the `contract:` block. It was the declaration underneath:
# 5 models documented 24 of the 144 columns they emit between them -- a
# 120-column shortfall -- and no column anywhere in the project carried a
# `data_type`. Both halves came from `DESCRIBE`
# against the built relations rather than from reading the SQL -- 307 columns,
# names and types together -- which is the same derivation the fixture rule
# above now depends on, and the opposite of the hand-transcription that
# produced the three stale fixtures Stage X found.
DBT_CONTRACT_WAIVERS: tuple[Waiver, ...] = ()


def _declares_an_enforced_contract(name: str, path: Path) -> bool:
    """dbt accepts the contract in the model's own config or in its schema file."""
    if re.search(r"contract\s*=\s*\{\s*['\"]enforced['\"]\s*:\s*[Tt]rue",
                 path.read_text(encoding="utf-8")):
        return True
    for schema in sorted(path.parent.glob("*.yml")):
        document = yaml.safe_load(schema.read_text(encoding="utf-8")) or {}
        for model in document.get("models") or ():
            if model["name"] != name:
                continue
            contract = (model.get("config") or {}).get("contract") or {}
            if contract.get("enforced") is True:
                return True
    return False


def test_every_dbt_model_declares_an_enforced_contract():
    """0 waivers since 2026-09-06, and the zero is the point.

    The fixture rule above trusts `schema.yml`. What makes `schema.yml` true is
    dbt refusing to build a model whose output stops matching it. Without an
    enforced contract a model's declaration is a comment that a checker happens
    to read, which is why this was seeded fully waived at 23; every model now
    carries one, so every declaration that rule reads is defended by a build.
    """
    unenforced = {
        _relative(path)
        for path in sorted((REPO_ROOT / "dbt" / "models").rglob("*.sql"))
        if not _declares_an_enforced_contract(path.stem, path)
    }
    _assert_exactly(
        unenforced,
        DBT_CONTRACT_WAIVERS,
        "these dbt models do not declare `contract: {enforced: true}`, so "
        "nothing fails when the model's output stops matching its schema.yml "
        "and every rule reading that file is trusting documentation:",
    )


# ---------------------------------------------------------------------------
# Plan 162 Stage S -- the lists that name the lake's source tables are
# reconciled against dbt's own, because four of them existed and no two were
# checked against each other.
#
# `--require-non-empty` is the gate that stops a snapshot build being green
# over an empty world, and until Stage S the sources it checked were a tuple
# typed into `scripts/seed_lake_snapshot.py`. A seventh source declared in
# `dbt/models/sources.yml` would have been read by the build and counted by
# nothing: the gate would have passed, saying six sources have rows, over a
# world where the seventh had none. That is this plan's own recurring defect --
# a maintained copy of a derivable list -- sitting inside the instrument the
# stage depends on.
#
# The seeder now derives the list (`dbt_source_tables()`), so the forward
# direction is closed at the point of use. What a derivation cannot do on its
# own is fail when the *other* lists drift, and there are two more:
#
#   * `lake_source_audit.SOURCE_TABLE_SPECS` -- logical name -> Parquet glob,
#     what the audit reads and what the seeder resolves `sources.yml` through;
#   * `lake_snapshot_export_cache.INCLUDED_TABLES` -- what the snapshot writer
#     puts in the archive, and what its fingerprint hashes.
#
# The join key is the path, not the name. `silver.observations` in sources.yml
# and `silver_observations` in the specs look like they need a special case;
# they do not, because both carry the identical string
# `silver_normalized/observations/**/*.parquet`. Matching on paths means the
# reconciliation needs no name-translation table of its own -- which would have
# been a fifth list.
# ---------------------------------------------------------------------------


def test_the_non_empty_gate_reconciles_with_the_dbt_source_list():
    """Every dbt source is seedable, and every seedable table is a dbt source.

    Both directions, and each fails a different real mistake.

    **Forward** -- a source declared in `sources.yml` that resolves to no known
    spec is a source a snapshot cannot carry. The dbt build will read it; the
    seed will not fill it; `--require-non-empty` will not mention it. This is
    the failure Stage S closes, and `dbt_source_tables()` raising rather than
    skipping is what closes it: an unrecognised source that is silently not
    counted is indistinguishable from a source that is fine.

    **Reverse** -- a spec or a Postgres allowlist entry that names no dbt
    source is dead weight the exporter still writes, still ships in the
    archive, and the gate still demands rows for. It fails a seed for a table
    no build reads.

    Asserting through the seeder's own resolver rather than re-parsing
    `sources.yml` here is deliberate: a second parser in the test would let the
    two disagree about what a source is, and the test would be checking itself.
    """
    resolved = dbt_source_tables()

    lake = {key for kind, key in resolved.values() if kind == "lake"}
    postgres = {key for kind, key in resolved.values() if kind == "postgres"}
    known_postgres = {f"{schema}.{table}" for schema, table in POSTGRES_SNAPSHOT_TABLES}

    assert lake == set(SOURCE_TABLE_SPECS), (
        "the Parquet sources in dbt/models/sources.yml and "
        "archiver.processors.lake_source_audit.SOURCE_TABLE_SPECS disagree. "
        f"declared by dbt and carried by no spec: {sorted(lake - set(SOURCE_TABLE_SPECS))}; "
        f"specced and named by no dbt source: {sorted(set(SOURCE_TABLE_SPECS) - lake)}. "
        "The first is a source a snapshot cannot seed and --require-non-empty "
        "cannot check; the second is a table the exporter writes for nobody."
    )
    assert postgres == known_postgres, (
        "the postgres_scan() sources in dbt/models/sources.yml and "
        "shared.lake_snapshot_postgres.POSTGRES_SNAPSHOT_TABLES disagree. "
        f"declared by dbt and carried by no allowlist entry: {sorted(postgres - known_postgres)}; "
        f"allowlisted and named by no dbt source: {sorted(known_postgres - postgres)}. "
        "Left empty, stg_search_configs reads nothing and mart_vehicle_snapshot "
        "builds green over an empty world -- which is the whole reason the "
        "Postgres half travels inside the snapshot at all."
    )


def test_the_snapshot_writer_and_the_source_auditor_include_the_same_tables():
    """`INCLUDED_TABLES` is not imported from `SOURCE_TABLE_SPECS`, and should not be.

    Its comment states the reason: the export fingerprint has to hash exactly
    what *this writer* includes, "independent of the source-audit module's own
    evolution". That is sound. A fingerprint that changed because an unrelated
    module grew a diagnostic column would invalidate every cached archive for
    nothing, and one that silently followed another module's list would stop
    describing the bytes it names.

    But that reasoning argues against **importing**, not against being
    **checked**. The two lists are still claims about the same four tables, and
    nothing made them agree. The failure they permit is quiet in both
    directions: a table added to the specs but not to the writer is audited and
    never exported, so the seed's non-empty gate demands rows for something no
    archive carries; a table added to the writer but not to the specs is
    exported and never audited, so it ships with no row counts and no
    timestamp bounds to say whether it is any good.

    So the literal stays a literal and this test is what makes it true. Drift
    fails here, in a test whose job is comparison, instead of at a snapshot
    build weeks later.
    """
    assert set(INCLUDED_TABLES) == set(SOURCE_TABLE_SPECS), (
        "archiver.processors.lake_snapshot_export_cache.INCLUDED_TABLES and "
        "archiver.processors.lake_source_audit.SOURCE_TABLE_SPECS name "
        "different lake tables. "
        f"exported but not audited: {sorted(set(INCLUDED_TABLES) - set(SOURCE_TABLE_SPECS))}; "
        f"audited but not exported: {sorted(set(SOURCE_TABLE_SPECS) - set(INCLUDED_TABLES))}. "
        "INCLUDED_TABLES is deliberately a literal rather than an import, so "
        "that the export fingerprint hashes what this writer includes -- keep "
        "it a literal and fix the drift; do not replace it with an import."
    )


# ---------------------------------------------------------------------------
# Rule 5k -- the recorder wraps every client production reaches an engine
# through, and the client set is derived rather than kept.
#
# **This is the rule the recorder's first design would have failed.** Keying
# capture to the fixtures that hand out connections sees ``psycopg2`` and
# ``duckdb`` and misses ``asyncpg`` and ``pyspark``; it would have shipped
# recording nothing for ``scraper/sql/`` and gone on recording nothing when
# Spark lands. The fix is not a longer list of clients -- that is
# ``_SQL_CALL_NAMES`` with a different noun. It is to derive the *imports* and
# make anything unclassified fail.
#
# So the surface is every third-party top-level import across production
# Python, and the contract classifies each one as reaching an engine or not.
# **A new engine is a new import**, and a new import fails here until somebody
# decides which it is. That is the property a maintained client list cannot
# have, and it is why this reads imports rather than clients.
# ---------------------------------------------------------------------------
_CLIENT_ROW = re.compile(r"^\| ((?:`\w+`(?:, )?)+) \| (\*\*yes\*\*|no) \|", re.M)


@lru_cache(maxsize=None)
def classified_imports() -> dict[str, bool]:
    """``{import name: reaches an engine}``, from the contract's own table."""
    section = _read(CONTRACT).split("### How production reaches an engine")[1]
    section = section.split("### Mocking")[0]
    classified: dict[str, bool] = {}
    for names, verdict in _CLIENT_ROW.findall(section):
        for name in re.findall(r"`(\w+)`", names):
            classified[name] = verdict == "**yes**"
    assert classified, (
        f"{CONTRACT}'s 'How production reaches an engine' table no longer "
        f"parses into rows"
    )
    return classified


@lru_cache(maxsize=None)
def production_imports() -> frozenset[str]:
    """Every third-party top-level import across production Python.

    The standard library and this repository's own packages are dropped
    because neither can be a database client somebody forgot to wrap. The
    local set includes the bare module names the dashboard and the DAG tree
    import flat -- ``db``, ``queries``, ``sensors`` and their kin -- which are
    this repository's modules reached through the dual import identity
    ``docs/TESTING.md`` records as G18, not third-party packages.
    """
    local = set(service_packages()) | {
        "tests", "scripts", "airflow", "dbt", "lakehouse", "dags",
        "db", "queries", "pages", "sensors", "dag_queries", "notifications",
        "pools", "coordination_contract",
    }
    found: set[str] = set()
    for path in production_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [(node.module or "").split(".")[0]]
            else:
                continue
            found.update(
                name for name in names
                if name and name not in sys.stdlib_module_names and name not in local
            )
    return frozenset(found)


def production_db_clients() -> frozenset[str]:
    """The imports the contract says production reaches an engine through."""
    return frozenset(name for name, reaches in classified_imports().items() if reaches)


def test_every_production_import_is_classified():
    """Both directions, and the second one is what keeps the table honest.

    An unclassified import is the failure that matters: it is how a new engine
    arrives, and the whole recorder rests on there being no such thing. A row
    for an import nothing imports any more is the mirror -- a table describing
    a tree that has moved on, which is `ARCHITECTURE.md:179` in miniature.
    """
    classified = set(classified_imports())
    imported = set(production_imports())
    assert not imported - classified, (
        "these third-party imports are not classified in "
        f"{CONTRACT}'s 'How production reaches an engine':\n  "
        + "\n  ".join(sorted(imported - classified))
        + "\n\nSay whether production reaches a database engine through each. "
        "If it does, the recorder must wrap it; if not, the row says so."
    )
    assert not classified - imported, (
        f"{CONTRACT} classifies imports that no production module imports any "
        "more, so the table describes a tree that has moved on:\n  "
        + "\n  ".join(sorted(classified - imported))
    )


def test_the_recorder_instruments_every_client_production_reaches():
    """The contract and the plugin, compared rather than assumed to agree.

    Declared rather than probed at runtime on both sides: what a job happens to
    have installed must not decide what the contract says is owed. A job
    without Spark records nothing for Spark and reports that it did not wrap
    it -- which reads differently from Spark executing nothing, and has to.
    """
    from tests.plugins.sql_execution_recorder import INSTRUMENTED_CLIENTS

    declared = production_db_clients()
    unwrapped = sorted(declared - INSTRUMENTED_CLIENTS)
    assert not unwrapped, (
        "the contract says production reaches an engine through these, and the "
        "execution recorder does not wrap them, so every statement they carry "
        f"is invisible to the record: {unwrapped}"
    )
    phantom = sorted(INSTRUMENTED_CLIENTS - declared)
    assert not phantom, (
        "the execution recorder wraps clients the contract does not list as "
        f"reaching an engine: {phantom}. Either the table is stale or the "
        "recorder is wrapping something it should not."
    )


#: The gate that reads what the recorder wrote, and the pieces of ``ci.yml``
#: without which it measures nothing. Named here rather than in the workflow
#: alone because that is exactly how the record was lost the first time: the
#: upload steps existed in some jobs and not others, the gate read what
#: happened to arrive, and no test noticed.
RECORD_ENV = "SQL_EXECUTION_RECORD"
RECORDER_MODULE = "tests.plugins.sql_execution_recorder"
COVERAGE_GATE_SCRIPT = "scripts/check_sql_execution_coverage.py"
_RECORD_ARTIFACT = "sql-execution-"


def _sql_execution_wiring() -> tuple[set[str], set[str], dict]:
    """``(jobs running pytest, jobs uploading a record, the gate job)``."""
    document = yaml.safe_load(_read(WORKFLOW))
    running: set[str] = set()
    uploading: set[str] = set()
    gate: dict = {}
    for key, job in document["jobs"].items():
        for step in job.get("steps", []) or []:
            run = str(step.get("run", ""))
            for line in run.splitlines():
                match = _PYTEST_INVOCATION.search(line.strip())
                if match and match.group("args").startswith(("tests", "-", "--")):
                    running.add(key)
            if COVERAGE_GATE_SCRIPT in run:
                gate = job | {"__key__": key, "__run__": run}
            with_ = step.get("with", {}) or {}
            if "upload-artifact" in str(step.get("uses", "")):
                if str(with_.get("name", "")).startswith(_RECORD_ARTIFACT):
                    uploading.add(key)
    return running, uploading, gate


def test_every_job_that_runs_pytest_has_its_record_read_by_the_gate():
    """The four pieces, because any one of them missing measures nothing.

    **This is the rule the first CI run needed and did not have.** The gate
    landed with upload steps in some jobs and not others; it then read the
    records that happened to arrive and reported a coverage number for the
    whole repository from a fraction of it. Nothing failed, because the only
    statement of which jobs owe a record was the workflow file agreeing with
    itself.

    So the owing set is *derived*: a job that runs pytest produces a record, so
    a job that runs pytest owes an upload, and the gate owes a ``needs`` on it.
    A job added next year is covered by the derivation rather than by anyone
    remembering this file exists -- the same reason ``RECORD_ENV`` is set at
    workflow level and asserted there.

    The last clause is the ratchet: ``--report`` prints the reading and exits
    0, which is what the gate needed for the two landings it took to get an
    honest number. Leaving it in place would have made the gate a decoration,
    so putting it back now costs a diff that touches this docstring.
    """
    document = yaml.safe_load(_read(WORKFLOW))
    assert RECORD_ENV in document.get("env", {}), (
        f"{WORKFLOW} no longer sets {RECORD_ENV} at workflow level, so a job "
        f"written after this one records nothing and its statements read as "
        f"never executed."
    )

    running, uploading, gate = _sql_execution_wiring()
    silent = sorted(running - uploading)
    assert not silent, (
        f"jobs in {WORKFLOW} that run pytest but upload no execution record: "
        f"{silent}. Every statement they execute is invisible to "
        f"{COVERAGE_GATE_SCRIPT}, which will report it as executing nowhere."
    )

    assert gate, f"{WORKFLOW} no longer runs {COVERAGE_GATE_SCRIPT} at all."
    unread = sorted(uploading - set(gate.get("needs", [])))
    assert not unread, (
        f"{gate['__key__']} does not wait on {unread}, which upload execution "
        f"records. Without the dependency the gate can start before they "
        f"finish and read a different set of records on every run."
    )
    assert "--report" not in gate["__run__"], (
        f"{COVERAGE_GATE_SCRIPT} is running with --report, which prints the "
        f"reading and exits 0. It existed to seed the ledger honestly and the "
        f"ledger is seeded; with it the gate cannot fail."
    )

    config = tomllib.loads(_read("pyproject.toml"))["tool"]["pytest"]["ini_options"]
    assert f"-p {RECORDER_MODULE}" in config.get("addopts", ""), (
        f"pyproject.toml no longer registers {RECORDER_MODULE} through "
        f"addopts, so every job sets {RECORD_ENV} and none of them records."
    )


# ---------------------------------------------------------------------------
# Rule 6 -- the layer numbers in the code are this document's.
# ---------------------------------------------------------------------------
# Empty since Plan 162 Stage F (CAR-49) swept all 16 on 2026-09-01. The rule
# below is the whole of G11 now: it is what stops Plan 84's numbering coming
# back the next time someone copies a docstring header from an older file.
LAYER_NUMBER_WAIVERS: tuple[Waiver, ...] = ()

# A module's *own* claim is a leading ``Layer N`` on the first line of its
# docstring. A ``Layer N`` further in is a cross-reference to another layer --
# ``tests/ops/routers/test_scrape.py`` points at the Layer 4 integration tests
# that cover its SQL -- and reading those as claims would fail on prose that is
# correct.
_DOCSTRING_CLAIM = re.compile(r"^Layer (\d)\b")
_STEP_NAME_LAYER = re.compile(r"\(Layer (\d)\)")


def test_every_test_directory_is_assigned_a_layer():
    """The gate the next two assertions stand on.

    A directory the contract does not place is not a small documentation gap:
    it is a suite whose purpose, dependencies and CI home nobody has stated,
    and every rule below silently skips it.
    """
    unplaced = sorted(
        _relative(directory)
        for directory in _test_directories()
        if _layer_of(directory) is None
    )
    assert not unplaced, (
        f"{CONTRACT} assigns no layer to {unplaced}. Add a row to 'Where the "
        f"newer suites sit', or a '**Lives in:**' pattern that covers it."
    )


def test_every_layer_number_in_the_code_matches_the_contract():
    """G11: the numbers in the tree are Plan 84's, and this is the +1 shift.

    Plan 84 numbered the three integration tiers and left unit tests unnumbered
    beside them, so the dependency-free tier read as the last one. The contract
    renumbers by dependency cost and every tier keeps its relative position.
    The sweep is mechanical; this assertion is what stops it drifting back.
    """
    mismatched = set()
    for path in sorted(TESTS_DIR.rglob("*.py")):
        docstring = ast.get_docstring(
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        )
        if not docstring:
            continue
        claim = _DOCSTRING_CLAIM.match(docstring.strip())
        if not claim:
            continue
        actual = _layer_of(path.parent)
        if actual is not None and int(claim.group(1)) != actual:
            mismatched.add(f"{_relative(path)}: {claim.group(1)}, not {actual}")

    for _, step, argument in pytest_steps():
        named = _STEP_NAME_LAYER.search(step)
        if not named:
            continue
        target = argument.split()[0]
        actual = _layer_of(REPO_ROOT / target)
        if actual is not None and int(named.group(1)) != actual:
            mismatched.add(f"ci.yml '{step}': {named.group(1)}, not {actual}")

    _assert_exactly(
        mismatched,
        LAYER_NUMBER_WAIVERS,
        f"Every 'Layer N' a module or CI step claims for itself matches the "
        f"layer {CONTRACT} assigns its directory. Each entry reads "
        f"'<subject>: <claimed>, not <actual>'.",
    )


# ---------------------------------------------------------------------------
# Rule 7 -- the harness must not decide the outcome.
# ---------------------------------------------------------------------------
def test_every_pytest_invocation_in_ci_sets_pythonpath():
    """One of two mechanically checkable clauses of "the harness must not
    decide the outcome" -- Stage 6b added the other, below. The rest of that
    rule is judgement, and the contract says so.

    ``tests/test_planning_docs.py`` passed or failed on one machine, one OS and
    one commit purely on whether the checkout directory name was a valid Python
    identifier: 35 passed as ``cartracker-scraper``, 2 failed as
    ``new_car_tracker``, which is what CI uses. The repo root carries an
    ``__init__.py``, so pytest walks up for the package root and where it stops
    depends on the directory's name. ``PYTHONPATH`` settles it. CAR-42 fixed
    the one step that lacked it; this keeps the next one from being added.

    The rule's third instance was ``test_verify_recovery_live_state.py``'s
    canary command, which quoted ``sys.executable`` with ``shlex.quote`` --
    POSIX quoting ``cmd.exe`` does not honour, so it failed on Windows and
    passed in CI. Stage 5 replaced the interpreter with ``exit 3``, a shell
    builtin that needs no quoting. Nothing here could have caught it: CI is
    Linux, and the only check that finds this class of defect is running the
    suite somewhere else.
    """
    without = sorted(
        f"{job}: {step}"
        for job, step, _ in pytest_steps()
        if "PYTHONPATH" not in _step_env(job, step)
    )
    assert not without, (
        f"pytest steps in {WORKFLOW} with no PYTHONPATH: {without}. A test "
        f"whose result depends on where the repository happens to be checked "
        f"out is not testing the code."
    )


@lru_cache(maxsize=None)
def _step_env(job_name: str, step_name: str) -> tuple[str, ...]:
    """The ``env:`` keys visible to one step -- the workflow's, its job's, its own.

    The workflow level was added by Plan 162 Stage U, which sets the
    declared-skips gate there precisely so that a job nobody has written yet
    inherits it. Reading only the two inner scopes would have reported every
    step as ungated.
    """
    document = yaml.safe_load(_read(WORKFLOW))
    for job in document["jobs"].values():
        if job.get("name") != job_name:
            continue
        keys = list(document.get("env", {})) + list(job.get("env", {}))
        for step in job.get("steps", []):
            if _step_name(step) == step_name:
                keys += list(step.get("env", {}))
        return tuple(keys)
    return ()


# The second mechanically checkable clause of the same rule, added by Stage 6b
# after the class the row above calls judgement produced another instance.
ENCODING_WAIVERS = ()

# Not source, and ``.claude/`` is the one that matters: in the primary checkout
# it holds every active worktree, so walking it would report each violation
# once per worktree and make the count depend on how many branches happen to be
# open. The rest mirror ``[tool.ruff] exclude``.
_NOT_SOURCE = frozenset({
    ".claude", ".git", "__pycache__", ".venv", "venv",
    "node_modules", ".ruff_cache", ".pytest_cache", ".mypy_cache",
    "dbt_packages", "target",
})

# ``pathlib`` and nothing else defines these two, so the receiver needs no type
# inference: an attribute call by this name is a text read or write, whatever
# expression produced the object. That is the entire reason this check exists
# rather than a ruff setting -- see the docstring below.
_TEXT_IO_METHODS = frozenset({"read_text", "write_text"})

# Text-mode subprocess decodes the child's output through the locale, so it is
# the same defect wearing different clothes: the same command yields str on one
# machine and raises on another. Only text mode qualifies -- a bytes-mode call
# has no encoding to state, which is why the text/universal_newlines flags are
# read rather than the function name alone.
_SUBPROCESS_CALLS = frozenset({"run", "Popen", "check_output", "check_call", "call"})
_TEXT_MODE_FLAGS = ("text", "universal_newlines")

# Every logging handler that opens a file takes ``encoding`` and defaults to the
# locale. ``StreamHandler`` is deliberately absent: it wraps an existing stream
# and has no encoding of its own to state.
_FILE_LOG_HANDLERS = frozenset({
    "FileHandler", "RotatingFileHandler", "TimedRotatingFileHandler",
    "WatchedFileHandler",
})


def _source_files() -> list[Path]:
    """Every Python file in the repository, minus the ones that are not it.

    Enumerated through git rather than by walking the tree, because a walk
    cannot see ``.gitignore`` and the working tree holds files the repository
    does not own. ``graphify-out/`` is the instance that taught us: a local
    scratch directory whose generated ``.py`` files carry a BOM, so the walk
    handed one to ``ast.parse`` and every developer who had run that tool got a
    ``SyntaxError`` on a file no commit contains, while CI -- checking out
    fresh -- stayed green. That is the inverse of the defect this rule exists
    to catch, and the same reason: a check whose subject depends on the machine
    running it.

    ``--cached --others --exclude-standard`` is the tracked files plus the
    untracked ones git would not ignore, so a new file is checked before it is
    added and ignored scratch never is. ``_NOT_SOURCE`` still applies on top,
    for the directories that are tracked but are not this repository's source.
    """
    listed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z",
         "--", "*.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
        text=True,
        encoding="utf-8",
    ).stdout
    return sorted(
        REPO_ROOT / name
        for name in listed.split("\0")
        if name and not _NOT_SOURCE & set(Path(name).parts)
    )


def _encoding_free_text_io(source: str, filename: str = "<canary>") -> set[int]:
    """Line numbers in *source* where a text operation names no encoding.

    Three shapes, each identified by name rather than by inferring the type of
    a receiver, because every one of them is unambiguous by name in this
    repository: the two ``pathlib`` methods, a text-mode subprocess, and a
    logging handler that opens a file.

    Separated from the check below so the rule itself can be tested. A
    structural check nothing exercises is a check that quietly stops matching
    and reports a clean repository either way, which is the failure this file
    exists to prevent.
    """
    tree = ast.parse(source, filename=filename)
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if any(keyword.arg == "encoding" for keyword in node.keywords):
            continue
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}

        if isinstance(node.func, ast.Attribute):
            name = node.func.attr
        elif isinstance(node.func, ast.Name):
            name = node.func.id
        else:
            continue

        if isinstance(node.func, ast.Attribute) and name in _TEXT_IO_METHODS:
            found.add(node.lineno)
        elif name in _SUBPROCESS_CALLS and any(
            isinstance(keywords.get(flag), ast.Constant)
            and keywords[flag].value is True
            for flag in _TEXT_MODE_FLAGS
        ):
            found.add(node.lineno)
        elif name in _FILE_LOG_HANDLERS:
            found.add(node.lineno)
    return found


def test_the_encoding_rule_sees_the_shape_ruff_cannot():
    """Stage 6b's exit criterion, kept as an assertion rather than a measurement.

    The first line is the defect that broke master on 2026-09-01, reduced. Ruff
    reports ``All checks passed`` on it under ``PLW1514 --preview``, verified
    the same day; if this rule ever agrees with ruff, it has lost the only
    thing it was built to add and the loss would otherwise be silent.

    The receiver shapes below are the three the repository actually writes.
    ``Path(...)`` is the one ruff already sees, and it is here so that
    narrowing this rule to the fixture idiom alone would fail.

    Lines 5 and 6 are the shapes PEP 597's ``EncodingWarning`` found at runtime
    on 2026-09-01, after this rule had already been written and committed. They
    are asserted statically now, over this repository's files only -- see the
    stage's decision record for why the runtime check that discovered them was
    not kept in CI.
    """
    caught = _encoding_free_text_io(
        'from pathlib import Path\n'
        '(tmp_path / "a.md").write_text("—")\n'
        'target.write_text("x")\n'
        'Path("b.md").read_text()\n'
        'subprocess.run(cmd, capture_output=True, text=True)\n'
        'RotatingFileHandler(path, maxBytes=5)\n'
    )
    assert caught == {2, 3, 4, 5, 6}, (
        "the encoding rule no longer sees every shape: expected lines "
        f"2 through 6, got {sorted(caught)}"
    )

    clean = _encoding_free_text_io(
        '(tmp_path / "a.md").write_text("—", encoding="utf-8")\n'
        'Path("b.md").read_text(encoding="utf-8")\n'
        'archive.read_bytes()\n'
        'tarfile.open(path)\n'
        # Bytes-mode subprocess has no encoding to state, and neither does a
        # handler that wraps an existing stream. Flagging either would make the
        # rule fire on correct code, which is how a rule gets switched off.
        'subprocess.run(cmd, capture_output=True)\n'
        'logging.StreamHandler(sys.stdout)\n'
    )
    assert not clean, (
        f"the encoding rule fires on calls that are already correct: {sorted(clean)}"
    )


def test_every_text_read_and_write_states_its_encoding():
    """The clause the row above called judgement, made mechanical.

    ``Path.write_text`` with no ``encoding=`` does not choose an encoding. It
    asks the operating system, which answers UTF-8 on Linux and cp1252 on
    Windows. An em-dash is three bytes one way and one byte the other, so a
    fixture written without an encoding and read back as UTF-8 -- correctly,
    explicitly -- raises ``UnicodeDecodeError`` on a developer's machine and
    passes in CI. That is ``tests/scripts/test_build_public_roadmap.py`` on
    2026-09-01, and it is the benign direction of this rule: green where it is
    measured, red where the work happens.

    **Ruff's PLW1514 does not cover this and cannot be made to.** It resolves a
    receiver by type, so it fires on ``Path("b.md").write_text(...)`` and stays
    silent on ``(tmp_path / "a.md").write_text(...)`` -- with or without a
    ``Path`` annotation on the fixture. Measured on 2026-09-01 the rule found
    28 call sites and the repository had 213; the 92 built with ``/`` from a
    fixture, which is the idiom nearly every test here uses, were all in the
    silent set, including the one that broke master. Ruff has no plugin
    interface, so a check that reads these calls has to be Python.

    **The division of labour is deliberate.** ``PLW1514`` is enabled in
    ``pyproject.toml`` under ``explicit-preview-rules`` and owns ``open`` and
    ``tempfile.NamedTemporaryFile``, where its type inference is the right
    instrument and this rule's would not be -- ``tarfile.open`` and
    ``os.open`` take no encoding and a name-only check would flag them. This
    rule owns ``read_text`` and ``write_text``, which only ``pathlib``
    defines, so the name alone is proof and no inference is needed. Between
    them there is no gap and no double report.

    **Two further shapes are here because a runtime check found them and this
    one had not.** ``subprocess.run(..., text=True)`` decodes the child's
    output through the locale, and ``logging.RotatingFileHandler`` writes its
    file the same way -- 21 sites, one of them the ops log that
    ``ops/routers/admin.py`` reads. PEP 597's ``EncodingWarning`` surfaced
    them; it is not in CI, because as an interpreter-wide flag it also judges
    dbt's and Airflow's own file handling by this repository's policy, and its
    attribution is unreliable -- the same warning was blamed on the caller
    locally and on ``configparser`` in CI. Both are recorded in the stage's
    decision record. The shapes it taught us are checked here instead, over
    this repository's files, where ownership is not in question.

    What this does **not** close is the rest of the class. Path separators,
    line endings and case-insensitive filesystems still decide outcomes that
    only a second platform can see, and CI is ``ubuntu-latest`` in all ten
    jobs. Stage 6b's decision record says why that is accepted rather than
    fixed with a Windows runner, and success criterion 2 names it.
    """
    found = {
        f"{_relative(path)}:{line}"
        for path in _source_files()
        for line in _encoding_free_text_io(
            path.read_text(encoding="utf-8"), filename=str(path)
        )
    }
    _assert_exactly(
        found,
        ENCODING_WAIVERS,
        "these text reads and writes let the machine choose the encoding, so "
        "their result depends on the locale of whoever runs them:",
    )


# ---------------------------------------------------------------------------
# Rule 8 -- the instrument can see every service, and its number is read.
# ---------------------------------------------------------------------------
COVERAGE_CONFIG = "pyproject.toml"


@lru_cache(maxsize=None)
def coverage_sources() -> frozenset[str]:
    """``[tool.coverage.run] source``, as written."""
    config = tomllib.loads(_read(COVERAGE_CONFIG))
    source = config["tool"]["coverage"]["run"]["source"]
    assert source, f"{COVERAGE_CONFIG} declares an empty coverage source list"
    return frozenset(source)


@lru_cache(maxsize=None)
def coverage_omissions() -> frozenset[str]:
    """``[tool.coverage.run] omit``, as written. Absent is empty, not an error."""
    config = tomllib.loads(_read(COVERAGE_CONFIG))
    return frozenset(config["tool"]["coverage"]["run"].get("omit", ()))


@lru_cache(maxsize=None)
def script_buckets() -> dict[str, bool]:
    """Each ``scripts/`` bucket the contract declares, to "is it measured?"."""
    rows = {
        path.rstrip("/"): answer == "yes"
        for path, answer in _SCRIPT_BUCKET_ROW.findall(_read(CONTRACT))
    }
    assert "scripts" in rows, (
        f"{CONTRACT} no longer says whether the top level of scripts/ is "
        f"measured. See 'Where scripts sit, and what the directory declares'."
    )
    return rows


def test_every_script_directory_is_classified():
    """A script directory nobody placed is three guesses, not one gap.

    ``scripts/`` carries production tooling and spent tooling in the same
    tree, and the path is the only thing that says which. A subdirectory the
    contract does not classify is read one way by ``[tool.coverage.run]``,
    another by ``scripts/ci_change_scope.py``, and a third by whoever opens it
    -- and nothing makes them agree. Plan 162 Stage G split the tree; this is
    what stops the next bucket arriving undeclared.
    """
    on_disk = {
        _relative(path)
        for path in (REPO_ROOT / "scripts").iterdir()
        if path.is_dir() and path.name != "__pycache__"
    }
    unclassified = sorted(on_disk - set(script_buckets()))
    assert not unclassified, (
        f"{CONTRACT} classifies no script directory {unclassified}. Add a row "
        f"to 'Where scripts sit, and what the directory declares' saying "
        f"whether it is in the coverage denominator and what belongs in it."
    )

    phantom = sorted(
        bucket for bucket in script_buckets()
        if not (REPO_ROOT / bucket).is_dir()
    )
    assert not phantom, (
        f"{CONTRACT} classifies script directories that do not exist: "
        f"{phantom}. A bucket described but absent is a rule about nothing."
    )


def test_every_unmeasured_script_bucket_is_omitted_from_coverage():
    """The prose and the config are one statement, asserted in both directions.

    A bucket the contract calls unmeasured that coverage still counts is a
    denominator nobody chose; one that coverage omits while the contract calls
    it measured is code that silently stopped being graded. The second is the
    dangerous direction and the reason this asserts both.
    """
    for bucket, measured in sorted(script_buckets().items()):
        omitted = any(
            pattern.rstrip("*").rstrip("/") == bucket
            for pattern in coverage_omissions()
        )
        assert omitted is not measured, (
            f"{CONTRACT} calls `{bucket}/` "
            f"{'measured' if measured else 'unmeasured'}, but "
            f"{COVERAGE_CONFIG}'s [tool.coverage.run] omit "
            f"{'omits' if omitted else 'does not omit'} it."
        )


def test_every_service_directory_is_measured_by_coverage():
    """G10's first half: a service coverage cannot see reads as covered.

    Until Plan 162 Stage C this list named six packages and omitted
    ``container_health``, ``dashboard``, ``scripts`` and ``airflow/dags`` --
    so the two services the "enough" table puts furthest below the floor were
    the two the instrument was blind to, and the 88% it reported was 88% of
    the code already being tested. Adding them moved the honest number to 76%.

    The forward direction is what this rule is for: a service package added
    without a line here is measured by nothing, silently, and the total goes
    *up* for it. ``scripts`` and ``airflow/dags`` are in ``source`` for the
    same reason but cannot be demanded by the same derivation -- neither is a
    package, so :func:`service_packages` does not see them. The phantom check
    below is what still catches those two being renamed away.
    """
    missing = sorted(service_packages() - coverage_sources())
    assert not missing, (
        f"service directories absent from [tool.coverage.run] source in "
        f"{COVERAGE_CONFIG}: {missing}. Coverage reports a percentage of what "
        f"it was pointed at, so an unlisted service does not lower the "
        f"number -- it disappears from it."
    )

    phantom = sorted(
        entry for entry in coverage_sources()
        if not (REPO_ROOT / entry).is_dir()
    )
    assert not phantom, (
        f"[tool.coverage.run] source in {COVERAGE_CONFIG} names directories "
        f"that do not exist: {phantom}. Coverage skips them without "
        f"complaint, which is how a renamed package stops being measured."
    )


def test_the_coverage_number_the_unit_job_produces_is_consumed():
    """G10's second half, and the half that was the whole of the gap.

    ``--cov`` with nothing reading the result is a step that cannot fail on
    coverage, which is a measurement and not a check. Plan 139 Stage A added
    the measurement and stopped there. Both directions are asserted: a step
    that measures without a floor, and a floor with nothing measuring, are the
    same gap wearing different clothes.

    The flags are compared as whole tokens rather than by substring, because
    ``"--cov" in args`` is true of ``--cov-fail-under`` too -- which would make
    the second half of this check unreachable and turn it into decoration.
    """
    measuring = {
        f"{job}: {step}" for job, step, args in pytest_steps()
        if any(flag == "--cov" or flag.startswith("--cov=")
               for flag in args.split())
    }
    gated = {
        f"{job}: {step}" for job, step, args in pytest_steps()
        if any(flag.startswith("--cov-fail-under") for flag in args.split())
    }
    ungated = sorted(measuring - gated)
    assert not ungated, (
        f"pytest steps in {WORKFLOW} that measure coverage and set no "
        f"threshold: {ungated}. The number is produced and discarded, which "
        f"is exactly what G10 recorded."
    )
    unmeasured = sorted(gated - measuring)
    assert not unmeasured, (
        f"pytest steps in {WORKFLOW} that set a coverage threshold without "
        f"measuring coverage: {unmeasured}."
    )


# ---------------------------------------------------------------------------
# Rule 9 -- every skip in a CI run is declared, and the gate that says so runs.
#
# The runtime half is ``tests/plugins/declared_skips.py``, which fails a run on
# an undeclared skip, on a declared skip that stopped skipping, and on one that
# skipped for something other than the condition it declares. What lives here
# is the half a hook cannot check about itself: that the registry names real
# tests, that it may not be used where a skip is inadmissible, that the gate
# and the plugin registration are both still in place, and that the registry
# only shrinks.
# ---------------------------------------------------------------------------
PLUGIN_MODULE = "tests.plugins.declared_skips"

# The layers where no declaration is admissible at all. Layer 2 is the whole
# list and the reason is the plan's origin: a Layer 2 test that skips executes
# no SQL against a real engine, and the statement it covers is the one that
# reached production unexecuted. ``REQUIRE_LAYER_2_EXECUTION`` used to say this
# by naming ``tests/integration/sql/`` in that suite's own conftest, which made
# it a fact about one path. Said as a layer it is derived from the contract's
# headings through :func:`_layer_of`, so a second Layer 2 root is strict on the
# day the contract declares it rather than on the day someone remembers.
LAYERS_ADMITTING_NO_SKIP = frozenset({2})


@lru_cache(maxsize=None)
def _test_ids(relative: str) -> frozenset[str]:
    """Every ``Class::function`` and bare ``function`` id defined in a test file."""
    tree = ast.parse((REPO_ROOT / relative).read_text(encoding="utf-8"))
    ids = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            ids.add(node.name)
        elif isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    ids.add(f"{node.name}::{child.name}")
    return frozenset(ids)


def test_every_declared_skip_names_a_test_that_exists():
    """A declaration that names nothing is a comment with a dataclass around it.

    The nodeid is the join between the registry and the run, so a renamed or
    deleted test has to fail here rather than quietly stop matching -- at which
    point the entry would sit in the tuple describing a decision about a test
    nobody can find, and the *real* skip, if it came back under the new name,
    would read as undeclared and fail a run with no explanation attached.
    """
    missing = []
    for entry in DECLARED_SKIPS:
        path, _, test_id = entry.nodeid.partition("::")
        if not (REPO_ROOT / path).is_file():
            missing.append(f"{entry.nodeid} -- no such file")
        elif test_id not in _test_ids(path):
            missing.append(f"{entry.nodeid} -- {path} defines no {test_id}")
    assert not missing, (
        "DECLARED_SKIPS entries naming a test that does not exist:\n  "
        + "\n  ".join(missing)
        + "\nRename the entry with the test, or delete it."
    )


def test_no_declared_skip_sits_at_a_layer_that_admits_none():
    """Retiring ``REQUIRE_LAYER_2_EXECUTION`` may not loosen Layer 2.

    The general gate offers a door the suite-scoped hook did not: a skip there
    used to be unconditionally fatal, and under a registry someone could make
    one legal by adding four lines. This is the door being nailed shut for the
    layer that needed it shut, in the one form that does not have to be
    maintained -- :func:`_layer_of` reads the contract's own headings, so this
    rule follows the contract rather than a path list beside it.

    It fails at the registry rather than in the hook, which is deliberate: the
    plugin loads in a job that installs three packages and must stay importable
    with nothing but the standard library, and a rule read out of
    ``docs/TESTING.md`` is not that.
    """
    layers = {
        entry.nodeid: _layer_of((REPO_ROOT / entry.nodeid.split("::")[0]).parent)
        for entry in DECLARED_SKIPS
    }
    inadmissible = sorted(
        f"{nodeid} (Layer {layer})"
        for nodeid, layer in layers.items()
        if layer in LAYERS_ADMITTING_NO_SKIP
    )
    assert not inadmissible, (
        f"these skips are declared at a layer that admits none "
        f"{sorted(LAYERS_ADMITTING_NO_SKIP)}:\n  " + "\n  ".join(inadmissible)
        + "\nA Layer 2 test that skips executes no SQL. Fix the fixture the "
        "test depends on; there is no declaration for this."
    )


def test_every_pytest_step_runs_under_the_declared_skip_gate():
    """Both halves, because either one alone is inert.

    Nothing guarded ``REQUIRE_LAYER_2_EXECUTION``. It was one line of YAML, and
    deleting it would have restored the blind spot with no test failing and no
    reviewer prompted -- which is still true today of ``REQUIRE_DUCKDB``,
    ``REQUIRE_MINIO`` and ``REQUIRE_AIRFLOW_SCHEMA``. So the general gate is
    asserted from the workflow file itself, and loosening it now costs a diff
    that touches this docstring.

    The registration is the other half: with the gate set and no ``-p``, every
    job passes having loaded no hook at all. ``docs-tests`` is why it is
    ``addopts`` rather than a conftest -- it runs ``--noconftest``, and
    ``pythonpath`` is what makes the module importable at the point ``-p``
    resolves it, which is before the collection that would otherwise put the
    repository root on ``sys.path``.
    """
    ungated = sorted(
        f"{job}: {step}"
        for job, step, _ in pytest_steps()
        if GATE not in _step_env(job, step)
    )
    assert not ungated, (
        f"pytest steps in {WORKFLOW} that do not run under {GATE}: {ungated}. "
        f"The gate is set once at workflow level so that every job inherits "
        f"it; a step this reports has shadowed or removed it."
    )

    config = tomllib.loads(_read("pyproject.toml"))["tool"]["pytest"]["ini_options"]
    assert f"-p {PLUGIN_MODULE}" in config.get("addopts", ""), (
        f"pyproject.toml no longer registers {PLUGIN_MODULE} through addopts, "
        f"so {GATE} is set in every job and read in none of them."
    )
    assert "." in config.get("pythonpath", ()), (
        f"pyproject.toml no longer puts the repository root on pythonpath, so "
        f"`-p {PLUGIN_MODULE}` cannot import at plugin-registration time."
    )


def test_the_declared_skip_registry_only_ratchets_down():
    """The ``--cov-fail-under`` idiom, pointed the other way.

    A ceiling is what stops a third declaration being a quiet tuple append.
    With one, adding a skip means moving a number that carries a comment, in
    the same diff, and the number is the thing review argues about -- which is
    the entire difference between a registry and a place to put things.

    It fails in both directions for the same reason the waiver checks do: a
    ceiling left above the real count is a budget nobody spent and everybody
    may.
    """
    assert len(DECLARED_SKIPS) <= DECLARED_SKIP_CEILING, (
        f"{len(DECLARED_SKIPS)} declared skips against a ceiling of "
        f"{DECLARED_SKIP_CEILING}. Fix the cause, or make the case for raising "
        f"the ceiling in the same diff."
    )
    assert len(DECLARED_SKIPS) == DECLARED_SKIP_CEILING, (
        f"DECLARED_SKIP_CEILING is {DECLARED_SKIP_CEILING} and there are "
        f"{len(DECLARED_SKIPS)} declared skips. A stage that removed one lowers "
        f"the ceiling with it; headroom left behind is headroom that gets used."
    )


# ---------------------------------------------------------------------------
# The contract's claims about its own enforcement.
# ---------------------------------------------------------------------------
_ASSERTED_BY_ROW = re.compile(r"^\|[^|]+\|([^|]+)\|[^|]+\|\s*$", re.M)
_TEST_NAME = re.compile(r"`(test_\w+)`")


def test_every_asserted_rule_names_a_real_test():
    """The rules table may not claim a check the suite does not implement.

    Every other check in this file compares the contract to the repository.
    This one compares the contract to *this file*, and it exists because that
    was the one direction nothing looked in.

    Found on 2026-08-31, during Plan 162's first run of this suite rather than
    by anything failing: the Layer 2 row read "every ``.sql`` file **and
    module-level statement** is executed by a Layer 2 test". Only the
    ``.sql``-file half was ever implemented. The inline-SQL half is G5, it is
    measured by nothing, and the table had been asserting otherwise since the
    day it was written. A document drifting from its mechanism while claiming
    to be the mechanism is precisely what ``ARCHITECTURE.md:179`` did, one
    document later.

    **Only the forward direction is asserted** -- every rule the table names
    exists. The reverse, that every test here appears in the table, is not
    checked and deliberately: the waiver-hygiene checks below have no rule row
    of their own, and enumerating the exceptions would need exactly the
    curated list this file refuses to keep. So a rule can be implemented
    without a row. A row cannot exist without a rule, and overclaiming was the
    failure that happened.
    """
    section = _read(CONTRACT).split("## What CI asserts")[1]
    section = section.split("### Specified here")[0]
    matched = _ASSERTED_BY_ROW.findall(section)
    assert len(matched) > 2, (
        f"no rules table parsed out of {CONTRACT} under 'What CI asserts'"
    )
    # A markdown table opens with exactly two non-content rows. Asserting their
    # shape rather than skipping them blind means a reordered or renamed column
    # fails here instead of silently exempting the first real rule.
    header, separator, *rows = matched
    assert header.strip() == "Asserted by", (
        f"the second column of the rules table in {CONTRACT} is "
        f"'{header.strip()}', not 'Asserted by'"
    )
    assert set(separator.strip()) <= set("-:"), (
        f"expected a markdown separator row, got '{separator.strip()}'"
    )

    # **Every test module, not just this one.** Until Plan 162 Stage X this
    # read only this file, which made the rules table unable to name a check
    # living anywhere else -- and the first rule that legitimately does arrived
    # with that stage: `PREPARE`-ing every `tests/sql/` statement needs an
    # engine, so it belongs in a Layer 2 suite, not in this Layer 0 file. The
    # narrow reading would have forced the choice between a row that lies about
    # where its check is and no row at all.
    #
    # Widening it is derived rather than listed -- a second hardcoded filename
    # would be the inventory this file refuses to keep -- and it costs only
    # that a name could be satisfied by a same-named test in another module,
    # which is a weaker guard than the original for a claim nobody makes by
    # accident.
    defined = {
        node.name
        for path in all_test_modules()
        for node in ast.parse(path.read_text(encoding="utf-8"), filename=str(path)).body
        if isinstance(node, ast.FunctionDef)
    }

    unnamed = [cell.strip() for cell in rows if not _TEST_NAME.search(cell)]
    assert not unnamed, (
        f"rules in {CONTRACT} with no test named in the 'Asserted by' column: "
        f"{unnamed}. A rule with no test belongs under 'Specified here, not "
        f"yet asserted', where it is honest about being unenforced."
    )

    phantom = sorted(
        name
        for cell in rows
        for name in _TEST_NAME.findall(cell)
        if name not in defined
    )
    assert not phantom, (
        f"{CONTRACT} names these as asserting a rule, and they do not exist "
        f"in {Path(__file__).name}: {phantom}. Either the check was never "
        f"written, or it was renamed and the contract now describes a "
        f"mechanism that is not there."
    )


# ---------------------------------------------------------------------------
# Rule 10 -- a handler observes the mutation it performs.
# ---------------------------------------------------------------------------
# Plan 162 Stage Y found this by writing the stage above it. `toggle_search`
# executed `UPDATE search_configs ... WHERE search_key = %s`, never read
# `rowcount`, and returned 303 unconditionally: one code declared, one code
# produced, declared equals produced -- and it reported success whether the key
# existed or not. The route rule passes it. The response rule passes it. What
# nothing asked was whether the work happened.
#
# **The scope is a WHERE-bearing UPDATE or DELETE**, because that is the shape
# where "no rows matched" is a different outcome from "one row matched" and only
# the caller can say which one the client should hear about. An INSERT that
# raises on conflict needs no rowcount, and a statement with no WHERE addresses a
# set rather than a row.
#
# **There is no waiver ledger, and that is deliberate.** One landed with
# `scripts/check_sql_execution_coverage.py` shaped like the `*_WAIVERS` tuples
# and was deleted rather than kept empty, with the argument recorded in
# `docs/TESTING.md`: an empty ledger and no ledger differ in exactly one way --
# what the next violation costs to repair. With a ledger that is a tuple append;
# without one it is a repair, and restoring the escape hatch is a diff that has
# to argue for itself. Thirteen sites were measured and all thirteen resolved,
# so there is nothing to grandfather.
_MUTATING_VERB = re.compile(r"\b(?:UPDATE|DELETE\s+FROM)\b", re.I)
_SQL_WHERE = re.compile(r"\bWHERE\b", re.I)
_SQL_RETURNING = re.compile(r"\bRETURNING\b", re.I)
_SQL_LINE_COMMENT = re.compile(r"--.*")
_CURSOR_READS = frozenset({"fetchone", "fetchall", "fetchmany"})
_SQL_EXECUTING_CALLS = frozenset({"execute", "executemany", "execute_values"})


@lru_cache(maxsize=None)
def mutating_statements() -> dict[str, bool]:
    """Each mutating ``.sql`` path -> whether it carries ``RETURNING``.

    Derived from :func:`production_sql_files`, so it inherits that corpus's
    exemptions rather than deciding its own. Comments are stripped first:
    several of these files explain their own ``WHERE`` at length, and one of
    them says the word ``RETURNING`` in prose.
    """
    found: dict[str, bool] = {}
    for relative in production_sql_files():
        body = _SQL_LINE_COMMENT.sub("", _read(relative))
        if _MUTATING_VERB.search(body) and _SQL_WHERE.search(body):
            found[relative] = bool(_SQL_RETURNING.search(body))
    return found


@lru_cache(maxsize=None)
def query_constants() -> dict[str, str]:
    """``CONSTANT`` -> the ``.sql`` path it loads, as ``load_query`` resolves it.

    Parsed rather than pattern-matched, because the re-exports wrap: three of
    them are ``NAME = (\\n    shared_queries.NAME\\n)`` and a line-oriented read
    sees an assignment with no value. ``shared/queries.py`` is resolved first so
    the services that re-export from it resolve too.
    """
    modules = [
        path for path in REPO_ROOT.rglob("queries.py")
        if "__pycache__" not in path.parts
        and not path.relative_to(REPO_ROOT).as_posix().startswith("tests/")
    ]
    modules.sort(key=lambda p: p.parent.name != "shared")

    constants: dict[str, str] = {}
    for module in modules:
        sql_dir = (module.parent / "sql").relative_to(REPO_ROOT).as_posix()
        tree = ast.parse(module.read_text(encoding="utf-8"))
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if not targets:
                continue
            value = node.value
            if (isinstance(value, ast.Call) and isinstance(value.func, ast.Name)
                    and value.func.id == "_q" and value.args
                    and isinstance(value.args[0], ast.Constant)):
                path = f"{sql_dir}/{value.args[0].value}.sql"
            elif (isinstance(value, ast.Attribute)
                  and isinstance(value.value, ast.Name)
                  and value.value.id == "shared_queries"
                  and value.attr in constants):
                path = constants[value.attr]
            else:
                continue
            for name in targets:
                constants[name] = path
    return constants


def _names_bound_from(function: ast.AST, attribute: str) -> set[str]:
    """``x`` for every ``x = <anything>.attribute`` in *function*."""
    bound = set()
    for node in ast.walk(function):
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Attribute)
                and node.value.attr == attribute):
            bound |= {t.id for t in node.targets if isinstance(t, ast.Name)}
    return bound


def _names_bound_from_a_read(function: ast.AST) -> dict[str, int]:
    """``x`` -> line, for every ``x = cur.fetchone()`` and its kin."""
    bound: dict[str, int] = {}
    for node in ast.walk(function):
        if not isinstance(node, ast.Assign):
            continue
        call = node.value
        if isinstance(call, ast.ListComp):
            call = call.generators[0].iter if call.generators else None
        if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr in _CURSOR_READS):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    bound[target.id] = node.lineno
    return bound


def _outcome_subtrees(function: ast.AST):
    """Every expression a function's behaviour actually turns on.

    An ``If`` contributes its **test** only. A name appearing inside a branch it
    did not choose is not the branch being taken on it, and crediting that is
    how a rule starts passing things it should not.
    """
    for node in ast.walk(function):
        if isinstance(node, ast.If):
            yield node.test
        elif isinstance(node, (ast.Return, ast.Raise)):
            yield node
        elif isinstance(node, ast.Dict):
            yield from node.values


def _observes_rowcount(function: ast.AST) -> bool:
    """Does ``rowcount`` reach a branch, a raise, or something returned?

    Both spellings, because both are in the tree: `revoke_user` binds it
    (``matched = cur.rowcount``) and `_set_status` reads it in place
    (``if not cur.rowcount:``). Reading only the binding form reported six
    compliant functions as violations the first time this rule ran.
    """
    bound = _names_bound_from(function, "rowcount")
    for subtree in _outcome_subtrees(function):
        for child in ast.walk(subtree):
            if isinstance(child, ast.Attribute) and child.attr == "rowcount":
                return True
            if isinstance(child, ast.Name) and child.id in bound:
                return True
    return False


def _reaches_an_outcome(function: ast.AST, names: set[str]) -> bool:
    """Does one of *names* reach a branch, a raise, or something returned?

    **This is the clause that stops the rule being satisfiable by a dead
    assignment.** ``matched = cur.rowcount`` followed by nothing reads the
    rowcount and changes no behaviour, which is `assert True` wearing a
    different hat -- and this stage exists because of an assertion that could
    not fail. An ``If`` is searched by its **test** only, so a name that merely
    appears inside a branch it did not choose does not count.
    """
    if not names:
        return False
    for node in ast.walk(function):
        if isinstance(node, ast.If):
            subtrees = [node.test]
        elif isinstance(node, (ast.Return, ast.Raise)):
            subtrees = [node]
        elif isinstance(node, ast.Dict):
            subtrees = list(node.values)
        else:
            continue
        for subtree in subtrees:
            for child in ast.walk(subtree):
                if isinstance(child, ast.Name) and child.id in names:
                    return True
    return False


def _is_gated_by_an_earlier_read(function: ast.AST, before: int) -> bool:
    """Did a read *before* line ``before`` establish the row and gate on it?

    The third way a mutation can be observed, and the reason `advance_rotation`
    and `_reap_stuck_processing` needed no repair: the row's existence is
    settled by a ``SELECT`` whose result the function then branches on or
    iterates, so a mutation keyed on that row cannot match nothing.

    **Scoped to one function on purpose.** `_record_last_used` was correct by a
    gate three early-returns up in its caller, which no reading of this function
    could have seen -- so Stage Y moved the write next to the gate rather than
    teaching this rule to walk the call graph. A checker that follows callers is
    a checker that can be silently wrong, which is the thing being repaired.
    """
    reads = {name: line for name, line in _names_bound_from_a_read(function).items()
             if line < before}
    if not reads:
        return False
    for node in ast.walk(function):
        if isinstance(node, ast.If):
            gate_returns = any(
                isinstance(child, (ast.Return, ast.Raise))
                for child in ast.walk(node)
            )
            names_in_test = {
                child.id for child in ast.walk(node.test)
                if isinstance(child, ast.Name)
            }
            if gate_returns and names_in_test & set(reads):
                return True
        if isinstance(node, ast.For):
            for child in ast.walk(node.iter):
                if isinstance(child, ast.Name) and child.id in reads:
                    return True
    return False


def _unobserved_mutations(path: Path) -> list[str]:
    """``file:function`` for every mutation this module performs and ignores."""
    statements = mutating_statements()
    constants = query_constants()
    relative = path.relative_to(REPO_ROOT).as_posix()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    found = []
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        # Keyed on the statement being *named*, never on where it is executed.
        # `ops/routers/admin.py` binds it first -- `sql = TOGGLE_SEARCH_CONFIG_
        # ENABLED` then `cur.execute(sql, params)` -- so a rule reading the
        # execute call's arguments does not see those three handlers at all.
        # The first draft of this rule did exactly that and reported them
        # compliant; the mutation below caught it. It is G5's finding again: a
        # rule keyed on the call site is escapable by not using the call site.
        executed = [
            (node.lineno, statements[constants[node.id]])
            for node in ast.walk(function)
            if isinstance(node, ast.Name) and constants.get(node.id) in statements
        ]
        if not executed:
            continue

        # A function that names a statement but executes nothing has handed it
        # to somebody else -- `expire_orphan_detail_claims` is
        # `return _run_maintenance_query(EXPIRE_ORPHAN_DETAIL_CLAIMS, ())`, and
        # the helper reads the RETURNING rows and counts them. Delegation is
        # observed when the delegate's answer is what this function returns; a
        # result thrown away is not, and still fails.
        executes = any(
            isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in _SQL_EXECUTING_CALLS
            for node in ast.walk(function)
        )
        if not executes:
            delegated = any(
                isinstance(child, ast.Name) and constants.get(child.id) in statements
                for node in ast.walk(function)
                if isinstance(node, ast.Return)
                for child in ast.walk(node)
            )
            if delegated:
                continue
        observes_rowcount = _observes_rowcount(function)
        reads_a_result = any(
            isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in _CURSOR_READS
            for node in ast.walk(function)
        )
        for line, has_returning in executed:
            if observes_rowcount:
                continue
            if has_returning and reads_a_result:
                continue
            if _is_gated_by_an_earlier_read(function, line):
                continue
            found.append(f"{relative}:{function.name}")
    return sorted(set(found))


def test_every_mutation_observes_whether_it_changed_anything():
    """A route that reports success for work it did not do is the whole of G27.

    Three ways to satisfy it, and all three were found in the repository rather
    than invented for it: read ``rowcount`` and let it reach an outcome, take
    ``RETURNING`` and read the result, or settle the row with a read this
    function already branches on. Eight sites were repaired, five already
    complied, and one was restructured so its gate and its write share a frame.

    What this cannot see is a mutation whose guard lives in another function.
    That is stated rather than papered over: `_record_last_used` was moved for
    exactly this reason, and a rule that chased it into its caller would be a
    dataflow analysis that fails quietly, which is worse than one that fails.
    """
    found = set()
    for path in production_python_files():
        found |= set(_unobserved_mutations(path))

    assert not found, (
        "these functions execute a WHERE-bearing UPDATE or DELETE and never "
        "learn whether it matched anything, so they answer the same way when "
        "it did not:\n  " + "\n  ".join(sorted(found)) +
        "\n\nRead cur.rowcount and branch on it, take RETURNING and read the "
        "result, or gate the write on a read in the same function. There is no "
        "waiver list for this rule and adding one is a decision, not a fix."
    )


def test_the_mutation_corpus_is_not_empty():
    """A set difference over an empty corpus is empty, and reads as compliance.

    The same floor `test_the_production_sql_corpus_is_not_empty` puts under the
    coverage numbers. A glob that stops matching, or a `queries.py` that stops
    being parsed, would silently retire the rule above.
    """
    statements = mutating_statements()
    assert len(statements) >= 30, (
        f"only {len(statements)} mutating statements found; the rule above is "
        f"measuring almost nothing. Check production_sql_files() and the "
        f"UPDATE/DELETE pattern before trusting a green run."
    )
    constants = query_constants()
    resolved = {name for name, path in constants.items() if path in statements}
    assert len(resolved) >= 20, (
        f"only {len(resolved)} constants resolve to a mutating statement, out "
        f"of {len(constants)} parsed. query_constants() has stopped following "
        f"how load_query resolves a name."
    )


# ---------------------------------------------------------------------------
# Rule 10b -- a swallowed write changes what the caller is told.
# ---------------------------------------------------------------------------
# The other half of G27, and the same defect arriving a second way. A mutation
# can go unobserved because nobody read its rowcount, or because an exception
# ate it and the handler carried on to the success path it would have reached
# anyway. `revoke_user` had both: no row matched *and* a raising DELETE were
# each reported to the admin as a successful revocation.
#
# **A swallowed read is not this.** It gives the caller less data, and an empty
# page or a missing field is visible. A swallowed *write* gives the caller a
# success message for work that did not happen, and
# `scraper/processors/scrape_detail.py:196` is the control case for the
# difference: it swallows a MinIO write and binds `minio_write_error` into the
# artifact it returns, so the caller can tell. That is the shape this rule asks
# for, and it was already in the tree.
#
# **Scoped to route handlers**, because the rule is about what a route tells its
# client. A helper that swallows and returns nothing tells nobody anything --
# `ops/routers/snapshots.py` swallows its `last_used_at` write deliberately, and
# Plan 173 argues it in prose: a credential that has already authenticated must
# not be refused because its bookkeeping failed. Distinguishing *the operation*
# from *bookkeeping incidental to it* is a judgement no AST can make, so the
# rule asks the question only where a response exists to be wrong.
SWALLOWED_WRITE_WAIVERS: tuple[Waiver, ...] = (
    # Both call `dbt_runner` endpoints that were deleted in April and May
    # (9f08336, d88a41e), swallow the failure with a bare `except Exception:
    # pass`, and return the same 303 they would on success -- which is why the
    # admin dbt panel has done nothing since April without reporting it.
    #
    # **Waived rather than repaired, and unlike the rowcount clause above this
    # one needs a ledger.** There the thirteen sites all resolved and an empty
    # list was deleted; here two violations are genuinely outstanding and their
    # repair is not this stage's to make: whether the intent UI is deleted or
    # `dbt_runner` regains the endpoints is the same decision as what the
    # repaired test asserts, and Stage AA owns it. A waiver with a named owner
    # is exactly the object for that, and it dies when Plan 162 archives.
    Waiver("ops/routers/admin.py:dbt_intent_upsert", "G27", 162, date(2026, 9, 8)),
    Waiver("ops/routers/admin.py:dbt_intent_delete", "G27", 162, date(2026, 9, 8)),
)

_HTTP_WRITE_VERBS = frozenset({"post", "put", "patch", "delete"})


def _route_decorated(function: ast.AST) -> bool:
    """Is this function registered as a route?

    Reads the decorator rather than the app's routing table on purpose: this
    rule is about the shape of the source, and importing six FastAPI apps to
    answer a question the decorator already answers is the harness deciding
    another test's outcome.
    """
    return any(
        isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr in _HTTP_VERBS | {"api_route"}
        for decorator in getattr(function, "decorator_list", [])
    )


def _performs_a_write(block: list[ast.stmt]) -> bool:
    """Does this ``try`` body mutate anything -- our database or someone else's?"""
    module = ast.Module(body=block, type_ignores=[])
    constants = query_constants()
    statements = mutating_statements()
    for node in ast.walk(module):
        if isinstance(node, ast.Name):
            path = constants.get(node.id)
            if path in statements:
                return True
    for node in ast.walk(module):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in _HTTP_WRITE_VERBS):
            return True
    return False


def _handler_exits(block: list[ast.stmt]) -> bool:
    module = ast.Module(body=block, type_ignores=[])
    return any(
        isinstance(node, (ast.Return, ast.Raise)) for node in ast.walk(module)
    )


def _silently_swallowed_writes(path: Path) -> list[str]:
    """``file:handler`` for each route handler that eats a failed write."""
    relative = path.relative_to(REPO_ROOT).as_posix()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    found = []
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not _route_decorated(function):
            continue

        returned = {
            child.id
            for node in ast.walk(function) if isinstance(node, ast.Return)
            for child in ast.walk(node) if isinstance(child, ast.Name)
        }
        for attempt in [n for n in ast.walk(function) if isinstance(n, ast.Try)]:
            if not _performs_a_write(attempt.body):
                continue
            for handler in attempt.handlers:
                if _handler_exits(handler.body):
                    continue
                bound = {
                    target.id
                    for node in ast.walk(ast.Module(body=handler.body,
                                                    type_ignores=[]))
                    if isinstance(node, ast.Assign)
                    for target in node.targets if isinstance(target, ast.Name)
                }
                if bound & returned:
                    continue
                found.append(f"{relative}:{function.name}")
    return sorted(set(found))


def test_no_route_swallows_a_failed_write_and_reports_success():
    """The second half of the same defect: the write failed and nobody said so.

    Satisfied three ways, and the first two are already how most of the tree
    behaves: return or raise from the handler, or bind something in the
    ``except`` that the response carries. Only falling through to the success
    path with nothing recorded fails.
    """
    found = set()
    for path in production_python_files():
        found |= set(_silently_swallowed_writes(path))

    _assert_exactly(
        found,
        SWALLOWED_WRITE_WAIVERS,
        "A route handler swallowed a failed write and answered exactly as it "
        "would have on success.",
    )


# ---------------------------------------------------------------------------
# Rule 10c -- an outcome that came back is not thrown away.
# ---------------------------------------------------------------------------
# G27's third clause. The first two ask whether a handler noticed what its own
# statement did; this asks whether it noticed what something else told it.
#
# `_set_intent` returns five statuses -- ok, locked, invalid, unavailable
# (Postgres unreachable) and error (Postgres refused the write, with a reason) --
# and `ops/routers/admin.py`'s deploy buttons call it as a bare statement and
# redirect 303. Clicking **Start deploy** in the admin panel gives the same page
# whether intent was taken, was already held by somebody else, or the database
# was down. `_intent_release`'s own docstring records the near miss: it returned
# a bare `bool` until Plan 162 Stage K "collapsing five outcomes into False" --
# Stage K widened the signal and the caller never started reading it.
#
# **What counts as an outcome type is derived from its shape, not its name**,
# and the difference is load-bearing here. `MaterializationResult` ends in
# `Result` and carries no outcome -- it is a data carrier, and discarding one
# would be legitimate. `CandidateSet` and `PresentationSnapshot` do not end in
# `Result` and both carry `status` and `error`. A rule keyed on the suffix
# would have both of those backwards.
#
# **Scope is why this is narrow enough to be worth having.** 452 functions here
# carry a non-None return annotation and 17 call sites discard one -- but almost
# all of those are correct: `flush()` returning a bool, `write_json()` returning
# the path it wrote, a cache `refresh()`. Requiring every annotated return to be
# consumed would fail fifteen sound call sites to catch two. Requiring it only
# of a value whose whole purpose is to report an outcome fails none of them.
_OUTCOME_FIELDS = frozenset({"status", "ok", "error", "detail", "failure_reason"})


@lru_cache(maxsize=None)
def outcome_types() -> frozenset[str]:
    """Classes defined here that exist to report how something went.

    A class with a ``status``, ``ok``, ``error``, ``detail`` or
    ``failure_reason`` field is answering "what happened", and dropping one on
    the floor drops the answer. Derived by walking annotated class bodies, so a
    new result type is covered the day it is written rather than when somebody
    remembers to add it to a list.
    """
    found = set()
    for path in production_python_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            fields = {
                statement.target.id
                for statement in node.body
                if isinstance(statement, ast.AnnAssign)
                and isinstance(statement.target, ast.Name)
            }
            if fields & _OUTCOME_FIELDS:
                found.add(node.name)
    return frozenset(found)


@lru_cache(maxsize=None)
def outcome_returning_functions() -> frozenset[str]:
    """Functions annotated as returning one of :func:`outcome_types`."""
    outcomes = outcome_types()
    found = set()
    for path in production_python_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.returns is not None and ast.unparse(node.returns) in outcomes:
                found.add(node.name)
    return frozenset(found)


def _discarded_outcomes(path: Path) -> list[str]:
    """``file:line:caller -> callee()`` for each outcome dropped on the floor."""
    producers = outcome_returning_functions()
    relative = path.relative_to(REPO_ROOT).as_posix()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    found = []
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(function):
            # A call in statement position -- its value goes nowhere at all.
            if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)):
                continue
            # A **free function** only, never a method. A free function's
            # return value is its only channel; a method has a receiver to
            # record in, and `public_stats_cache.refresh()` does exactly that --
            # it stores the snapshot on `self` and logs its own failure, so the
            # caller discarding it discards nothing. Flagging it would be the
            # rule failing on correct code.
            if not isinstance(node.value.func, ast.Name):
                continue
            callee = node.value.func.id
            if callee in producers:
                found.append(f"{relative}:{function.name} -> {callee}()")
    return sorted(found)


DISCARDED_OUTCOME_WAIVERS: tuple[Waiver, ...] = (
    # Both are the legacy deploy buttons, and both predate the redeploy work
    # that made them redundant: `scripts/redeploy.sh` drives drain,
    # authorization and release through the coordination API, and these two
    # never moved with it. They are the same neglected-surface class as the
    # `dbt_intent_*` handlers beside them, and Stage AA owns what replaces them
    # -- so they are waived here rather than repaired into a shape nobody wants
    # to keep.
    Waiver("ops/routers/admin.py:deploy_start -> _set_intent()",
           "G27", 162, date(2026, 9, 8)),
    Waiver("ops/routers/admin.py:deploy_complete -> _intent_release()",
           "G27", 162, date(2026, 9, 8)),
)


def test_no_caller_discards_an_outcome_it_asked_for():
    """A five-valued answer thrown away answers nothing.

    The rule that would have caught `deploy_start` -- and the reason it is worth
    having with its violations already known is that both were found by
    accident, one chasing a phantom 422 and one chasing a path-matching tie.
    Neither was found by looking, and the next one should not have to be.
    """
    found = set()
    for path in production_python_files():
        found |= set(_discarded_outcomes(path))

    _assert_exactly(
        found,
        DISCARDED_OUTCOME_WAIVERS,
        "These calls returned a value describing how the work went, and the "
        "caller discarded it.",
    )


def test_the_outcome_type_corpus_is_not_empty():
    """A rule over an empty set of types is a rule about nothing."""
    types_found = outcome_types()
    assert len(types_found) >= 4, (
        f"only {sorted(types_found)} look like outcome types; the rule above "
        f"is measuring almost nothing. Check that production_python_files() "
        f"still resolves and that annotated class bodies are being read."
    )
    producers = outcome_returning_functions()
    assert producers, (
        f"no function is annotated as returning one of {sorted(types_found)}, "
        f"so nothing can be discarded and the rule above cannot fail."
    )


# ---------------------------------------------------------------------------
# Rule 10d -- a response we asked for has its status read.
# ---------------------------------------------------------------------------
# G27's fourth clause, and the one that reaches across a service boundary. The
# first three ask whether a handler noticed what happened locally; this asks
# whether it noticed what the other end said.
#
# **This rule was nearly written with an escape clause, and the escape clause
# was the bug.** Six helpers in `ops/coordination_drain.py` and
# `ops/coordination_release.py` never read the status and were nonetheless
# correct: each subscripts the parsed payload immediately, so an error body
# raises `KeyError` into a handler that returns an `unknown` verdict. The first
# draft of this rule credited that pattern -- "the shape check subsumes the
# status check" -- which is an inference, and one inference away from crediting
# a 200 that happens to parse. **The conforming code was changed instead**, and
# the change was free: `HTTPError` subclasses `RequestException`, which those
# gates already catch and already turn into `unknown`, so behaviour is identical
# and the rule has no clause that can be wrong.
#
# That is the trade this rule records: six explicit checks in working code, in
# exchange for a rule that states one thing and cannot be argued around.
_RESPONSE_INSPECTIONS = frozenset({"status_code", "raise_for_status", "ok"})
_OUTBOUND_VERBS = frozenset({"get", "post", "put", "patch", "delete", "head"})


def _is_outbound_call(node: ast.AST) -> bool:
    """A call through a ``requests``-shaped client, rather than a local method.

    Keyed on the receiver's name containing ``request``, which covers
    ``requests.get`` and this repository's ``http_requests`` alias, and excludes
    the ``cur.get``/``client.get`` shapes that are not HTTP at all.
    """
    if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
        return False
    if node.func.attr not in _OUTBOUND_VERBS:
        return False
    receiver = node.func.value
    name = getattr(receiver, "id", None) or getattr(receiver, "attr", "")
    return "request" in str(name).lower()


def _uninspected_responses(path: Path) -> list[str]:
    """``file:line:function`` for each outbound call whose answer is not read."""
    relative = path.relative_to(REPO_ROOT).as_posix()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    found = []
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        for node in ast.walk(function):
            # The response is never even captured: nothing can read it later.
            if isinstance(node, ast.Expr) and _is_outbound_call(node.value):
                found.append(f"{relative}:{function.name}")
                continue
            if not (isinstance(node, ast.Assign) and _is_outbound_call(node.value)):
                continue
            for target in node.targets:
                if not isinstance(target, ast.Name):
                    continue
                inspected = any(
                    isinstance(child, ast.Attribute)
                    and child.attr in _RESPONSE_INSPECTIONS
                    and isinstance(child.value, ast.Name)
                    and child.value.id == target.id
                    for child in ast.walk(function)
                )
                if not inspected:
                    found.append(f"{relative}:{function.name}")
    return sorted(set(found))


UNINSPECTED_RESPONSE_WAIVERS: tuple[Waiver, ...] = (
    # All five call `dbt_runner` endpoints that no longer exist -- `/dbt/lock`,
    # `/dbt/intents` and `/logs`, deleted by 9f08336 and d88a41e -- so there is
    # no status worth reading and no repair that is not first a decision about
    # whether the admin dbt panel and log viewer survive at all. Stage AA owns
    # that decision, and these leave with whichever answer it takes.
    Waiver("ops/routers/admin.py:_fetch_dbt_context", "G27", 162, date(2026, 9, 8)),
    Waiver("ops/routers/admin.py:view_logs", "G27", 162, date(2026, 9, 8)),
    Waiver("ops/routers/admin.py:dbt_intent_delete", "G27", 162, date(2026, 9, 8)),
)


def test_every_response_we_ask_for_has_its_status_read():
    """Asking and not listening is not asking.

    The waived six are all calls to endpoints that were deleted from
    `dbt_runner` in April and May and never removed from their caller, which is
    the defect this whole clause exists to make loud: they were found by
    accident four and a half months later, and nothing in the suite had an
    opinion about them the entire time.
    """
    found = set()
    for path in production_python_files():
        found |= set(_uninspected_responses(path))

    _assert_exactly(
        found,
        UNINSPECTED_RESPONSE_WAIVERS,
        "These calls went out to another service and the answer was never read.",
    )


# ---------------------------------------------------------------------------
# Rule 11 -- every status code a route can produce is asserted by a test.
# ---------------------------------------------------------------------------
# G28, and it is *Endpoint coverage* strengthened rather than a new rule. That
# section says every route is reached "and the test asserts the status code" --
# singular, and every vacuous 303 test in this repository satisfied it. What was
# missing is a denominator: until Stage Y read the codes out of each handler,
# "every code" could not be said.
#
# **Keyed on what a handler can produce, not on what it declares.** Declared and
# produced are made equal by the rule above, so after the declarations land the
# two readings are the same -- but produced is available now, and a rule keyed
# on declarations would pass vacuously until every route is annotated.
#
# **The route is matched by the tail of its decorator path**, which is what lets
# this run with no prefix reconstruction, no walk of `app.routes` and no
# `operationId` parsing -- each of which has a way of being quietly wrong that
# this does not. `@router.post("/searches/{key}/toggle")` is a suffix of the
# `/admin/searches/x/toggle` a test requests, and the router prefix never has to
# be recovered. Three things make it precise rather than approximate:
#
#   * a decorator path of only template segments is never matched, because
#     `/{snapshot_id}` is a suffix of everything. Measured: without this, 99 of
#     100 requested paths matched more than one handler; with it, 8.
#   * the method must agree, which took 8 to 4.
#   * where two handlers still match, the more literal one wins, as a router
#     resolves -- and if they tie, the request counts for **neither**. An
#     undetermined owner fails rather than being skipped, which is the rule Stage
#     W established and Stage AA restates.
_STATUS_ASSERTION = re.compile(r"status_code\s*(?:==|,)\s*(\d{3})")
_RESPONSE_CLASSES = frozenset({
    "Response", "JSONResponse", "PlainTextResponse", "HTMLResponse",
    "FileResponse", "StreamingResponse", "RedirectResponse", "ORJSONResponse",
})
_REDIRECT_DEFAULT = 307


def _parametrized_ints(function: ast.FunctionDef) -> set[int]:
    """Every integer ``parametrize`` injects into *function*.

    The int twin of :func:`_parametrized_strings`, and it exists because of a
    concrete miss: `tests/ops/routers/test_coordination.py:156` asserts
    ``status_code == status_code`` with the code injected, which a literal scan
    reads as no assertion at all. Counting only literals put that suite's 409s
    and 503s in the uncovered column when they are covered twice over.
    """
    found: set[int] = set()
    for decorator in function.decorator_list:
        if not (
            isinstance(decorator, ast.Call)
            and isinstance(decorator.func, ast.Attribute)
            and decorator.func.attr == "parametrize"
            and len(decorator.args) >= 2
        ):
            continue
        values_node = decorator.args[1]
        if not isinstance(values_node, (ast.List, ast.Tuple)):
            continue
        for row in values_node.elts:
            cells = row.elts if isinstance(row, (ast.Tuple, ast.List)) else [row]
            for cell in cells:
                if isinstance(cell, ast.Constant) and isinstance(cell.value, int) \
                        and 100 <= cell.value <= 599:
                    found.add(cell.value)
    return found


def _status_constant(node: ast.AST, constants: dict[str, int]) -> int | None:
    """A status code written as a literal, a ``status.HTTP_*`` name, or a name."""
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return node.value
    if isinstance(node, ast.Attribute):
        match = re.match(r"HTTP_(\d{3})_", node.attr)
        if match:
            return int(match.group(1))
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    return None


def _codes_at_call_sites(function: ast.AST, constants: dict[str, int]) -> set[int]:
    """Codes named directly in *function*.

    **Keyed on the ``status_code=`` keyword rather than on a list of response
    classes.** The first draft enumerated `Response`, `JSONResponse` and their
    kin and missed every code that reaches a client through
    `templates.TemplateResponse(..., status_code=404)` -- which is how both of
    this repository's admin routers answer -- so `toggle_search` read as
    producing 303 and nothing else. An inventory of class names is escapable by
    using a class the inventory has not heard of, which is the same failure G5
    records for call-site names.
    """
    codes: set[int] = set()
    for node in ast.walk(function):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", getattr(node.func, "attr", None))
        for keyword in node.keywords:
            if keyword.arg == "status_code":
                code = _status_constant(keyword.value, constants)
                if code is not None:
                    codes.add(code)
        if name == "HTTPException" and node.args:
            code = _status_constant(node.args[0], constants)
            if code is not None:
                codes.add(code)
        if name == "RedirectResponse" and not any(
            keyword.arg == "status_code" for keyword in node.keywords
        ):
            codes.add(_REDIRECT_DEFAULT)
    return codes


def _returns_a_bare_value(function: ast.AST) -> bool:
    """Does any ``return`` hand back something FastAPI will serialise itself?

    That is the only way the implicit success code is reachable. A handler whose
    every return is a constructed response -- every admin form post, which
    redirects 303 or renders an error page -- can never answer 200, and adding
    it to the produced set demanded a test for a code the route cannot emit.
    Sixteen findings on the first run, ten of them this.
    """
    for node in ast.walk(function):
        if isinstance(node, ast.Return) and node.value is not None:
            if not isinstance(node.value, ast.Call):
                return True
            name = getattr(node.value.func, "id",
                           getattr(node.value.func, "attr", None))
            if name and not (
                name in _RESPONSE_CLASSES
                or name.endswith("Response")
                or name.endswith("_response")
            ):
                return True
    return False


@lru_cache(maxsize=None)
def _module_functions(relative: str) -> dict[str, ast.AST]:
    """Top-level functions of one module here, by name."""
    path = REPO_ROOT / relative
    if not path.is_file():
        return {}
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return {}
    return {
        node.name: node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _imported_helpers(module: ast.Module, path: Path) -> dict[str, ast.AST]:
    """Functions this module imported from elsewhere in the repository.

    **The trap this closes was latent, not live.** The four response helpers
    that carry every admin route's 404 and 503 are duplicated inside
    `admin.py` and `users.py`, so same-module resolution saw them. Deduplicating
    them into one shared module is the obvious next refactor, and it would have
    made a *new* handler's 503 invisible here -- the declaration would go
    unwritten and nothing would say so. An existing one moving is safe by
    accident, because the rule's other direction fires on the code it can no
    longer see produced; it is new code that would slip.
    """
    here = path.relative_to(REPO_ROOT).parent
    found: dict[str, ast.AST] = {}
    for node in ast.walk(module):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        if node.level:                      # `from .x import y`, `from ..x import y`
            base = here
            for _ in range(node.level - 1):
                base = base.parent
            target = base.joinpath(*node.module.split("."))
        else:                               # `from ops.routers.x import y`
            target = Path(*node.module.split("."))
        for candidate in (f"{target.as_posix()}.py", f"{target.as_posix()}/__init__.py"):
            functions = _module_functions(candidate)
            if not functions:
                continue
            for alias in node.names:
                if alias.name in functions:
                    found[alias.asname or alias.name] = functions[alias.name]
            break
    return found


def _produced_codes(
    function: ast.AST, constants: dict[str, int], module: ast.Module, path: Path
) -> set[int]:
    """Every status code *function* can answer with, helpers included.

    Resolved one level into same-module helpers, because that is where this
    repository puts its error pages: `_not_found_response` and
    `_db_error_response` carry the 404 and the 503 for every admin route that
    has one. One level and no further -- a fixed point over the call graph is
    the dataflow analysis G27 declined to build, and for the same reason.
    """
    helpers = {
        node.name: node for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    helpers.update(_imported_helpers(module, path))
    codes = _codes_at_call_sites(function, constants)
    for node in ast.walk(function):
        if isinstance(node, ast.Call):
            name = getattr(node.func, "id", None)
            if name in helpers and helpers[name] is not function:
                codes |= _codes_at_call_sites(helpers[name], constants)
    return codes


@lru_cache(maxsize=None)
def route_handlers() -> tuple[tuple[str, str, str, str, frozenset, frozenset], ...]:
    """``(service, file, function, decorator path, methods, produced codes)``."""
    found = []
    for service in sorted(service_packages()):
        root = REPO_ROOT / service
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:
                continue
            constants = {
                target.id: node.value.value
                for node in tree.body if isinstance(node, ast.Assign)
                for target in node.targets
                if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, int)
            }
            for function in ast.walk(tree):
                if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for decorator in function.decorator_list:
                    if not (isinstance(decorator, ast.Call)
                            and isinstance(decorator.func, ast.Attribute)):
                        continue
                    verb = decorator.func.attr
                    if verb not in _HTTP_VERBS | {"api_route"}:
                        continue
                    if not (decorator.args and isinstance(decorator.args[0], ast.Constant)):
                        continue
                    methods = {verb.upper()} if verb in _HTTP_VERBS else set()
                    declared_success = 200
                    for keyword in decorator.keywords:
                        if keyword.arg == "methods":
                            if isinstance(keyword.value, ast.List):
                                methods |= {
                                    element.value.upper()
                                    for element in keyword.value.elts
                                    if isinstance(element, ast.Constant)
                                }
                            elif isinstance(keyword.value, ast.Name):
                                # ``_PUBLIC_METHODS`` -- the one such constant here
                                methods |= {"GET", "HEAD"}
                        if keyword.arg == "status_code":
                            resolved = _status_constant(keyword.value, constants)
                            if resolved is not None:
                                declared_success = resolved
                    declared = set()
                    for keyword in decorator.keywords:
                        if keyword.arg == "status_code":
                            resolved = _status_constant(keyword.value, constants)
                            if resolved is not None:
                                declared.add(resolved)
                        if keyword.arg == "responses" and isinstance(
                                keyword.value, ast.Dict):
                            for key in keyword.value.keys:
                                resolved = _status_constant(key, constants)
                                if resolved is not None:
                                    declared.add(resolved)
                    codes = _produced_codes(function, constants, tree, path)
                    if _returns_a_bare_value(function) or not codes:
                        codes |= {declared_success}
                    found.append((
                        service, path.relative_to(REPO_ROOT).as_posix(),
                        function.name, decorator.args[0].value,
                        frozenset(methods), frozenset(codes),
                        frozenset(declared), declared_success,
                    ))
    return tuple(found)


def _literal_segments(path: str) -> list[str]:
    return [s for s in path.strip("/").split("/") if s and not s.startswith("{")]


def _matches_tail(decorator_path: str, requested: str) -> bool:
    decorator = [s for s in decorator_path.strip("/").split("/") if s]
    request = [s for s in requested.strip("/").split("/") if s]
    if not decorator or len(decorator) > len(request):
        return False
    tail = request[len(request) - len(decorator):]
    # A `*` stands for a segment a fixture supplied, so it matches a **template**
    # segment through the first clause and nothing else. Letting it match a
    # literal too was measured at 38 of 89 handlers going ambiguous -- 43% of the
    # surface skipped while the rule reported four gaps and looked healthy.
    return all(a.startswith("{") or a == b for a, b in zip(decorator, tail))


def _resolve_request_path(
    node: ast.AST, constants: dict[str, str], injected: dict[str, set[str]]
) -> set[str]:
    """Like :func:`_resolve_path`, but an unresolvable segment is a wildcard.

    **Deliberately more permissive than the route-coverage rule, and only here.**
    That rule fails closed because "named somewhere in tests/" is the weak
    reading it exists to reject. This one is asking a different question -- which
    handler a request reached -- and a segment built from a fixture is precisely
    a path *parameter*: `api_client.post(f"/admin/searches/{key}/toggle")` with
    `key` from a fixture is a request to `/searches/{search_key}/toggle` and
    nothing else. Failing closed here silently uncounted three Layer 4 tests
    written the same afternoon, which is the false negative that matters most:
    a coverage rule that cannot see a test demands a second one.
    """
    if isinstance(node, ast.JoinedStr):
        combined = {""}
        for part in node.values:
            if isinstance(part, ast.FormattedValue):
                pieces = _resolve_request_path(part.value, constants, injected) or {"*"}
            else:
                pieces = _resolve_request_path(part, constants, injected)
            if not pieces:
                return set()
            combined = {prefix + piece for prefix in combined for piece in pieces}
        return combined
    return _resolve_path(node, constants, injected)


@lru_cache(maxsize=None)
def asserted_status_codes() -> tuple[tuple[str | None, str, str, frozenset], ...]:
    """``(service hint, METHOD, path, codes)`` for every request a test makes."""
    found = []
    for path in sorted((REPO_ROOT / "tests").rglob("test_*.py")):
        hint = next((s for s in service_packages() if s in path.parts), None)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        constants = _module_constants(tree)
        for function in ast.walk(tree):
            if not isinstance(function, ast.FunctionDef):
                continue
            source = ast.get_source_segment(
                path.read_text(encoding="utf-8"), function) or ""
            codes = {int(c) for c in _STATUS_ASSERTION.findall(source)}
            codes |= _parametrized_ints(function)
            if not codes:
                continue
            injected = _parametrized_strings(function)
            for node in ast.walk(function):
                if not (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr in _HTTP_VERBS and node.args):
                    continue
                for requested in _resolve_request_path(
                        node.args[0], constants, injected):
                    if requested.startswith("/"):
                        found.append((hint, node.func.attr.upper(),
                                      requested.split("?")[0], frozenset(codes)))
    return tuple(found)


def _coverage() -> tuple[dict[str, set[int]], set[str]]:
    """Per handler, the codes some test asserts for it -- and the ambiguous ones."""
    covered: dict[str, set[int]] = {}
    ambiguous: set[str] = set()
    handlers = [h for h in route_handlers() if _literal_segments(h[3])]
    for hint, method, requested, codes in asserted_status_codes():
        candidates = [
            h for h in handlers
            if method in h[4]
            and (hint is None or hint == h[0])
            and _matches_tail(h[3], requested)
        ]
        if not candidates:
            continue
        best = max(len(_literal_segments(h[3])) for h in candidates)
        winners = [h for h in candidates if len(_literal_segments(h[3])) == best]
        keys = {f"{h[1]}:{h[2]}" for h in winners}
        if len(keys) > 1:
            ambiguous |= keys
            continue
        covered.setdefault(next(iter(keys)), set()).update(codes)
    return covered, ambiguous


AMBIGUOUS_ROUTE_WAIVERS: tuple[Waiver, ...] = (
    # `ops` serves both `/deploy/start` and `/admin/deploy/start` from two
    # handlers whose decorator strings are **identical** -- one mounted bare, one
    # under `include_router(admin_router, prefix="/admin")` -- so a tail match
    # cannot separate them and neither can be credited. Declared rather than
    # skipped: a handler quietly dropped from a coverage rule is the vacuity this
    # plan is about, and 38 of 89 were being dropped before the wildcard clause
    # was tightened, with the rule reporting four gaps and looking healthy.
    #
    # The admin pair are legacy buttons from before `scripts/redeploy.sh` drove
    # drain, authorization and release through the coordination API, and they
    # never moved with it. Stage AA owns what replaces them, and removing them
    # removes the ambiguity for the two API handlers as well -- one decision
    # drains all four.
    Waiver("ops/routers/admin.py:deploy_start", "G28", 162, date(2026, 9, 8)),
    Waiver("ops/routers/admin.py:deploy_complete", "G28", 162, date(2026, 9, 8)),
    Waiver("ops/routers/deploy.py:start_deploy_intent", "G28", 162, date(2026, 9, 8)),
    Waiver("ops/routers/deploy.py:complete_deployment", "G28", 162, date(2026, 9, 8)),
)

#: Seeded at 11 on 2026-09-08 and drained by Stage Y's fourth step. Every entry
#: is a code the route can answer with and no test asserts for it -- not a code
#: that cannot happen, and not a route nothing reaches. `recap_index`'s 404 is
#: the whole of what `/recaps` does when nothing is published; the snapshot
#: download's four are its entire refusal surface.
UNEXERCISED_CODE_WAIVERS: tuple[Waiver, ...] = (
    Waiver("archiver/app.py:trigger_pack_bronze_html [400]", "G28", 162, date(2026, 9, 8)),
    Waiver("archiver/app.py:trigger_prune_packed_source_html [400]", "G28", 162, date(2026, 9, 8)),
    Waiver("ops/routers/coordination.py:complete_coordination [503]", "G28", 162, date(2026, 9, 8)),
    Waiver(
        "ops/routers/coordination.py:coordination_drain_status [503]",
        "G28", 162, date(2026, 9, 8),
    ),
    Waiver(
        "ops/routers/coordination.py:coordination_release_status [503]",
        "G28", 162, date(2026, 9, 8),
    ),
    Waiver("ops/routers/coordination.py:coordination_status [503]", "G28", 162, date(2026, 9, 8)),
    Waiver(
        "ops/routers/coordination.py:submit_host_evidence [409, 503]",
        "G28", 162, date(2026, 9, 8),
    ),
    Waiver("ops/routers/public.py:recap_index [404]", "G28", 162, date(2026, 9, 8)),
    Waiver(
        "ops/routers/snapshots.py:download_snapshot_archive [400, 401, 403, 503]",
        "G28", 162, date(2026, 9, 8),
    ),
    Waiver("ops/routers/users.py:approve_access_request [503]", "G28", 162, date(2026, 9, 8)),
    Waiver("ops/routers/users.py:deny_access_request [503]", "G28", 162, date(2026, 9, 8)),
)


def test_every_status_code_a_route_can_produce_is_asserted():
    """A code nothing exercises is a claim, not a contract.

    The rule Stage Y's declarations make expressible, and the one that answers
    whether a test could have failed -- not by grading an assertion, but by
    requiring the produced set to be covered. `toggle_search` declaring 303 and
    404 means somebody must write the 404, and the 404 cannot be written against
    a handler that returns 303 either way.
    """
    covered, ambiguous = _coverage()
    _assert_exactly(
        ambiguous, AMBIGUOUS_ROUTE_WAIVERS,
        "These handlers share a decorator path with another, so no request can "
        "be attributed to one of them and neither can be credited.",
    )
    gaps = set()
    for handler in route_handlers():
        _service, file, name, decorator_path, _methods, codes = handler[:6]
        key = f"{file}:{name}"
        if not _literal_segments(decorator_path) or key in ambiguous:
            continue
        missing = sorted(codes - covered.get(key, set()))
        if missing:
            gaps.add(f"{key} {missing}")

    _assert_exactly(
        gaps, UNEXERCISED_CODE_WAIVERS,
        "These routes can answer with a status code no test asserts for them.",
    )


def test_the_route_code_corpus_is_not_empty():
    """The floor under the rule above, and it is not a formality here.

    Every bug this rule had while it was being written moved the same number in
    the same direction -- fewer routes examined, more codes credited, a healthier
    looking result. A decorator path of only templates matched everything and
    took 99 of 100 requests ambiguous; a wildcard that matched a literal took 38
    of 89 **handlers** out of scope while the failure list still read four. None
    of those made a test go red. A rule whose failure mode is silence needs a
    floor that is loud.
    """
    handlers = [h for h in route_handlers() if _literal_segments(h[3])]
    assert len(handlers) >= 80, (
        f"only {len(handlers)} route handlers found with an anchorable path; "
        f"the rule above is measuring almost nothing."
    )
    pairs = sum(len(h[5]) for h in handlers)
    assert pairs >= 150, (
        f"only {pairs} (handler, code) pairs derived. The produced-code reader "
        f"has stopped seeing something -- check _codes_at_call_sites and the "
        f"one-level helper resolution before trusting a green run."
    )
    covered, ambiguous = _coverage()
    assert len(covered) >= 75, (
        f"only {len(covered)} handlers have any code credited to them, out of "
        f"{len(handlers)}. The path matcher has stopped matching."
    )
    assert len(ambiguous) <= 8, (
        f"{len(ambiguous)} handlers cannot be told apart by their decorator "
        f"path, up from the four this rule was written against. Each one is a "
        f"handler no request can be attributed to, so the rule silently stops "
        f"asking about it."
    )
    requests = asserted_status_codes()
    assert len(requests) >= 100, (
        f"only {len(requests)} status assertions resolved to a path; "
        f"_resolve_request_path has stopped resolving."
    )


# ---------------------------------------------------------------------------
# Rule 12 -- a route declares the statuses it can return.
# ---------------------------------------------------------------------------
# G21, and the stage's own name. Every service's OpenAPI schema declares exactly
# `200` and `422` -- FastAPI's defaults -- while the suite asserts eleven
# distinct codes across 137 assertions. Every real code is raised inside a
# handler body and surfaces nowhere a machine can read, so the schema is not a
# weak contract but a false one, and Stage Z's committed artifact would inherit
# the falsehood.
#
# **Both directions.** An undeclared code is the obvious failure. A declared code
# nothing produces is the one that matters over time: without it the
# declarations rot into a second description of whatever the routes used to do,
# which is `ARCHITECTURE.md:179` again in a different file.
#
# **The success code is excluded from the comparison.** A handler returning a
# dict answers 200 by FastAPI's own default, and demanding `responses={200: ...}`
# on all 89 routes would be noise rather than contract. What must be declared is
# every *other* code the body can produce.
#: Seeded at 52 on 2026-09-08 from this rule's own first run, and drained by
#: declaring each route's codes. **The list is the work**: nothing else tracks
#: which of the 89 routes have been done, and an entry that stops describing a
#: mismatch fails until it is deleted, so the ledger cannot lag the repair.
DECLARED_CODE_WAIVERS: tuple[Waiver, ...] = (
)


def test_every_route_declares_the_statuses_it_can_return():
    """The rule this stage is named for.

    It is written before the declarations exist on purpose: its failure list is
    the work, and it shrinks as the work lands. Nothing tracks which of the
    routes are done except this.
    """
    mismatches = set()
    for handler in route_handlers():
        _service, file, name, _path, _methods, produced, declared, success = handler
        undeclared = sorted(produced - declared - {success})
        overdeclared = sorted(declared - produced)
        if undeclared or overdeclared:
            detail = []
            if undeclared:
                detail.append(f"produces undeclared {undeclared}")
            if overdeclared:
                detail.append(f"declares unproduced {overdeclared}")
            mismatches.add(f"{file}:{name} {'; '.join(detail)}")

    _assert_exactly(
        mismatches, DECLARED_CODE_WAIVERS,
        "A route's declared status codes must equal the codes its handler can "
        "produce.",
    )


# ---------------------------------------------------------------------------
# The waiver list itself.
# ---------------------------------------------------------------------------
ALL_WAIVERS = (
    CI_INVOCATION_WAIVERS
    + MOCKER_WAIVERS
    + ROUTE_WAIVERS
    + LAYER_2_WAIVERS
    + DUPLICATE_SQL_WAIVERS
    + SWALLOWED_WRITE_WAIVERS
    + DISCARDED_OUTCOME_WAIVERS
    + UNINSPECTED_RESPONSE_WAIVERS
    + UNEXERCISED_CODE_WAIVERS
    + AMBIGUOUS_ROUTE_WAIVERS
    + DECLARED_CODE_WAIVERS
    + INLINE_SQL_WAIVERS
    + SQL_LITERAL_WAIVERS
    + TEST_SQL_WAIVERS
    + TEST_SQL_TEMPLATE_WAIVERS
    + DBT_CONTRACT_WAIVERS
    + LAYER_NUMBER_WAIVERS
    + ENCODING_WAIVERS
)

_ARCHIVE_ROW = re.compile(r"^\| (\d+)(?:\.\d+)? \| ", re.M)


def test_no_waiver_outlives_the_plan_that_owns_it():
    """An entry whose owner plan has closed is itself a failure.

    This is the hinge the whole waiver mechanism turns on. Without it a waiver
    survives its owner, the repair it was waiting for never happens, and the
    list stops being a queue and becomes a second description of whatever the
    repository already does -- which is ``ARCHITECTURE.md:179`` again, in a
    file whose entire purpose was to prevent that.
    """
    archived = {int(number) for number in _ARCHIVE_ROW.findall(_read(ARCHIVE))}
    assert archived, f"no archive rows parsed out of {ARCHIVE}"
    orphaned = sorted(
        f"{waiver.subject} (waived {waiver.since} against {waiver.gap}, "
        f"owner Plan {waiver.owner})"
        for waiver in ALL_WAIVERS
        if waiver.owner in archived
    )
    assert not orphaned, (
        "these waivers name an owner plan that has been archived, so nothing "
        "is going to fix them:\n  " + "\n  ".join(orphaned)
    )


# The gap list's last column. Restricting the scan to it is what keeps this
# check honest: ``docs/TESTING.md`` cites Plan 84 throughout because Plan 84 is
# where the layer numbering came from, and Plan 84 is archived. A citation is
# not an owner. Only the Owner cell claims someone is going to do something.
_GAP_ROW_OWNER = re.compile(r"^\| (G\d+) \|.*\|([^|]*)\|\s*$", re.M)
_OWNER_PLAN = re.compile(r"Plan (\d+)")


def test_no_gap_entry_outlives_the_plan_that_owns_it():
    """The same hinge as above, for the list the waivers point at.

    This existed as prose in the contract before it existed as a check, and the
    gap it left was found the way these things are always found: Plan 139
    archived on 2026-08-31 with Stage F delivered, and G3 and G10 went on
    naming it as their owner. Nothing failed, because the owner rule had been
    implemented for waivers and not for the entries they cite.

    A gap whose owner has archived is worse than an unowned one. It reads as
    scheduled work, so nobody schedules it.

    **Every plan the Owner cell names is treated as an owner**, which makes the
    cell a place for owners and nothing else. That is a real constraint and it
    caught its author immediately: G10's reassignment was first written as
    "Plan 162 -- Stage D moved there entire when Plan 139 archived", and the
    trailing history read as a second owner. Provenance belongs in the plan
    documents, which is where it now is.
    """
    archived = {int(number) for number in _ARCHIVE_ROW.findall(_read(ARCHIVE))}
    assert archived, f"no archive rows parsed out of {ARCHIVE}"
    gap_list = _read(CONTRACT).split("## The gap list")[1]
    rows = _GAP_ROW_OWNER.findall(gap_list)
    assert rows, f"no gap rows parsed out of {CONTRACT}"
    orphaned = sorted(
        f"{entry} names Plan {number}, archived"
        for entry, owner in rows
        for number in (int(n) for n in _OWNER_PLAN.findall(owner))
        if number in archived
    )
    assert not orphaned, (
        "these gap entries name an owner plan that has been archived, so "
        "nothing is going to fix them:\n  " + "\n  ".join(orphaned) +
        "\n\nEither the gap is closed and the row should go, or it has a new "
        "owner and the row should say so."
    )


def test_every_waiver_names_a_gap_entry_that_exists():
    """A waiver whose gap entry was deleted has lost its reason to exist."""
    gap_list = _read(CONTRACT).split("## The gap list")[1]
    missing = sorted(
        {
            f"{waiver.gap} ({waiver.subject})"
            for waiver in ALL_WAIVERS
            if not re.search(rf"^\| {waiver.gap} \|", gap_list, re.M)
        }
    )
    assert not missing, (
        f"waivers naming a gap entry that is not in {CONTRACT}'s gap list: "
        f"{missing}"
    )


@pytest.mark.parametrize(
    "rule,waivers",
    [
        ("CI invocation", CI_INVOCATION_WAIVERS),
        ("mocker", MOCKER_WAIVERS),
        ("route coverage", ROUTE_WAIVERS),
        ("Layer 2 SQL", LAYER_2_WAIVERS),
        ("duplicate SQL", DUPLICATE_SQL_WAIVERS),
        ("inline SQL", INLINE_SQL_WAIVERS),
        ("SQL literal", SQL_LITERAL_WAIVERS),
        ("test SQL", TEST_SQL_WAIVERS),
        ("test SQL template", TEST_SQL_TEMPLATE_WAIVERS),
        ("dbt contract", DBT_CONTRACT_WAIVERS),
        ("layer numbering", LAYER_NUMBER_WAIVERS),
    ],
)
def test_no_waiver_is_listed_twice(rule, waivers):
    """Duplicates would make the list look longer than the debt it records."""
    subjects = [waiver.subject for waiver in waivers]
    duplicated = sorted({s for s in subjects if subjects.count(s) > 1})
    assert not duplicated, f"{rule} waivers listed more than once: {duplicated}"


# ---------------------------------------------------------------------------
# Plan 162 Stage W -- a checker may not retype a vocabulary the database owns.
#
# `check_snapshot_result` accepted only {"created"} while the exporter returns
# "exported", and the test that should have caught it seeded the status itself.
# Stage P repaired that instance. The class is wider and the census is in
# docs/evidence/plan_162_stage_W_evidence.md: 262 literal comparisons across
# production Python, of which the ones that can drift are the ones keyed on a
# vocabulary some *other* artifact owns.
#
# `db/migrations/` is the largest such owner and the only one that is closed:
# `CHECK (<column> IN (...))` is enforced by Postgres, so it is not one opinion
# about the vocabulary, it is the vocabulary. That makes both sides of these
# two rules derivable, which is the whole reason they are rules and not a
# registry of contracts somebody remembered to add.
# ---------------------------------------------------------------------------
_SQL_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_SQL_LINE_COMMENT = re.compile(r"--[^\n]*")
_CREATE_TABLE_BODY = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)(.*?);",
    re.IGNORECASE | re.DOTALL,
)
_CHECK_IN = re.compile(
    r"CHECK\s*\(\s*(\w+)\s+IN\s*\(([^)]*)\)", re.IGNORECASE | re.DOTALL
)
_SQL_STRING = re.compile(r"'([^']*)'")

#: A floor under the derived owner corpus, far below the 18 it finds. A rule
#: whose population comes from a regex fails open when the regex stops
#: matching -- a set difference over an empty corpus is empty -- and this is
#: the same guard `test_the_production_sql_corpus_is_not_empty` puts under the
#: SQL rules for the same reason.
_DB_VOCABULARY_FLOOR = 10


def _without_sql_comments(text: str) -> str:
    """Comment prose contains `FROM the` and `CHECK (` often enough to matter."""
    return _SQL_LINE_COMMENT.sub(" ", _SQL_BLOCK_COMMENT.sub(" ", text))


@lru_cache(maxsize=None)
def check_constrained_columns() -> dict[tuple[str, str], frozenset[str]]:
    """Every ``CHECK (<column> IN (...))`` in ``db/migrations/``, by table.

    Read from the migrations rather than from a live database on purpose: this
    is a Layer 0 rule and needs no engine, and the migrations are what a fresh
    deployment gets. A constraint added by anything other than a migration is
    invisible here, which is correct -- there is no such path.
    """
    found: dict[tuple[str, str], set[str]] = {}
    for path in sorted((REPO_ROOT / "db" / "migrations").glob("*.sql")):
        body = _without_sql_comments(path.read_text(encoding="utf-8"))
        for table, definition in _CREATE_TABLE_BODY.findall(body):
            for column, values in _CHECK_IN.findall(definition):
                members = set(_SQL_STRING.findall(values))
                if members:
                    found.setdefault((table.split(".")[-1], column), set()).update(
                        members
                    )
    return {key: frozenset(values) for key, values in found.items()}


def test_the_check_constraint_corpus_is_not_empty():
    """The floor under the rule below, and the reason it is not decoration.

    Both rules that follow are set comparisons against this corpus. An empty
    corpus makes both of them pass while checking nothing, so a regex that
    stops matching -- a migration written with a different `CHECK` spelling,
    a directory that moved -- has to fail here rather than quietly disarm the
    two rules downstream.
    """
    owners = check_constrained_columns()
    assert len(owners) >= _DB_VOCABULARY_FLOOR, (
        f"only {len(owners)} CHECK-constrained column(s) found under "
        f"db/migrations/, against a floor of {_DB_VOCABULARY_FLOOR}. Either the "
        f"reader stopped matching or the constraints were dropped; both are "
        f"failures, and the second is a much larger one."
    )


def test_every_check_constrained_column_has_one_declared_vocabulary():
    """``shared/db_vocabularies.py`` is a copy, and this is what makes it safe.

    Both directions, because each catches a different way the copy rots. A
    ``CHECK`` with no vocabulary is a value set production types by hand at
    every call site -- the state this stage found. A vocabulary naming a column
    that no longer carries a constraint is a declaration nobody is reading.

    And then equality, which is the direction that actually bites: a migration
    that renames a value fails here until this module moves with it, and the
    rule below is what makes every call site move too.
    """
    owners = check_constrained_columns()
    declared = set(DB_VOCABULARIES)

    undeclared = sorted(set(owners) - declared)
    assert not undeclared, (
        f"{len(undeclared)} CHECK-constrained column(s) with no vocabulary in "
        f"shared/db_vocabularies.py: {undeclared}. Postgres already enforces "
        f"the values; declaring them is what stops production retyping them "
        f"one call site at a time."
    )
    orphaned = sorted(declared - set(owners))
    assert not orphaned, (
        f"{len(orphaned)} vocabulary/vocabularies in shared/db_vocabularies.py "
        f"naming a column with no CHECK constraint: {orphaned}. Either the "
        f"migration dropped it -- in which case nothing enforces these values "
        f"any more and that is the finding -- or the name is wrong."
    )
    for key in sorted(owners):
        table, column = key
        assert frozenset(DB_VOCABULARIES[key]) == owners[key], (
            f"{table}.{column}: the migration permits {sorted(owners[key])} and "
            f"{DB_VOCABULARIES[key].__name__} declares "
            f"{sorted(str(member) for member in DB_VOCABULARIES[key])}. The "
            f"migration is the owner -- Postgres rejects a write outside it -- "
            f"so this module is what moves."
        )


_COMPARISONS = (ast.In, ast.NotIn, ast.Eq, ast.NotEq)
_IDENTIFIER = re.compile(r"[A-Za-z_]\w*")
_QUOTED_KEY = re.compile(r"""['"]([^'"]*)['"]""")


def _literal_strings(node: ast.AST) -> list[str] | None:
    """The string literals in *node*, or ``None`` if it is not all literals."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        found = []
        for element in node.elts:
            if isinstance(element, ast.Constant) and isinstance(element.value, str):
                found.append(element.value)
            else:
                return None
        return found or None
    return None


def _subject_names(node: ast.AST) -> set[str]:
    """Every identifier and quoted key in the expression being compared.

    ``state["phase"]`` names ``phase`` as a subscript, ``current.get("kind")``
    names ``kind`` as an argument, and a bare ``role`` names it directly. All
    three are the same question -- which column is this value -- so the
    expression is unparsed and read for both identifiers and quoted strings
    rather than matched shape by shape.
    """
    text = ast.unparse(node)
    return set(_IDENTIFIER.findall(text)) | set(_QUOTED_KEY.findall(text))


def _database_vocabulary_retypings() -> list[str]:
    """Every literal comparison that restates a value the database owns.

    **Two conditions, and the second is what makes the rule usable.** The
    expression must name a CHECK-constrained column, *and* the literal must be
    a member of that column's vocabulary. Naming alone is far too loose:
    ``status`` and ``kind`` are the most reused words in the repository, and
    scoping by name only, ``result.status == "ok"`` in ``ops/routers/deploy.py``
    reads as a claim about ``artifacts_queue.status``. Measured over the whole
    corpus that is 83 comparisons naming a constrained column and **33** whose
    literal is actually a member -- the other 50 are `ok`, `success`,
    `unknown`, `locked`, vocabularies that belong to something else and that
    this rule must not touch.

    A membership test is what separates them, and it costs nothing in strength:
    a literal that is not in the vocabulary cannot be a copy of it.
    """
    owners: dict[str, set[str]] = {}
    for (_table, column), values in check_constrained_columns().items():
        owners.setdefault(column, set()).update(values)

    found = []
    for path in production_python_files():
        relative = _relative(path)
        if relative == "shared/db_vocabularies.py":
            continue
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        # A module-level or local name bound to a collection of literals is the
        # same retyping one step removed -- `REQUESTABLE_ROLES = ("observer",
        # ...)` then `role not in REQUESTABLE_ROLES`. Reported at the
        # comparison, because that is where the column name appears.
        literal_collections: dict[str, list[str]] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                members = _literal_strings(node.value)
                if members:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            literal_collections[target.id] = members
        for node in ast.walk(tree):
            if not isinstance(node, ast.Compare) or len(node.ops) != 1:
                continue
            if not isinstance(node.ops[0], _COMPARISONS):
                continue
            right = node.comparators[0]
            members = _literal_strings(right)
            if members is None and isinstance(right, ast.Name):
                members = literal_collections.get(right.id)
            if members is None:
                continue
            names = _subject_names(node.left)
            for column in sorted(owners):
                if column not in names:
                    continue
                retyped = sorted(set(members) & owners[column])
                if retyped:
                    found.append(
                        f"{relative}:{node.lineno} compares {column} against "
                        f"{retyped}, which db/migrations/ owns"
                    )
                break
    return found


def test_no_module_retypes_a_database_vocabulary_it_could_import():
    """The half that makes the declared vocabulary reach the call sites.

    Without it, ``shared/db_vocabularies.py`` is a fifteenth copy: the rule
    above would hold it equal to the constraint while every ``==
    "draining"`` in the repository went on being its own copy, and a rename
    would move the migration and the module together and leave the call sites
    behind. **That is the failure mode this stage exists to close**, one level
    down from where Stage P found it.

    It is also what makes the pair non-vacuous. On its own this rule passes the
    moment a migration renames a value -- the old literal stops being a member,
    so the comparison stops being in scope and nothing fails. The rule above is
    what fails in that instant, and this one is what makes the repair reach
    further than one file.
    """
    retypings = _database_vocabulary_retypings()
    assert not retypings, (
        f"{len(retypings)} comparison(s) restate a value db/migrations/ owns "
        f"instead of importing it from shared/db_vocabularies.py:\n  "
        + "\n  ".join(retypings)
    )


# ---------------------------------------------------------------------------
# Plan 162 Stage W -- the other owner, and the instance the stage came from.
#
# `db/migrations/` owns the vocabularies above and a service's HTTP response
# owns this one. The export DAG's `check_snapshot_result` accepted only
# {"created"} while `archiver/processors/export_ci_lake_snapshot.py` returns
# "exported": a DAG-triggered export would have published its archive and both
# pointers and then failed the task.
#
# **`airflow/dags/` is the one place in this repository where the import that
# would remove the copy is impossible**, and that is a fact about the deploy
# rather than a preference: docker-compose.yml mounts `airflow/dags`,
# `airflow/sql` and `airflow/plugins` into the Airflow containers and nothing
# else, and `airflow/dags/dag_queries.py` records at length why mounting
# `shared/` would put unimportable modules on the DAG tree's path. So the DAG
# must restate the vocabulary, and what it restates has to be checked.
#
# Both halves are derived. The service comes from the module's own
# `<NAME>_URL = "http://<host>:<port>"` constant, and the module that answers
# for it is the one with the same basename -- the convention the tree already
# follows for seven of its DAGs. A DAG that checks a status and resolves to no
# counterpart fails here rather than being skipped, because "we could not tell
# who owns this" is the state the defect lived in.
# ---------------------------------------------------------------------------
_SERVICE_URL = re.compile(
    r"^([A-Z][A-Z0-9_]*)_URL\s*=\s*[\"']http://([a-z0-9_-]+):", re.MULTILINE
)
_DAG_SUPPORT_MODULES = frozenset(
    {"coordination_contract", "dag_queries", "notifications", "pools", "sensors"}
)


def _emitted_statuses(path: Path) -> frozenset[str]:
    """Every string *path* can put in a ``status=`` field.

    Two shapes reach it and both have to be read. Most are a literal keyword;
    the rest are returned by a helper and arrive through a local, as
    ``status=failure_status``. A reader that saw only the first returns a set
    short of ``coverage_failed`` -- which is what the single-instance repair
    did, silently, for four days.

    So an expression this cannot follow **fails** rather than being dropped. An
    incomplete emitted set only makes the subset check below stricter, never
    weaker, so this guard is not load-bearing for correctness -- it is here
    because a reader that quietly narrows is the defect this stage is about,
    wearing the instrument's uniform.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    produced_by: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            called = getattr(node.value.func, "id", None) or getattr(
                node.value.func, "attr", ""
            )
            for target in node.targets:
                if isinstance(target, ast.Name) and called in functions:
                    produced_by[target.id] = called

    found: set[str] = set()
    unresolved: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "status":
                continue
            value = keyword.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                found.add(value.value)
            elif isinstance(value, ast.Name) and value.id in produced_by:
                found |= {
                    inner.value.value
                    for inner in ast.walk(functions[produced_by[value.id]])
                    if isinstance(inner, ast.Return)
                    and isinstance(inner.value, ast.Constant)
                    and isinstance(inner.value.value, str)
                }
            else:
                unresolved.append(f"line {value.lineno}: status={ast.unparse(value)}")
    assert not unresolved, (
        f"{_relative(path)} passes a status this reader cannot follow to a "
        f"literal: {unresolved}. The emitted set it returns is short, and a "
        f"short set makes the check below fail on a status the service really "
        f"does return."
    )
    return frozenset(found)


def _dag_status_checks() -> dict[str, frozenset[str]]:
    """Per DAG module, the statuses its checker will let through."""
    accepted: dict[str, set[str]] = {}
    for path in sorted((REPO_ROOT / "airflow" / "dags").glob("*.py")):
        if path.stem in _DAG_SUPPORT_MODULES:
            continue
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        literal_collections: dict[str, list[str]] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                members = _literal_strings(node.value)
                if members:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            literal_collections.setdefault(target.id, []).extend(members)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Compare) or len(node.ops) != 1:
                continue
            if not isinstance(node.ops[0], _COMPARISONS):
                continue
            if "status" not in _subject_names(node.left):
                continue
            right = node.comparators[0]
            members = _literal_strings(right)
            if members is None and isinstance(right, ast.Name):
                members = literal_collections.get(right.id)
            if members:
                accepted.setdefault(path.stem, set()).update(members)
    return {stem: frozenset(values) for stem, values in accepted.items()}


def _dag_counterpart(stem: str, source: str) -> list[Path]:
    """The production module that answers for *stem*, via its own service URL."""
    packages = {
        host.replace("-", "_") for _name, host in _SERVICE_URL.findall(source)
    }
    packages = {
        package for package in packages if (REPO_ROOT / package / "__init__.py").is_file()
    }
    return sorted(
        path
        for package in packages
        for path in (REPO_ROOT / package).rglob(f"{stem}.py")
        if "__pycache__" not in path.parts
    )


def test_every_dag_status_check_accepts_only_statuses_its_service_emits():
    """The assertion that would have caught it, with neither half written here.

    A status the DAG accepts and the service never returns is a branch that can
    only ever fail, which is exactly what ``created`` was -- and the test that
    should have caught it seeded ``{"status": "created"}`` itself, so it passed
    for any string its author picked.
    """
    for stem, accepted in sorted(_dag_status_checks().items()):
        source = (REPO_ROOT / "airflow" / "dags" / f"{stem}.py").read_text(
            encoding="utf-8"
        )
        counterparts = _dag_counterpart(stem, source)
        assert len(counterparts) == 1, (
            f"airflow/dags/{stem}.py checks a status against {sorted(accepted)} "
            f"and this rule cannot tell which module produces it: it resolved "
            f"{[_relative(path) for path in counterparts]}. A DAG that checks a "
            f"status names one service in a `<NAME>_URL` constant and shares a "
            f"basename with the module behind it; an undetermined owner is the "
            f"state the defect this rule exists for lived in, so it fails here."
        )
        emitted = _emitted_statuses(counterparts[0])
        unreachable = sorted(accepted - emitted)
        assert not unreachable, (
            f"airflow/dags/{stem}.py accepts {unreachable}, which "
            f"{_relative(counterparts[0])} never returns. It returns "
            f"{sorted(emitted)}."
        )


def test_the_dag_status_rule_has_something_to_check():
    """A floor, for the same reason every other derived rule here has one.

    This rule reads DAG modules for a shape. A rename in the DAG tree, or a
    checker rewritten to compare something other than ``status``, empties the
    population -- and a loop over nothing passes. The number is 1 because that
    is what the repository holds, and it is the DAG this stage came from.
    """
    checks = _dag_status_checks()
    assert checks, (
        "no DAG module checks a status against literals any more. Either the "
        "checker moved and this rule is reading the wrong shape, or the last "
        "one was deleted; the first is a broken instrument and the second is a "
        "change worth noticing."
    )
