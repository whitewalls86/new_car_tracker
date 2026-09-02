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

Eight rules are mechanical and are asserted here, matching the table in
`docs/TESTING.md` under *What CI asserts*. Four more are judgement -- whether
the thing under test is the thing being mocked, whether a failure branch
matters to another service, whether an assertion is meaningful, and whether a
``SELECT`` in a test file paraphrases production or seeds a fixture. Those
belong to ``.claude/skills/testing-contract/``, which flags them and refuses to
certify them. **Nothing in this file should grow to imply it checks them.**

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

    Plan 162 Stage 1 ran them. 66 of the 73 passed; the 7 that did not were two
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
# Empty since Plan 162 Stage 5 (CAR-49) converted all 34 on 2026-09-01 -- the
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
# Empty since Plan 162 Stage 7 (CAR-51). Every production .sql file is
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

    That is not hypothetical. Until Plan 162 Stage 8,
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
        "scripts/audit_adaptive_refresh_features.py:147",
        "scripts/audit_adaptive_refresh_features.py:155",
        "scripts/audit_adaptive_refresh_features.py:163",
        "scripts/audit_adaptive_refresh_features.py:173",
        "scripts/compare_gate_a_parity.py:223",
        "scripts/compare_gate_b_parity.py:595",
        "scripts/estimate_dictionary_savings.py:164",
        "scripts/export_volatility_features_to_iceberg.py:123",
        "scripts/export_volatility_features_to_iceberg.py:134",
        "scripts/export_volatility_features_to_iceberg.py:155",
        "scripts/preflight_local_lakehouse_snapshot.py:301",
        "scripts/run_dbt_spark.py:158",
        "scripts/spike_iceberg_lakehouse.py:133",
        "scripts/verify_dialect_datediff.py:128",
    )
)

# Every name in this stack that takes a SQL string, whether or not it is used
# here today. Scoping the set to what the repository currently calls is the
# mistake this plan keeps finding in its own instruments: the census undercounted
# G14 and it undercounted G5, both because the check was fitted to the code in
# front of it. ``executemany`` matches nothing on 2026-09-01 and is here anyway,
# because the cost of a name that never fires is zero and the cost of a missing
# one is a gap nothing reports. ``sql`` covers ``spark.sql(...)``, which is not
# called yet either -- see the docstring below on what does and does not survive
# PySpark.
_SQL_CALL_NAMES = frozenset({
    # DB-API and psycopg2
    "execute", "executemany", "executescript", "execute_batch", "execute_values",
    "mogrify", "copy_expert",
    # DuckDB and Spark
    "sql", "query", "from_query",
    # pandas and SQLAlchemy
    "read_sql", "read_sql_query", "read_sql_table", "text",
})

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


def _leading_sql_literal(node: ast.AST) -> str | None:
    """The literal text at the head of *node*, through the shapes that hide one.

    A rule that only reads a bare ``ast.Constant`` makes concatenation the
    escape hatch: ``"SELECT ..." + where`` and ``f"SELECT ... {col}"`` are the
    two ways inline SQL is actually written once it needs a variable, and they
    are the ones worth catching most.
    """
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _leading_sql_literal(node.left)
    if isinstance(node, ast.JoinedStr) and node.values:
        return _leading_sql_literal(node.values[0])
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr in {"format", "dedent", "strip", "lstrip", "join"}:
            return _leading_sql_literal(node.func.value)
    return None


def _inline_sql_sites(source: str, filename: str = "<canary>") -> set[int]:
    """Line numbers where a SQL-taking call is handed a literal statement.

    **Every argument is read, not the first.** ``execute_values(cur, sql, rows)``
    puts its statement second, and a first-argument rule is blind to it by
    construction -- which is not hypothetical: it is
    ``ops/routers/maintenance.py:152``, a literal INSERT into
    ``staging.blocked_cooldown_events`` that the census never named because the
    gap list describes the shape as "``.execute(`` with a literal first
    argument".
    """
    tree = ast.parse(source, filename=filename)
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            name = node.func.attr
        elif isinstance(node.func, ast.Name):
            name = node.func.id
        else:
            continue
        if name not in _SQL_CALL_NAMES:
            continue
        arguments = list(node.args) + [keyword.value for keyword in node.keywords]
        for argument in arguments:
            text = _leading_sql_literal(argument)
            if text is None:
                continue
            match = _SQL_VERB.match(text)
            if match and match.group(1).upper() not in _EXEMPT_VERBS:
                found.add(node.lineno)
                break
    return found


def test_the_inline_sql_rule_sees_the_shapes_that_hide_a_statement():
    """The rule below, tested on the shapes it exists to catch.

    A structural check nothing exercises reports a clean repository whether or
    not it still matches anything, which is the failure this whole file exists
    to prevent -- so the detector is separated from the sweep and canaried here.
    """
    caught = _inline_sql_sites(
        'cur.execute("SELECT 1")\n'                              # 1  bare literal
        'cur.execute("SELECT * FROM t WHERE a = " + a)\n'        # 2  concatenated
        'cur.execute(f"SELECT {col} FROM t")\n'                  # 3  f-string
        'execute_values(cur, "INSERT INTO t VALUES %s", rows)\n'  # 4  second argument
        'pd.read_sql(sql="SELECT 1", con=c)\n'                   # 5  keyword argument
        'spark.sql("MERGE INTO t USING s ON t.id = s.id")\n'     # 6  Spark, no caller yet
    )
    assert caught == {1, 2, 3, 4, 5, 6}, (
        f"the inline-SQL rule no longer sees every shape: caught {sorted(caught)}"
    )

    clean = _inline_sql_sites(
        'cur.execute(CLAIM_ARTIFACTS, (limit,))\n'      # a loaded constant is the fix
        'df.query("price > 100")\n'                     # not SQL: no leading verb
        'con.execute("INSTALL httpfs")\n'               # session setup, exempt
        'con.execute("SET s3_url_style=?", ["path"])\n'  # session setup, exempt
    )
    assert not clean, (
        f"the inline-SQL rule fires on calls that are already correct: {sorted(clean)}"
    )


def test_no_production_module_holds_sql_at_its_execute_call_site():
    """G5, which the census recorded in prose and nothing has ever checked.

    Inline SQL is not merely untidy. It is what *manufactures* the paraphrase
    the contract calls worse than no test: a statement written at its call site
    cannot be imported, so the only way to give it a test is to retype it, and
    a retyped statement passes forever while the original rots. Moving SQL into
    a ``.sql`` file is not the goal -- it is what makes the retyping
    unnecessary, and it is why this rule and
    :func:`test_every_production_sql_file_is_touched_by_a_layer_2_test` are one
    stage rather than two.

    **What this does not survive is PySpark, and the residue is three named
    things rather than an open question.** ``spark.sql("SELECT ...")`` is caught
    already -- ``sql`` is in the name set and the verb guard does not care about
    dialect. What is not caught: SQL *fragments* (``df.selectExpr("price >
    msrp")``, ``F.expr(...)``, ``df.filter("year > 2020")``) start with no verb,
    and the guard that makes a generous name set safe is exactly what makes it
    blind to them; the DataFrame API is not text at all, so it can drift from a
    schema with nothing textual to see; and a ``.sql`` file only earns its
    keep if some engine executes it, which for Spark means the Lakekeeper and
    PySpark services ``tests/integration/lakehouse`` is
    :data:`DORMANT_SUITES`-declared against until Plan 125 Gate C returns them.
    Static reading stops at the first of those three. The other two are caught
    by executing them in CI or not at all.
    """
    found = {
        f"{_relative(path)}:{line}"
        for path in production_python_files()
        for line in _inline_sql_sites(path.read_text(encoding="utf-8"), str(path))
    }
    _assert_exactly(
        found,
        INLINE_SQL_WAIVERS,
        "these production modules hold SQL at the call site, where no test can "
        "import it and only a paraphrase can cover it:",
    )


# ---------------------------------------------------------------------------
# Rule 5c -- no production module keeps a SQL statement in a Python literal.
# ---------------------------------------------------------------------------
SQL_LITERAL_WAIVERS: tuple[Waiver, ...] = tuple(
    Waiver(subject, gap="G15", owner=162)
    for subject in (
        "archiver/processors/delete_packed_source_html.py:304",
        "archiver/processors/lake_snapshot_cohort.py:109",
        "archiver/processors/lake_snapshot_cohort.py:156",
        "archiver/processors/lake_snapshot_cohort.py:354",
        "archiver/processors/lake_snapshot_cohort.py:502",
        "archiver/processors/lake_snapshot_cohort.py:535",
        "archiver/processors/lake_snapshot_cohort.py:560",
        "archiver/processors/lake_snapshot_cohort.py:594",
        "archiver/processors/lake_snapshot_cohort.py:627",
        "archiver/processors/lake_snapshot_export.py:130",
        "archiver/processors/lake_snapshot_selectors.py:129",
        "archiver/processors/lake_source_audit.py:113",
        "archiver/processors/pack_bronze_html.py:440",
        "ops/routers/maintenance.py:36",
        "processing/writers/silver_writer.py:38",
        "scripts/audit_adaptive_refresh_features.py:128",
        "scripts/audit_adaptive_refresh_features.py:135",
        "scripts/estimate_dictionary_savings.py:206",
        "shared/deploy_intent.py:25",
    )
)


def _sql_literal_bindings(source: str, filename: str = "<canary>") -> set[int]:
    """Line numbers where a SQL statement is bound to a Python name.

    Rule 5b catches SQL *at* an ``.execute()`` call site, where it cannot be
    imported at all. This catches the shape one step back: assigned to a name
    first, then executed. That statement **is** importable, so the paraphrase
    hazard is gone -- which is exactly why it is easy to miss, and why it
    needs its own rule rather than a wider version of 5b's.

    A ``return`` is included because a function that hands back a statement is
    the same binding with a different keyword, and scoping this to the
    assignments that happen to exist today is the mistake this plan has now
    made twice in its own instruments.
    """
    tree = ast.parse(source, filename=filename)
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            candidates = [node.value]
        elif isinstance(node, ast.Return):
            candidates = [node.value]
        else:
            continue
        for candidate in candidates:
            if candidate is None:
                continue
            text = _leading_sql_literal(candidate)
            if text is None:
                continue
            match = _SQL_VERB.match(text)
            if match and match.group(1).upper() not in _EXEMPT_VERBS:
                found.add(node.lineno)
    return found


def test_the_sql_literal_rule_sees_a_statement_bound_to_a_name():
    """The rule below, canaried on the shapes it exists to catch."""
    caught = _sql_literal_bindings(
        'SQL = "SELECT 1"\n'                                  # 1  module constant
        'def f():\n'
        '    sql = "SELECT * FROM t WHERE a = %s"\n'          # 3  function local
        '    return sql\n'
        'def g(where):\n'
        '    return "SELECT * FROM t WHERE " + where\n'       # 6  returned, concatenated
        'TEMPLATE: str = f"SELECT {cols} FROM t"\n'           # 7  annotated f-string
    )
    assert caught == {1, 3, 6, 7}, (
        f"the SQL-literal rule no longer sees every shape: caught {sorted(caught)}"
    )

    clean = _sql_literal_bindings(
        'CLAIM_ARTIFACTS = _q("claim_artifacts")\n'   # loaded from a .sql file: the fix
        'name = "select_user_role"\n'                 # a filename, not a statement
        'mode = "SETTINGS"\n'                         # not a SQL verb, despite the prefix
        'PRAGMA_SQL = "PRAGMA threads=4"\n'           # session setup, exempt
    )
    assert not clean, (
        f"the SQL-literal rule fires on bindings that are already correct: "
        f"{sorted(clean)}"
    )


def test_no_production_module_keeps_a_sql_statement_in_a_python_literal():
    """G15, the gap that closing G5 revealed.

    Stage 7 extracted six of these by hand -- three module-level constants in
    ``ops/routers/coordination.py``, three function-local ``sql = \"\"\"...\"\"\"``
    variables in ``ops/routers/deploy.py`` -- and only because a human happened
    to read the files while doing something else. **Neither instrument could
    see them.** Rule 5b does not fire, because the literal is not at the call
    site. Rule 5 does not fire, because there is no ``.sql`` file to be
    uncovered. They satisfied the letter of both while sitting outside both,
    and the sweep that found them was not repeatable.

    The measured cost of that blind spot was 21 more, in modules nobody had
    looked at: six in ``ops/routers/admin.py`` alone, a router Stage 7 never
    touched because every one of its statements is assigned before it is
    executed.

    **What this rule is not.** It is not a claim that a statement in a Python
    string is untestable -- it is importable, so a test can execute the real
    text, which is the whole point of the other two rules. It is a claim that
    this repository decided its SQL lives in ``.sql`` files, and a statement
    that does not is invisible to the census that counts them. Rule 5's
    denominator is ``production_sql_files()``; anything held in Python is
    outside it and can never be reported as uncovered.
    """
    found = {
        f"{_relative(path)}:{line}"
        for path in production_python_files()
        for line in _sql_literal_bindings(path.read_text(encoding="utf-8"), str(path))
    }
    _assert_exactly(
        found,
        SQL_LITERAL_WAIVERS,
        "these production modules keep a SQL statement in a Python literal, so "
        "it is in no .sql file and the Layer 2 census cannot count it:",
    )


# ---------------------------------------------------------------------------
# Rule 6 -- the layer numbers in the code are this document's.
# ---------------------------------------------------------------------------
# Empty since Plan 162 Stage 5 (CAR-49) swept all 16 on 2026-09-01. The rule
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
    """The ``env:`` keys visible to one step -- its own, and its job's."""
    document = yaml.safe_load(_read(WORKFLOW))
    for job in document["jobs"].values():
        if job.get("name") != job_name:
            continue
        keys = list(job.get("env", {}))
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
    -- and nothing makes them agree. Plan 162 Stage 5b split the tree; this is
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

    Until Plan 162 Stage 2 this list named six packages and omitted
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

    defined = {
        node.name
        for node in ast.parse(Path(__file__).read_text(encoding="utf-8")).body
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
# The waiver list itself.
# ---------------------------------------------------------------------------
ALL_WAIVERS = (
    CI_INVOCATION_WAIVERS
    + MOCKER_WAIVERS
    + ROUTE_WAIVERS
    + LAYER_2_WAIVERS
    + DUPLICATE_SQL_WAIVERS
    + INLINE_SQL_WAIVERS
    + SQL_LITERAL_WAIVERS
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
        ("layer numbering", LAYER_NUMBER_WAIVERS),
    ],
)
def test_no_waiver_is_listed_twice(rule, waivers):
    """Duplicates would make the list look longer than the debt it records."""
    subjects = [waiver.subject for waiver in waivers]
    duplicated = sorted({s for s in subjects if subjects.count(s) > 1})
    assert not duplicated, f"{rule} waivers listed more than once: {duplicated}"
