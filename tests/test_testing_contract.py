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

Seven rules are mechanical and are asserted here, matching the table in
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

Three of the seven checks report more violations than the contract's gap list
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
CI_INVOCATION_WAIVERS = (
    Waiver("tests/integration/lakehouse", gap="G2", owner=162),
    Waiver("tests/integration/processing", gap="G1", owner=162),
    Waiver("tests/integration/scraper", gap="G1", owner=162),
    Waiver("tests/integration/shared", gap="G1", owner=162),
)


def test_every_integration_suite_is_invoked_by_a_ci_step():
    """G1 is the reason this file exists at all.

    73 integration-marked tests in 11 files were written, reviewed, merged and
    maintained while no CI step ran them, and ``tests/integration/processing/``
    -- 58 of them -- has never appeared in ``ci.yml`` in its history. Nothing
    failed. No mechanism existed that could notice.

    Dormancy is declared here rather than through a second mechanism. A waiver
    already carries a reason, an owner and a date, and it already only shrinks;
    inventing a marker file beside it would put the same fact in two places.
    ``tests/integration/lakehouse/`` is dormant by decision (Plan 125 pulled the
    job in ``863a2f2``) and its waiver says so by naming G2 rather than G1 --
    which is the whole of what G2 asked for: dormant and orphaned, told apart.
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
    _assert_exactly(
        suites - invoked,
        CI_INVOCATION_WAIVERS,
        "Every tests/integration/<dir> is invoked by a named CI step, or is "
        "waived with the gap entry that says why (docs/TESTING.md, 'What CI "
        "asserts').",
    )


# ---------------------------------------------------------------------------
# Rule 2 -- patching is ``mocker``.
# ---------------------------------------------------------------------------
MOCKER_WAIVERS = tuple(
    Waiver(subject, gap="G4", owner=162)
    for subject in (
        # unittest.mock.patch -- the 20 files G4 measured.
        "tests/airflow/test_notifications.py",
        "tests/airflow/test_pack_bronze_html_dag.py",
        "tests/archiver/test_compact_silver.py",
        "tests/integration/airflow/test_hourly_analytics_refresh.py",
        "tests/integration/airflow/test_scrape_listings.py",
        "tests/lakehouse/test_register_warehouse.py",
        "tests/processing/test_detail_writer.py",
        "tests/processing/test_srp_writer.py",
        "tests/scraper/processors/test_scrape_detail.py",
        "tests/scripts/test_audit_parquet_layout.py",
        "tests/scripts/test_audit_sectioned_html_storage.py",
        "tests/scripts/test_estimate_dictionary_savings.py",
        "tests/scripts/test_estimate_recompression_savings.py",
        "tests/scripts/test_recompress_bronze_html.py",
        "tests/scripts/test_rewrite_parquet_layout.py",
        "tests/scripts/test_train_html_dictionary.py",
        "tests/shared/test_minio_dictionary.py",
        # monkeypatch.setattr -- the 16 G4 did not count. Every one targets a
        # module object (a repo module, or httpx), never process state, so the
        # contract's narrower reading and this check's mechanical one pick out
        # the same files today.
        "tests/airflow/test_coordination_admission.py",
        "tests/archiver/test_delete_packed_source_html.py",
        "tests/archiver/test_disk_usage.py",
        "tests/archiver/test_pack_bronze_html.py",
        "tests/integration/archiver/test_compact_silver_integration.py",
        "tests/integration/archiver/test_pack_bronze_html_integration.py",
        "tests/integration/ops/test_coordination_release.py",
        "tests/integration/scripts/test_plan145_canary_commit.py",
        "tests/integration/shared/test_read_html_pack_fallback.py",
        "tests/lakehouse/test_export_volatility_features_metadata.py",
        "tests/lakehouse/test_iceberg_spike_metadata.py",
        "tests/lakehouse/test_preflight_local_lakehouse.py",
        "tests/ops/routers/test_auth.py",
        "tests/ops/routers/test_users.py",
        "tests/scripts/test_reconcile_april_detail.py",
        "tests/scripts/test_seed_lake_snapshot.py",
        "tests/shared/test_minio_packfallback.py",
    )
)

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

    ``tests/integration/airflow/`` is the interesting pair. Its venv installs
    ``apache-airflow``, ``pytest``, ``psycopg2-binary`` and ``requests``, so
    ``mocker`` genuinely does not exist in that interpreter -- and that is an
    argument that the venv is built wrong, not that the convention forks.
    ``pytest-mock`` depends only on ``pytest``, which that venv already has.
    Both files are waived by name against G4, like the eighteen others, because
    a waiver is how a defect waits its turn and an exemption says the code is
    correct.
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
ROUTE_WAIVERS = tuple(
    Waiver(subject, gap="G6", owner=162)
    for subject in (
        # container_health has no TestClient anywhere in the repository. Its
        # two interesting endpoints do have tests -- which call the handlers
        # directly, and were green throughout the eleven hours
        # /project-status/{project} was returning 404 in production.
        "container_health: GET /health",
        "container_health: GET /metrics",
        "container_health: GET /oneoff-processes",
        "container_health: GET /project-status/{project}",
        # G6 named these two.
        "ops: POST /maintenance/evict-delisted-cooldowns",
        "ops: POST /maintenance/reconcile-cooldown-cohorts",
        # And these six it did not: measured by eye on 2026-08-31, found by
        # walking app.routes on 2026-08-31.
        "ops: GET /admin/snapshots/adaptive-refresh/latest",
        "ops: GET /admin/snapshots/adaptive-refresh/{snapshot_id}",
        "ops: GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download",
        "ops: GET /coordination/status",
        "ops: POST /coordination/begin-validation",
        "ops: POST /coordination/cancel",
    )
)

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


def _requested_routes(directories: list[Path]) -> set[tuple[str, str]]:
    """``(METHOD, path)`` literals the tests under *directories* request."""
    hits: set[tuple[str, str]] = set()
    for directory in directories:
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in _HTTP_VERBS
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                    and node.args[0].value.startswith("/")
                ):
                    hits.add((node.func.attr.upper(), node.args[0].value.split("?")[0]))
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
    it was. That attribution is also why ``tests/test_container_health_app.py``
    contributes nothing here: it is a Layer 1 test of a service sitting in
    Layer 0's directory, which is G9, and the two gaps are the same mistake
    seen from two sides.
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
LAYER_2_WAIVERS = tuple(
    Waiver(subject, gap="G14", owner=162)
    for subject in (
        "airflow/sql/delete_stale_emails.sql",
        "archiver/sql/get_queue_cleanup_candidates.sql",
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
        "archiver/sql/lake_snapshot_selectors/price_increase.sql",
        "archiver/sql/lake_snapshot_selectors/relisted_vin.sql",
        "archiver/sql/lake_snapshot_selectors/srp_fallback.sql",
        "archiver/sql/lake_snapshot_selectors/stable_state_run.sql",
        "archiver/sql/lake_snapshot_selectors/state_change_run.sql",
        "dashboard/sql/data_health_batch_outcomes.sql",
        "dashboard/sql/data_health_block_rate.sql",
        "dashboard/sql/data_health_cooldown_cohorts.sql",
        "dashboard/sql/data_health_inventory_coverage.sql",
        "dashboard/sql/data_health_price_freshness.sql",
        "dashboard/sql/data_health_scrape_volume.sql",
        "ops/sql/evict_delisted_cooldowns.sql",
        "ops/sql/expire_orphan_detail_claims.sql",
        "ops/sql/insert_artifact_event.sql",
        "ops/sql/insert_blocked_cooldown_cleared_event.sql",
        "ops/sql/mark_artifact_status.sql",
        "ops/sql/select_live_cooldown_listings.sql",
        "ops/sql/select_pending_cleared_listings.sql",
        "ops/sql/select_stuck_processing_artifacts.sql",
        "processing/sql/batch_lookup_vin_to_listing.sql",
        "processing/sql/claim_artifacts.sql",
        "processing/sql/clear_blocked_cooldown.sql",
        "processing/sql/delete_price_observation.sql",
        "processing/sql/delete_price_observation_by_vin.sql",
        "processing/sql/get_active_search_configs.sql",
        "processing/sql/get_tracked_models.sql",
        "processing/sql/insert_artifact_event.sql",
        "processing/sql/insert_blocked_cooldown_cleared_event.sql",
        "processing/sql/insert_detail_claim_event.sql",
        "processing/sql/insert_price_observation_event.sql",
        "processing/sql/insert_tracked_model_event.sql",
        "processing/sql/insert_vin_to_listing_event.sql",
        "processing/sql/lookup_vin_collision.sql",
        "processing/sql/mark_artifact_status.sql",
        "processing/sql/release_detail_claims.sql",
        "processing/sql/upsert_price_observation.sql",
        "processing/sql/upsert_tracked_model.sql",
        "processing/sql/upsert_vin_to_listing.sql",
        "scraper/sql/get_blocked_cooldown_attempts.sql",
        "scraper/sql/insert_blocked_cooldown_event.sql",
        "scraper/sql/upsert_blocked_cooldown.sql",
    )
)

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


def test_every_production_sql_file_is_touched_by_a_layer_2_test():
    """54 of 76 are not, at the *weakest* available reading.

    The reading is deliberately weak: a file counts as covered if some module
    under ``tests/integration/sql/`` so much as names it or its constant. The
    contract is clear that a weak reading is not the rule, and this one is
    chosen anyway for a specific reason -- 54 files fail it. A stricter check
    can only find more, so nothing is being hidden, and tightening it is worth
    doing when the number is small enough for the difference to be legible.
    Plan 162 owns both halves.

    The failure this catches is already in the tree and is the sharpest example
    of what the contract calls worse than no test: ``test_ops_queries.py`` and
    ``test_processing_queries.py`` are named for the services whose statements
    they are supposed to execute, import nothing from either ``queries.py``,
    and paraphrase the SQL instead. A paraphrase passes forever. It is a copy
    that cannot notice the original changed.
    """
    layer_2 = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((TESTS_DIR / "integration" / "sql").rglob("*.py"))
    )
    assert layer_2.strip(), "tests/integration/sql/ holds no Python at all"
    untouched = {
        relative
        for relative in production_sql_files()
        if Path(relative).stem not in layer_2
        and Path(relative).stem.upper() not in layer_2
    }
    _assert_exactly(
        untouched,
        LAYER_2_WAIVERS,
        "Every production .sql file is executed by a Layer 2 test. Flyway's "
        "migrations and dbt's models are the two exemptions; a statement no "
        "layer ever runs against a real engine is the search_path incident "
        "waiting to happen again.",
    )


# ---------------------------------------------------------------------------
# Rule 6 -- the layer numbers in the code are this document's.
# ---------------------------------------------------------------------------
LAYER_NUMBER_WAIVERS = tuple(
    Waiver(subject, gap="G11", owner=162)
    for subject in (
        "tests/integration/archiver/test_flush_silver_observations.py: 1, not 4",
        "tests/integration/archiver/test_flush_staging_events.py: 1, not 4",
        "tests/integration/ops/conftest.py: 3, not 4",
        "tests/integration/ops/test_access_requests.py: 3, not 4",
        "tests/integration/ops/test_auth.py: 3, not 4",
        "tests/integration/ops/test_deploy_intent.py: 3, not 4",
        "tests/integration/ops/test_maintenance_api.py: 3, not 4",
        "tests/integration/ops/test_scrape.py: 3, not 4",
        "tests/integration/ops/test_search_crud.py: 3, not 4",
        "tests/integration/ops/test_user_management.py: 3, not 4",
        "tests/integration/sql/test_dashboard_queries.py: 1, not 2",
        "tests/integration/sql/test_ops_queries.py: 1, not 2",
        "tests/integration/sql/test_ops_views.py: 1, not 2",
        "tests/integration/sql/test_processing_queries.py: 1, not 2",
        "ci.yml 'Run SQL smoke tests (Layer 1)': 1, not 2",
        "ci.yml 'Run API integration tests (Layer 3)': 3, not 4",
    )
)

# A module's *own* claim is a leading ``Layer N`` on the first line of its
# docstring. A ``Layer N`` further in is a cross-reference to another layer --
# ``test_maintenance_api.py`` points at the Layer 1 tests beside it -- and
# reading those as claims would fail on prose that is correct.
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
    """G13, and the only one of the seven with nothing left to waive.

    ``tests/test_planning_docs.py`` passed or failed on one machine, one OS and
    one commit purely on whether the checkout directory name was a valid Python
    identifier: 35 passed as ``cartracker-scraper``, 2 failed as
    ``new_car_tracker``, which is what CI uses. The repo root carries an
    ``__init__.py``, so pytest walks up for the package root and where it stops
    depends on the directory's name. ``PYTHONPATH`` settles it. CAR-42 fixed
    the one step that lacked it; this keeps the next one from being added.
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
    + LAYER_NUMBER_WAIVERS
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
        ("layer numbering", LAYER_NUMBER_WAIVERS),
    ],
)
def test_no_waiver_is_listed_twice(rule, waivers):
    """Duplicates would make the list look longer than the debt it records."""
    subjects = [waiver.subject for waiver in waivers]
    duplicated = sorted({s for s in subjects if subjects.count(s) > 1})
    assert not duplicated, f"{rule} waivers listed more than once: {duplicated}"
