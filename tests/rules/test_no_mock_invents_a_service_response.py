"""No test may fabricate a response for a service this repository owns.

Plan 162 Stage AA, gap G23.

**This is the sibling rule to
``test_no_mock_invents_a_shape_production_defines``, at the HTTP boundary.**
That one refuses a mock that restates what a *function* returns; this one
refuses a mock that invents what a *service* answers. Both exist because a test
that writes down someone else's output passes for whatever its author typed.

The difference is which way the defect is silent. A test of our own endpoint
asserting an impossible code is self-correcting -- ``TestClient`` runs the real
route and the assertion goes red. A test of a *caller* invents both the code
and the body of a service it never contacts, so it passes for any pair its
author picked, and if the callee changes the caller's test still agrees with
itself.

**Ownership is derived, three ways, and only one of them is this stage's.**
``tests/service_contracts.py`` reads ``docker-compose.yml``: a service built
from a ``<package>/Dockerfile`` with a committed contract can be asked what it
returns; one with an ``image:`` and no ``build:`` is third-party; a host that is
no compose service at all is external. The last two are Stage AB's and are left
alone here rather than reported against a contract that does not exist.

**The demonstration this stage owes lives in this rule.**
``tests/ops/test_coordination_release.py`` fabricated ``{"known": False}`` from
``container_health``'s ``/project-status/{project}``, and that service cannot
produce it: both of its routes hardcode ``known: True``, so the model pins the
field ``Literal[True]`` and the contract carries ``"const": true``. The
fabricated body is refused by the schema rather than by a special case.
"""
from __future__ import annotations

import ast
from pathlib import Path

from tests.service_contracts import (
    body_violations,
    caller_endpoints,
    owned_hosts,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS = REPO_ROOT / "tests"

# Where a caller's HTTP seam is patched, the module it belongs to is the first
# part of the target. `mocker.patch("ops.coordination_drain.requests.get")`
# names `ops/coordination_drain.py`; `scrape_listings.requests.post` names a
# DAG, which the sys.path shim in tests/airflow makes importable bare.
_HTTP_CLIENTS = frozenset({"requests", "stdlib_requests", "http_requests", "httpx"})
_DAG_ROOT = "airflow/dags"


def _seam_module(target: str) -> str | None:
    """The production module whose HTTP client *target* patches."""
    parts = target.split(".")
    if len(parts) < 3 or parts[-2] not in _HTTP_CLIENTS:
        return None
    owner = parts[:-2]
    candidate = "/".join(owner) + ".py"
    if (REPO_ROOT / candidate).is_file():
        return candidate
    dag = f"{_DAG_ROOT}/{owner[-1]}.py"
    return dag if (REPO_ROOT / dag).is_file() else None


# A call that builds a body from the callee's contract rather than restating
# it. Handing one of these over is what draining a waiver looks like.
_DERIVED = frozenset({"service_response"})


def _fabrications(tree: ast.AST) -> dict[str, list[tuple[int, dict]]]:
    """Local name to the response bodies a test assigns onto it."""
    bodies: dict[str, list[tuple[int, dict]]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if isinstance(node.value, ast.Call):
            called = getattr(node.value.func, "id", "") or getattr(node.value.func, "attr", "")
            if called in _DERIVED:
                continue
        if not isinstance(node.value, ast.Dict):
            continue
        for target in node.targets:
            rendered = ast.unparse(target)
            if not rendered.endswith(".json.return_value"):
                continue
            root = rendered.split(".")[0]
            literal: dict = {}
            for key, value in zip(node.value.keys, node.value.values):
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    literal[key.value] = (
                        value.value if isinstance(value, ast.Constant) else "<expr>"
                    )
            bodies.setdefault(root, []).append((node.lineno, literal))
    return bodies


def _seams(tree: ast.AST) -> dict[str, str]:
    """Local name handed to a patched HTTP client, to that client's module."""
    seams: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        rendered = ast.unparse(node.func)
        if not (rendered.endswith(".patch") or rendered == "patch"):
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        module = _seam_module(str(node.args[0].value))
        if module is None:
            continue
        for keyword in node.keywords:
            if keyword.arg in {"return_value", "side_effect"}:
                if isinstance(keyword.value, ast.Name):
                    seams[keyword.value.id] = module
    return seams


def fabricated_service_responses() -> list[tuple[str, str, list[str]]]:
    """Every fabricated body a service this repository owns cannot produce."""
    offenders: list[tuple[str, str, list[str]]] = []
    for path in sorted(TESTS.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        seams = _seams(tree)
        if not seams:
            continue
        for name, bodies in _fabrications(tree).items():
            module = seams.get(name)
            if module is None:
                continue
            candidates = caller_endpoints(module)
            if not candidates:
                continue
            for lineno, body in bodies:
                where = f"{path.relative_to(REPO_ROOT).as_posix()}:{lineno}"
                reasons = body_violations(body, candidates) or [
                    "restates a body the callee owns; build it with "
                    "`service_response(package, verb, path)` so the two cannot "
                    f"disagree. Candidates: {[f'{p} {v} {r}' for p, v, r in candidates]}"
                ]
                offenders.append((where, module, reasons))
    return offenders


# Seeded 2026-09-09 and drained by repairing each fabrication to a body the
# callee can actually produce. Every entry is a test asserting a response no
# service in this repository returns.
FABRICATED_RESPONSE_WAIVERS: tuple[str, ...] = ()


def test_the_service_seam_corpus_is_not_empty():
    """The floor. A reader that resolves no seams accuses nobody.

    Both halves have to stay non-empty: the seams a test patches, and the
    endpoints the module behind them can call. A rename in either -- the client
    attribute, a ``<NAME>_URL`` constant, a compose service's dockerfile --
    empties this and leaves the rule below passing over nothing.
    """
    # Not a curated list. Every production module that names a host this
    # repository builds must resolve to at least one endpoint of that host --
    # so the set is derived from `docker-compose.yml` and the tree, and a
    # module that stops resolving fails here rather than quietly leaving the
    # rule below with less to read.
    # Every package with a committed contract is reachable by name. Removing a
    # service's `build.dockerfile` from docker-compose.yml drops it from
    # `owned_hosts`, and a floor that only counted callers did not notice: the
    # modules naming that host stopped being examined rather than started
    # failing, so every fabrication behind that seam went unreported and the
    # rule stayed green. The harness recorded exactly that as unnoticed.
    owned = set(owned_hosts())
    answered = set(owned_hosts().values())
    committed = {path.stem for path in (REPO_ROOT / "contracts").glob("*.json")}
    assert committed <= answered, (
        f"contracts exist for {sorted(committed - answered)} and no compose "
        f"service builds them. A package this repository ships a contract for "
        f"but no host answers for is a service whose callers this rule stops "
        f"examining -- silently, because a caller of an unowned host is out of "
        f"scope rather than in violation."
    )

    callers, blind = [], []
    for package in ("ops", "airflow/dags", "scraper", "archiver", "processing"):
        root = REPO_ROOT / package
        if not root.is_dir():
            continue
        for module in sorted(root.rglob("*.py")):
            if "__pycache__" in module.parts:
                continue
            # A host named in a docstring is prose, not a call. `sensors.py`
            # documents `http_health_sensor("archiver", "http://archiver:8001")`
            # in its own module docstring and reaches every service through a
            # parameter, so a text search demanded it resolve to an endpoint it
            # never names in code.
            source = module.read_text(encoding="utf-8")
            if not any(f"//{host}:" in source for host in owned):
                continue
            tree = ast.parse(source, filename=str(module))
            # Identified by node, not by text: `ast.get_docstring` returns the
            # cleaned string and `Constant.value` the raw one, so comparing the
            # two matches nothing and every docstring reads as code.
            docstrings = {
                id(node.body[0].value)
                for node in ast.walk(tree)
                if isinstance(
                    node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                )
                and node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            }
            in_code = any(
                any(f"//{host}:" in node.value for host in owned)
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in docstrings
            )
            if not in_code:
                continue
            relative = module.relative_to(REPO_ROOT).as_posix()
            callers.append(relative)
            if not caller_endpoints(relative):
                blind.append(relative)

    assert callers, (
        "no production module names a host this repository builds. Either the "
        "URL constants moved or docker-compose.yml stopped naming the "
        "dockerfiles that say which package answers for a host."
    )
    assert not blind, (
        f"these modules name a service this repository owns and resolve to none "
        f"of its endpoints: {blind}. The path they build no longer matches any "
        f"route in that service's contract -- which is either a dead call or a "
        f"reader that has stopped following, and both leave every fabrication "
        f"behind that seam unreported."
    )


def test_no_mock_invents_a_service_response():
    """A body the callee cannot produce is an assertion about nothing.

    The repair is to fabricate what the service actually answers -- or, where
    the fabrication exists to exercise a refusal the service cannot express, to
    stop asserting a branch that cannot happen.
    """
    offenders = [
        (where, module, reasons)
        for where, module, reasons in fabricated_service_responses()
        if where not in FABRICATED_RESPONSE_WAIVERS
    ]
    assert not offenders, (
        f"{len(offenders)} fabricated response(s) name a service this "
        f"repository owns and a body it cannot produce:\n  "
        + "\n  ".join(
            f"{where} (through {module})\n      " + "\n      ".join(reasons)
            for where, module, reasons in offenders
        )
    )


def test_every_waiver_names_a_fabrication_that_still_exists():
    """A waiver for a repaired site grandfathers nothing and hides the next."""
    live = {where for where, _module, _reasons in fabricated_service_responses()}
    stale = sorted(set(FABRICATED_RESPONSE_WAIVERS) - live)
    assert not stale, (
        f"these waivers name fabrications that no longer exist: {stale}."
    )
