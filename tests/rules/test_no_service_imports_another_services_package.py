"""A service's code is reachable only over HTTP; no service imports another's.

Plan 162 Stage AL, gap G33.

**The rule is not worth having for two violations. It is worth having because
every seam statement depends on it being true.** The HTTP statements in
``docs/TESTING.md`` §*How a service reaches another service* describe the one
approved channel between services; an import is a second channel those
statements cannot see, and a dependency travelling through it is invisible to
every contract, caller reader and mock rule at the seam.

**Both violations today are a shared declaration misfiled into a service
package**, not a service calling another's code — §Stage AL of the plan says
what each needs. ``airflow.dags.coordination_contract`` is a pure data module
four ``ops`` modules consume, and it moves to ``shared/``.
``container_health.expected`` resolves from ``maintenance-running-set.txt``
and belongs to a package that cannot import ``shared/``; it is closed by
``ops`` reading the manifest instead — one file, two readers, the shape
``healthcheck-exemptions.txt`` already runs.

**``shared/`` is not a violation.** Both sides importing a common library is
the design; the defect is one service's package appearing on another's import
path.

**What a service is, is derived from ``docker-compose.yml``.** A directory a
compose service builds from (``build.dockerfile: <root>/Dockerfile``) or
mounts code out of (a ``./<dir>`` volume holding ``.py`` files) is that
service's code. ``airflow/dags`` is why the derivation cannot stop at Python
packages: it holds no ``__init__.py``, resolves from ``ops`` as a namespace
package all the same, and is exactly where one of the two violations reaches.
"""
from __future__ import annotations

import ast
from functools import lru_cache

import yaml

from tests.rules.test_testing_contract import REPO_ROOT, service_packages


@lru_cache(maxsize=1)
def service_code_roots() -> frozenset[str]:
    """Every top-level directory whose code a compose service runs.

    Two shapes, both read from ``docker-compose.yml`` rather than listed: a
    ``build.dockerfile`` of the form ``<root>/Dockerfile``, and a ``./<dir>``
    volume whose directory holds ``.py`` files — which is how the Airflow
    containers get ``airflow/dags`` without building an image from it.
    """
    compose = yaml.safe_load(
        (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    )
    roots: set[str] = set()
    for service in (compose.get("services") or {}).values():
        build = service.get("build")
        if isinstance(build, dict):
            dockerfile = build.get("dockerfile") or ""
            if "/" in dockerfile:
                roots.add(dockerfile.split("/")[0])
        for volume in service.get("volumes") or []:
            if not isinstance(volume, str) or not volume.startswith("./"):
                continue
            host = volume[2:].split(":", 1)[0]
            directory = REPO_ROOT / host
            if directory.is_dir() and any(directory.rglob("*.py")):
                roots.add(host.split("/", 1)[0])
    return frozenset(roots)


def _import_targets(node: ast.AST) -> list[str]:
    """The dotted module paths one import statement names, absolute only.

    A relative import cannot leave its own package, so ``node.level`` being
    set takes the statement out of scope rather than needing resolution.
    """
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom) and node.module and not node.level:
        return [node.module]
    return []


def _resolves_in_repo(module: str) -> bool:
    """Does *module* name a file or package in this repository?

    This is what tells ``airflow.dags.coordination_contract`` — a directory on
    disk here — from ``airflow.operators.python``, which is the installed
    library and resolves to nothing in the tree.
    """
    base = REPO_ROOT.joinpath(*module.split("."))
    return base.is_dir() or base.with_suffix(".py").is_file()


def cross_service_imports() -> set[str]:
    """Every import in one service's code that resolves into another's."""
    roots = service_code_roots()
    found: set[str] = set()
    for root in sorted(roots):
        for path in sorted((REPO_ROOT / root).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                tree = ast.parse(
                    path.read_text(encoding="utf-8"), filename=str(path)
                )
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                for module in _import_targets(node):
                    head = module.split(".")[0]
                    if head == root or head not in roots:
                        continue
                    if _resolves_in_repo(module):
                        found.add(
                            f"{path.relative_to(REPO_ROOT).as_posix()} "
                            f"imports {module}"
                        )
    return found


# Keyed on file-plus-import, never a line number, for the reason
# `GUESSED_BOUND_WAIVERS` gives: every edit above a line rots the key, and a
# rotted entry either grandfathers something nobody chose or silently stops
# covering what it named.
#
# **An entry here is "not yet converted", not a tolerated violation.** Both are
# a shared declaration misfiled into a service package, and §Stage AL of
# `docs/plans/plan_162_testing_census_and_restructure.md` names each one's
# repair: `coordination_contract` moves to `shared/`, and the
# `container_health.expected` import is closed by `ops` reading
# `maintenance-running-set.txt` — the file that declaration is itself resolved
# from. Draining this ledger is that conversion; deleting an entry without it
# is the rule below going red.
CROSS_SERVICE_IMPORT_LEDGER: tuple[str, ...] = (
    "ops/coordination_drain.py imports airflow.dags.coordination_contract",
    "ops/coordination_release.py imports container_health.expected",
)


def test_the_service_root_corpus_is_complete():
    """The floor. A derivation that loses a service stops reading its imports.

    Derived equality, not a count: every top-level Python package except
    ``shared/`` must be claimed by the compose file as some service's code.
    ``shared/`` is the one package that is deliberately nobody's — it is the
    library both sides of the seam import — and ``tests/`` is the suite,
    excluded by ``service_packages()`` itself. The non-package roots
    (``airflow/``, mounted rather than built) cannot appear in this equality;
    what guards them is the ledger's staleness direction, because losing
    ``airflow`` from the roots strands an entry that names it.
    """
    packaged = frozenset(
        root for root in service_code_roots()
        if (REPO_ROOT / root / "__init__.py").is_file()
    )
    assert packaged == service_packages() - {"shared"}, (
        f"docker-compose.yml claims {sorted(packaged)} as built service code "
        f"and the tree holds packages {sorted(service_packages() - {'shared'})}. "
        f"A package the compose derivation misses is a service whose imports "
        f"the rule below never reads."
    )


def test_no_service_imports_another_services_package():
    """An import between services is a channel no seam rule can see.

    Both directions, which is what keeps the ledger describing something: an
    import not in the ledger fails, and a ledger entry describing no import
    fails until it is deleted — so a repair cannot leave its entry behind, and
    a reader that goes blind strands both seeded entries loudly.
    """
    found = cross_service_imports()
    ledgered = set(CROSS_SERVICE_IMPORT_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "one service's code imports another's, outside the ledger:\n  "
        + "\n  ".join(unwaived)
        + "\n\nA dependency between services travels over HTTP, or the "
        "declaration both need moves to shared/ (or to a file both read, "
        "where an image cannot carry shared/). The ledger records what is "
        "not yet converted; adding to it is a decision, not a convenience."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these ledger entries no longer describe an import and must be "
        "deleted:\n  " + "\n  ".join(stale)
    )
