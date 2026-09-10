"""No module calls an owned service by hand — the caller edge's ledger.

Plan 162 Stage AL, gap G33 — the client rule the plan's design seeds at 17,
and the caller half of statement 1.

**Without this rule the glue runs from handler to contract and stops.**
Every other seam rule holds B's side — what a handler does, what its
declaration says, what a test may fabricate of it. The caller's side has
only the permissive reader: ``caller_endpoints()`` counts a call it can
place and *drops* one it cannot, which is load-bearing (it is what stops
the reader accusing a cars.com fetch of missing a contract) and is also why
the six calls to ``dbt_runner`` endpoints deleted in April kept passing
until somebody noticed by hand. Making the reader exact is not a change to
the rule but to what it reads — 17 modules reach 35 endpoints through
ad-hoc clients, and there is nothing common to key on. So the common thing
has to be built: **one declared client seam**, routes resolved from the
generated contracts, request models enforced where the call is made. That
seam does not exist yet.

**This ledger is the conversion's work list, seeded before the seam
exists** — the same move as the unreadable-exits and retyped-status
ledgers. The signature is the URL literal itself: a module that writes
``http://<owned-host>:`` in code is reaching a service by hand, and that
is not escapable by a cleverer call shape, because whatever the shape, the
host has to be named. Draining an entry is converting the module to the
seam; a new module naming an owned host fails on arrival instead of
joining the quiet majority.

**The Airflow constraint is known and is the seam's first design input.**
Thirteen of the seventeen are DAG modules, and ``airflow/dags`` cannot
import ``shared/`` — so the seam reaches them the way every previous
declaration did: a generated copy under the mounted tree with an equality
rule, or a file both sides read. ``post_json`` in the plugins is where
that lands.

**The request half of statement 2 stays unasserted, and this docstring
says so** rather than implying coverage: no rule in this repository reads
an outbound request body. The seam is what makes that assertable — the
call site hands over the declared request model instead of a dict — which
is why the halves drain together.
"""
from __future__ import annotations

import ast

from tests.rules.test_no_service_imports_another_services_package import (
    service_code_roots,
)
from tests.rules.test_testing_contract import REPO_ROOT
from tests.service_contracts import caller_endpoints, owned_hosts


def _names_an_owned_host_in_code(source: str, tree: ast.AST) -> bool:
    """Does this module hold an owned host's URL in a non-docstring string?

    Docstrings are excluded by node identity, not by text — the lesson
    ``test_the_service_seam_corpus_is_not_empty`` records: ``sensors.py``
    documents a URL it never calls, and ``ast.get_docstring`` returns
    cleaned text that matches nothing.
    """
    owned = set(owned_hosts())
    if not any(f"//{host}:" in source for host in owned):
        return False
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
    return any(
        any(f"//{host}:" in node.value for host in owned)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    )


def hand_written_callers() -> set[str]:
    """Every production module that names an owned service's host in code."""
    found: set[str] = set()
    for root in sorted(service_code_roots()):
        for path in sorted((REPO_ROOT / root).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            source = path.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=str(path))
            except SyntaxError:
                continue
            if _names_an_owned_host_in_code(source, tree):
                found.add(path.relative_to(REPO_ROOT).as_posix())
    return found


# Keyed on the module, which is the grain the conversion happens at. Seeded
# 2026-09-10 at the 17 the plan's design names — thirteen DAGs, the
# archiver's self-scheduling calls, the two coordination gates and the
# admin panel's dbt calls. **An entry is "not yet converted": it leaves
# when its module reaches services through the declared client seam**, and
# not before, because deleting an entry without the seam is this rule going
# quiet about exactly the modules it exists to watch.
HAND_WRITTEN_CALLER_LEDGER: tuple[str, ...] = (
    "airflow/dags/cleanup_queue.py",
    "airflow/dags/compact_silver.py",
    "airflow/dags/dbt_build.py",
    "airflow/dags/disk_usage.py",
    "airflow/dags/export_ci_lake_snapshot.py",
    "airflow/dags/flush_silver_observations.py",
    "airflow/dags/flush_staging_events.py",
    "airflow/dags/hourly_analytics_refresh.py",
    "airflow/dags/orphan_checker.py",
    "airflow/dags/pack_bronze_html.py",
    "airflow/dags/results_processing.py",
    "airflow/dags/scrape_detail_pages.py",
    "airflow/dags/scrape_listings.py",
    "archiver/app.py",
    "ops/coordination_drain.py",
    "ops/coordination_release.py",
    "ops/routers/admin.py",
)


def test_the_caller_signature_and_the_resolver_agree():
    """The floor: two independent readers, one caller set, equal exactly.

    The signature (an owned host named in code) and the resolver
    (``caller_endpoints`` placing a call against a contract) are different
    instruments with different failure modes — a `<NAME>_URL` constant
    renamed breaks one, a route scheme change breaks the other — and today
    they agree on all seventeen modules. Either drifting from the other is
    a reader losing callers silently, which is the failure every seam rule
    here exists against.
    """
    named = hand_written_callers()
    resolving = {
        path.relative_to(REPO_ROOT).as_posix()
        for root in sorted(service_code_roots())
        for path in sorted((REPO_ROOT / root).rglob("*.py"))
        if "__pycache__" not in path.parts
        and caller_endpoints(path.relative_to(REPO_ROOT).as_posix())
    }
    assert named == resolving, (
        f"the host-literal signature finds {sorted(named - resolving)} that "
        f"resolve to no endpoint, and {sorted(resolving - named)} resolve "
        f"without naming a host in code. The first is a dead or drifted "
        f"call; the second is a caller one of the two readers has gone "
        f"blind to."
    )


def test_no_module_calls_a_service_by_hand():
    """A hand-built call is a call no rule can hold to the contract.

    Both directions: a new module naming an owned host fails on arrival,
    and a module converted to the seam fails until its entry is deleted —
    so the ledger is the conversion's work list and can only shrink.
    """
    found = hand_written_callers()
    ledgered = set(HAND_WRITTEN_CALLER_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "these modules reach an owned service by hand, outside the "
        "ledger:\n  " + "\n  ".join(unwaived)
        + "\n\nCall through the declared client seam once it exists; until "
        "then, a new ad-hoc caller is a decision this ledger records, not "
        "a convenience."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these ledger entries name modules that no longer call by hand "
        "and must be deleted:\n  " + "\n  ".join(stale)
    )
