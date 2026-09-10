"""The generated contract may not declare a code its handler cannot return.

Plan 162 Stage AA, gap G32.

**Two rules already stand either side of this and neither can see it.**
``test_every_route_declares_the_statuses_it_can_return`` compares a route's own
``responses={...}`` dict against the codes its handler produces, and
``test_every_status_code_a_route_can_produce_is_asserted`` requires a test for
each produced code. Both read the decorator and the AST. Stage Z's
``service-contracts`` job generates ``contracts/*.json`` from the running apps
and diffs it against itself. Nothing joins the two, so a code that reaches the
artifact without passing through a ``responses=`` dict is declared to every
reader of that contract and checked by nobody.

FastAPI puts one there on every route: the **success code**, 200 unless the
decorator says otherwise. For a route that answers a redirect on success and a
template on refusal, that 200 is a body no code path can produce -- and
``contracts/ops.json`` carries it today on the redirect-only admin routes.

**Found by inference rather than by a rule**, which is the reason this exists.
Draining the deploy buttons' ambiguity waiver made them visible to the coverage
rule, their phantom 200 surfaced there, and only then did reading the other
redirect-only routes show the same declaration sitting unchecked in the
artifact. A defect that has to be noticed is a defect that will be missed.

**A handler is only judged when every one of its exits is readable.** Most
routes return a bare dict on success, which is exactly how their 200 is
produced -- and an AST reader sees no status code in `return {"ok": True}`.
Treating that silence as "cannot produce 200" would report every healthy route
in the repository. So the reader resolves each `return` to a code, one level
into same-module helpers as ``_produced_codes`` does, and a handler with even
one exit it cannot resolve is left alone. The direction is safe: an unreadable
handler is skipped, never accused.
"""
from __future__ import annotations

import ast
import json
from functools import lru_cache
from pathlib import Path

import pytest

from tests.rules.test_testing_contract import (
    REPO_ROOT,
    _codes_at_call_sites,
    _imported_helpers,
)

CONTRACTS = REPO_ROOT / "contracts"

# `422` is FastAPI's validation response and reaches the artifact the same way
# the success code does. It has its own rule --
# `test_no_route_declares_a_422_no_request_can_trigger` -- with its own ledger
# and a permanent exemption for the two ownerless `/recaps/{slug}` routes, so
# claiming it here would be a second opinion about a settled question.
_OWNED_ELSEWHERE = frozenset({422})

_VERBS = frozenset({"get", "post", "put", "patch", "delete"})


def _handler_index(package: str) -> dict[str, tuple[ast.AST, ast.Module, Path]]:
    """Function name to its AST, module and path, for one service package."""
    found: dict[str, tuple[ast.AST, ast.Module, Path]] = {}
    for path in sorted((REPO_ROOT / package).rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(module):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                found.setdefault(node.name, (node, module, path))
    return found


@lru_cache(maxsize=None)
def _module_classes(relative: str) -> frozenset[str]:
    """Top-level class names of one module here, mirroring ``_module_functions``."""
    path = REPO_ROOT / relative
    if not path.is_file():
        return frozenset()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return frozenset()
    return frozenset(
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    )


def _known_classes(module: ast.Module, path: Path) -> set[str]:
    """Class names resolvable in *module* — local, or imported from this repo.

    What tells ``return ProcessArtifactResult(...)`` — a model instance the
    framework serialises as the success outcome — from a call into another
    module this reader cannot follow. Resolution mirrors
    ``_imported_helpers``, for the reason that docstring gives: the obvious
    refactor moves these classes between modules, and a name-only convention
    would go quietly blind to the move.
    """
    here = path.relative_to(REPO_ROOT).parent
    found = {node.name for node in module.body if isinstance(node, ast.ClassDef)}
    for node in ast.walk(module):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        if node.level:
            base = here
            for _ in range(node.level - 1):
                base = base.parent
            target = base.joinpath(*node.module.split("."))
        else:
            target = Path(*node.module.split("."))
        for candidate in (f"{target.as_posix()}.py", f"{target.as_posix()}/__init__.py"):
            classes = _module_classes(candidate)
            if not classes:
                continue
            for alias in node.names:
                if alias.name in classes:
                    found.add(alias.asname or alias.name)
            break
    return found


def _declared_success(function: ast.AST) -> int:
    """The success code the decorator declares — 200 unless it says otherwise."""
    for decorator in getattr(function, "decorator_list", []):
        if not isinstance(decorator, ast.Call):
            continue
        for keyword in decorator.keywords:
            if (
                keyword.arg == "status_code"
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, int)
            ):
                return keyword.value.value
    return 200


def _exit_codes(
    function: ast.AST, module: ast.Module, path: Path, success: int | None = None,
) -> tuple[frozenset[int], bool]:
    """Codes this handler can answer with, and whether every exit was readable.

    The second half is what makes the rule safe. A `return` this cannot resolve
    to a status -- a bare dict, a variable, a call into another module -- means
    the handler's success code is reachable after all, and the caller skips it.

    Three exit shapes resolve to the decorator's declared success code (Plan
    162 Stage AL, and the readable set went from 12 of 93 handlers to
    measurement at seed time): a returned instance of a class this repository
    defines, which FastAPI serialises as the success outcome; a kwarg-less
    framework response, whose ``endswith("Response")`` convention
    ``_returns_a_bare_value`` already leans on; and, through
    ``_codes_at_call_sites``, a raised declared refusal resolved from
    ``shared/api_envelope.py``. The rule that one unreadable exit disqualifies
    the whole handler is kept exactly -- the direction stays safe.
    """
    helpers = {
        node.name: node for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    helpers.update(_imported_helpers(module, path))
    classes = _known_classes(module, path)
    if success is None:
        success = _declared_success(function)

    codes: set[int] = set(_codes_at_call_sites(function, {}))
    complete = True

    # A dependency is an exit this reader cannot read. `Depends(...)` runs
    # before the handler and raises its own codes -- `require_snapshot_token`
    # carries the 401, 403 and 503 that `/download` declares -- so a handler
    # that has one is unreadable rather than judged on its body alone, which
    # is the same safe direction as an unresolvable `return`.
    defaults = list(function.args.defaults) + [
        default for default in function.args.kw_defaults if default is not None
    ]
    if any(
        keyword.arg == "dependencies"
        for decorator in getattr(function, "decorator_list", [])
        if isinstance(decorator, ast.Call)
        for keyword in decorator.keywords
    ) or any(
        isinstance(default, ast.Call)
        and (
            getattr(default.func, "id", None)
            or getattr(default.func, "attr", None)
        )
        == "Depends"
        for default in defaults
    ):
        complete = False

    for node in ast.walk(function):
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        value = node.value
        if not isinstance(value, ast.Call):
            complete = False
            continue
        name = getattr(value.func, "id", None) or getattr(value.func, "attr", None)
        explicit = {
            keyword.value.value for keyword in value.keywords
            if keyword.arg == "status_code"
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, int)
        }
        if explicit:
            codes |= explicit
        elif name in helpers:
            # A helper's *raised* codes are not its whole story. `_status`
            # raises 503 and returns a dict, so a caller that hands its result
            # straight back can answer 200 -- and reading only the raise would
            # report that 200 as unproducible on every coordination route.
            # The declared success is passed down, because a helper has no
            # decorator of its own to read one from.
            inner_codes, inner_complete = _exit_codes(
                helpers[name], module, path, success
            )
            codes |= inner_codes
            if not inner_complete:
                complete = False
        elif name == "RedirectResponse":
            # Kwarg-less: its framework default is already credited by
            # `_codes_at_call_sites`, and crediting the declared success too
            # would be the over-crediting direction this rule fears.
            pass
        elif name in classes or (name and name.endswith("Response")):
            codes.add(success)
        else:
            complete = False
    return frozenset(codes), complete


@lru_cache(maxsize=1)
def artifact_declarations() -> list[tuple[str, str, str, str, frozenset[int]]]:
    """``(service, path, verb, operation id, declared codes)`` from every contract."""
    rows: list[tuple[str, str, str, str, frozenset[int]]] = []
    for file in sorted(CONTRACTS.glob("*.json")):
        spec = json.loads(file.read_text(encoding="utf-8"))
        for route, operations in sorted(spec["paths"].items()):
            for verb, operation in operations.items():
                if verb not in _VERBS:
                    continue
                codes = frozenset(
                    int(code) for code in (operation.get("responses") or {})
                    if code.isdigit()
                )
                rows.append(
                    (file.stem, route, verb, operation.get("operationId", ""), codes)
                )
    return rows


def _handler_name(operation_id: str, route: str, verb: str) -> str:
    """The function behind an operation id, which ends in its path and method."""
    # Only the leading separator is dropped. FastAPI replaces every non-alnum
    # character with an underscore and keeps the runs -- `/x/{y}` becomes
    # `_x__y_` -- so collapsing them here resolved 73 of 93 routes and silently
    # excluded every path-parameter one, which is most of the interesting ones.
    slug = "".join(character if character.isalnum() else "_" for character in route)
    suffix = f"_{slug.lstrip('_')}_{verb}"
    return operation_id[: -len(suffix)] if operation_id.endswith(suffix) else operation_id


def phantom_declarations() -> list[str]:
    """Every artifact-declared code its handler has no exit for."""
    found: list[str] = []
    indexes: dict[str, dict[str, tuple[ast.AST, ast.Module, Path]]] = {}
    for service, route, verb, operation_id, declared in artifact_declarations():
        if service not in indexes:
            indexes[service] = _handler_index(service)
        handler = indexes[service].get(_handler_name(operation_id, route, verb))
        if handler is None:
            continue
        codes, complete = _exit_codes(*handler)
        if not complete or not codes:
            continue
        phantom = sorted(declared - codes - _OWNED_ELSEWHERE)
        if phantom:
            found.append(
                f"{service} {verb.upper()} {route} declares {phantom}; "
                f"{_handler_name(operation_id, route, verb)} answers "
                f"{sorted(codes)}"
            )
    return sorted(found)


# Empty by construction: the rule was written after the declarations it
# corrects, and the two routes that carried a phantom 200 now set
# `status_code=303` on their decorator, which is what tells FastAPI the success
# outcome is a redirect.
PHANTOM_DECLARATION_WAIVERS: tuple[str, ...] = ()


def test_the_artifact_declaration_corpus_is_not_empty():
    """The floor. A reader that resolves no handlers accuses nobody.

    Two halves can empty independently -- the contracts directory, and the
    operation-id-to-function resolution, which depends on a naming scheme
    FastAPI owns rather than this repository. Either going quiet leaves the
    rule below passing over nothing.
    """
    rows = artifact_declarations()

    # Not a threshold. Every service with a committed contract must appear, and
    # which services those are is settled by `test_every_service_has_a_committed
    # _contract` -- so this asks the same question that rule does rather than
    # guessing a number below today's count.
    described = {service for service, *_rest in rows}
    committed = {path.stem for path in CONTRACTS.glob("*.json")}
    assert described == committed, (
        f"contracts exist for {sorted(committed)} and declarations were parsed "
        f"for {sorted(described)}. A contract that parses to no route leaves "
        f"that whole service unchecked by the rule below."
    )

    # And every declaration resolves to a handler -- all of them, not most.
    # The first version of this reader collapsed the double underscores FastAPI
    # writes where `/{` meets, resolved 73 of 93, and passed a floor set at 80%
    # while silently excluding every path-parameter route. The three `/metrics`
    # routes that used to be unresolvable were the library's closure; they are
    # declared in their own services now, which is what makes `==` available
    # here instead of a tolerance.
    indexes: dict[str, dict[str, tuple[ast.AST, ast.Module, Path]]] = {}
    unresolved = []
    for service, route, verb, operation_id, _codes in rows:
        if service not in indexes:
            indexes[service] = _handler_index(service)
        if _handler_name(operation_id, route, verb) not in indexes[service]:
            unresolved.append(f"{service} {verb.upper()} {route}")
    assert not unresolved, (
        f"{len(unresolved)} of {len(rows)} declarations resolve to no handler "
        f"in their own service: {unresolved}. Every route in a contract is "
        f"served by a function in this repository, so one that resolves to "
        f"nothing means either the operation-id scheme moved underneath this "
        f"reader or a route is being registered by something outside the tree "
        f"-- and both leave the rule below measuring less than it appears to."
    )


def unreadable_handlers() -> set[str]:
    """Every handler with an exit ``_exit_codes`` cannot resolve to a code."""
    found: set[str] = set()
    indexes: dict[str, dict[str, tuple[ast.AST, ast.Module, Path]]] = {}
    seen: set[tuple[str, str]] = set()
    for service, route, verb, operation_id, _declared in artifact_declarations():
        if service not in indexes:
            indexes[service] = _handler_index(service)
        name = _handler_name(operation_id, route, verb)
        handler = indexes[service].get(name)
        if handler is None or (service, name) in seen:
            continue
        seen.add((service, name))
        codes, complete = _exit_codes(*handler)
        if not complete or not codes:
            _function, _module, path = handler
            found.add(f"{path.relative_to(REPO_ROOT).as_posix()}::{name}")
    return found


# Keyed on file-plus-handler, never a line number. **An entry is "not yet
# converted", and draining this ledger is the conversion**: a handler leaves
# it by returning its declared model instance and raising named refusals from
# `shared/api_envelope.py` -- the shape the 2026-09-10 probe proved free of
# any change on the wire (`processing` went 0-of-5 readable to 5-of-5 with
# the artifact byte-identical). While a handler is here, the artifact rule
# above cannot judge it, so every code it declares is a code checked by
# nobody -- which is what the seeded size of this list actually measures.
#
# Seeded 2026-09-10 at 58 handlers, the same tree Step 0's `12 81` baseline
# was reproduced on: the old reader judged 12 of 93 declarations, the Stage
# AL reader (model returns, framework responses and declared refusals credit
# the decorator's success code; a `Depends` disqualifies) judges 35 of 93
# handlers and leaves these 58.
UNREADABLE_EXIT_LEDGER: tuple[str, ...] = (
    "archiver/app.py::health",
    "archiver/app.py::ready",
    "archiver/app.py::run_cleanup_queue_batch",
    "archiver/app.py::trigger_cleanup_queue",
    "archiver/app.py::trigger_compact_silver",
    "archiver/app.py::trigger_disk_usage",
    "archiver/app.py::trigger_flush_silver",
    "archiver/app.py::trigger_flush_staging",
    "archiver/app.py::trigger_pack_bronze_html",
    "archiver/app.py::trigger_prune_packed_source_html",
    "archiver/app.py::trigger_snapshot_export",
    "archiver/app.py::trigger_verify_pack_read_path",
    "container_health/app.py::active_oneoff_processes",
    "container_health/app.py::health",
    "container_health/app.py::project_status",
    "dbt_runner/app.py::dbt_build",
    "dbt_runner/app.py::dbt_docs_generate",
    "dbt_runner/app.py::get_docs_status",
    "dbt_runner/app.py::get_selectors",
    "dbt_runner/app.py::health",
    "dbt_runner/app.py::ready",
    "ops/app.py::health",
    "ops/routers/coordination.py::authorize_coordination",
    "ops/routers/coordination.py::begin_coordination_drain",
    "ops/routers/coordination.py::begin_coordination_validation",
    "ops/routers/coordination.py::cancel_coordination",
    "ops/routers/coordination.py::complete_coordination",
    "ops/routers/coordination.py::coordination_drain_status",
    "ops/routers/coordination.py::coordination_release_status",
    "ops/routers/coordination.py::coordination_status",
    "ops/routers/coordination.py::local_drain_status",
    "ops/routers/coordination.py::request_coordination",
    "ops/routers/coordination.py::submit_host_evidence",
    "ops/routers/deploy.py::complete_deployment",
    "ops/routers/deploy.py::get_current_intent",
    "ops/routers/deploy.py::start_deploy_intent",
    "ops/routers/maintenance.py::evict_delisted_cooldowns",
    "ops/routers/maintenance.py::expire_orphan_detail_claims",
    "ops/routers/maintenance.py::reap_stuck_processing",
    "ops/routers/maintenance.py::reconcile_cooldown_cohorts",
    "ops/routers/scrape.py::advance_rotation",
    "ops/routers/scrape.py::claim_batch",
    "ops/routers/scrape.py::release_claims",
    "ops/routers/snapshots.py::download_snapshot_archive",
    "ops/routers/snapshots.py::get_latest_snapshot",
    "ops/routers/snapshots.py::get_snapshot_manifest",
    "processing/app.py::health",
    "processing/app.py::ready",
    "processing/routers/artifact.py::process_single_artifact",
    "processing/routers/batch.py::process_batch",
    "scraper/app.py::get_completed_jobs",
    "scraper/app.py::health",
    "scraper/app.py::list_all_jobs",
    "scraper/app.py::mark_job_fetched",
    "scraper/app.py::ready",
    "scraper/app.py::run_scrape_results",
    "scraper/app.py::scrape_detail",
    "scraper/app.py::scrape_detail_batch_endpoint",
)


def test_every_handlers_exits_are_readable():
    """Statement 4's rule: an unreadable handler is an unjudged one.

    The artifact rule above skips a handler it cannot read -- the safe
    direction -- and skipping is precisely how 81 of 93 declarations went
    unjudged while its floor asserted `==` and passed. This ledger is the
    skip set made loud: both directions, so a handler that becomes readable
    fails until its entry is deleted, and a new unreadable handler fails on
    arrival rather than joining the quiet majority.
    """
    found = unreadable_handlers()
    ledgered = set(UNREADABLE_EXIT_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "these handlers have an exit the reader cannot resolve to a status "
        "code, so the artifact rule cannot judge them:\n  "
        + "\n  ".join(unwaived)
        + "\n\nReturn the declared model instance and raise named refusals "
        "from shared/api_envelope.py, or add the exit shape to the reader if "
        "it is one the repository has legitimately adopted."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these handlers are now fully readable and their ledger entries "
        "must be deleted:\n  " + "\n  ".join(stale)
    )


@pytest.mark.parametrize("phantom", phantom_declarations() or [None])
def test_the_artifact_declares_no_code_its_handler_cannot_return(phantom):
    """A generated contract is only as true as the handler behind it.

    Stage Z made the contract impossible to forget to update. This makes it
    impossible for it to describe a response nothing sends -- which is the same
    defect one layer along, and the one `ARCHITECTURE.md:179` was.
    """
    if phantom is None:
        return
    assert phantom in PHANTOM_DECLARATION_WAIVERS, (
        f"{phantom}\n\nThe artifact carries a code no exit of that handler "
        f"produces. Set `status_code=` on the decorator where the success "
        f"outcome is not a 200, or stop declaring the code."
    )
