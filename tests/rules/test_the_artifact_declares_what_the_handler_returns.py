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


def _exit_codes(
    function: ast.AST, module: ast.Module, path: Path,
) -> tuple[frozenset[int], bool]:
    """Codes this handler can answer with, and whether every exit was readable.

    The second half is what makes the rule safe. A `return` this cannot resolve
    to a status -- a bare dict, a variable, a call into another module -- means
    the handler's success code is reachable after all, and the caller skips it.
    """
    helpers = {
        node.name: node for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    helpers.update(_imported_helpers(module, path))

    codes: set[int] = set(_codes_at_call_sites(function, {}))
    complete = True
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
            inner_codes, inner_complete = _exit_codes(helpers[name], module, path)
            codes |= inner_codes
            if not inner_complete:
                complete = False
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
