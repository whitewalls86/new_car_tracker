"""A status code means the same thing to the caller and the service.

Plan 162 Stage AL, gap G33 — statement 7, the only one with no existing rule
at all.

**Three rules watch the caller and none of them asks this.** The status is
read, the outcome is not discarded, the DAG's comparisons are against values
its service emits — and all three are satisfied by a caller that reads a
code and acts on the wrong meaning, which is what `ops/coordination_drain.py`
does with `/ready`'s 503 today. The meaning half had no artifact to be
checked against; `shared/api_envelope.py` is that artifact now.

**Scope is derived from the envelope, not hardcoded to 503.** A code with
one declared meaning cannot be misread by code alone; the rule reads the
codes the envelope declares *more than one* meaning for — today exactly 503,
whose three members are `Busy`, `DatabaseUnavailable` and
`DependencyUnreachable` — and grows its scope the day another code's meaning
splits.

**For each declared response at an ambiguous code, the declaration must be
proven against the handler.** The declared description must be one of the
envelope's meanings for that code, and every meaning the handler can
actually answer that code with — a raised declared refusal, or an
`HTTPException` whose detail is a string literal, one level into helpers —
must equal it. A handler whose meaning the reader cannot resolve is
*unproven*, not skipped: "we could not tell what this means" is the state
the defect lives in, which is Stage W's clause again.

**The seeded ledger is the live defect set §Stage AL names, sharpened.**
The four `/ready` 503s declare `DependencyUnreachable`'s text and mean
*busy* — their handlers raise a dict detail the reader cannot read, so they
sit here as unproven until the conversion makes them say what they mean.
Of the 29 `"Database unavailable."` declarations, the routes whose handlers
provably raise only that text are correct and carry no entry; the rest are
proven mismatches (`ops/routers/coordination.py` raises five distinct
meanings against one declared text) or unproven, and the reconciliation is
in plan_162 §Record. `Busy` is declared by no response anywhere — the
defect's other face — and has its own one-entry ledger below.
"""
from __future__ import annotations

import ast
import json
from functools import lru_cache
from pathlib import Path

from tests.rules.test_testing_contract import _imported_helpers
from tests.rules.test_the_artifact_declares_what_the_handler_returns import (
    REPO_ROOT,
    _handler_index,
    _handler_name,
)
from tests.rules.test_the_refusal_envelope_is_one_declaration import (
    SHARED,
    declared_refusals,
)

CONTRACTS = REPO_ROOT / "contracts"

_VERBS = frozenset({"get", "post", "put", "patch", "delete"})


@lru_cache(maxsize=1)
def ambiguous_meanings() -> dict[int, frozenset[str]]:
    """Code to its declared meanings, for codes with more than one."""
    by_code: dict[int, set[str]] = {}
    for _name, (code, meaning) in declared_refusals(SHARED).items():
        by_code.setdefault(code, set()).add(meaning)
    return {
        code: frozenset(meanings)
        for code, meanings in by_code.items()
        if len(meanings) > 1
    }


def _raised_meanings(
    function: ast.AST, module: ast.Module, path: Path, code: int, depth: int = 0,
) -> tuple[set[str], bool]:
    """Meaning texts *function* can answer *code* with, and full readability.

    A raised declared refusal resolves to its class's meaning; an
    ``HTTPException`` at that code resolves to its detail only when the
    detail is a string literal — a dict (which is what `/ready` raises) or a
    variable is a meaning the reader cannot read, and says so. One level
    into same-module and imported helpers, the same depth every other reader
    here goes.
    """
    refusals = declared_refusals(SHARED)
    helpers = {
        node.name: node for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    helpers.update(_imported_helpers(module, path))
    texts: set[str] = set()
    complete = True
    for node in ast.walk(function):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if name in refusals and refusals[name][0] == code:
            texts.add(refusals[name][1])
            continue
        if name == "HTTPException":
            raised: int | None = None
            if node.args and isinstance(node.args[0], ast.Constant):
                raised = node.args[0].value
            for keyword in node.keywords:
                if keyword.arg == "status_code" and isinstance(
                    keyword.value, ast.Constant
                ):
                    raised = keyword.value.value
            if raised != code:
                continue
            detail: ast.AST | None = node.args[1] if len(node.args) > 1 else None
            for keyword in node.keywords:
                if keyword.arg == "detail":
                    detail = keyword.value
            if isinstance(detail, ast.Constant) and isinstance(detail.value, str):
                texts.add(detail.value)
            else:
                complete = False
        elif name in helpers and depth < 1:
            inner_texts, inner_complete = _raised_meanings(
                helpers[name], module, path, code, depth + 1
            )
            texts |= inner_texts
            complete = complete and inner_complete
    return texts, complete


def unproven_meanings() -> set[str]:
    """Every declared ambiguous-code response not proven against its handler."""
    found: set[str] = set()
    indexes: dict[str, dict[str, tuple[ast.AST, ast.Module, Path]]] = {}
    for file in sorted(CONTRACTS.glob("*.json")):
        spec = json.loads(file.read_text(encoding="utf-8"))
        service = file.stem
        for route, operations in sorted(spec["paths"].items()):
            for verb, operation in operations.items():
                if verb not in _VERBS:
                    continue
                for code_text, response in (
                    operation.get("responses") or {}
                ).items():
                    if not code_text.isdigit():
                        continue
                    code = int(code_text)
                    if code not in ambiguous_meanings():
                        continue
                    declared = response.get("description")
                    key = f"{service} {verb.upper()} {route} {code}"
                    if declared not in ambiguous_meanings()[code]:
                        found.add(key)
                        continue
                    if service not in indexes:
                        indexes[service] = _handler_index(service)
                    handler = indexes[service].get(
                        _handler_name(operation.get("operationId", ""), route, verb)
                    )
                    if handler is None:
                        found.add(key)
                        continue
                    texts, complete = _raised_meanings(*handler, code)
                    if not complete or texts != {declared}:
                        found.add(key)
    return found


# Keyed on service-verb-route-code, never a line number. **The live defect
# set, seeded rather than waived**: the four `/ready` rows declare
# `DependencyUnreachable`'s text for a 503 that means *busy* and raise a
# dict the reader cannot read; the coordination rows declare one meaning
# while their handlers raise up to five; the admin rows produce their 503
# through a rendered template whose meaning no reader can see; the
# `processing` batch row declares a text the envelope does not carry at
# all. Each drains by the handler raising the named refusal whose meaning
# the declaration carries — conversion work, held open here.
UNPROVEN_MEANING_LEDGER: tuple[str, ...] = (
    "archiver GET /ready 503",
    "dbt_runner GET /ready 503",
    "ops POST /admin/access-requests/{req_id}/approve 503",
    "ops POST /admin/access-requests/{req_id}/deny 503",
    "ops POST /admin/deploy/release 503",
    "ops POST /admin/deploy/request 503",
    "ops GET /admin/searches/ 503",
    "ops POST /admin/searches/ 503",
    "ops POST /admin/searches/{search_key} 503",
    "ops POST /admin/searches/{search_key}/delete 503",
    "ops GET /admin/searches/{search_key}/edit 503",
    "ops POST /admin/searches/{search_key}/toggle 503",
    "ops GET /admin/snapshots/adaptive-refresh/latest 503",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id} 503",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download 503",
    "ops POST /admin/users/{user_id}/revoke 503",
    "ops POST /admin/users/{user_id}/role 503",
    "ops GET /auth/check 503",
    "ops POST /coordination/authorize 503",
    "ops POST /coordination/complete 503",
    "ops GET /coordination/drain-status 503",
    "ops POST /coordination/host-evidence 503",
    "ops GET /coordination/release-status 503",
    "ops GET /coordination/status 503",
    "ops POST /request-access 503",
    "processing POST /process/batch 503",
    "processing GET /ready 503",
    "scraper GET /ready 503",
)

# The other direction: a declared meaning no response anywhere uses. `Busy`
# is the four `/ready` 503s' true meaning, and the fact that nothing
# declares it *is* the defect those rows record — this entry and those four
# drain together, by the repair rather than by either list being argued
# with.
UNDECLARED_MEANING_LEDGER: tuple[str, ...] = ("Busy",)


def test_the_meaning_rule_has_something_to_read():
    """The floor. Both corpora can empty without an edit to this file.

    The envelope's 503 split collapsing — three meanings becoming one —
    would take every ambiguous code with it and leave the rule below
    reading nothing; a contracts regeneration that dropped the ambiguous
    declarations would do the same from the other side.
    """
    assert ambiguous_meanings(), (
        "the envelope declares no code with more than one meaning, so the "
        "rule below has no scope. If the 503 split was deliberately "
        "collapsed, this rule and its ledgers leave with it — by decision, "
        "not by silence."
    )
    read = {
        int(code)
        for file in sorted(CONTRACTS.glob("*.json"))
        for operations in json.loads(file.read_text(encoding="utf-8"))["paths"].values()
        for verb, operation in operations.items()
        if verb in _VERBS
        for code in (operation.get("responses") or {})
        if code.isdigit() and int(code) in ambiguous_meanings()
    }
    assert read, (
        "no contract declares a response at any ambiguous code; the rule "
        "below is passing over nothing."
    )


def test_every_declared_meaning_of_an_ambiguous_code_is_proven():
    """Where one code carries several meanings, the declaration must be
    the one the handler actually answers with — proven, not plausible.

    Both directions: a newly unproven declaration fails on arrival, and a
    conversion that proves one fails until its entry is deleted.
    """
    found = unproven_meanings()
    ledgered = set(UNPROVEN_MEANING_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "these declared responses carry a meaning their handler is not "
        "proven to answer with:\n  " + "\n  ".join(unwaived)
        + "\n\nRaise the named refusal from shared/api_envelope.py whose "
        "meaning the declaration carries, or fix the declaration. Adding a "
        "ledger entry is a decision, not a convenience."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these declarations are now proven and their ledger entries must "
        "be deleted:\n  " + "\n  ".join(stale)
    )


def test_every_ambiguous_meaning_is_declared_somewhere():
    """A meaning nothing declares is a distinction nobody can rely on.

    `Busy` is the reason the 503 split exists, and no response declares it
    — the defect the four `/ready` rows record, seen from the vocabulary's
    side. Both directions, so the entry dies with the repair.
    """
    used = {
        response.get("description")
        for file in sorted(CONTRACTS.glob("*.json"))
        for operations in json.loads(file.read_text(encoding="utf-8"))["paths"].values()
        for operation in operations.values()
        if isinstance(operation, dict)
        for response in (operation.get("responses") or {}).values()
    }
    refusals = declared_refusals(SHARED)
    undeclared = sorted(
        name
        for name, (code, meaning) in refusals.items()
        if code in ambiguous_meanings() and meaning not in used
    )

    unwaived = sorted(set(undeclared) - set(UNDECLARED_MEANING_LEDGER))
    assert not unwaived, (
        f"these envelope meanings are declared by no response: {unwaived}. "
        f"Either a response should carry them or the meaning should not "
        f"exist — deciding which is the drain, not the ledger."
    )
    stale = sorted(set(UNDECLARED_MEANING_LEDGER) - set(undeclared))
    assert not stale, (
        f"these ledger entries name meanings that are now declared and "
        f"must be deleted: {stale}"
    )
