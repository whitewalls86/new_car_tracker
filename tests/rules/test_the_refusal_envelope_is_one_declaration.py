"""The refusal envelope is one declaration, and its one copy cannot drift.

Plan 162 Stage AL, gap G33.

**`shared/api_envelope.py` is where every declared refusal's status code
lives as a literal, and `container_health/api_envelope.py` is its one
permitted copy.** The copy exists for the same reason `api_models.py` has
one there: the image that holds the Docker socket grant copies only its own
package, so it cannot import `shared/` — a boundary being honoured, not a
duplication that escaped notice. A copy nothing compares is Stage W's
defect one seam over, which is why this rule lands with the module rather
than after it.

**Read by AST, not imported**, because that is how every other rule reads
these files — `_codes_at_call_sites` resolves a raised refusal through the
same class-attribute literals this rule checks, so a shape this rule
accepts is a shape those readers can read.
"""
from __future__ import annotations

import ast

from tests.rules.test_testing_contract import REPO_ROOT

SHARED = "shared/api_envelope.py"
COPY = "container_health/api_envelope.py"


def declared_refusals(relative: str) -> dict[str, tuple[int, str]]:
    """``class name -> (status code, meaning)`` from one envelope file.

    Only classes carrying both a ``status_code`` int literal and a
    ``meaning`` string literal count — the base class declares neither and
    is scaffolding, not a member.
    """
    tree = ast.parse(
        (REPO_ROOT / relative).read_text(encoding="utf-8"), filename=relative
    )
    found: dict[str, tuple[int, str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        code: int | None = None
        meaning: str | None = None
        for statement in node.body:
            if not isinstance(statement, ast.Assign):
                continue
            for target in statement.targets:
                if not isinstance(target, ast.Name):
                    continue
                if (
                    target.id == "status_code"
                    and isinstance(statement.value, ast.Constant)
                    and isinstance(statement.value.value, int)
                ):
                    code = statement.value.value
                if (
                    target.id == "meaning"
                    and isinstance(statement.value, ast.Constant)
                    and isinstance(statement.value.value, str)
                ):
                    meaning = statement.value.value
        if code is not None and meaning is not None:
            found[node.name] = (code, meaning)
    return found


def test_the_envelope_declares_what_a_refusal_needs():
    """The floor. A member short of either literal is one no reader resolves.

    Derived equality, not a count: every class the module registers in
    ``DECLARED_REFUSALS`` must be readable as a ``(status_code, meaning)``
    pair, and every readable pair must be registered — a class outside the
    registry is a refusal the comparison rules never see.
    """
    readable = declared_refusals(SHARED)
    assert readable, f"{SHARED} declares no readable refusal classes."

    tree = ast.parse((REPO_ROOT / SHARED).read_text(encoding="utf-8"))
    registered = {
        element.id
        for node in ast.walk(tree)
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "DECLARED_REFUSALS"
        and node.value is not None
        for call in ast.walk(node.value)
        if isinstance(call, ast.Tuple)
        for element in call.elts
        if isinstance(element, ast.Name)
    }
    assert registered == set(readable), (
        f"{SHARED} registers {sorted(registered)} and declares readable "
        f"members {sorted(readable)}. A refusal outside DECLARED_REFUSALS "
        f"is one the lookup rules cannot resolve; a registered name with no "
        f"literals is one no AST reader can credit."
    )


def test_the_refusal_envelope_is_one_declaration():
    """The copy matches, in both directions, member by member.

    A meaning changed in one file fails until the other moves with it —
    which is the property that makes a copy tolerable at all, and it is
    asserted rather than remembered because `container_health`'s image
    boundary means nothing will ever import the two side by side.
    """
    shared = declared_refusals(SHARED)
    copy = declared_refusals(COPY)

    missing = sorted(set(shared) - set(copy))
    extra = sorted(set(copy) - set(shared))
    assert not missing and not extra, (
        f"{COPY} is missing {missing} and adds {extra}. The copy carries "
        f"exactly the members shared/ declares, or it is not a copy."
    )

    drifted = sorted(
        f"{name}: shared declares {shared[name]}, the copy {copy[name]}"
        for name in shared
        if shared[name] != copy[name]
    )
    assert not drifted, (
        "the container_health copy has drifted from shared/:\n  "
        + "\n  ".join(drifted)
    )
