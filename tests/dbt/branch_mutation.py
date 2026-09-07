"""Removing a guard must break the constraint that guard produces.

Plan 162 Stage S, exit 3. 161 column constraints are declared across the 23
models and nothing demonstrated that any of them was load-bearing. A
``not_null`` is a claim about a branch guard, so **the mutation set is derived
from the branch list rather than written by hand**: for each branch, delete it
from the model, rebuild the model alone, and run that model's own compiled data
tests. A constraint that goes red is held up by that branch. A constraint no
mutation can break is decorative.

**Two classes, not three.** ``not_null_mart_vehicle_snapshot_vin`` cannot be
broken from inside ``mart_vehicle_snapshot``: the guard is ``where vin17 is not
null`` in ``int_latest_observation``, and the mart takes that model as its
driving table. Calling the mart's constraint decorative is correct rather than a
false verdict -- it restates an invariant established upstream, where
``not_null_int_latest_observation_vin17`` sits and *is* locally load-bearing.
Cross-model mutation would re-confirm the same guard down a more expensive path.
What is kept from that idea is a reason string: when a constraint comes back
decorative, :func:`upstream_guard_for` names the upstream branch that actually
holds it, so the ledger is a map of where each invariant is enforced rather than
a list of shrugs -- and the case where *no* model declares it becomes visible.

**The stated limit.** Mutation measures the code as it stands, so it cannot
distinguish "decorative because redundant" from "decorative but a useful
regression barrier" -- ``not_null_mart_vehicle_snapshot_vin`` would still catch
someone turning ``obs`` into a left join later. That is why the exit records
these rather than deleting them.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from sqlglot import exp

from tests.dbt.branch_list import (
    Branch,
    branches_in_scope,
    scope_names,
)

#: dbt compiles a model's data tests into a directory beside the model.
TEST_DIR_SUFFIX = ".schema.yml"


@dataclass(frozen=True)
class Mutant:
    branch: Branch
    sql: str


@dataclass(frozen=True)
class ConstraintVerdict:
    model: str
    test_name: str
    killed_by: tuple[str, ...]
    upstream_guard: str = ""

    @property
    def load_bearing(self) -> bool:
        return bool(self.killed_by)


def _drop_from_conjunction(clause: exp.Expression, ordinal: int) -> exp.Expression | None:
    """Remove the *ordinal*-th ``AND`` conjunct, or the whole clause if it is last."""
    parts: list[exp.Expression] = []
    stack = [clause]
    while stack:
        node = stack.pop()
        if isinstance(node, exp.And):
            stack.extend([node.right, node.left])
        else:
            parts.append(node)
    if ordinal >= len(parts):
        return clause
    kept = [p for i, p in enumerate(parts) if i != ordinal]
    if not kept:
        return None
    current = kept[0]
    for part in kept[1:]:
        current = exp.And(this=current, expression=part)
    return current


def mutate(tree: exp.Expression, branch: Branch) -> exp.Expression | None:
    """A copy of *tree* with *branch* removed, or ``None`` if it cannot be.

    Every operator is a *removal*: the mutant must still be valid SQL that
    computes the model without that guard. An operator that produced a syntax
    error would show up as every constraint dying, which reads as total coverage
    and is the one failure mode this must not have.
    """
    clone = tree.copy()
    scopes, names = scope_names(clone)

    target = None
    for scope in scopes:
        name = names[id(scope.expression)]
        if name != branch.scope:
            continue
        for found in branches_in_scope(scope, branch.model, name):
            if (found.kind, found.ordinal) == (branch.kind, branch.ordinal):
                target = (scope, found)
                break
        if target:
            break
    if target is None:
        return None

    scope, _ = target
    select = scope.expression

    if branch.kind in ("where_conjunct", "having_conjunct", "qualify_conjunct"):
        key = {"where_conjunct": "where",
               "having_conjunct": "having",
               "qualify_conjunct": "qualify"}[branch.kind]
        clause = select.args.get(key)
        if clause is None:
            return None
        replacement = _drop_from_conjunction(clause.this, branch.ordinal)
        if replacement is None:
            select.set(key, None)
        else:
            clause.set("this", replacement)
        return clone

    nodes = _owned_branch_nodes(scope, branch.kind)
    if branch.kind == "case_arm":
        index, case = _nth_case_arm(nodes, branch.ordinal)
        if case is None:
            return None
        ifs = list(case.args.get("ifs") or ())
        del ifs[index]
        case.set("ifs", ifs)
        if not ifs and case.args.get("default") is not None:
            case.replace(case.args["default"].copy())
        return clone

    if branch.kind == "case_else":
        case = _nth(nodes, branch.ordinal, lambda n: n.args.get("default") is not None)
        if case is None:
            return None
        case.set("default", None)
        return clone

    if branch.kind == "coalesce_fallback":
        node, offset = _nth_coalesce(nodes, branch.ordinal)
        if node is None:
            return None
        expressions = list(node.args.get("expressions") or ())
        del expressions[offset]
        if expressions:
            node.set("expressions", expressions)
        else:
            node.replace(node.this.copy())
        return clone

    if branch.kind == "agg_filter":
        node = _nth(nodes, branch.ordinal, lambda n: isinstance(n, exp.Filter))
        if node is None:
            return None
        node.replace(node.this.copy())
        return clone

    if branch.kind == "nullif":
        node = _nth(nodes, branch.ordinal, lambda n: isinstance(n, exp.Nullif))
        if node is None:
            return None
        node.replace(node.this.copy())
        return clone

    if branch.kind == "greatest_least":
        node = _nth(nodes, branch.ordinal,
                    lambda n: isinstance(n, (exp.Greatest, exp.Least)))
        if node is None:
            return None
        node.replace(node.this.copy())
        return clone

    if branch.kind == "outer_join":
        node = _nth_outer_join(select, branch.ordinal)
        if node is None:
            return None
        # An inner join removes the null-extended arm while keeping the join.
        node.set("side", None)
        node.set("kind", "INNER")
        return clone

    return None


def _owned_branch_nodes(scope, kind: str) -> list[exp.Expression]:
    from tests.dbt.branch_list import _owned_nodes

    wanted = {
        "case_arm": exp.Case,
        "case_else": exp.Case,
        "coalesce_fallback": exp.Coalesce,
        "agg_filter": exp.Filter,
        "nullif": exp.Nullif,
        "greatest_least": (exp.Greatest, exp.Least),
    }[kind]
    return [n for n in _owned_nodes(scope) if isinstance(n, wanted)]


def _nth(nodes: Sequence[exp.Expression], ordinal: int, predicate) -> exp.Expression | None:
    seen = 0
    for node in nodes:
        if not predicate(node):
            continue
        if seen == ordinal:
            return node
        seen += 1
    return None


def _nth_case_arm(nodes, ordinal: int):
    seen = 0
    for node in nodes:
        for index in range(len(node.args.get("ifs") or ())):
            if seen == ordinal:
                return index, node
            seen += 1
    return -1, None


def _nth_coalesce(nodes, ordinal: int):
    seen = 0
    for node in nodes:
        for offset in range(len(node.args.get("expressions") or ())):
            if seen == ordinal:
                return node, offset
            seen += 1
    return None, 0


def _nth_outer_join(select: exp.Expression, ordinal: int):
    seen = 0
    for join in select.args.get("joins") or ():
        if (join.side or join.kind or "").upper() not in ("LEFT", "RIGHT", "FULL"):
            continue
        if seen == ordinal:
            return join
        seen += 1
    return None


_RELATION = re.compile(r'"[^"]+"\."[^"]+"\."([A-Za-z_][\w]*)"')


def compiled_tests_for(model: str, compiled_root: Path) -> list[tuple[str, str]]:
    """``(test name, SQL)`` for every data test dbt compiled for *model*."""
    directory = None
    for candidate in compiled_root.rglob(f"{model}{TEST_DIR_SUFFIX}"):
        if candidate.is_dir():
            directory = candidate
            break
    if directory is None:
        return []
    return [
        (path.stem, path.read_text(encoding="utf-8"))
        for path in sorted(directory.glob("*.sql"))
    ]


def retarget(sql: str, model: str, replacement: str) -> str:
    """Point a compiled statement at *replacement* instead of *model*."""
    return _RELATION.sub(
        lambda m: replacement if m.group(1) == model else m.group(0), sql
    )


def upstream_guard_for(model: str, column: str, branches: Sequence[Branch],
                       upstream: Sequence[str]) -> str:
    """The branch in an upstream model whose guard plausibly holds *column*.

    Deliberately a *name*, not a proof. The proof would be cross-model mutation,
    which this stage rejected on the argument that a propagated constraint is
    either mirrored upstream -- where local mutation finds it -- or is not, and
    then the finding is that the model establishing the invariant does not
    declare it. This turns a decorative verdict into a pointer at where the
    invariant actually lives.
    """
    for branch in branches:
        if branch.model not in upstream:
            continue
        if re.search(rf"\b{re.escape(column)}\b", branch.predicate):
            return branch.id
    return ""
