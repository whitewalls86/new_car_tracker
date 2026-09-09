"""The branch list, read out of the model SQL rather than maintained.

Plan 162 Stage S. Three lists in this repository already claim to cover the dbt
models' branches -- ``archiver/config/lake_snapshot_selectors.yml`` with real
production rows, ``scripts/seed_lake_snapshot_fixture.py`` with synthetic ones,
and ``dbt/models/*/unit_tests.yml`` with mocked inputs -- and **none of them is
derived from the models**. No two are checked against each other. This module is
the artifact that was missing: the branch list itself, so that the three become
a comparison rather than three separate readings of somebody's understanding.

**It parses compiled SQL, not model source.** ``regex_matches()``,
``parquet_source()``, ``datediff_days()`` and ``now_ts()`` are Jinja macros that
sit *inside* the expressions a branch lives in, so raw model text does not parse
and a stub renderer that rendered one of them branchless would undercount
without failing. ``dbt compile`` needs no data at all -- measured 2026-09-06, it
produced all 23 models and all 161 data tests against an empty warehouse -- so
depending on it costs a compile step, not a database.

**Branch identity is positional, and that is a decision rather than a
convenience.** A branch is keyed ``model.scope.kind.ordinal``. Keying on the
predicate text instead would rename a branch on every edit and silently detach
whatever claimed it, which is the failure that turns a coverage list into
decoration. The predicate travels alongside as :attr:`Branch.predicate`, printed
by the reconciliation for a human to read, and is never the key. The accepted
cost is that reordering two ``CASE`` arms transfers a claim between them --
which changes the model's semantics anyway, and so is already review's business.

**Every branch counts the same.** 73 of the 216 branch points a fresh compile
yields are ``coalesce`` fallbacks, and 48 of those are ``coalesce(field, '')``
normalizations inside the two fingerprint concats. Filtering them out was considered and rejected: a
filter is a judgement that shrinks the denominator, which is the defect this
plan has now found in four separate instruments, and a field that is never null
in the fixture is a field the fingerprint has never been shown to distinguish
on. See the plan document, "Six decisions taken while scoping this stage".
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import build_scope

REPO_ROOT = Path(__file__).parents[2]

#: dbt writes compiled model SQL here. The directory is a build artifact, so
#: every consumer of this module has to have run ``dbt compile`` (or any command
#: that implies it) first, and :func:`compiled_model_paths` says so rather than
#: returning an empty list that would make every downstream count read zero.
COMPILED_ROOT = REPO_ROOT / "dbt" / "target" / "compiled" / "cartracker" / "models"

DIALECT = "duckdb"

#: The scope name given to a model's outermost ``SELECT``. Deliberately not a
#: legal identifier, so it cannot collide with a CTE somebody adds later.
FINAL_SCOPE = "<final>"


@dataclass(frozen=True)
class Branch:
    """One branch point in one model, addressed by where it sits.

    ``predicate`` is the branch's condition rendered back to SQL. It is a
    fingerprint for review and for probe generation -- never an identity. Two
    branches with identical predicate text in different scopes are different
    branches, and one branch whose predicate is edited is still the same branch.

    ``phase`` is empty for the 16 models that compile identically whether or not
    their relation already exists, and ``full`` or ``incremental`` for the seven
    that do not. See :func:`branch_list` for why that distinction is not
    optional.
    """

    model: str
    scope: str
    kind: str
    ordinal: int
    predicate: str
    phase: str = ""

    @property
    def id(self) -> str:
        suffix = f"@{self.phase}" if self.phase else ""
        return f"{self.model}.{self.scope}.{self.kind}.{self.ordinal}{suffix}"

    def __str__(self) -> str:  # pragma: no cover - diagnostic only
        return f"{self.id}  `{self.predicate}`"


def _manifest_model_relative_paths(base: Path) -> set[str]:
    """Each model's source path, relative to ``models/``, from dbt's manifest.

    Walked up from the compiled root rather than hardcoded, so the same rule
    resolves ``dbt/target/compiled/...`` (manifest two levels up, in
    ``target/``) and the temporary copies ``compiled_in_both_phases()`` makes
    (manifest copied into the temp directory beside ``compiled/``).

    **The manifest's ``path``, not its ``name``.** dbt lays the compiled tree
    out so that a model's compiled SQL sits at exactly ``<compiled
    root>/<node path>``, which makes the mapping a lookup rather than a search.
    Reading the name and searching for it is what put 45 fixture files in the
    model list -- see :func:`compiled_model_paths`.
    """
    import json

    for candidate in [base, *base.parents]:
        manifest = candidate / "manifest.json"
        if manifest.is_file():
            document = json.loads(manifest.read_text(encoding="utf-8"))
            paths = {
                node["path"]
                for node in document["nodes"].values()
                if node["resource_type"] == "model"
            }
            assert paths, f"{manifest} declares no models"
            return paths
    raise FileNotFoundError(
        f"no manifest.json found beside {base} or any of its parents. The "
        f"model list comes from dbt's manifest, not from filename convention "
        f"-- a compiled data test declared in a plain schema.yml would "
        f"otherwise enumerate as a model, its WHERE clause would enter the "
        f"branch denominator, and its SQL would be mutated as model code."
    )


def compiled_model_paths(root: Path | None = None) -> list[Path]:
    """Every compiled model, at the path dbt's manifest gives it.

    Read by lookup -- ``<compiled root>/<node path>`` -- and never by search.

    **The search was wrong, and it was wrong quietly.** This globbed every
    ``*.sql`` under the compiled root and kept the ones whose *stem* matched a
    manifest model name. dbt writes each unit test's **input fixtures** into
    ``<properties file>.yml/`` named after the input *model* -- so
    ``marts/unit_tests.yml/stg_blocked_cooldown_events.sql`` holds a literal
    ``select ... union all select ...`` row list and matched the filter.
    Measured 2026-09-09 on this project: **68 paths returned, 45 of them unit
    test fixtures and 23 of them models.** Two thirds of the branch-coverage
    denominator was fixture rows, on every green run, attributed to the models
    those fixtures feed.

    It also produced the flake that found it. Three unit tests in
    ``marts/coverage/unit_tests.yml`` declare ``input:
    ref('stg_blocked_cooldown_events')`` with different rows, and all three
    compile to that one path under ``threads: 2``; an interleaved write leaves
    SQL with a ``union all select`` missing from the middle, which is a
    ``sqlglot`` ``ParseError`` in a file this function should never have
    returned. Nothing in production is affected -- dbt executes SQL it holds in
    memory and ``target/compiled`` is an inspection artifact, which is why the
    build reported ``PASS=290 ERROR=0`` while the file on disk was torn.

    The previous docstring argued for the manifest over filename convention and
    was right about that; it applied the manifest to the *name*, and the name is
    the part dbt reuses. The path is unique per model.
    """
    base = root or COMPILED_ROOT
    if not base.is_dir():
        raise FileNotFoundError(
            f"{base} does not exist. The branch list is read from dbt's compiled "
            f"output, so `dbt compile --profiles-dir . --target duckdb` (or any "
            f"build) has to have run first. Returning an empty list here would "
            f"make every branch-coverage number downstream read zero and pass."
        )
    declared = _manifest_model_relative_paths(base)
    paths = sorted(base / relative for relative in declared)
    missing = [str(path) for path in paths if not path.is_file()]
    assert not missing, (
        f"{len(missing)} model(s) the manifest declares have no compiled SQL "
        f"at the path it gives:\n    " + "\n    ".join(missing) +
        "\n\nDropping them silently would shrink the branch denominator and "
        "read as coverage improving."
    )
    return paths


def _conjuncts(predicate: exp.Expression) -> list[exp.Expression]:
    """Split on ``AND`` only.

    ``where a and b`` is two independent guards and each can be exercised or
    missed on its own. ``where a or b`` is one guard with two ways to pass, and
    splitting it would demand coverage of a branch the model does not have.
    """
    out: list[exp.Expression] = []
    stack = [predicate]
    while stack:
        node = stack.pop()
        if isinstance(node, exp.And):
            stack.extend([node.left, node.right])
        else:
            out.append(node)
    return out


def _own_alias(scope) -> str | None:
    """The CTE or derived-table alias naming *scope*, if it has one."""
    parent = scope.expression.parent
    while parent is not None:
        alias = parent.args.get("alias")
        if isinstance(alias, exp.TableAlias) and alias.name:
            return alias.name
        if isinstance(parent, exp.CTE) and parent.alias:
            return parent.alias
        if isinstance(parent, (exp.Subquery, exp.Paren)):
            parent = parent.parent
            continue
        return None
    return None


def _anchor_of(scope, names: dict[int, str]) -> str:
    """The nearest enclosing scope that already has a name."""
    node = scope.expression.parent
    while node is not None:
        if isinstance(node, exp.Select) and id(node) in names:
            return names[id(node)]
        node = node.parent
    return FINAL_SCOPE


def _role_of(scope) -> str:
    if scope.is_derived_table:
        return "derived"
    if scope.is_union:
        return "union"
    return "subquery"


def scope_names(tree: exp.Expression) -> tuple[list, dict[int, str]]:
    """Name every scope in *tree*, preferring the alias a reader would grep for.

    Almost every scope in these models is a named CTE, and its alias is the
    stablest name available: it survives edits elsewhere in the model and it is
    what somebody looking for the branch will search. The exceptions are
    unaliased derived tables and scalar subqueries -- ``int_latest_observation``
    uses one of each -- and those are named ``<anchor>/derived0``,
    ``<anchor>/subquery0`` by role and sibling order under their nearest named
    ancestor.

    A global traversal counter was tried first and rejected: it renames every
    later scope in the model when an earlier one is added, which moves branch
    ids that nothing about the branch changed. Sibling order under a named
    anchor carries the same exposure as an ordinal within a scope -- adding a
    sibling ahead of one shifts it -- which is the cost already accepted for
    positional identity, and no more.
    """
    scopes = list(build_scope(tree).traverse())
    names: dict[int, str] = {}

    # Two passes, because traversal order is not containment order: a scalar
    # subquery inside a CTE's WHERE can be visited before the CTE itself, and a
    # one-pass walk would anchor it to <final> and give it a name that moves the
    # day traversal order changes.
    for scope in scopes:
        if scope.is_root:
            names[id(scope.expression)] = FINAL_SCOPE
        else:
            alias = _own_alias(scope)
            if alias:
                names[id(scope.expression)] = alias

    counters: dict[tuple[str, str], int] = {}
    for scope in scopes:
        if id(scope.expression) in names:
            continue
        anchor = _anchor_of(scope, names)
        role = _role_of(scope)
        index = counters.get((anchor, role), 0)
        counters[(anchor, role)] = index + 1
        names[id(scope.expression)] = f"{anchor}/{role}{index}"

    return scopes, names


def _owned_nodes(scope) -> Iterator[exp.Expression]:
    """Nodes belonging to *scope* itself, not to a scope nested inside it.

    A ``CASE`` inside a CTE belongs to that CTE, and a probe for it has to run
    against that CTE rather than against the model's final ``SELECT`` -- which
    is why this walk stops at the boundary instead of using ``find_all``.
    """
    root = scope.expression

    def walk(node: exp.Expression) -> Iterator[exp.Expression]:
        for child in node.args.values():
            children = child if isinstance(child, list) else [child]
            for item in children:
                if not isinstance(item, exp.Expression):
                    continue
                if isinstance(item, (exp.Select, exp.Subquery, exp.CTE)):
                    continue
                yield item
                yield from walk(item)

    yield from walk(root)


def _normalize(node: exp.Expression) -> str:
    return " ".join(node.sql(dialect=DIALECT).split())


def _and_all(conditions: Sequence[exp.Expression]) -> exp.Expression:
    """``a AND b AND c``, parenthesised so precedence cannot bite."""
    current = exp.paren(conditions[0].copy())
    for condition in conditions[1:]:
        current = exp.And(this=current, expression=exp.paren(condition.copy()))
    return current


def _none_of(conditions: Sequence[exp.Expression]) -> exp.Expression:
    """``NOT a AND NOT b`` -- the condition under which a CASE reaches its ELSE.

    Written three-valued-safe: a WHEN whose condition is NULL does not fire, so
    reaching the ELSE means every condition was *not true* rather than false,
    and ``NOT (c)`` alone would be NULL where ``c`` is NULL and count as
    neither arm.
    """
    negated = [
        exp.Not(this=exp.func("coalesce", condition.copy(), exp.false()))
        for condition in conditions
    ]
    return _and_all(negated)


def _all_null(expressions: Sequence[exp.Expression]) -> exp.Expression:
    """``a IS NULL AND b IS NULL`` -- when a coalesce falls through to the next."""
    return _and_all([exp.Is(this=e.copy(), expression=exp.Null()) for e in expressions])


def _first_wins(node: exp.Expression) -> exp.Expression:
    """Whether ``greatest``/``least``'s first argument is the one that wins.

    The branch a ``least(a, b)`` holds is which side the cap fell on, so the
    condition is a comparison rather than the call's value.
    """
    rest = node.args.get("expressions") or []
    if not rest:
        return exp.true()
    comparison = exp.LTE if isinstance(node, exp.Least) else exp.GTE
    return _and_all([
        comparison(this=node.this.copy(), expression=other.copy()) for other in rest
    ])


def branches_in_scope(scope, model: str, scope_name: str) -> list[Branch]:
    """Every branch point owned by one ``SELECT``.

    The kinds are the shapes that make a row take one path rather than another.
    ``outer_join`` is included because a left join's two outcomes -- a matched
    row and a null-extended one -- are exactly a branch, and a fixture that only
    ever matches has never exercised the model's behaviour on a miss.
    """
    found: list[Branch] = []
    counts: dict[str, int] = {}

    def add(kind: str, node: exp.Expression) -> None:
        ordinal = counts.get(kind, 0)
        counts[kind] = ordinal + 1
        found.append(Branch(model, scope_name, kind, ordinal, _normalize(node)))

    owned = list(_owned_nodes(scope))

    for node in owned:
        if isinstance(node, exp.Case):
            # A *simple* CASE (`case source when 'detail' then ...`) stores the
            # operand in `this` and bare values in its WHENs, so the arm's
            # condition is the comparison, not the value. Taking the value would
            # hand the probe `where 'detail'`, which is not a predicate at all.
            operand = node.args.get("this")
            conditions = [
                exp.EQ(this=operand.copy(), expression=c.this.copy())
                if operand is not None else c.this
                for c in node.args.get("ifs") or ()
            ]
            for condition in conditions:
                add("case_arm", condition)
            if node.args.get("default") is not None:
                # The ELSE arm is taken when every WHEN was false, which is what
                # a probe has to evaluate. Storing the CASE node itself would
                # store a value where a condition belongs.
                add("case_else", _none_of(conditions))
        elif isinstance(node, exp.Coalesce):
            # The i-th fallback is used when everything before it is NULL. The
            # fallback *value* is not the branch -- the nullness of what
            # precedes it is.
            preceding = [node.this]
            for fallback in node.args.get("expressions") or []:
                add("coalesce_fallback", _all_null(preceding))
                preceding = preceding + [fallback]
        elif isinstance(node, exp.Filter):
            condition = node.expression
            add("agg_filter", condition.this if isinstance(condition, exp.Where) else condition)
        elif isinstance(node, exp.Nullif):
            # nullif(a, b) yields NULL exactly when a = b.
            add("nullif", exp.EQ(this=node.this.copy(), expression=node.expression.copy()))
        elif isinstance(node, (exp.Greatest, exp.Least)):
            add("greatest_least", _first_wins(node))
        elif isinstance(node, exp.Join):
            side = (node.side or node.kind or "").upper()
            if side in ("LEFT", "RIGHT", "FULL"):
                add("outer_join", node.args.get("on") or node)

    root = scope.expression
    for key, kind in (("where", "where_conjunct"),
                      ("having", "having_conjunct"),
                      ("qualify", "qualify_conjunct")):
        clause = root.args.get(key)
        if clause is not None:
            for conjunct in _conjuncts(clause.this):
                add(kind, conjunct)

    return found


def branches_for_model(path: Path, phase: str = "") -> list[Branch]:
    """The branch list for one compiled model, in scope order."""
    model = path.stem
    tree = sqlglot.parse_one(path.read_text(encoding="utf-8"), dialect=DIALECT)
    scopes, names = scope_names(tree)
    found: list[Branch] = []
    for scope in scopes:
        name = names[id(scope.expression)]
        found.extend(
            Branch(b.model, b.scope, b.kind, b.ordinal, b.predicate, phase)
            for b in branches_in_scope(scope, model, name)
        )
    return found


def branch_list(root: Path | None = None) -> list[Branch]:
    """Every branch point across every dbt model, derived, never maintained.

    Single-phase: what one ``dbt compile`` left on disk. Correct only when the
    caller knows which phase that was, which is why
    :func:`branch_list_both_phases` exists and why the gate uses it.
    """
    found: list[Branch] = []
    for path in compiled_model_paths(root):
        found.extend(branches_for_model(path))
    return found


def branch_list_both_phases(full_root: Path, incremental_root: Path) -> list[Branch]:
    """The union over both compile states, which is the only honest branch list.

    **A model's compiled SQL is not a function of its source alone.** Seven of
    the 23 models wrap part of their body in ``{% if is_incremental() %}``, and
    that predicate is false whenever the relation does not yet exist -- so a
    compile against an empty warehouse silently omits 39 lines of SQL, every one
    of them a Plan 123 late-arrival lookback window. Enumerating from a fresh
    compile alone would report those models fully covered while never having
    seen the path production actually runs, which is this plan's own recurring
    defect wearing a new hat.

    It is not enough to compile twice and concatenate, because **the ordinals
    move**: an incremental block that contributes a ``where`` conjunct shifts
    every later conjunct in that scope by one, so the same id would name
    different predicates in the two states. Branches from models that compile
    identically in both states are emitted once, unphased; branches from the
    seven that differ are emitted twice, tagged ``@full`` and ``@incremental``,
    and each tag owes its own coverage. That is not double-counting -- a cold
    build and an incremental build are different code, and the fixture exercises
    them in separate phases precisely because they are.
    """
    full_paths = {path.name: path for path in compiled_model_paths(full_root)}
    incr_paths = {path.name: path for path in compiled_model_paths(incremental_root)}

    missing = sorted(set(full_paths) ^ set(incr_paths))
    if missing:
        raise ValueError(
            f"the two compile states do not hold the same models: {missing}. "
            f"One of the two compiles was partial, so the union would be a "
            f"branch list with a hole in it rather than a superset."
        )

    found: list[Branch] = []
    for name, full_path in sorted(full_paths.items()):
        incr_path = incr_paths[name]
        same = full_path.read_text(encoding="utf-8") == incr_path.read_text(encoding="utf-8")
        if same:
            found.extend(branches_for_model(full_path))
        else:
            found.extend(branches_for_model(full_path, "full"))
            found.extend(branches_for_model(incr_path, "incremental"))
    return found


def by_model(branches: Sequence[Branch]) -> dict[str, list[Branch]]:
    grouped: dict[str, list[Branch]] = {}
    for branch in branches:
        grouped.setdefault(branch.model, []).append(branch)
    return grouped


def by_kind(branches: Sequence[Branch]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for branch in branches:
        counts[branch.kind] = counts.get(branch.kind, 0) + 1
    return counts
