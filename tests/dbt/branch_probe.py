"""Whether a branch was taken, observed rather than claimed.

Plan 162 Stage S, exit 2. The alternative design was nominal: each of the three
lists that claim to cover the dbt models -- the snapshot selectors, the
synthetic fixture and the unit tests -- declares which branch ids it exercises,
and a gate checks both directions. That was rejected. It relocates the
hand-curation this stage exists to close, and a claim somebody types can be
wrong forever without anything failing.

**A probe asks the data instead.** For a branch whose condition is ``p``, the
probe is ``count(*) filter (where p)`` and ``count(*) filter (where not p)``,
evaluated in the branch's own scope. Both non-zero means both arms were
exercised; either zero names an arm no row has ever taken. Nothing is declared
and nothing can drift, because the question is asked of the same relations the
model itself reads.

**The scope is the whole difficulty.** A ``CASE`` inside a CTE has to be
evaluated against that CTE's own ``FROM``, not against the model's final
``SELECT`` -- the columns it names do not exist there. So a probe is built by
taking the scope's own ``FROM`` and joins, carrying the model's full ``WITH``
chain along so the references resolve, and replacing the projection with the
two counts.

**Not every branch is probeable, and the ones that are not are recorded rather
than assumed covered.** A condition over a window function or an aggregate has
no row-level scope to attach a ``filter`` to. :func:`probe_for` returns ``None``
for those and :data:`UNPROBEABLE_REASON` says why, so they appear in the record
as a stated limit instead of quietly counting as passes.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import sqlglot
from sqlglot import exp

from tests.dbt.branch_list import (
    DIALECT,
    REPO_ROOT,
    Branch,
    branches_in_scope,
    scope_names,
)

#: Why a branch could not be turned into a row-level probe.
UNPROBEABLE_REASON = {
    "window": "the condition reads a window function, which has no row-level "
              "scope a `filter (where ...)` can be evaluated in",
    "aggregate": "the condition reads an aggregate, so it is a property of a "
                 "group rather than of a row",
    "no_source": "the scope selects from no relation (a constant SELECT), so "
                 "there are no rows to have taken either arm",
    "unparseable": "the branch's condition is not a standalone expression -- a "
                   "join written `USING (...)` rather than `ON ...`, whose "
                   "merged key columns are non-null on matched and unmatched "
                   "rows alike, so the result cannot distinguish the two arms",
}

#: Why a branch can never take one of its arms, whatever the data says.
UNREACHABLE_REASON = {
    "count_in_group": "`count(*) = 0` inside a grouped scope: a group exists "
                      "only because it has at least one row, so the count is "
                      "never zero and this guard cannot fire. Defensive code "
                      "that defends nothing -- correct to leave in place, wrong "
                      "to count as coverage",
}


def _asserts_non_null(condition: exp.Expression) -> set[str]:
    """Columns this predicate guarantees are not NULL if it holds.

    ``NOT x IS NULL`` is the explicit form. ``x IN (...)``, ``x = 'detail'`` and
    any other comparison are the implicit ones: SQL's three-valued logic makes a
    comparison against NULL yield NULL, which a WHERE treats as false, so a row
    surviving the filter cannot have a NULL there either.
    """
    found: set[str] = set()
    for part in _conjuncts_of(condition):
        target = None
        if isinstance(part, exp.Not) and isinstance(part.this, exp.Is) \
                and isinstance(part.this.expression, exp.Null):
            target = part.this.this
        elif isinstance(part, (exp.In, exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            target = part.this
        if isinstance(target, exp.Column):
            found.add(target.name)
    return found


def _conjuncts_of(condition: exp.Expression) -> list[exp.Expression]:
    out, stack = [], [condition]
    while stack:
        node = stack.pop()
        if isinstance(node, exp.And):
            stack.extend([node.left, node.right])
        else:
            out.append(node)
    return out


def guaranteed_non_null(tree: exp.Expression) -> dict[str, set[str]]:
    """``{scope name: columns that scope's WHERE guarantees are not NULL}``."""
    scopes, names = scope_names(tree)
    guarantees: dict[str, set[str]] = {}
    for scope in scopes:
        clause = scope.expression.args.get("where")
        if clause is not None:
            guarantees[names[id(scope.expression)]] = _asserts_non_null(clause.this)
    return guarantees


def _sources_of(scope) -> set[str]:
    """The relation names this scope selects from, including its joins."""
    names: set[str] = set()
    source = scope.expression.args.get("from_")
    parts = ([source.this] if source is not None else [])
    parts += [join.this for join in scope.expression.args.get("joins") or ()]
    for part in parts:
        if isinstance(part, exp.Table):
            names.add(part.name)
        alias = part.args.get("alias") if isinstance(part, exp.Expression) else None
        if isinstance(alias, exp.TableAlias) and alias.name:
            names.add(alias.name)
    return names


def _unreachable_reason(predicate: exp.Expression, scope,
                        guarantees: dict[str, set[str]] | None = None) -> str:
    """Arms no data can reach, as opposed to arms no probe can express.

    The distinction matters for the same reason ``error`` is separate from
    ``unprobeable``: an unreachable arm is a fact about the model that no
    fixture will ever change, and filing it as a coverage gap would leave a
    permanent entry in the waiver ledger that nobody can ever delete.
    """
    # A coalesce whose argument the model has already filtered non-null.
    #
    # `coalesce(listing_id, '')` sitting one CTE below `where listing_id is not
    # null` has a fallback no row can take -- not for want of fixture data, but
    # because the model itself removed every row that would. Ten such guards
    # were sitting in the coverage ledger implying somebody could close them.
    #
    # Kept in the SQL and reported here rather than deleted: the guard is free,
    # and the filter it depends on is one edit away from moving. What is wrong
    # is counting it as a gap.
    if guarantees and isinstance(predicate, exp.Paren):
        inner = predicate.this
        if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null) \
                and isinstance(inner.this, exp.Column):
            column = inner.this.name
            for source in _sources_of(scope):
                if column in guarantees.get(source, set()):
                    return (
                        f"`{column}` is filtered non-null by the `{source}` scope "
                        f"this one reads from, so no row reaching here can take "
                        f"the fallback. A guard the model has already made "
                        f"redundant -- correct to keep, wrong to count as a gap"
                    )

    grouped = scope.expression.args.get("group") is not None
    if not grouped:
        return ""
    if not isinstance(predicate, exp.EQ):
        return ""
    sides = (predicate.this, predicate.expression)
    counts = [s for s in sides if isinstance(s, exp.Count) and isinstance(s.this, exp.Star)]
    zeroes = [s for s in sides if isinstance(s, exp.Literal) and s.this == "0"]
    if counts and zeroes:
        return UNREACHABLE_REASON["count_in_group"]
    return ""


@dataclass(frozen=True)
class ProbeResult:
    """One branch's measurement, or the reason there isn't one.

    ``unprobeable`` and ``error`` are deliberately separate fields and must
    never be merged. ``unprobeable`` is a property of the model's SQL -- a
    condition over a window has no row to attach to, and no amount of data will
    change that. ``error`` is the probe failing to run, which is a broken
    instrument. They were one field for about an hour, and in that hour a
    missing S3 credential turned 143 measured branches into "unprobeable" and
    the gate reported an improvement. An error must be loud; an unprobeable
    branch is a declared limit.
    """

    branch: Branch
    taken_true: int
    taken_false: int
    unprobeable: str = ""
    error: str = ""
    unreachable: str = ""

    @property
    def both_arms(self) -> bool:
        return self.taken_true > 0 and self.taken_false > 0

    @property
    def measured(self) -> bool:
        return not self.unprobeable and not self.error and not self.unreachable

    @property
    def missing_arms(self) -> list[str]:
        if not self.measured:
            return []
        missing = []
        if not self.taken_true:
            missing.append("true")
        if not self.taken_false:
            missing.append("false")
        return missing


def _unprobeable_reason(predicate: exp.Expression) -> str:
    if list(predicate.find_all(exp.Window)):
        return UNPROBEABLE_REASON["window"]
    for node in predicate.find_all(exp.AggFunc):
        # An aggregate inside its own scalar subquery is fine -- it is evaluated
        # once and compared per row, which is exactly what the incremental
        # lookback windows do.
        parent = node.parent
        while parent is not None and parent is not predicate:
            if isinstance(parent, (exp.Subquery, exp.Select)):
                break
            parent = parent.parent
        else:
            return UNPROBEABLE_REASON["aggregate"]
        if not isinstance(parent, (exp.Subquery, exp.Select)):
            return UNPROBEABLE_REASON["aggregate"]
    return ""


#: Marker returned beside a probe that yields one boolean rather than two counts.
_SCALAR = "\x00scalar"


def _is_scalar_over_scope(predicate: exp.Expression, select: exp.Expression) -> bool:
    """Is this condition one answer for the whole relation, not one per row?

    True for an aggregate that is not inside a subquery of its own and whose
    scope does not group -- ``max(fetched_at) is null`` over the target table.
    False for ``count(*) = 0`` under a ``GROUP BY``, which is per group, and
    false for the incremental lookback comparisons, whose aggregate sits inside
    a scalar subquery and is compared against a per-row column.
    """
    if select.args.get("group") is not None:
        return False
    for node in predicate.find_all(exp.AggFunc):
        parent = node.parent
        while parent is not None and parent is not predicate:
            if isinstance(parent, (exp.Subquery, exp.Select)):
                break
            parent = parent.parent
        else:
            return True
        if not isinstance(parent, (exp.Subquery, exp.Select)):
            return True
    return False


def _counted(predicate: exp.Expression, alias: str) -> exp.Expression:
    return exp.Filter(
        this=exp.func("count", exp.Star()),
        expression=exp.Where(this=predicate),
    ).as_(alias)


def probe_for(tree: exp.Expression, scope, branch: Branch) -> tuple[str | None, str]:
    """``(sql, reason)`` -- the probe for *branch*, or ``None`` and why not."""
    try:
        predicate = sqlglot.parse_one(branch.predicate, dialect=DIALECT)
    except sqlglot.errors.ParseError:
        # A join written `USING (a, b)` is the case this catches. Its condition
        # is not an expression, and after the join the merged key columns are
        # non-null on both matched and unmatched rows, so nothing in the result
        # distinguishes the two arms. Rewriting the model to an explicit ON
        # would make it probeable; that is a change to the model, not to this.
        return None, UNPROBEABLE_REASON["unparseable"]

    select = scope.expression
    source = select.args.get("from_")
    if source is None:
        return None, UNPROBEABLE_REASON["no_source"]

    if _is_scalar_over_scope(predicate, select):
        # An aggregate over the scope's whole relation is one question with one
        # answer -- "is this table empty?", for the seven incremental models'
        # cold-start guards -- so the probe selects the condition itself rather
        # than counting rows that satisfy it. Counting would be nonsense: there
        # is no row for which `max(fetched_at) is null` is true or false.
        probe = exp.Select().select(exp.alias_(predicate.copy(), "scalar_arm"))
        probe.set("from_", source.copy())
        ctes = tree.args.get("with_")
        if ctes is not None:
            probe.set("with_", ctes.copy())
        return probe.sql(dialect=DIALECT), _SCALAR

    reason = _unprobeable_reason(predicate)
    if reason:
        return None, reason

    if branch.kind == "outer_join":
        # An outer join's arms are "the right side matched" and "it did not",
        # and the second cannot be written as `not <on>`: on an unmatched row
        # every right column is NULL, so `<on>` is NULL, and `NOT NULL` is NULL
        # -- which a `filter` does not count. The false arm has to be the
        # explicit three-valued form, or every left join reads as fully covered
        # while nothing has ever missed.
        true_arm = predicate.copy()
        false_arm = exp.Not(this=exp.func("coalesce", predicate.copy(), exp.false()))
    else:
        true_arm = predicate.copy()
        false_arm = exp.not_(predicate.copy())

    probe = exp.Select().select(
        _counted(true_arm, "taken_true"),
        _counted(false_arm, "taken_false"),
    )
    probe.set("from_", source.copy())
    for join in select.args.get("joins") or ():
        probe.append("joins", join.copy())

    ctes = tree.args.get("with_")
    if ctes is not None:
        probe.set("with_", ctes.copy())

    return probe.sql(dialect=DIALECT), ""


def _unreachable_for(branch: Branch, scope, guarantees=None) -> str:
    try:
        predicate = sqlglot.parse_one(branch.predicate, dialect=DIALECT)
    except sqlglot.errors.ParseError:
        return ""
    return _unreachable_reason(predicate, scope, guarantees)


def _scalar_result(branch: Branch, sql: str, connection) -> ProbeResult:
    """One boolean, so exactly one arm is observed in this warehouse state.

    A cold-start guard reads true only while the target table is empty, which is
    true during a first build and false ever after. Both arms therefore exist
    across the build *sequence* CI runs -- a cold build in ``dbt-models`` and
    warm rebuilds in the incremental suite -- but never within one measurement.
    So the observed arm is recorded and the other is left as a gap rather than
    assumed: an arm nobody watched is not an arm that was taken.
    """
    try:
        row = connection.execute(sql).fetchone()
    except Exception as error:  # noqa: BLE001 - reported, never absorbed
        return ProbeResult(
            branch, 0, 0, "",
            f"{type(error).__name__}: {str(error).splitlines()[0][:200]}",
        )
    value = bool(row[0]) if row and row[0] is not None else False
    return ProbeResult(branch, 1 if value else 0, 0 if value else 1)


def probe_model(path: Path, connection, phase: str = "") -> list[ProbeResult]:
    """Run every branch probe for one compiled model against *connection*."""
    model = path.stem
    tree = sqlglot.parse_one(path.read_text(encoding="utf-8"), dialect=DIALECT)
    scopes, names = scope_names(tree)
    guarantees = guaranteed_non_null(tree)

    results: list[ProbeResult] = []
    for scope in scopes:
        name = names[id(scope.expression)]
        for found in branches_in_scope(scope, model, name):
            branch = Branch(found.model, found.scope, found.kind,
                            found.ordinal, found.predicate, phase)
            unreachable = _unreachable_for(branch, scope, guarantees)
            if unreachable:
                results.append(ProbeResult(branch, 0, 0, "", "", unreachable))
                continue
            sql, reason = probe_for(tree, scope, branch)
            if sql is None:
                results.append(ProbeResult(branch, 0, 0, reason))
                continue
            if reason == _SCALAR:
                results.append(_scalar_result(branch, sql, connection))
                continue
            try:
                row = connection.execute(sql).fetchone()
            except Exception as error:  # noqa: BLE001 - reported, never absorbed
                results.append(ProbeResult(
                    branch, 0, 0, "",
                    f"{type(error).__name__}: {str(error).splitlines()[0][:200]}",
                ))
                continue
            results.append(ProbeResult(branch, int(row[0] or 0), int(row[1] or 0)))
    return results


def probe_all(paths: Iterable[Path], connection, phase: str = "") -> list[ProbeResult]:
    results: list[ProbeResult] = []
    for path in paths:
        results.extend(probe_model(path, connection, phase))
    return results


def compiled_unit_tests(root: Path) -> list[tuple[str, Path]]:
    """``(model name, compiled unit test)`` for every unit test dbt compiled.

    The compiled file is named for the *test*, not the model, so the mapping
    comes from ``unit_tests.yml`` itself. It matters because a unit test's SQL
    **is the model's SQL** -- dbt replaces each ``ref()`` with a
    ``__dbt__cte__`` holding the test's ``given`` rows and leaves everything
    else verbatim -- so its branches are the model's branches and have to be
    attributed to the model to count toward it.
    """
    import yaml

    models: dict[str, str] = {}
    for declaration in (REPO_ROOT / "dbt" / "models").rglob("unit_tests.yml"):
        document = yaml.safe_load(declaration.read_text(encoding="utf-8")) or {}
        for unit_test in document.get("unit_tests") or ():
            models[unit_test["name"]] = unit_test["model"]

    found = []
    for path in sorted(root.rglob("*.sql")):
        if "unit_tests.yml" not in str(path):
            continue
        model = models.get(path.stem)
        if model:
            found.append((model, path))
    return found


#: dbt names the CTEs it substitutes for `ref()` in a unit test. Branches inside
#: one belong to the test's mocked input, not to the model under test.
_MOCK_CTE_PREFIX = "__dbt__cte__"


def probe_unit_tests(root: Path, connection, phase: str = "") -> list[ProbeResult]:
    """Probe every compiled unit test, attributing branches to its model.

    **The phase comes from the test's own compiled SQL, not from the caller.**
    dbt writes an identical compiled unit test into both compile roots, so the
    root a test was read from says nothing about which form it rendered. Tagging
    by root credited a cold-form test to an ``@incremental`` id, and where a
    scope orders its conjuncts differently between phases that is a different
    branch: a test of ``NOT fetched_at IS NULL`` was closing a Plan 123 lookback
    window.

    Predicate-text equality was tried as the discriminator and is too strict --
    a test that pins ``now_ts`` through ``overrides.macros`` compiles
    ``>= CAST('...') - INTERVAL '7' DAYS`` where the model has
    ``>= NOW() - INTERVAL '7' DAYS``. Same branch, different text, attribution
    silently lost. :func:`rendered_warm` reads the structural fact instead.
    """
    results: list[ProbeResult] = []
    for model, path in compiled_unit_tests(root):
        compiled = path.read_text(encoding="utf-8")
        rendered = "incremental" if rendered_warm(compiled, model) else "full"
        for result in probe_model(path, connection, phase):
            if result.branch.scope.startswith(_MOCK_CTE_PREFIX):
                continue
            branch = Branch(model, result.branch.scope, result.branch.kind,
                            result.branch.ordinal, result.branch.predicate,
                            rendered)
            results.append(ProbeResult(branch, result.taken_true,
                                       result.taken_false, result.unprobeable,
                                       result.error, result.unreachable))
    return results


def models_differing_by_phase(full_root: Path, incremental_root: Path) -> set[str]:
    """The models whose compiled SQL depends on whether their relation exists."""
    differing = set()
    for path in full_root.rglob("*.sql"):
        if ".schema.yml" in str(path) or "unit_tests.yml" in str(path):
            continue
        twin = incremental_root / path.relative_to(full_root)
        if not twin.exists():
            continue
        if path.read_text(encoding="utf-8") != twin.read_text(encoding="utf-8"):
            differing.add(path.stem)
    return differing


def drop_phase_where_identical(results: Sequence[ProbeResult],
                               differing: set[str]) -> list[ProbeResult]:
    """Strip the phase tag from models that compile the same either way.

    Only seven models differ between the phases. Tagging the other sixteen
    ``@full`` and ``@incremental`` would double their branch ids for no
    information -- the same SQL probed against the same data twice -- and double
    the waiver ledger with it. The tag is kept exactly where it distinguishes
    something, so an id carries a phase only when the phase is load-bearing.
    """
    out = []
    for result in results:
        branch = result.branch
        if branch.phase and branch.model not in differing:
            branch = Branch(branch.model, branch.scope, branch.kind,
                            branch.ordinal, branch.predicate, "")
        out.append(ProbeResult(branch, result.taken_true, result.taken_false,
                               result.unprobeable, result.error,
                               result.unreachable))
    return out


def covered_ids(results: Sequence[ProbeResult]) -> set[str]:
    """Branch ids this measurement saw take both arms."""
    return {r.branch.id for r in results if r.both_arms}


def rendered_warm(compiled_sql: str, model: str) -> bool:
    """Did this compiled unit test render the model's incremental form?

    dbt substitutes every mocked input with a CTE named ``__dbt__cte__<name>``,
    and the input a unit test mocks to reach the warm form is ``this`` -- the
    model itself. So a compiled unit test carrying a CTE named after its own
    model is one that overrode ``is_incremental`` and supplied a target
    relation; anything else rendered the cold form, whatever root it was read
    from.
    """
    return f"__dbt__cte__{model}" in compiled_sql


def attribute_units(unit_results: Sequence[ProbeResult],
                    model_results: Sequence[ProbeResult]) -> list[ProbeResult]:
    """Decide which compile phase each unit-test result is evidence for.

    **Two rules, because one discriminator does not cover both cases**, and each
    was tried alone first.

    *Predicate equality* credits a phase whose branch at that id carries the same
    condition. This is what handles the majority: only the ``source_rows`` scope
    of an incremental model changes between forms, so a cold-form test genuinely
    exercises every branch in the scopes that did not change, and those ids
    appear identically under both phases. Used alone it is too strict -- a test
    pinning ``now_ts`` through ``overrides.macros`` compiles
    ``>= CAST('2026-01-15...') - INTERVAL '7' DAYS`` where the model has
    ``>= NOW() - ...``, and the attribution is silently lost.

    *Rendered form* credits the phase the test's own SQL actually rendered, read
    structurally via :func:`rendered_warm`. This is what handles the macro
    override, and the ``@incremental`` lookback branches that only a warm-form
    test reaches. Used alone it is too coarse -- it withdraws credit from every
    unchanged scope, which took the gap from 0 to 25 when tried.

    A result is evidence for a phase if *either* holds. Neither rule alone was
    right, and tagging by the compile root a test was read from -- the first
    attempt -- was simply wrong: dbt writes the identical compiled test into
    both roots, so the root says nothing at all.
    """
    by_id: dict[str, str] = {}
    for result in model_results:
        by_id[result.branch.id] = result.branch.predicate

    out: list[ProbeResult] = []
    for result in unit_results:
        branch = result.branch
        rendered = branch.phase
        for phase in ("", "full", "incremental"):
            candidate = Branch(branch.model, branch.scope, branch.kind,
                               branch.ordinal, branch.predicate, phase)
            if candidate.id not in by_id:
                continue
            # An unphased id only exists for a model that compiles identically
            # either way, so whichever form the test rendered is that model's
            # only form and the rendering cannot disagree with it. Without this
            # a test pinning a macro lost credit on every non-incremental model:
            # its predicate text differs from the model's, and `rendered` is
            # always "full" or "incremental", so neither rule below could fire
            # on a branch carrying no phase at all.
            unphased = phase == ""
            same_condition = by_id[candidate.id] == branch.predicate
            if unphased or same_condition or phase == rendered:
                out.append(ProbeResult(candidate, result.taken_true,
                                       result.taken_false, result.unprobeable,
                                       result.error, result.unreachable))
    return out


def merge(*sources: Sequence[ProbeResult]) -> list[ProbeResult]:
    """One result per branch id, taking the best evidence any source found.

    A branch is covered if *anything* took each arm -- the fixture, a selector's
    production rows, or a unit test's ``given``. That is the whole point of
    reconciling three lists rather than three separate verdicts: the question is
    whether the branch is exercised, not which list exercised it.

    An arm counts as taken if any source saw it, so the counts are maxima. An
    error anywhere is kept, because a source that failed to run is a broken
    instrument wherever it broke. A branch is unprobeable only if no source
    could probe it.
    """
    best: dict[str, ProbeResult] = {}
    for source in sources:
        for result in source:
            key = result.branch.id
            previous = best.get(key)
            if previous is None:
                best[key] = result
                continue
            best[key] = ProbeResult(
                previous.branch,
                max(previous.taken_true, result.taken_true),
                max(previous.taken_false, result.taken_false),
                previous.unprobeable if (previous.unprobeable and result.unprobeable) else "",
                previous.error or result.error,
                previous.unreachable or result.unreachable,
            )
    return [best[key] for key in sorted(best)]


def summarize(results: Sequence[ProbeResult]) -> dict[str, int]:
    return {
        "branches": len(results),
        "both_arms": sum(1 for r in results if r.both_arms),
        "one_arm": sum(1 for r in results if r.measured and not r.both_arms),
        "unprobeable": sum(1 for r in results if r.unprobeable),
        "unreachable": sum(1 for r in results if r.unreachable),
        "errored": sum(1 for r in results if r.error),
    }
