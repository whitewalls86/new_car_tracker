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


def _unreachable_reason(predicate: exp.Expression, scope) -> str:
    """Arms no data can reach, as opposed to arms no probe can express.

    The distinction matters for the same reason ``error`` is separate from
    ``unprobeable``: an unreachable arm is a fact about the model that no
    fixture will ever change, and filing it as a coverage gap would leave a
    permanent entry in the waiver ledger that nobody can ever delete.
    """
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


def _unreachable_for(branch: Branch, scope) -> str:
    try:
        predicate = sqlglot.parse_one(branch.predicate, dialect=DIALECT)
    except sqlglot.errors.ParseError:
        return ""
    return _unreachable_reason(predicate, scope)


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

    results: list[ProbeResult] = []
    for scope in scopes:
        name = names[id(scope.expression)]
        for found in branches_in_scope(scope, model, name):
            branch = Branch(found.model, found.scope, found.kind,
                            found.ordinal, found.predicate, phase)
            unreachable = _unreachable_for(branch, scope)
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
    """Probe every compiled unit test, attributing branches to its model."""
    results: list[ProbeResult] = []
    for model, path in compiled_unit_tests(root):
        for result in probe_model(path, connection, phase):
            if result.branch.scope.startswith(_MOCK_CTE_PREFIX):
                continue
            branch = Branch(model, result.branch.scope, result.branch.kind,
                            result.branch.ordinal, result.branch.predicate, phase)
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
