"""Every dbt model branch is exercised in both directions, or it is declared.

Plan 162 Stage S, exit 2. The branch list comes from
:mod:`tests.dbt.branch_list`, which parses it out of dbt's compiled SQL rather
than reading a list somebody maintains. This module runs it: for each branch, a
probe counts the rows that took each arm against the warehouse a real
``dbt build`` just produced.

**Seeded full, and the number is the point.** 73 of the 308 probeable branch
points had one arm no row ever took when this gate landed. Every entry deleted
from :data:`BRANCH_COVERAGE_WAIVERS` is one branch whose second arm some fixture
scenario, selector or unit test now reaches.

**The seed is the union of all three lists, not the fixture alone.** Measured
against the fixture by itself the gap was 142 of 212 -- and 42 of those were
already covered by a dbt unit test, whose compiled SQL is the model's own SQL
with its ``ref()``s replaced by the test's ``given`` rows. Publishing the 142
would have been an overstatement of exactly the kind this plan keeps catching in
other people's instruments, so the gate merges the fixture build, the compiled
unit tests and both compile phases before calling anything uncovered.

**A new branch is not waived, so it fails.** That is exit 1's demonstration
requirement discharged by construction rather than by assertion: a model that
gains a branch nothing exercises turns this red on the commit that adds it,
because a waiver naming it does not exist and cannot be written by accident.

**Twelve branches are not probeable and say why.** Joins in
``int_listing_volatility_features`` written ``USING (...)`` have merged key
columns that are non-null on matched and unmatched rows alike, so nothing in the
result distinguishes the arms; ``nullif`` guards over aggregates are properties
of a group rather than of a row. They are listed separately from the coverage
gaps because they are a limit of the instrument rather than a hole in the
fixture, and conflating the two would let a real gap hide among them -- which is
not hypothetical. An earlier version of this module filed probe *execution
errors* in the same field, and one missing S3 credential read as 143 branches
becoming unmeasurable rather than as a broken connection.
"""
import os
from functools import lru_cache

import pytest

from tests.dbt.branch_list import compiled_model_paths
from tests.dbt.branch_probe import (
    attribute_units,
    covered_ids,
    drop_phase_where_identical,
    merge,
    models_differing_by_phase,
    probe_all,
    probe_unit_tests,
)

from .real_build import analytics_con, compiled_in_both_phases

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("DUCKDB_PATH"),
        reason="DUCKDB_PATH not set -- no real dbt build to probe",
    ),
]


#: Branches no dbt unit test takes both arms of. **Empty, and that is exit 2
#: met**: seeded at 160 on 2026-09-07 and drained the same day. An empty ledger
#: is kept rather than deleted because the rule it guards is a ratchet -- a
#: branch added tomorrow with no unit test lands here as a failure, not as a
#: quiet tuple append.
#:
#: Getting to zero needed `dbt run --empty` in CI. A unit test on an incremental
#: model overrides `is_incremental` true and mocks `- input: this`, and dbt reads
#: the model's own relation to type those rows -- but runs the unit tests before
#: materializing it, so on a clean warehouse the test blocks the relation it
#: needs. That reads as a deadlock and is not one; dbt's documentation
#: prescribes the pre-step, and 19 branch points are reachable no other way.
UNIT_TEST_WAIVERS: frozenset[str] = frozenset({})


#: Branches with one arm nothing has ever taken. Ratchets down, never up.
BRANCH_COVERAGE_WAIVERS: frozenset[str] = frozenset({})

#: Branches no row-level probe can express. Each is a property of the model's
#: SQL rather than of the data, so this set moves only when a model is
#: rewritten.
UNPROBEABLE_BRANCHES: frozenset[str] = frozenset({})

#: Arms no data can ever reach. **Empty, and it emptied by deletion rather
#: than by declaration.** Every entry it ever held turned out to be code that
#: could be removed: `nullif(count(*), 0)` under a GROUP BY, where a group
#: exists because it has a row; `coalesce(x, '')` on a column the same model
#: had already filtered non-null, whose real enforcement was the `not_null`
#: constraint that fails the build by name; and a LEFT JOIN whose key domain
#: was identical to one an inner join upstream had already required.
#:
#: So "unreachable" was never a category of branch. It was a symptom, and it
#: read as one of three defects every time: dead code, a wrong predicate, or a
#: guard whose enforcement lived somewhere else. The detector below still runs,
#: because a new one should be found the same way -- but what it reports is a
#: deletion to make, not a fact to file.
UNREACHABLE_BRANCHES: frozenset[str] = frozenset({})


@lru_cache(maxsize=1)
def _measure():
    """Every branch, measured against all three lists in both compile phases.

    **All three, because the question is whether the branch is exercised**, not
    which list exercised it. The fixture build supplies the synthetic scenarios
    and, in the snapshot job, production rows; the compiled unit tests supply
    the mocked inputs. Measuring only the fixture reported a gap of 142 where
    the true figure was 100.

    **Both phases, because a model's compiled SQL is not a function of its
    source alone.** Seven models wrap SQL in ``{% if is_incremental() %}``, and
    a compile against an empty warehouse omits all of it -- 39 lines, every one
    a Plan 123 late-arrival lookback window. ``dbt compile --full-refresh``
    renders the cold form even where the relation exists, so both are captured
    from one build rather than by dropping tables in between.

    Probed once for the module: the staging models are views over Parquet in
    MinIO, so each probe is an object-store read, and the tests below ask
    different questions of one measurement rather than repeating it.
    """
    full_root, incremental_root = compiled_in_both_phases()
    differing = models_differing_by_phase(full_root, incremental_root)
    connection = analytics_con()
    try:
        models = [
            probe_all(compiled_model_paths(full_root), connection, "full"),
            probe_all(compiled_model_paths(incremental_root), connection, "incremental"),
        ]
        # Probed once. The compiled unit test is byte-identical in both roots,
        # so which root it came from says nothing about which form it renders --
        # attribute_by_predicate decides that from the SQL itself.
        raw_units = probe_unit_tests(full_root, connection)
    finally:
        connection.close()

    models = [drop_phase_where_identical(source, differing) for source in models]
    units = drop_phase_where_identical(
        attribute_units(raw_units, [r for source in models for r in source]),
        differing,
    )
    return tuple(merge(*models, units)), tuple(merge(units))


def test_no_probe_failed_to_execute():
    """A probe that errors is a broken instrument, and must never be excused.

    This test exists because the alternative was tried. When an execution
    failure and a structurally unprobeable branch shared one field, a missing S3
    credential turned 143 measured branches into "unprobeable" -- and the gate
    read that as the coverage gap shrinking. A number that improves because the
    measurement broke is the exact failure this plan is about, so the error path
    gets its own test and its own loud message.
    """
    broken = sorted(
        f"{result.branch.id}: {result.error}"
        for result in _measure()[0]
        if result.error
    )
    assert not broken, (
        f"{len(broken)} branch probes would not execute. This is the instrument "
        f"failing, not the models -- a connection that cannot reach MinIO reads "
        f"every stg_* view as an error, and counting those as unmeasurable "
        f"would report the coverage gap shrinking:\n  " + "\n  ".join(broken[:20])
    )


def test_every_branch_is_exercised_by_a_dbt_unit_test():
    """Exit 2. **A unit test, specifically -- not the union of everything.**

    The gate below asks whether anything at all reaches a branch. This asks
    whether a *unit test* does, and they are different questions with different
    answers: measured 2026-09-07, unit tests reached 148 of 308 measurable
    branches and the union reached 200. A branch the fixture covers and no unit
    test covers passes the gate below and fails here, which is the whole point.

    **Why unit tests carry exhaustiveness.** They are the only list that can
    construct a state production has never produced -- a selector finds rows and
    cannot find the absence of them. And keying the obligation here frees the
    fixture: its data stops being pinned in place by a duty to reach particular
    branches, and answers the one question nothing else can, which is whether a
    build over it populates every model.

    Duplication with the other lists is deliberate. A branch a unit test also
    covers is not wasted work; it is the branch no longer depending on fixture
    data staying exactly as it is.
    """
    _, unit_tests = _measure()
    reached = covered_ids(unit_tests)
    missing = sorted(
        f"{result.branch.id} -- `{result.branch.predicate}`"
        for result in _measure()[0]
        if result.measured
        and result.branch.id not in reached
        and result.branch.id not in UNIT_TEST_WAIVERS
    )
    assert not missing, (
        "these dbt model branches have no unit test taking both arms. Add one to "
        "the model's unit_tests.yml with `given` rows that reach each side -- a "
        "unit test can construct inputs production has never produced, which is "
        "why this obligation sits here and not on the fixture:\n  "
        + "\n  ".join(missing)
    )


def test_every_branch_is_exercised_in_both_directions_or_declared():
    """The gate. A branch missing an arm and missing a waiver fails here."""
    undeclared = sorted(
        f"{result.branch.id} -- never took the "
        f"{' or '.join(result.missing_arms)} arm; `{result.branch.predicate}`"
        for result in _measure()[0]
        if result.measured
        and not result.both_arms
        and result.branch.id not in BRANCH_COVERAGE_WAIVERS
    )
    assert not undeclared, (
        "these dbt model branches have an arm no row has ever taken, and no "
        "waiver declares it. Give the fixture a row that takes the missing arm "
        "(scripts/seed_lake_snapshot_fixture.py), or a selector that finds one "
        "in production (archiver/config/lake_snapshot_selectors.yml), or a dbt "
        "unit test whose `given` reaches it. Adding a waiver is the last resort "
        "and owes a reason a reader can check:\n  " + "\n  ".join(undeclared)
    )


def test_no_branch_waiver_outlives_the_branch_it_names():
    """A waiver for a branch that no longer exists is a lie the ratchet tells.

    Positional ids move when a model is edited -- the accepted cost of keying on
    position rather than on predicate text -- so a stale waiver is not merely
    untidy. It is a live exemption pointing at nothing, while the branch it used
    to name is now unwaived under a different id, which the gate above catches
    from the other side.
    """
    live = {result.branch.id for result in _measure()[0]}
    declared = BRANCH_COVERAGE_WAIVERS | UNPROBEABLE_BRANCHES | UNIT_TEST_WAIVERS
    stale = sorted(declared - live)
    assert not stale, (
        "these waived branch ids are no longer in the branch list. Either the "
        "model was edited and the id moved -- re-run the gate and re-seed from "
        "what it reports -- or the branch is gone and its waiver goes with "
        "it:\n  " + "\n  ".join(stale)
    )


def test_no_coverage_waiver_names_a_branch_that_is_already_covered():
    """The other direction, which the two waiver ledgers were missing.

    ``UNPROBEABLE_BRANCHES`` and ``UNREACHABLE_BRANCHES`` are asserted as exact
    sets, so neither can take an entry that is not true -- the gate recomputes
    what belongs in them and compares. The two *waiver* ledgers had only the
    staleness half: they checked that a waived id still names a live branch,
    not that the branch still needs waiving. A waiver for a branch that is
    fully covered therefore passed, which is the shape of the failure this
    whole stage exists to catch -- an exemption that reads as a fact and is
    checked in one direction only.

    Verified 2026-09-07 by adding
    ``mart_block_rate.__main__.case_when.0`` -- a covered branch -- to
    ``UNIT_TEST_WAIVERS``: the gate passed 6 of 6 before this test existed.

    With both ledgers empty this is trivially satisfied today, and that is the
    point of adding it now. The cost of an unnecessary waiver is paid on the
    day someone drains the branch it names and leaves the entry behind, and by
    then nothing remembers it was unnecessary.
    """
    results, unit_tests = _measure()
    reached = covered_ids(unit_tests)

    unnecessary = sorted(
        f"UNIT_TEST_WAIVERS: {result.branch.id} -- a unit test takes both arms"
        for result in results
        if result.branch.id in UNIT_TEST_WAIVERS and result.branch.id in reached
    ) + sorted(
        f"BRANCH_COVERAGE_WAIVERS: {result.branch.id} -- both arms are taken"
        for result in results
        if result.branch.id in BRANCH_COVERAGE_WAIVERS and result.both_arms
    )
    assert not unnecessary, (
        "these waivers exempt branches that are already covered. Delete them: "
        "a waiver that describes no violation is an exemption nobody is "
        "watching, and it will still be here on the day the branch it names "
        "regresses:\n  " + "\n  ".join(unnecessary)
    )


def test_the_unprobeable_set_is_exactly_what_the_prober_cannot_express():
    """Both directions, so a newly unprobeable branch cannot arrive unnoticed.

    A model rewritten from ``ON`` to ``USING``, or a guard moved over an
    aggregate, silently removes a branch from measurement -- and a branch that
    stops being measured reads as the gap count improving.
    """
    actual = {result.branch.id for result in _measure()[0] if result.unprobeable}
    assert actual == UNPROBEABLE_BRANCHES, (
        f"the unprobeable set moved. Newly unprobeable: "
        f"{sorted(actual - UNPROBEABLE_BRANCHES)}; no longer unprobeable: "
        f"{sorted(UNPROBEABLE_BRANCHES - actual)}. A branch becoming "
        f"unprobeable is a loss of measurement that reads as an improvement, "
        f"which is why it is declared rather than absorbed."
    )


def test_the_unreachable_set_is_exactly_what_no_data_can_reach():
    """Both directions, because this set shrinking is also an improvement-shaped lie.

    A guard that becomes reachable -- someone removes the GROUP BY, or the
    aggregate changes -- stops being unreachable and starts owing coverage. A
    guard that becomes unreachable has turned into dead code. Neither should
    pass silently, and the difference between them is not something a count can
    tell you.
    """
    actual = {result.branch.id for result in _measure()[0] if result.unreachable}
    assert actual == UNREACHABLE_BRANCHES, (
        f"the unreachable set moved. Newly unreachable: "
        f"{sorted(actual - UNREACHABLE_BRANCHES)} -- these are now dead guards; "
        f"no longer unreachable: {sorted(UNREACHABLE_BRANCHES - actual)} -- these "
        f"now owe coverage like any other branch."
    )
