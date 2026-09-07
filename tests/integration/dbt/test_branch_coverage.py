"""Every dbt model branch is exercised in both directions, or it is declared.

Plan 162 Stage S, exit 2. The branch list comes from
:mod:`tests.dbt.branch_list`, which parses it out of dbt's compiled SQL rather
than reading a list somebody maintains. This module runs it: for each branch, a
probe counts the rows that took each arm against the warehouse a real
``dbt build`` just produced.

**Seeded full, and the number is the point.** 88 of the 308 probeable branch
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


#: Branches no dbt unit test takes both arms of. Seeded at 160 on 2026-09-07,
#: ratchets down. This is the ledger exit 2 drains, and it is deliberately
#: larger than BRANCH_COVERAGE_WAIVERS below: 52 of these branches *are*
#: reached, by the fixture build, and still owe a unit test. The duplication
#: is the point -- it is what lets the fixture's data stop being pinned in
#: place by an obligation to reach particular branches.
UNIT_TEST_WAIVERS: frozenset[str] = frozenset({
    "int_benchmarks.<final>.case_arm.0",
    "int_benchmarks.<final>.where_conjunct.0",
    "int_benchmarks.<final>.where_conjunct.1",
    "int_benchmarks.<final>.where_conjunct.2",
    "int_latest_observation.<final>/derived0.case_else.0@full",
    "int_latest_observation.<final>/derived0.case_else.0@incremental",
    "int_latest_observation.affected_vins.where_conjunct.1@incremental",
    "int_latest_observation.affected_vins/subquery0.coalesce_fallback.0@incremental",
    "int_latest_observation.candidates.where_conjunct.0@full",
    "int_latest_observation.candidates.where_conjunct.0@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.0@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.0@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.11@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.11@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.12@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.12@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.13@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.13@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.14@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.14@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.15@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.15@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.16@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.16@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.17@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.17@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.18@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.18@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.19@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.19@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.1@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.1@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.20@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.20@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.21@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.21@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.22@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.22@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.23@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.23@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.24@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.24@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.25@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.25@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.26@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.26@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.27@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.27@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.28@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.28@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.29@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.29@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.3@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.3@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.6@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.6@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.7@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.7@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.8@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.8@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.9@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.9@incremental",
    "int_listing_observation_fingerprints.source_rows.where_conjunct.1@full",
    "int_listing_observation_fingerprints.source_rows.where_conjunct.1@incremental",
    "int_listing_observation_fingerprints.source_rows.where_conjunct.2@incremental",
    "int_listing_observation_fingerprints.source_rows/subquery0.coalesce_fallback.0@incremental",
    "int_listing_observation_runs.affected_listings.where_conjunct.0@incremental",
    "int_listing_observation_runs.affected_listings/subquery0.coalesce_fallback.0@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.0@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.0@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.1@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.1@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.2@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.2@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.3@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.3@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.4@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.4@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.5@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.5@incremental",
    "int_listing_state_fingerprints.<final>.where_conjunct.0@full",
    "int_listing_state_fingerprints.<final>.where_conjunct.0@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.0@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.0@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.12@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.12@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.1@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.1@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.2@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.2@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.3@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.3@incremental",
    "int_listing_state_fingerprints.source_rows.where_conjunct.2@incremental",
    "int_listing_state_fingerprints.source_rows/subquery0.coalesce_fallback.0@incremental",
    "int_listing_state_runs.affected_vins.where_conjunct.0@incremental",
    "int_listing_state_runs.affected_vins/subquery0.coalesce_fallback.0@incremental",
    "int_listing_volatility_features.<final>.case_arm.0",
    "int_listing_volatility_features.<final>.case_else.0",
    "int_listing_volatility_features.<final>.coalesce_fallback.0",
    "int_listing_volatility_features.<final>.coalesce_fallback.1",
    "int_listing_volatility_features.<final>.coalesce_fallback.2",
    "int_listing_volatility_features.<final>.outer_join.0",
    "int_listing_volatility_features.<final>.outer_join.1",
    "int_listing_volatility_features.<final>.outer_join.2",
    "int_listing_volatility_features.<final>.outer_join.3",
    "int_listing_volatility_features.listing_state_change_counts.agg_filter.0",
    "int_listing_volatility_features.open_observation_runs.where_conjunct.0",
    "int_listing_volatility_features.vin_listing_meta.where_conjunct.1",
    "int_price_history.affected_vins.where_conjunct.0@incremental",
    "int_price_history.affected_vins/subquery0.coalesce_fallback.0@incremental",
    "mart_block_rate.<final>.where_conjunct.0",
    "mart_cooldown_cohorts.bucketed.case_arm.1",
    "mart_cooldown_cohorts.bucketed.case_arm.5",
    "mart_cooldown_event_funnel.bucketed.case_arm.3",
    "mart_cooldown_event_funnel.bucketed.case_arm.7",
    "mart_cooldown_event_funnel.bucketed.where_conjunct.1",
    "mart_cooldown_event_funnel.bucketed.where_conjunct.2",
    "mart_deal_scores.<final>.case_arm.2",
    "mart_deal_scores.<final>.case_else.0",
    "mart_deal_scores.dealer_inventory.where_conjunct.0",
    "mart_deal_scores.price_percentiles.where_conjunct.0",
    "mart_deal_scores.price_percentiles.where_conjunct.1",
    "mart_deal_scores.scored.case_arm.0",
    "mart_deal_scores.scored.case_arm.1",
    "mart_deal_scores.scored.case_arm.2",
    "mart_deal_scores.scored.coalesce_fallback.0",
    "mart_deal_scores.scored.coalesce_fallback.1",
    "mart_deal_scores.scored.coalesce_fallback.2",
    "mart_deal_scores.scored.coalesce_fallback.3",
    "mart_deal_scores.scored.coalesce_fallback.4",
    "mart_deal_scores.scored.coalesce_fallback.5",
    "mart_deal_scores.scored.coalesce_fallback.6",
    "mart_deal_scores.scored.coalesce_fallback.7",
    "mart_deal_scores.scored.greatest_least.0",
    "mart_deal_scores.scored.greatest_least.1",
    "mart_deal_scores.scored.greatest_least.4",
    "mart_deal_scores.scored.greatest_least.5",
    "mart_deal_scores.scored.nullif.0",
    "mart_deal_scores.scored.outer_join.1",
    "mart_deal_scores.scored.outer_join.2",
    "mart_deal_scores.scored.outer_join.3",
    "mart_deal_scores.scored.where_conjunct.0",
    "mart_deal_scores.scored.where_conjunct.1",
    "mart_detail_batch_outcomes.<final>.where_conjunct.0",
    "mart_inventory_coverage.<final>.where_conjunct.0",
    "mart_price_freshness_trend.<final>.agg_filter.1",
    "mart_price_freshness_trend.<final>.agg_filter.2",
    "mart_price_freshness_trend.<final>.agg_filter.3",
    "mart_scrape_volume.source_rows.where_conjunct.0@full",
    "mart_scrape_volume.source_rows.where_conjunct.0@incremental",
    "mart_scrape_volume.source_rows.where_conjunct.1@incremental",
    "mart_scrape_volume.source_rows/subquery0.coalesce_fallback.0@incremental",
    "mart_vehicle_snapshot.<final>.outer_join.1",
    "mart_vehicle_snapshot.latest_state.where_conjunct.0",
    "stg_observations.<final>.case_arm.0",
    "stg_observations.<final>.case_else.0",
    "stg_price_events.<final>.where_conjunct.0",
    "stg_price_events.<final>.where_conjunct.1",
    "stg_price_events.<final>.where_conjunct.2",
    "stg_price_events.<final>.where_conjunct.3",
})


#: Branches with one arm nothing has ever taken. Ratchets down, never up.
BRANCH_COVERAGE_WAIVERS: frozenset[str] = frozenset({
    "int_benchmarks.<final>.case_arm.0",
    "int_benchmarks.<final>.where_conjunct.0",
    "int_benchmarks.<final>.where_conjunct.1",
    "int_benchmarks.<final>.where_conjunct.2",
    "int_latest_observation.<final>/derived0.case_else.0@incremental",
    "int_latest_observation.affected_vins/subquery0.coalesce_fallback.0@incremental",
    "int_latest_observation.candidates.where_conjunct.0@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.0@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.0@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.11@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.11@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.1@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.1@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.21@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.21@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.22@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.22@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.23@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.23@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.24@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.24@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.25@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.25@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.26@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.26@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.27@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.27@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.28@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.28@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.29@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.29@incremental",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.3@full",
    "int_listing_observation_fingerprints.fingerprinted.coalesce_fallback.3@incremental",
    "int_listing_observation_fingerprints.source_rows.where_conjunct.1@full",
    "int_listing_observation_fingerprints.source_rows.where_conjunct.1@incremental",
    "int_listing_observation_fingerprints.source_rows.where_conjunct.2@incremental",
    "int_listing_observation_fingerprints.source_rows/subquery0.coalesce_fallback.0@incremental",
    "int_listing_observation_runs.affected_listings/subquery0.coalesce_fallback.0@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.0@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.1@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.1@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.2@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.2@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.3@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.4@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.4@incremental",
    "int_listing_observation_runs.ordered.coalesce_fallback.5@full",
    "int_listing_observation_runs.ordered.coalesce_fallback.5@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.0@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.0@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.12@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.12@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.1@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.1@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.2@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.2@incremental",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.3@full",
    "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.3@incremental",
    "int_listing_state_fingerprints.source_rows/subquery0.coalesce_fallback.0@incremental",
    "int_listing_state_runs.affected_vins/subquery0.coalesce_fallback.0@incremental",
    "int_listing_volatility_features.<final>.case_arm.0",
    "int_listing_volatility_features.<final>.coalesce_fallback.0",
    "int_listing_volatility_features.<final>.outer_join.0",
    "int_price_history.affected_vins/subquery0.coalesce_fallback.0@incremental",
    "mart_block_rate.<final>.where_conjunct.0",
    "mart_cooldown_event_funnel.bucketed.where_conjunct.1",
    "mart_cooldown_event_funnel.bucketed.where_conjunct.2",
    "mart_deal_scores.price_percentiles.where_conjunct.0",
    "mart_deal_scores.scored.case_arm.0",
    "mart_deal_scores.scored.case_arm.1",
    "mart_deal_scores.scored.case_arm.2",
    "mart_deal_scores.scored.coalesce_fallback.3",
    "mart_deal_scores.scored.greatest_least.5",
    "mart_deal_scores.scored.nullif.0",
    "mart_deal_scores.scored.where_conjunct.0",
    "mart_detail_batch_outcomes.<final>.where_conjunct.0",
    "mart_inventory_coverage.<final>.where_conjunct.0",
    "mart_price_freshness_trend.<final>.agg_filter.1",
    "mart_price_freshness_trend.<final>.agg_filter.2",
    "mart_price_freshness_trend.<final>.agg_filter.3",
    "mart_scrape_volume.source_rows.where_conjunct.0@full",
    "mart_scrape_volume.source_rows.where_conjunct.1@incremental",
    "mart_scrape_volume.source_rows/subquery0.coalesce_fallback.0@incremental",
    "mart_vehicle_snapshot.<final>.outer_join.1",
    "stg_price_events.<final>.where_conjunct.0",
    "stg_price_events.<final>.where_conjunct.1",
    "stg_price_events.<final>.where_conjunct.2",
    "stg_price_events.<final>.where_conjunct.3",
})

#: Branches no row-level probe can express. Each is a property of the model's
#: SQL rather than of the data, so this set moves only when a model is
#: rewritten.
UNPROBEABLE_BRANCHES: frozenset[str] = frozenset({})

#: Arms no data can ever reach, as distinct from arms no probe can
#: express. Both `nullif(count(*), 0)` guards sit under a GROUP BY, and a
#: group exists only because it has a row -- so the count is never zero and
#: the guard cannot fire. Defensive code that defends nothing: correct to
#: leave in the SQL, wrong to count as coverage, and wrong to file as a gap
#: somebody could close.
UNREACHABLE_BRANCHES: frozenset[str] = frozenset({
    "mart_detail_batch_outcomes.<final>.nullif.0",
    "mart_scrape_volume.<final>.nullif.0@full",
    "mart_scrape_volume.<final>.nullif.0@incremental",
})


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
        sources = [
            probe_all(compiled_model_paths(full_root), connection, "full"),
            probe_all(compiled_model_paths(incremental_root), connection, "incremental"),
            probe_unit_tests(full_root, connection, "full"),
            probe_unit_tests(incremental_root, connection, "incremental"),
        ]
    finally:
        connection.close()
    tidy = [drop_phase_where_identical(source, differing) for source in sources]
    # sources[2:] are the unit-test probes; kept separately because exit 2 is a
    # claim about unit tests specifically, not about the union.
    return tuple(merge(*tidy)), tuple(merge(*tidy[2:]))


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
    stale = sorted((BRANCH_COVERAGE_WAIVERS | UNPROBEABLE_BRANCHES) - live)
    assert not stale, (
        "these waived branch ids are no longer in the branch list. Either the "
        "model was edited and the id moved -- re-run the gate and re-seed from "
        "what it reports -- or the branch is gone and its waiver goes with "
        "it:\n  " + "\n  ".join(stale)
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
