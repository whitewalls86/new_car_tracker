"""Removing a guard must break the constraint that guard produces.

Plan 162 Stage S, exit 4. 161 column constraints are declared across the 23
models and nothing demonstrated that any of them was load-bearing. A `not_null`
is a claim about a branch guard, so **the mutation set is derived from the
branch list rather than written by hand**: for each branch
:mod:`tests.dbt.branch_list` finds, delete it from the model, rematerialize that
model alone, and run that model's own compiled data tests against the mutant. A
constraint that goes red is held up by that branch. A constraint no mutation can
break is decorative.

**Two classes, not three.** Mutation happens only inside the model that declares
the constraint. `not_null_mart_vehicle_snapshot_vin` cannot be broken from
inside `mart_vehicle_snapshot` -- the guard is `where vin17 is not null` in
`int_latest_observation`, and the mart takes that model as its driving table --
and calling the mart's constraint decorative is *correct* rather than a false
verdict: it restates an invariant established upstream, where
`not_null_int_latest_observation_vin17` sits and **is** locally load-bearing.
Cross-model mutation would re-confirm the same guard down a much more expensive
path. What is kept from that idea is a reason string: a decorative verdict
carries the upstream branch that actually holds the invariant, so
:data:`DECORATIVE` is a map of where each invariant is enforced rather than a
list of shrugs -- and the case where *no* model declares it becomes visible,
which is the more useful finding and comes free.

**The failure mode this is built against.** A mutation operator that produces
invalid SQL shows up as *every* constraint dying, which reads as total coverage.
Three separate tests refuse that reading:
:func:`test_the_unmutated_baseline_is_green` establishes that the harness can
compute each model and that its constraints pass before anything is mutated;
:func:`test_no_mutant_failed_to_execute` treats a mutant that will not run as an
error rather than as a kill; and :func:`test_every_branch_yields_a_mutant`
refuses the quieter version, where an operator declines a branch and the branch
silently leaves the denominator. All three were earning their keep on the first
run -- see :mod:`tests.dbt.branch_mutation` for the four defects they caught.

**Zero-row models are a third class, and keeping them out of the other two is
not bookkeeping.** A `not_null` over an empty relation is trivially true, so a
constraint on a model that materializes nothing passes and no mutation can break
it -- arithmetically identical to decorative, and the opposite thing. Folding
them together would hide a live defect inside a measurement, which is this
plan's own recurring failure.

The distinction earned itself inside a single afternoon. Measured 05:14 UTC on
2026-09-07, four models built empty -- `int_benchmarks`, `mart_deal_scores`,
`mart_price_freshness_trend`, `mart_vehicle_snapshot` -- putting **26 of the 161
constraints in this class**, and the gate as it stands would have recorded them
as decorative had it not asked the question separately. Re-measured at 05:25
against the same warehouse, all 23 models had rows and the class was empty:
Plan 162's own fix for the empty world had landed rows in `ops.tracked_models`
between the two runs, and `int_price_history` went from 7 rows to 29. That is
also the caveat on :data:`VACUOUS_MODELS` being seeded empty -- it is seeded
from the second reading, and a fixture that does not itself seed
`ops.tracked_models` will fail this test with the four models named, which is
the gate working rather than the gate being stale.

**The ledger tracks the data as well as the code, and both ratchets point the
same way because of it.** A mutation kills a constraint only if some row would
violate it once the guard is gone, so a fixture that gains rows turns decorative
constraints into load-bearing ones -- measured on 2026-09-07, `stg_observations`
going from 77 rows to 93 moved
`unique_int_listing_observation_fingerprints_observation_id` and
`unique_int_listing_state_fingerprints_artifact_id` across, with no change to
any model. That is the instrument working: the fixture had never contained a row
that would collide, so until then nothing had shown those guards did anything.
:data:`LOAD_BEARING` therefore only grows and :data:`DECORATIVE` only shrinks,
and a re-seed after a fixture change is expected work rather than a symptom.

**Mutation runs against the cold compile, and only the cold one.** Seven models
render differently under `is_incremental()`, and exit 2's branch list covers
both phases for that reason. This gate cannot: rematerializing a model computes
it whole, while the incremental form computes a delta against `{{ this }}` and
would leave a partial relation whose data tests mean nothing. So the mutation
denominator is the 216 branches of the cold form rather than the 308 measurable
branch points exit 2 reports, and the incremental-only guards are outside what
this instrument can say anything about.

**Where the mutants live.** :func:`~tests.integration.dbt.real_build.attached_warehouse`
attaches the built warehouse `READ_ONLY` to an in-memory database and mutants
are materialized in the latter, so a crashed run leaves no stray relation for
the other suites to trip over and no assertion in this directory can be made to
describe something other than the build. The precondition -- no dbt invocation
in this process -- is stated by
:func:`~tests.integration.dbt.real_build.assert_no_in_process_dbt` rather than
left to a DuckDB lock error, which is also why the cold compile below is a
subprocess.

**So this module needs a pytest invocation of its own, and today it does not get
one.** Every other suite in this directory calls
:func:`~tests.integration.dbt.real_build.run_dbt`, which imports dbt into the
interpreter for good; `test_branch_coverage.py` -- exit 2's gate, doing the same
job from the other end -- sorts before this file, so a bare
`pytest tests/integration/dbt` loads dbt first and every test here fails on the
precondition. That is the guard working, not a bug in it: the alternative
readings are a DuckDB lock error six frames down, or a skip, and a skip is a
gate that disappears without saying so. The fix is a CI step of its own, the
pattern Stage E already established when it split the dbt job. Run it as
`pytest tests/integration/dbt/test_constraint_mutation.py`.
"""
import json
import os
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import pytest
import sqlglot

from tests.dbt.branch_list import DIALECT, branches_for_model
from tests.dbt.branch_mutation import (
    ConstraintVerdict,
    compiled_tests_for,
    mutate,
    retarget,
    source_columns_for,
    upstream_guard_for,
)
from tests.sql_loader import queries

from .real_build import DBT_DIR, assert_no_in_process_dbt, attached_warehouse, dbt_is_installed

SQL = queries(__file__)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("DUCKDB_PATH"),
        reason="DUCKDB_PATH not set -- no real dbt build to mutate against",
    ),
    pytest.mark.skipif(not dbt_is_installed(), reason="dbt is not installed"),
]


#: Constraints a mutation inside their own model breaks. **Ratchets up**:
#: measured 15 of 161 on 2026-09-07, and a constraint listed here that stops
#: dying fails the gate. This is the half of the ledger that is evidence rather
#: than debt -- every name here is a guard whose removal was shown to turn a
#: declared test red, and the comment beside it names the branch that did it.
#:
#: **15 of 161 is the finding, not a shortfall in the instrument.** All 216
#: branches were deleted and every mutant ran; what the number says is that most
#: of this project's declared constraints are claims about its *inputs* rather
#: than about anything the model does -- see :data:`DECORATIVE` for the three
#: shapes that recur.
LOAD_BEARING: frozenset[str] = frozenset({
    # killed by int_latest_observation.candidates.where_conjunct.1
    "not_null_int_latest_observation_vin17",
    # killed by int_listing_state_fingerprints.source_rows.where_conjunct.0
    "not_null_int_listing_state_fingerprints_vin17",
    # killed by int_listing_volatility_features.<final>.coalesce_fallback.2
    "not_null_int_listing_volatility_features_price_change_count_30d",
    # killed by int_listing_volatility_features.<final>.coalesce_fallback.1
    "not_null_int_listing_volatility_features_price_change_count_7d",
    # killed by mart_cooldown_cohorts.bucketed.case_else.0
    "not_null_mart_cooldown_cohorts_attempt_bucket",
    # killed by mart_cooldown_cohorts.bucketed.case_else.1
    "not_null_mart_cooldown_cohorts_bucket_order",
    # killed by mart_cooldown_event_funnel.bucketed.case_else.0
    "not_null_mart_cooldown_event_funnel_attempt_bucket",
    # killed by mart_cooldown_event_funnel.bucketed.case_else.1
    "not_null_mart_cooldown_event_funnel_bucket_order",
    # killed by mart_deal_scores.<final>.case_else.0
    "not_null_mart_deal_scores_deal_tier",
    # killed by mart_vehicle_snapshot.<final>.case_else.0,
    #   mart_vehicle_snapshot.<final>.coalesce_fallback.0
    "not_null_mart_vehicle_snapshot_listing_state",
    # killed by int_latest_observation.<final>.where_conjunct.0
    "unique_int_latest_observation_vin17",
    # killed by int_listing_observation_fingerprints.<final>.where_conjunct.0
    "unique_int_listing_observation_fingerprints_observation_id",
    # killed by int_listing_state_fingerprints.<final>.where_conjunct.0
    "unique_int_listing_state_fingerprints_artifact_id",
    # killed by int_listing_volatility_features.open_observation_runs.where_conjunct.0,
    #   int_listing_volatility_features.open_runs.where_conjunct.0
    "unique_int_listing_volatility_features_vin17",
    # killed by mart_cooldown_cohorts.bucketed.case_arm.0 through .3
    "unique_mart_cooldown_cohorts_attempt_bucket",
})


#: Constraints no mutation inside their own model can break, against the branch
#: whose guard holds the invariant upstream -- or `""` where **no model in the
#: project declares one**, which is the finding rather than the gap. **Ratchets
#: down**: seeded at 146 on 2026-09-07, of which 22 name an upstream guard, and
#: a constraint that becomes decorative without an entry fails the gate.
#:
#: The 124 empty values are the majority and they are not all the same thing.
#: Three shapes recur, and the branch predicates the gate prints tell them apart:
#:
#: - **A property of the source, not of any guard.** `stg_price_events.event_id`
#:   is non-null because the Parquet it is read from has it. Nothing in dbt
#:   defends that, and a constraint here is the only thing that would notice the
#:   day it stops being true -- which is the case for keeping it.
#: - **A property of the expression.** `md5(concat_ws('|', ...))` cannot return
#:   NULL, so the 48 `coalesce(field, '')` branches inside the two fingerprint
#:   concats are unbreakable by a `not_null` **structurally** rather than for
#:   want of data: `concat_ws` skips NULL arguments. Removing one changes the
#:   digest, and no declared constraint is a claim about the digest's value.
#: - **A guard that exists and that the fixture never exercises alone.**
#:   `not_null_int_latest_observation_make` is held by `o.make is not null` in
#:   the model's own `candidates` CTE, and deleting it changes nothing because
#:   the fixture's only two NULL-make rows also have a NULL `vin17` and are
#:   removed by the conjunct beside it. That is a fixture gap, and exit 2's
#:   ledger is where it is owed.
DECORATIVE: dict[str, str] = {
    "accepted_values_int_latest_obs_cc9f3cfbd4d341fd48eaabb23a6ed3d3": "",
    "accepted_values_int_listing_ob_1ce5aa8ecc45d916cf60586bdcc22f86": "",
    "accepted_values_mart_cooldown__5ed6907c0a3f9936a0b1f9ba48e8bf76": "",
    "accepted_values_mart_cooldown__be2883a0831bb82e5b74132994b20564": "",
    "accepted_values_mart_deal_scor_78902edefdd33e65d2ea44dfcf0a918c": "",
    "accepted_values_mart_scrape_vo_76347f55db3059e065e0c7e43e835a1a": "",
    "accepted_values_mart_vehicle_s_080dbc70ddc2994072a32bd9085d9bcb": "",
    "accepted_values_stg_blocked_co_bfebdbe4ac45dc6ae06f9f8f2799d1cf": "",
    "accepted_values_stg_observations_source__srp__detail__carousel": "",
    "not_null_int_active_make_models_make": "",
    "not_null_int_active_make_models_model": "",
    "not_null_int_benchmarks_make": "int_latest_observation.candidates.where_conjunct.0",
    "not_null_int_benchmarks_model": "",
    "not_null_int_benchmarks_national_avg_price": "",
    "not_null_int_benchmarks_national_listing_count": "",
    "not_null_int_benchmarks_national_median_price": "",
    "not_null_int_benchmarks_national_p10_price": "",
    "not_null_int_benchmarks_national_p25_price": "",
    "not_null_int_benchmarks_national_p75_price": "",
    "not_null_int_benchmarks_national_p90_price": "",
    "not_null_int_latest_observation_make": "",
    "not_null_int_latest_observation_source": "",
    "not_null_int_listing_observati_52d331d76c0ad62222776f37f3b9e532": "",
    "not_null_int_listing_observati_ba56f19c0aa7b49973303e49ccafb3da": "",
    "not_null_int_listing_observation_fingerprints_artifact_id": "",
    "not_null_int_listing_observation_fingerprints_fetched_at": "",
    "not_null_int_listing_observation_fingerprints_listing_id": "",
    "not_null_int_listing_observation_fingerprints_observation_id": "",
    "not_null_int_listing_observation_fingerprints_source": "",
    "not_null_int_listing_observation_runs_carousel_seen": "",
    "not_null_int_listing_observation_runs_detail_observation_count": "",
    "not_null_int_listing_observation_runs_detail_seen": "",
    "not_null_int_listing_observation_runs_distinct_source_count": "",
    "not_null_int_listing_observation_runs_is_open_run": "",
    "not_null_int_listing_observation_runs_listing_id":
        "int_listing_observation_fingerprints.source_rows.where_conjunct.0",
    "not_null_int_listing_observation_runs_observation_count": "",
    "not_null_int_listing_observation_runs_observation_state_key": "",
    "not_null_int_listing_observation_runs_run_duration_hours": "",
    "not_null_int_listing_observation_runs_run_ended_at": "",
    "not_null_int_listing_observation_runs_run_started_at": "",
    "not_null_int_listing_observation_runs_srp_observation_count": "",
    "not_null_int_listing_observation_runs_srp_seen": "",
    "not_null_int_listing_state_fingerprints_artifact_id": "",
    "not_null_int_listing_state_fingerprints_fetched_at": "",
    "not_null_int_listing_state_fingerprints_listing_id": "",
    "not_null_int_listing_state_fingerprints_parsed_fingerprint": "",
    "not_null_int_listing_state_runs_artifact_count": "",
    "not_null_int_listing_state_runs_is_open_run": "",
    "not_null_int_listing_state_runs_listing_id":
        "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.0",
    "not_null_int_listing_state_runs_parsed_fingerprint": "",
    "not_null_int_listing_state_runs_run_duration_hours": "",
    "not_null_int_listing_state_runs_run_ended_at": "",
    "not_null_int_listing_state_runs_run_started_at": "",
    "not_null_int_listing_state_runs_vin17":
        "int_listing_state_fingerprints.source_rows.where_conjunct.0",
    "not_null_int_listing_volatilit_17463d72b17959a6c1ca1bf6375276df": "",
    "not_null_int_listing_volatilit_188ecc237135c0ee23aee1fc252dad6b": "",
    "not_null_int_listing_volatilit_64486d2f77224a3656f72e6006cd5434": "",
    "not_null_int_listing_volatilit_9636ef07cdbf09528ca2ede4e31fcb6e": "",
    "not_null_int_listing_volatilit_9c936e0e3de1aa0c62dc6bf7ada0d225": "",
    "not_null_int_listing_volatilit_a77a0d04603911dfa8ab1f0f68fe6476": "",
    "not_null_int_listing_volatilit_b0b6a86bd22026bc9703bfcf14a6c786": "",
    "not_null_int_listing_volatilit_c6947ca2254d4b5e1ef7631af8f4d87c": "",
    "not_null_int_listing_volatilit_dcf33b1f3bcb730feaa903d520dcb283": "",
    "not_null_int_listing_volatility_features_first_seen_at": "",
    "not_null_int_listing_volatility_features_latest_fetched_at": "",
    "not_null_int_listing_volatility_features_listing_id":
        "int_listing_state_fingerprints.fingerprinted.coalesce_fallback.0",
    "not_null_int_listing_volatility_features_total_state_changes": "",
    "not_null_int_listing_volatility_features_vin17":
        "int_listing_state_fingerprints.source_rows.where_conjunct.0",
    "not_null_int_price_history_current_price": "stg_price_events.<final>.where_conjunct.0",
    "not_null_int_price_history_first_price": "stg_price_events.<final>.where_conjunct.0",
    "not_null_int_price_history_first_seen_at": "",
    "not_null_int_price_history_last_seen_at": "",
    "not_null_int_price_history_max_price": "stg_price_events.<final>.where_conjunct.0",
    "not_null_int_price_history_min_price": "stg_price_events.<final>.where_conjunct.0",
    "not_null_int_price_history_price_drop_count": "stg_price_events.<final>.where_conjunct.0",
    "not_null_int_price_history_price_increase_count": "stg_price_events.<final>.where_conjunct.0",
    "not_null_int_price_history_price_observed_at": "",
    "not_null_int_price_history_total_price_observations": "",
    "not_null_int_price_history_vin": "stg_price_events.<final>.where_conjunct.3",
    "not_null_mart_block_rate_block_increments": "",
    "not_null_mart_block_rate_hour": "",
    "not_null_mart_block_rate_max_attempts_seen": "",
    "not_null_mart_block_rate_new_blocks": "",
    "not_null_mart_block_rate_total_block_events": "",
    "not_null_mart_block_rate_unique_listings_blocked": "",
    "not_null_mart_cooldown_cohorts_listing_count": "",
    "not_null_mart_cooldown_cohorts_max_attempts": "",
    "not_null_mart_cooldown_cohorts_min_attempts": "",
    "not_null_mart_cooldown_event_funnel_event_hour": "",
    "not_null_mart_cooldown_event_funnel_scrape_count": "",
    "not_null_mart_cooldown_event_funnel_unique_listing_count": "",
    "not_null_mart_deal_scores_current_price": "int_benchmarks.<final>.where_conjunct.2",
    "not_null_mart_deal_scores_deal_score": "",
    "not_null_mart_detail_batch_outcomes_detail_artifacts": "",
    "not_null_mart_detail_batch_outcomes_detail_observations": "",
    "not_null_mart_detail_batch_outcomes_obs_date": "",
    "not_null_mart_detail_batch_outcomes_unique_vins_enriched": "",
    "not_null_mart_detail_batch_outcomes_valid_vin_count": "",
    "not_null_mart_inventory_coverage_coverage_pct":
        "int_latest_observation.<final>/derived0.case_arm.0",
    "not_null_mart_inventory_coverage_detail_enriched":
        "int_latest_observation.<final>/derived0.case_arm.0",
    "not_null_mart_inventory_coverage_make": "int_latest_observation.candidates.where_conjunct.0",
    "not_null_mart_inventory_coverage_model": "",
    "not_null_mart_inventory_coverage_srp_only":
        "int_latest_observation.<final>/derived0.case_arm.0",
    "not_null_mart_inventory_coverage_total_vins": "",
    "not_null_mart_price_freshness_trend_fresh_1_3d": "",
    "not_null_mart_price_freshness_trend_fresh_4_7d": "",
    "not_null_mart_price_freshness_trend_fresh_8_14d": "",
    "not_null_mart_price_freshness_trend_fresh_lt_1d": "",
    "not_null_mart_price_freshness_trend_fresh_lt_7d_pct": "",
    "not_null_mart_price_freshness_trend_make":
        "int_latest_observation.candidates.where_conjunct.0",
    "not_null_mart_price_freshness_trend_model": "",
    "not_null_mart_price_freshness_trend_stale_gt_14d": "",
    "not_null_mart_price_freshness_trend_total_vins": "",
    "not_null_mart_scrape_volume_artifact_count": "",
    "not_null_mart_scrape_volume_hour": "",
    "not_null_mart_scrape_volume_observation_count": "",
    "not_null_mart_scrape_volume_scrape_volume_key": "",
    "not_null_mart_scrape_volume_source": "",
    "not_null_mart_scrape_volume_unique_listings": "",
    "not_null_mart_scrape_volume_valid_vin_count": "",
    "not_null_mart_vehicle_snapshot_vin": "int_latest_observation.affected_vins.where_conjunct.0",
    "not_null_stg_blocked_cooldown_events_event_at": "",
    "not_null_stg_blocked_cooldown_events_event_id": "",
    "not_null_stg_blocked_cooldown_events_event_type": "",
    "not_null_stg_blocked_cooldown_events_listing_id": "",
    "not_null_stg_blocked_cooldown_events_num_of_attempts": "",
    "not_null_stg_dealers_customer_id": "",
    "not_null_stg_observations_source": "",
    "not_null_stg_price_events_event_at": "",
    "not_null_stg_price_events_event_id": "",
    "not_null_stg_price_events_event_type": "",
    "not_null_stg_price_events_listing_id": "",
    "not_null_stg_price_events_price": "",
    "not_null_stg_price_events_vin": "",
    "not_null_stg_search_configs_make_slug": "",
    "not_null_stg_search_configs_model_slug": "",
    "not_null_stg_search_configs_radius_miles": "",
    "not_null_stg_search_configs_zip": "",
    "unique_int_price_history_vin": "stg_price_events.<final>.where_conjunct.3",
    "unique_mart_block_rate_hour": "",
    "unique_mart_detail_batch_outcomes_obs_date": "",
    "unique_mart_scrape_volume_scrape_volume_key": "",
    "unique_mart_vehicle_snapshot_vin": "int_latest_observation.affected_vins.where_conjunct.0",
    "unique_stg_blocked_cooldown_events_event_id": "",
    "unique_stg_dealers_customer_id": "",
    "unique_stg_price_events_event_id": "",
}


#: Models that materialize zero rows, so every constraint they declare passes
#: over an empty relation and no mutation can break it. **Asserted in both
#: directions**, because this set shrinking is an improvement and this set
#: growing is a defect, and a count cannot tell you which happened.
#:
#: **Empty as of 05:25 UTC on 2026-09-07, and it was four models eleven minutes
#: earlier.** No waiver list, which is the position the plan document argues at
#: length: a waiver is exactly where "this model is legitimately empty in the
#: fixture" gets written down once and rot gets recorded instead of repaired.
#: The cause of the four is one line of provenance, recorded in the plan
#: document under "Five models build over an empty world".
VACUOUS_MODELS: frozenset[str] = frozenset()


def _cold_compile() -> Path:
    """Render every model's cold form, in a subprocess, and return its root.

    **A subprocess, not** :func:`~tests.integration.dbt.real_build.run_dbt`.
    dbt-duckdb caches its adapter and holds `DUCKDB_PATH` open read-write for
    the life of the interpreter, so an in-process invocation would make
    `attached_warehouse()` impossible for the rest of the session -- the
    precondition `assert_no_in_process_dbt()` exists to state.

    `--full-refresh` renders the cold form even where the relation already
    exists, which is what a rematerialization computes. Without it the compiled
    tree is whichever phase happened to run last, and the gate's denominator
    would depend on how many times somebody had built the warehouse.
    """
    result = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "compile", "--full-refresh",
         "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
         "--target", "duckdb"],
        capture_output=True, text=True, encoding="utf-8",
    )
    assert result.returncode == 0, (
        f"dbt compile --full-refresh failed, so there is no branch list and no "
        f"model SQL to mutate:\n{result.stdout[-3000:]}\n{result.stderr[-2000:]}"
    )
    return DBT_DIR / "target" / "compiled" / "cartracker" / "models"


class _Ledger:
    """One pass of the whole gate, so the tests below read it rather than repeat it."""

    def __init__(self):
        self.baseline_rows: dict[str, int] = {}
        self.baseline_failures: list[str] = []
        self.errors: list[str] = []
        self.declined: list[str] = []
        self.orphaned: list[str] = []           # compiled test SQL with no manifest entry
        self.branch_ids: set[str] = set()
        self.constraints: dict[str, str] = {}      # constraint -> model
        self.killed: dict[str, set[str]] = {}      # constraint -> branch ids
        self.reasons: dict[str, str] = {}          # decorative constraint -> branch id

    @property
    def load_bearing(self) -> set[str]:
        return {name for name, killers in self.killed.items() if killers}

    @property
    def vacuous(self) -> set[str]:
        return {name for name, model in self.constraints.items()
                if self.baseline_rows.get(model) == 0}

    @property
    def decorative(self) -> set[str]:
        return set(self.constraints) - self.load_bearing - self.vacuous

    def verdicts(self) -> tuple[ConstraintVerdict, ...]:
        """One verdict per declared constraint, in name order.

        The three classes read off it: `killed_by` non-empty is load-bearing,
        empty is decorative or vacuous depending on whether the model produced a
        row, and `upstream_guard` is where the invariant lives when this model
        does not establish it.
        """
        return tuple(
            ConstraintVerdict(
                model=model,
                test_name=name,
                killed_by=tuple(sorted(self.killed.get(name, ()))),
                upstream_guard=self.reasons.get(name, ""),
            )
            for name, model in sorted(self.constraints.items())
        )


def _ancestors(name: str, models: dict, by_uid: dict) -> list[str]:
    """Model names upstream of *name*, **nearest first**.

    Order is the whole point: `upstream_guard_for` returns the first branch
    whose predicate mentions the column, so a breadth-first walk makes that the
    closest model that guards it rather than an arbitrary one several joins
    away.
    """
    found: list[str] = []
    seen = {name}
    frontier = [models[name]["unique_id"]]
    while frontier:
        following = []
        for uid in frontier:
            for parent in by_uid.get(uid, {}).get("depends_on", {}).get("nodes", []):
                node = by_uid.get(parent)
                if node is None or node["resource_type"] != "model" or node["name"] in seen:
                    continue
                seen.add(node["name"])
                found.append(node["name"])
                following.append(parent)
        frontier = following
    return found


@lru_cache(maxsize=1)
def _measure() -> _Ledger:
    """Mutate every branch of every model and record what each mutation breaks.

    Measured once for the module. The cost is one `dbt compile --full-refresh`
    plus 216 rematerializations of a single model each, which on 2026-09-07 ran
    in about four seconds against the fixture warehouse -- cheap enough that
    this is a standing gate rather than a thing somebody runs by hand.
    """
    assert_no_in_process_dbt()
    compiled_root = _cold_compile()

    manifest = json.loads((DBT_DIR / "target" / "manifest.json").read_text(encoding="utf-8"))
    by_uid = manifest["nodes"]
    models = {n["name"]: n for n in by_uid.values() if n["resource_type"] == "model"}
    # The compiled file's stem, not the manifest's `name`: dbt truncates and
    # hashes long generated test names on the way to disk, and the truncated
    # form is what dbt's own output calls the test.
    declared = {
        Path(n["compiled_path"]).stem: (
            by_uid[n.get("attached_node") or n["depends_on"]["nodes"][0]]["name"],
            n.get("column_name") or "",
        )
        for n in by_uid.values()
        if n["resource_type"] == "test"
    }

    ledger = _Ledger()
    branches = {
        name: branches_for_model(DBT_DIR / node["compiled_path"])
        for name, node in models.items()
    }
    for found in branches.values():
        ledger.branch_ids.update(b.id for b in found)

    connection = attached_warehouse()
    try:
        for name, node in sorted(models.items()):
            path = DBT_DIR / node["compiled_path"]
            sql = path.read_text(encoding="utf-8")
            # Discovery is the compiled directory; **the manifest is what makes
            # it a fact.** `dbt compile` writes test SQL and never deletes it,
            # so a constraint removed from a schema file leaves its compiled
            # statement on disk indefinitely, and a harness that listed the
            # directory would go on classifying a constraint the project no
            # longer declares -- coverage over something that is not there.
            # A file with no manifest entry is recorded and fails
            # test_every_declared_constraint_is_classified rather than being
            # skipped or, as it did first, raising a KeyError three functions
            # later.
            tests = []
            for test_name, test_sql in compiled_tests_for(name, compiled_root):
                if test_name not in declared:
                    ledger.orphaned.append(f"{name}: {test_name}")
                    continue
                tests.append((test_name, test_sql))
                ledger.constraints[test_name] = name
                ledger.killed.setdefault(test_name, set())

            base = f'memory.main."baseline__{name}"'
            try:
                connection.execute(
                    SQL("materialize_relation").format(relation=base, body=sql))
                ledger.baseline_rows[name] = connection.execute(
                    SQL("count_relation_rows").format(
                        relation=base)).fetchone()[0]
            except Exception as error:  # noqa: BLE001 - reported, never swallowed
                ledger.baseline_failures.append(
                    f"{name}: would not materialize: {type(error).__name__}: {error}")
                continue
            for test_name, test_sql in tests:
                failing = connection.execute(
                    SQL("count_failing_rows").format(
                        test_query=retarget(test_sql, name, base))).fetchone()[0]
                if failing:
                    ledger.baseline_failures.append(
                        f"{name}.{test_name}: {failing} rows before any mutation")

            tree = sqlglot.parse_one(sql, dialect=DIALECT)
            for branch in branches[name]:
                mutant = mutate(tree, branch)
                if mutant is None:
                    ledger.declined.append(f"{branch.id} -- `{branch.predicate}`")
                    continue
                relation = 'memory.main."mutant"'
                try:
                    connection.execute(SQL("materialize_relation").format(
                        relation=relation, body=mutant.sql(dialect=DIALECT)))
                except Exception as error:  # noqa: BLE001
                    ledger.errors.append(
                        f"{branch.id}: mutant would not materialize: "
                        f"{type(error).__name__}: {error}")
                    continue
                for test_name, test_sql in tests:
                    try:
                        failing = connection.execute(
                            SQL("count_failing_rows").format(
                                test_query=retarget(test_sql, name, relation),
                            )).fetchone()[0]
                    except Exception as error:  # noqa: BLE001
                        ledger.errors.append(
                            f"{branch.id}: {test_name} would not run against the "
                            f"mutant: {type(error).__name__}: {error}")
                        continue
                    if failing:
                        ledger.killed[test_name].add(branch.id)
    finally:
        connection.close()

    for test_name in ledger.decorative:
        model, column = declared[test_name]
        if not column:
            ledger.reasons[test_name] = ""
            continue
        upstream = _ancestors(model, models, by_uid)
        ordered = [b for parent in upstream for b in branches[parent]]
        tree = sqlglot.parse_one(
            (DBT_DIR / models[model]["compiled_path"]).read_text(encoding="utf-8"),
            dialect=DIALECT,
        )
        reason = ""
        for candidate in source_columns_for(tree, column):
            reason = upstream_guard_for(model, candidate, ordered, upstream)
            if reason:
                break
        ledger.reasons[test_name] = reason
    return ledger


def test_the_unmutated_baseline_is_green():
    """Nothing this gate reports means anything until this passes.

    A mutation operator that emits invalid SQL makes every constraint in the
    model die, and a gate that only counted kills would read that as total
    coverage -- the one failure mode a mutation instrument must not have. This
    is the precondition that forecloses it from the other side: before a single
    branch is deleted, every model is computed from its own compiled SQL in the
    in-memory database and every one of its declared constraints is run against
    the result. If the harness cannot build a green baseline, its kills are
    measuring the harness.

    It is not the same claim as `dbt build` passing. dbt tested the relations it
    wrote into the warehouse; this recomputes each model against whatever its
    upstream holds *now*, which for the four staging views and the two
    `postgres_scan` sources is live data rather than a build artifact. A
    baseline that fails here where the build passed is that divergence, and it
    has to be visible rather than absorbed into a kill count.
    """
    ledger = _measure()
    assert not ledger.baseline_failures, (
        f"{len(ledger.baseline_failures)} models or constraints are already red "
        f"before any mutation, so no verdict below is trustworthy -- a kill "
        f"cannot be distinguished from a baseline that was never green:\n  "
        + "\n  ".join(ledger.baseline_failures[:20])
    )


def test_no_mutant_failed_to_execute():
    """A mutant that will not run is a broken operator, never a kill.

    This is the loud half of the guard above. An operator that leaves `CASE END`
    behind produces a mutant DuckDB refuses to parse, and the tempting reading
    of an exception is "the constraint broke". It did not; the instrument did.
    The first run of this gate found exactly that in six models, and the
    operator that caused it is now the one documented case in
    :func:`tests.dbt.branch_mutation.mutate`.
    """
    ledger = _measure()
    assert not ledger.errors, (
        f"{len(ledger.errors)} mutants would not execute. This is the mutation "
        f"operator failing, not a constraint being broken -- counting these as "
        f"kills would report the coverage this gate exists to measure:\n  "
        + "\n  ".join(ledger.errors[:20])
    )


def test_every_branch_yields_a_mutant():
    """The quiet half: an operator that declines a branch shrinks the denominator.

    A branch `mutate()` returns `None` for is never deleted, so no constraint it
    holds can ever be shown load-bearing, and the constraint lands in
    :data:`DECORATIVE` for want of an operator rather than for want of a guard.
    Unlike an unparseable mutant this costs no error and no red test, which is
    why it gets one of its own.
    """
    ledger = _measure()
    assert not ledger.declined, (
        f"{len(ledger.declined)} branches produced no mutant, so nothing they "
        f"hold can be shown load-bearing and every constraint over them reads "
        f"as decorative. Add the operator in tests/dbt/branch_mutation.py "
        f"rather than recording the verdict:\n  " + "\n  ".join(ledger.declined)
    )


def test_every_load_bearing_constraint_stays_load_bearing():
    """Exit 4's evidence half, and it ratchets up.

    Every name in :data:`LOAD_BEARING` was shown load-bearing by deleting a
    guard and watching its test go red. A constraint that stops dying has had
    the guard that produced it removed or weakened, and the constraint left
    behind is now asserting something the model no longer establishes -- the
    silent version of a regression, since the test itself still passes.
    """
    ledger = _measure()
    lost = sorted(LOAD_BEARING - ledger.load_bearing)
    assert not lost, (
        "these constraints were load-bearing and no longer are: no mutation of "
        "their own model breaks them any more. Either the guard that produced "
        "the invariant is gone -- in which case the constraint now asserts "
        "something nothing establishes -- or a mutation operator regressed. "
        "Moving the name to DECORATIVE is the last resort and owes the reason:"
        "\n  " + "\n  ".join(lost)
    )


def test_the_decorative_ledger_is_exactly_what_no_mutation_breaks():
    """Exit 4's debt half, and it ratchets down.

    Both directions. A constraint that becomes decorative fails here, because
    the alternative is a guard quietly leaving a model while the constraint over
    it goes on passing. A constraint that becomes load-bearing fails here too,
    because leaving it in the ledger would understate what the models defend --
    the same improvement-shaped lie the branch-coverage gate refuses in
    `test_the_unprobeable_set_is_exactly_what_the_prober_cannot_express`.

    **A decorative verdict is not a defect to be fixed.** The stated limit of
    mutation is that it measures the code as it stands and cannot tell
    "decorative because redundant" from "decorative but a useful regression
    barrier" -- `not_null_mart_vehicle_snapshot_vin` would still catch someone
    turning `obs` into a left join later. That is why this records them rather
    than asking for their deletion, and why the entry's value is a pointer at
    where the invariant lives rather than a waiver.
    """
    ledger = _measure()
    measured = ledger.decorative
    arrived = sorted(measured - set(DECORATIVE))
    departed = sorted(set(DECORATIVE) - measured)
    assert not arrived and not departed, (
        f"the decorative set moved. Newly decorative: {arrived} -- no mutation "
        f"of their own model breaks these any more, so check whether the guard "
        f"that produced the invariant is still there before recording them; no "
        f"longer decorative: {departed} -- these are now held up by a branch in "
        f"their own model and belong in LOAD_BEARING."
    )


def test_every_decorative_verdict_names_where_the_invariant_actually_lives():
    """The reason string is derived, so the ledger cannot drift from the models.

    A hand-written reason is a claim that can be wrong forever without failing,
    which is the defect this whole stage was scoped against. So `DECORATIVE`'s
    values are re-derived from the branch list on every run and compared, and an
    invariant that moves upstream -- or a `""` that becomes a real guard, which
    is a model gaining a defence it did not have -- shows up as a diff a reader
    can check.

    Branch ids are positional and move when a model is edited. That is the cost
    already accepted for positional identity in
    :mod:`tests.dbt.branch_list`, and it is the right cost here for the same
    reason: keying on predicate text would silently detach every reason on the
    next edit.
    """
    ledger = _measure()
    decorative = ledger.decorative & set(DECORATIVE)
    wrong = sorted(
        f"{verdict.model}.{verdict.test_name}: recorded "
        f"{DECORATIVE[verdict.test_name]!r}, measured {verdict.upstream_guard!r}"
        for verdict in ledger.verdicts()
        if verdict.test_name in decorative
        and not verdict.load_bearing
        and DECORATIVE[verdict.test_name] != verdict.upstream_guard
    )
    assert not wrong, (
        "these decorative constraints name an upstream guard that is no longer "
        "the one holding them. A value that was a branch id and is now empty "
        "means the guard left the project and nothing establishes the invariant "
        "any more; a value that was empty and is now a branch id means a model "
        "gained the defence:\n  " + "\n  ".join(wrong)
    )


def test_no_declared_upstream_guard_outlives_the_branch_it_names():
    """A reason pointing at nothing is worse than no reason.

    The branch-coverage gate makes the same argument about its waivers: a
    positional id that no longer exists is not untidy, it is a live claim about
    a guard that has moved, while the guard it used to name is now unclaimed
    under a different id.
    """
    ledger = _measure()
    stale = sorted(
        f"{name} -> {branch_id}"
        for name, branch_id in DECORATIVE.items()
        if branch_id and branch_id not in ledger.branch_ids
    )
    assert not stale, (
        "these upstream-guard reasons name branch ids that are not in the "
        "branch list. Either the model was edited and the id moved -- re-run "
        "the gate and re-seed from what it reports -- or the guard is gone, and "
        "the constraint downstream now restates an invariant nothing "
        "establishes:\n  " + "\n  ".join(stale)
    )


def test_no_ledger_entry_outlives_the_constraint_it_names():
    """A ledger of constraints that no longer exist is a ratchet with nothing behind it."""
    ledger = _measure()
    declared = set(ledger.constraints)
    stale = sorted((LOAD_BEARING | set(DECORATIVE)) - declared)
    assert not stale, (
        "these ledger entries name constraints the dbt project no longer "
        "declares. A constraint that was deleted takes its row here with it; "
        "one that was renamed needs the ledger re-seeded, because until then it "
        "is unclassified and this gate is silently smaller:\n  "
        + "\n  ".join(stale)
    )


def test_every_declared_constraint_is_classified():
    """The three classes partition the 161, so nothing can be quietly omitted.

    Load-bearing, decorative and vacuous are derived from one measurement and
    are disjoint by construction; this asserts they are also exhaustive against
    the manifest, **in both directions**.

    Neither direction is hypothetical. A constraint dbt declares that discovery
    does not find -- a schema file renamed, a test compiled somewhere the
    `rglob` does not look -- leaves the gate passing over a smaller project than
    exists. And compiled test SQL with no manifest entry is the mirror: `dbt
    compile` writes those files and never removes them, so a constraint deleted
    from a schema file leaves its statement on disk, and a harness reading the
    directory would go on grading a constraint that no longer exists. That one
    arrived by accident -- adding a `not_null` to demonstrate the ratchet, then
    reverting it, left the compiled artifact behind and the next run crashed on
    it -- which is a fair sample of how build-artifact staleness actually shows
    up.
    """
    ledger = _measure()
    manifest = json.loads((DBT_DIR / "target" / "manifest.json").read_text(encoding="utf-8"))
    declared = {
        Path(node["compiled_path"]).stem
        for node in manifest["nodes"].values()
        if node["resource_type"] == "test"
    }
    missing = sorted(declared - set(ledger.constraints))
    assert not missing, (
        f"dbt declares {len(declared)} data tests and this gate found "
        f"{len(ledger.constraints)}. The unfound ones are unclassified, so they "
        f"are neither shown load-bearing nor recorded decorative -- the gate is "
        f"passing over a smaller project than exists:\n  "
        + "\n  ".join(missing[:20])
    )
    assert not ledger.orphaned, (
        f"{len(ledger.orphaned)} compiled data tests have no entry in the "
        f"manifest, so dbt does not declare them and this gate cannot classify "
        f"them. They are stale build artifacts from an earlier schema file: "
        f"delete dbt/target/compiled and re-run, and if they come back, the "
        f"manifest and the compiled tree disagree about what the project "
        f"is:\n  " + "\n  ".join(sorted(ledger.orphaned)[:20])
    )


def test_no_constraint_asserts_over_an_empty_relation():
    """Exit 4's third answer, kept apart from the other two on purpose.

    A `not_null` over an empty relation is trivially true, so every constraint
    on a zero-row model passes and no mutation can break one. Filing those as
    decorative would be the arithmetic working and the meaning inverting: they
    are not constraints that turned out to be redundant, they are constraints
    that have never been evaluated against a row.

    Asserted in both directions and with **no waiver list**, which is the
    position the plan document argues at length: a waiver is exactly where "this
    model is legitimately empty in the fixture" gets written down once and rot
    gets recorded instead of repaired. `mart_vehicle_snapshot`'s
    `last_seen_at >= now() - interval '7 days'` arm was covered when it was
    written and has been dead for weeks because the fixture's dates are absolute
    and wall-clock time moved past them; a waiver is where that would have gone
    to hide.
    """
    ledger = _measure()
    empty = {model for model, rows in ledger.baseline_rows.items() if rows == 0}
    arrived = sorted(empty - VACUOUS_MODELS)
    departed = sorted(VACUOUS_MODELS - empty)
    assert not arrived and not departed, (
        f"the set of models that build to zero rows moved. Newly empty: "
        f"{arrived} -- every constraint these declare now passes over nothing, "
        f"and the fixture owes them a row; no longer empty: {departed} -- their "
        f"constraints are real assertions again and belong in the two classes "
        f"above rather than here. Constraints currently asserting over an empty "
        f"relation: {sorted(ledger.vacuous)}"
    )
