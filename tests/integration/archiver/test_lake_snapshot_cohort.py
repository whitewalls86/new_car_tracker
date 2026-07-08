"""
Cohort allocation + entity closure correctness for CI lake snapshot exports
(Plan 120 Gate C), run against real MinIO in CI.

Exercises `collect_selector_candidates`, `allocate_cohort`,
`expand_entity_closure`, and `build_snapshot_cohort` against the shared MinIO
fixture seeded by `scripts/seed_lake_snapshot_fixture.py`. The closure SQL joins
across all four source tables, so this is where a column drift in the ops event
tables (vin/listing/artifact/previous_listing_id) surfaces.

Pure allocation logic that does not read data (dedup/bucketing) is unit-tested
in tests/archiver/test_export_ci_lake_snapshot.py; here every test touches real
Parquet. Entity constants are imported from the seed so seeding and assertions
cannot drift apart.

Requires MINIO_ENDPOINT (set by the CI `dbt` job). Skipped everywhere else.
"""
import os

import pytest

from archiver.processors.export_ci_lake_snapshot import (
    SnapshotRequest,
    export_ci_lake_snapshot,
)
from archiver.processors.lake_snapshot_cohort import (
    CohortAllocation,
    allocate_cohort,
    build_snapshot_cohort,
    collect_all_selector_candidates,
    collect_selector_candidates,
    expand_entity_closure,
)
from scripts import seed_lake_snapshot_fixture as fx
from shared.duckdb_s3 import get_duckdb_s3_connection

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT"),
        reason="MINIO_ENDPOINT not set — no MinIO fixture to build a cohort from",
    ),
]


@pytest.fixture(scope="module")
def minio_con():
    con = get_duckdb_s3_connection()
    yield con
    con.close()


def _allocation(**overrides) -> CohortAllocation:
    """A CohortAllocation seeded only with the given sets (empty otherwise)."""
    base = dict(
        vin_seeds=frozenset(),
        listing_seeds=frozenset(),
        artifact_seeds=frozenset(),
        make_model_seeds=frozenset(),
        selector_coverage={},
        fill_vins_added=0,
        pre_fill_vin_count=0,
        required_vin_seeds=frozenset(),
    )
    base.update(overrides)
    return CohortAllocation(**base)


# ---------------------------------------------------------------------------
# Candidate collection
# ---------------------------------------------------------------------------

class TestCollectCandidates:
    def test_returns_full_pool_not_five_sample(self, minio_con):
        # no_price_history matches far more than the 5-entity diagnostic sample.
        candidate = collect_selector_candidates(minio_con, "no_price_history", base_path=None)
        assert candidate.error is None
        assert len(candidate.entities) > 5

    def test_selected_entities_capped_at_required(self, minio_con):
        candidate = collect_selector_candidates(minio_con, "relisted_vin", base_path=None)
        assert candidate.required == 10
        assert len(candidate.selected_entities) <= candidate.required
        assert fx.VIN_RELISTED in candidate.selected_entities

    def test_collect_all_covers_every_runnable_selector(self, minio_con):
        from archiver.processors.lake_snapshot_selectors import RUNNABLE_SELECTORS
        candidates = collect_all_selector_candidates(minio_con, base_path=None)
        assert set(candidates.keys()) == set(RUNNABLE_SELECTORS)


# ---------------------------------------------------------------------------
# Allocation fill / determinism (reads representative VINs from real data)
# ---------------------------------------------------------------------------

class TestAllocationFill:
    def test_respects_target_vins_with_deterministic_fill(self, minio_con):
        candidates = collect_all_selector_candidates(
            minio_con, names=["relisted_vin"], base_path=None,
        )
        allocation = allocate_cohort(candidates, 10, minio_con, None)
        assert len(allocation.vin_seeds) == 10
        assert fx.VIN_RELISTED in allocation.vin_seeds

    def test_allocation_is_deterministic_across_repeated_runs(self, minio_con):
        results = []
        for _ in range(2):
            candidates = collect_all_selector_candidates(
                minio_con, names=["relisted_vin"], base_path=None,
            )
            results.append(allocate_cohort(candidates, 8, minio_con, None).vin_seeds)
        assert results[0] == results[1]


# ---------------------------------------------------------------------------
# Entity closure
# ---------------------------------------------------------------------------

class TestExpandClosure:
    def test_closure_adds_listing_ids_for_selected_vins(self, minio_con):
        closure = expand_entity_closure(
            minio_con, None, None, None,
            _allocation(vin_seeds=frozenset({fx.VIN_RELISTED})),
        )
        assert {fx.LISTING_RELISTED_1, fx.LISTING_RELISTED_2} <= closure["listing_ids"]

    def test_closure_adds_previous_listing_ids_from_remap_events(self, minio_con):
        # L2 -> VIN_RELISTED -> remap event reveals previous_listing_id L1.
        closure = expand_entity_closure(
            minio_con, None, None, None,
            _allocation(listing_seeds=frozenset({fx.LISTING_RELISTED_2})),
        )
        assert fx.LISTING_RELISTED_1 in closure["listing_ids"]
        assert closure["previous_listing_ids_added"] >= 1

    def test_closure_adds_artifact_ids(self, minio_con):
        closure = expand_entity_closure(
            minio_con, None, None, None,
            _allocation(vin_seeds=frozenset({fx.VIN_RELISTED})),
        )
        assert {fx.ARTIFACT_RELISTED_1, fx.ARTIFACT_RELISTED_2} <= closure["artifact_ids"]

    def test_closure_resolves_listing_context_for_artifact_only_seeds(self, minio_con):
        # invalid_or_null_vin seeds artifact 108 (vin=None, listing L8a); closure
        # must still pull L8a in even though that row's vin can't be resolved.
        closure = expand_entity_closure(
            minio_con, None, None, None,
            _allocation(artifact_seeds=frozenset({fx.ARTIFACT_NULL_VIN})),
        )
        assert fx.LISTING_NULL_VIN in closure["listing_ids"]
        assert fx.ARTIFACT_NULL_VIN in closure["artifact_ids"]

    def test_closure_resolves_non_vin_seeds_back_to_vins(self, minio_con):
        closure = expand_entity_closure(
            minio_con, None, None, None,
            _allocation(listing_seeds=frozenset({fx.LISTING_PRICE_7D})),
        )
        assert fx.VIN_L16 in closure["closed_vins"]

    def test_empty_allocation_does_not_crash(self, minio_con):
        closure = expand_entity_closure(minio_con, None, None, None, _allocation())
        assert closure["closed_vins"] == set()
        assert closure["listing_ids"] == set()
        assert closure["artifact_ids"] == set()

    def test_closure_logs_initial_counts_and_pass_progress(self, minio_con, caplog):
        """Progress logging (Plan 120 worker visibility): the closure must
        announce its initial seed counts, log each pass, and log why it
        stopped, so `docker logs -f` shows activity during a long closure."""
        with caplog.at_level("INFO", logger="archiver"):
            closure = expand_entity_closure(
                minio_con, None, None, None,
                _allocation(vin_seeds=frozenset({fx.VIN_RELISTED})),
            )
        messages = [r.message for r in caplog.records]
        assert any(
            "expand_entity_closure initial vins=1" in m for m in messages
        )
        assert any("closure pass=1 start" in m for m in messages)
        assert any(
            "closure pass=1 end" in m and "elapsed_s=" in m for m in messages
        )
        assert any(
            f"closure pass={closure['closure_passes']} no_change stopping" in m
            for m in messages
        )


# ---------------------------------------------------------------------------
# build_snapshot_cohort orchestration
# ---------------------------------------------------------------------------

class TestBuildSnapshotCohort:
    def test_builds_cohort_with_diagnostics(self, minio_con):
        cohort = build_snapshot_cohort(
            minio_con, None, None, None, target_vins=20,
            names=["relisted_vin", "cooldown_bucket_11_plus"],
        )
        assert fx.VIN_RELISTED in cohort.seed_vins
        assert cohort.diagnostics["closed_vins"] >= cohort.diagnostics["seed_vins"]
        assert cohort.selector_coverage["cooldown_bucket_11_plus"]["found"] >= 1
        assert "selector_coverage" in cohort.diagnostics

    def test_zero_candidate_selector_does_not_break_build(self, minio_con):
        # A window that excludes every candidate row must degrade to a coverage
        # miss, not an exception.
        from datetime import datetime, timezone
        cohort = build_snapshot_cohort(
            minio_con, None,
            datetime(2099, 6, 1, tzinfo=timezone.utc),
            datetime(2099, 12, 1, tzinfo=timezone.utc),
            target_vins=None,
            names=["cooldown_bucket_11_plus"],
        )
        assert cohort.selector_coverage["cooldown_bucket_11_plus"]["found"] == 0
        assert cohort.selector_coverage["cooldown_bucket_11_plus"]["status"] == "fail"
        assert cohort.seed_vins == frozenset()

    def test_make_model_expansion_flags_target_overrun(self, minio_con):
        # benchmark_dense_make_model is keyed on make_model, so allocate sees 0
        # vin pressure pre-closure; the vins only appear once the Honda Civic
        # group is resolved. The diagnostic must still flag the overrun.
        cohort = build_snapshot_cohort(
            minio_con, None, None, None, target_vins=1,
            names=["benchmark_dense_make_model"],
        )
        assert cohort.diagnostics["pre_fill_vin_count"] == 0
        assert cohort.diagnostics["required_vin_seed_count"] > 1
        assert cohort.diagnostics["target_vins_exceeded_by_required_selectors"] is True


# ---------------------------------------------------------------------------
# Exporter build_cohort integration
# ---------------------------------------------------------------------------

class TestExportBuildCohort:
    def test_dry_run_with_build_cohort_returns_cohort_diagnostics(self):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            target_vins=15, min_selector_coverage=False,
        ))
        assert result.status == "planned"
        assert result.cohort_diagnostics is not None
        assert result.seed_vin_count is not None
        assert result.closed_vin_count >= result.seed_vin_count
        assert result.listing_count is not None
        assert result.artifact_count is not None
        assert "selector_coverage" in result.cohort_diagnostics

    def test_dry_run_without_build_cohort_skips_closure(self):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, min_selector_coverage=False,
        ))
        assert result.cohort_diagnostics is None
        assert result.seed_vin_count is None

    def test_non_dry_run_ignores_build_cohort(self):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=False, run_selectors=True, build_cohort=True,
        ))
        assert result.status == "not_implemented"
        assert result.cohort_diagnostics is None
