"""
Selector SQL correctness for CI lake snapshot exports (Plan 120), run against
real MinIO in CI.

These tests execute the production selector SQL (via `run_lake_selectors` /
`collect_selector_candidates` / `audit_source_tables`) directly against the
shared MinIO fixture seeded by `scripts/seed_lake_snapshot_fixture.py`. Because
the fixture writes production-shaped Parquet (schema imported from the real
flush writers), a column rename or type change in the lake surfaces here as a
failing selector — which is the whole point of testing SQL against real data
rather than a hand-built local fixture that can only agree with itself.

Entity constants are imported from the seed module so seeding and assertions
cannot drift apart. Assertions are membership-based: the shared fixture packs
every selector's scenarios into one bucket, so selectors legitimately see more
than one entity — each test checks its known entity is (or is not) present.

Requires MINIO_ENDPOINT (set by the CI `dbt` job, which seeds the fixture and
starts MinIO). Skipped everywhere else.
"""
import os
from datetime import datetime, timezone

import pytest

from archiver.processors.export_ci_lake_snapshot import (
    SnapshotRequest,
    export_ci_lake_snapshot,
)
from archiver.processors.lake_snapshot_cohort import collect_selector_candidates
from archiver.processors.lake_snapshot_selectors import RUNNABLE_SELECTORS, run_lake_selectors
from archiver.processors.lake_source_audit import audit_source_tables
from scripts import seed_lake_snapshot_fixture as fx
from shared.duckdb_s3 import get_duckdb_s3_connection

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT"),
        reason="MINIO_ENDPOINT not set — no MinIO fixture to run selectors against",
    ),
]


@pytest.fixture(scope="module")
def minio_con():
    con = get_duckdb_s3_connection()
    yield con
    con.close()


def _candidates(con, name, **window) -> set:
    """Full (bounded) candidate entity set for a selector against real MinIO."""
    return set(collect_selector_candidates(con, name, base_path=None, **window).entities)


# ---------------------------------------------------------------------------
# Source audit
# ---------------------------------------------------------------------------

class TestSourceAudit:
    def test_all_four_tables_exist_and_have_rows(self):
        audit = audit_source_tables(base_path=None)
        assert audit["ok"] is True, audit["errors"]
        for table in (
            "silver_observations", "price_observation_events",
            "vin_to_listing_events", "blocked_cooldown_events",
        ):
            assert audit["tables"][table]["exists"] is True
            assert audit["tables"][table]["rows"] > 0

    def test_window_filter_reduces_row_counts(self):
        # The fixture's silver rows span 2026-01 (stale) through 2026-07; a
        # narrow window must exclude the January stale rows.
        full = audit_source_tables(base_path=None)
        windowed = audit_source_tables(
            base_path=None,
            window_start=datetime(2026, 7, 1, tzinfo=timezone.utc),
            window_end=datetime(2026, 8, 1, tzinfo=timezone.utc),
        )
        assert (
            windowed["tables"]["silver_observations"]["rows"]
            < full["tables"]["silver_observations"]["rows"]
        )


# ---------------------------------------------------------------------------
# Selector candidate correctness (membership against real data)
# ---------------------------------------------------------------------------

class TestSilverSelectors:
    def test_stable_state_run_finds_unchanged_fingerprint(self, minio_con):
        assert fx.VIN_IDENT in _candidates(minio_con, "stable_state_run")

    def test_state_change_run_finds_changed_fingerprints(self, minio_con):
        changed = _candidates(minio_con, "state_change_run")
        assert {fx.VIN_PRICE, fx.VIN_STATECHG, fx.VIN_RELIST2, fx.VIN_ABA} <= changed
        # A VIN with identical consecutive states must not be flagged as changed.
        assert fx.VIN_IDENT not in changed

    def test_active_to_unlisted_finds_active_then_unlisted(self, minio_con):
        assert fx.LISTING_ACTIVE_UNLIST in _candidates(minio_con, "active_to_unlisted")

    def test_detail_beats_srp_finds_detail_winner(self, minio_con):
        assert fx.VIN_DETAIL_WINS in _candidates(minio_con, "detail_beats_srp")

    def test_srp_fallback_finds_srp_only_vin(self, minio_con):
        assert fx.VIN_SRP_ONLY in _candidates(minio_con, "srp_fallback")

    def test_carousel_only_finds_carousel_vin(self, minio_con):
        assert fx.VIN_CAROUSEL in _candidates(minio_con, "carousel_only_or_low_priority")

    def test_no_price_history_finds_vin_absent_from_price_events(self, minio_con):
        assert fx.VIN_NO_PRICE in _candidates(minio_con, "no_price_history")

    def test_invalid_or_null_vin_finds_bad_rows(self, minio_con):
        artifacts = _candidates(minio_con, "invalid_or_null_vin")
        assert {fx.ARTIFACT_NULL_VIN, fx.ARTIFACT_SHORT_VIN} <= artifacts

    def test_benchmark_dense_finds_dense_group(self, minio_con):
        assert "Honda Civic" in _candidates(minio_con, "benchmark_dense_make_model")

    def test_benchmark_sparse_finds_sparse_group(self, minio_con):
        sparse = _candidates(minio_con, "benchmark_sparse_make_model")
        assert "Rare Bird" in sparse
        assert "Honda Civic" not in sparse  # dense group must not leak into sparse

    def test_fresh_recent_listing_finds_freshest(self, minio_con):
        assert fx.LISTING_FRESH in _candidates(minio_con, "fresh_recent_listing")

    def test_stale_listing_finds_oldest(self, minio_con):
        assert fx.LISTING_STALE in _candidates(minio_con, "stale_listing")


class TestPriceSelectors:
    def test_price_drop_finds_consecutive_lower_price(self, minio_con):
        assert fx.LISTING_RELISTED_1 in _candidates(minio_con, "price_drop")

    def test_price_increase_finds_consecutive_higher_price(self, minio_con):
        assert fx.LISTING_RELISTED_1 in _candidates(minio_con, "price_increase")

    def test_price_changed_7d_finds_change_near_anchor(self, minio_con):
        seven_d = _candidates(minio_con, "price_changed_7d")
        assert fx.LISTING_PRICE_7D in seven_d
        assert fx.LISTING_PRICE_30D not in seven_d  # 7/10 change is outside 7d of the anchor

    def test_price_changed_30d_only_finds_change_outside_7d(self, minio_con):
        thirty_d = _candidates(minio_con, "price_changed_30d_only")
        assert fx.LISTING_PRICE_30D in thirty_d
        assert fx.LISTING_PRICE_7D not in thirty_d  # 8/1 change belongs to the 7d bucket

    def test_price_changed_7d_anchors_to_explicit_window_end(self, minio_con):
        # With window_end far past every change, nothing is within 7d — proves
        # recency anchors to the requested window_end, not MAX(event_at).
        seven_d = _candidates(
            minio_con, "price_changed_7d",
            window_end=datetime(2026, 12, 1, tzinfo=timezone.utc),
        )
        assert fx.LISTING_PRICE_7D not in seven_d


class TestCooldownSelectors:
    def test_cooldown_blocked_finds_first_attempt(self, minio_con):
        assert fx.LISTING_COOLDOWN_SINGLE in _candidates(minio_con, "cooldown_blocked")

    def test_cooldown_incremented_finds_repeated_attempt(self, minio_con):
        assert fx.LISTING_RELISTED_1 in _candidates(minio_con, "cooldown_incremented")

    def test_cooldown_bucket_3_4(self, minio_con):
        assert fx.LISTING_COOLDOWN_3_4 in _candidates(minio_con, "cooldown_bucket_3_4")

    def test_cooldown_bucket_5_10(self, minio_con):
        assert fx.LISTING_COOLDOWN_5_10 in _candidates(minio_con, "cooldown_bucket_5_10")

    def test_cooldown_bucket_11_plus(self, minio_con):
        assert fx.LISTING_COOLDOWN_11_PLUS in _candidates(minio_con, "cooldown_bucket_11_plus")


class TestRelistedSelector:
    def test_relisted_vin_finds_multi_listing_vin(self, minio_con):
        relisted = _candidates(minio_con, "relisted_vin")
        assert fx.VIN_RELISTED in relisted
        assert fx.VIN_SINGLE not in relisted  # single-listing VIN is not relisted


# ---------------------------------------------------------------------------
# run_lake_selectors / exporter diagnostics contract against real data
# ---------------------------------------------------------------------------

class TestRunLakeSelectorsContract:
    def test_all_selectors_run_without_error(self):
        diagnostics = run_lake_selectors(base_path=None)
        assert set(diagnostics["selectors"].keys()) == set(RUNNABLE_SELECTORS)
        assert diagnostics["ok"] is True, diagnostics["errors"]
        assert diagnostics["errors"] == []

    def test_export_run_selectors_returns_diagnostics(self):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, min_selector_coverage=False,
        ))
        assert result.status == "planned"
        assert result.selector_diagnostics is not None
        assert result.selector_diagnostics["ok"] is True
        assert result.selector_diagnostics["selectors"]["relisted_vin"]["entities"] >= 1
