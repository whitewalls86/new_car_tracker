"""Unit tests for archiver/processors/export_ci_lake_snapshot.py (Plan 120, Phase 1)."""
from datetime import datetime, timezone

import pytest

from archiver.processors.export_ci_lake_snapshot import (
    TIER_DEFAULTS,
    SnapshotRequest,
    SnapshotRequestError,
    export_ci_lake_snapshot,
    format_coverage_failures,
    generate_snapshot_id,
    resolve_request_defaults,
    validate_request,
)
from archiver.processors.lake_snapshot_selectors import build_selector_registry

# ---------------------------------------------------------------------------
# Request validation
# ---------------------------------------------------------------------------

class TestValidateRequest:
    def test_valid_tiers_pass(self):
        for tier in ("edge", "ci", "dev", "full"):
            validate_request(SnapshotRequest(tier=tier))

    def test_invalid_tier_rejected(self):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(tier="bogus"))

    def test_valid_snapshot_id_passes(self):
        validate_request(SnapshotRequest(tier="ci", snapshot_id="adaptive-refresh-2026-07-07"))

    def test_invalid_snapshot_id_rejected(self):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(tier="ci", snapshot_id="not-a-valid-id!"))

    def test_invalid_snapshot_id_wrong_prefix_rejected(self):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(tier="ci", snapshot_id="other-2026-07-07"))

    @pytest.mark.parametrize("field_name", ["target_vins", "max_archive_mb", "max_rows"])
    def test_negative_limits_rejected(self, field_name):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(tier="ci", **{field_name: -1}))

    @pytest.mark.parametrize("field_name", ["target_vins", "max_archive_mb", "max_rows"])
    def test_zero_limits_rejected(self, field_name):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(tier="ci", **{field_name: 0}))

    @pytest.mark.parametrize("field_name", ["target_vins", "max_archive_mb", "max_rows"])
    def test_positive_limits_pass(self, field_name):
        validate_request(SnapshotRequest(tier="ci", **{field_name: 10}))

    def test_one_sided_source_window_rejected(self):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(
                tier="ci", source_window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ))

    def test_inverted_source_window_rejected(self):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(
                tier="ci",
                source_window_start=datetime(2026, 6, 1, tzinfo=timezone.utc),
                source_window_end=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ))

    def test_valid_source_window_passes(self):
        validate_request(SnapshotRequest(
            tier="ci",
            source_window_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            source_window_end=datetime(2026, 6, 1, tzinfo=timezone.utc),
        ))

    def test_null_source_window_passes(self):
        validate_request(SnapshotRequest(tier="ci"))


# ---------------------------------------------------------------------------
# Tier defaults
# ---------------------------------------------------------------------------

class TestTierDefaults:
    def test_edge_defaults(self):
        req = resolve_request_defaults(SnapshotRequest(tier="edge"))
        assert req.target_vins == 100
        assert req.max_archive_mb == 50

    def test_ci_defaults(self):
        req = resolve_request_defaults(SnapshotRequest(tier="ci"))
        assert req.target_vins == 5000
        assert req.max_archive_mb == 250

    def test_dev_defaults(self):
        req = resolve_request_defaults(SnapshotRequest(tier="dev"))
        assert req.target_vins == 25000
        assert req.max_archive_mb == 1024

    def test_full_defaults_have_no_cap(self):
        req = resolve_request_defaults(SnapshotRequest(tier="full"))
        assert req.target_vins is None
        assert req.max_archive_mb is None

    def test_explicit_values_override_tier_defaults(self):
        req = resolve_request_defaults(SnapshotRequest(tier="ci", target_vins=42, max_archive_mb=7))
        assert req.target_vins == 42
        assert req.max_archive_mb == 7

    def test_tier_defaults_table_matches_plan(self):
        assert TIER_DEFAULTS["edge"] == {"target_vins": 100, "max_archive_mb": 50}
        assert TIER_DEFAULTS["ci"] == {"target_vins": 5000, "max_archive_mb": 250}
        assert TIER_DEFAULTS["dev"] == {"target_vins": 25000, "max_archive_mb": 1024}


# ---------------------------------------------------------------------------
# Snapshot ID generation
# ---------------------------------------------------------------------------

class TestGenerateSnapshotId:
    def test_matches_expected_pattern(self):
        now = datetime(2026, 7, 7, 17, 45, 0, tzinfo=timezone.utc)
        snap_id = generate_snapshot_id("ci", now=now)
        assert snap_id == "adaptive-refresh-2026-07-07-174500"


# ---------------------------------------------------------------------------
# Coverage failure formatting
# ---------------------------------------------------------------------------

class TestFormatCoverageFailures:
    def test_no_failures_when_all_pass(self):
        coverage = {"relisted_vin": {"required": 10, "entities": 42}}
        assert format_coverage_failures(coverage) == []

    def test_failure_when_short(self):
        coverage = {"cooldown_bucket_11_plus": {"required": 1, "entities": 0}}
        failures = format_coverage_failures(coverage)
        assert len(failures) == 1
        assert "cooldown_bucket_11_plus" in failures[0]

    def test_mixed_pass_and_fail(self):
        coverage = {
            "relisted_vin": {"required": 10, "entities": 42},
            "cooldown_bucket_11_plus": {"required": 1, "entities": 0},
        }
        failures = format_coverage_failures(coverage)
        assert len(failures) == 1
        assert "relisted_vin" not in failures[0]


# ---------------------------------------------------------------------------
# Selector registry
# ---------------------------------------------------------------------------

class TestSelectorRegistry:
    def test_registry_has_unique_names(self):
        registry = build_selector_registry()
        assert len(registry) == len(set(registry.keys()))

    def test_registry_includes_planned_initial_selectors(self):
        registry = build_selector_registry()
        expected = {
            "stable_state_run",
            "state_change_run",
            "relisted_vin",
            "active_to_unlisted",
            "price_drop",
            "price_increase",
            "price_changed_7d",
            "price_changed_30d_only",
            "no_price_history",
            "detail_beats_srp",
            "srp_fallback",
            "carousel_only_or_low_priority",
            "invalid_or_null_vin",
            "benchmark_dense_make_model",
            "benchmark_sparse_make_model",
            "cooldown_blocked",
            "cooldown_incremented",
            "cooldown_bucket_3_4",
            "cooldown_bucket_5_10",
            "cooldown_bucket_11_plus",
            "fresh_recent_listing",
            "stale_listing",
        }
        assert expected.issubset(registry.keys())

    def test_each_selector_has_required_fields(self):
        registry = build_selector_registry()
        for name, selector in registry.items():
            assert selector.name == name
            assert selector.min_entities > 0
            assert selector.entity_key
            assert selector.sql


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — dry run
# ---------------------------------------------------------------------------

class TestExportDryRun:
    def test_dry_run_returns_planned_status(self):
        result = export_ci_lake_snapshot(SnapshotRequest(tier="ci", dry_run=True))
        assert result.status == "planned"
        assert result.tier == "ci"
        assert result.snapshot_id.startswith("adaptive-refresh-")
        assert result.coverage_failures == []

    def test_dry_run_uses_explicit_snapshot_id(self):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="edge", snapshot_id="adaptive-refresh-test-123", dry_run=True,
        ))
        assert result.snapshot_id == "adaptive-refresh-test-123"

    def test_dry_run_invalid_request_raises(self):
        with pytest.raises(SnapshotRequestError):
            export_ci_lake_snapshot(SnapshotRequest(tier="nope", dry_run=True))

    def test_dry_run_resolves_source_window_from_months(self):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", source_window_months=12, dry_run=True,
        ))
        assert result.source_window_start is not None
        assert result.source_window_end is not None

    def test_dry_run_no_window_when_not_requested(self):
        result = export_ci_lake_snapshot(SnapshotRequest(tier="ci", dry_run=True))
        assert result.source_window_start is None
        assert result.source_window_end is None


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — non-dry-run (deferred)
# ---------------------------------------------------------------------------

class TestExportNonDryRun:
    def test_non_dry_run_returns_not_implemented(self):
        result = export_ci_lake_snapshot(SnapshotRequest(tier="ci", dry_run=False))
        assert result.status == "not_implemented"
        assert result.archive_key is None
        assert result.manifest_key is None
