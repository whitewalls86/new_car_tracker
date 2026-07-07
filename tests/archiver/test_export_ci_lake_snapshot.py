"""Unit tests for archiver/processors/export_ci_lake_snapshot.py (Plan 120, Phase 1-2)."""
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
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
from archiver.processors.lake_snapshot_selectors import (
    RUNNABLE_SELECTORS,
    build_selector_query,
    build_selector_registry,
    run_lake_selectors,
)
from archiver.processors.lake_source_audit import audit_source_tables

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

    @pytest.mark.parametrize(
        "field_name", ["target_vins", "max_archive_mb", "max_rows", "source_window_months"]
    )
    def test_negative_limits_rejected(self, field_name):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(tier="ci", **{field_name: -1}))

    @pytest.mark.parametrize(
        "field_name", ["target_vins", "max_archive_mb", "max_rows", "source_window_months"]
    )
    def test_zero_limits_rejected(self, field_name):
        with pytest.raises(SnapshotRequestError):
            validate_request(SnapshotRequest(tier="ci", **{field_name: 0}))

    @pytest.mark.parametrize(
        "field_name", ["target_vins", "max_archive_mb", "max_rows", "source_window_months"]
    )
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


# ---------------------------------------------------------------------------
# Source audit (Plan 120, Phase 2) — local Parquet fixtures
# ---------------------------------------------------------------------------

def _write_parquet(path, **columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)


@pytest.fixture
def lake_fixture(tmp_path):
    """A tiny local Parquet lake matching the four audited source tables."""
    base = tmp_path / "lake"
    _write_parquet(
        base / "silver_normalized/observations/source=detail"
               "/obs_year=2026/obs_month=7/part-000.parquet",
        fetched_at=pa.array(
            [datetime(2026, 7, 1, tzinfo=timezone.utc), datetime(2026, 7, 3, tzinfo=timezone.utc)],
            type=pa.timestamp("us", tz="UTC"),
        ),
        vin=["VIN1", "VIN2"],
        listing_id=["L1", "L2"],
    )
    _write_parquet(
        base / "ops_normalized/price_observation_events/year=2026/month=7/part-000.parquet",
        event_at=pa.array(
            [datetime(2026, 7, 2, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC"),
        ),
        vin=["VIN1"],
        listing_id=["L1"],
    )
    _write_parquet(
        base / "ops_normalized/vin_to_listing_events/year=2026/month=7/part-000.parquet",
        event_at=pa.array(
            [datetime(2026, 7, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC"),
        ),
        vin=["VIN1"],
        listing_id=["L1"],
    )
    _write_parquet(
        base / "ops_normalized/blocked_cooldown_events/year=2026/month=7/part-000.parquet",
        event_at=pa.array(
            [datetime(2026, 7, 1, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC"),
        ),
        listing_id=["L1"],
    )
    return base


class TestAuditSourceTables:
    def test_local_fixture_counts_rows(self, lake_fixture):
        audit = audit_source_tables(base_path=str(lake_fixture))
        assert audit["ok"] is True
        assert audit["tables"]["silver_observations"]["rows"] == 2
        assert audit["tables"]["price_observation_events"]["rows"] == 1
        assert audit["tables"]["vin_to_listing_events"]["rows"] == 1
        assert audit["tables"]["blocked_cooldown_events"]["rows"] == 1

    def test_local_fixture_min_max_timestamps(self, lake_fixture):
        audit = audit_source_tables(base_path=str(lake_fixture))
        table = audit["tables"]["silver_observations"]
        assert table["min_timestamp"].startswith("2026-07-01")
        assert table["max_timestamp"].startswith("2026-07-03")

    def test_local_fixture_distinct_vin_and_listing_counts(self, lake_fixture):
        audit = audit_source_tables(base_path=str(lake_fixture))
        table = audit["tables"]["silver_observations"]
        assert table["distinct_vins"] == 2
        assert table["distinct_listing_ids"] == 2

    def test_blocked_cooldown_events_has_no_vin_column(self, lake_fixture):
        audit = audit_source_tables(base_path=str(lake_fixture))
        table = audit["tables"]["blocked_cooldown_events"]
        assert table["distinct_vins"] is None
        assert table["distinct_listing_ids"] == 1

    def test_missing_table_path_produces_table_level_error_not_crash(self, tmp_path):
        audit = audit_source_tables(base_path=str(tmp_path / "does_not_exist"))
        assert audit["ok"] is False
        assert len(audit["errors"]) == 4
        for table in audit["tables"].values():
            assert table["exists"] is False
            assert table["error"] is not None

    def test_window_filters_row_counts(self, lake_fixture):
        audit = audit_source_tables(
            base_path=str(lake_fixture),
            window_start=datetime(2026, 7, 2, tzinfo=timezone.utc),
            window_end=datetime(2026, 7, 4, tzinfo=timezone.utc),
        )
        assert audit["tables"]["silver_observations"]["rows"] == 1

    def test_window_included_in_result(self, lake_fixture):
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        end = datetime(2026, 7, 4, tzinfo=timezone.utc)
        audit = audit_source_tables(base_path=str(lake_fixture), window_start=start, window_end=end)
        assert audit["window"]["start"] == start.isoformat()
        assert audit["window"]["end"] == end.isoformat()

    def test_no_window_leaves_window_null(self, lake_fixture):
        audit = audit_source_tables(base_path=str(lake_fixture))
        assert audit["window"] == {"start": None, "end": None}


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — audit_sources (Plan 120, Phase 2)
# ---------------------------------------------------------------------------

class TestExportAuditSources:
    def test_audit_false_dry_run_remains_planned(self):
        result = export_ci_lake_snapshot(SnapshotRequest(tier="ci", dry_run=True))
        assert result.status == "planned"
        assert result.source_audit is None

    def test_audit_true_returns_source_audit(self, lake_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, audit_sources=True, source_base_path=str(lake_fixture),
        ))
        assert result.status == "audited"
        assert result.source_audit is not None
        assert result.source_audit["ok"] is True

    def test_audit_true_without_dry_run_still_audits(self, lake_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=False, audit_sources=True, source_base_path=str(lake_fixture),
        ))
        assert result.status == "audited"
        assert result.source_audit["ok"] is True

    def test_audit_true_missing_tables_sets_ok_false(self, tmp_path):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", audit_sources=True, source_base_path=str(tmp_path / "empty"),
        ))
        assert result.status == "audited"
        assert result.source_audit["ok"] is False

    def test_audit_true_with_source_window_months_sets_effective_window(self, lake_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", audit_sources=True, source_base_path=str(lake_fixture),
            source_window_months=1,
        ))
        assert result.source_window_start is not None
        assert result.source_window_end is not None
        assert result.source_audit["window"]["start"] is not None
        assert result.source_audit["window"]["end"] is not None

    def test_audit_true_with_explicit_source_window_filters_counts(self, lake_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci",
            audit_sources=True,
            source_base_path=str(lake_fixture),
            source_window_start=datetime(2026, 7, 2, tzinfo=timezone.utc),
            source_window_end=datetime(2026, 7, 4, tzinfo=timezone.utc),
        ))
        assert result.source_audit["tables"]["silver_observations"]["rows"] == 1


# ---------------------------------------------------------------------------
# Selector execution (Plan 120, Phase 2) — local Parquet fixtures
# ---------------------------------------------------------------------------

def _ts(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def selector_fixture(tmp_path):
    """A tiny local lake exercising each of the five Phase 2 selectors once."""
    base = tmp_path / "lake"

    _write_parquet(
        base / "ops_normalized/vin_to_listing_events/year=2026/month=7/part-000.parquet",
        event_id=[1, 2, 3],
        vin=["VIN_RELISTED", "VIN_RELISTED", "VIN_SINGLE"],
        listing_id=["L1", "L2", "L3"],
        artifact_id=[101, 102, 103],
        event_type=["mapped", "remapped", "mapped"],
        previous_listing_id=[None, "L1", None],
        event_at=pa.array(
            [_ts(2026, 7, 1), _ts(2026, 7, 2), _ts(2026, 7, 1)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    )
    _write_parquet(
        base / "ops_normalized/price_observation_events/year=2026/month=7/part-000.parquet",
        event_id=[1, 2, 3, 4],
        listing_id=["L1", "L1", "L1", "L4"],
        vin=["VIN_RELISTED", "VIN_RELISTED", "VIN_RELISTED", "VIN_OTHER"],
        artifact_id=[101, 101, 101, 104],
        price=pa.array([20000, 19000, 21000, 15000], type=pa.int32()),
        event_type=["upserted", "upserted", "upserted", "upserted"],
        event_at=pa.array(
            [_ts(2026, 7, 1), _ts(2026, 7, 2), _ts(2026, 7, 3), _ts(2026, 7, 1)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    )
    _write_parquet(
        base / "ops_normalized/blocked_cooldown_events/year=2026/month=7/part-000.parquet",
        event_id=[1, 2, 3],
        listing_id=["L1", "L1", "L5"],
        event_type=["blocked", "blocked", "blocked"],
        num_of_attempts=pa.array([1, 2, 1], type=pa.int32()),
        event_at=pa.array(
            [_ts(2026, 7, 1), _ts(2026, 7, 2), _ts(2026, 7, 1)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    )
    _write_parquet(
        base / "silver_normalized/observations/source=detail"
               "/obs_year=2026/obs_month=7/part-000.parquet",
        vin=["VIN_STABLE", "VIN_STABLE", "VIN_CHANGE", "VIN_CHANGE"],
        listing_id=["L6", "L6", "L7", "L7"],
        artifact_id=[106, 106, 107, 107],
        fetched_at=pa.array(
            [_ts(2026, 7, 1), _ts(2026, 7, 2), _ts(2026, 7, 1), _ts(2026, 7, 2)],
            type=pa.timestamp("us", tz="UTC"),
        ),
        price=pa.array([10000, 10000, 10000, 9000], type=pa.int32()),
        mileage=pa.array([5000, 5000, 5000, 5000], type=pa.int32()),
        listing_state=["active", "active", "active", "active"],
    )
    return base


class TestBuildSelectorQuery:
    def test_runnable_selectors_match_expected_five(self):
        assert set(RUNNABLE_SELECTORS) == {
            "relisted_vin", "price_drop", "price_increase",
            "cooldown_incremented", "stable_state_run",
        }

    def test_unimplemented_selector_raises(self):
        with pytest.raises(ValueError):
            build_selector_query("state_change_run", "s3://bronze/whatever/**/*.parquet")

    def test_query_embeds_resolved_path(self):
        sql, _ = build_selector_query("relisted_vin", "s3://bronze/foo/**/*.parquet")
        assert "s3://bronze/foo/**/*.parquet" in sql


class TestRunLakeSelectors:
    def test_relisted_vin_finds_multi_listing_vin(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        selector = diagnostics["selectors"]["relisted_vin"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["VIN_RELISTED"]
        assert selector["candidate_rows"] == 2
        assert selector["error"] is None

    def test_price_drop_finds_consecutive_lower_price(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        selector = diagnostics["selectors"]["price_drop"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["L1"]
        assert selector["candidate_rows"] == 1

    def test_price_increase_finds_consecutive_higher_price(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        selector = diagnostics["selectors"]["price_increase"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["L1"]
        assert selector["candidate_rows"] == 1

    def test_cooldown_incremented_finds_repeated_attempt(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        selector = diagnostics["selectors"]["cooldown_incremented"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["L1"]
        assert selector["candidate_rows"] == 1

    def test_stable_state_run_finds_unchanged_fingerprint(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        selector = diagnostics["selectors"]["stable_state_run"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["VIN_STABLE"]
        assert selector["candidate_rows"] == 1

    def test_all_five_selectors_run_by_default(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        assert set(diagnostics["selectors"].keys()) == set(RUNNABLE_SELECTORS)
        assert diagnostics["ok"] is True
        assert diagnostics["errors"] == []

    def test_required_and_status_reflect_registry_minimums(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        selector = diagnostics["selectors"]["relisted_vin"]
        assert selector["required"] == 10
        assert selector["status"] == "fail"  # only 1 entity found, min is 10

    def test_window_filters_candidates(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture),
            window_start=datetime(2026, 7, 3, tzinfo=timezone.utc),
            window_end=datetime(2026, 7, 5, tzinfo=timezone.utc),
        )
        selector = diagnostics["selectors"]["relisted_vin"]
        assert selector["entities"] == 0
        assert selector["candidate_rows"] == 0

    def test_missing_table_produces_selector_level_error_not_crash(self, tmp_path):
        diagnostics = run_lake_selectors(base_path=str(tmp_path / "does_not_exist"))
        assert diagnostics["ok"] is False
        assert len(diagnostics["errors"]) == len(RUNNABLE_SELECTORS)
        for selector in diagnostics["selectors"].values():
            assert selector["error"] is not None

    def test_names_param_limits_selectors_run(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture), names=["price_drop"])
        assert set(diagnostics["selectors"].keys()) == {"price_drop"}


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — run_selectors (Plan 120, Phase 2)
# ---------------------------------------------------------------------------

class TestExportRunSelectors:
    def test_dry_run_without_run_selectors_stays_cheap(self):
        result = export_ci_lake_snapshot(SnapshotRequest(tier="ci", dry_run=True))
        assert result.status == "planned"
        assert result.selector_diagnostics is None

    def test_dry_run_with_run_selectors_returns_diagnostics(self, selector_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True,
            source_base_path=str(selector_fixture),
        ))
        assert result.status == "planned"
        assert result.selector_diagnostics is not None
        assert result.selector_diagnostics["selectors"]["relisted_vin"]["entities"] == 1

    def test_non_dry_run_ignores_run_selectors(self, selector_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=False, run_selectors=True,
            source_base_path=str(selector_fixture),
        ))
        assert result.status == "not_implemented"
        assert result.selector_diagnostics is None

    def test_min_selector_coverage_true_reports_coverage_failures(self, selector_fixture):
        # Fixture only ever provides 1 candidate entity per selector, well below
        # every selector's min_entities, so every selector should be reported short.
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, min_selector_coverage=True,
            source_base_path=str(selector_fixture),
        ))
        assert len(result.coverage_failures) == 5
        assert any("relisted_vin" in f for f in result.coverage_failures)

    def test_min_selector_coverage_false_skips_coverage_failures(self, selector_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, min_selector_coverage=False,
            source_base_path=str(selector_fixture),
        ))
        assert result.coverage_failures == []
