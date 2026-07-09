"""Unit tests for archiver/processors/export_ci_lake_snapshot.py (Plan 120).

Pure-Python logic only — request validation, tier defaults, snapshot-id
generation, coverage-failure formatting, selector-registry/config shape, cheap
dry-run planning, and cohort *allocation* logic that does not read data
(dedup/bucketing with an explicit candidate set).

Anything that executes real selector/cohort SQL against Parquet lives in the
integration suite (tests/integration/archiver/test_lake_snapshot_selectors.py
and test_lake_snapshot_cohort.py), which runs against real MinIO in CI. Those
tests catch schema drift; re-implementing selector SQL over hand-built local
fixtures here would only prove the SQL agrees with itself.
"""
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
from archiver.processors.lake_snapshot_cohort import (
    CandidateSet,
    allocate_cohort,
    candidate_sets_to_selector_diagnostics,
    collect_selector_candidates,
)
from archiver.processors.lake_snapshot_export import MaterializationResult
from archiver.processors.lake_snapshot_selectors import (
    RUNNABLE_SELECTORS,
    build_selector_query,
    build_selector_registry,
)

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

    def test_no_selector_uses_placeholder_todo_sql(self):
        """Gate B requirement: every registered selector must be runnable —
        none may be left with `-- TODO: implement selector SQL` placeholder
        text in RUNNABLE_SELECTORS."""
        registry = build_selector_registry()
        for name in RUNNABLE_SELECTORS:
            assert "TODO" not in registry[name].sql

    def test_runnable_selectors_cover_the_full_registry(self):
        registry = build_selector_registry()
        assert set(RUNNABLE_SELECTORS) == set(registry.keys())

    def test_selector_sql_loads_from_sql_files(self):
        """Selector SQL is loaded from archiver/sql/lake_snapshot_selectors/*.sql
        rather than embedded as Python string constants (Plan 120 Gate B
        refactor) — every runnable selector's template must resolve to a
        readable file, not an empty/placeholder string."""
        from archiver.processors.lake_snapshot_selector_config import DEFAULT_SQL_DIR
        from archiver.processors.lake_snapshot_selectors import _SELECTOR_CONFIGS, _q

        assert DEFAULT_SQL_DIR.is_dir()
        registry = build_selector_registry()
        for name in RUNNABLE_SELECTORS:
            config = _SELECTOR_CONFIGS[name]
            assert (DEFAULT_SQL_DIR / f"{config.sql_template}.sql").is_file()
            assert registry[name].sql.strip()
            assert registry[name].sql == _q(config.sql_template)


# ---------------------------------------------------------------------------
# Selector config validation (archiver/config/lake_snapshot_selectors.yml)
# ---------------------------------------------------------------------------

class TestSelectorConfigValidation:
    """Validates archiver/config/lake_snapshot_selectors.yml, the single
    source of truth for selector metadata (Plan 120 Gate B config refactor)."""

    def _load(self, tmp_path, yaml_text):
        from archiver.processors.lake_snapshot_selector_config import (
            DEFAULT_SQL_DIR,
            load_selector_configs,
        )

        config_path = tmp_path / "selectors.yml"
        config_path.write_text(yaml_text)
        return load_selector_configs(config_path, DEFAULT_SQL_DIR)

    def test_real_config_loads_without_error(self):
        from archiver.processors.lake_snapshot_selector_config import load_selector_configs

        configs = load_selector_configs()
        assert configs

    def test_missing_top_level_selectors_key_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="top-level 'selectors' mapping"):
            self._load(tmp_path, "not_selectors: {}\n")

    def test_top_level_selectors_must_be_a_mapping(self, tmp_path):
        with pytest.raises(ValueError, match="top-level 'selectors' mapping"):
            self._load(tmp_path, "selectors: []\n")

    def test_missing_required_key_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="missing required key 'entity_key'"):
            self._load(tmp_path, """
selectors:
  relisted_vin:
    min_entities: 10
    source_table: vin_to_listing_events
    sql_template: relisted_vin
    timestamp_column: event_at
    description: test
""")

    def test_negative_min_entities_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="non-negative integer"):
            self._load(tmp_path, """
selectors:
  relisted_vin:
    min_entities: -1
    entity_key: vin
    source_table: vin_to_listing_events
    sql_template: relisted_vin
    timestamp_column: event_at
    description: test
""")

    def test_non_integer_min_entities_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="non-negative integer"):
            self._load(tmp_path, """
selectors:
  relisted_vin:
    min_entities: "ten"
    entity_key: vin
    source_table: vin_to_listing_events
    sql_template: relisted_vin
    timestamp_column: event_at
    description: test
""")

    def test_non_list_base_filters_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="key 'base_filters' must be a list of strings"):
            self._load(tmp_path, """
selectors:
  relisted_vin:
    min_entities: 10
    entity_key: vin
    source_table: vin_to_listing_events
    sql_template: relisted_vin
    timestamp_column: event_at
    base_filters: "not a list"
    description: test
""")

    def test_non_list_extra_source_tables_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="key 'extra_source_tables' must be a list of strings"):
            self._load(tmp_path, """
selectors:
  no_price_history:
    min_entities: 10
    entity_key: vin
    source_table: silver_observations
    sql_template: no_price_history
    timestamp_column: fetched_at
    extra_source_tables: "not a list"
    description: test
""")

    def test_invalid_window_anchor_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="window_anchor must be one of"):
            self._load(tmp_path, """
selectors:
  price_changed_7d:
    min_entities: 25
    entity_key: listing_id
    source_table: price_observation_events
    sql_template: price_changed_7d
    timestamp_column: event_at
    window_anchor: window_start
    description: test
""")

    def test_negative_lookback_days_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="lookback_days must be a positive integer"):
            self._load(tmp_path, """
selectors:
  stale_listing:
    min_entities: 25
    entity_key: listing_id
    source_table: silver_observations
    sql_template: stale_listing
    timestamp_column: fetched_at
    window_anchor: window_end
    lookback_days: 0
    description: test
""")

    def test_non_integer_lookback_days_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="lookback_days must be a positive integer"):
            self._load(tmp_path, """
selectors:
  stale_listing:
    min_entities: 25
    entity_key: listing_id
    source_table: silver_observations
    sql_template: stale_listing
    timestamp_column: fetched_at
    window_anchor: window_end
    lookback_days: "sixty"
    description: test
""")

    def test_unknown_source_table_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="unknown source table"):
            self._load(tmp_path, """
selectors:
  relisted_vin:
    min_entities: 10
    entity_key: vin
    source_table: not_a_real_table
    sql_template: relisted_vin
    timestamp_column: event_at
    description: test
""")

    def test_unknown_extra_source_table_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="unknown source table"):
            self._load(tmp_path, """
selectors:
  no_price_history:
    min_entities: 10
    entity_key: vin
    source_table: silver_observations
    sql_template: no_price_history
    timestamp_column: fetched_at
    extra_source_tables:
      - not_a_real_table
    description: test
""")

    def test_unknown_sql_template_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="no matching .sql file"):
            self._load(tmp_path, """
selectors:
  relisted_vin:
    min_entities: 10
    entity_key: vin
    source_table: vin_to_listing_events
    sql_template: does_not_exist
    timestamp_column: event_at
    description: test
""")

    def test_runnable_selectors_exactly_matches_yaml_keys(self):
        import yaml

        from archiver.processors.lake_snapshot_selector_config import DEFAULT_CONFIG_PATH

        raw = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())
        assert set(RUNNABLE_SELECTORS) == set(raw["selectors"].keys())

    def test_registry_returns_one_selector_per_yaml_entry(self):
        import yaml

        from archiver.processors.lake_snapshot_selector_config import DEFAULT_CONFIG_PATH

        raw = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text())
        registry = build_selector_registry()
        assert set(registry.keys()) == set(raw["selectors"].keys())
        assert len(registry) == len(raw["selectors"])


# ---------------------------------------------------------------------------
# build_selector_query — SQL string shape (no data)
# ---------------------------------------------------------------------------

class TestBuildSelectorQuery:
    def test_runnable_selectors_include_every_registered_selector(self):
        registry = build_selector_registry()
        assert set(RUNNABLE_SELECTORS) == set(registry.keys())

    def test_unknown_selector_raises(self):
        with pytest.raises(ValueError):
            build_selector_query("not_a_real_selector", "s3://bronze/whatever/**/*.parquet")

    def test_query_embeds_resolved_path(self):
        sql, _ = build_selector_query("relisted_vin", "s3://bronze/foo/**/*.parquet")
        assert "s3://bronze/foo/**/*.parquet" in sql


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — cheap dry run (no data read)
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

    def test_dry_run_without_run_selectors_stays_cheap(self):
        result = export_ci_lake_snapshot(SnapshotRequest(tier="ci", dry_run=True))
        assert result.status == "planned"
        assert result.selector_diagnostics is None
        assert result.cohort_diagnostics is None


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — avoid duplicate selector scans (Plan 120 Gate C.5)
# ---------------------------------------------------------------------------

class TestExportSelectorCohortDedup:
    """When both run_selectors and build_cohort are requested, the exporter
    must collect selector candidates once and reuse them for both selector
    diagnostics and cohort allocation, instead of scanning twice."""

    def test_run_selectors_and_build_cohort_scans_candidates_once(self, mocker):
        mock_collect = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.collect_all_selector_candidates",
            return_value={},
        )
        mock_run_selectors = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.run_lake_selectors"
        )
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
            return_value={"selectors": {}, "errors": [], "ok": True},
        )
        mocker.patch("archiver.processors.export_ci_lake_snapshot.open_duckdb_connection")
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        fake_cohort = mocker.Mock()
        fake_cohort.diagnostics = {}
        fake_cohort.seed_vins = fake_cohort.closed_vins = fake_cohort.listing_ids = set()
        fake_cohort.artifact_ids = set()
        fake_cohort.artifact_row_keys = set()
        mock_build_cohort = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.build_snapshot_cohort",
            return_value=fake_cohort,
        )

        export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
        ))

        assert mock_collect.call_count == 1
        assert not mock_run_selectors.called
        # The precomputed candidate_sets must be threaded into cohort building.
        assert mock_build_cohort.call_args.kwargs["candidate_sets"] == {}

    def test_run_selectors_without_build_cohort_uses_run_lake_selectors(self, mocker):
        mock_collect = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.collect_all_selector_candidates"
        )
        mock_run_selectors = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.run_lake_selectors",
            return_value={"selectors": {}, "errors": [], "ok": True},
        )

        export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=False,
        ))

        assert mock_run_selectors.call_count == 1
        assert not mock_collect.called


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — persisted planning cache (Plan 120 Gate C.75)
# ---------------------------------------------------------------------------

def _mock_heavy_path(mocker, candidate_sets=None):
    """Mock the heavy selector-collection + cohort-closure path so tests can
    exercise the surrounding planning/export logic without a real DuckDB/
    MinIO connection."""
    mocker.patch("archiver.processors.export_ci_lake_snapshot.open_duckdb_connection")
    mock_collect = mocker.patch(
        "archiver.processors.export_ci_lake_snapshot.collect_all_selector_candidates",
        return_value=candidate_sets if candidate_sets is not None else {},
    )
    mocker.patch(
        "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
        return_value={"selectors": {}, "errors": [], "ok": True},
    )
    fake_cohort = mocker.Mock()
    fake_cohort.diagnostics = {"closed_vins": 1}
    fake_cohort.seed_vins = fake_cohort.closed_vins = fake_cohort.listing_ids = {"x"}
    fake_cohort.artifact_ids = {1}
    fake_cohort.artifact_row_keys = set()
    mock_build_cohort = mocker.patch(
        "archiver.processors.export_ci_lake_snapshot.build_snapshot_cohort",
        return_value=fake_cohort,
    )
    return mock_collect, mock_build_cohort


class TestExportPlanningCache:
    """The heavy planning path (dry_run + run_selectors + build_cohort) reads/
    writes a persisted planning cache instead of always rescanning the lake.
    Cache reuse is always explicit (default flags never reuse)."""

    def test_query_and_response_use_the_resolved_planning_window(self, mocker):
        """The window actually passed to selector/cohort collection (and
        reported in the response) must be whatever resolve_planning_window
        resolves to — not the raw, unbucketed window_start/window_end.
        Otherwise two calls in the same weekly bucket could share a
        fingerprint while querying different exact windows, and a cache hit
        would silently serve a cohort computed over the wrong window."""
        from datetime import datetime, timezone

        mock_collect, mock_build_cohort = _mock_heavy_path(mocker)
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_planning_cache",
            return_value=None,
        )
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")

        bucketed_start = datetime(2026, 6, 6, tzinfo=timezone.utc)
        bucketed_end = datetime(2026, 7, 6, tzinfo=timezone.utc)
        mock_resolve = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.resolve_planning_window",
            return_value=(bucketed_start, bucketed_end),
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            source_window_months=1, planning_cache_bucket_grain="week",
        ))

        assert mock_resolve.called
        assert mock_collect.call_args.kwargs["window_start"] == bucketed_start
        assert mock_collect.call_args.kwargs["window_end"] == bucketed_end
        assert mock_build_cohort.call_args[0][2] == bucketed_start
        assert mock_build_cohort.call_args[0][3] == bucketed_end
        assert result.source_window_start == bucketed_start.isoformat()
        assert result.source_window_end == bucketed_end.isoformat()

    def test_single_now_sample_shared_across_window_resolution(self, mocker):
        """resolve_source_window() and resolve_planning_window() must be
        given the exact same 'now' — sampling datetime.now() independently
        in each could straddle a UTC day/week boundary and bucket the two
        resolutions differently."""
        _mock_heavy_path(mocker)
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_planning_cache",
            return_value=None,
        )
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mock_resolve_source_window = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.resolve_source_window",
            return_value=(None, None),
        )
        mock_resolve_planning_window = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.resolve_planning_window",
            return_value=(None, None),
        )

        export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            source_window_months=1, planning_cache_bucket_grain="week",
        ))

        source_window_now = mock_resolve_source_window.call_args.kwargs["now"]
        planning_window_now = mock_resolve_planning_window.call_args.kwargs["now"]
        assert source_window_now is not None
        assert source_window_now == planning_window_now

    def test_default_computes_and_writes_cache(self, mocker):
        _mock_heavy_path(mocker)
        mock_load = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_planning_cache"
        )
        mock_write = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_planning_cache"
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
        ))

        assert not mock_load.called
        assert mock_write.call_count == 1
        assert result.planning_cache_hit is False
        assert result.planning_cache_action == "computed"
        assert result.planning_cache_key is not None
        assert result.planning_cache_path is not None

    def test_default_logs_fingerprint_and_write_start(self, mocker, caplog):
        """Progress logging (Plan 120 worker visibility): a fingerprint
        compute log must precede the cache lookup, and a write-start log must
        precede the (mocked) write itself."""
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.load_planning_cache")
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")

        with caplog.at_level("INFO", logger="archiver"):
            export_ci_lake_snapshot(SnapshotRequest(
                tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            ))

        messages = [r.message for r in caplog.records]
        assert any("planning_cache fingerprint compute start" in m for m in messages)
        assert any(
            "planning_cache write start" in m and "fingerprint=" in m for m in messages
        )

    def test_reuse_hit_skips_write_start_log(self, mocker, caplog):
        mock_collect, mock_build_cohort = _mock_heavy_path(mocker)
        cached_artifact = {
            "cache_schema_version": 1,
            "selector_diagnostics": {"selectors": {}, "errors": [], "ok": True},
            "cohort_diagnostics": {"cached": True},
            "seed_vins": ["v1", "v2", "v3", "v4", "v5", "v6", "v7"],
            "closed_vins": ["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"],
            "listing_ids": ["l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8", "l9"],
            "artifact_ids": list(range(10)),
            "artifact_row_keys": [],
            "seed_vin_count": 7,
            "closed_vin_count": 8,
            "listing_count": 9,
            "artifact_count": 10,
        }
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_planning_cache",
            return_value=cached_artifact,
        )
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")

        with caplog.at_level("INFO", logger="archiver"):
            export_ci_lake_snapshot(SnapshotRequest(
                tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
                reuse_planning_cache=True,
            ))

        messages = [r.message for r in caplog.records]
        assert not mock_collect.called
        assert not mock_build_cohort.called
        assert any("planning_cache hit" in m for m in messages)
        assert not any("planning_cache write start" in m for m in messages)

    def test_reuse_loads_existing_cache_and_skips_computation(self, mocker):
        mock_collect, mock_build_cohort = _mock_heavy_path(mocker)
        cached_artifact = {
            "cache_schema_version": 1,
            "selector_diagnostics": {"selectors": {}, "errors": [], "ok": True},
            "cohort_diagnostics": {"cached": True},
            "seed_vins": ["v1", "v2", "v3", "v4", "v5", "v6", "v7"],
            "closed_vins": ["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"],
            "listing_ids": ["l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8", "l9"],
            "artifact_ids": list(range(10)),
            "artifact_row_keys": [],
            "seed_vin_count": 7,
            "closed_vin_count": 8,
            "listing_count": 9,
            "artifact_count": 10,
        }
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_planning_cache",
            return_value=cached_artifact,
        )
        mock_write = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_planning_cache"
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            reuse_planning_cache=True,
        ))

        assert not mock_collect.called
        assert not mock_build_cohort.called
        assert not mock_write.called
        assert result.planning_cache_hit is True
        assert result.planning_cache_action == "reused"
        assert result.seed_vin_count == 7
        assert result.closed_vin_count == 8
        assert result.listing_count == 9
        assert result.artifact_count == 10
        assert result.cohort_diagnostics == {"cached": True}

    def test_refresh_ignores_existing_cache_and_recomputes(self, mocker):
        mock_collect, mock_build_cohort = _mock_heavy_path(mocker)
        mock_load = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_planning_cache"
        )
        mock_write = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_planning_cache"
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            refresh_planning_cache=True,
        ))

        # refresh never looks at the existing cache — it recomputes unconditionally.
        assert not mock_load.called
        assert mock_collect.call_count == 1
        assert mock_build_cohort.call_count == 1
        assert mock_write.call_count == 1
        assert result.planning_cache_hit is False
        assert result.planning_cache_action == "refreshed"

    def test_reuse_miss_computes_and_writes(self, mocker):
        mock_collect, mock_build_cohort = _mock_heavy_path(mocker)
        mock_load = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_planning_cache",
            return_value=None,
        )
        mock_write = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_planning_cache"
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            reuse_planning_cache=True,
        ))

        assert mock_load.call_count == 1
        assert mock_collect.call_count == 1
        assert mock_build_cohort.call_count == 1
        assert mock_write.call_count == 1
        assert result.planning_cache_hit is False
        assert result.planning_cache_action == "computed"

    def test_reuse_and_refresh_together_fails_validation(self):
        with pytest.raises(SnapshotRequestError):
            export_ci_lake_snapshot(SnapshotRequest(
                tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
                reuse_planning_cache=True, refresh_planning_cache=True,
            ))

    def test_invalid_bucket_grain_fails_validation(self):
        with pytest.raises(SnapshotRequestError):
            export_ci_lake_snapshot(SnapshotRequest(
                tier="ci", dry_run=True, planning_cache_bucket_grain="month",
            ))

    def test_failed_compute_does_not_write_cache(self, mocker):
        mocker.patch("archiver.processors.export_ci_lake_snapshot.open_duckdb_connection")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.collect_all_selector_candidates",
            side_effect=RuntimeError("boom"),
        )
        mock_write = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_planning_cache"
        )

        with pytest.raises(RuntimeError):
            export_ci_lake_snapshot(SnapshotRequest(
                tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            ))

        assert not mock_write.called


# ---------------------------------------------------------------------------
# CLI argument parsing — planning cache flags (Plan 120 Gate C.75)
# ---------------------------------------------------------------------------

class TestParseArgsPlanningCache:
    def test_defaults(self):
        from archiver.processors.export_ci_lake_snapshot import _parse_args

        args = _parse_args(["--tier", "ci"])
        assert args.reuse_planning_cache is False
        assert args.refresh_planning_cache is False
        assert args.planning_cache_bucket_grain == "week"
        assert args.planning_cache_prefix == "snapshot_planning_cache"

    def test_explicit_flags(self):
        from archiver.processors.export_ci_lake_snapshot import _parse_args

        args = _parse_args([
            "--tier", "ci",
            "--reuse-planning-cache",
            "--planning-cache-bucket-grain", "day",
            "--planning-cache-prefix", "custom_prefix",
        ])
        assert args.reuse_planning_cache is True
        assert args.planning_cache_bucket_grain == "day"
        assert args.planning_cache_prefix == "custom_prefix"

    def test_refresh_flag(self):
        from archiver.processors.export_ci_lake_snapshot import _parse_args

        args = _parse_args(["--tier", "ci", "--refresh-planning-cache"])
        assert args.refresh_planning_cache is True


class TestParseArgsRequireSelectorCoverage:
    def test_defaults_to_false(self):
        from archiver.processors.export_ci_lake_snapshot import _parse_args

        args = _parse_args(["--tier", "ci"])
        assert args.require_selector_coverage is False

    def test_flag_sets_true(self):
        from archiver.processors.export_ci_lake_snapshot import _parse_args

        args = _parse_args(["--tier", "ci", "--require-selector-coverage"])
        assert args.require_selector_coverage is True

    def test_main_threads_flag_into_request(self, mocker):
        mock_export = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.export_ci_lake_snapshot"
        )
        mock_export.return_value.to_dict.return_value = {}
        mocker.patch("archiver.processors.export_ci_lake_snapshot.configure_logging")

        from archiver.processors.export_ci_lake_snapshot import main

        main(["--tier", "ci", "--require-selector-coverage"])

        request = mock_export.call_args[0][0]
        assert request.require_selector_coverage is True


# ---------------------------------------------------------------------------
# CLI main() — logging must be configured (Plan 120 worker visibility)
# ---------------------------------------------------------------------------

class TestMainLogging:
    def test_main_configures_logging_before_running(self, mocker):
        """The snapshot-worker container runs this module's main() directly
        (not archiver.app), so it never hits archiver.app's configure_logging()
        call. Without configuring logging here, all the new progress
        logger.info(...) calls are silently dropped by the default root
        WARNING level and docker logs -f stays quiet."""
        mock_configure = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.configure_logging"
        )
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.export_ci_lake_snapshot",
            return_value=mocker.Mock(to_dict=lambda: {}),
        )
        from archiver.processors.export_ci_lake_snapshot import main

        main(["--tier", "ci", "--dry-run"])

        mock_configure.assert_called_once_with()


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — dry-run selector-failure enforcement
# ---------------------------------------------------------------------------

class TestExportDryRunSelectorFailures:
    """An audit dry run (--dry-run --run-selectors --build-cohort
    --require-selector-coverage) must be able to catch the same selector
    failures a real export would — a dry run is often exactly how an
    operator validates a planning cache before committing to a real,
    expensive export, so it can't silently report status="planned" over a
    selector error or (when requested) an unenforced coverage shortfall."""

    def test_dry_run_selector_errors_return_export_failed(self, mocker):
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
            return_value={"selectors": {}, "errors": ["relisted_vin: boom"], "ok": False},
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
        ))

        assert result.status == "export_failed"
        assert any("boom" in f for f in result.coverage_failures)

    def test_dry_run_require_selector_coverage_true_blocks(self, mocker):
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
            return_value={
                "selectors": {"cooldown_bucket_11_plus": {"required": 1, "entities": 0}},
                "errors": [], "ok": True,
            },
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
            require_selector_coverage=True,
        ))

        assert result.status == "coverage_failed"
        assert result.coverage_failures

    def test_dry_run_default_does_not_block_on_coverage_failures(self, mocker):
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
            return_value={
                "selectors": {"cooldown_bucket_11_plus": {"required": 1, "entities": 0}},
                "errors": [], "ok": True,
            },
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=True,
        ))

        assert result.status == "planned"
        assert result.coverage_failures  # preserved as a warning

    def test_dry_run_run_selectors_only_errors_return_export_failed(self, mocker):
        """The run_selectors-only (no build_cohort) dry-run branch must also
        enforce, not just the heavy run_selectors+build_cohort path."""
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.run_lake_selectors",
            return_value={"selectors": {}, "errors": ["relisted_vin: boom"], "ok": False},
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, build_cohort=False,
        ))

        assert result.status == "export_failed"
        assert any("boom" in f for f in result.coverage_failures)


# ---------------------------------------------------------------------------
# export_ci_lake_snapshot — non-dry-run (deferred)
# ---------------------------------------------------------------------------

class TestExportNonDryRun:
    """A real export always needs a closed cohort, so it always runs the same
    heavy planning as dry_run+run_selectors+build_cohort — regardless of
    those flags — then materializes filtered Parquet under an
    export-fingerprint-addressed prefix (Gate D)."""

    def _mock_materialize(self, mocker, tables=None, data_path="data/path", ok=True):
        tables = tables if tables is not None else {}
        result = MaterializationResult(
            tables=tables,
            generation_id="gen1",
            data_path=data_path if ok else None,
        )
        return mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.materialize_filtered_tables",
            return_value=result,
        )

    def test_non_dry_run_exports_and_writes_manifest(self, mocker):
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mock_materialize = self._mock_materialize(mocker)
        mock_write_manifest = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_export_manifest",
            return_value=True,
        )
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_export_manifest",
            return_value=None,
        )

        result = export_ci_lake_snapshot(
            SnapshotRequest(tier="ci", dry_run=False, build_cohort=True)
        )

        assert result.status == "exported"
        assert result.export_fingerprint is not None
        assert result.export_cache_hit is False
        assert result.export_cache_action == "computed"
        assert result.manifest_key is not None
        assert result.materialized_snapshot_path == "data/path"
        assert mock_materialize.called
        assert mock_write_manifest.called

    def test_non_dry_run_ignores_run_selectors_flag(self, mocker):
        """run_selectors only gates dry-run diagnostic scope — a real export
        runs the same planning whether or not it's set."""
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        self._mock_materialize(mocker)
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_export_manifest",
            return_value=True,
        )
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_export_manifest",
            return_value=None,
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=False, run_selectors=False, build_cohort=True,
        ))
        assert result.status == "exported"

    def test_non_dry_run_reuses_export_cache(self, mocker):
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mock_materialize = self._mock_materialize(mocker)
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_export_manifest",
            return_value={
                "export_cache_schema_version": 2, "export_fingerprint": "x",
                "data_path": "cached/data/path",
            },
        )
        mock_write_manifest = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_export_manifest"
        )

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=False, build_cohort=True, reuse_export_cache=True,
        ))

        assert result.status == "exported"
        assert result.export_cache_hit is True
        assert result.export_cache_action == "reused"
        assert result.materialized_snapshot_path == "cached/data/path"
        assert not mock_materialize.called
        assert not mock_write_manifest.called

    def test_non_dry_run_table_error_returns_export_failed(self, mocker):
        """A table read error must surface as export_failed and must never
        write/publish a manifest for the partial result."""
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        self._mock_materialize(
            mocker,
            tables={"silver_observations": {"error": "boom", "rows": 0}},
            ok=False,
        )
        mock_write_manifest = mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_export_manifest"
        )
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_export_manifest",
            return_value=None,
        )

        result = export_ci_lake_snapshot(
            SnapshotRequest(tier="ci", dry_run=False, build_cohort=True)
        )

        assert result.status == "export_failed"
        assert any("boom" in f for f in result.coverage_failures)
        assert result.materialized_snapshot_path is None
        assert not mock_write_manifest.called

    def test_non_dry_run_manifest_write_failure_returns_export_failed(self, mocker):
        """The manifest write is the actual publish step — if it fails, the
        export must not be reported as exported even though materialization
        itself succeeded."""
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        self._mock_materialize(mocker)
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.write_export_manifest",
            return_value=False,
        )
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_export_manifest",
            return_value=None,
        )

        result = export_ci_lake_snapshot(
            SnapshotRequest(tier="ci", dry_run=False, build_cohort=True)
        )

        assert result.status == "export_failed"
        assert result.materialized_snapshot_path is None

    def test_non_dry_run_require_selector_coverage_true_blocks(self, mocker):
        """require_selector_coverage=True is the explicit opt-in strict/audit
        mode — a coverage shortfall blocks the export before materialization."""
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
            return_value={
                "selectors": {
                    "cooldown_bucket_11_plus": {"required": 1, "entities": 0},
                },
                "errors": [], "ok": True,
            },
        )
        mock_materialize = self._mock_materialize(mocker)

        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=False, build_cohort=True, require_selector_coverage=True,
        ))

        assert result.status == "coverage_failed"
        assert result.coverage_failures
        assert not mock_materialize.called

    def test_non_dry_run_default_does_not_block_on_coverage_failures(self, mocker):
        """Coverage shortfalls are non-blocking by default (Plan 120 selector
        policy correction) — the export proceeds, but the shortfall detail
        is still preserved on the result/manifest as a warning."""
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
            return_value={
                "selectors": {
                    "cooldown_bucket_11_plus": {"required": 1, "entities": 0},
                },
                "errors": [], "ok": True,
            },
        )
        self._mock_materialize(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_export_manifest")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.load_export_manifest",
            return_value=None,
        )

        result = export_ci_lake_snapshot(
            SnapshotRequest(tier="ci", dry_run=False, build_cohort=True)
        )
        assert result.status == "exported"
        assert result.coverage_failures  # preserved as a warning, not cleared

    def test_non_dry_run_selector_errors_return_export_failed(self, mocker):
        """Unlike a coverage shortfall, a selector query/source error is
        always a hard failure, regardless of require_selector_coverage."""
        _mock_heavy_path(mocker)
        mocker.patch("archiver.processors.export_ci_lake_snapshot.write_planning_cache")
        mocker.patch(
            "archiver.processors.export_ci_lake_snapshot.candidate_sets_to_selector_diagnostics",
            return_value={
                "selectors": {},
                "errors": ["relisted_vin: boom"], "ok": False,
            },
        )
        mock_materialize = self._mock_materialize(mocker)

        result = export_ci_lake_snapshot(
            SnapshotRequest(tier="ci", dry_run=False, build_cohort=True)
        )

        assert result.status == "export_failed"
        assert any("boom" in f for f in result.coverage_failures)
        assert not mock_materialize.called


# ---------------------------------------------------------------------------
# Cohort allocation — pure logic (explicit candidate sets, no data read)
# ---------------------------------------------------------------------------

def _candidate(name, entity_key, required, entities):
    entities = tuple(entities)
    return CandidateSet(
        selector_name=name,
        entity_key=entity_key,
        required=required,
        entities=entities,
        candidate_rows=len(entities),
        selected_entities=entities[:required],
        status="pass" if len(entities) >= required else "fail",
    )


def _candidate_with_row_keys(name, entity_key, entities, row_keys):
    entities = tuple(entities)
    return CandidateSet(
        selector_name=name,
        entity_key=entity_key,
        required=len(entities),
        entities=entities,
        candidate_rows=len(entities),
        selected_entities=entities,
        status="pass",
        selected_row_keys=tuple(row_keys),
    )


class TestAllocateCohortLogic:
    """allocate_cohort's dedup/bucketing/coverage logic. With target_vins=None
    no representative-fill query runs, so con is unused and passed as None."""

    def test_dedupes_entities_across_selectors(self):
        candidate_sets = {
            "sel_a": _candidate("sel_a", "vin", 2, ["VIN1", "VIN2"]),
            "sel_b": _candidate("sel_b", "vin", 2, ["VIN2", "VIN3"]),
        }
        allocation = allocate_cohort(candidate_sets, None, None, None)
        assert allocation.vin_seeds == frozenset({"VIN1", "VIN2", "VIN3"})

    def test_buckets_by_entity_key_type(self):
        candidate_sets = {
            "vin_sel": _candidate("vin_sel", "vin", 5, ["VIN1"]),
            "listing_sel": _candidate("listing_sel", "listing_id", 5, ["L1"]),
            "artifact_sel": _candidate("artifact_sel", "artifact_id", 5, [101]),
            "benchmark_sel": _candidate("benchmark_sel", "make_model", 3, ["Honda Civic"]),
        }
        allocation = allocate_cohort(candidate_sets, None, None, None)
        assert allocation.vin_seeds == frozenset({"VIN1"})
        assert allocation.listing_seeds == frozenset({"L1"})
        assert allocation.artifact_seeds == frozenset({101})
        assert allocation.make_model_seeds == frozenset({"Honda Civic"})

    def test_empty_and_short_candidates_do_not_crash(self):
        candidate_sets = {
            "cooldown_bucket_11_plus": _candidate("cooldown_bucket_11_plus", "listing_id", 1, []),
            "relisted_vin": _candidate("relisted_vin", "vin", 10, ["VIN1"]),
        }
        allocation = allocate_cohort(candidate_sets, None, None, None)
        assert allocation.listing_seeds == frozenset()
        assert allocation.vin_seeds == frozenset({"VIN1"})
        assert allocation.selector_coverage["cooldown_bucket_11_plus"]["found"] == 0
        assert allocation.selector_coverage["cooldown_bucket_11_plus"]["status"] == "fail"

    def test_required_vin_seeds_captured_before_fill(self):
        candidate_sets = {"relisted_vin": _candidate("relisted_vin", "vin", 10, ["VIN1", "VIN2"])}
        allocation = allocate_cohort(candidate_sets, None, None, None)
        assert allocation.required_vin_seeds == frozenset({"VIN1", "VIN2"})
        assert allocation.pre_fill_vin_count == 2
        assert allocation.fill_vins_added == 0

    def test_boundary_row_key_seeds_its_vin_into_closure(self):
        """stale_listing's captured boundary row key establishes real
        vehicle identity — its vin must seed vin_seeds directly, since the
        listing's only evidence may predate window_start (making the normal
        window-bounded vin<->listing_id closure lookup unable to discover it
        on its own). Without this, the vehicle's other listings/price
        events/remaps would be silently excluded from the export."""
        candidate_sets = {
            "stale_listing": _candidate_with_row_keys(
                "stale_listing", "listing_id", ["L_STALE"],
                [(900, "VIN_STALE", "L_STALE")],
            ),
        }
        allocation = allocate_cohort(candidate_sets, None, None, None)
        assert allocation.vin_seeds == frozenset({"VIN_STALE"})
        assert allocation.listing_seeds == frozenset({"L_STALE"})

    def test_artifact_only_row_key_does_not_seed_vin_closure(self):
        """invalid_or_null_vin's row key is artifact-only provenance (often
        a null/malformed vin) and must retain its existing non-expanding
        behavior — it must never seed vin_seeds, unlike a
        capture_boundary_row_key selector's row key."""
        candidate_sets = {
            "invalid_or_null_vin": _candidate_with_row_keys(
                "invalid_or_null_vin", "artifact_id", [200],
                [(200, None, "L_BAD_VIN")],
            ),
        }
        allocation = allocate_cohort(candidate_sets, None, None, None)
        assert allocation.vin_seeds == frozenset()
        assert allocation.artifact_seeds == frozenset({200})
        # listing_id is not this selector's entity_key, so it must not be
        # bucketed as a listing seed either — only the artifact_row_keys
        # pool (used solely for Gate D's exact-row export match) carries it.
        assert allocation.listing_seeds == frozenset()

    def test_logs_required_allocation_without_fill(self, caplog):
        candidate_sets = {"relisted_vin": _candidate("relisted_vin", "vin", 10, ["VIN1", "VIN2"])}
        with caplog.at_level("INFO", logger="archiver"):
            allocate_cohort(candidate_sets, None, None, None)
        messages = [r.message for r in caplog.records]
        assert any("required_allocation_done vin_seeds=2" in m for m in messages)
        assert not any("deterministic_fill" in m for m in messages)

    def test_logs_deterministic_fill_start_and_end(self, mocker, caplog):
        candidate_sets = {"relisted_vin": _candidate("relisted_vin", "vin", 10, ["VIN1"])}
        mocker.patch(
            "archiver.processors.lake_snapshot_cohort._fill_representative_vins",
            return_value=["VIN2", "VIN3"],
        )
        with caplog.at_level("INFO", logger="archiver"):
            allocation = allocate_cohort(candidate_sets, 3, mocker.Mock(), None)
        assert allocation.fill_vins_added == 2
        messages = [r.message for r in caplog.records]
        assert any(
            "deterministic_fill start" in m and "pre_fill_vin_count=1" in m
            and "needed=2" in m
            for m in messages
        )
        assert any(
            "deterministic_fill end" in m and "fill_vins_added=2" in m
            and "seed_vin_count=3" in m
            for m in messages
        )


# ---------------------------------------------------------------------------
# CandidateSet.entity_count + diagnostics conversion (Plan 120 Gate C.5)
# ---------------------------------------------------------------------------

class TestCandidateSetEntityCount:
    def test_defaults_to_len_entities_when_unset(self):
        candidate = _candidate("relisted_vin", "vin", 10, ["VIN1", "VIN2"])
        assert candidate.entity_count == 2

    def test_explicit_entity_count_preserved(self):
        """A bounded candidate list (candidate_cap) may be shorter than the
        true distinct entity count found in the lake."""
        candidate = CandidateSet(
            selector_name="relisted_vin", entity_key="vin", required=1,
            entities=("VIN1",), candidate_rows=500, selected_entities=("VIN1",),
            status="pass", entity_count=500,
        )
        assert candidate.entity_count == 500
        assert len(candidate.entities) == 1


class TestCandidateSetsToSelectorDiagnostics:
    def test_shape_matches_run_lake_selectors(self):
        candidate_sets = {
            "relisted_vin": _candidate("relisted_vin", "vin", 10, ["VIN1", "VIN2"]),
        }
        diagnostics = candidate_sets_to_selector_diagnostics(candidate_sets, base_path=None)
        assert set(diagnostics.keys()) == {"selectors", "errors", "ok"}
        entry = diagnostics["selectors"]["relisted_vin"]
        assert entry["entities"] == 2
        assert entry["required"] == 10
        assert entry["status"] == "fail"
        assert entry["sample_entities"] == ["VIN1", "VIN2"]
        assert diagnostics["ok"] is True
        assert diagnostics["errors"] == []

    def test_captures_candidate_errors(self):
        errored = CandidateSet(
            selector_name="relisted_vin", entity_key="vin", required=1,
            entities=(), candidate_rows=0, selected_entities=(), status="fail",
            error="boom",
        )
        diagnostics = candidate_sets_to_selector_diagnostics(
            {"relisted_vin": errored}, base_path=None,
        )
        assert diagnostics["ok"] is False
        assert diagnostics["errors"] == ["relisted_vin: boom"]


# ---------------------------------------------------------------------------
# Candidate collection — error handling (no external service)
# ---------------------------------------------------------------------------

class TestCollectSelectorCandidatesErrorHandling:
    def test_missing_table_produces_captured_error_not_crash(self, tmp_path):
        """A missing/unreadable source path must surface as a captured error on
        the CandidateSet, never an exception — mirrors run_selector's contract.
        Uses a bogus local path with a plain in-process DuckDB connection, so
        no MinIO is required."""
        import duckdb

        con = duckdb.connect()
        try:
            candidate = collect_selector_candidates(
                con, "relisted_vin", base_path=str(tmp_path / "does_not_exist"),
            )
        finally:
            con.close()
        assert candidate.error is not None
        assert candidate.entities == ()
        assert candidate.selected_entities == ()

    def test_missing_table_logs_start_and_error_with_elapsed_s(self, tmp_path, caplog):
        """Progress logging (Plan 120 worker visibility) must announce a
        selector query before it runs and report failures with elapsed_s
        before returning the captured-error CandidateSet."""
        import duckdb

        con = duckdb.connect()
        try:
            with caplog.at_level("INFO", logger="archiver"):
                collect_selector_candidates(
                    con, "relisted_vin", base_path=str(tmp_path / "does_not_exist"),
                )
        finally:
            con.close()
        messages = [r.message for r in caplog.records]
        assert any(
            "selector=relisted_vin start" in m and "entity_key=" in m for m in messages
        )
        assert any(
            "selector=relisted_vin error" in m and "elapsed_s=" in m for m in messages
        )
