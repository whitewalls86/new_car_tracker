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
    collect_selector_candidates,
)
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
# export_ci_lake_snapshot — non-dry-run (deferred)
# ---------------------------------------------------------------------------

class TestExportNonDryRun:
    def test_non_dry_run_returns_not_implemented(self):
        result = export_ci_lake_snapshot(SnapshotRequest(tier="ci", dry_run=False))
        assert result.status == "not_implemented"
        assert result.archive_key is None
        assert result.manifest_key is None


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
