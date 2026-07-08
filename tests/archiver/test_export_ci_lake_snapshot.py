"""Unit tests for archiver/processors/export_ci_lake_snapshot.py (Plan 120, Phase 1-2)."""
from datetime import datetime, timezone
from typing import Dict, List

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
        from archiver.processors.lake_snapshot_selectors import (
            _SELECTOR_CONFIGS,
            _SQL_DIR,
            _q,
        )

        assert _SQL_DIR.is_dir()
        registry = build_selector_registry()
        for name in RUNNABLE_SELECTORS:
            config = _SELECTOR_CONFIGS[name]
            assert (_SQL_DIR / f"{config.sql_template}.sql").is_file()
            assert registry[name].sql.strip()
            assert registry[name].sql == _q(config.sql_template)


# ---------------------------------------------------------------------------
# Selector config validation (archiver/config/lake_snapshot_selectors.yml)
# ---------------------------------------------------------------------------

class TestSelectorConfigValidation:
    """Validates archiver/config/lake_snapshot_selectors.yml, the single
    source of truth for selector metadata (Plan 120 Gate B config refactor)."""

    def _load(self, tmp_path, yaml_text):
        from archiver.processors.lake_snapshot_selectors import _load_selector_configs

        config_path = tmp_path / "selectors.yml"
        config_path.write_text(yaml_text)
        return _load_selector_configs(config_path)

    def test_real_config_loads_without_error(self):
        from archiver.processors.lake_snapshot_selectors import _load_selector_configs

        configs = _load_selector_configs()
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

        from archiver.processors.lake_snapshot_selectors import _CONFIG_PATH

        raw = yaml.safe_load(_CONFIG_PATH.read_text())
        assert set(RUNNABLE_SELECTORS) == set(raw["selectors"].keys())

    def test_registry_returns_one_selector_per_yaml_entry(self):
        import yaml

        from archiver.processors.lake_snapshot_selectors import _CONFIG_PATH

        raw = yaml.safe_load(_CONFIG_PATH.read_text())
        registry = build_selector_registry()
        assert set(registry.keys()) == set(raw["selectors"].keys())
        assert len(registry) == len(raw["selectors"])


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


def _write_parquet_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _write_silver_observations(base, rows):
    """Write observation rows into per-source partition folders, matching the
    real layout from flush_silver_observations.py. Writing every row under a
    single hardcoded 'source=detail' folder regardless of its actual `source`
    value would let DuckDB's hive-partition inference silently override the
    real column value for srp/carousel rows."""
    by_source: Dict[str, List[dict]] = {}
    for row in rows:
        by_source.setdefault(row["source"], []).append(row)
    for source, source_rows in by_source.items():
        _write_parquet_rows(
            base / "silver_normalized/observations"
                   f"/source={source}/obs_year=2026/obs_month=7/part-000.parquet",
            source_rows,
        )


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
# Selector execution (Gate B) — local Parquet fixtures
# ---------------------------------------------------------------------------

def _ts(*args):
    return datetime(*args, tzinfo=timezone.utc)


def _vin17(tag: str) -> str:
    """Deterministic 17-char alphanumeric VIN for fingerprint-sensitive
    selectors (stable_state_run/state_change_run), which validate vin17 the
    same way stg_observations.sql does."""
    return (tag.upper() + "0" * 17)[:17]


_OBS_DEFAULTS = dict(
    vin="ZZZZZZZZZZZZZZZZZ",
    listing_id="L0",
    artifact_id=0,
    source="detail",
    fetched_at=_ts(2026, 7, 1),
    price=10000,
    mileage=5000,
    msrp=30000,
    make="Toyota",
    model="Camry",
    trim="LE",
    year=2022,
    stock_type="Used",
    fuel_type="Gasoline",
    body_style="Sedan",
    listing_state="active",
    dealer_name="ABC Motors",
    dealer_zip="90210",
    dealer_city="Beverly Hills",
    dealer_state="CA",
    customer_id="CUST1",
)


def _obs_row(**overrides):
    row = dict(_OBS_DEFAULTS)
    row.update(overrides)
    return row


VIN_STABLE = _vin17("STABLE")
VIN_CHANGE = _vin17("CHANGE")
VIN_UNLIST = "VIN_UNLIST_TARGET"
VIN_DETAIL_WINS = "VIN_DETAIL_WINS_X"
VIN_SRP_ONLY = "VIN_SRP_ONLY_X"
VIN_CAROUSEL = "VIN_CAROUSEL_X"
VIN_NO_PRICE = "VIN_NO_PRICE_X"


def _dense_rows(n=20):
    return [
        _obs_row(
            vin=f"DENSE{i:012d}", listing_id=f"LDENSE{i}", artifact_id=2000 + i,
            make="Honda", model="Civic", fetched_at=_ts(2026, 7, 1),
        )
        for i in range(n)
    ]


def _sparse_rows(n=2):
    return [
        _obs_row(
            vin=f"SPARSE{i:011d}", listing_id=f"LSPARSE{i}", artifact_id=3000 + i,
            make="Rare", model="Bird", fetched_at=_ts(2026, 7, 1),
        )
        for i in range(n)
    ]


@pytest.fixture
def selector_fixture(tmp_path):
    """A local lake exercising every runnable selector at least once."""
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
        event_id=[1, 2, 3, 4, 5, 6, 7, 8],
        listing_id=["L1", "L1", "L1", "L4", "L16", "L16", "L17", "L17"],
        vin=[
            "VIN_RELISTED", "VIN_RELISTED", "VIN_RELISTED", "VIN_OTHER",
            "VIN_L16", "VIN_L16", "VIN_L17", "VIN_L17",
        ],
        artifact_id=[101, 101, 101, 104, 116, 116, 117, 117],
        price=pa.array([20000, 19000, 21000, 15000, 12000, 13000, 8000, 8500], type=pa.int32()),
        event_type=["upserted"] * 8,
        event_at=pa.array(
            [
                _ts(2026, 7, 1), _ts(2026, 7, 2), _ts(2026, 7, 3), _ts(2026, 7, 1),
                _ts(2026, 7, 20), _ts(2026, 8, 1), _ts(2026, 7, 1), _ts(2026, 7, 10),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    )
    _write_parquet(
        base / "ops_normalized/blocked_cooldown_events/year=2026/month=7/part-000.parquet",
        event_id=[1, 2, 3, 4, 5, 6],
        listing_id=["L1", "L1", "L5", "L20", "L21", "L22"],
        event_type=["blocked"] * 6,
        num_of_attempts=pa.array([1, 2, 1, 3, 7, 15], type=pa.int32()),
        event_at=pa.array(
            [_ts(2026, 7, 1), _ts(2026, 7, 2), _ts(2026, 7, 1), _ts(2026, 7, 1),
             _ts(2026, 7, 1), _ts(2026, 7, 1)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    )

    silver_rows = [
        # stable_state_run: two identical detail fingerprints for VIN_STABLE
        _obs_row(vin=VIN_STABLE, listing_id="L6", artifact_id=1061, fetched_at=_ts(2026, 7, 1)),
        _obs_row(vin=VIN_STABLE, listing_id="L6", artifact_id=1062, fetched_at=_ts(2026, 7, 2)),
        # state_change_run: price changes between the two detail rows for VIN_CHANGE
        _obs_row(
            vin=VIN_CHANGE, listing_id="L7", artifact_id=1071,
            fetched_at=_ts(2026, 7, 1), price=10000,
        ),
        _obs_row(
            vin=VIN_CHANGE, listing_id="L7", artifact_id=1072,
            fetched_at=_ts(2026, 7, 2), price=9000,
        ),
        # invalid_or_null_vin: null vin and a too-short/invalid vin
        _obs_row(vin=None, listing_id="L8a", artifact_id=108, fetched_at=_ts(2026, 7, 1)),
        _obs_row(vin="SHORTVIN", listing_id="L8b", artifact_id=109, fetched_at=_ts(2026, 7, 1)),
        # active_to_unlisted: active row followed by an unlisted row, same listing
        _obs_row(
            vin=VIN_UNLIST, listing_id="L9", artifact_id=1091, fetched_at=_ts(2026, 7, 1),
            listing_state="active", price=10000,
        ),
        _obs_row(
            vin=VIN_UNLIST, listing_id="L9", artifact_id=1092, fetched_at=_ts(2026, 7, 3),
            listing_state="unlisted", price=None, make=None, model=None,
        ),
        # detail_beats_srp: detail (earlier) should win over a later srp row
        _obs_row(
            vin=VIN_DETAIL_WINS, listing_id="L10", artifact_id=1101, source="detail",
            fetched_at=_ts(2026, 7, 1),
        ),
        _obs_row(
            vin=VIN_DETAIL_WINS, listing_id="L10", artifact_id=1102, source="srp",
            fetched_at=_ts(2026, 7, 3),
        ),
        # srp_fallback: usable srp row, no detail row at all for this VIN
        _obs_row(
            vin=VIN_SRP_ONLY, listing_id="L11", artifact_id=111, source="srp",
            fetched_at=_ts(2026, 7, 1),
        ),
        # carousel_only_or_low_priority: only a carousel row for this VIN
        _obs_row(
            vin=VIN_CAROUSEL, listing_id="L12", artifact_id=112, source="carousel",
            fetched_at=_ts(2026, 7, 1), make=None, model=None,
        ),
        # no_price_history: VIN present in silver_observations, absent from
        # price_observation_events entirely
        _obs_row(vin=VIN_NO_PRICE, listing_id="L13", artifact_id=113, fetched_at=_ts(2026, 7, 1)),
        # fresh_recent_listing: first/last seen both close to the window anchor
        _obs_row(vin="VIN_FRESH", listing_id="L14", artifact_id=1141, fetched_at=_ts(2026, 7, 8)),
        _obs_row(vin="VIN_FRESH", listing_id="L14", artifact_id=1142, fetched_at=_ts(2026, 7, 10)),
        # stale_listing: last seen far before the window anchor (2026-07-10)
        _obs_row(vin="VIN_STALE", listing_id="L15", artifact_id=1151, fetched_at=_ts(2026, 1, 1)),
        _obs_row(vin="VIN_STALE", listing_id="L15", artifact_id=1152, fetched_at=_ts(2026, 1, 5)),
    ]
    silver_rows += _dense_rows()
    silver_rows += _sparse_rows()

    _write_silver_observations(base, silver_rows)
    return base


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


class TestRunLakeSelectors:
    def test_relisted_vin_finds_multi_listing_vin(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture), names=["relisted_vin"])
        selector = diagnostics["selectors"]["relisted_vin"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["VIN_RELISTED"]
        assert selector["candidate_rows"] == 2
        assert selector["error"] is None

    def test_price_drop_finds_consecutive_lower_price(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture), names=["price_drop"])
        selector = diagnostics["selectors"]["price_drop"]
        assert selector["entities"] == 1
        assert "L1" in selector["sample_entities"]

    def test_price_increase_finds_consecutive_higher_price(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture), names=["price_increase"])
        selector = diagnostics["selectors"]["price_increase"]
        assert "L1" in selector["sample_entities"]

    def test_cooldown_incremented_finds_repeated_attempt(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["cooldown_incremented"],
        )
        selector = diagnostics["selectors"]["cooldown_incremented"]
        assert "L1" in selector["sample_entities"]

    def test_stable_state_run_finds_unchanged_fingerprint(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["stable_state_run"],
        )
        selector = diagnostics["selectors"]["stable_state_run"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == [VIN_STABLE]

    def test_state_change_run_finds_changed_fingerprint(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["state_change_run"],
        )
        selector = diagnostics["selectors"]["state_change_run"]
        assert VIN_CHANGE in selector["sample_entities"]
        assert VIN_STABLE not in selector["sample_entities"]

    def test_active_to_unlisted_finds_active_then_unlisted(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["active_to_unlisted"],
        )
        selector = diagnostics["selectors"]["active_to_unlisted"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["L9"]

    def test_price_changed_7d_finds_change_near_anchor(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["price_changed_7d"],
        )
        selector = diagnostics["selectors"]["price_changed_7d"]
        assert "L16" in selector["sample_entities"]
        assert "L17" not in selector["sample_entities"]

    def test_price_changed_30d_only_finds_change_outside_7d(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["price_changed_30d_only"],
        )
        selector = diagnostics["selectors"]["price_changed_30d_only"]
        assert "L17" in selector["sample_entities"]
        assert "L16" not in selector["sample_entities"]

    def test_price_changed_7d_anchors_to_explicit_window_end_not_max_event(self, selector_fixture):
        # Regression: recency must anchor to the requested window_end, not
        # MAX(event_at) of whatever survived the filter. L16's change is on
        # 2026-08-01 — with window_end far past that (2026-09-15), it is 45
        # days stale and must NOT be misclassified as "within 7d" just
        # because it happens to be the latest surviving event.
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["price_changed_7d"],
            window_end=datetime(2026, 9, 15, tzinfo=timezone.utc),
        )
        selector = diagnostics["selectors"]["price_changed_7d"]
        assert selector["entities"] == 0
        assert "L16" not in selector["sample_entities"]

    def test_price_changed_30d_only_anchors_to_explicit_window_end_not_max_event(
        self, selector_fixture,
    ):
        # Same regression for the 30d-only bucket: L17's change on
        # 2026-07-10 is 67 days before window_end=2026-09-15 and must not be
        # misclassified as "within 30d" via a MAX(event_at) anchor of
        # 2026-08-01 (22 days apart) instead of the real window_end.
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["price_changed_30d_only"],
            window_end=datetime(2026, 9, 15, tzinfo=timezone.utc),
        )
        selector = diagnostics["selectors"]["price_changed_30d_only"]
        assert selector["entities"] == 0
        assert "L17" not in selector["sample_entities"]

    def test_no_price_history_finds_vin_absent_from_price_events(self, selector_fixture):
        # sample_entities is capped at 5 (alphabetically first), and most
        # fixture VINs lack priced events, so check the full candidate set
        # directly rather than relying on the diagnostic sample.
        import duckdb

        from archiver.processors.lake_snapshot_selectors import build_selector_query
        from archiver.processors.lake_source_audit import resolve_table_path

        path = resolve_table_path("silver_observations", str(selector_fixture))
        extra_paths = {
            "price_observation_events_path": resolve_table_path(
                "price_observation_events", str(selector_fixture)
            )
        }
        sql, params = build_selector_query("no_price_history", path, extra_paths=extra_paths)
        con = duckdb.connect()
        try:
            found_vins = {row[0] for row in con.execute(sql, params).fetchall()}
        finally:
            con.close()
        assert VIN_NO_PRICE in found_vins

    def test_detail_beats_srp_finds_detail_winner(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["detail_beats_srp"],
        )
        selector = diagnostics["selectors"]["detail_beats_srp"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == [VIN_DETAIL_WINS]

    def test_srp_fallback_finds_srp_only_vin(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture), names=["srp_fallback"])
        selector = diagnostics["selectors"]["srp_fallback"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == [VIN_SRP_ONLY]

    def test_carousel_only_finds_carousel_vin(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["carousel_only_or_low_priority"]
        )
        selector = diagnostics["selectors"]["carousel_only_or_low_priority"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == [VIN_CAROUSEL]

    def test_invalid_or_null_vin_finds_bad_rows(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["invalid_or_null_vin"],
        )
        selector = diagnostics["selectors"]["invalid_or_null_vin"]
        # artifact_id 108 (null vin) and 109 ("SHORTVIN") are the two smallest
        # artifact_ids in the whole fixture, so they sort into the top-5 sample.
        assert 108 in selector["sample_entities"]
        assert 109 in selector["sample_entities"]

    def test_benchmark_dense_make_model_finds_dense_group(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["benchmark_dense_make_model"]
        )
        selector = diagnostics["selectors"]["benchmark_dense_make_model"]
        assert selector["entities"] == 1
        assert selector["sample_entities"] == ["Honda Civic"]

    def test_benchmark_sparse_make_model_finds_sparse_group(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["benchmark_sparse_make_model"]
        )
        selector = diagnostics["selectors"]["benchmark_sparse_make_model"]
        assert "Rare Bird" in selector["sample_entities"]
        # dense group must not leak into the sparse bucket
        assert "Honda Civic" not in selector["sample_entities"]

    def test_cooldown_blocked_finds_first_attempt(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["cooldown_blocked"],
        )
        selector = diagnostics["selectors"]["cooldown_blocked"]
        assert "L1" in selector["sample_entities"]
        assert "L5" in selector["sample_entities"]

    def test_cooldown_bucket_3_4_finds_bucket(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["cooldown_bucket_3_4"],
        )
        selector = diagnostics["selectors"]["cooldown_bucket_3_4"]
        assert selector["sample_entities"] == ["L20"]

    def test_cooldown_bucket_5_10_finds_bucket(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["cooldown_bucket_5_10"],
        )
        selector = diagnostics["selectors"]["cooldown_bucket_5_10"]
        assert selector["sample_entities"] == ["L21"]

    def test_cooldown_bucket_11_plus_finds_bucket(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["cooldown_bucket_11_plus"]
        )
        selector = diagnostics["selectors"]["cooldown_bucket_11_plus"]
        assert selector["sample_entities"] == ["L22"]

    def test_fresh_recent_listing_finds_recent_listing(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture), names=["fresh_recent_listing"],
        )
        selector = diagnostics["selectors"]["fresh_recent_listing"]
        assert selector["sample_entities"] == ["L14"]

    def test_stale_listing_finds_old_listing(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture), names=["stale_listing"])
        selector = diagnostics["selectors"]["stale_listing"]
        assert selector["sample_entities"] == ["L15"]

    def test_all_default_selectors_run(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture))
        assert set(diagnostics["selectors"].keys()) == set(RUNNABLE_SELECTORS)
        assert diagnostics["ok"] is True
        assert diagnostics["errors"] == []

    def test_required_and_status_reflect_registry_minimums(self, selector_fixture):
        diagnostics = run_lake_selectors(base_path=str(selector_fixture), names=["relisted_vin"])
        selector = diagnostics["selectors"]["relisted_vin"]
        assert selector["required"] == 10
        assert selector["status"] == "fail"  # only 1 entity found, min is 10

    def test_window_filters_candidates(self, selector_fixture):
        diagnostics = run_lake_selectors(
            base_path=str(selector_fixture),
            names=["relisted_vin"],
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
        # The fixture provides deliberately thin candidate sets, so most
        # selectors fall short of their registry minimums.
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, min_selector_coverage=True,
            source_base_path=str(selector_fixture),
        ))
        assert len(result.coverage_failures) > 0
        assert any("relisted_vin" in f for f in result.coverage_failures)
        assert any("stable_state_run" in f for f in result.coverage_failures)
        # cooldown buckets only require 1 entity each, and the fixture
        # provides exactly one qualifying row per bucket, so those pass.
        assert not any("cooldown_bucket_3_4" in f for f in result.coverage_failures)

    def test_min_selector_coverage_false_skips_coverage_failures(self, selector_fixture):
        result = export_ci_lake_snapshot(SnapshotRequest(
            tier="ci", dry_run=True, run_selectors=True, min_selector_coverage=False,
            source_base_path=str(selector_fixture),
        ))
        assert result.coverage_failures == []

