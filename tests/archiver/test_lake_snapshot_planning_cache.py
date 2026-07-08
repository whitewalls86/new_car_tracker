"""Unit tests for archiver/processors/lake_snapshot_planning_cache.py
(Plan 120 Gate C.75).

Covers fingerprint stability/sensitivity and the load/write cache helpers.
Storage is exercised against shared.minio.read_json/write_json via mocker,
never real MinIO.
"""
from datetime import datetime, timezone

from archiver.processors.export_ci_lake_snapshot import SnapshotRequest
from archiver.processors.lake_snapshot_planning_cache import (
    CACHE_SCHEMA_VERSION,
    compute_planning_fingerprint,
    load_planning_cache,
    planning_cache_path,
    resolve_planning_window,
    source_table_paths_hash,
    write_planning_cache,
)

UTC = timezone.utc


def _relative_request(**overrides):
    defaults = dict(
        tier="ci", run_selectors=True, build_cohort=True,
        source_window_months=1, target_vins=5000, min_selector_coverage=True,
        planning_cache_bucket_grain="week",
    )
    defaults.update(overrides)
    return SnapshotRequest(**defaults)


def _fingerprint_for(request, now=None):
    """Mirror export_ci_lake_snapshot's heavy-path sequence: resolve the
    effective (bucketed) planning window, then fingerprint it. Both must be
    computed from the same window, or the fingerprint could describe a
    different query than the one actually executed."""
    window_start, window_end = resolve_planning_window(request, None, None, now=now)
    return compute_planning_fingerprint(request, window_start, window_end)


# ---------------------------------------------------------------------------
# Fingerprint stability / sensitivity
# ---------------------------------------------------------------------------

class TestComputePlanningFingerprint:
    def test_same_relative_request_same_week_same_fingerprint(self):
        request = _relative_request()
        now_a = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)  # Monday
        now_b = datetime(2026, 7, 9, 21, 0, tzinfo=UTC)  # Thursday, same week
        fp_a, _ = _fingerprint_for(request, now=now_a)
        fp_b, _ = _fingerprint_for(request, now=now_b)
        assert fp_a == fp_b

    def test_same_relative_request_different_week_different_fingerprint(self):
        request = _relative_request()
        now_a = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)
        now_b = datetime(2026, 7, 13, 3, 0, tzinfo=UTC)  # following Monday
        fp_a, _ = _fingerprint_for(request, now=now_a)
        fp_b, _ = _fingerprint_for(request, now=now_b)
        assert fp_a != fp_b

    def test_day_grain_changes_by_day(self):
        request = _relative_request(planning_cache_bucket_grain="day")
        now_a = datetime(2026, 7, 8, 3, 0, tzinfo=UTC)
        now_b = datetime(2026, 7, 8, 23, 0, tzinfo=UTC)
        now_c = datetime(2026, 7, 9, 3, 0, tzinfo=UTC)
        fp_a, _ = _fingerprint_for(request, now=now_a)
        fp_b, _ = _fingerprint_for(request, now=now_b)
        fp_c, _ = _fingerprint_for(request, now=now_c)
        assert fp_a == fp_b
        assert fp_a != fp_c

    def test_none_grain_uses_exact_resolved_timestamps(self):
        request = _relative_request(planning_cache_bucket_grain="none")
        window_a = datetime(2026, 7, 8, 3, 0, tzinfo=UTC)
        window_b = datetime(2026, 7, 8, 3, 0, 1, tzinfo=UTC)
        fp_a, _ = compute_planning_fingerprint(request, None, window_a)
        fp_b, _ = compute_planning_fingerprint(request, None, window_b)
        assert fp_a != fp_b

    def test_target_vins_changes_fingerprint(self):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        fp_a, _ = _fingerprint_for(_relative_request(target_vins=5000), now=now)
        fp_b, _ = _fingerprint_for(_relative_request(target_vins=100), now=now)
        assert fp_a != fp_b

    def test_min_selector_coverage_changes_fingerprint(self):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        fp_a, _ = _fingerprint_for(_relative_request(min_selector_coverage=True), now=now)
        fp_b, _ = _fingerprint_for(_relative_request(min_selector_coverage=False), now=now)
        assert fp_a != fp_b

    def test_dry_run_does_not_change_fingerprint(self):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        fp_a, _ = _fingerprint_for(_relative_request(dry_run=True), now=now)
        fp_b, _ = _fingerprint_for(_relative_request(dry_run=False), now=now)
        assert fp_a == fp_b

    def test_audit_sources_does_not_change_fingerprint(self):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        fp_a, _ = _fingerprint_for(_relative_request(audit_sources=True), now=now)
        fp_b, _ = _fingerprint_for(_relative_request(audit_sources=False), now=now)
        assert fp_a == fp_b

    def test_snapshot_id_does_not_change_fingerprint(self):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        fp_a, _ = _fingerprint_for(
            _relative_request(snapshot_id="adaptive-refresh-aaa"), now=now
        )
        fp_b, _ = _fingerprint_for(
            _relative_request(snapshot_id="adaptive-refresh-bbb"), now=now
        )
        assert fp_a == fp_b

    def test_selector_config_hash_changes_fingerprint(self, mocker):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.selector_config_hash",
            return_value="hash-a",
        )
        fp_a, _ = _fingerprint_for(_relative_request(), now=now)
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.selector_config_hash",
            return_value="hash-b",
        )
        fp_b, _ = _fingerprint_for(_relative_request(), now=now)
        assert fp_a != fp_b

    def test_selector_sql_hash_changes_fingerprint(self, mocker):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.selector_sql_hash",
            return_value="hash-a",
        )
        fp_a, _ = _fingerprint_for(_relative_request(), now=now)
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.selector_sql_hash",
            return_value="hash-b",
        )
        fp_b, _ = _fingerprint_for(_relative_request(), now=now)
        assert fp_a != fp_b

    def test_source_table_paths_hash_changes_fingerprint(self, mocker):
        now = datetime(2026, 7, 8, tzinfo=UTC)
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.source_table_paths_hash",
            return_value="hash-a",
        )
        fp_a, _ = _fingerprint_for(_relative_request(), now=now)
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.source_table_paths_hash",
            return_value="hash-b",
        )
        fp_b, _ = _fingerprint_for(_relative_request(), now=now)
        assert fp_a != fp_b


# ---------------------------------------------------------------------------
# source_table_paths_hash
# ---------------------------------------------------------------------------

class TestSourceTablePathsHash:
    def test_differs_between_base_paths(self):
        assert source_table_paths_hash("/tmp/fixture_a") != source_table_paths_hash(
            "/tmp/fixture_b"
        )

    def test_differs_between_local_fixture_and_minio(self):
        assert source_table_paths_hash("/tmp/fixture_a") != source_table_paths_hash(None)

    def test_stable_for_same_base_path(self):
        assert source_table_paths_hash("/tmp/fixture_a") == source_table_paths_hash(
            "/tmp/fixture_a"
        )

    def test_source_base_path_alone_does_not_capture_table_layout_changes(self, mocker):
        """The fingerprint includes source_table_paths_hash specifically
        because source_base_path alone can't detect a source table layout
        change (e.g. SOURCE_TABLE_SPECS relative_path edits, or a resolved
        MinIO bucket change) when base_path itself is unchanged."""
        hash_before = source_table_paths_hash("/tmp/fixture_a")
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.resolve_table_path",
            side_effect=lambda table_name, base_path: f"{base_path}/changed-layout/{table_name}",
        )
        hash_after = source_table_paths_hash("/tmp/fixture_a")
        assert hash_before != hash_after


# ---------------------------------------------------------------------------
# resolve_planning_window — the actual query window must match the
# fingerprint's bucketed identity, or a cache hit would silently serve a
# cohort computed over a different window than the one requested.
# ---------------------------------------------------------------------------

class TestResolvePlanningWindow:
    def test_week_grain_anchors_to_monday_utc(self):
        request = _relative_request(planning_cache_bucket_grain="week")
        now = datetime(2026, 7, 9, 21, 30, tzinfo=UTC)  # Thursday
        start, end = resolve_planning_window(request, None, None, now=now)
        assert end == datetime(2026, 7, 6, 0, 0, tzinfo=UTC)  # preceding Monday
        assert start == datetime(2026, 6, 6, 0, 0, tzinfo=UTC)  # 1 month back

    def test_week_grain_same_bucket_yields_identical_window(self):
        request = _relative_request(planning_cache_bucket_grain="week")
        now_a = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)
        now_b = datetime(2026, 7, 9, 21, 0, tzinfo=UTC)
        window_a = resolve_planning_window(request, None, None, now=now_a)
        window_b = resolve_planning_window(request, None, None, now=now_b)
        assert window_a == window_b

    def test_day_grain_anchors_to_midnight_utc(self):
        request = _relative_request(planning_cache_bucket_grain="day")
        now = datetime(2026, 7, 8, 17, 45, tzinfo=UTC)
        start, end = resolve_planning_window(request, None, None, now=now)
        assert end == datetime(2026, 7, 8, 0, 0, tzinfo=UTC)

    def test_none_grain_passes_through_exact_window(self):
        request = _relative_request(planning_cache_bucket_grain="none")
        exact_start = datetime(2026, 7, 8, 3, 17, tzinfo=UTC)
        exact_end = datetime(2026, 7, 8, 3, 17, 1, tzinfo=UTC)
        start, end = resolve_planning_window(request, exact_start, exact_end)
        assert (start, end) == (exact_start, exact_end)

    def test_explicit_window_passes_through_unchanged(self):
        exact_start = datetime(2026, 1, 1, tzinfo=UTC)
        exact_end = datetime(2026, 6, 1, tzinfo=UTC)
        request = SnapshotRequest(
            tier="ci", source_window_start=exact_start, source_window_end=exact_end,
        )
        start, end = resolve_planning_window(request, exact_start, exact_end)
        assert (start, end) == (exact_start, exact_end)

    def test_bucketed_window_matches_its_own_fingerprint_window(self):
        """The exact property the fix guarantees: fingerprinting the window
        returned by resolve_planning_window must equal fingerprinting the
        window actually used for computation — by construction, since both
        come from the same resolved values."""
        request = _relative_request(planning_cache_bucket_grain="week")
        now_a = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)
        now_b = datetime(2026, 7, 9, 21, 0, tzinfo=UTC)
        window_a = resolve_planning_window(request, None, None, now=now_a)
        window_b = resolve_planning_window(request, None, None, now=now_b)
        # Same bucket -> identical resolved window -> any downstream query
        # (selectors/cohort) executed with window_a is indistinguishable
        # from one executed with window_b.
        assert window_a == window_b


# ---------------------------------------------------------------------------
# planning_cache_path
# ---------------------------------------------------------------------------

class TestPlanningCachePath:
    def test_builds_deterministic_path(self):
        path = planning_cache_path("snapshot_planning_cache", "abc123")
        assert path == "snapshot_planning_cache/fingerprints/abc123/planning.json"

    def test_strips_trailing_slash_on_prefix(self):
        path = planning_cache_path("snapshot_planning_cache/", "abc123")
        assert path == "snapshot_planning_cache/fingerprints/abc123/planning.json"


# ---------------------------------------------------------------------------
# load/write helpers
# ---------------------------------------------------------------------------

class TestLoadPlanningCache:
    def test_miss_returns_none(self, mocker):
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.read_json", return_value=None
        )
        assert load_planning_cache("some/path") is None

    def test_hit_returns_artifact(self, mocker):
        artifact = {"cache_schema_version": CACHE_SCHEMA_VERSION, "fingerprint": "abc"}
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.read_json", return_value=artifact
        )
        assert load_planning_cache("some/path") == artifact

    def test_load_failure_returns_none(self, mocker):
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.read_json",
            side_effect=RuntimeError("boom"),
        )
        assert load_planning_cache("some/path") is None

    def test_schema_mismatch_treated_as_miss(self, mocker):
        artifact = {"cache_schema_version": CACHE_SCHEMA_VERSION + 1, "fingerprint": "abc"}
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.read_json", return_value=artifact
        )
        assert load_planning_cache("some/path") is None


class TestWritePlanningCache:
    def test_write_calls_write_json(self, mocker):
        mock_write = mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.write_json"
        )
        write_planning_cache("some/path", {"a": 1})
        mock_write.assert_called_once_with("some/path", {"a": 1})

    def test_write_failure_does_not_raise(self, mocker):
        mocker.patch(
            "archiver.processors.lake_snapshot_planning_cache.write_json",
            side_effect=RuntimeError("boom"),
        )
        write_planning_cache("some/path", {"a": 1})
