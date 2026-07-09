"""
Unit tests for the `stale_listing` selector's as-of window semantics
(Plan 120 selector policy correction).

The selector must answer "which listings' most recent observation at or
before the requested window_end is at least 30 days old" — using the
resolved window_end as an explicit anchor, never wall-clock `now()` and never
`max(last_seen_at)` over whatever rows happen to survive a [window_start,
window_end) filter (the prior bug: in a narrow one-month window, a listing's
last observation is almost always inside the window, so it can never be 30
days behind the newest row in that same window).

Exercised against small local Parquet fixtures (DuckDB's local-fixture
`base_path` mode — no MinIO required), mirroring test_lake_snapshot_export.py.
This selector's specific temporal edge cases (as-of anchor, pre-window_start
history, ignored future rows) are business-rule properties worth proving
directly, unlike the "selector SQL agrees with itself" concern the sibling
integration suite's docstring warns about.
"""
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from archiver.processors.lake_snapshot_cohort import collect_selector_candidates

UTC = timezone.utc

_SCHEMA = pa.schema([
    pa.field("listing_id", pa.string()),
    pa.field("vin", pa.string()),
    pa.field("artifact_id", pa.int64()),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
])


def _write_silver(tmp_path, rows):
    for i, row in enumerate(rows):
        row.setdefault("vin", f"VIN{i}")
        row.setdefault("artifact_id", i)
    table = pa.Table.from_pylist(rows, schema=_SCHEMA)
    root = tmp_path / "silver_normalized" / "observations"
    root.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(root / "data.parquet"))


def _stale_candidate_set(tmp_path, window_start=None, window_end=None):
    con = duckdb.connect()
    try:
        candidate = collect_selector_candidates(
            con, "stale_listing", base_path=str(tmp_path),
            window_start=window_start, window_end=window_end,
        )
    finally:
        con.close()
    assert candidate.error is None, candidate.error
    return candidate


def _stale_candidates(tmp_path, window_start=None, window_end=None) -> set:
    return set(_stale_candidate_set(tmp_path, window_start, window_end).entities)


class TestStaleListingAsOfSemantics:
    def test_last_observed_over_30d_before_window_end_is_selected(self, tmp_path):
        """Selected even though its last observation predates window_start —
        the bounded lookback must read history from before window_start."""
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {"listing_id": "L_STALE", "fetched_at": datetime(2026, 6, 20, tzinfo=UTC)},
        ])
        assert "L_STALE" in _stale_candidates(tmp_path, window_start, window_end)

    def test_recently_observed_before_window_end_is_not_selected(self, tmp_path):
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {"listing_id": "L_FRESH", "fetched_at": datetime(2026, 7, 28, tzinfo=UTC)},
        ])
        assert "L_FRESH" not in _stale_candidates(tmp_path, window_start, window_end)

    def test_future_observations_after_window_end_are_ignored(self, tmp_path):
        """A listing whose only *recent* row is after window_end must still
        be judged stale from its last pre-window_end observation — a future
        row must never count as "last seen"."""
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {"listing_id": "L_FUTURE_TRAP", "fetched_at": datetime(2026, 6, 20, tzinfo=UTC)},
            {"listing_id": "L_FUTURE_TRAP", "fetched_at": datetime(2026, 8, 10, tzinfo=UTC)},
        ])
        assert "L_FUTURE_TRAP" in _stale_candidates(tmp_path, window_start, window_end)

    def test_anchors_to_explicit_window_end_not_max_of_filtered_rows(self, tmp_path):
        """The prior bug derived the anchor from MAX(last_seen_at) over the
        already-window-filtered rows — for a single stale listing that
        anchor degenerates to its own last_seen_at (diff=0, never flagged).
        The fix must anchor to the requested window_end instead."""
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {"listing_id": "L_ONLY", "fetched_at": datetime(2026, 6, 20, tzinfo=UTC)},
        ])
        assert "L_ONLY" in _stale_candidates(tmp_path, window_start, window_end)

    def test_no_window_end_falls_back_to_unbounded_scan(self, tmp_path):
        """With no window_end at all (an unbounded/no-window call), the
        selector still runs — no wall-clock now() is ever involved."""
        _write_silver(tmp_path, [
            {"listing_id": "L_OLD", "fetched_at": datetime(2026, 1, 1, tzinfo=UTC)},
            {"listing_id": "L_NEW", "fetched_at": datetime(2026, 7, 1, tzinfo=UTC)},
        ])
        candidates = _stale_candidates(tmp_path)
        assert "L_OLD" in candidates
        assert "L_NEW" not in candidates

    def test_deterministic_across_repeated_calls(self, tmp_path):
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {"listing_id": "L_STALE", "fetched_at": datetime(2026, 6, 20, tzinfo=UTC)},
            {"listing_id": "L_FRESH", "fetched_at": datetime(2026, 7, 28, tzinfo=UTC)},
        ])
        first = _stale_candidates(tmp_path, window_start, window_end)
        second = _stale_candidates(tmp_path, window_start, window_end)
        assert first == second == {"L_STALE"}


class TestStaleListingBoundaryRowKeyCapture:
    """A selected stale listing's boundary (last-observation) row may predate
    window_start, so it must be captured as an exact row-key export
    exemption (`capture_boundary_row_key` in selector config) — otherwise the
    listing is selected into the cohort but exported with zero supporting
    silver_observations rows (see lake_snapshot_export._build_table_query)."""

    def test_selected_stale_listing_captures_its_last_observation_row_key(self, tmp_path):
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {
                "listing_id": "L_STALE", "vin": "VIN_STALE", "artifact_id": 900,
                "fetched_at": datetime(2026, 6, 20, tzinfo=UTC),
            },
        ])
        candidate = _stale_candidate_set(tmp_path, window_start, window_end)
        assert "L_STALE" in candidate.selected_entities
        assert candidate.selected_row_keys == ((900, "VIN_STALE", "L_STALE"),)

    def test_non_selected_fresh_listing_has_no_row_key_captured(self, tmp_path):
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {
                "listing_id": "L_FRESH", "vin": "VIN_FRESH", "artifact_id": 901,
                "fetched_at": datetime(2026, 7, 28, tzinfo=UTC),
            },
        ])
        candidate = _stale_candidate_set(tmp_path, window_start, window_end)
        assert candidate.selected_row_keys == ()

    def test_boundary_row_key_picks_the_last_observation_not_the_first(self, tmp_path):
        window_start = datetime(2026, 7, 1, tzinfo=UTC)
        window_end = datetime(2026, 8, 1, tzinfo=UTC)
        _write_silver(tmp_path, [
            {
                "listing_id": "L_STALE", "vin": "VIN_STALE", "artifact_id": 100,
                "fetched_at": datetime(2026, 6, 1, tzinfo=UTC),
            },
            {
                "listing_id": "L_STALE", "vin": "VIN_STALE", "artifact_id": 200,
                "fetched_at": datetime(2026, 6, 20, tzinfo=UTC),
            },
        ])
        candidate = _stale_candidate_set(tmp_path, window_start, window_end)
        assert candidate.selected_row_keys == ((200, "VIN_STALE", "L_STALE"),)
