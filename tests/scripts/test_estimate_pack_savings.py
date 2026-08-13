"""Unit coverage for Plan 131 Stage 0d's measurement functions.

No MinIO, no DuckDB. Everything here exercises the pure arithmetic, because
that is where a storage decision gets quietly mismeasured -- Plan 114 shipped a
bytes-only projection that was wrong by 254 points, and Plan 129 had to re-run a
measurement whose train/test split leaked.
"""
from __future__ import annotations

import datetime as dt

import pytest

from scripts.estimate_pack_savings import (
    OBJECT_DIR_BYTES,
    Capture,
    derive_b_and_d,
    measure_listing,
    physical_bytes,
    project_corpus,
    summarize,
)


def cap(listing: str, aid: int, month: str, day: int, content: bytes) -> Capture:
    return Capture(
        artifact_id=aid,
        listing_id=listing,
        capture_month=month,
        fetched_at=dt.datetime(int(month[:4]), int(month[5:7]), day),
        content=content,
    )


# --------------------------------------------------------------------------
# physical_bytes -- the floor is the whole reason this plan exists
# --------------------------------------------------------------------------

class TestPhysicalBytes:
    def test_tiny_payload_pays_the_8kb_file_floor_plus_directory(self):
        assert physical_bytes(1) == 8192 + OBJECT_DIR_BYTES

    def test_payload_at_the_floor_does_not_round_up_further(self):
        assert physical_bytes(8192) == 8192 + OBJECT_DIR_BYTES

    def test_payload_above_the_floor_rounds_to_a_4kb_block(self):
        assert physical_bytes(8193) == 12288 + OBJECT_DIR_BYTES
        assert physical_bytes(28 * 1024) == 28 * 1024 + OBJECT_DIR_BYTES

    def test_saving_content_below_the_floor_frees_nothing(self):
        """The Plan 129 finding that capped useful dictionary size."""
        assert physical_bytes(7300) == physical_bytes(2000)


# --------------------------------------------------------------------------
# measure_listing
# --------------------------------------------------------------------------

class TestMeasureListing:
    def test_grouping_identical_captures_beats_per_object_compression(self):
        body = b"<html>" + b"vehicle detail block " * 500 + b"</html>"
        caps = [cap("L1", i, "2026-05", i + 1, body) for i in range(5)]

        m = measure_listing(caps, None)

        assert m["captures"] == 5
        assert m["grouped_bytes"] < m["baseline_bytes"]

    def test_monthly_split_costs_more_than_one_group_when_months_differ(self):
        body = b"<html>" + b"vehicle detail block " * 500 + b"</html>"
        caps = [cap("L1", 1, "2026-05", 1, body), cap("L1", 2, "2026-05", 2, body),
                cap("L1", 3, "2026-06", 1, body), cap("L1", 4, "2026-06", 2, body)]

        m = measure_listing(caps, None)

        assert m["months"] == 2
        assert m["monthly_bytes"] > m["grouped_bytes"]
        assert m["monthly_bytes"] < m["baseline_bytes"]

    def test_single_month_listing_has_monthly_equal_to_grouped(self):
        body = b"x" * 4000
        caps = [cap("L1", i, "2026-05", i + 1, body) for i in range(3)]

        m = measure_listing(caps, None)

        assert m["months"] == 1
        assert m["monthly_bytes"] == m["grouped_bytes"]

    def test_members_are_ordered_by_fetched_at_not_input_order(self):
        early, late = b"a" * 3000, b"z" * 3000
        shuffled = [cap("L1", 2, "2026-05", 9, late), cap("L1", 1, "2026-05", 1, early)]

        m = measure_listing(shuffled, None)

        # first_alone must describe the EARLIEST capture, or B is measured on
        # whichever row the database happened to return first.
        assert m["first_alone_bytes"] == measure_listing(
            [cap("L1", 1, "2026-05", 1, early)], None
        )["baseline_bytes"]

    def test_no_separator_is_added_between_members(self):
        """A real pack carries offsets in its index; a separator would measure
        bytes the format does not spend."""
        from shared.compression import compress_frame

        a, b = b"first" * 100, b"second" * 100
        caps = [cap("L1", 1, "2026-05", 1, a), cap("L1", 2, "2026-05", 2, b)]

        m = measure_listing(caps, None)

        assert m["grouped_bytes"] == len(compress_frame(a + b, level=9, dict_id=None))

    def test_single_capture_listing_is_measurable_and_degenerate(self):
        m = measure_listing([cap("L1", 1, "2026-05", 1, b"y" * 2000)], None)
        assert m["captures"] == 1
        assert m["grouped_bytes"] == m["baseline_bytes"] == m["first_alone_bytes"]


# --------------------------------------------------------------------------
# derive_b_and_d -- the two numbers the whole decision turns on
# --------------------------------------------------------------------------

class TestDeriveBAndD:
    def test_singleton_listings_are_excluded(self):
        """A 1-capture listing has no marginal capture and would drag D toward B."""
        rows = [
            {"captures": 1, "first_alone_bytes": 100, "grouped_bytes": 100},
            {"captures": 3, "first_alone_bytes": 100, "grouped_bytes": 140},
        ]
        bd = derive_b_and_d(rows)

        assert bd["listings_used"] == 1
        assert bd["marginal_captures"] == 2
        assert bd["D_marginal_capture_bytes"] == pytest.approx(20.0)

    def test_d_is_aggregate_weighted_not_a_per_listing_mean(self):
        """A 2-capture listing must not weigh as much as a 100-capture one."""
        rows = [
            {"captures": 2, "first_alone_bytes": 100, "grouped_bytes": 200},    # D=100
            {"captures": 101, "first_alone_bytes": 100, "grouped_bytes": 200},  # D=1
        ]
        bd = derive_b_and_d(rows)

        # per-listing mean would be 50.5; aggregate is 200/101
        assert bd["D_marginal_capture_bytes"] == pytest.approx(200 / 101)

    def test_d_over_b_reports_the_ratio_the_decision_hinges_on(self):
        rows = [{"captures": 5, "first_alone_bytes": 1000, "grouped_bytes": 2000}]
        bd = derive_b_and_d(rows)
        assert bd["D_over_B"] == pytest.approx(250 / 1000)

    def test_no_multi_capture_listings_returns_empty_not_a_bogus_number(self):
        assert derive_b_and_d([{"captures": 1, "first_alone_bytes": 5, "grouped_bytes": 5}]) == {}

    def test_empty_input_returns_empty(self):
        assert derive_b_and_d([]) == {}


# --------------------------------------------------------------------------
# project_corpus
# --------------------------------------------------------------------------

class TestProjectCorpus:
    def test_monthly_costs_more_than_per_listing_when_d_is_below_b(self):
        proj = project_corpus({"B_first_capture_bytes": 7300.0,
                               "D_marginal_capture_bytes": 1500.0})
        ceiling, monthly = proj["layouts"][0], proj["layouts"][1]

        assert ceiling["layout"].startswith("per-listing")
        assert monthly["logical_bytes"] > ceiling["logical_bytes"]
        assert proj["bucketing_cost_bytes"] > 0

    def test_no_grouping_win_when_d_equals_b(self):
        """The assumption that produced the withdrawn '5%' estimate."""
        proj = project_corpus({"B_first_capture_bytes": 7300.0,
                               "D_marginal_capture_bytes": 7300.0})
        for row in proj["layouts"]:
            assert row["logical_saving_pct"] == pytest.approx(0.0, abs=1e-6)

    def test_bucketing_cost_is_expressed_against_the_grouping_win(self):
        proj = project_corpus({"B_first_capture_bytes": 8000.0,
                               "D_marginal_capture_bytes": 1000.0})
        assert 0.0 < proj["bucketing_cost_pct_of_ceiling_saving"] < 100.0

    def test_physical_charges_the_floor_on_the_baseline(self):
        proj = project_corpus({"B_first_capture_bytes": 2000.0,
                               "D_marginal_capture_bytes": 500.0})
        # 2 KB payload still costs 8 KB + 4 KB on disk, so physical must exceed logical.
        assert proj["baseline_physical_bytes"] > proj["baseline_logical_bytes"]

    def test_empty_b_and_d_returns_empty(self):
        assert project_corpus({}) == {}


# --------------------------------------------------------------------------
# summarize
# --------------------------------------------------------------------------

class TestSummarize:
    def test_retention_is_the_share_of_the_grouping_win_monthly_keeps(self):
        rows = [{"captures": 10, "raw_bytes": 0, "baseline_bytes": 1000,
                 "baseline_physical": 0, "grouped_bytes": 600, "monthly_bytes": 800}]
        s = summarize(rows)
        # grouping saves 400, monthly saves 200 -> retains half
        assert s["monthly_retains_pct_of_grouping"] == pytest.approx(50.0)

    def test_savings_are_percentages_of_the_like_for_like_baseline(self):
        rows = [{"captures": 2, "raw_bytes": 0, "baseline_bytes": 200,
                 "baseline_physical": 0, "grouped_bytes": 150, "monthly_bytes": 150}]
        s = summarize(rows)
        assert s["grouped_saving_pct"] == pytest.approx(25.0)
        assert s["monthly_saving_pct"] == pytest.approx(25.0)

    def test_empty_returns_empty(self):
        assert summarize([]) == {}
