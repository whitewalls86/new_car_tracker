"""
Plan 125 Gate A: unit tests for scripts/compare_gate_a_parity.py's comparison
logic. No live Spark/DuckDB/MinIO required -- compare_mart_block_rate() is a
pure function over row lists, and pyspark/duckdb are imported only inside the
reader helpers.

These cover the failure modes the real parity run is meant to catch, by
asserting the comparator actually FAILS on them. A parity script that cannot
fail is worse than none: it would have rubber-stamped Gate A.
"""
from datetime import datetime

from scripts.compare_gate_a_parity import compare_mart_block_rate


def _row(hour, new_blocks=1, block_increments=0, total_block_events=1,
         unique_listings_blocked=1, max_attempts_seen=1):
    return {
        "hour": datetime(2026, 1, 10, hour),
        "new_blocks": new_blocks,
        "block_increments": block_increments,
        "total_block_events": total_block_events,
        "unique_listings_blocked": unique_listings_blocked,
        "max_attempts_seen": max_attempts_seen,
    }


def _failed(checks):
    return {c.name for c in checks if not c.passed}


class TestIdenticalOutput:
    def test_identical_rows_pass_every_check(self):
        rows = [_row(10), _row(11)]
        checks = compare_mart_block_rate(rows, list(rows))

        assert _failed(checks) == set()
        assert checks, "expected at least one check to run"

    def test_row_order_does_not_matter(self):
        """Spark makes no ordering guarantee, and the model's `order by` is not
        binding on an Iceberg table read. Parity must compare on the key."""
        duck = [_row(10), _row(11)]
        spark = [_row(11), _row(10)]

        assert _failed(compare_mart_block_rate(duck, spark)) == set()


class TestDetectsRealDifferences:
    def test_detects_row_count_difference(self):
        checks = compare_mart_block_rate([_row(10), _row(11)], [_row(10)])

        assert "row count" in _failed(checks)

    def test_detects_measure_difference(self):
        duck = [_row(10, new_blocks=5)]
        spark = [_row(10, new_blocks=4)]
        failed = _failed(compare_mart_block_rate(duck, spark))

        assert "sum(new_blocks)" in failed
        assert "row-by-row equality" in failed

    def test_detects_per_hour_difference_that_cancels_in_aggregate(self):
        """The reason row-by-row equality exists: two compensating per-hour
        errors leave every sum identical, so aggregates alone would pass."""
        duck = [_row(10, new_blocks=1), _row(11, new_blocks=3)]
        spark = [_row(10, new_blocks=3), _row(11, new_blocks=1)]
        checks = compare_mart_block_rate(duck, spark)

        assert next(c for c in checks if c.name == "sum(new_blocks)").passed
        assert "row-by-row equality" in _failed(checks)

    def test_detects_differing_hour_keys(self):
        checks = compare_mart_block_rate([_row(10)], [_row(12)])
        failed = _failed(checks)

        assert "min hour" in failed
        assert "row-by-row equality" in failed

    def test_detects_duplicate_keys(self):
        """`hour` is unique-tested on DuckDB; a Spark write that duplicated the
        grain must fail rather than be hidden by matching totals."""
        spark = [_row(10, new_blocks=1), _row(10, new_blocks=1)]
        duck = [_row(10, new_blocks=2)]
        failed = _failed(compare_mart_block_rate(duck, spark))

        assert "duplicate hour keys (expect 0 on both)" in failed

    def test_detects_missing_column(self):
        spark = [{"hour": datetime(2026, 1, 10, 10), "new_blocks": 1}]
        duck = [_row(10)]

        assert "sum(block_increments)" in _failed(compare_mart_block_rate(duck, spark))


class TestEmptyOutput:
    def test_two_empty_builds_do_not_crash(self):
        """Both empty is degenerate but not a *difference*; it must not raise
        on the min/max checks."""
        checks = compare_mart_block_rate([], [])

        assert "row count" not in _failed(checks)

    def test_one_empty_build_fails(self):
        assert "row count" in _failed(compare_mart_block_rate([_row(10)], []))
