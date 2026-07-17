"""
Plan 125 Gate B: unit tests for scripts/compare_gate_b_parity.py's comparison
logic. No live Spark/DuckDB/MinIO required -- compare_model() is a pure function
over row lists, and pyspark/duckdb are imported only inside the reader helpers.

These assert the comparator actually FAILS on the differences it exists to
catch. A parity script that cannot fail is worse than none: it would
rubber-stamp the gate.

Two areas get disproportionate attention, because both are places where a
plausible-looking comparator would silently pass:

  * The tie category. On the real seeded snapshot there are ZERO arg_max ties,
    so that code path never executed during the Gate B parity run. It is
    therefore proven only here, and these tests are the only evidence that a
    tie is reported rather than swallowed -- and, just as important, that a tie
    key does not become a blanket excuse for unrelated differences.
  * The absence of numeric tolerance. test_f12_truncation_bug_is_not_tolerated
    encodes the reasoning in the module docstring: the ::int rounding bug is a
    one-dollar difference, which is exactly what a +/-1 tolerance would forgive.
"""
import datetime as dt
from decimal import Decimal

from scripts.compare_gate_b_parity import ModelSpec, compare_model

UTC = dt.timezone.utc


def _spec(**kwargs):
    base = dict(name="int_test", key=("k",))
    base.update(kwargs)
    return ModelSpec(**base)


def _row(k="A", value=1, ts=None):
    return {"k": k, "value": value, "ts": ts or dt.datetime(2026, 6, 1, 12, tzinfo=UTC)}


def _failed(checks):
    return {c.name for c in checks if not c.passed}


class TestIdenticalOutput:
    def test_identical_rows_pass_every_check(self):
        rows = [_row("A"), _row("B")]
        checks, ties = compare_model(_spec(), rows, list(rows))

        assert _failed(checks) == set()
        assert checks, "expected at least one check to run"
        assert ties.entries == []

    def test_row_order_does_not_matter(self):
        """Spark makes no ordering guarantee and a model's `order by` is not
        binding on an Iceberg table read, so parity must compare on the key."""
        duck = [_row("A"), _row("B")]
        spark = [_row("B"), _row("A")]

        assert _failed(compare_model(_spec(), duck, spark)[0]) == set()


class TestDetectsRealDifferences:
    def test_row_count_difference_fails(self):
        checks, _ = compare_model(_spec(), [_row("A"), _row("B")], [_row("A")])
        assert "int_test: row count" in _failed(checks)

    def test_value_difference_fails(self):
        checks, _ = compare_model(_spec(), [_row("A", value=1)], [_row("A", value=2)])
        assert "int_test: row-by-row equality (exact)" in _failed(checks)

    def test_key_present_on_one_side_only_fails(self):
        checks, _ = compare_model(_spec(), [_row("A")], [_row("B")])
        assert "int_test: row-by-row equality (exact)" in _failed(checks)

    def test_duplicate_keys_fail(self):
        """The merge models' safety argument IS row-uniqueness of the key."""
        checks, _ = compare_model(_spec(), [_row("A"), _row("A")], [_row("A"), _row("A")])
        assert "int_test: duplicate keys (expect 0 on both)" in _failed(checks)

    def test_missing_column_fails(self):
        """int_latest_observation's explicit column list replaced
        `select * exclude`, so column-set drift is now a real risk."""
        duck = [{"k": "A", "value": 1, "extra": 9}]
        spark = [{"k": "A", "value": 1}]
        checks, _ = compare_model(_spec(), duck, spark)
        assert "int_test: column set" in _failed(checks)

    def test_null_count_difference_fails(self):
        spec = _spec(null_checked=("value",))
        checks, _ = compare_model(spec, [_row("A", value=None)], [_row("A", value=1)])
        assert "int_test: null count in value" in _failed(checks)

    def test_freshness_difference_fails(self):
        spec = _spec(freshness=("ts",))
        duck = [_row("A", ts=dt.datetime(2026, 6, 1, 12, tzinfo=UTC))]
        spark = [_row("A", ts=dt.datetime(2026, 6, 2, 12, tzinfo=UTC))]
        checks, _ = compare_model(spec, duck, spark)
        assert "int_test: max(ts)" in _failed(checks)

    def test_distribution_difference_fails(self):
        spec = _spec(distribution="value")
        duck = [_row("A", value="srp"), _row("B", value="srp")]
        spark = [_row("A", value="srp"), _row("B", value="detail")]
        checks, _ = compare_model(spec, duck, spark)
        assert "int_test: value distribution" in _failed(checks)

    def test_f12_truncation_bug_is_not_tolerated(self):
        """The reason there is no numeric tolerance.

        DuckDB's ::int rounds 1.9 -> 2; Spark's cast truncates -> 1. That is a
        one-dollar difference on every benchmark row, which is exactly the
        magnitude a +/-1 tolerance would forgive. The bug and the tolerance are
        the same size, so the tolerance cannot exist.
        """
        checks, _ = compare_model(_spec(), [_row("A", value=2)], [_row("A", value=1)])
        assert "int_test: row-by-row equality (exact)" in _failed(checks)


class TestRepresentationOnlyDifferences:
    """Type differences between the two Python drivers that are NOT value
    differences. Each must compare equal, or the whole run drowns in noise."""

    def test_naive_spark_timestamp_equals_aware_duckdb_timestamp(self):
        duck = [_row("A", ts=dt.datetime(2026, 6, 1, 12, tzinfo=UTC))]
        spark = [_row("A", ts=dt.datetime(2026, 6, 1, 12))]
        assert _failed(compare_model(_spec(freshness=("ts",)), duck, spark)[0]) == set()

    def test_timestamp_normalization_does_not_equate_different_instants(self):
        """The guard on the above: only the REPRESENTATION is forgiven. A naive
        value is read as UTC, so a genuinely different instant must still fail."""
        duck = [_row("A", ts=dt.datetime(2026, 6, 1, 12, tzinfo=UTC))]
        spark = [_row("A", ts=dt.datetime(2026, 6, 1, 13))]
        checks, _ = compare_model(_spec(freshness=("ts",)), duck, spark)
        assert "int_test: row-by-row equality (exact)" in _failed(checks)

    def test_decimal_equals_float_of_same_value(self):
        duck = [_row("A", value=Decimal("3.98"))]
        spark = [_row("A", value=3.98)]
        assert _failed(compare_model(_spec(), duck, spark)[0]) == set()

    def test_decimal_of_different_value_still_fails(self):
        duck = [_row("A", value=Decimal("3.98"))]
        spark = [_row("A", value=3.99)]
        checks, _ = compare_model(_spec(), duck, spark)
        assert "int_test: row-by-row equality (exact)" in _failed(checks)

    def test_timestamp_keys_normalize_so_runs_models_join(self):
        """int_listing_state_runs keys on run_started_at. Aware-vs-naive keys
        would otherwise make every row look one-sided."""
        spec = _spec(key=("k", "ts"), unique_key=False)
        duck = [_row("A", ts=dt.datetime(2026, 6, 1, 12, tzinfo=UTC))]
        spark = [_row("A", ts=dt.datetime(2026, 6, 1, 12))]
        assert _failed(compare_model(spec, duck, spark)[0]) == set()


class TestTieHandling:
    """The arg_max/arg_min tie category.

    Unexercised by the real snapshot (zero ties in it), so this is the only
    evidence the path behaves. DuckDB takes the first row on a tie and Spark the
    last; neither guarantees either, so a tie difference is not a port defect --
    but it must never become a licence to ignore real differences.
    """

    def test_tie_column_difference_on_tie_key_is_reported_not_failed(self):
        spec = _spec(tie_columns=("value",))
        checks, ties = compare_model(
            spec, [_row("A", value=1)], [_row("A", value=2)], tie_keys={("A",)}
        )

        assert _failed(checks) == set(), "a genuine tie must not fail the gate"
        assert len(ties.entries) == 1
        assert "duckdb=1 spark=2" in ties.entries[0]

    def test_tie_difference_is_visible_in_output(self):
        """'Reported' must mean reported -- a silent pass would be worse than a
        failure, because it would look like exact parity."""
        spec = _spec(tie_columns=("value",))
        _, ties = compare_model(
            spec, [_row("A", value=1)], [_row("A", value=2)], tie_keys={("A",)}
        )
        rendered = ties.render()
        assert "TIES" in rendered
        assert "1 tie-caused difference" in rendered

    def test_tie_column_difference_on_NON_tie_key_fails(self):
        """A tie must be proven from the source data for that specific key.
        Otherwise arg_max columns would get a permanent exemption."""
        spec = _spec(tie_columns=("value",))
        checks, ties = compare_model(
            spec, [_row("A", value=1)], [_row("A", value=2)], tie_keys=set()
        )

        assert "int_test: row-by-row equality (exact)" in _failed(checks)
        assert ties.entries == []

    def test_non_tie_column_difference_on_tie_key_fails(self):
        """A tie explains the tied column and nothing else. A real defect on a
        tie-affected key must not hide behind the tie."""
        spec = _spec(tie_columns=("value",))
        duck = [{"k": "A", "value": 1, "other": 10}]
        spark = [{"k": "A", "value": 1, "other": 99}]
        checks, ties = compare_model(spec, duck, spark, tie_keys={("A",)})

        assert "int_test: row-by-row equality (exact)" in _failed(checks)
        assert ties.entries == []

    def test_no_ties_configured_means_every_difference_fails(self):
        checks, ties = compare_model(
            _spec(), [_row("A", value=1)], [_row("A", value=2)], tie_keys={("A",)}
        )
        assert "int_test: row-by-row equality (exact)" in _failed(checks)
        assert ties.entries == []


class TestEmptyOutput:
    """Gate A's guard, kept: 0 == 0 satisfies every equality check while proving
    nothing, so an unseeded MinIO must fail rather than report parity."""

    def test_two_empty_builds_fail_by_default(self):
        checks, _ = compare_model(_spec(), [], [])
        assert "int_test: output is non-empty (both builds)" in _failed(checks)

    def test_allow_empty_opts_out_deliberately(self):
        checks, _ = compare_model(_spec(), [], [], allow_empty=True)
        assert _failed(checks) == set()

    def test_one_sided_empty_fails_even_with_allow_empty(self):
        checks, _ = compare_model(_spec(), [_row("A")], [], allow_empty=True)
        assert "int_test: row count" in _failed(checks)


class TestGrainSanity:
    def test_non_unique_comparison_key_is_flagged(self):
        """The _runs models legitimately have many rows per entity, so their
        ModelSpec.key must still identify a single row or the row-by-row join is
        meaningless. Catch a too-narrow key rather than silently comparing the
        wrong pairs."""
        spec = _spec(unique_key=False)
        rows = [_row("A", value=1), _row("A", value=2)]
        checks, _ = compare_model(spec, rows, list(rows))
        assert "int_test: comparison key is unique (grain sanity)" in _failed(checks)
