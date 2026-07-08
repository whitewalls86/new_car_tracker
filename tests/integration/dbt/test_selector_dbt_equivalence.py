"""
Guards against archiver/processors/lake_snapshot_selectors.py drifting away
from the real dbt models it mirrors:

    stable_state_run / state_change_run  <->  int_listing_state_fingerprints.sql
                                               + int_listing_state_runs.sql
    detail_beats_srp / srp_fallback      <->  int_latest_observation.sql

Unlike a unit test that re-implements dbt's SQL by hand in Python (which can
drift from the real model on both sides without either test file changing),
this test reads the dbt models' *actual* materialized output. The CI `dbt`
job:

  1. runs `scripts/seed_lake_snapshot_fixture.py` to upload known
     business-state scenarios to MinIO,
  2. runs the existing `dbt build --profiles-dir dbt --target duckdb` step,
     which builds `int_listing_state_runs`/`int_latest_observation` for real
     against that data,
  3. then runs this test, which opens the resulting DuckDB file and compares
     dbt's output to the selector SQL run over the same MinIO source data.

Requires MINIO_ENDPOINT and DUCKDB_PATH (both set by the CI `dbt` job).
Skipped everywhere else — there is no local dbt/MinIO stack to run this
against.
"""
import os

import duckdb
import pytest

from archiver.processors.lake_snapshot_selectors import build_selector_query
from archiver.processors.lake_source_audit import resolve_table_path
from scripts.seed_lake_snapshot_fixture import (
    VIN_ABA,
    VIN_DETAIL_WINS,
    VIN_IDENT,
    VIN_PRICE,
    VIN_RELIST2,
    VIN_SRP_ONLY,
    VIN_STATECHG,
)
from shared.duckdb_s3 import get_duckdb_s3_connection

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT") or not os.environ.get("DUCKDB_PATH"),
        reason="MINIO_ENDPOINT/DUCKDB_PATH not set — no local dbt/MinIO stack to compare against",
    ),
]


def _dbt_con():
    return duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)


def _selector_vins(name: str) -> set:
    """Run a selector's full (unsampled) candidate query against the same
    MinIO source data dbt just built from, and return every candidate VIN."""
    path = resolve_table_path("silver_observations", base_path=None)
    sql, params = build_selector_query(name, path)
    con = get_duckdb_s3_connection()
    try:
        return {row[0] for row in con.execute(sql, params).fetchall()}
    finally:
        con.close()


class TestStateChangeRunAgainstRealDbtModel:
    """int_listing_state_runs.sql collapses consecutive identical
    parsed_fingerprints into runs. A VIN with exactly one run row is
    "stable"; a VIN with more than one run row experienced state changes."""

    def _run_counts(self):
        con = _dbt_con()
        try:
            rows = con.execute(
                """
                SELECT vin17, count(*) AS run_count
                FROM main.int_listing_state_runs
                WHERE vin17 IN (?, ?, ?, ?, ?)
                GROUP BY vin17
                """,
                [VIN_IDENT, VIN_PRICE, VIN_STATECHG, VIN_RELIST2, VIN_ABA],
            ).fetchall()
        finally:
            con.close()
        return {vin: count for vin, count in rows}

    def test_identical_states_collapse_into_one_dbt_run(self):
        counts = self._run_counts()
        assert counts[VIN_IDENT] == 1

    def test_price_change_produces_two_dbt_runs(self):
        counts = self._run_counts()
        assert counts[VIN_PRICE] == 2

    def test_listing_state_change_produces_two_dbt_runs(self):
        counts = self._run_counts()
        assert counts[VIN_STATECHG] == 2

    def test_listing_id_change_produces_two_dbt_runs(self):
        counts = self._run_counts()
        assert counts[VIN_RELIST2] == 2

    def test_a_b_a_sequence_produces_three_dbt_runs(self):
        counts = self._run_counts()
        assert counts[VIN_ABA] == 3

    def test_selector_agrees_with_real_dbt_runs(self):
        """The actual coupling assertion: for every scenario VIN, the
        selector's stable/changed classification must match how many runs
        dbt's real int_listing_state_runs materialized for that VIN."""
        counts = self._run_counts()
        stable_vins = _selector_vins("stable_state_run")
        changed_vins = _selector_vins("state_change_run")

        for vin, run_count in counts.items():
            if run_count == 1:
                assert vin in stable_vins, f"{vin}: dbt has 1 run but selector missed it as stable"
                assert vin not in changed_vins, (
                    f"{vin}: dbt has 1 run but selector flagged a change"
                )
            else:
                assert vin in changed_vins, (
                    f"{vin}: dbt has {run_count} runs but selector missed a state change"
                )
                assert vin not in stable_vins, (
                    f"{vin}: dbt has {run_count} runs but selector flagged it stable"
                )


class TestSourcePriorityAgainstRealDbtModel:
    """int_latest_observation.sql ranks detail > srp > carousel per VIN."""

    def _winning_source(self, vin: str) -> str:
        con = _dbt_con()
        try:
            row = con.execute(
                "SELECT source FROM main.int_latest_observation WHERE vin17 = ?", [vin]
            ).fetchone()
        finally:
            con.close()
        assert row is not None, f"{vin} missing from real dbt int_latest_observation output"
        return row[0]

    def test_detail_wins_over_later_srp_in_real_dbt_output(self):
        assert self._winning_source(VIN_DETAIL_WINS) == "detail"

    def test_srp_wins_when_no_usable_detail_in_real_dbt_output(self):
        assert self._winning_source(VIN_SRP_ONLY) == "srp"

    def test_selector_agrees_with_real_dbt_source_priority(self):
        assert VIN_DETAIL_WINS in _selector_vins("detail_beats_srp")
        assert VIN_SRP_ONLY in _selector_vins("srp_fallback")
