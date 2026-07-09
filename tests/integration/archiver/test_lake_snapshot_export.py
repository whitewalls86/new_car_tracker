"""
Gate D materialization writer correctness for CI lake snapshot exports
(Plan 120), run against real MinIO in CI.

Builds a real closed cohort via `build_snapshot_cohort` against the shared
MinIO fixture (`scripts/seed_lake_snapshot_fixture.py`), then materializes it
with `materialize_filtered_tables` and inspects the written Parquet output.

The central assertion here is the artifact fan-out non-regression: the fixture
seeds ARTIFACT_SRP_SHARED co-occurring on two unrelated VIN/listing rows
(mirroring an SRP/carousel page). Seeding the cohort on one of those VINs must
not leak the other into the materialized silver_observations output — that is
exactly the pollution the Plan 120 closure fix removed at the cohort-closure
level, and this test proves the export writer doesn't reintroduce it via a
naive `artifact_id IN (...)` table filter.

Requires MINIO_ENDPOINT (set by the CI `dbt` job). Skipped everywhere else.
"""
import os

import pyarrow.parquet as pq
import pytest

from archiver.processors.lake_snapshot_cohort import build_snapshot_cohort
from archiver.processors.lake_snapshot_export import materialize_filtered_tables
from scripts import seed_lake_snapshot_fixture as fx
from shared.duckdb_s3 import get_duckdb_s3_connection
from shared.minio import BUCKET, get_s3fs

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT"),
        reason="MINIO_ENDPOINT not set — no MinIO fixture to materialize from",
    ),
]

_TEST_EXPORT_PREFIX = "test_snapshot_exports_gate_d"


@pytest.fixture(scope="module")
def minio_con():
    con = get_duckdb_s3_connection()
    yield con
    con.close()


def _read_written_table(data_path: str, relative_prefix: str):
    fs = get_s3fs()
    path = f"{BUCKET}/{data_path}/{relative_prefix}"
    return pq.read_table(path, filesystem=fs).to_pylist()


class TestMaterializeFilteredTablesIntegration:
    def test_artifact_cooccurrence_does_not_leak_unrelated_vin(self, minio_con):
        # Seeded directly (not via cohort closure) so this test isolates the
        # writer's own filter predicate — cohort closure's non-expansion of
        # artifact co-occurrence is already covered by
        # test_lake_snapshot_cohort.py's TestExpandClosure suite.
        vins = frozenset({fx.VIN_SRP_COOCCUR_A})
        listing_ids = frozenset({fx.LISTING_SRP_COOCCUR_A})
        fingerprint = "test-artifact-fanout"

        result = materialize_filtered_tables(
            minio_con, None, None, None,
            vins, listing_ids, frozenset(),
            fingerprint, _TEST_EXPORT_PREFIX,
        )
        assert result.ok
        assert result.tables["silver_observations"]["rows"] >= 1

        written = _read_written_table(result.data_path, "silver_normalized/observations")
        listing_ids_written = {row["listing_id"] for row in written}
        assert fx.LISTING_SRP_COOCCUR_A in listing_ids_written
        assert fx.LISTING_SRP_COOCCUR_B not in listing_ids_written

    def test_explicit_artifact_row_key_included_without_vin_match(self, minio_con):
        fingerprint = "test-artifact-row-key"
        result = materialize_filtered_tables(
            minio_con, None, None, None,
            frozenset(), frozenset(),
            frozenset({(fx.ARTIFACT_NULL_VIN, None, fx.LISTING_NULL_VIN)}),
            fingerprint, _TEST_EXPORT_PREFIX,
        )
        assert result.tables["silver_observations"]["rows"] == 1

        written = _read_written_table(result.data_path, "silver_normalized/observations")
        assert len(written) == 1
        assert written[0]["listing_id"] == fx.LISTING_NULL_VIN
        assert written[0]["vin"] is None

    def test_full_cohort_materializes_expected_relisted_vin_rows(self, minio_con):
        cohort = build_snapshot_cohort(
            minio_con, None, None, None, target_vins=None,
            names=["relisted_vin"],
        )
        assert fx.VIN_RELISTED in cohort.closed_vins

        fingerprint = "test-relisted-vin"
        result = materialize_filtered_tables(
            minio_con, None, None, None,
            cohort.closed_vins, cohort.listing_ids, cohort.artifact_row_keys,
            fingerprint, _TEST_EXPORT_PREFIX,
        )
        assert result.tables["silver_observations"]["rows"] >= 2

        written = _read_written_table(result.data_path, "silver_normalized/observations")
        listing_ids_written = {row["listing_id"] for row in written}
        assert {fx.LISTING_RELISTED_1, fx.LISTING_RELISTED_2} <= listing_ids_written
