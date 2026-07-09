"""
Plan 123 Phase 2: incremental behavior of int_listing_state_fingerprints.

dbt unit tests (dbt/models/intermediate/unit_tests.yml) pin the fingerprint
hash logic for a single dbt invocation, but they cannot exercise incremental
*state* across multiple invocations — bootstrap vs. incremental run,
idempotency, late-arrival lookback, or artifact_id replacement. This module
builds a throwaway dbt-duckdb project (its own dbt_project.yml/profiles.yml,
a seeded stg_observations stand-in, and the real model SQL read directly from
the repo) and drives real `dbt seed`/`dbt run` invocations against it, so a
change to the model's incremental config or filter logic is caught here
instead of only in production.

Requires a real `dbt` install (dbt-core + dbt-duckdb), same as the rest of
tests/integration/dbt/. The CI `dbt` job installs these; skipped elsewhere.
"""
import shutil
import subprocess
import textwrap
from pathlib import Path

import duckdb
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt is not installed"),
]

REPO_ROOT = Path(__file__).resolve().parents[3]
MODEL_SQL = (
    REPO_ROOT / "dbt" / "models" / "intermediate" / "int_listing_state_fingerprints.sql"
).read_text()

SEED_HEADER = (
    "artifact_id,listing_id,source,listing_state,fetched_at,vin17,price,mileage,"
    "make,model,vehicle_trim,model_year,msrp,stock_type,fuel_type,body_style,"
    "dealer_name,dealer_zip,dealer_city,dealer_state,customer_id"
)


def _row(
    artifact_id,
    listing_id="L1",
    source="detail",
    listing_state="active",
    fetched_at="2026-01-01 00:00:00",
    vin17="VIN0000000000001A",
    price=30000,
    mileage=5000,
    customer_id="D001",
):
    vin17_field = "" if vin17 is None else vin17
    return (
        f"{artifact_id},{listing_id},{source},{listing_state},{fetched_at},{vin17_field},"
        f"{price},{mileage},honda,crv,EX,2024,35000,new,gasoline,suv,"
        f"Test Dealer,90210,Los Angeles,CA,{customer_id}"
    )


@pytest.fixture(scope="module")
def dbt_project(tmp_path_factory):
    project_dir = tmp_path_factory.mktemp("fingerprints_incremental")
    (project_dir / "models").mkdir()
    (project_dir / "seeds").mkdir()

    (project_dir / "dbt_project.yml").write_text(textwrap.dedent("""\
        name: fingerprints_incremental_test
        version: "1.0"
        config-version: 2
        profile: fingerprints_incremental_test
        model-paths: ["models"]
        seed-paths: ["seeds"]
        vars:
          fingerprint_incremental_lookback_days: 3
        models:
          fingerprints_incremental_test:
            +materialized: view
    """))
    (project_dir / "profiles.yml").write_text(textwrap.dedent(f"""\
        fingerprints_incremental_test:
          target: duckdb
          outputs:
            duckdb:
              type: duckdb
              path: {(project_dir / 'test.duckdb').as_posix()}
              threads: 1
    """))
    (project_dir / "models" / "int_listing_state_fingerprints.sql").write_text(MODEL_SQL)
    return project_dir


def _write_seed(project_dir, rows):
    (project_dir / "seeds" / "stg_observations.csv").write_text(
        SEED_HEADER + "\n" + "\n".join(rows) + "\n"
    )


DBT_BIN = shutil.which("dbt")


def _dbt(project_dir, *args):
    result = subprocess.run(
        [DBT_BIN, *args, "--profiles-dir", str(project_dir),
         "--project-dir", str(project_dir)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def _fingerprint_rows(project_dir):
    con = duckdb.connect(str(project_dir / "test.duckdb"), read_only=True)
    try:
        return con.execute(
            "select artifact_id, price, parsed_fingerprint "
            "from main.int_listing_state_fingerprints order by artifact_id"
        ).fetchall()
    finally:
        con.close()


class TestFingerprintsIncrementalBehavior:
    """Sequential scenario: each step builds on the target state left by the
    previous one, mirroring how the model actually runs hourly in production."""

    def test_bootstrap_matches_full_build(self, dbt_project):
        _write_seed(dbt_project, [
            _row(1, fetched_at="2026-01-01 00:00:00"),
            _row(2, fetched_at="2026-01-01 06:00:00"),
            _row(3, listing_id="L2", vin17="VIN0000000000002B", source="srp",
                 fetched_at="2026-01-01 12:00:00"),
            _row(4, listing_id="L3", vin17=None, fetched_at="2026-01-01 12:00:00"),
        ])
        _dbt(dbt_project, "seed")
        _dbt(dbt_project, "run")

        rows = _fingerprint_rows(dbt_project)
        artifact_ids = {r[0] for r in rows}
        assert artifact_ids == {1, 2}, "srp row and null-vin17 row must be excluded"

    def test_second_run_is_idempotent(self, dbt_project):
        before = _fingerprint_rows(dbt_project)
        _dbt(dbt_project, "run")
        after = _fingerprint_rows(dbt_project)
        assert after == before

    def test_new_artifact_appends_once(self, dbt_project):
        _write_seed(dbt_project, [
            _row(1, fetched_at="2026-01-01 00:00:00"),
            _row(2, fetched_at="2026-01-01 06:00:00"),
            _row(3, listing_id="L2", vin17="VIN0000000000002B", source="srp",
                 fetched_at="2026-01-01 12:00:00"),
            _row(4, listing_id="L3", vin17=None, fetched_at="2026-01-01 12:00:00"),
            _row(5, fetched_at="2026-01-01 07:00:00", price=29000),
        ])
        _dbt(dbt_project, "seed")
        _dbt(dbt_project, "run")

        rows = _fingerprint_rows(dbt_project)
        assert {r[0] for r in rows} == {1, 2, 5}
        assert len(rows) == len({r[0] for r in rows}), "no duplicate artifact_id rows"

    def test_repeated_run_same_source_data_no_duplicates(self, dbt_project):
        before = _fingerprint_rows(dbt_project)
        _dbt(dbt_project, "run")
        after = _fingerprint_rows(dbt_project)
        assert after == before

    def test_late_artifact_inside_lookback_is_included(self, dbt_project):
        # artifact 6 is fetched *before* the current max(fetched_at) (2026-01-01 07:00)
        # but well inside the 3-day lookback — simulates a late-arriving artifact
        # discovered after the watermark had already advanced past it.
        _write_seed(dbt_project, [
            _row(1, fetched_at="2026-01-01 00:00:00"),
            _row(2, fetched_at="2026-01-01 06:00:00"),
            _row(3, listing_id="L2", vin17="VIN0000000000002B", source="srp",
                 fetched_at="2026-01-01 12:00:00"),
            _row(4, listing_id="L3", vin17=None, fetched_at="2026-01-01 12:00:00"),
            _row(5, fetched_at="2026-01-01 07:00:00", price=29000),
            _row(6, listing_id="L2", vin17="VIN0000000000002B", source="detail",
                 fetched_at="2026-01-01 02:00:00", price=21000),
        ])
        _dbt(dbt_project, "seed")
        _dbt(dbt_project, "run")

        rows = _fingerprint_rows(dbt_project)
        assert 6 in {r[0] for r in rows}

    def test_corrected_artifact_id_replaces_not_duplicates(self, dbt_project):
        # artifact_id=5 reappears with a corrected price inside the lookback window.
        _write_seed(dbt_project, [
            _row(1, fetched_at="2026-01-01 00:00:00"),
            _row(2, fetched_at="2026-01-01 06:00:00"),
            _row(3, listing_id="L2", vin17="VIN0000000000002B", source="srp",
                 fetched_at="2026-01-01 12:00:00"),
            _row(4, listing_id="L3", vin17=None, fetched_at="2026-01-01 12:00:00"),
            _row(5, fetched_at="2026-01-01 07:00:00", price=28500),
            _row(6, listing_id="L2", vin17="VIN0000000000002B", source="detail",
                 fetched_at="2026-01-01 02:00:00", price=21000),
        ])
        _dbt(dbt_project, "seed")
        _dbt(dbt_project, "run")

        rows = _fingerprint_rows(dbt_project)
        by_artifact = {r[0]: r[1] for r in rows}
        assert by_artifact[5] == 28500
        assert len(rows) == len({r[0] for r in rows}), (
            "corrected artifact_id must replace, not duplicate"
        )

    def test_incremental_output_equals_full_refresh(self, dbt_project):
        incremental_rows = sorted(_fingerprint_rows(dbt_project))

        _dbt(dbt_project, "run", "--full-refresh")
        full_refresh_rows = sorted(_fingerprint_rows(dbt_project))

        assert full_refresh_rows == incremental_rows
