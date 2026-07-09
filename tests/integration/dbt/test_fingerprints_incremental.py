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

The scenario below is one test function, not several test methods sharing a
module-scoped project: each step's assertions depend on exactly the state
left by the step before it (bootstrap -> idempotent rerun -> append -> ...),
so splitting it across separately-selectable tests would make partial reruns
or randomized ordering silently meaningless. Each named `# --- step ---`
section is independently readable even though they share one project.
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

DBT_BIN = shutil.which("dbt")


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


@pytest.fixture
def dbt_project(tmp_path):
    project_dir = tmp_path
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


def test_incremental_fingerprints_scenario(dbt_project):
    # --- bootstrap: empty target behaves like a full build ---
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
    assert {r[0] for r in rows} == {1, 2}, "srp row and null-vin17 row must be excluded"

    # --- idempotent rerun: unchanged source produces no new/duplicate rows ---
    before = rows
    _dbt(dbt_project, "run")
    assert _fingerprint_rows(dbt_project) == before

    # --- new artifact inside the window appends exactly once ---
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

    # --- repeated run with the same source data is still not duplicated ---
    before = rows
    _dbt(dbt_project, "run")
    assert _fingerprint_rows(dbt_project) == before

    # --- late artifact inside the lookback is picked up, and a source batch
    #     with two rows sharing one artifact_id collapses to a single row ---
    # artifact 6 is fetched *before* the current max(fetched_at) (2026-01-01 07:00)
    # but well inside the 3-day lookback — simulates a late-arriving artifact
    # discovered after the watermark had already advanced past it.
    # artifact 7 appears twice in the same batch (e.g. an ingestion retry);
    # the row_number()-based dedupe must keep exactly one, the latest fetched_at.
    _write_seed(dbt_project, [
        _row(1, fetched_at="2026-01-01 00:00:00"),
        _row(2, fetched_at="2026-01-01 06:00:00"),
        _row(3, listing_id="L2", vin17="VIN0000000000002B", source="srp",
             fetched_at="2026-01-01 12:00:00"),
        _row(4, listing_id="L3", vin17=None, fetched_at="2026-01-01 12:00:00"),
        _row(5, fetched_at="2026-01-01 07:00:00", price=29000),
        _row(6, listing_id="L2", vin17="VIN0000000000002B", source="detail",
             fetched_at="2026-01-01 02:00:00", price=21000),
        _row(7, listing_id="L3", vin17="VIN0000000000003C", source="detail",
             fetched_at="2026-01-01 08:00:00", price=15000),
        _row(7, listing_id="L3", vin17="VIN0000000000003C", source="detail",
             fetched_at="2026-01-01 09:00:00", price=15500),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    rows = _fingerprint_rows(dbt_project)
    assert 6 in {r[0] for r in rows}, "late artifact inside the lookback must be included"
    assert len(rows) == len({r[0] for r in rows}), (
        "duplicate artifact_id within one source batch must collapse to one row"
    )
    by_artifact = {r[0]: r[1] for r in rows}
    assert by_artifact[7] == 15500, (
        "duplicate artifact_id within one batch must keep the latest fetched_at row"
    )

    # --- corrected artifact_id inside the lookback replaces, not duplicates ---
    _write_seed(dbt_project, [
        _row(1, fetched_at="2026-01-01 00:00:00"),
        _row(2, fetched_at="2026-01-01 06:00:00"),
        _row(3, listing_id="L2", vin17="VIN0000000000002B", source="srp",
             fetched_at="2026-01-01 12:00:00"),
        _row(4, listing_id="L3", vin17=None, fetched_at="2026-01-01 12:00:00"),
        _row(5, fetched_at="2026-01-01 07:00:00", price=28500),
        _row(6, listing_id="L2", vin17="VIN0000000000002B", source="detail",
             fetched_at="2026-01-01 02:00:00", price=21000),
        _row(7, listing_id="L3", vin17="VIN0000000000003C", source="detail",
             fetched_at="2026-01-01 08:00:00", price=15000),
        _row(7, listing_id="L3", vin17="VIN0000000000003C", source="detail",
             fetched_at="2026-01-01 09:00:00", price=15500),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    rows = _fingerprint_rows(dbt_project)
    by_artifact = {r[0]: r[1] for r in rows}
    assert by_artifact[5] == 28500
    assert len(rows) == len({r[0] for r in rows}), (
        "corrected artifact_id must replace, not duplicate"
    )

    # --- incremental output equals a full-refresh over the same fixture ---
    incremental_rows = sorted(rows)
    _dbt(dbt_project, "run", "--full-refresh")
    assert sorted(_fingerprint_rows(dbt_project)) == incremental_rows
