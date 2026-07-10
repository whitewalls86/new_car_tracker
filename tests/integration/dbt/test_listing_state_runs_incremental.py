"""
Plan 123 Phase 4: incremental behavior of int_listing_state_runs.

dbt unit tests (dbt/models/intermediate/unit_tests.yml) pin the gaps-and-islands
logic for a single dbt invocation, but they cannot exercise incremental *state*
across multiple invocations — bootstrap vs. incremental run, idempotency, or
affected-vin17 replacement (including late-arriving fingerprints that split or
merge run history). This module builds a throwaway dbt-duckdb project (its own
dbt_project.yml/profiles.yml, a seeded stg_observations stand-in, and the real
SQL for both int_listing_state_fingerprints and int_listing_state_runs read
directly from the repo) and drives real `dbt seed`/`dbt run` invocations
against it. int_listing_state_runs depends on int_listing_state_fingerprints
(Plan 123 Phase 2), so this test exercises the full incremental chain, not
int_listing_state_runs in isolation.

Requires a real `dbt` install (dbt-core + dbt-duckdb), same as the rest of
tests/integration/dbt/. The CI `dbt` job installs these; skipped elsewhere.

One continuous scenario function, not several independently-selectable test
methods, since each step's assertions depend on exactly the state left by the
step before it.
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
FINGERPRINTS_SQL = (
    REPO_ROOT / "dbt" / "models" / "intermediate" / "int_listing_state_fingerprints.sql"
).read_text()
RUNS_SQL = (
    REPO_ROOT / "dbt" / "models" / "intermediate" / "int_listing_state_runs.sql"
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
    return (
        f"{artifact_id},{listing_id},{source},{listing_state},{fetched_at},{vin17},"
        f"{price},{mileage},honda,crv,EX,2024,35000,new,gasoline,suv,"
        f"Test Dealer,90210,Los Angeles,CA,{customer_id}"
    )


@pytest.fixture
def dbt_project(tmp_path):
    project_dir = tmp_path
    (project_dir / "models").mkdir()
    (project_dir / "seeds").mkdir()

    (project_dir / "dbt_project.yml").write_text(textwrap.dedent("""\
        name: runs_incremental_test
        version: "1.0"
        config-version: 2
        profile: runs_incremental_test
        model-paths: ["models"]
        seed-paths: ["seeds"]
        vars:
          fingerprint_incremental_lookback_days: 60
          listing_state_runs_incremental_lookback_days: 60
        models:
          runs_incremental_test:
            +materialized: view
    """))
    (project_dir / "profiles.yml").write_text(textwrap.dedent(f"""\
        runs_incremental_test:
          target: duckdb
          outputs:
            duckdb:
              type: duckdb
              path: {(project_dir / 'test.duckdb').as_posix()}
              threads: 1
    """))
    (project_dir / "models" / "int_listing_state_fingerprints.sql").write_text(FINGERPRINTS_SQL)
    (project_dir / "models" / "int_listing_state_runs.sql").write_text(RUNS_SQL)
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


def _runs(project_dir, vin17=None):
    con = duckdb.connect(str(project_dir / "test.duckdb"), read_only=True)
    try:
        query = (
            "select vin17, listing_id, parsed_fingerprint, run_started_at, run_ended_at, "
            "artifact_count, hours_until_change, is_open_run "
            "from main.int_listing_state_runs "
        )
        if vin17:
            query += f"where vin17 = '{vin17}' "
        query += "order by vin17, run_started_at"
        return con.execute(query).fetchall()
    finally:
        con.close()


VIN1 = "VIN0000000000001A"
VIN2 = "VIN0000000000002B"


def test_incremental_listing_state_runs_scenario(dbt_project):
    # --- bootstrap: two same-fingerprint artifacts form one open run ---
    _write_seed(dbt_project, [
        _row(1, vin17=VIN1, fetched_at="2026-01-01 00:00:00", price=30000),
        _row(2, vin17=VIN1, fetched_at="2026-01-01 06:00:00", price=30000),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    runs = _runs(dbt_project, VIN1)
    assert len(runs) == 1
    assert runs[0][5] == 2 and runs[0][7] is True and runs[0][6] is None

    # --- idempotent rerun: unchanged source produces identical run rows ---
    before = runs
    _dbt(dbt_project, "run")
    assert _runs(dbt_project, VIN1) == before

    # --- append with the same fingerprint extends the open run ---
    _write_seed(dbt_project, [
        _row(1, vin17=VIN1, fetched_at="2026-01-01 00:00:00", price=30000),
        _row(2, vin17=VIN1, fetched_at="2026-01-01 06:00:00", price=30000),
        _row(3, vin17=VIN1, fetched_at="2026-01-01 12:00:00", price=30000),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    runs = _runs(dbt_project, VIN1)
    assert len(runs) == 1
    assert runs[0][5] == 3 and runs[0][7] is True and runs[0][6] is None

    # --- append with a different fingerprint (price change) opens a new run ---
    _write_seed(dbt_project, [
        _row(1, vin17=VIN1, fetched_at="2026-01-01 00:00:00", price=30000),
        _row(2, vin17=VIN1, fetched_at="2026-01-01 06:00:00", price=30000),
        _row(3, vin17=VIN1, fetched_at="2026-01-01 12:00:00", price=30000),
        _row(4, vin17=VIN1, fetched_at="2026-01-02 00:00:00", price=29000),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    runs = _runs(dbt_project, VIN1)
    assert len(runs) == 2
    run_a, run_b = runs
    assert run_a[5] == 3 and run_a[7] is False and run_a[6] == 12
    assert run_b[5] == 1 and run_b[7] is True and run_b[6] is None

    # --- relisting (new listing_id) opens a new run ---
    _write_seed(dbt_project, [
        _row(1, vin17=VIN1, fetched_at="2026-01-01 00:00:00", price=30000),
        _row(2, vin17=VIN1, fetched_at="2026-01-01 06:00:00", price=30000),
        _row(3, vin17=VIN1, fetched_at="2026-01-01 12:00:00", price=30000),
        _row(4, vin17=VIN1, fetched_at="2026-01-02 00:00:00", price=29000),
        _row(5, vin17=VIN1, listing_id="L2", fetched_at="2026-01-05 00:00:00", price=29000),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    runs = _runs(dbt_project, VIN1)
    assert len(runs) == 3
    run_a, run_b, run_c = runs
    assert run_b[5] == 1 and run_b[7] is False and run_b[6] == 72
    assert run_c[1] == "L2" and run_c[7] is True and run_c[6] is None

    # --- late fingerprint (inside lookback) splits the first run ---
    # artifact 6 lands between artifacts 1 and 2/3 with a distinct price,
    # splitting the original single fp_a run into fp_a -> fp_x -> fp_a.
    _write_seed(dbt_project, [
        _row(1, vin17=VIN1, fetched_at="2026-01-01 00:00:00", price=30000),
        _row(2, vin17=VIN1, fetched_at="2026-01-01 06:00:00", price=30000),
        _row(3, vin17=VIN1, fetched_at="2026-01-01 12:00:00", price=30000),
        _row(4, vin17=VIN1, fetched_at="2026-01-02 00:00:00", price=29000),
        _row(5, vin17=VIN1, listing_id="L2", fetched_at="2026-01-05 00:00:00", price=29000),
        _row(6, vin17=VIN1, fetched_at="2026-01-01 03:00:00", price=31000),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    runs = _runs(dbt_project, VIN1)
    assert len(runs) == 5, "late artifact must split the original run into two"
    r1, r2, r3, r4, r5 = runs
    assert r1[5] == 1 and r1[6] == 3 and r1[7] is False  # fp_a (artifact 1) -> fp_x in 3h
    assert r2[5] == 1 and r2[6] == 3 and r2[7] is False  # fp_x (artifact 6) -> fp_a in 3h
    assert r3[5] == 2 and r3[6] == 12 and r3[7] is False  # fp_a (artifacts 2,3) -> fp_b in 12h
    assert r4[5] == 1 and r4[6] == 72 and r4[7] is False  # fp_b (artifact 4) -> L2 in 72h
    assert r5[1] == "L2" and r5[7] is True and r5[6] is None

    # snapshot VIN1's run state before touching VIN2, to prove it stays
    # untouched by an unrelated VIN's correction below
    incremental_vin1 = runs

    # --- separate VIN: a corrected artifact merges two runs back together ---
    # VIN2: fp_m (art 20) -> fp_n (art 21) -> fp_m again (art 22) makes three
    # runs. Correcting artifact 21's price to match fp_m collapses all three
    # into a single continuous run.
    _write_seed(dbt_project, [
        _row(1, vin17=VIN1, fetched_at="2026-01-01 00:00:00", price=30000),
        _row(2, vin17=VIN1, fetched_at="2026-01-01 06:00:00", price=30000),
        _row(3, vin17=VIN1, fetched_at="2026-01-01 12:00:00", price=30000),
        _row(4, vin17=VIN1, fetched_at="2026-01-02 00:00:00", price=29000),
        _row(5, vin17=VIN1, listing_id="L2", fetched_at="2026-01-05 00:00:00", price=29000),
        _row(6, vin17=VIN1, fetched_at="2026-01-01 03:00:00", price=31000),
        _row(20, vin17=VIN2, listing_id="L9", fetched_at="2026-01-10 00:00:00", price=40000),
        _row(21, vin17=VIN2, listing_id="L9", fetched_at="2026-01-10 06:00:00", price=41000),
        _row(22, vin17=VIN2, listing_id="L9", fetched_at="2026-01-10 12:00:00", price=40000),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    runs2 = _runs(dbt_project, VIN2)
    assert len(runs2) == 3, "fp_m -> fp_n -> fp_m forms three runs before correction"

    # correction: artifact 21's price is republished to match fp_m
    _write_seed(dbt_project, [
        _row(1, vin17=VIN1, fetched_at="2026-01-01 00:00:00", price=30000),
        _row(2, vin17=VIN1, fetched_at="2026-01-01 06:00:00", price=30000),
        _row(3, vin17=VIN1, fetched_at="2026-01-01 12:00:00", price=30000),
        _row(4, vin17=VIN1, fetched_at="2026-01-02 00:00:00", price=29000),
        _row(5, vin17=VIN1, listing_id="L2", fetched_at="2026-01-05 00:00:00", price=29000),
        _row(6, vin17=VIN1, fetched_at="2026-01-01 03:00:00", price=31000),
        _row(20, vin17=VIN2, listing_id="L9", fetched_at="2026-01-10 00:00:00", price=40000),
        _row(21, vin17=VIN2, listing_id="L9", fetched_at="2026-01-10 06:00:00", price=40000),
        _row(22, vin17=VIN2, listing_id="L9", fetched_at="2026-01-10 12:00:00", price=40000),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run")

    runs2 = _runs(dbt_project, VIN2)
    assert len(runs2) == 1, "corrected artifact must merge all three runs into one"
    assert runs2[0][5] == 3 and runs2[0][7] is True and runs2[0][6] is None

    # next_state_started_at / hours_until_change / is_open_run remain correct
    # for the untouched VIN1 across this VIN2-only change
    assert _runs(dbt_project, VIN1) == incremental_vin1

    # --- incremental output equals a full-refresh over the same fixture ---
    incremental_rows = sorted(_runs(dbt_project))
    _dbt(dbt_project, "run", "--full-refresh")
    assert sorted(_runs(dbt_project)) == incremental_rows
