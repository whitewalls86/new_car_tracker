"""
Plan 123: incremental behavior of int_listing_state_fingerprints,
int_price_history, int_listing_state_runs, mart_scrape_volume, and
int_latest_observation against the real dbt project and the shared MinIO
lake-snapshot fixture — replacing test_fingerprints_incremental.py,
test_price_history_incremental.py, and test_listing_state_runs_incremental.py,
each of which built its own throwaway dbt-duckdb shadow project (fake project
config, a seeded CSV stand-in for the real source, and the model SQL copied
into it). That worked but required inventing a parallel fake universe that
can drift from the real model graph over time.

Follows the same real-build pattern as
test_observation_fingerprints_real_build.py (Plan 123 Phase 2b): the CI `dbt`
job already seeds the base-phase fixture (`scripts/seed_lake_snapshot_fixture.py`,
phase="base") and runs a full `dbt build --target duckdb` before this test
runs, so each test below first queries the real materialized output for its
model's base-phase scenario rows, then seeds a phase-2 wave of rows (written
alongside, not over, the base phase's files) and reruns `dbt build --select
<model>` against the SAME DuckDB file with no `--full-refresh`, exercising the
model's real incremental logic for real: late arrivals, corrections,
same-batch duplicates, and affected-entity replacement. Each test finishes
with a repeated no-op incremental run (idempotency) and a `--full-refresh`
rebuild compared against the incremental result.

The five tests are independent (different models/fixture phases/VINs) except
that the int_listing_state_runs test also rebuilds its upstream
int_listing_state_fingerprints — rebuilding that model a second time is
idempotent for every VIN the fingerprints test already asserted on, so
execution order here does not matter for correctness, but they are kept in
top-to-bottom definition order (pytest's natural execution order within a
module) since that's the order the fixture phases document their lookback-
window anchors relative to each other.

Requires MINIO_ENDPOINT and DUCKDB_PATH (both set by the CI `dbt` job).
Skipped everywhere else — there is no local dbt/MinIO stack to run this
against.
"""
import os
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from scripts import seed_lake_snapshot_fixture as fx

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT") or not os.environ.get("DUCKDB_PATH"),
        reason="MINIO_ENDPOINT/DUCKDB_PATH not set — no local dbt/MinIO stack to build against",
    ),
    pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt is not installed"),
]

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_DIR = REPO_ROOT / "dbt"
DBT_BIN = shutil.which("dbt")


def _dbt_env() -> dict:
    env = dict(os.environ)
    # register_upstream_external_models() (dbt-duckdb's on-run-start hook,
    # dbt_project.yml) needs POSTGRES_URL on every invocation regardless of
    # --select. The CI step that runs this test doesn't export it (only the
    # earlier one-shot "dbt build" step does), so default it to the same
    # CI Postgres service credentials used elsewhere (tests/integration/conftest.py).
    env.setdefault("POSTGRES_URL", "postgresql://cartracker:cartracker@localhost:5432/cartracker")
    return env


def _run_dbt(*args):
    result = subprocess.run(
        [DBT_BIN, *args, "--profiles-dir", ".", "--target", "duckdb"],
        cwd=DBT_DIR, capture_output=True, text=True, env=_dbt_env(),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def _con():
    return duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)


# ===========================================================================
# int_listing_state_fingerprints — replaces test_fingerprints_incremental.py
# ===========================================================================

def _fingerprint_rows():
    con = _con()
    try:
        return con.execute(
            "select artifact_id, price, parsed_fingerprint "
            "from main.int_listing_state_fingerprints order by artifact_id"
        ).fetchall()
    finally:
        con.close()


def test_detail_fingerprint_incremental_real_build_scenario():
    # --- base phase: already seeded + built by the CI `dbt build` step ---
    by_artifact = {r[0]: r for r in _fingerprint_rows()}
    assert by_artifact[fx.ARTIFACT_FP_ANCHOR][1] == 25000
    assert by_artifact[fx.ARTIFACT_FP_DUP][1] == fx.FP_DUP_BASE_PRICE

    # --- incremental phase: seed phase 2, rerun dbt build with no --full-refresh ---
    fx.seed(phase="detail_fingerprint_incremental")
    _run_dbt("build", "--select", "int_listing_state_fingerprints")

    rows2 = _fingerprint_rows()
    by_artifact2 = {r[0]: r for r in rows2}

    assert fx.ARTIFACT_FP_LATE in by_artifact2, (
        "a late-arriving artifact inside the lookback window must appear after the "
        "incremental rebuild"
    )
    assert by_artifact2[fx.ARTIFACT_FP_DUP][1] == fx.FP_DUP_CORRECTED_PRICE, (
        "a correction with a later fetched_at must replace the existing target row, "
        "not add a second one"
    )
    assert by_artifact2[fx.ARTIFACT_FP_RETRY][1] == fx.FP_RETRY_LATE_PRICE, (
        "two source rows sharing one artifact_id in the same incremental batch must "
        "collapse to a single row, keeping the latest fetched_at"
    )

    artifact_ids2 = [r[0] for r in rows2]
    assert len(artifact_ids2) == len(set(artifact_ids2)), (
        "artifact_id must remain unique after the incremental rebuild"
    )

    # --- repeated incremental run with no new data is idempotent ---
    _run_dbt("build", "--select", "int_listing_state_fingerprints")
    assert _fingerprint_rows() == rows2

    # --- incremental output equals a full-refresh over the same final data ---
    incremental_snapshot = sorted(rows2)
    _run_dbt("build", "--select", "int_listing_state_fingerprints", "--full-refresh")
    assert sorted(_fingerprint_rows()) == incremental_snapshot


# ===========================================================================
# int_price_history — replaces test_price_history_incremental.py
# ===========================================================================

def _price_history_rows():
    con = _con()
    try:
        return con.execute(
            "select vin, current_price, first_price, min_price, max_price, "
            "total_price_observations, price_drop_count, price_increase_count, "
            "first_seen_at, last_seen_at "
            "from main.int_price_history order by vin"
        ).fetchall()
    finally:
        con.close()


def _price_history_columns():
    con = _con()
    try:
        return {
            r[0] for r in con.execute(
                "select column_name from information_schema.columns "
                "where table_name = 'int_price_history'"
            ).fetchall()
        }
    finally:
        con.close()


def test_price_history_incremental_real_build_scenario():
    # days_on_market correction (Plan 123 Phase 3): int_price_history no
    # longer stores it — see the model SQL for why.
    assert "days_on_market" not in _price_history_columns()

    # --- base phase: already seeded + built by the CI `dbt build` step ---
    by_vin = {r[0]: r for r in _price_history_rows()}
    affected_before = by_vin[fx.VIN_PH_AFFECTED]
    assert affected_before[1] == 39000 and affected_before[2] == 40000
    assert affected_before[5] == 2 and affected_before[6] == 1 and affected_before[7] == 0
    stable_before = by_vin[fx.VIN_PH_STABLE]

    # --- incremental phase: seed phase 2, rerun dbt build with no --full-refresh ---
    fx.seed(phase="price_history_incremental")
    _run_dbt("build", "--select", "int_price_history")

    by_vin2 = {r[0]: r for r in _price_history_rows()}
    affected = by_vin2[fx.VIN_PH_AFFECTED]

    # complete sequence by event_at: 40000 -> 39000 (drop) -> 38000 (late, drop)
    # -> 42000 (new, increase)
    assert affected[1] == 42000 and affected[2] == 40000, (
        "current_price/first_price must reflect the VIN's complete reread history"
    )
    assert affected[5] == 4, (
        "affected VIN's COMPLETE price history must be reread, not just the new events"
    )
    assert affected[6] == 2 and affected[7] == 1, (
        "the late event reorders the drop/increase sequence: 40k->39k (drop), "
        "39k->38k (drop), 38k->42k (increase)"
    )

    assert by_vin2[fx.VIN_PH_STABLE] == stable_before, (
        "a VIN untouched by the phase-2 events must be unaffected by the incremental rebuild"
    )

    # --- repeated incremental run with no new data is idempotent ---
    _run_dbt("build", "--select", "int_price_history")
    assert {r[0]: r for r in _price_history_rows()} == by_vin2

    # --- incremental output equals a full-refresh over the same final data ---
    incremental_snapshot = sorted(_price_history_rows())
    _run_dbt("build", "--select", "int_price_history", "--full-refresh")
    assert sorted(_price_history_rows()) == incremental_snapshot


# ===========================================================================
# int_listing_state_runs — replaces test_listing_state_runs_incremental.py
# ===========================================================================

def _runs_rows(vin17: str):
    con = _con()
    try:
        return con.execute(
            "select vin17, listing_id, parsed_fingerprint, run_started_at, run_ended_at, "
            "artifact_count, hours_until_change, is_open_run "
            "from main.int_listing_state_runs where vin17 = ? order by run_started_at",
            [vin17],
        ).fetchall()
    finally:
        con.close()


def test_listing_state_runs_incremental_real_build_scenario():
    # --- base phase: already seeded + built by the CI `dbt build` step ---
    runs_a_before = _runs_rows(fx.VIN_RUNS_A)
    assert len(runs_a_before) == 2, "base build: fp_a (2 artifacts) + fp_b (1, open)"
    runs_b_before = _runs_rows(fx.VIN_RUNS_B)
    assert len(runs_b_before) == 3, "base build: fp_m -> fp_n -> fp_m forms three runs"
    runs_stable_before = _runs_rows(fx.VIN_RUNS_STABLE)
    assert len(runs_stable_before) == 1

    # --- incremental phase: seed phase 2, rerun dbt build (fingerprints + runs) ---
    fx.seed(phase="listing_state_runs_incremental")
    _run_dbt("build", "--select", "int_listing_state_fingerprints", "int_listing_state_runs")

    runs_a = _runs_rows(fx.VIN_RUNS_A)
    assert len(runs_a) == 4, "a late artifact inside the lookback splits the original fp_a run"
    assert [r[5] for r in runs_a] == [1, 1, 1, 1]
    assert runs_a[-1][7] is True, "the last run stays open"

    runs_b = _runs_rows(fx.VIN_RUNS_B)
    assert len(runs_b) == 1, (
        "a corrected artifact matching an earlier fingerprint must merge all three runs "
        "into one"
    )
    assert runs_b[0][5] == 3 and runs_b[0][7] is True

    assert _runs_rows(fx.VIN_RUNS_STABLE) == runs_stable_before, (
        "an unaffected VIN's runs must be unchanged by another VIN's incremental rebuild"
    )

    # --- repeated incremental run with no new data is idempotent ---
    _run_dbt("build", "--select", "int_listing_state_fingerprints", "int_listing_state_runs")
    assert _runs_rows(fx.VIN_RUNS_A) == runs_a
    assert _runs_rows(fx.VIN_RUNS_B) == runs_b

    # --- incremental output equals a full-refresh over the same final data ---
    incremental_a = sorted(runs_a)
    incremental_b = sorted(runs_b)
    incremental_stable = sorted(runs_stable_before)
    _run_dbt(
        "build", "--select", "int_listing_state_fingerprints", "int_listing_state_runs",
        "--full-refresh",
    )
    assert sorted(_runs_rows(fx.VIN_RUNS_A)) == incremental_a
    assert sorted(_runs_rows(fx.VIN_RUNS_B)) == incremental_b
    assert sorted(_runs_rows(fx.VIN_RUNS_STABLE)) == incremental_stable


# ===========================================================================
# mart_scrape_volume — Plan 123 Phase 5 hourly_core optimization
# ===========================================================================

def _scrape_volume_row(hour, source):
    con = _con()
    try:
        return con.execute(
            "select hour, source, artifact_count, observation_count, "
            "unique_listings, valid_vin_count, vin_extraction_pct "
            "from main.mart_scrape_volume where hour = ? and source = ?",
            [hour, source],
        ).fetchone()
    finally:
        con.close()


def _scrape_volume_key_count():
    con = _con()
    try:
        return con.execute(
            "select count(*), count(distinct scrape_volume_key) from main.mart_scrape_volume"
        ).fetchone()
    finally:
        con.close()


def _all_scrape_volume_rows():
    con = _con()
    try:
        return con.execute(
            "select hour, source, artifact_count, observation_count, "
            "unique_listings, valid_vin_count, vin_extraction_pct "
            "from main.mart_scrape_volume order by hour, source"
        ).fetchall()
    finally:
        con.close()


def test_scrape_volume_incremental_real_build_scenario():
    # --- base phase: already seeded + built by the CI `dbt build` step ---
    affected_before = _scrape_volume_row(fx.SV_AFFECTED_HOUR, "detail")
    assert affected_before[2] == 1 and affected_before[3] == 1, "1 artifact, 1 observation"
    assert affected_before[5] == 1 and affected_before[6] == 100.0, (
        "the base row's valid vin17 gives valid_vin_count=1, vin_extraction_pct=100.0"
    )
    stable_before = _scrape_volume_row(fx.SV_STABLE_HOUR, "carousel")
    assert stable_before[2] == 1 and stable_before[3] == 1
    assert _scrape_volume_row(fx.SV_NEW_HOUR, "srp") is None

    # --- incremental phase: seed phase 2, rerun dbt build with no --full-refresh ---
    fx.seed(phase="scrape_volume_incremental")
    _run_dbt("build", "--select", "mart_scrape_volume")

    affected = _scrape_volume_row(fx.SV_AFFECTED_HOUR, "detail")
    assert affected[2] == 2 and affected[3] == 2, (
        "the affected hour's WHOLE aggregate must be recomputed from both rows, "
        "not incremented from just the new one"
    )
    assert affected[5] == 1 and affected[6] == 50.0, (
        "1 of 2 rows in the hour has a valid vin17 after the late row lands"
    )

    new_row = _scrape_volume_row(fx.SV_NEW_HOUR, "srp")
    assert new_row is not None and new_row[3] == 1, (
        "a brand new (hour, source) row must appear after the incremental rebuild"
    )

    assert _scrape_volume_row(fx.SV_STABLE_HOUR, "carousel") == stable_before, (
        "an hour outside the lookback window must be unaffected by the incremental rebuild"
    )

    total, distinct_keys = _scrape_volume_key_count()
    assert total == distinct_keys, (
        "scrape_volume_key must remain unique after the incremental rebuild"
    )

    # --- repeated incremental run with no new data is idempotent ---
    snapshot = _all_scrape_volume_rows()
    _run_dbt("build", "--select", "mart_scrape_volume")
    assert _all_scrape_volume_rows() == snapshot

    # --- incremental output equals a full-refresh over the same final data ---
    _run_dbt("build", "--select", "mart_scrape_volume", "--full-refresh")
    assert _all_scrape_volume_rows() == snapshot


# ===========================================================================
# int_latest_observation — Plan 123 Phase 5 hourly_core optimization
# ===========================================================================

def _latest_observation_row(vin17: str):
    con = _con()
    try:
        return con.execute(
            "select vin17, source, make, fetched_at "
            "from main.int_latest_observation where vin17 = ?",
            [vin17],
        ).fetchone()
    finally:
        con.close()


def _latest_observation_vin_count():
    con = _con()
    try:
        return con.execute(
            "select count(*), count(distinct vin17) from main.int_latest_observation"
        ).fetchone()
    finally:
        con.close()


def test_latest_observation_incremental_real_build_scenario():
    # --- base phase: already seeded + built by the CI `dbt build` step ---
    priority_before = _latest_observation_row(fx.VIN_LO_PRIORITY)
    assert priority_before[1] == "detail" and priority_before[2] == fx.LO_PRIORITY_DETAIL_MAKE
    upgrade_before = _latest_observation_row(fx.VIN_LO_DETAIL_UPGRADE)
    assert upgrade_before[1] == "detail" and upgrade_before[2] == fx.LO_DETAIL_OLD_MAKE
    stable_before = _latest_observation_row(fx.VIN_LO_STABLE)
    assert stable_before[1] == "detail" and stable_before[2] == fx.LO_STABLE_MAKE
    assert _latest_observation_row(fx.VIN_LO_NEW) is None

    # --- incremental phase: seed phase 2, rerun dbt build with no --full-refresh ---
    fx.seed(phase="latest_observation_incremental")
    _run_dbt("build", "--select", "int_latest_observation")

    priority = _latest_observation_row(fx.VIN_LO_PRIORITY)
    assert priority[1] == "detail" and priority[2] == fx.LO_PRIORITY_DETAIL_MAKE, (
        "the older detail row must still win over the newer SRP row added in phase 2 — "
        "source priority is checked before recency, even after a full history reread"
    )
    assert priority[3] == fx.LO_PRIORITY_DETAIL_FETCHED_AT

    upgrade = _latest_observation_row(fx.VIN_LO_DETAIL_UPGRADE)
    assert upgrade[1] == "detail" and upgrade[2] == fx.LO_DETAIL_NEW_MAKE, (
        "a newer detail row (same source tier) must win over the base-phase detail row"
    )
    assert upgrade[3] == fx.LO_DETAIL_NEW_FETCHED_AT

    new_row = _latest_observation_row(fx.VIN_LO_NEW)
    assert new_row is not None and new_row[2] == fx.LO_NEW_MAKE, (
        "a brand-new VIN with a late-arriving observation inside the lookback window "
        "must appear after the incremental rebuild"
    )

    assert _latest_observation_row(fx.VIN_LO_STABLE) == stable_before, (
        "a VIN untouched by the phase-2 rows must be unaffected by the incremental rebuild"
    )

    total, distinct_vins = _latest_observation_vin_count()
    assert total == distinct_vins, "vin17 must remain unique after the incremental rebuild"

    # --- repeated incremental run with no new data is idempotent ---
    snapshot = (
        _latest_observation_row(fx.VIN_LO_PRIORITY),
        _latest_observation_row(fx.VIN_LO_DETAIL_UPGRADE),
        _latest_observation_row(fx.VIN_LO_NEW),
        _latest_observation_row(fx.VIN_LO_STABLE),
    )
    _run_dbt("build", "--select", "int_latest_observation")
    assert (
        _latest_observation_row(fx.VIN_LO_PRIORITY),
        _latest_observation_row(fx.VIN_LO_DETAIL_UPGRADE),
        _latest_observation_row(fx.VIN_LO_NEW),
        _latest_observation_row(fx.VIN_LO_STABLE),
    ) == snapshot

    # --- incremental output equals a full-refresh over the same final data ---
    _run_dbt("build", "--select", "int_latest_observation", "--full-refresh")
    assert (
        _latest_observation_row(fx.VIN_LO_PRIORITY),
        _latest_observation_row(fx.VIN_LO_DETAIL_UPGRADE),
        _latest_observation_row(fx.VIN_LO_NEW),
        _latest_observation_row(fx.VIN_LO_STABLE),
    ) == snapshot
