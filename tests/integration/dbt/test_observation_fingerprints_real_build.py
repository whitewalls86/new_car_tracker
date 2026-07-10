"""
Plan 123 Phase 2b: int_listing_observation_fingerprints against the real dbt
project and the shared MinIO lake-snapshot fixture — not a throwaway
dbt-duckdb shadow project.

The CI `dbt` job already seeds the base-phase fixture
(`scripts/seed_lake_snapshot_fixture.py`, phase="base") and runs a full
`dbt build --target duckdb` before this test runs, so the first half of this
test just queries the real materialized `int_listing_observation_fingerprints`
table for the SRP-multi-listing and carousel-multi-listing scenarios seeded
there (see `_observation_fingerprint_rows` in the fixture module).

The second half seeds the fixture's
"observation_fingerprint_incremental" phase — silver-only rows written
alongside (not over) the base phase's files — and reruns `dbt build --select
int_listing_observation_fingerprints` against the SAME DuckDB file with no
`--full-refresh`, so the model's real incremental logic (late-arrival
lookback, observation_id replace-on-correction) runs for real against the
combined base+phase-2 data. A final `--full-refresh` rebuild proves the
incremental result matches a from-scratch build over the same final data.

This intentionally does not build its own dbt project, CSV seed, or model
copy: a prior throwaway-project version of this test (see git history) worked
but required inventing a parallel fake universe (fake source shape, fake
project setup, separate fixture data/expectations) that drifts from the real
model graph over time. Driving the real project against the real shared
fixture is slower per-assertion but has no drift surface.

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


def _observation_rows():
    con = duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)
    try:
        return con.execute(
            "select observation_id, artifact_id, listing_id, vin17, source, price, "
            "parsed_fingerprint "
            "from main.int_listing_observation_fingerprints "
            "order by artifact_id, listing_id"
        ).fetchall()
    finally:
        con.close()


def test_observation_fingerprints_real_build_scenario():
    # --- base phase: already seeded + built by the CI `dbt build` step ---
    rows = _observation_rows()
    by_key = {(r[1], r[2]): r for r in rows}

    assert (fx.ARTIFACT_SRP_MULTI, fx.LISTING_SRP_MULTI_A) in by_key, (
        "SRP artifact with multiple listing_ids must produce a row per listing_id"
    )
    assert (fx.ARTIFACT_SRP_MULTI, fx.LISTING_SRP_MULTI_B) in by_key
    assert by_key[(fx.ARTIFACT_SRP_MULTI, fx.LISTING_SRP_MULTI_A)][5] == 20000

    assert (fx.ARTIFACT_CAROUSEL_MULTI, fx.LISTING_CAROUSEL_MULTI_A) in by_key, (
        "carousel artifact with multiple listing_ids must produce a row per listing_id"
    )
    assert (fx.ARTIFACT_CAROUSEL_MULTI, fx.LISTING_CAROUSEL_MULTI_B) in by_key
    assert by_key[(fx.ARTIFACT_CAROUSEL_MULTI, fx.LISTING_CAROUSEL_MULTI_B)][3] is None, (
        "a carousel row with no resolved vin17 must still be retained when listing_id exists"
    )

    base_observation_ids = [r[0] for r in rows]
    assert len(base_observation_ids) == len(set(base_observation_ids)), (
        "SRP/carousel multi-listing artifacts must not collide on observation_id"
    )

    # --- incremental phase: seed phase 2, rerun dbt build with no --full-refresh ---
    fx.seed(phase="observation_fingerprint_incremental")
    _run_dbt("build", "--select", "int_listing_observation_fingerprints")

    rows2 = _observation_rows()
    by_key2 = {(r[1], r[2]): r for r in rows2}

    assert (fx.ARTIFACT_OBSFP_LATE_ARRIVAL, fx.LISTING_OBSFP_LATE_ARRIVAL) in by_key2, (
        "a late-arriving artifact inside the lookback window must appear after the "
        "incremental rebuild"
    )
    corrected_price = by_key2[(fx.ARTIFACT_SRP_MULTI, fx.LISTING_SRP_MULTI_A)][5]
    assert corrected_price == fx.OBSFP_CORRECTED_PRICE, (
        "a corrected observation must replace the existing target row, not add a second one"
    )
    assert by_key2[(fx.ARTIFACT_SRP_MULTI, fx.LISTING_SRP_MULTI_B)][5] == 21000, (
        "an unrelated base-phase row must be unaffected by the incremental rebuild"
    )

    observation_ids_2 = [r[0] for r in rows2]
    assert len(observation_ids_2) == len(set(observation_ids_2)), (
        "the corrected observation must not duplicate its observation_id"
    )

    # --- repeated incremental run with no new data is idempotent ---
    _run_dbt("build", "--select", "int_listing_observation_fingerprints")
    assert _observation_rows() == rows2

    # --- incremental output equals a full-refresh over the same final data ---
    incremental_snapshot = sorted(rows2)
    _run_dbt("build", "--select", "int_listing_observation_fingerprints", "--full-refresh")
    assert sorted(_observation_rows()) == incremental_snapshot
