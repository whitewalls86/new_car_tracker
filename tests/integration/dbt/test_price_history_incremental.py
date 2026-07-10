"""
Plan 123 Phase 3: incremental behavior of int_price_history.

dbt unit tests (dbt/models/intermediate/unit_tests.yml) pin the LAG()-based
price drop/increase logic for a single dbt invocation, but they cannot
exercise incremental *state* across multiple invocations — bootstrap vs.
incremental run, idempotency, late-arrival lookback, or affected-VIN
replacement. This module builds a throwaway dbt-duckdb project (its own
dbt_project.yml/profiles.yml, a seeded stg_price_events stand-in, and the
real model SQL read directly from the repo) and drives real `dbt seed`/
`dbt run` invocations against it, so a change to the model's incremental
config or aggregation logic is caught here instead of only in production.

Also covers the days_on_market correction: int_price_history no longer
stores days_on_market (it used to be `datediff('day', min(event_at), now())`,
which would go stale for VINs the affected-VIN replacement logic doesn't
touch on a given run). A stub downstream model in the fixture project
mirrors the real fix in mart_vehicle_snapshot — computing days_on_market
from the stable first_seen_at column at query time — and this test proves
days_on_market keeps advancing correctly as the downstream model is rebuilt
with later as-of dates, independent of whether int_price_history itself
reprocessed that VIN on any particular run.

Requires a real `dbt` install (dbt-core + dbt-duckdb), same as the rest of
tests/integration/dbt/. The CI `dbt` job installs these; skipped elsewhere.

One continuous scenario function, not several independently-selectable test
methods, since each step's assertions depend on exactly the state left by the
step before it.
"""
import json
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
    REPO_ROOT / "dbt" / "models" / "intermediate" / "int_price_history.sql"
).read_text()

SEED_HEADER = "vin,price,event_at"

DBT_BIN = shutil.which("dbt")

DOWNSTREAM_STUB_SQL = textwrap.dedent("""\
    {{ config(materialized='view') }}

    -- Mirrors the real fix in mart_vehicle_snapshot.sql: days_on_market is
    -- computed here from int_price_history's stable first_seen_at, not
    -- stored inside int_price_history itself.
    select
        vin,
        first_seen_at,
        datediff(
            'day', first_seen_at, cast('{{ var("as_of_date") }}' as timestamp)
        ) as days_on_market
    from {{ ref('int_price_history') }}
""")


@pytest.fixture
def dbt_project(tmp_path):
    project_dir = tmp_path
    (project_dir / "models").mkdir()
    (project_dir / "seeds").mkdir()

    (project_dir / "dbt_project.yml").write_text(textwrap.dedent("""\
        name: price_history_incremental_test
        version: "1.0"
        config-version: 2
        profile: price_history_incremental_test
        model-paths: ["models"]
        seed-paths: ["seeds"]
        vars:
          price_history_incremental_lookback_days: 3
          as_of_date: "2026-01-01"
        models:
          price_history_incremental_test:
            +materialized: view
    """))
    (project_dir / "profiles.yml").write_text(textwrap.dedent(f"""\
        price_history_incremental_test:
          target: duckdb
          outputs:
            duckdb:
              type: duckdb
              path: {(project_dir / 'test.duckdb').as_posix()}
              threads: 1
    """))
    (project_dir / "models" / "int_price_history.sql").write_text(MODEL_SQL)
    (project_dir / "models" / "downstream_days_on_market.sql").write_text(DOWNSTREAM_STUB_SQL)
    return project_dir


def _row(vin, price, event_at):
    return f"{vin},{price},{event_at}"


def _write_seed(project_dir, rows):
    (project_dir / "seeds" / "stg_price_events.csv").write_text(
        SEED_HEADER + "\n" + "\n".join(rows) + "\n"
    )


def _dbt(project_dir, *args, extra_vars=None):
    cmd = [DBT_BIN, *args, "--profiles-dir", str(project_dir), "--project-dir", str(project_dir)]
    if extra_vars:
        cmd += ["--vars", json.dumps(extra_vars)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def _price_history_rows(project_dir):
    con = duckdb.connect(str(project_dir / "test.duckdb"), read_only=True)
    try:
        return con.execute(
            "select vin, current_price, first_price, min_price, max_price, "
            "total_price_observations, price_drop_count, price_increase_count, "
            "first_seen_at, last_seen_at "
            "from main.int_price_history order by vin"
        ).fetchall()
    finally:
        con.close()


def _columns(project_dir):
    con = duckdb.connect(str(project_dir / "test.duckdb"), read_only=True)
    try:
        return {
            r[0] for r in con.execute(
                "select column_name from information_schema.columns "
                "where table_name = 'int_price_history'"
            ).fetchall()
        }
    finally:
        con.close()


def _days_on_market(project_dir):
    con = duckdb.connect(str(project_dir / "test.duckdb"), read_only=True)
    try:
        return dict(con.execute(
            "select vin, days_on_market from main.downstream_days_on_market order by vin"
        ).fetchall())
    finally:
        con.close()


def test_incremental_price_history_scenario(dbt_project):
    # --- bootstrap: empty target behaves like a full build ---
    _write_seed(dbt_project, [
        # VIN A: 100 -> 120 (increase) across the eventual watermark boundary
        _row("VINA0000000000001", 100, "2026-01-01 00:00:00"),
        _row("VINA0000000000001", 120, "2026-01-01 06:00:00"),
        # STABLE VIN: its own event never changes again for the rest of this
        # scenario, so once the watermark advances past its lookback window
        # it stops being reprocessed by int_price_history's affected-VIN logic
        _row("STABLEVIN000000001", 20000, "2026-01-01 00:00:00"),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-01-05"})

    rows = _price_history_rows(dbt_project)
    by_vin = {r[0]: r for r in rows}
    assert set(by_vin) == {"VINA0000000000001", "STABLEVIN000000001"}
    a = by_vin["VINA0000000000001"]
    assert a[1] == 120 and a[2] == 100 and a[5] == 2 and a[7] == 1 and a[6] == 0

    # days_on_market correction: int_price_history no longer stores it at all
    assert "days_on_market" not in _columns(dbt_project)
    assert _days_on_market(dbt_project)["STABLEVIN000000001"] == 4

    # --- idempotent rerun: unchanged source produces identical rows ---
    before = rows
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-01-05"})
    assert _price_history_rows(dbt_project) == before

    # --- new VIN appends without disturbing existing rows ---
    _write_seed(dbt_project, [
        _row("VINA0000000000001", 100, "2026-01-01 00:00:00"),
        _row("VINA0000000000001", 120, "2026-01-01 06:00:00"),
        _row("STABLEVIN000000001", 20000, "2026-01-01 00:00:00"),
        _row("VINC0000000000003", 50000, "2026-01-01 12:00:00"),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-01-06"})

    rows = _price_history_rows(dbt_project)
    by_vin = {r[0]: r for r in rows}
    assert set(by_vin) == {"VINA0000000000001", "STABLEVIN000000001", "VINC0000000000003"}
    prev_by_vin = {r[0]: r for r in before}
    assert by_vin["STABLEVIN000000001"] == prev_by_vin["STABLEVIN000000001"], (
        "STABLE VIN is unaffected by the new VINC event, so its row must be unchanged"
    )

    # Whether or not int_price_history's affected-VIN logic reprocessed
    # STABLE VIN on this particular run, the downstream days_on_market keeps
    # advancing because it's computed at query time from first_seen_at.
    assert _days_on_market(dbt_project)["STABLEVIN000000001"] == 5

    # --- additional event for an existing VIN recomputes its aggregates ---
    _write_seed(dbt_project, [
        _row("VINA0000000000001", 100, "2026-01-01 00:00:00"),
        _row("VINA0000000000001", 120, "2026-01-01 06:00:00"),
        _row("VINA0000000000001", 90, "2026-01-06 00:00:00"),
        _row("STABLEVIN000000001", 20000, "2026-01-01 00:00:00"),
        _row("VINC0000000000003", 50000, "2026-01-01 12:00:00"),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-01-07"})

    rows = _price_history_rows(dbt_project)
    by_vin = {r[0]: r for r in rows}
    a = by_vin["VINA0000000000001"]
    # 100 -> 120 (increase) -> 90 (drop): 1 increase, 1 drop
    assert a[1] == 90 and a[5] == 3 and a[6] == 1 and a[7] == 1

    # --- late event inserted between existing events (inside lookback) ---
    # A price of 110 at 2026-01-01 03:00 lands between the 100 and 120 events,
    # changing the drop/increase sequence: 100 -> 110 (inc) -> 120 (inc) -> 90 (drop).
    _write_seed(dbt_project, [
        _row("VINA0000000000001", 100, "2026-01-01 00:00:00"),
        _row("VINA0000000000001", 110, "2026-01-01 03:00:00"),
        _row("VINA0000000000001", 120, "2026-01-01 06:00:00"),
        _row("VINA0000000000001", 90, "2026-01-06 00:00:00"),
        _row("STABLEVIN000000001", 20000, "2026-01-01 00:00:00"),
        _row("VINC0000000000003", 50000, "2026-01-01 12:00:00"),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-01-07"})

    rows = _price_history_rows(dbt_project)
    by_vin = {r[0]: r for r in rows}
    a = by_vin["VINA0000000000001"]
    assert a[5] == 4, "late event must be picked up (total_price_observations=4)"
    assert a[6] == 1 and a[7] == 2, (
        "late event reorders the LAG() sequence: 100->110->120 (2 increases), 120->90 (1 drop)"
    )

    # --- corrected/duplicate event behavior is preserved, not newly invented ---
    # A duplicate row for VINA (same vin/price/event_at as its most recent
    # event, so it's still inside the lookback window) increases
    # total_price_observations, matching the pre-incremental model's
    # behavior (it never deduplicated raw price events either) — the
    # incremental conversion must not silently change this.
    _write_seed(dbt_project, [
        _row("VINA0000000000001", 100, "2026-01-01 00:00:00"),
        _row("VINA0000000000001", 110, "2026-01-01 03:00:00"),
        _row("VINA0000000000001", 120, "2026-01-01 06:00:00"),
        _row("VINA0000000000001", 90, "2026-01-06 00:00:00"),
        _row("VINA0000000000001", 90, "2026-01-06 00:00:00"),
        _row("STABLEVIN000000001", 20000, "2026-01-01 00:00:00"),
        _row("VINC0000000000003", 50000, "2026-01-01 12:00:00"),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-01-07"})

    rows = _price_history_rows(dbt_project)
    by_vin = {r[0]: r for r in rows}
    a = by_vin["VINA0000000000001"]
    assert a[5] == 5, "duplicate row is counted, matching prior full-table behavior"
    assert a[6] == 1 and a[7] == 2, (
        "the duplicate 90==90 pair is neither a drop nor an increase"
    )

    # --- drop/increase counts across the watermark boundary ---
    # VIN D has an old event well outside the 3-day lookback plus a brand new
    # event inside it. The affected-VIN replacement must reread VIN D's
    # COMPLETE history (not just the new event) to get the drop/increase
    # count right — a naive "only scan new events" implementation would miss
    # the old->new comparison entirely.
    _write_seed(dbt_project, [
        _row("VINA0000000000001", 100, "2026-01-01 00:00:00"),
        _row("VINA0000000000001", 110, "2026-01-01 03:00:00"),
        _row("VINA0000000000001", 120, "2026-01-01 06:00:00"),
        _row("VINA0000000000001", 90, "2026-01-06 00:00:00"),
        _row("VINA0000000000001", 90, "2026-01-06 00:00:00"),
        _row("STABLEVIN000000001", 20000, "2026-01-01 00:00:00"),
        _row("VINC0000000000003", 50000, "2026-01-01 12:00:00"),
        _row("VIND0000000000004", 40000, "2026-01-01 00:00:00"),
        _row("VIND0000000000004", 35000, "2026-01-07 00:00:00"),
    ])
    _dbt(dbt_project, "seed")
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-01-07"})

    rows = _price_history_rows(dbt_project)
    by_vin = {r[0]: r for r in rows}
    d = by_vin["VIND0000000000004"]
    assert d[1] == 35000 and d[5] == 2 and d[6] == 1 and d[7] == 0, (
        "VIN D's drop must be computed across the full history, spanning the watermark boundary"
    )

    # --- days_on_market stays fresh long after STABLE VIN drops out of the
    #     lookback window entirely (by this point the watermark has advanced
    #     to 2026-01-07, so STABLE's 2026-01-01 event is 6 days back — well
    #     outside the 3-day lookback, meaning int_price_history's
    #     affected-VIN logic is no longer reprocessing it at all) ---
    stable_row_before_jump = by_vin["STABLEVIN000000001"]
    assert _days_on_market(dbt_project)["STABLEVIN000000001"] == 6
    _dbt(dbt_project, "run", extra_vars={"as_of_date": "2026-02-01"})
    assert _days_on_market(dbt_project)["STABLEVIN000000001"] == 31, (
        "days_on_market must keep advancing with real elapsed time even though "
        "STABLE VIN is well outside int_price_history's incremental lookback window"
    )
    stable_row_after_jump = {
        r[0]: r for r in _price_history_rows(dbt_project)
    }["STABLEVIN000000001"]
    assert stable_row_after_jump == stable_row_before_jump, (
        "int_price_history's own row for STABLE VIN is unaffected by the as_of_date bump"
    )

    # --- incremental output equals a full-refresh over the same fixture ---
    incremental_rows = sorted(_price_history_rows(dbt_project))
    _dbt(dbt_project, "run", "--full-refresh", extra_vars={"as_of_date": "2026-01-07"})
    assert sorted(_price_history_rows(dbt_project)) == incremental_rows
