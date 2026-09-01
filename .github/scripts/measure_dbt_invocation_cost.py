"""TEMPORARY — Plan 162 Stage 4 / Plan 139 Stage C measurement. Delete after.

`tests/integration/dbt/` is 21 `dbt build --select` subprocesses at a measured
4.44s each, and 99.2% of the suite's 94s. Stage C's instruction is measurement
before proposal, so this decomposes one invocation into its parts and then
times the two things that could remove most of it:

  1. `dbtRunner().invoke(...)` in-process, which pays the Python + dbt import
     cost once for the process instead of once per invocation;
  2. `dbtRunner(manifest=...)`, which additionally reuses an already-parsed
     manifest instead of reading the partial-parse cache each time.

Run from the `dbt/` directory, after the job's real build, so nothing this
does can affect an assertion. Per the standing rule this is CI-only: it is
never run locally and never installs dbt on a developer machine.
"""

import os
import time

DBT_ARGS = ["--profiles-dir", ".", "--target", "duckdb"]
BUILD = ["build", "--select", "int_price_history", *DBT_ARGS]

# The hook needs this on every invocation regardless of --select, exactly as
# tests/integration/dbt/test_incremental_models_real_build.py defaults it.
os.environ.setdefault(
    "POSTGRES_URL", "postgresql://cartracker:cartracker@localhost:5432/cartracker"
)


def _row(label: str, seconds: float, note: str = "") -> None:
    print(f"  {label:<44} {seconds:7.2f}s  {note}")


def main() -> None:
    print("=== dbt per-invocation cost decomposition ===")

    start = time.perf_counter()
    from dbt.cli.main import dbtRunner

    _row("import dbt.cli.main", time.perf_counter() - start, "paid once per process")

    runner = dbtRunner()

    start = time.perf_counter()
    parsed = runner.invoke(["parse", *DBT_ARGS])
    _row("dbt parse (in-process)", time.perf_counter() - start, f"ok={parsed.success}")

    print("\n--- dbtRunner(), fresh manifest read each invoke ---")
    for attempt in range(1, 4):
        start = time.perf_counter()
        result = dbtRunner().invoke(BUILD)
        _row(f"invoke #{attempt}", time.perf_counter() - start, f"ok={result.success}")

    print("\n--- dbtRunner(manifest=...), manifest reused ---")
    manifest = parsed.result
    warm = dbtRunner(manifest=manifest)
    for attempt in range(1, 4):
        start = time.perf_counter()
        result = warm.invoke(BUILD)
        _row(f"invoke #{attempt}", time.perf_counter() - start, f"ok={result.success}")

    print(
        "\nCompare against the subprocess baseline in the same job: the "
        "--durations table above reports 21 invocations totalling 93.25s."
    )
    probe_duckdb_coexistence()


def probe_duckdb_coexistence() -> None:
    """The one thing that stands between the measurement and the change.

    Every real-build test reads its result with
    ``duckdb.connect(DUCKDB_PATH, read_only=True)``. Today that happens after
    a subprocess has exited, so nothing else holds the file. In-process,
    dbt-duckdb caches its environment so repeated invokes stay fast -- which
    means the adapter may still hold the same database open read-write when
    the test's read-only connect runs, and DuckDB refuses a second connection
    to one file under a different configuration.

    If read_only=True raises here, the change needs the read side adjusted;
    if it succeeds, `_run_dbt` can move to dbtRunner with no other edit.
    """
    import duckdb

    path = os.environ["DUCKDB_PATH"]
    print("\n--- DuckDB coexistence with dbt's in-process connection ---")
    print(f"  duckdb {duckdb.__version__}, path {path}")

    for label, kwargs in (
        ("connect(read_only=True)", {"read_only": True}),
        ("connect() read-write", {}),
    ):
        try:
            con = duckdb.connect(path, **kwargs)
            rows = con.execute(
                "select count(*) from main.int_price_history"
            ).fetchone()[0]
            con.close()
            print(f"  {label:<28} OK, int_price_history has {rows} rows")
        except Exception as exc:  # noqa: BLE001 - the failure mode is the finding
            print(f"  {label:<28} RAISED {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
