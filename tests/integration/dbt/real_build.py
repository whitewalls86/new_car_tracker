"""Shared dbt invocation for the two real-build suites in this directory.

Not a test module. It exists because `test_incremental_models_real_build.py`
and `test_observation_fingerprints_real_build.py` carried byte-identical
`_dbt_env` / `_run_dbt` helpers, and because Plan 162 Stage 4 changed how that
invocation works and there should be one place it changed.

**Why in-process rather than a subprocess.** Measured in CI on 2026-09-01
(Plan 139 Stage C, which asked for exactly this measurement before any
change): the suite was 16 tests in 93.99s, of which the seven real-build tests
were 93.25s and the nine selector-equivalence tests were 0.44s. Those seven
drive 21 `dbt build --select` invocations, a mean 4.44s each — while dbt's own
report for one of those builds reads *"1 incremental model, 1 project hook, 12
data tests, 4 unit tests in 0.61 seconds"*. The gap was never the models:

    import dbt.cli.main                 1.37s   (once per process)
    dbtRunner().invoke(build)           1.19s - 1.24s
    dbtRunner(manifest=...).invoke      1.04s - 1.14s
    the same build as a subprocess      ~4.2s

So roughly 3s of every 4.4s was Python starting up and importing dbt, paid 21
times. Calling dbt in-process pays it once for the pytest process.

`dbtRunner()` is constructed per call rather than shared, and no manifest is
passed. That is the variant the numbers above were measured against; reusing a
parsed manifest is a further ~0.15s per invocation and would trade a measured
configuration for an unmeasured one to buy about three seconds.

**What did not change.** Every invocation is still a real `dbt build` against
the real project and the real DuckDB file, with no shadow project and no
`--full-refresh` where the test does not ask for one. Each test's subject is a
*sequence* of incremental builds with fixture data seeded between them, so the
builds cannot be collapsed into one; only their startup could be shared, and
that is all this does.
"""

import importlib.util
import os
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_DIR = REPO_ROOT / "dbt"

# register_upstream_external_models() (dbt-duckdb's on-run-start hook,
# dbt_project.yml) needs POSTGRES_URL on every invocation regardless of
# --select. The CI step that runs these tests doesn't export it (only the
# earlier one-shot "dbt build" step does), so default it to the same CI
# Postgres service credentials used elsewhere (tests/integration/conftest.py).
#
# Set on os.environ rather than passed to a subprocess, because there is no
# longer a subprocess to pass an environment to.
os.environ.setdefault(
    "POSTGRES_URL", "postgresql://cartracker:cartracker@localhost:5432/cartracker"
)


def dbt_is_installed() -> bool:
    """Cheap enough to evaluate at collection time.

    ``find_spec("dbt")`` searches the path without importing the package, so
    a machine with no dbt skips these tests without paying the 1.37s import
    that the check is asking about.
    """
    return importlib.util.find_spec("dbt") is not None


def run_dbt(*args):
    """Invoke dbt in this process and fail the test if it did not succeed.

    ``--project-dir`` replaces the subprocess's ``cwd=DBT_DIR``: pytest runs
    from the repository root, and dbt resolves the project, its ``target/``
    partial-parse cache and its ``logs/`` relative to this flag.
    """
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke([
        *args,
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--target", "duckdb",
    ])
    assert result.success, (
        f"dbt {' '.join(str(a) for a in args)} failed: {result.exception!r}"
    )
    return result


def analytics_con():
    """Open the DuckDB file the build writes, for reading.

    **This deliberately does not pass ``read_only=True``, and that is a real
    property given up rather than an oversight.** Every one of these
    connections was read-only until 2026-09-01, which guaranteed no assertion
    could mutate the warehouse it was inspecting.

    In-process dbt makes that impossible. dbt-duckdb caches its environment
    across invocations -- which is exactly why an invoke costs 1.2s instead of
    4.4s -- so the adapter holds this same file open read-write for the life
    of the pytest process, and DuckDB refuses a second connection to one file
    under a different configuration. Measured in CI rather than reasoned
    about, on duckdb 1.5.5:

        connect(read_only=True)   ConnectionException: Can't open a connection
                                  to same database file with a different
                                  configuration than existing connections
        connect()                 OK

    So the choice was the read-only guard or the 63 seconds, and it is
    recorded here in those terms. Nothing replaces the guard: these callers
    issue SELECTs by inspection only, which is weaker than the mechanism it
    replaced. The alternative considered and rejected was reading a copy of
    the file -- it restores the guard, but every assertion would then be made
    against a snapshot rather than the warehouse the build actually wrote,
    which trades a checkable property for an unfalsifiable one.
    """
    return duckdb.connect(os.environ["DUCKDB_PATH"])
