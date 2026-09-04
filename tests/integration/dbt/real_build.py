"""Shared dbt invocation for the two real-build suites in this directory.

Not a test module. It exists because `test_incremental_models_real_build.py`
and `test_observation_fingerprints_real_build.py` carried byte-identical
`_dbt_env` / `_run_dbt` helpers, and because Plan 162 Stage E changed how that
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


# DuckDB's own parser decides what a statement is; this list decides which
# kinds may run. Deny by default -- a statement type added by a future DuckDB
# is refused until someone reads it and adds it here, which is the direction
# that cannot quietly widen. COPY is the reason the allowlist is worth having
# beyond the obvious four: `COPY t TO 'file'` reads the warehouse and writes
# the filesystem, and no read-only *connection* would have stopped it either.
_READ_ONLY_STATEMENTS = frozenset({
    duckdb.StatementType.SELECT,
    duckdb.StatementType.EXPLAIN,
})


class ReadOnlyConnection:
    """A DuckDB connection that refuses to run anything but a read.

    **Why the guard is here and not on the connection.** Until 2026-09-01
    every reader in this directory opened the warehouse with
    ``read_only=True``, so no assertion could mutate what it inspected. Moving
    dbt in-process took that away: dbt-duckdb caches its environment across
    invocations -- exactly why an invoke costs 1.2s instead of 4.4s -- so the
    adapter holds this file open read-write for the life of the pytest
    process, and DuckDB will not open a second connection to one file under a
    different configuration.

    Two ways of keeping a read-only *connection* were tried against duckdb
    1.5.5, the version CI runs, and both are closed:

        connect(path, read_only=True)     ConnectionException: Can't open a
                                          connection to same database file
                                          with a different configuration
        :memory: + ATTACH (READ_ONLY)     BinderException: Unique file handle
                                          conflict

    A third, reading a copy of the file, works and was rejected on meaning
    rather than mechanism: every assertion would then describe a snapshot
    instead of the warehouse the build actually wrote.

    So the guard moved from the connection to the statement, which is a
    different mechanism for the same property and, on one axis, a stricter
    one -- it is the *statements* that were ever the risk, and a read-only
    connection never had an opinion about ``COPY ... TO``. Classification is
    ``duckdb.extract_statements``, the engine's own parser, so it is not a
    regex over SQL text and ``WITH ... SELECT`` needs no special case.

    What it does not cover, stated so the limit is known: a caller that
    reaches past this wrapper for a raw connection. Nothing in this directory
    does, and ``test_analytics_connection_guard.py`` is what notices if that
    changes.
    """

    def __init__(self, con):
        self._con = con

    def execute(self, sql, parameters=None):
        refuse_writes(sql)
        if parameters is None:
            self._con.execute(sql)
        else:
            self._con.execute(sql, parameters)
        return self

    def fetchall(self):
        return self._con.fetchall()

    def fetchone(self):
        return self._con.fetchone()

    @property
    def description(self):
        return self._con.description

    def close(self):
        self._con.close()


def refuse_writes(sql: str) -> None:
    """Raise unless every statement in ``sql`` is a read.

    Parsing here and again in ``execute`` costs microseconds on the queries
    this suite runs, and buys a refusal that names the statement type rather
    than a DuckDB error after the write has already landed.
    """
    offending = sorted(
        str(statement.type).removeprefix("StatementType.")
        for statement in duckdb.extract_statements(sql)
        if statement.type not in _READ_ONLY_STATEMENTS
    )
    if offending:
        raise AssertionError(
            f"the analytics connection is for reading: refused {offending} in "
            f"{sql.strip()[:120]!r}. These tests assert against the warehouse "
            f"dbt built; a statement that changes it makes every later "
            f"assertion in the session describe something else."
        )


def analytics_con() -> ReadOnlyConnection:
    """Open the DuckDB file the build writes, for reading only."""
    return ReadOnlyConnection(duckdb.connect(os.environ["DUCKDB_PATH"]))
