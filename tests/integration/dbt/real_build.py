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
import sys
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


_COMPILED = DBT_DIR / "target" / "compiled"


def attached_warehouse(alias: str | None = None):
    """An in-memory database with the warehouse attached **read-only**.

    **The alias defaults to the warehouse file's own stem, and has to.** dbt
    names the catalog after ``DUCKDB_PATH``, so every compiled statement in
    ``target/`` says ``"<stem>"."main"."<relation>"`` -- attaching under any
    other name leaves all of that unresolvable, and a caller replaying compiled
    SQL would have to rewrite every relation reference in it rather than only
    the one it means to redirect.

    Plan 162 Stage S. Two gates need to read what ``dbt build`` produced, and
    one of them -- the constraint mutation gate -- also needs somewhere to
    materialize a mutated model so it can run that model's own data tests
    against it. Writing the mutant into the warehouse is not an option: these
    suites assert against the build, and a relation created mid-run makes every
    later assertion describe something else.

    So the warehouse is attached ``READ_ONLY`` and the mutants live in the
    in-memory database instead. **The immutability comes from the attach mode
    rather than from a wrapper somebody has to remember to use**, which is the
    difference that matters -- :class:`ReadOnlyConnection` protects the same
    property by inspecting statements, and only for callers who go through it.

    Mutants are disposable by construction: they exist in a process-local
    database that vanishes when the connection closes, so a crashed run leaves
    no stray relation behind for the census tests to trip over.

    **The one precondition**: no dbt invocation may have run in this process.
    dbt-duckdb caches its adapter and holds the warehouse open read-write for
    the life of the interpreter, and DuckDB refuses a second handle to one file
    under a different configuration. :func:`assert_no_in_process_dbt` states
    that rather than leaving it to a lock error nobody can read.
    """
    warehouse = Path(os.environ["DUCKDB_PATH"])
    alias = alias or warehouse.stem
    connection = duckdb.connect(":memory:")
    _configure_s3(connection)
    # ATTACH takes no bind parameter -- DuckDB's parser rejects `ATTACH ?`
    # outright -- so the path is inlined, with `'` doubled the way SQL asks.
    path = str(warehouse).replace("'", "''")
    connection.execute(f"ATTACH '{path}' AS {alias} (READ_ONLY)")
    connection.execute(f"USE {alias}.main")
    return connection


def assert_no_in_process_dbt() -> None:
    """Fail with the reason, rather than with a lock error six frames deep."""
    if "dbt.adapters.duckdb" in sys.modules:
        raise AssertionError(
            "dbt has been invoked in this process, so it holds DUCKDB_PATH open "
            "read-write and no second handle can be opened under a different "
            "configuration. This suite must run in its own pytest invocation, "
            "separate from the real-build suites that call run_dbt()."
        )


def _configure_s3(connection) -> None:
    """Point a connection at MinIO, so Parquet-backed views resolve.

    Four of the 23 models materialize as views over Parquet rather than as
    tables, so a connection without these settings reads every mart and fails on
    every ``stg_*``.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if not endpoint:
        return
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(
        "SET s3_endpoint=?",
        [endpoint.replace("https://", "").replace("http://", "")],
    )
    connection.execute("SET s3_access_key_id=?",
                       [os.environ.get("MINIO_ROOT_USER", "cartracker")])
    connection.execute("SET s3_secret_access_key=?",
                       [os.environ.get("MINIO_ROOT_PASSWORD", "")])
    connection.execute("SET s3_use_ssl=?", [endpoint.startswith("https://")])
    connection.execute("SET s3_url_style=?", ["path"])


def compiled_in_both_phases() -> tuple[Path, Path]:
    """``(cold, incremental)`` compiled trees, captured after one build.

    ``is_incremental()`` is false when a model's relation does not exist, so a
    compile against an empty warehouse renders the cold form and a compile after
    a build renders the incremental one -- and **the two differ for seven of the
    23 models**, by the 39 lines of Plan 123 late-arrival lookback that only the
    incremental form contains. Enumerating from one of them alone silently omits
    the other's branches.

    Capturing both would ordinarily mean dropping the models between compiles.
    ``--full-refresh`` avoids that: it renders the cold form even where the
    relation exists, so both trees come from one warehouse and one build, and
    neither compile touches data.

    Each tree is copied out because ``dbt compile`` overwrites ``target/``, so
    the second run would otherwise destroy the first.
    """
    import shutil
    import tempfile

    captured = []
    for extra in (["--full-refresh"], []):
        run_dbt("compile", *extra)
        parent = Path(tempfile.mkdtemp(prefix="dbt-compiled-"))
        destination = parent / "compiled"
        shutil.copytree(_COMPILED, destination)
        # The manifest rides along because `compiled_model_paths` reads the
        # model list from it rather than from filename convention, and it walks
        # up from the compiled root to find it -- same rule as dbt/target/.
        shutil.copy2(_COMPILED.parent / "manifest.json", parent / "manifest.json")
        captured.append(destination / "cartracker" / "models")
    return captured[0], captured[1]


def analytics_con() -> ReadOnlyConnection:
    """Open the DuckDB file the build writes, for reading only.

    **S3 is configured before the read-only wrapper goes on**, and it has to be
    in that order: ``SET`` is not a ``SELECT``, so the wrapper refuses it.

    Four of the 23 models materialize as views over Parquet in MinIO rather than
    as tables in this file, so a connection without the httpfs settings can read
    every mart and fails on every ``stg_*``. That was latent until Plan 162
    Stage S queried the staging models -- and it failed in the worst available
    way, because the caller saw an exception per branch rather than one obvious
    error, and a caller that catalogued those exceptions would report a smaller
    denominator instead of a broken connection.
    """
    connection = duckdb.connect(os.environ["DUCKDB_PATH"])
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if endpoint:
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
        connection.execute(
            "SET s3_endpoint=?",
            [endpoint.replace("https://", "").replace("http://", "")],
        )
        connection.execute("SET s3_access_key_id=?",
                           [os.environ.get("MINIO_ROOT_USER", "cartracker")])
        connection.execute("SET s3_secret_access_key=?",
                           [os.environ.get("MINIO_ROOT_PASSWORD", "")])
        connection.execute("SET s3_use_ssl=?", [endpoint.startswith("https://")])
        connection.execute("SET s3_url_style=?", ["path"])
    return ReadOnlyConnection(connection)
