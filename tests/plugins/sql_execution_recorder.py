"""What text executed against which client, recorded at the client.

Plan 162 Stage X, second half. Every rule in ``tests/test_testing_contract.py``
reads the repository *statically*: a ``.sql`` file counts as covered when a
Layer 2 module names its stem as a whole word. This plugin is what replaces
that reading with the strongest available one -- **this file's text executed in
this run** -- and the capture half has a deadline the aggregation half does
not, because a baseline taken after Plan 125 Gate D is not a baseline.

**Keyed on the client, not on the fixtures.** The first design keyed capture to
the fixtures that hand out connections -- ``cur``, ``viewer_cur``,
``duckdb_con``, ``duckdb_s3_con`` -- and the repository caught it before it was
written: that set sees ``psycopg2`` and ``duckdb`` and misses ``asyncpg`` in
``scraper/db.py``, exercised unmocked since Stage M, and ``pyspark`` entirely.
It would have shipped recording nothing for ``scraper/sql/`` and gone on
recording nothing when Spark arrives. That is ``_SQL_CALL_NAMES`` again, which
Stage N deleted rather than lengthened. Wrapping the library at its entry point
means every connection *any* fixture opens is recorded and the fixture list
stops existing.

**Attribution is exact rather than inferred**, because
:class:`shared.query_loader.SqlText` carries the files it was composed from
and preserves them through ``.format()`` -- including the origins of a
statement formatted *into* another, so a nested statement is credited to its
own file and not only to its wrapper. A statement that arrives as a plain
``str`` is recorded with no origins rather than guessed at -- an unattributed
execution is a visible hole, and a wrong attribution is not.

**dbt is the one surface not wrapped, and does not need to be.** It runs in a
subprocess and already writes what it executed to ``target/run/`` beside
``run_results.json``. A declared second mechanism, not a hole.

**Three things this does not do**, recorded here rather than discovered at
Gate D. Spark's DataFrame API is not text, so it is invisible to a recorder
that records text -- exactly as
[G15](../../docs/TESTING.md#the-gap-list) already says for the static rule. And
the *cross-engine* assertion -- "this ran on the engine production uses for it"
-- needs two live engines to design honestly and belongs to Plan 125 Gate D.

**And the entry points below are still a list of names, which is the half of
the F1 finding this file did not close.** The proxies stopped naming methods;
:func:`_install` still names *factories*, and ``asyncpg`` is a live miss:
``scraper/db.py`` opens its pool with ``asyncpg.create_pool``, not
``asyncpg.connect``, so nothing on that path is proxied while
``INSTRUMENTED_CLIENTS`` lists ``asyncpg`` as instrumented. It records nothing
today for a reason that is itself worth knowing -- the pool is created at
startup, closed at shutdown, and never queried -- so the miss is latent rather
than live. Closing it properly means deriving the entry points production
actually calls, the way ``test_every_production_import_is_classified`` derives
the imports; measured 2026-09-06 and left as a decision rather than taken
unasked, because the honest repair may be deleting the unused pool.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The clients this plugin instruments. Compared in **both directions** against
#: `production_db_clients()` by `test_the_recorder_instruments_every_client_
#: production_reaches`, so a new engine's import fails the suite until it is
#: wrapped here or the contract says in writing why not. It is a constant
#: rather than a runtime probe on purpose: what a job happens to have installed
#: must not decide what the contract says is owed.
INSTRUMENTED_CLIENTS = frozenset({"psycopg2", "duckdb", "asyncpg", "pyspark"})

#: Filled during the run. ``(client, statement, origins)`` -- origins are the
#: ``.sql`` files the statement was composed from, when it came through
#: ``shared.query_loader``, and empty when it did not.
_RECORDED: list[dict[str, object]] = []

#: Which of :data:`INSTRUMENTED_CLIENTS` were actually importable here. A job
#: without Spark records nothing for Spark and says so, rather than the absence
#: reading as "nothing executed".
_WRAPPED: set[str] = set()

_ARTIFACT_ENV = "SQL_EXECUTION_RECORD"


def _origins_of(statement: object) -> list[str]:
    """The ``.sql`` files a statement came from. Empty if it was typed.

    A list and not a single path because one executed string can be owned by
    more than one file: ``lake_snapshot_cohort`` formats a selector's statement
    into ``wrap_candidate_query.sql``, and crediting only the wrapper made the
    fourteen selectors read as never executed. ``SqlText.format`` unions the
    origins; this only flattens them to repo-relative strings for the record.
    """
    origins = getattr(statement, "origins", None)
    if not origins:
        return []
    flattened = []
    for origin in origins:
        try:
            flattened.append(Path(origin).resolve().relative_to(REPO_ROOT).as_posix())
        except ValueError:
            flattened.append(str(origin))
    return sorted(flattened)


def record(client: str, statement: object) -> None:
    if not isinstance(statement, str):
        return
    _RECORDED.append(
        {
            "client": client,
            "statement": str(statement),
            "origins": _origins_of(statement),
        }
    )


def _recording_attribute(target: object, name: str, client: str):
    """*target.name*, wrapped to record every string argument if it is callable.

    The whole of what replaced the method-name list. A non-callable attribute
    -- ``description``, ``rowcount`` -- is passed straight through, so the
    proxy stays transparent for everything that is not a call.
    """
    attribute = getattr(target, name)
    if not callable(attribute):
        return attribute

    def recording(*args, **kwargs):
        for value in (*args, *kwargs.values()):
            record(client, value)
        return attribute(*args, **kwargs)

    return recording


class _RecordingCursor:
    """A thin proxy that records **any string handed to any method**.

    A proxy rather than a ``cursor_factory`` subclass because the suites pass
    their own factory -- ``RealDictCursor`` -- and a factory that replaced it
    would change what the tests receive. Delegation by ``__getattr__`` keeps
    that surface identical.

    **Why it names no methods.** It used to define ``execute`` and
    ``executemany``, which is a list of database-client method names -- the
    ``_SQL_CALL_NAMES`` shape Stage N deleted rather than lengthened, sitting
    inside the instrument built to replace it. A driver method the list had not
    heard of recorded nothing, silently: ``asyncpg`` reaches a connection
    through ``fetch``/``fetchrow``/``fetchval``, none of which were named, and
    DuckDB has ``query`` and ``from_query`` beside ``execute``. Now the wrapper
    keys on the *argument* instead: every callable is wrapped, and every string
    argument is recorded, so a method nobody anticipated is covered by the rule
    rather than by an edit here.

    **What that trades**, stated rather than discovered later: "a string
    reached the driver" is marginally weaker than "a statement executed". A
    driver method that takes SQL *without* running it -- ``mogrify``,
    ``prepare`` -- would now be credited as an execution. Measured 2026-09-06:
    zero such call sites in production, and the coverage gate reads whole-file
    text matches, so a fragment cannot credit a file by accident.

    It also fixes what the transparency finding originally reported. The named
    methods were defined *unconditionally*, so a wrapped ``psycopg2``
    connection answered ``hasattr(conn, "sql")`` with True where the real one
    says False. Defining nothing means the proxy's surface is the wrapped
    object's surface, with no engine-specific conditional deciding it.
    """

    __slots__ = ("_cursor", "_client")

    def __init__(self, cursor: object, client: str) -> None:
        object.__setattr__(self, "_cursor", cursor)
        object.__setattr__(self, "_client", client)

    def __getattr__(self, name):
        return _recording_attribute(self._cursor, name, self._client)

    def __setattr__(self, name, value):
        setattr(self._cursor, name, value)

    def __iter__(self):
        return iter(self._cursor)

    def __enter__(self):
        self._cursor.__enter__()
        return self

    def __exit__(self, *exc):
        return self._cursor.__exit__(*exc)


class _RecordingConnection:
    """Same idea one level up, so every cursor a connection hands out is wrapped."""

    __slots__ = ("_connection", "_client")

    def __init__(self, connection: object, client: str) -> None:
        object.__setattr__(self, "_connection", connection)
        object.__setattr__(self, "_client", client)

    def cursor(self, *args, **kwargs):
        # The one name still spelled out, and it is here to **propagate the
        # proxy**, not to record: a cursor handed out unwrapped is a whole
        # surface the recorder cannot see. Recording happens in __getattr__.
        return _RecordingCursor(self._connection.cursor(*args, **kwargs), self._client)

    def __getattr__(self, name):
        return _recording_attribute(self._connection, name, self._client)

    def __setattr__(self, name, value):
        setattr(self._connection, name, value)

    def __enter__(self):
        entered = self._connection.__enter__()
        # psycopg2 returns the connection itself; DuckDB returns a connection.
        if entered is self._connection:
            return self
        return _RecordingConnection(entered, self._client)

    def __exit__(self, *exc):
        return self._connection.__exit__(*exc)


def _wrap_factory(module, attribute: str, client: str) -> None:
    """Replace ``module.attribute`` with one returning a recording connection."""
    original = getattr(module, attribute, None)
    if original is None or getattr(original, "_records_sql", False):
        return

    def wrapper(*args, **kwargs):
        return _RecordingConnection(original(*args, **kwargs), client)

    wrapper._records_sql = True  # type: ignore[attr-defined]
    wrapper.__name__ = getattr(original, "__name__", attribute)
    setattr(module, attribute, wrapper)


def _install() -> None:
    """Wrap every client that is importable here. Absence is recorded, not fatal."""
    try:
        import psycopg2

        _wrap_factory(psycopg2, "connect", "psycopg2")
        _WRAPPED.add("psycopg2")
    except ImportError:
        pass

    try:
        import duckdb

        _wrap_factory(duckdb, "connect", "duckdb")
        _WRAPPED.add("duckdb")
    except ImportError:
        pass

    try:
        import asyncpg

        _wrap_factory(asyncpg, "connect", "asyncpg")
        _WRAPPED.add("asyncpg")
    except ImportError:
        pass

    # ``execute_values`` is the one call in the tree that **composes a new
    # statement**: psycopg2 expands ``VALUES %s`` into ``VALUES (...),(...)``
    # internally and hands the cursor a plain ``str``, so the cursor proxy sees
    # a statement with no origin and the file reads as never executed.
    # `processing/sql/insert_silver_observations.sql` was exactly that.
    #
    # **It is wrapped here rather than added to a list of helpers**, and the
    # difference matters. Measured 2026-09-05: seven callees other than
    # ``.execute`` receive a loaded SQL constant across 27 sites, and five of
    # them are project-local helpers -- ``run_duckdb_query`` (19 sites),
    # ``_database_count``, ``_run_maintenance_query`` -- which no library entry
    # point can reach. Every one of those passes the constant *through* to
    # ``.execute`` unchanged, so the origin survives and there is nothing to
    # wrap. Only a composer breaks attribution, and there is one.
    #
    # What stops this becoming an inventory is not this wrapper but
    # ``no_execution_is_unattributable`` in the coverage gate: an unattributed
    # execution whose text matches a ``.sql`` file fails, whatever caused it.
    try:
        from psycopg2 import extras

        original_values = extras.execute_values
        if not getattr(original_values, "_records_sql", False):

            def execute_values(cur, sql, argslist, *args, **kwargs):
                record("psycopg2", sql)
                return original_values(cur, sql, argslist, *args, **kwargs)

            execute_values._records_sql = True  # type: ignore[attr-defined]
            extras.execute_values = execute_values
    except ImportError:
        pass

    try:
        from pyspark.sql import SparkSession

        original = SparkSession.sql
        if not getattr(original, "_records_sql", False):

            def spark_sql(self, statement, *args, **kwargs):
                record("pyspark", statement)
                return original(self, statement, *args, **kwargs)

            spark_sql._records_sql = True  # type: ignore[attr-defined]
            SparkSession.sql = spark_sql  # type: ignore[method-assign]
        _WRAPPED.add("pyspark")
    except ImportError:
        pass


def _dbt_executions() -> list[dict[str, object]]:
    """dbt's own record of what it ran, from ``target/run/``.

    The one execution surface that cannot be wrapped, because it is a
    subprocess -- and the one that does not need to be, because it already
    writes every compiled statement it executed to disk.
    """
    run_root = REPO_ROOT / "dbt" / "target" / "run"
    if not run_root.is_dir():
        return []
    return [
        {
            "client": "dbt",
            "statement": path.read_text(encoding="utf-8"),
            "origins": [path.relative_to(REPO_ROOT).as_posix()],
        }
        for path in sorted(run_root.rglob("*.sql"))
    ]


def pytest_configure(config) -> None:  # noqa: ARG001 - pytest hook signature
    _install()


def pytest_unconfigure(config) -> None:  # noqa: ARG001 - pytest hook signature
    """Write this job's slice of the record.

    **Per job, because a statement may execute in any of five CI jobs.** The
    aggregation that turns these slices into a coverage reading is what CAR-78
    settles the job definitions for, and it lands with or after Stage Q. What
    cannot wait is the capture, because its baseline has to be taken while
    DuckDB is still authoritative.
    """
    destination = os.environ.get(_ARTIFACT_ENV)
    if not destination:
        return
    payload = {
        "wrapped": sorted(_WRAPPED),
        "declared": sorted(INSTRUMENTED_CLIENTS),
        "invocation": " ".join(sys.argv[1:]),
        "executions": _RECORDED + _dbt_executions(),
    }
    path = Path(destination)
    if path.suffix != ".json":
        # **A directory, because a job runs pytest more than once.** The
        # `service-integration` job alone has five invocations; pointed at one
        # filename they would overwrite each other and the job would report its
        # last suite as its whole record. A file path is still honoured, which
        # is what the Stage X baseline recipe uses.
        path.mkdir(parents=True, exist_ok=True)
        path = path / f"record-{os.getpid()}-{len(_RECORDED)}.json"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
