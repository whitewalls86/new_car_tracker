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
:class:`shared.query_loader.SqlText` carries its origin and preserves it
through ``.format()``. A statement that arrives as a plain ``str`` is recorded
with no origin rather than guessed at -- an unattributed execution is a visible
hole, and a wrong attribution is not.

**dbt is the one surface not wrapped, and does not need to be.** It runs in a
subprocess and already writes what it executed to ``target/run/`` beside
``run_results.json``. A declared second mechanism, not a hole.

**Two things this does not do**, recorded here rather than discovered at Gate
D. Spark's DataFrame API is not text, so it is invisible to a recorder that
records text -- exactly as
[G15](../../docs/TESTING.md#the-gap-list) already says for the static rule. And
the *cross-engine* assertion -- "this ran on the engine production uses for it"
-- needs two live engines to design honestly and belongs to Plan 125 Gate D.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The clients this plugin instruments. Compared in **both directions** against
#: `production_db_clients()` by `test_the_recorder_instruments_every_client_
#: production_reaches`, so a new engine's import fails the suite until it is
#: wrapped here or the contract says in writing why not. It is a constant
#: rather than a runtime probe on purpose: what a job happens to have installed
#: must not decide what the contract says is owed.
INSTRUMENTED_CLIENTS = frozenset({"psycopg2", "duckdb", "asyncpg", "pyspark"})

#: Filled during the run. ``(client, statement, origin)`` -- origin is the
#: ``.sql`` file when the statement came through ``shared.query_loader``.
_RECORDED: list[dict[str, str | None]] = []

#: Which of :data:`INSTRUMENTED_CLIENTS` were actually importable here. A job
#: without Spark records nothing for Spark and says so, rather than the absence
#: reading as "nothing executed".
_WRAPPED: set[str] = set()

_ARTIFACT_ENV = "SQL_EXECUTION_RECORD"


def _origin_of(statement: object) -> str | None:
    """The ``.sql`` file a statement came from, or None if it was typed."""
    origin = getattr(statement, "origin", None)
    if origin is None:
        return None
    try:
        return Path(origin).resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(origin)


def record(client: str, statement: object) -> None:
    if not isinstance(statement, str):
        return
    _RECORDED.append(
        {"client": client, "statement": str(statement), "origin": _origin_of(statement)}
    )


class _RecordingCursor:
    """A thin proxy. Everything but ``execute`` goes straight through.

    A proxy rather than a ``cursor_factory`` subclass because the suites pass
    their own factory -- ``RealDictCursor`` -- and a factory that replaced it
    would change what the tests receive. Delegation by ``__getattr__`` keeps
    that surface identical.
    """

    __slots__ = ("_cursor", "_client")

    def __init__(self, cursor: object, client: str) -> None:
        object.__setattr__(self, "_cursor", cursor)
        object.__setattr__(self, "_client", client)

    def execute(self, statement, *args, **kwargs):
        record(self._client, statement)
        return self._cursor.execute(statement, *args, **kwargs)

    def executemany(self, statement, *args, **kwargs):
        record(self._client, statement)
        return self._cursor.executemany(statement, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._cursor, name)

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
        return _RecordingCursor(self._connection.cursor(*args, **kwargs), self._client)

    def execute(self, statement, *args, **kwargs):
        # DuckDB executes on the connection; psycopg2 does not have this.
        record(self._client, statement)
        return self._connection.execute(statement, *args, **kwargs)

    def sql(self, statement, *args, **kwargs):
        record(self._client, statement)
        return self._connection.sql(statement, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._connection, name)

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


def _dbt_executions() -> list[dict[str, str | None]]:
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
            "origin": path.relative_to(REPO_ROOT).as_posix(),
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
        "executions": _RECORDED + _dbt_executions(),
    }
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
