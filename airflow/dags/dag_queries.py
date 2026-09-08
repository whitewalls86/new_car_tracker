"""Saved SQL for the DAG tree.

Mirrors every service's ``queries.py`` -- statements live in ``.sql`` files and
are loaded once at import -- with one deliberate difference: the loader below
is local instead of ``shared.query_loader``.

That is a decided exemption, not an oversight. ``shared/query_loader.py`` is
two lines, but reaching it means mounting ``shared/`` into the Airflow
container, and the tree holds ``minio``, ``duckdb_s3``, ``iceberg_catalog`` and
``packfile`` -- modules needing boto3, duckdb and pyiceberg, none of which are
in ``airflow/requirements.txt``. Mounting it would put imports on the DAG
tree's path that resolve in the main venv and fail *at DAG-parse time in
production*. The isolation is the same one that forces a separate Airflow venv
in CI. Two duplicated lines is the cheaper side of that trade. See
docs/TESTING.md, "where SQL lives".

``airflow/sql`` is bind-mounted to ``/opt/airflow/sql`` alongside
``/opt/airflow/dags``, so the path arithmetic below holds in the container and
in the checkout, and a new ``.sql`` file lands on a ``git pull`` with no image
rebuild.

**``dag_queries`` and not ``queries``, which is the name every service uses.**
Airflow puts ``dags/`` on ``sys.path`` directly, so a module here competes in
the *top-level* namespace -- and bare ``queries`` is already taken by
``scraper/queries.py``, which is imported flat because its Dockerfile does
``WORKDIR /app; COPY scraper/ .``. Named ``queries``, this module imported
clean on its own and, in a full-suite run that had already put ``scraper/`` on
the path, ``from queries import DEPLOY_INTENT_GATE_SQL`` resolved to the
scraper's module and raised ImportError. That is the dual import identity
``docs/TESTING.md`` records as G18, reached from a second direction. The
prefix costs nothing and the collision is silent, so keep it.

This module imports nothing but the standard library on purpose: it is the one
part of the DAG tree a reader without Airflow installed can import.
"""
from collections.abc import Iterable
from pathlib import Path

_SQL_DIR = Path(__file__).parent.parent / "sql"


class SqlText(str):
    """``shared.query_loader.SqlText``, duplicated here for the same reason
    :func:`load_query` is: this module may not import ``shared``.

    **Plan 162 Stage X, and the recorder is what found it.** The execution
    recorder attributes a statement to its file through this attribute, and
    these three were the only statements in the repository reaching an engine
    with no origin on them -- nine executions in one suite, invisible to every
    coverage reading that will be built on the record. The duplication is the
    cost already accepted for this module existing at all; carrying it here is
    cheaper than mounting ``shared/`` into the Airflow image, which is the
    trade [G12](../../docs/TESTING.md#the-gap-list) settled.

    ``origins`` is a set and not a single path for the reason
    ``shared/query_loader.py`` states at length: a statement formatted into
    another belongs to both files. Kept identical to the original because a
    copy that drifts is worse than a copy.
    """

    origins: frozenset[Path]

    def __new__(cls, text: str, origins: Iterable[Path]) -> "SqlText":
        instance = super().__new__(cls, text)
        instance.origins = frozenset(origins)
        return instance

    def format(self, *args: object, **kwargs: object) -> "SqlText":
        nested = [
            value.origins
            for value in (*args, *kwargs.values())
            if isinstance(value, SqlText)
        ]
        return SqlText(str.format(self, *args, **kwargs), self.origins.union(*nested))


def load_query(name: str) -> SqlText:
    path = _SQL_DIR / f"{name}.sql"
    return SqlText(path.read_text(encoding="utf-8"), (path,))


DEPLOY_INTENT_GATE_SQL = load_query("deploy_intent_gate")
GATE_OBSERVATION_SQL = load_query("record_gate_observation")
DELETE_STALE_EMAILS_SQL = load_query("delete_stale_emails")
