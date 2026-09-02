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

This module imports nothing but ``pathlib`` on purpose: it is the one part of
the DAG tree a reader without Airflow installed can import.
"""
from pathlib import Path

_SQL_DIR = Path(__file__).parent.parent / "sql"


def load_query(name: str) -> str:
    return (_SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


DEPLOY_INTENT_GATE_SQL = load_query("deploy_intent_gate")
GATE_OBSERVATION_SQL = load_query("record_gate_observation")
DELETE_STALE_EMAILS_SQL = load_query("delete_stale_emails")
