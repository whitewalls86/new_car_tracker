"""
Layer 2 — SQL smoke tests for the statements the DAG tree owns.

``airflow/dags/`` may not import ``shared`` (G12), so its modules read their
own ``.sql`` files by path rather than through ``shared.query_loader``. These
tests read the same files the same way and execute them against Postgres with
Flyway's migrations applied.

**Loading the file rather than importing the DAG module is deliberate.** The
airflow modules need ``apache-airflow``, which lives in its own CI venv because
its starlette/fastapi pins conflict with the services'. Reading the file keeps
these tests in the main Layer 2 job, and the anti-paraphrase property survives
intact: there is still exactly one copy of each statement, and this executes
that copy. What it does not prove is that the DAG module loads the file it
should -- ``tests/integration/airflow/`` owns that half.
"""
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_SQL_DIR = Path(__file__).resolve().parents[3] / "airflow" / "sql"


def _sql(name: str) -> str:
    return (_SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")


class TestGateObservationStatement:
    """Plan 162 Stage 7 pulled this out of ``sensors.py``'s module scope.

    It was a module-level constant, so no rule saw it: not at an ``.execute()``
    call site, and in no ``.sql`` file for the Layer 2 census to count.
    """

    def test_record_gate_observation_inserts_then_upserts(self, cur):
        sql = _sql("record_gate_observation")
        generation, dag_id, run_id = 999_999, "test_dag", str(uuid.uuid4())

        cur.execute(sql, (generation, dag_id, run_id))
        assert cur.rowcount == 1

        # A sensor pokes repeatedly; re-entry must refresh the row, not add one.
        cur.execute(sql, (generation, dag_id, run_id))
        assert cur.rowcount == 1
        cur.execute(
            "SELECT count(*) AS n FROM coordination_gate_observations "
            "WHERE generation = %s AND dag_id = %s AND run_id = %s",
            (generation, dag_id, run_id),
        )
        assert cur.fetchone()["n"] == 1


class TestStaleEmailCleanup:
    """Waived under G14 since the census: a `.sql` file no layer executed."""

    def test_delete_stale_emails_matching_nothing(self, cur):
        cur.execute(_sql("delete_stale_emails"))
