"""
Layer 2 — SQL smoke tests for the statements the DAG tree owns.

``airflow/dags/`` does not import ``shared`` -- a decided exemption, not a gap,
since Plan 162 Stage N -- so the tree loads its statements through its own
``airflow/dags/dag_queries.py``, which reads the same ``airflow/sql/`` directory
every other service's ``queries.py`` mirrors. These tests read those files the
same way and execute them against Postgres with Flyway's migrations applied.

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
    """Plan 162 Stage L pulled this out of ``sensors.py``'s module scope.

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
        # No migration or fixture seeds access_requests, so the 48-hour
        # predicate matches nothing: the statement proves it plans without
        # clearing a notification address this test did not write.
        cur.execute(_sql("delete_stale_emails"))
        assert cur.rowcount == 0


class TestDeployIntentGate:
    """Plan 162 Stage N pulled this out of ``sensors.py``'s ``poke()``.

    It was the DAG tree's one inline statement and the last thing forcing an
    ``ast`` read of production source: ``test_coordination_admission.py``
    asserted ``"scope ? 'host'" in sensor_source``, which is a substring match
    against Python text and passes forever once written. Nothing executed it.

    Both singleton rows exist from Flyway's migrations and the ``cur`` fixture
    rolls back, so these write to them directly rather than seeding.
    """

    SURFACES = ["analytics", "processing"]

    def _poke(self, cur, surfaces=None):
        cur.execute(_sql("deploy_intent_gate"), (surfaces or self.SURFACES,))
        return cur.fetchone()

    @staticmethod
    def _coordinate(cur, phase, scope, generation=0):
        """Put coordination_state into a phase, respecting its own CHECK.

        V043's table constraint, as amended by V050, ties the columns together:
        a phase other than 'none' requires a non-null ``kind`` and a non-empty
        ``targets``. Setting ``phase`` and ``scope`` alone raises
        CheckViolation -- which is the reason to write these through Postgres
        rather than to reason about the statement. A fixture that seeded rows
        the real table would reject proves nothing about the real table.
        """
        cur.execute(
            "UPDATE coordination_state "
            "   SET phase = %s, scope = %s::jsonb, generation = %s, "
            "       kind = %s, targets = %s::jsonb "
            " WHERE id = 1",
            (
                phase,
                scope,
                generation,
                None if phase == "none" else "deploy",
                "[]" if phase == "none" else '["ops"]',
            ),
        )

    def test_the_gate_returns_its_four_columns_in_the_order_poke_indexes(self, cur):
        """The one assertion that would have caught a silent misread.

        ``_DeployIntentSensor.poke`` reads ``row[0]``..``row[3]`` because
        ``PostgresHook.get_first`` hands back a tuple, so the select list's
        *order* is production behaviour: swap the first two and
        ``row[0] != "none"`` starts testing ``phase``, which admits DAG runs
        during a deploy and fails as a corrupted run rather than as an error.
        Names alone would not catch it -- the order is the contract.
        """
        self._poke(cur)
        assert [column.name for column in cur.description] == [
            "intent",
            "phase",
            "intersects",
            "generation",
        ]

    def test_a_host_scoped_deploy_intersects_every_dag(self, cur):
        # 'host' is the whole-fleet scope: no DAG declares it as a surface, so
        # the `?` arm is the only thing that can admit it.
        self._coordinate(cur, "draining", '["host"]', generation=77)
        row = self._poke(cur)
        assert row["intersects"] is True
        assert row["phase"] == "draining"
        assert row["generation"] == 77

    def test_an_overlapping_scope_intersects_and_a_disjoint_one_does_not(self, cur):
        self._coordinate(cur, "active", '["processing", "archive"]')
        assert self._poke(cur)["intersects"] is True

        self._coordinate(cur, "active", '["listing_fetch"]')
        assert self._poke(cur)["intersects"] is False

    def test_an_empty_scope_intersects_nothing(self, cur):
        """V050's invariant, asserted against the engine rather than restated.

        V050 allows a non-'none' record to name no surface, and its own comment
        says why that is safe: "in airflow/dags/sensors.py `cs.scope ?| %s::
        text[]` is false against an empty array, so no DAG blocks on it". That
        sentence is a claim about *this* statement, and until now nothing
        executed it -- the migration's stated safety argument rested on a read
        of a Python string.
        """
        self._coordinate(cur, "requested", "[]")
        assert self._poke(cur)["intersects"] is False

    def test_deploy_intent_is_read_independently_of_coordination(self, cur):
        """The two holds are independent, which is why one row carries both."""
        cur.execute("UPDATE deploy_intent SET intent = %s WHERE id = 1", ("pause",))
        self._coordinate(cur, "none", "[]")
        row = self._poke(cur)
        assert row["intent"] == "pause"
        assert row["intersects"] is False
