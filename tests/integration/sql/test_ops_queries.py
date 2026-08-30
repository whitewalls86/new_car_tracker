"""
Layer 1 — SQL smoke tests for ops service queries.

Every query the ops service runs against Postgres is executed here against a real
DB with Flyway migrations applied. The goal is to catch schema breakage (column
renames, dropped tables, type mismatches) — not to validate business logic.
"""
import ast
import uuid
from pathlib import Path

import pytest

from ops import coordination_drain

pytestmark = pytest.mark.integration


def _sensor_constant(name: str) -> str:
    """Read a module-level constant out of airflow/dags/sensors.py.

    This suite runs in the main venv, which has no Airflow, so sensors.py
    cannot be imported here -- but the statement under test is the sensor's
    own, verbatim, not a copy that can drift away from it.
    """
    source = (Path(__file__).parents[3] / "airflow" / "dags" / "sensors.py").read_text()
    return next(
        node.value.value
        for node in ast.parse(source).body
        if isinstance(node, ast.Assign)
        and any(t.id == name for t in node.targets if isinstance(t, ast.Name))
    )


GATE_OBSERVATION_SQL = _sensor_constant("GATE_OBSERVATION_SQL")

# The surfaces a full-fleet deploy expands to; see ops/coordination_contract.py.
DEPLOY_SCOPE = frozenset(
    {"analytics", "archive", "detail_fetch", "listing_fetch", "observability", "processing"}
)


# ============================================================================
# admin.py — search config queries
# ============================================================================

class TestSearchConfigQueries:

    def test_list_searches(self, cur):
        cur.execute("""
            SELECT search_key, enabled, source, params,
                   rotation_order, last_queued_at, created_at, updated_at
            FROM search_configs
            ORDER BY enabled DESC, rotation_order NULLS LAST, search_key
        """)
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_get_search_by_key(self, cur, seed_search_config):
        cur.execute(
            "SELECT search_key, enabled, source, params, rotation_order, last_queued_at"
            " FROM search_configs WHERE search_key = %s",
            (seed_search_config,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_insert_search_config(self, cur):
        key = f"smoke-{uuid.uuid4().hex[:8]}"
        cur.execute(
            """
            INSERT INTO search_configs
                (search_key, enabled, params, rotation_order, rotation_slot, created_at, updated_at)
            VALUES (%s, %s, %s::jsonb, %s, %s, now(), now())
            """,
            (key, True, '{"makes": ["test"]}', 1, 0),
        )
        assert cur.rowcount == 1

    def test_update_search_config(self, cur, seed_search_config):
        cur.execute(
            """
            UPDATE search_configs
            SET enabled = %s, params = %s::jsonb, rotation_order = %s,
                rotation_slot = %s, updated_at = now()
            WHERE search_key = %s
            """,
            (False, '{"makes": ["updated"]}', 2, 1, seed_search_config),
        )
        assert cur.rowcount == 1

    def test_toggle_search_config(self, cur, seed_search_config):
        cur.execute(
            "UPDATE search_configs SET enabled = NOT enabled, updated_at = now()"
            " WHERE search_key = %s",
            (seed_search_config,),
        )
        assert cur.rowcount == 1

    def test_soft_delete_search_config(self, cur, seed_search_config):
        deleted_key = f"deleted_{seed_search_config}"
        cur.execute(
            "UPDATE search_configs SET enabled = false, search_key = %s, updated_at = now()"
            " WHERE search_key = %s",
            (deleted_key, seed_search_config),
        )
        assert cur.rowcount == 1


# ============================================================================
# deploy.py — deploy intent queries
# ============================================================================

class TestDeployIntentQueries:

    def test_intent_status(self, cur):
        cur.execute("""
            WITH pending_artifacts AS (
                SELECT COUNT(*) AS number_running,
                       MIN(created_at) AS min_started_at
                FROM ops.artifacts_queue
                WHERE status IN ('pending', 'processing')
            ), running_detail_claims AS (
                SELECT COUNT(*) AS number_running,
                       MIN(claimed_at) AS min_started_at
                FROM ops.detail_scrape_claims
                WHERE status = 'running'
            )
            SELECT di.intent, di.requested_at, di.requested_by,
                   pa.number_running + rdc.number_running AS number_running,
                   LEAST(pa.min_started_at, rdc.min_started_at) AS min_started_at
            FROM deploy_intent di
            LEFT JOIN pending_artifacts pa ON 1=1
            LEFT JOIN running_detail_claims rdc ON 1=1
            WHERE di.id = 1
        """)
        row = cur.fetchone()
        assert row is not None

    def test_set_intent(self, cur):
        cur.execute(
            """UPDATE deploy_intent
               SET intent = 'pending', requested_at = now(), requested_by = %s
               WHERE id = 1
                 AND (intent = 'none'
                      OR requested_at < now() - interval '%s minutes')
               RETURNING intent""",
            ("smoke_test", 30),
        )
        row = cur.fetchone()
        assert row is not None

    def test_release_intent(self, cur):
        cur.execute("""
            UPDATE deploy_intent
            SET intent = 'none', requested_at = NULL, requested_by = NULL
            WHERE id = 1
            RETURNING intent
        """)
        row = cur.fetchone()
        assert row is not None


# ============================================================================
# auth.py — auth check query
# ============================================================================

class TestAuthQueries:

    def test_auth_check_lookup(self, cur, seed_authorized_user):
        _user_id, email_hash = seed_authorized_user
        cur.execute(
            "SELECT role FROM authorized_users WHERE email_hash = %s",
            (email_hash,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_auth_check_miss(self, cur):
        cur.execute(
            "SELECT role FROM authorized_users WHERE email_hash = %s",
            ("nonexistent_hash",),
        )
        row = cur.fetchone()
        assert row is None


# ============================================================================
# users.py — user management queries
# ============================================================================

class TestUserManagementQueries:

    def test_list_authorized_users(self, cur):
        cur.execute("""
            SELECT id, email_hash, role, display_name, created_at
            FROM authorized_users ORDER BY role, created_at
        """)
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_update_user_role(self, cur, seed_authorized_user):
        user_id, _hash = seed_authorized_user
        cur.execute(
            "UPDATE authorized_users SET role = %s WHERE id = %s",
            ("observer", user_id),
        )
        assert cur.rowcount == 1

    def test_revoke_user(self, cur, seed_authorized_user):
        user_id, _hash = seed_authorized_user
        cur.execute("DELETE FROM authorized_users WHERE id = %s", (user_id,))
        assert cur.rowcount == 1

    def test_list_access_requests(self, cur):
        cur.execute("""
            SELECT id, email_hash, display_name, requested_role, requested_at, status,
                   resolved_at, resolved_by
            FROM access_requests
            ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, requested_at DESC
        """)
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_get_pending_request_details(self, cur, seed_access_request):
        req_id, _hash = seed_access_request
        cur.execute(
            """SELECT email_hash, requested_role, display_name, notification_email
               FROM access_requests WHERE id = %s AND status = 'pending'""",
            (req_id,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_approve_access_request(self, cur, seed_access_request):
        req_id, email_hash = seed_access_request
        admin_hash = "admin_approver_hash"
        # Upsert into authorized_users
        cur.execute(
            """INSERT INTO authorized_users (email_hash, role, display_name, created_by)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (email_hash) DO UPDATE
                   SET role = EXCLUDED.role, created_by = EXCLUDED.created_by""",
            (email_hash, "viewer", "Approved User", admin_hash),
        )
        # Update request status
        cur.execute(
            """UPDATE access_requests
               SET status = 'approved', resolved_at = now(), resolved_by = %s,
                   notification_email = NULL
               WHERE id = %s""",
            (admin_hash, req_id),
        )
        assert cur.rowcount == 1

    def test_deny_access_request(self, cur, seed_access_request):
        req_id, _hash = seed_access_request
        cur.execute(
            """UPDATE access_requests
               SET status = 'denied', resolved_at = now(), resolved_by = %s,
                   notification_email = NULL
               WHERE id = %s AND status = 'pending'""",
            ("admin_hash", req_id),
        )
        assert cur.rowcount == 1

    def test_check_pending_access_request(self, cur, seed_access_request):
        _req_id, email_hash = seed_access_request
        cur.execute(
            "SELECT status FROM access_requests"
            " WHERE email_hash = %s AND status = 'pending'"
            " ORDER BY requested_at DESC LIMIT 1",
            (email_hash,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_insert_access_request(self, cur):
        email_hash = f"newhash_{uuid.uuid4().hex[:12]}"
        cur.execute(
            """INSERT INTO access_requests
                   (email_hash, requested_role, display_name, notification_email)
               VALUES (%s, %s, %s, %s)""",
            (email_hash, "observer", "New User", None),
        )
        assert cur.rowcount == 1

    def test_get_notification_email(self, cur, seed_access_request):
        req_id, _hash = seed_access_request
        cur.execute(
            """SELECT notification_email FROM access_requests
               WHERE id = %s AND status = 'pending'""",
            (req_id,),
        )
        row = cur.fetchone()
        assert row is not None


# ============================================================================
# Plan 97 — artifacts_queue schema smoke tests
# ============================================================================

class TestArtifactsQueueSchema:
    """Layer 1 smoke tests: verify ops.artifacts_queue table and constraints exist."""

    def test_table_exists_and_has_expected_columns(self, cur):
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'artifacts_queue'
            ORDER BY ordinal_position
        """)
        cols = {row["column_name"] for row in cur.fetchall()}
        for expected in ("artifact_id", "minio_path", "artifact_type", "status", "created_at"):
            assert expected in cols, f"ops.artifacts_queue missing column: {expected}"

    def test_minio_path_is_not_nullable(self, cur):
        cur.execute("""
            SELECT is_nullable FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'artifacts_queue'
              AND column_name = 'minio_path'
        """)
        row = cur.fetchone()
        assert row is not None
        assert row["is_nullable"] == "NO"

    def test_insert_valid_row_succeeds(self, cur):
        minio_path = f"s3://bronze/html/year=2026/month=4/artifact_type=results_page/{uuid.uuid4()}.html.zst"
        cur.execute(
            """INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
               VALUES (%s, 'results_page', now(), 'pending') RETURNING artifact_id""",
            (minio_path,),
        )
        row = cur.fetchone()
        assert row["artifact_id"] is not None

    def test_status_check_constraint_rejects_invalid_value(self, cur):
        import psycopg2
        minio_path = f"s3://bronze/test/{uuid.uuid4()}.html.zst"
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                """INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
                   VALUES (%s, 'results_page', now(), 'invalid_status')""",
                (minio_path,),
            )

    def test_artifact_type_check_constraint_rejects_invalid_value(self, cur):
        import psycopg2
        minio_path = f"s3://bronze/test/{uuid.uuid4()}.html.zst"
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute(
                """INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
                   VALUES (%s, 'bad_type', now(), 'pending')""",
                (minio_path,),
            )

# ============================================================================
# Plan 98 — staging.artifacts_queue_events schema smoke tests
# ============================================================================

class TestArtifactsQueueEventsSchema:
    """Layer 1 smoke tests: verify staging.artifacts_queue_events exists."""

    def _insert_queue_row(self, cur) -> int:
        minio_path = f"s3://bronze/html/year=2026/month=4/artifact_type=results_page/{uuid.uuid4()}.html.zst"
        cur.execute(
            """INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
               VALUES (%s, 'results_page', now(), 'pending') RETURNING artifact_id""",
            (minio_path,),
        )
        return cur.fetchone()["artifact_id"]

    def test_table_exists_and_has_expected_columns(self, cur):
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'staging' AND table_name = 'artifacts_queue_events'
            ORDER BY ordinal_position
        """)
        cols = {row["column_name"] for row in cur.fetchall()}
        for expected in ("event_id", "artifact_id", "status", "event_at",
                         "minio_path", "artifact_type", "fetched_at", "listing_id", "run_id"):
            assert expected in cols, f"staging.artifacts_queue_events missing column: {expected}"

    def test_insert_event_row_succeeds(self, cur):
        artifact_id = self._insert_queue_row(cur)
        minio_path = f"s3://bronze/html/year=2026/month=4/artifact_type=results_page/{uuid.uuid4()}.html.zst"
        cur.execute(
            """INSERT INTO artifacts_queue_events
                   (artifact_id, status, minio_path, artifact_type, fetched_at)
               VALUES (%s, 'pending', %s, 'results_page', now())
               RETURNING event_id""",
            (artifact_id, minio_path),
        )
        row = cur.fetchone()
        assert row["event_id"] is not None

    def test_event_at_defaults_to_now(self, cur):
        artifact_id = self._insert_queue_row(cur)
        minio_path = f"s3://bronze/test/{uuid.uuid4()}.html.zst"
        cur.execute(
            """INSERT INTO artifacts_queue_events
                   (artifact_id, status, minio_path, artifact_type)
               VALUES (%s, 'pending', %s, 'results_page')
               RETURNING event_at""",
            (artifact_id, minio_path),
        )
        row = cur.fetchone()
        assert row["event_at"] is not None

    def test_multiple_events_per_artifact(self, cur):
        artifact_id = self._insert_queue_row(cur)
        minio_path = f"s3://bronze/test/{uuid.uuid4()}.html.zst"
        for status in ("pending", "processing", "complete"):
            cur.execute(
                """INSERT INTO artifacts_queue_events
                       (artifact_id, status, minio_path, artifact_type)
                   VALUES (%s, %s, %s, 'results_page')""",
                (artifact_id, status, minio_path),
            )
        cur.execute(
            "SELECT COUNT(*) as cnt FROM artifacts_queue_events WHERE artifact_id = %s",
            (artifact_id,),
        )
        assert cur.fetchone()["cnt"] == 3

    def test_both_inserts_in_same_transaction(self, cur):
        """Verifies the scraper write pattern: artifacts_queue + event in one transaction."""
        minio_path = f"s3://bronze/test/{uuid.uuid4()}.html.zst"
        cur.execute(
            """INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
               VALUES (%s, 'detail_page', now(), 'pending') RETURNING artifact_id""",
            (minio_path,),
        )
        artifact_id = cur.fetchone()["artifact_id"]
        cur.execute(
            """INSERT INTO artifacts_queue_events
                   (artifact_id, status, minio_path, artifact_type, fetched_at)
               VALUES (%s, 'pending', %s, 'detail_page', now())""",
            (artifact_id, minio_path),
        )
        cur.execute(
            "SELECT status FROM artifacts_queue_events WHERE artifact_id = %s",
            (artifact_id,),
        )
        assert cur.fetchone()["status"] == "pending"


# ============================================================================
# coordination_drain.py — Plan 142 Stage 1 drain evidence queries
# ============================================================================

class TestCoordinationDrainQueries:
    """
    These execute against a real schema because the unit tests cannot.

    `tests/ops/test_coordination_drain.py` patches `_database_count`, so every
    drain query string is asserted without ever reaching a database. Three of
    them shipped naming the wrong schema and the defect was invisible: the ops
    role's search_path is `ops, staging, public`, `detail_scrape_claims` lives
    in `ops` and the two Airflow tables live in `airflow`. `_database_count`
    catches the resulting error and returns `unknown`, and unknown fails
    closed, so the first production deploy drained forever instead of failing.
    """

    def test_running_detail_claims_resolves(self, cur):
        cur.execute(coordination_drain.RUNNING_DETAIL_CLAIMS_SQL)
        row = cur.fetchone()
        assert row is not None and row["count"] >= 0

    def test_airflow_task_instance_query_resolves(self, cur, airflow_metadata_standin):
        query = coordination_drain.task_instance_query(DEPLOY_SCOPE)
        assert query is not None, "the deploy scope must drain some task instances"
        cur.execute(*query)
        assert cur.fetchone() is not None

    def test_gate_observation_query_resolves(self, cur, airflow_metadata_standin):
        query = coordination_drain.gate_observation_query(DEPLOY_SCOPE, 1)
        assert query is not None, "the deploy scope must cover some admission DAGs"
        cur.execute(*query)
        assert cur.fetchone() is not None

    # --- Plan 158: the seam. The two statements above and below never met.
    # --- `coordination_gate_observations` was empty for every generation that
    # --- had ever existed, because the sensor's INSERT sat below a return that
    # --- always fired during a deploy. These execute the sensor's real write
    # --- against the drain's real count.

    def test_gate_observation_count_falls_to_zero_as_live_runs_observe(
        self, cur, airflow_metadata_standin
    ):
        generation = 158
        query = coordination_drain.gate_observation_query(DEPLOY_SCOPE, generation)
        affected = query[1][:-1]
        runs = [(dag_id, f"{dag_id}-{uuid.uuid4().hex[:8]}") for dag_id in affected]
        for dag_id, run_id in runs:
            cur.execute(
                "INSERT INTO airflow.dag_run (dag_id, run_id, state, start_date)"
                " VALUES (%s, %s, 'running', now())",
                (dag_id, run_id),
            )

        cur.execute(*query)
        assert cur.fetchone()["count"] == len(runs), "every live affected run blocks"

        for dag_id, run_id in runs:
            cur.execute(GATE_OBSERVATION_SQL, (generation, dag_id, run_id))

        cur.execute(*query)
        assert cur.fetchone()["count"] == 0, "an observed run no longer blocks the drain"

    def test_an_observation_does_not_satisfy_the_next_generation(
        self, cur, airflow_metadata_standin
    ):
        dag_id = coordination_drain.gate_observation_query(DEPLOY_SCOPE, 1)[1][0]
        run_id = f"{dag_id}-{uuid.uuid4().hex[:8]}"
        cur.execute(
            "INSERT INTO airflow.dag_run (dag_id, run_id, state, start_date)"
            " VALUES (%s, %s, 'running', now())",
            (dag_id, run_id),
        )
        cur.execute(GATE_OBSERVATION_SQL, (158, dag_id, run_id))

        cur.execute(*coordination_drain.gate_observation_query(DEPLOY_SCOPE, 158))
        observed = cur.fetchone()["count"]
        cur.execute(*coordination_drain.gate_observation_query(DEPLOY_SCOPE, 159))
        next_generation = cur.fetchone()["count"]

        assert next_generation == observed + 1, (
            "a release increments the generation, so the new drain must be "
            "observed afresh"
        )

    def test_repeated_observation_of_one_run_keeps_a_single_row(
        self, cur, airflow_metadata_standin
    ):
        """The reschedule sensor pokes every 60s for the length of the drain."""
        run_id = f"orphan_checker-{uuid.uuid4().hex[:8]}"
        for _ in range(3):
            cur.execute(GATE_OBSERVATION_SQL, (158, "orphan_checker", run_id))

        cur.execute(
            "SELECT COUNT(*) FROM public.coordination_gate_observations"
            " WHERE generation = %s AND dag_id = %s AND run_id = %s",
            (158, "orphan_checker", run_id),
        )
        assert cur.fetchone()["count"] == 1

    def test_every_drain_table_is_schema_qualified(self):
        """A bare table name is only correct by accident of search_path."""
        sql = " ".join(
            [
                coordination_drain.RUNNING_DETAIL_CLAIMS_SQL,
                coordination_drain.task_instance_query(DEPLOY_SCOPE)[0],
                coordination_drain.gate_observation_query(DEPLOY_SCOPE, 1)[0],
            ]
        )
        for table in (
            "detail_scrape_claims",
            "task_instance",
            "dag_run",
            "coordination_gate_observations",
        ):
            assert f" {table}" not in sql, f"{table} is referenced without a schema"
