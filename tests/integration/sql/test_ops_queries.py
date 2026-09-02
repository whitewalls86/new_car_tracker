"""
Layer 2 — SQL smoke tests for ops service queries.

Every query the ops service runs against Postgres is executed here against a real
DB with Flyway migrations applied. The goal is to catch schema breakage (column
renames, dropped tables, type mismatches) — not to validate business logic.
"""
import json
import uuid
from pathlib import Path

import pytest

from ops import coordination_drain
from ops.queries import (
    ACQUIRE_COORDINATION_LOCK,
    ADVANCE_COORDINATION_STATE,
    APPROVE_ACCESS_REQUEST,
    AUTHORIZE_COORDINATION_STATE,
    CANCEL_COORDINATION_STATE,
    CLAIM_DETAIL_SCRAPE_BATCH,
    CLEAR_DEPLOY_INTENT,
    COMPLETE_COORDINATION_STATE,
    DELETE_AUTHORIZED_USER,
    DELETE_DETAIL_SCRAPE_CLAIMS,
    DENY_ACCESS_REQUEST,
    EVICT_DELISTED_COOLDOWNS,
    EXPIRE_ORPHAN_DETAIL_CLAIMS,
    INSERT_ACCESS_REQUEST,
    INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT,
    INSERT_BLOCKED_COOLDOWN_EVENTS_BATCH,
    INSERT_COMPLETION_RECEIPT,
    INSERT_COORDINATION_RELEASE_EVIDENCE,
    INSERT_COORDINATION_STATE_EVENT,
    INSERT_SEARCH_CONFIG,
    MARK_ROTATION_SLOT_QUEUED,
    MARK_SEARCH_CONFIG_QUEUED,
    RECORD_DETAIL_FETCHES,
    RELEASE_COORDINATION_STATE,
    RELEASE_DEPLOY_COORDINATION,
    REQUEST_COORDINATION_STATE,
    REQUEST_DEPLOY_COORDINATION,
    RETIRE_SEARCH_CONFIG,
    SELECT_ACCESS_REQUESTS,
    SELECT_AUTHORIZED_USERS,
    SELECT_COMPLETION_RECEIPT,
    SELECT_COORDINATION_STATE,
    SELECT_COORDINATION_STATE_ACTOR,
    SELECT_COORDINATION_STATE_FOR_DEPLOY,
    SELECT_COORDINATION_STATE_KIND,
    SELECT_COORDINATION_STATE_METRICS,
    SELECT_DEPLOY_INTENT_STATUS,
    SELECT_LAST_QUEUED_AT,
    SELECT_LEGACY_SEARCH_CONFIG,
    SELECT_LIVE_COOLDOWN_LISTINGS,
    SELECT_NEXT_ROTATION_SLOT,
    SELECT_PENDING_CLEARED_LISTINGS,
    SELECT_PENDING_REQUEST_DETAILS,
    SELECT_PENDING_REQUEST_FOR_EMAIL,
    SELECT_PENDING_REQUEST_ID_FOR_EMAIL,
    SELECT_PENDING_REQUEST_NOTIFICATION_EMAIL,
    SELECT_RELEASE_EVIDENCE,
    SELECT_ROTATION_SLOT_CONFIGS,
    SELECT_RUNNING_DETAIL_CLAIMS,
    SELECT_SEARCH_CONFIG_BY_KEY,
    SELECT_SEARCH_CONFIGS,
    SELECT_STUCK_PROCESSING_ARTIFACTS,
    SELECT_USER_ROLE,
    SET_DEPLOY_INTENT,
    TOGGLE_SEARCH_CONFIG_ENABLED,
    UPDATE_SEARCH_CONFIG,
    UPDATE_USER_ROLE,
    UPSERT_AUTHORIZED_USER,
)
from ops.routers.coordination import _TRANSITIONS, COORDINATION_LOCK_ID
from ops.routers.deploy import STALE_LOCK_MINUTES

pytestmark = pytest.mark.integration


# The sensor's own statement, read from the file the sensor reads.
#
# This was an AST scrape of a module-level constant in sensors.py until Plan
# 162 Stage 7 moved the statement into airflow/sql/, which is what the scrape
# was working around: this suite runs in the main venv and cannot import
# Airflow. Reading the .sql file is the same guarantee -- one copy, executed
# here -- without parsing Python to get at a string.
GATE_OBSERVATION_SQL = (
    Path(__file__).parents[3] / "airflow" / "sql" / "record_gate_observation.sql"
).read_text(encoding="utf-8")

# The surfaces a full-fleet deploy expands to; see ops/coordination_contract.py.
DEPLOY_SCOPE = frozenset(
    {"analytics", "archive", "detail_fetch", "listing_fetch", "observability", "processing"}
)


# ============================================================================
# admin.py — search config queries
# ============================================================================

class TestSearchConfigQueries:

    def test_list_searches(self, cur):
        cur.execute(SELECT_SEARCH_CONFIGS)
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_get_search_by_key(self, cur, seed_search_config):
        cur.execute(SELECT_SEARCH_CONFIG_BY_KEY, (seed_search_config,))
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
        cur.execute(CLEAR_DEPLOY_INTENT)
        row = cur.fetchone()
        assert row is not None


# ============================================================================
# auth.py — auth check query
# ============================================================================

class TestAuthQueries:

    def test_auth_check_lookup(self, cur, seed_authorized_user):
        _user_id, email_hash = seed_authorized_user
        cur.execute(
            SELECT_USER_ROLE,
            (email_hash,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_auth_check_miss(self, cur):
        cur.execute(
            SELECT_USER_ROLE,
            ("nonexistent_hash",),
        )
        row = cur.fetchone()
        assert row is None


# ============================================================================
# users.py — user management queries
# ============================================================================

class TestUserManagementQueries:

    def test_list_authorized_users(self, cur):
        cur.execute(SELECT_AUTHORIZED_USERS)
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_update_user_role(self, cur, seed_authorized_user):
        user_id, _hash = seed_authorized_user
        cur.execute(
            UPDATE_USER_ROLE,
            ("observer", user_id),
        )
        assert cur.rowcount == 1

    def test_revoke_user(self, cur, seed_authorized_user):
        user_id, _hash = seed_authorized_user
        cur.execute(DELETE_AUTHORIZED_USER, (user_id,))
        assert cur.rowcount == 1

    def test_list_access_requests(self, cur):
        cur.execute(SELECT_ACCESS_REQUESTS)
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_get_pending_request_details(self, cur, seed_access_request):
        req_id, _hash = seed_access_request
        cur.execute(
            SELECT_PENDING_REQUEST_DETAILS,
            (req_id,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_approve_access_request(self, cur, seed_access_request):
        req_id, email_hash = seed_access_request
        admin_hash = "admin_approver_hash"
        # Upsert into authorized_users
        cur.execute(
            UPSERT_AUTHORIZED_USER,
            (email_hash, "viewer", "Approved User", admin_hash),
        )
        # Update request status
        cur.execute(
            APPROVE_ACCESS_REQUEST,
            (admin_hash, req_id),
        )
        assert cur.rowcount == 1

    def test_deny_access_request(self, cur, seed_access_request):
        req_id, _hash = seed_access_request
        cur.execute(
            DENY_ACCESS_REQUEST,
            ("admin_hash", req_id),
        )
        assert cur.rowcount == 1

    def test_check_pending_access_request(self, cur, seed_access_request):
        _req_id, email_hash = seed_access_request
        cur.execute(
            SELECT_PENDING_REQUEST_FOR_EMAIL,
            (email_hash,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_insert_access_request(self, cur):
        email_hash = f"newhash_{uuid.uuid4().hex[:12]}"
        cur.execute(
            INSERT_ACCESS_REQUEST,
            (email_hash, "observer", "New User", None),
        )
        assert cur.rowcount == 1

    def test_get_notification_email(self, cur, seed_access_request):
        req_id, _hash = seed_access_request
        cur.execute(
            SELECT_PENDING_REQUEST_NOTIFICATION_EMAIL,
            (req_id,),
        )
        row = cur.fetchone()
        assert row is not None


# ============================================================================
# Plan 97 — artifacts_queue schema smoke tests
# ============================================================================

class TestArtifactsQueueSchema:
    """Layer 2 smoke tests: verify ops.artifacts_queue table and constraints exist."""

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
    """Layer 2 smoke tests: verify staging.artifacts_queue_events exists."""

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

def _insert_dag_run(cur, dag_id: str, run_id: str) -> None:
    """
    A running `airflow.dag_run` row, as Airflow's own schema requires one.

    `run_type` and `run_after` are NOT NULL with no server default -- Airflow
    fills them from Python, so a raw INSERT must supply them. The retired
    `airflow_metadata_standin` needed neither, which is precisely the drift
    this fixture now catches: these columns are Airflow's to change.
    """
    cur.execute(
        "INSERT INTO airflow.dag_run"
        " (dag_id, run_id, state, start_date, run_type, run_after)"
        " VALUES (%s, %s, 'running', now(), 'manual', now())",
        (dag_id, run_id),
    )


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

    def test_airflow_task_instance_query_resolves(self, cur, airflow_metadata):
        query = coordination_drain.task_instance_query(DEPLOY_SCOPE)
        assert query is not None, "the deploy scope must drain some task instances"
        cur.execute(*query)
        assert cur.fetchone() is not None

    def test_gate_observation_query_resolves(self, cur, airflow_metadata):
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
        self, cur, airflow_metadata
    ):
        generation = 158
        query = coordination_drain.gate_observation_query(DEPLOY_SCOPE, generation)
        affected = query[1][:-1]
        runs = [(dag_id, f"{dag_id}-{uuid.uuid4().hex[:8]}") for dag_id in affected]
        for dag_id, run_id in runs:
            _insert_dag_run(cur, dag_id, run_id)

        cur.execute(*query)
        assert cur.fetchone()["count"] == len(runs), "every live affected run blocks"

        for dag_id, run_id in runs:
            cur.execute(GATE_OBSERVATION_SQL, (generation, dag_id, run_id))

        cur.execute(*query)
        assert cur.fetchone()["count"] == 0, "an observed run no longer blocks the drain"

    def test_an_observation_does_not_satisfy_the_next_generation(
        self, cur, airflow_metadata
    ):
        dag_id = coordination_drain.gate_observation_query(DEPLOY_SCOPE, 1)[1][0]
        run_id = f"{dag_id}-{uuid.uuid4().hex[:8]}"
        _insert_dag_run(cur, dag_id, run_id)
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
        self, cur, airflow_metadata
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


# ===========================================================================
# Statements imported from ops.queries — Plan 162 Stage 7
# ===========================================================================

class TestExtractedOpsStatements:
    """The first statements in this file that are the production text.

    Everything above paraphrases: it retypes SQL that resembles what ops runs
    and executes the copy, which passes forever whatever the original does.
    These import from ``ops.queries``, the module ops itself imports, so a
    column renamed underneath them fails here. The rest of the file is repaired
    the same way.
    """

    def test_select_user_role(self, cur):
        cur.execute(SELECT_USER_ROLE, ("no-such-email-hash",))
        assert cur.fetchone() is None

    def test_select_coordination_state_metrics(self, cur):
        cur.execute(SELECT_COORDINATION_STATE_METRICS)
        row = cur.fetchone()
        # V0xx seeds the singleton row, so this reads a real one rather than
        # proving only that the statement parses.
        assert row is not None
        assert set(row) == {"kind", "phase", "generation", "scope", "updated_at"}

    def test_insert_blocked_cooldown_events_batch(self, cur):
        from psycopg2.extras import execute_values

        execute_values(
            cur,
            INSERT_BLOCKED_COOLDOWN_EVENTS_BATCH,
            [(str(uuid.uuid4()), "cleared", 3)],
        )
        assert cur.rowcount == 1


# ===========================================================================
# ops/routers/*.py — Plan 162 Stage 7
#
# The four routers held 49 statements at their .execute() call sites, which no
# test could import and so no test could execute. They are .sql files now, and
# these four classes run them. Every constant below comes from ops.queries --
# the module the routers themselves import -- so a column renamed underneath
# production fails here rather than passing against a copy.
# ===========================================================================

def _column_names(cur) -> list[str]:
    """The result columns in the order the server returned them.

    Order matters to two of these statements: deploy.py reads its coordination
    row positionally.
    """
    return [description[0] for description in cur.description]


def _open_coordination_request(cur, kind: str = "host_maintenance") -> int:
    """Move the singleton row out of 'none' so a transition has somewhere to go.

    coordination_state carries a CHECK tying phase to kind/targets/scope: a
    phase other than 'none' requires all three to be populated. Every
    transition statement below therefore needs a real request first, which is
    also how production reaches them.
    """
    cur.execute(
        REQUEST_COORDINATION_STATE,
        (
            kind,
            json.dumps(["host"]),
            json.dumps(["host"]),
            "layer-2-test",
            "Plan 162 Stage 7 smoke test",
            json.dumps([]),
            None,
            None,
        ),
    )
    return cur.fetchone()["generation"]


class TestCoordinationStatements:
    """ops/routers/coordination.py — the Plan 142 state machine."""

    def test_acquire_coordination_lock(self, cur):
        cur.execute(ACQUIRE_COORDINATION_LOCK, (COORDINATION_LOCK_ID,))
        # pg_advisory_xact_lock returns void; the row proves it was taken, and
        # the fixture's rollback releases it.
        assert _column_names(cur) == ["pg_advisory_xact_lock"]

    def test_select_coordination_state(self, cur):
        cur.execute(SELECT_COORDINATION_STATE)
        row = cur.fetchone()
        # V043 seeds the singleton, so this reads a real row rather than
        # proving only that the statement parses.
        assert row is not None
        assert set(row) == {
            "kind",
            "phase",
            "generation",
            "requested_by",
            "reason",
            "targets",
            "scope",
            "requested_at",
            "draining_at",
            "active_at",
            "validating_at",
            "completed_at",
            "expected_work",
            "manifest_location",
            "operator_notes",
            "updated_at",
        }

    def test_the_two_confirming_reads_stay_different_widths(self, cur):
        """The 4-column and 3-column reads are near-duplicates, on purpose.

        /request has no prior actor to attribute, so it does not select
        requested_by. Consolidating them would widen one of the two.
        """
        cur.execute(SELECT_COORDINATION_STATE_ACTOR)
        assert _column_names(cur) == ["phase", "generation", "kind", "requested_by"]
        cur.execute(SELECT_COORDINATION_STATE_KIND)
        assert _column_names(cur) == ["phase", "generation", "kind"]

    def test_insert_coordination_state_event(self, cur):
        cur.execute(
            INSERT_COORDINATION_STATE_EVENT,
            (1, "none", "requested", "deploy", "layer-2-test"),
        )
        assert cur.rowcount == 1

    def test_insert_coordination_release_evidence(self, cur):
        cur.execute(
            INSERT_COORDINATION_RELEASE_EVIDENCE,
            (
                1,
                "layer-2-test",
                json.dumps({"preflight": {"verdict": "pass", "reason": "smoke"}}),
                json.dumps({"preflight": "0" * 64, "manifest": "1" * 64}),
            ),
        )
        assert set(cur.fetchone()) == {"evidence_id", "submitted_at"}

    def test_select_release_evidence(self, cur):
        # generation 0 is never issued -- V045 requires generation >= 1 -- so
        # this asserts the statement's shape against the real table.
        cur.execute(SELECT_RELEASE_EVIDENCE, (0,))
        assert cur.fetchall() == []

    def test_request_coordination_state_bumps_the_generation(self, cur):
        cur.execute(SELECT_COORDINATION_STATE_ACTOR)
        before = cur.fetchone()["generation"]
        assert _open_coordination_request(cur) == before + 1

    def test_every_transition_names_a_real_timestamp_column(self, cur):
        """All four _TRANSITIONS values, in the order production walks them.

        {timestamp_column} is interpolated rather than bound because it names a
        column, so nothing but this test proves the four names exist. Running
        them in sequence from a fresh request is also the only order the
        phase/kind CHECK permits.
        """
        _open_coordination_request(cur)
        for operation, (_, target, timestamp_column) in _TRANSITIONS.items():
            if target == "none":
                statement = RELEASE_COORDINATION_STATE.format(
                    timestamp_column=timestamp_column
                )
                cur.execute(statement)
            else:
                statement = ADVANCE_COORDINATION_STATE.format(
                    timestamp_column=timestamp_column
                )
                cur.execute(statement, (target,))
            assert cur.fetchone() is not None, operation
        cur.execute(SELECT_COORDINATION_STATE_ACTOR)
        assert cur.fetchone()["phase"] == "none"

    def test_complete_holds_the_generation_and_cancel_bumps_it(self, cur):
        """The one difference between two otherwise identical statements.

        complete_coordination_state.sql must not bump: the completion receipt
        is written against the generation that just finished.
        """
        generation = _open_coordination_request(cur)
        cur.execute(COMPLETE_COORDINATION_STATE)
        assert cur.fetchone()["generation"] == generation

        generation = _open_coordination_request(cur)
        cur.execute(CANCEL_COORDINATION_STATE)
        assert cur.fetchone()["generation"] == generation + 1

    def test_completion_receipt_round_trip(self, cur):
        digest = uuid.uuid4().hex + uuid.uuid4().hex  # V046 requires 64 chars
        cur.execute(SELECT_COMPLETION_RECEIPT, (1, digest))
        assert cur.fetchone() is None
        cur.execute(INSERT_COMPLETION_RECEIPT, (1, digest))
        cur.execute(SELECT_COMPLETION_RECEIPT, (1, digest))
        assert cur.fetchone()["generation"] == 1

    def test_authorize_coordination_state_refuses_a_stale_generation(self, cur):
        generation = _open_coordination_request(cur)
        cur.execute(
            ADVANCE_COORDINATION_STATE.format(timestamp_column="draining_at"),
            ("draining",),
        )
        assert cur.fetchone() is not None
        cur.execute(AUTHORIZE_COORDINATION_STATE, (generation,))
        assert cur.rowcount == 1
        # The row is 'active' now, so the same authorization matches nothing.
        cur.execute(AUTHORIZE_COORDINATION_STATE, (generation,))
        assert cur.rowcount == 0


class TestDeployFacadeStatements:
    """ops/routers/deploy.py — the legacy deploy_intent facade."""

    def test_select_deploy_intent_status(self, cur):
        cur.execute(SELECT_DEPLOY_INTENT_STATUS)
        row = cur.fetchone()
        # V002 seeds the singleton, and both CTEs are unconditional aggregates,
        # so a row comes back even with nothing in flight.
        assert row is not None
        assert set(row) == {
            "intent",
            "requested_at",
            "requested_by",
            "number_running",
            "min_started_at",
            "pause_long_jobs",
        }

    def test_the_two_four_column_reads_are_not_interchangeable(self, cur):
        """deploy.py reads its coordination row positionally.

        Its statement selects (kind, phase, ...) while coordination.py's selects
        (phase, generation, kind, ...). The two look like duplicates and are
        kept apart because merging them silently swaps row[0] and row[1].
        """
        cur.execute(SELECT_COORDINATION_STATE_FOR_DEPLOY)
        assert _column_names(cur) == ["kind", "phase", "generation", "requested_by"]
        cur.execute(SELECT_COORDINATION_STATE_ACTOR)
        assert _column_names(cur) == ["phase", "generation", "kind", "requested_by"]

    def test_set_deploy_intent_is_a_lock(self, cur):
        cur.execute(SET_DEPLOY_INTENT, ("layer-2-test", True, STALE_LOCK_MINUTES))
        assert cur.fetchone()["intent"] == "pending"
        # Held and still fresh, so the second attempt returns no row at all --
        # which is exactly how _set_intent decides it lost.
        cur.execute(SET_DEPLOY_INTENT, ("layer-2-test", True, STALE_LOCK_MINUTES))
        assert cur.fetchone() is None

    def test_clear_deploy_intent(self, cur):
        cur.execute(SET_DEPLOY_INTENT, ("layer-2-test", False, STALE_LOCK_MINUTES))
        assert cur.fetchone() is not None
        cur.execute(CLEAR_DEPLOY_INTENT)
        assert cur.fetchone()["intent"] == "none"

    def test_request_then_release_deploy_coordination(self, cur):
        cur.execute(
            REQUEST_DEPLOY_COORDINATION,
            (json.dumps(["ops"]), json.dumps(["services"]), "layer-2-test"),
        )
        generation = cur.fetchone()["generation"]
        cur.execute(RELEASE_DEPLOY_COORDINATION)
        assert cur.fetchone()["generation"] == generation + 1


class TestUserManagementStatements:
    """ops/routers/users.py — authorization and the access-request queue."""

    def test_no_pending_request_for_an_unknown_email(self, cur):
        cur.execute(SELECT_PENDING_REQUEST_FOR_EMAIL, ("no-such-email-hash",))
        assert cur.fetchone() is None
        cur.execute(SELECT_PENDING_REQUEST_ID_FOR_EMAIL, ("no-such-email-hash",))
        assert cur.fetchone() is None

    def test_access_request_approval_path(self, cur):
        email_hash = uuid.uuid4().hex
        cur.execute(
            INSERT_ACCESS_REQUEST,
            (email_hash, "viewer", "Layer 2", "nobody@example.invalid"),
        )
        cur.execute(SELECT_PENDING_REQUEST_FOR_EMAIL, (email_hash,))
        assert cur.fetchone()["status"] == "pending"

        cur.execute(SELECT_PENDING_REQUEST_ID_FOR_EMAIL, (email_hash,))
        request_id = cur.fetchone()["id"]
        cur.execute(SELECT_PENDING_REQUEST_DETAILS, (request_id,))
        row = cur.fetchone()
        assert set(row) == {
            "email_hash",
            "requested_role",
            "display_name",
            "notification_email",
        }

        cur.execute(
            UPSERT_AUTHORIZED_USER,
            (row["email_hash"], row["requested_role"], row["display_name"], None),
        )
        cur.execute(APPROVE_ACCESS_REQUEST, (None, request_id))
        assert cur.rowcount == 1

        cur.execute(SELECT_USER_ROLE, (email_hash,))
        assert cur.fetchone()["role"] == "viewer"
        cur.execute(SELECT_PENDING_REQUEST_ID_FOR_EMAIL, (email_hash,))
        assert cur.fetchone() is None

    def test_access_request_denial_is_idempotent(self, cur):
        email_hash = uuid.uuid4().hex
        cur.execute(
            INSERT_ACCESS_REQUEST,
            (email_hash, "observer", None, "nobody@example.invalid"),
        )
        cur.execute(SELECT_PENDING_REQUEST_ID_FOR_EMAIL, (email_hash,))
        request_id = cur.fetchone()["id"]

        cur.execute(SELECT_PENDING_REQUEST_NOTIFICATION_EMAIL, (request_id,))
        assert cur.fetchone()["notification_email"] == "nobody@example.invalid"

        cur.execute(DENY_ACCESS_REQUEST, (None, request_id))
        assert cur.rowcount == 1
        # The 'pending' predicate deny keeps and approve does not is the guard:
        # a repeated denial matches nothing.
        cur.execute(DENY_ACCESS_REQUEST, (None, request_id))
        assert cur.rowcount == 0
        cur.execute(SELECT_PENDING_REQUEST_NOTIFICATION_EMAIL, (request_id,))
        assert cur.fetchone() is None

    def test_select_access_requests_puts_pending_first(self, cur):
        cur.execute(INSERT_ACCESS_REQUEST, (uuid.uuid4().hex, "viewer", None, None))
        cur.execute(SELECT_ACCESS_REQUESTS)
        rows = cur.fetchall()
        assert set(rows[0]) == {
            "id",
            "email_hash",
            "display_name",
            "requested_role",
            "requested_at",
            "status",
            "resolved_at",
            "resolved_by",
        }
        assert rows[0]["status"] == "pending"

    def test_upsert_regrants_without_overwriting_the_display_name(self, cur):
        email_hash = uuid.uuid4().hex
        cur.execute(UPSERT_AUTHORIZED_USER, (email_hash, "viewer", "Layer 2", None))
        cur.execute(UPSERT_AUTHORIZED_USER, (email_hash, "observer", "ignored", None))

        cur.execute(SELECT_AUTHORIZED_USERS)
        rows = {row["email_hash"]: row for row in cur.fetchall()}
        assert set(rows[email_hash]) == {
            "id",
            "email_hash",
            "role",
            "display_name",
            "created_at",
        }
        assert rows[email_hash]["role"] == "observer"
        # DO UPDATE sets role and created_by only, so the name survives.
        assert rows[email_hash]["display_name"] == "Layer 2"

        user_id = rows[email_hash]["id"]
        cur.execute(UPDATE_USER_ROLE, ("power_user", user_id))
        assert cur.rowcount == 1
        cur.execute(DELETE_AUTHORIZED_USER, (user_id,))
        assert cur.rowcount == 1


class TestScrapeStatements:
    """ops/routers/scrape.py — rotation claiming and detail claims."""

    def test_select_last_queued_at(self, cur):
        cur.execute(SELECT_LAST_QUEUED_AT)
        assert _column_names(cur) == ["max"]

    def test_rotation_slot_claim_round_trip(self, cur):
        slot = 999_000
        keys = sorted(f"layer2-{uuid.uuid4().hex}" for _ in range(2))
        for order, key in enumerate(keys):
            cur.execute(
                "INSERT INTO search_configs (search_key, params, rotation_slot,"
                " rotation_order) VALUES (%s, '{}'::jsonb, %s, %s)",
                (key, slot, order),
            )

        # A never-queued config exists now, so some slot must be due.
        cur.execute(SELECT_NEXT_ROTATION_SLOT, (1439,))
        assert _column_names(cur) == ["rotation_slot"]
        assert cur.fetchone() is not None

        cur.execute(MARK_ROTATION_SLOT_QUEUED, (slot,))
        assert cur.rowcount == 2
        cur.execute(SELECT_ROTATION_SLOT_CONFIGS, (slot,))
        rows = cur.fetchall()
        assert [row["search_key"] for row in rows] == keys
        assert set(rows[0]) == {"search_key", "params"}

        # Claimed, so the slot this test created is no longer due.
        cur.execute(SELECT_NEXT_ROTATION_SLOT, (1439,))
        assert slot not in {row["rotation_slot"] for row in cur.fetchall()}

    def test_legacy_search_config_claim(self, cur):
        cur.execute(
            "INSERT INTO search_configs (search_key, params) VALUES (%s, '{}'::jsonb)",
            (f"layer2-{uuid.uuid4().hex}",),
        )
        cur.execute(SELECT_LEGACY_SEARCH_CONFIG, (1439,))
        row = cur.fetchone()
        assert row is not None
        assert set(row) == {"search_key", "params"}
        cur.execute(MARK_SEARCH_CONFIG_QUEUED, (row["search_key"],))
        assert cur.rowcount == 1

    def test_claim_detail_scrape_batch(self, cur):
        """Executes the whole CTE -- anti-join, INSERT and ON CONFLICT.

        An empty queue returns no rows and still parses and plans every branch,
        which is what a schema change to ops.ops_detail_scrape_queue or to
        detail_scrape_claims would break.
        """
        cur.execute(CLAIM_DETAIL_SCRAPE_BATCH, (5, str(uuid.uuid4())))
        assert cur.description is not None
        assert len(cur.fetchall()) <= 5

    def test_release_claims_writes_the_fetch_guard(self, cur):
        run_id = str(uuid.uuid4())
        listing_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO detail_scrape_claims (listing_id, claimed_by, status)"
            " VALUES (%s::uuid, %s, 'running')",
            (listing_id, run_id),
        )
        cur.execute(DELETE_DETAIL_SCRAPE_CLAIMS, ([listing_id], run_id))
        assert cur.rowcount == 1
        # No observation exists for a fresh uuid, so this asserts the column
        # Plan 147 added is still there rather than asserting a row count.
        cur.execute(RECORD_DETAIL_FETCHES, ([listing_id],))
        assert cur.rowcount == 0


# ===========================================================================
# The maintenance statements, executed — Plan 162 Stage 7
# ===========================================================================

class TestMaintenanceStatements:
    """The five ops statements the census found no layer executing.

    Each is parameterless and touches a table ``ops/routers/maintenance.py``
    owns, so the seeding here is what makes them more than a parse check: an
    unseeded ``DELETE ... RETURNING`` proves the statement plans and nothing
    about which rows it takes.
    """

    def test_select_stuck_processing_artifacts(self, cur):
        # The reaper builds its retry payload from these six by name, so the
        # projection is the contract; nothing here is stuck, which is why the
        # columns rather than the rows are what this asserts.
        cur.execute(SELECT_STUCK_PROCESSING_ARTIFACTS)
        assert cur.fetchall() == []
        assert _column_names(cur) == [
            "artifact_id", "minio_path", "artifact_type", "fetched_at",
            "listing_id", "run_id",
        ]

    def test_expire_orphan_detail_claims(self, cur):
        # No fixture seeds a running claim, so the 2-hour predicate matches
        # nothing and this proves the statement plans and returns what the
        # caller reads without expiring a claim the test did not create.
        cur.execute(EXPIRE_ORPHAN_DETAIL_CLAIMS)
        assert cur.fetchall() == []
        assert _column_names(cur) == ["listing_id"]

    def test_evict_delisted_cooldowns_takes_a_listing_with_no_observation(self, cur):
        # A cooldown whose listing has no price observation is precisely what
        # this evicts, so seeding one exercises the NOT EXISTS branch rather
        # than only proving the statement plans.
        listing_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO ops.blocked_cooldown (listing_id, num_of_attempts) "
            "VALUES (%s, 1)",
            (listing_id,),
        )
        cur.execute(EVICT_DELISTED_COOLDOWNS)
        assert listing_id in {str(row["listing_id"]) for row in cur.fetchall()}

    def test_select_live_cooldown_listings(self, cur):
        listing_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO ops.blocked_cooldown (listing_id, num_of_attempts) "
            "VALUES (%s, 2)",
            (listing_id,),
        )
        cur.execute(SELECT_LIVE_COOLDOWN_LISTINGS)
        assert listing_id in {row["listing_id"] for row in cur.fetchall()}

    def test_cleared_event_is_read_back_by_the_pending_query(self, cur):
        # The reconcile pass writes with one statement and reads with the
        # other, so the two have to agree on event_type = 'cleared'. Running
        # them as a pair is what proves that; either alone would not.
        listing_id = str(uuid.uuid4())
        cur.execute(INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT, {
            "listing_id": listing_id,
            "num_of_attempts": 4,
        })
        assert cur.rowcount == 1

        cur.execute(SELECT_PENDING_CLEARED_LISTINGS)
        assert listing_id in {row["listing_id"] for row in cur.fetchall()}


class TestSearchConfigAdminStatements:
    """admin.py's writes, which nothing executed until Plan 162 Stage 7.

    All six were ``sql = \"\"\"...\"\"\"`` locals -- importable in principle and in
    no .sql file in practice, so the Layer 2 census could not count them and
    the two read tests above had to retype the statement to test anything.
    """

    def test_insert_then_read_back_by_key(self, cur):
        key = f"test-{uuid.uuid4().hex[:8]}"
        cur.execute(INSERT_SEARCH_CONFIG, (key, True, json.dumps({"makes": []}), 1, 1))
        assert cur.rowcount == 1

        cur.execute(SELECT_SEARCH_CONFIG_BY_KEY, (key,))
        row = cur.fetchone()
        assert row is not None and row["enabled"] is True

    def test_update_rewrites_every_mutable_field(self, cur):
        key = f"test-{uuid.uuid4().hex[:8]}"
        cur.execute(INSERT_SEARCH_CONFIG, (key, True, json.dumps({}), 1, 1))
        cur.execute(
            UPDATE_SEARCH_CONFIG,
            (False, json.dumps({"makes": ["Honda"]}), 9, 2, key),
        )
        assert cur.rowcount == 1
        cur.execute(SELECT_SEARCH_CONFIG_BY_KEY, (key,))
        row = cur.fetchone()
        assert row["enabled"] is False and row["rotation_order"] == 9

    def test_toggle_flips_without_reading_first(self, cur):
        key = f"test-{uuid.uuid4().hex[:8]}"
        cur.execute(INSERT_SEARCH_CONFIG, (key, True, json.dumps({}), 1, 1))
        # NOT enabled rather than a supplied value, so this is what proves two
        # toggles cannot both write the same state from a stale read.
        cur.execute(TOGGLE_SEARCH_CONFIG_ENABLED, (key,))
        cur.execute(SELECT_SEARCH_CONFIG_BY_KEY, (key,))
        assert cur.fetchone()["enabled"] is False

    def test_retire_disables_and_frees_the_key(self, cur):
        key = f"test-{uuid.uuid4().hex[:8]}"
        cur.execute(INSERT_SEARCH_CONFIG, (key, True, json.dumps({}), 1, 1))
        cur.execute(RETIRE_SEARCH_CONFIG, (f"{key}-retired", key))
        assert cur.rowcount == 1

        cur.execute(SELECT_SEARCH_CONFIG_BY_KEY, (key,))
        assert cur.fetchone() is None, "the original key must be free to reuse"
        cur.execute(SELECT_SEARCH_CONFIG_BY_KEY, (f"{key}-retired",))
        assert cur.fetchone()["enabled"] is False


class TestDrainGateStatement:

    def test_select_running_detail_claims(self, cur):
        cur.execute(SELECT_RUNNING_DETAIL_CLAIMS)
        count, oldest = cur.fetchone().values()
        # MIN is what separates "busy" from "stuck", so it must come back even
        # when there is nothing running.
        assert count == 0 or oldest is not None
