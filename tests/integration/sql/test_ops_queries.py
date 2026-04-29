"""
Layer 1 — SQL smoke tests for ops service queries.

Every query the ops service runs against Postgres is executed here against a real
DB with Flyway migrations applied. The goal is to catch schema breakage (column
renames, dropped tables, type mismatches) — not to validate business logic.
"""
import uuid

import pytest

pytestmark = pytest.mark.integration


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

    def test_raw_artifacts_has_nullable_minio_path_column(self, cur):
        cur.execute("""
            SELECT is_nullable FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'raw_artifacts'
              AND column_name = 'minio_path'
        """)
        row = cur.fetchone()
        assert row is not None, "raw_artifacts.minio_path column missing"
        assert row["is_nullable"] == "YES"


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
