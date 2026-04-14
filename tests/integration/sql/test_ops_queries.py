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
# admin.py — run history queries
# ============================================================================

class TestRunQueries:

    def test_list_runs(self, cur):
        cur.execute("""
            SELECT run_id, started_at, finished_at, status, trigger,
                   progress_count, total_count, error_count, last_error, notes
            FROM runs ORDER BY started_at DESC LIMIT 20
        """)
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_get_run_by_id(self, cur, seed_run):
        cur.execute(
            """SELECT run_id, started_at, finished_at, status, trigger,
                      progress_count, total_count, error_count, last_error, notes
               FROM runs WHERE run_id = %s""",
            (seed_run,),
        )
        row = cur.fetchone()
        assert row is not None

    def test_get_scrape_jobs_for_run(self, cur, seed_scrape_job):
        _job_id, run_id, _key = seed_scrape_job
        cur.execute(
            """SELECT job_id, search_key, scope, status, created_at,
                      started_at, completed_at, artifact_count, error, retry_count
               FROM scrape_jobs WHERE run_id = %s ORDER BY created_at""",
            (run_id,),
        )
        rows = cur.fetchall()
        assert len(rows) >= 1


# ============================================================================
# deploy.py — deploy intent queries
# ============================================================================

class TestDeployIntentQueries:

    def test_intent_status(self, cur):
        cur.execute("""
            WITH current_executions AS (
                SELECT COUNT(execution_id) as number_running,
                       MIN(started_at) as min_started_at
                FROM n8n_executions WHERE status = 'running'
            ), current_runs AS (
                SELECT COUNT(*) as number_running,
                       MIN(started_at) as min_started_at
                FROM runs WHERE status = 'running'
            ), current_processing_runs AS (
                SELECT COUNT(*) as number_running,
                       MIN(started_at) as min_started_at
                FROM processing_runs WHERE status = 'processing'
            )
            SELECT di.intent, di.requested_at, di.requested_by,
                   ce.number_running + cr.number_running + cpr.number_running as number_running,
                   LEAST(ce.min_started_at, cr.min_started_at, cpr.min_started_at) as min_started_at
            FROM deploy_intent di
            LEFT JOIN current_executions ce ON 1=1
            LEFT JOIN current_runs cr ON 1=1
            LEFT JOIN current_processing_runs cpr ON 1=1
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
