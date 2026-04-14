"""
Layer 1 — SQL smoke tests for dbt_runner service queries.

Validates that every query the dbt_runner executes runs without error
against the real schema.
"""
import pytest

pytestmark = pytest.mark.integration

STALE_LOCK_MINUTES = 30


# ============================================================================
# dbt_lock — mutex queries
# ============================================================================

class TestDbtLockQueries:

    def test_acquire_lock(self, cur):
        cur.execute(
            """UPDATE dbt_lock
               SET locked = true, locked_at = now(), locked_by = %s
               WHERE id = 1
                   AND (locked = false
                   OR locked_at < now() - interval '%s minutes')
               RETURNING locked""",
            ("smoke_test", STALE_LOCK_MINUTES),
        )
        row = cur.fetchone()
        assert row is not None

    def test_release_lock(self, cur):
        cur.execute("""
            UPDATE dbt_lock
            SET locked = false, locked_at = null, locked_by = null
            WHERE id = 1
        """)
        assert cur.rowcount == 1

    def test_lock_status(self, cur):
        cur.execute("SELECT locked, locked_at, locked_by FROM dbt_lock WHERE id = 1")
        row = cur.fetchone()
        assert row is not None


# ============================================================================
# dbt_runs — build history
# ============================================================================

class TestDbtRunsQueries:

    def test_record_run(self, cur):
        cur.execute(
            """INSERT INTO dbt_runs
               (started_at, finished_at, duration_s, ok, intent, select_args,
                models_pass, models_error, models_skip, returncode)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                "2026-01-01 00:00:00+00",
                "2026-01-01 00:01:00+00",
                60.0,
                True,
                "full",
                "+stg_srp_observations +stg_detail_observations",
                10, 0, 2, 0,
            ),
        )
        assert cur.rowcount == 1


# ============================================================================
# dbt_intents — named build targets
# ============================================================================

class TestDbtIntentsQueries:

    def test_load_intents(self, cur):
        cur.execute("SELECT intent_name, select_args FROM dbt_intents ORDER BY intent_name")
        rows = cur.fetchall()
        assert isinstance(rows, list)

    def test_save_intent(self, cur):
        cur.execute(
            """INSERT INTO dbt_intents (intent_name, select_args, updated_at)
               VALUES (%s, %s, now())
               ON CONFLICT (intent_name) DO UPDATE
               SET select_args = EXCLUDED.select_args, updated_at = now()""",
            ("smoke_intent", ["stg_srp_observations"]),
        )
        assert cur.rowcount == 1

    def test_delete_intent(self, cur):
        # Insert then delete
        cur.execute(
            """INSERT INTO dbt_intents (intent_name, select_args, updated_at)
               VALUES (%s, %s, now())
               ON CONFLICT (intent_name) DO UPDATE
               SET select_args = EXCLUDED.select_args, updated_at = now()""",
            ("delete_me", ["test"]),
        )
        cur.execute("DELETE FROM dbt_intents WHERE intent_name = %s", ("delete_me",))
        assert cur.rowcount == 1
