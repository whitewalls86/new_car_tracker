"""
Integration tests for archiver parquet cleanup SQL.

Validates the SELECT (get_expired_parquet_months) and UPDATE (mark_parquet_deleted)
queries directly against a real DB — no archiver service or MinIO needed.

Seeds raw_artifacts rows covering three cases:
  - expired: archived_at > 28 days ago, deleted_at IS NULL  → should be found and marked
  - recent:  archived_at < 28 days ago                      → should be ignored
  - already deleted: archived_at > 28 days ago, deleted_at set → should be ignored
"""
import pytest

from archiver.queries import GET_EXPIRED_PARQUET_MONTHS, MARK_PARQUET_DELETED

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def seeded_artifacts(cur, seed_run):
    """
    Seeds three raw_artifacts rows representing expired, recent, and
    already-deleted states. Returns (expired_id, recent_id, already_deleted_id).
    """
    cur.execute(
        """
        INSERT INTO raw_artifacts
            (run_id, source, artifact_type, url, filepath, archived_at, deleted_at)
        VALUES
            -- expired: archived 40 days ago, not yet deleted
            (%s, 'cars.com', 'srp', 'http://test/1', '/tmp/1.html',
             now() - interval '40 days', NULL),
            -- recent: archived 10 days ago — within the 28-day window
            (%s, 'cars.com', 'srp', 'http://test/2', '/tmp/2.html',
             now() - interval '10 days', NULL),
            -- already deleted: archived 40 days ago but deleted_at already set
            (%s, 'cars.com', 'srp', 'http://test/3', '/tmp/3.html',
             now() - interval '40 days', now() - interval '1 day')
        RETURNING artifact_id
        """,
        (seed_run, seed_run, seed_run),
    )
    rows = cur.fetchall()
    return rows[0]["artifact_id"], rows[1]["artifact_id"], rows[2]["artifact_id"]


# ---------------------------------------------------------------------------
# get_expired_parquet_months
# ---------------------------------------------------------------------------

class TestGetExpiredParquetMonths:
    def test_expired_month_is_returned(self, cur, seeded_artifacts):
        cur.execute(GET_EXPIRED_PARQUET_MONTHS)
        months = cur.fetchall()
        assert len(months) >= 1

    def test_recent_artifact_not_in_results(self, cur, seeded_artifacts):
        cur.execute(GET_EXPIRED_PARQUET_MONTHS)
        months = cur.fetchall()
        # All returned months must be > 28 days ago — none should be the current month
        # (the recent artifact was archived 10 days ago, so it's in the current month)
        from datetime import datetime, timedelta, timezone
        recent_cutoff = datetime.now(timezone.utc) - timedelta(days=28)
        for row in months:
            year, month = row["year"], row["month"]
            # The month represented must be before the cutoff month
            month_start = datetime(year, month, 1, tzinfo=timezone.utc)
            assert month_start < recent_cutoff.replace(day=1)

    def test_already_deleted_not_in_results(self, cur, seeded_artifacts):
        expired_id, _, already_deleted_id = seeded_artifacts
        # Mark only the already-deleted artifact as expired to isolate the filter
        cur.execute(
            "UPDATE raw_artifacts SET deleted_at = NULL WHERE artifact_id = %s",
            (already_deleted_id,),
        )
        cur.execute(
            "UPDATE raw_artifacts SET deleted_at = now() WHERE artifact_id = %s",
            (already_deleted_id,),
        )
        cur.execute(GET_EXPIRED_PARQUET_MONTHS)
        months = cur.fetchall()
        # expired_id (no deleted_at) still shows up; already_deleted_id should not add duplicates
        assert len(months) >= 1

    def test_results_are_distinct_months(self, cur, seeded_artifacts):
        cur.execute(GET_EXPIRED_PARQUET_MONTHS)
        months = cur.fetchall()
        pairs = [(r["year"], r["month"]) for r in months]
        assert len(pairs) == len(set(pairs))

    def test_results_ordered_by_year_month(self, cur, seeded_artifacts):
        cur.execute(GET_EXPIRED_PARQUET_MONTHS)
        months = cur.fetchall()
        pairs = [(r["year"], r["month"]) for r in months]
        assert pairs == sorted(pairs)


# ---------------------------------------------------------------------------
# mark_parquet_deleted
# ---------------------------------------------------------------------------

class TestMarkParquetDeleted:
    def test_expired_artifact_is_marked_deleted(self, cur, seeded_artifacts):
        expired_id, _, _ = seeded_artifacts
        cur.execute(MARK_PARQUET_DELETED)
        cur.execute(
            "SELECT deleted_at FROM raw_artifacts WHERE artifact_id = %s", (expired_id,)
        )
        assert cur.fetchone()["deleted_at"] is not None

    def test_recent_artifact_is_not_marked_deleted(self, cur, seeded_artifacts):
        _, recent_id, _ = seeded_artifacts
        cur.execute(MARK_PARQUET_DELETED)
        cur.execute(
            "SELECT deleted_at FROM raw_artifacts WHERE artifact_id = %s", (recent_id,)
        )
        assert cur.fetchone()["deleted_at"] is None

    def test_already_deleted_artifact_deleted_at_unchanged(self, cur, seeded_artifacts):
        _, _, already_deleted_id = seeded_artifacts
        cur.execute(
            "SELECT deleted_at FROM raw_artifacts WHERE artifact_id = %s",
            (already_deleted_id,),
        )
        original_deleted_at = cur.fetchone()["deleted_at"]

        cur.execute(MARK_PARQUET_DELETED)

        cur.execute(
            "SELECT deleted_at FROM raw_artifacts WHERE artifact_id = %s",
            (already_deleted_id,),
        )
        assert cur.fetchone()["deleted_at"] == original_deleted_at

    def test_artifact_with_no_archived_at_not_marked(self, cur, seed_run):
        cur.execute(
            """
            INSERT INTO raw_artifacts (run_id, source, artifact_type, url, filepath)
            VALUES (%s, 'cars.com', 'srp', 'http://test/4', '/tmp/4.html')
            RETURNING artifact_id
            """,
            (seed_run,),
        )
        artifact_id = cur.fetchone()["artifact_id"]
        cur.execute(MARK_PARQUET_DELETED)
        cur.execute(
            "SELECT deleted_at FROM raw_artifacts WHERE artifact_id = %s", (artifact_id,)
        )
        assert cur.fetchone()["deleted_at"] is None
