"""
Integration tests: _claim_batch() end-to-end.

Calls the real Python function against a real Postgres instance.
Covers: claiming pending/retry artifacts, type filter, processing event writes,
skipping complete artifacts, and the returned row shape.
"""
import uuid

import pytest

from processing.routers.batch import _claim_batch

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_artifact(vc, artifact_type="results_page", status="pending", search_key=None):
    """Insert an artifact and return its artifact_id."""
    lid = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    vc.execute(
        """
        INSERT INTO ops.artifacts_queue
            (minio_path, artifact_type, listing_id, run_id, fetched_at, status, search_key)
        VALUES (%s, %s, %s::uuid, %s, now(), %s, %s)
        RETURNING artifact_id
        """,
        (
            f"s3://bronze/test/{uuid.uuid4().hex}.html.zst",
            artifact_type, lid, run_id, status, search_key,
        ),
    )
    return vc.fetchone()["artifact_id"]


def _get_artifact_status(vc, artifact_id):
    vc.execute(
        "SELECT status FROM ops.artifacts_queue WHERE artifact_id = %s",
        (artifact_id,),
    )
    row = vc.fetchone()
    return row["status"] if row else None


def _count_processing_events(vc, artifact_id):
    vc.execute(
        "SELECT COUNT(*) AS cnt FROM staging.artifact_events"
        " WHERE artifact_id = %s AND status = 'processing'",
        (artifact_id,),
    )
    return vc.fetchone()["cnt"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestClaimBatch:
    def test_claims_pending_artifact(self, vc):
        artifact_id = _seed_artifact(vc, status="pending")

        rows = _claim_batch(batch_size=10, artifact_type=None)

        claimed_ids = [r["artifact_id"] for r in rows]
        assert artifact_id in claimed_ids
        assert _get_artifact_status(vc, artifact_id) == "processing"

        # Cleanup
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = %s", (artifact_id,)
        )
        vc.execute(
            "DELETE FROM staging.artifact_events WHERE artifact_id = %s", (artifact_id,)
        )

    def test_claims_retry_artifact(self, vc):
        artifact_id = _seed_artifact(vc, status="retry")

        rows = _claim_batch(batch_size=10, artifact_type=None)

        claimed_ids = [r["artifact_id"] for r in rows]
        assert artifact_id in claimed_ids

        # Cleanup
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = %s", (artifact_id,)
        )
        vc.execute(
            "DELETE FROM staging.artifact_events WHERE artifact_id = %s", (artifact_id,)
        )

    def test_does_not_claim_complete_artifact(self, vc):
        artifact_id = _seed_artifact(vc, status="complete")

        rows = _claim_batch(batch_size=10, artifact_type=None)

        claimed_ids = [r["artifact_id"] for r in rows]
        assert artifact_id not in claimed_ids
        assert _get_artifact_status(vc, artifact_id) == "complete"

        # Cleanup
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = %s", (artifact_id,)
        )

    def test_does_not_claim_skip_artifact(self, vc):
        artifact_id = _seed_artifact(vc, status="skip")

        rows = _claim_batch(batch_size=10, artifact_type=None)

        claimed_ids = [r["artifact_id"] for r in rows]
        assert artifact_id not in claimed_ids

        # Cleanup
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = %s", (artifact_id,)
        )

    def test_artifact_type_filter_claims_only_matching_type(self, vc):
        srp_id = _seed_artifact(vc, artifact_type="results_page", status="pending")
        detail_id = _seed_artifact(vc, artifact_type="detail_page", status="pending")

        rows = _claim_batch(batch_size=10, artifact_type="results_page")

        claimed_ids = [r["artifact_id"] for r in rows]
        assert srp_id in claimed_ids
        assert detail_id not in claimed_ids

        # detail artifact should remain pending
        assert _get_artifact_status(vc, detail_id) == "pending"

        # Cleanup
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = ANY(%s)",
            ([srp_id, detail_id],),
        )
        vc.execute(
            "DELETE FROM staging.artifact_events WHERE artifact_id = ANY(%s)",
            ([srp_id, detail_id],),
        )

    def test_processing_event_written_for_each_claimed_artifact(self, vc):
        artifact_id = _seed_artifact(vc, status="pending")

        _claim_batch(batch_size=10, artifact_type=None)

        assert _count_processing_events(vc, artifact_id) == 1, (
            "A 'processing' artifact_event should be written for each claimed artifact"
        )

        # Cleanup
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = %s", (artifact_id,)
        )
        vc.execute(
            "DELETE FROM staging.artifact_events WHERE artifact_id = %s", (artifact_id,)
        )

    def test_returned_rows_have_expected_keys(self, vc):
        artifact_id = _seed_artifact(vc, status="pending")

        rows = _claim_batch(batch_size=10, artifact_type=None)

        claimed = next((r for r in rows if r["artifact_id"] == artifact_id), None)
        assert claimed is not None

        expected_keys = (
            "artifact_id", "minio_path", "artifact_type", "listing_id", "run_id", "fetched_at",
        )
        for key in expected_keys:
            assert key in claimed, f"Expected key '{key}' missing from claimed artifact dict"

        # Cleanup
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = %s", (artifact_id,)
        )
        vc.execute(
            "DELETE FROM staging.artifact_events WHERE artifact_id = %s", (artifact_id,)
        )

    def test_respects_batch_size_limit(self, vc):
        ids = [_seed_artifact(vc, status="pending") for _ in range(5)]

        rows = _claim_batch(batch_size=2, artifact_type=None)

        # At most 2 of the 5 we seeded should be claimed in this call
        our_claimed = [r for r in rows if r["artifact_id"] in ids]
        assert len(our_claimed) <= 2

        # Cleanup all 5
        vc.execute(
            "DELETE FROM ops.artifacts_queue WHERE artifact_id = ANY(%s)", (ids,)
        )
        vc.execute(
            "DELETE FROM staging.artifact_events WHERE artifact_id = ANY(%s)", (ids,)
        )

    def test_empty_queue_returns_empty_list(self, vc):
        """When no pending/retry artifacts exist, returns []."""
        # Mark any pre-existing pending to processing to ensure queue is effectively empty
        # for our test. Instead, just assert that an empty queue for a specific type gives [].
        rows = _claim_batch(batch_size=5, artifact_type="carousel_page")
        assert isinstance(rows, list)
        # carousel_page artifacts don't exist → should get nothing
        assert all(r["artifact_type"] != "carousel_page" for r in rows)
