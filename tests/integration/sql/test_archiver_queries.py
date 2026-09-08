"""
Layer 2 — SQL smoke tests for the archiver service's queue statements.

Imports the constants from ``archiver.queries`` — the same module the archiver
imports — and executes them against Postgres with Flyway's migrations applied.
Nothing here retypes a statement: what runs in this file is the text that runs
in production, which is the only arrangement in which a passing test means
anything.

The lake-snapshot selectors are covered separately by
``tests/integration/archiver/test_lake_snapshot_selectors.py``, which executes
them against real Parquet in MinIO.
"""
import pytest

from archiver.queries import DELETE_CLEANUP_CANDIDATES, GET_QUEUE_CLEANUP_CANDIDATES

pytestmark = pytest.mark.integration


class TestQueueCleanupQueries:
    """Plan 97's artifacts_queue cleanup, both halves.

    ``DELETE_CLEANUP_CANDIDATES`` was inline at its call site in
    ``archiver/processors/cleanup_queue.py`` until Plan 162 Stage L, so it
    could not be imported and nothing executed it. Its sibling
    ``GET_QUEUE_CLEANUP_CANDIDATES`` was already a file and was already waived
    for having no Layer 2 test; both are executed here.
    """

    def test_get_queue_cleanup_candidates(self, cur):
        # cleanup_queue.py reads all four by name to build the delete batch and
        # the MinIO key list, so the projection is what this asserts; no
        # fixture seeds a completed artifact, so there is no row to assert on.
        cur.execute(GET_QUEUE_CLEANUP_CANDIDATES)
        assert cur.fetchall() == []
        assert [column[0] for column in cur.description] == [
            "artifact_id", "minio_path", "artifact_type", "status",
        ]

    def test_delete_cleanup_candidates_matching_nothing(self, cur):
        # -1 matches no artifact_id, so this proves the statement plans and its
        # columns exist without deleting a row the fixture did not create.
        cur.execute(DELETE_CLEANUP_CANDIDATES, ([-1],))
        assert cur.fetchall() == []
