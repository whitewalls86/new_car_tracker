"""
Layer 2 — SQL smoke tests for flush_staging_events.

Validates the SELECT / DELETE SQL patterns used by flush_staging_events against
a real DB with Flyway migrations applied. The goal is to catch schema breakage
(column renames, type changes, dropped tables) — not to test business logic.

All tests run inside a rolled-back transaction; no data persists.
"""
import uuid
from datetime import datetime, timezone

import pytest

from archiver.processors.flush_staging_events import _TABLE_CONFIGS
from archiver.queries import (
    DELETE_STAGING_ROWS_UP_TO_PK,
    SELECT_STAGING_MAX_PK,
    SELECT_STAGING_ROWS_UP_TO_PK,
)
from tests.sql_loader import queries

SQL = queries(__file__)

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _insert_aq_event(cur, artifact_id=None) -> int:
    """Insert a row into staging.artifacts_queue_events. Returns event_id."""
    minio = f"s3://bronze/html/year=2026/month=4/results_page/{uuid.uuid4()}.html.zst"
    cur.execute(
        SQL("insert_staging_artifacts_queue_events"),
        (artifact_id or 999999, _NOW, minio, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_claim_event(cur) -> int:
    """Insert a row into staging.detail_scrape_claim_events. Returns event_id."""
    listing_id = str(uuid.uuid4())
    cur.execute(
        SQL("insert_staging_detail_scrape_claim_events"),
        (listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_blocked_event(cur) -> int:
    """Insert a row into staging.blocked_cooldown_events. Returns event_id."""
    listing_id = str(uuid.uuid4())
    cur.execute(
        SQL("insert_staging_blocked_cooldown_events"),
        (listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_price_event(cur) -> int:
    """Insert a row into staging.price_observation_events. Returns event_id."""
    listing_id = str(uuid.uuid4())
    cur.execute(
        SQL("insert_staging_price_observation_events"),
        (listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_vin_event(cur) -> int:
    """Insert a row into staging.vin_to_listing_events. Returns event_id."""
    vin = str(uuid.uuid4())
    listing_id = str(uuid.uuid4())
    cur.execute(
        SQL("insert_staging_vin_to_listing_events"),
        (vin, listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


# ---------------------------------------------------------------------------
# SELECT MAX(event_id) — snapshot boundary query
# ---------------------------------------------------------------------------

class TestSelectMaxEventId:
    def test_aq_events_max_returns_none_when_empty(self, cur):
        cur.execute(SQL("select_max_from_staging_artifacts_queue_events"))
        assert cur.fetchone()["max"] is None

    def test_aq_events_max_returns_inserted_id(self, cur):
        event_id = _insert_aq_event(cur)
        cur.execute(SQL("select_max_from_staging_artifacts_queue_events"))
        assert cur.fetchone()["max"] == event_id

    def test_claim_events_max_returns_none_when_empty(self, cur):
        cur.execute(SQL("select_max_from_staging_detail_scrape_claim_events"))
        assert cur.fetchone()["max"] is None

    def test_blocked_events_max_returns_none_when_empty(self, cur):
        cur.execute(SQL("select_max_from_staging_blocked_cooldown_events"))
        assert cur.fetchone()["max"] is None

    def test_price_events_max_returns_none_when_empty(self, cur):
        cur.execute(SQL("select_max_from_staging_price_observation_events"))
        assert cur.fetchone()["max"] is None

    def test_vin_events_max_returns_none_when_empty(self, cur):
        cur.execute(SQL("select_max_from_staging_vin_to_listing_events"))
        assert cur.fetchone()["max"] is None


# ---------------------------------------------------------------------------
# SELECT cols WHERE event_id <= max — fetch rows query
# ---------------------------------------------------------------------------

class TestSelectRowsUpToMax:
    def test_aq_events_select_columns_present(self, cur):
        _insert_aq_event(cur)
        cur.execute(
            SQL("select_event_id_artifact_id_status_from_staging_artifacts_queue_events")
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "artifact_id", "status", "event_at",
                    "minio_path", "artifact_type", "fetched_at", "listing_id", "run_id"):
            assert col in row

    def test_claim_events_select_columns_present(self, cur):
        _insert_claim_event(cur)
        cur.execute(
            SQL("select_event_id_listing_id_from_staging_detail_scrape_claim_events")
        )
        row = cur.fetchone()
        assert row is not None
        for col in (
            "event_id", "listing_id", "run_id", "status", "stale_reason", "vin", "event_at"
        ):
            assert col in row

    def test_blocked_events_select_columns_present(self, cur):
        _insert_blocked_event(cur)
        cur.execute(
            SQL("select_event_id_listing_id_from_staging_blocked_cooldown_events")
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "listing_id", "event_type", "num_of_attempts", "event_at"):
            assert col in row

    def test_price_events_select_columns_present(self, cur):
        _insert_price_event(cur)
        cur.execute(
            SQL("select_event_id_listing_id_vin_from_staging_price_observation_events")
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "listing_id", "vin", "price", "make", "model",
                    "artifact_id", "event_type", "source", "event_at"):
            assert col in row

    def test_vin_events_select_columns_present(self, cur):
        _insert_vin_event(cur)
        cur.execute(
            SQL("select_event_id_vin_listing_id_from_staging_vin_to_listing_events")
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "vin", "listing_id", "artifact_id",
                    "event_type", "previous_listing_id", "event_at"):
            assert col in row

    def test_snapshot_boundary_excludes_later_rows(self, cur):
        """Rows with event_id > max_pk at snapshot time must not be selected."""
        id1 = _insert_aq_event(cur)
        id2 = _insert_aq_event(cur)
        # Snapshot was taken before id2 was inserted — simulate by using id1 as boundary
        cur.execute(
            SQL("select_event_id_from_staging_artifacts_queue_events"),
            (id1,),
        )
        returned = {r["event_id"] for r in cur.fetchall()}
        assert id1 in returned
        assert id2 not in returned


# ---------------------------------------------------------------------------
# DELETE WHERE event_id <= max — flush delete query
# ---------------------------------------------------------------------------

class TestDeleteUpToMax:
    def test_aq_events_delete_by_max(self, cur):
        eid = _insert_aq_event(cur)
        cur.execute(
            SQL("delete_staging_artifacts_queue_events"), (eid,)
        )
        cur.execute(
            SQL("select_event_id_from_staging_artifacts_queue_events_2"), (eid,)
        )
        assert cur.fetchone() is None

    def test_claim_events_delete_by_max(self, cur):
        eid = _insert_claim_event(cur)
        cur.execute(
            SQL("delete_staging_detail_scrape_claim_events"), (eid,)
        )
        cur.execute(
            SQL("select_event_id_from_staging_detail_scrape_claim_events"), (eid,)
        )
        assert cur.fetchone() is None

    def test_blocked_events_delete_by_max(self, cur):
        eid = _insert_blocked_event(cur)
        cur.execute(
            SQL("delete_staging_blocked_cooldown_events"), (eid,)
        )
        cur.execute(
            SQL("select_event_id_from_staging_blocked_cooldown_events"), (eid,)
        )
        assert cur.fetchone() is None

    def test_price_events_delete_by_max(self, cur):
        eid = _insert_price_event(cur)
        cur.execute(
            SQL("delete_staging_price_observation_events"), (eid,)
        )
        cur.execute(
            SQL("select_event_id_from_staging_price_observation_events"), (eid,)
        )
        assert cur.fetchone() is None

    def test_vin_events_delete_by_max(self, cur):
        eid = _insert_vin_event(cur)
        cur.execute(
            SQL("delete_staging_vin_to_listing_events"), (eid,)
        )
        cur.execute(
            SQL("select_event_id_from_staging_vin_to_listing_events"), (eid,)
        )
        assert cur.fetchone() is None

    def test_delete_only_affects_rows_up_to_boundary(self, cur):
        """Rows inserted after the snapshot boundary must survive the delete."""
        id1 = _insert_aq_event(cur)
        id2 = _insert_aq_event(cur)
        cur.execute(
            SQL("delete_staging_artifacts_queue_events"), (id1,)
        )
        cur.execute(
            SQL("select_event_id_from_staging_artifacts_queue_events_2"), (id2,)
        )
        assert cur.fetchone() is not None


# ===========================================================================
# Statements imported from archiver.queries — Plan 162 Stage L
# ===========================================================================

# The parametrisation is _TABLE_CONFIGS itself, not a list of table names
# copied out of it. A staging table added to the flush is covered here the day
# it is added; a list retyped here would leave the eighth table untested and
# still green, which is the same failure as a retyped statement one level up.
_CONFIG_IDS = [config["table"] for config in _TABLE_CONFIGS]


class TestExtractedStagingFlushStatements:
    """The three statements of the flush, as ``_flush_one`` itself holds them.

    Everything above retypes SQL that resembles what the processor runs, table
    by table. Until Plan 162 Stage L there was no alternative: all three
    statements were f-strings at their ``cur.execute()`` call sites, so no test
    could import them. These execute ``archiver.queries``' constants — the same
    objects ``_flush_one`` executes — filled from the same ``_TABLE_CONFIGS``
    entries production fills them from. The relation and primary-key names are
    identifiers, so they are formatted in rather than bound; every value they
    can take is in this parametrisation.

    Postgres (``cur``), not DuckDB: the flush reads and deletes the
    ``staging.*`` tables over psycopg2, and only the Parquet write in between
    touches MinIO — that half is pyarrow, not SQL.

    Two of the seven tables the flush serves —
    ``staging.coordination_state_events`` and
    ``staging.coordination_release_evidence`` — appear in this file for the
    first time here, because the paraphrases above were written against five.
    """

    @pytest.mark.parametrize("config", _TABLE_CONFIGS, ids=_CONFIG_IDS)
    def test_select_max_pk_plans_for_every_flushed_table(self, cur, config):
        cur.execute(
            SELECT_STAGING_MAX_PK.format(pk=config["pk"], table=config["table"])
        )
        assert "max" in cur.fetchone()

    @pytest.mark.parametrize("config", _TABLE_CONFIGS, ids=_CONFIG_IDS)
    def test_every_column_the_flush_projects_exists(self, cur, config):
        """db_columns against the real tables — the drift this rule exists for.

        A column dropped or renamed by a migration fails here, in the exact
        projection the flush issues for that table, rather than in production.
        The boundary is ``-1`` so no seeded row is needed: an empty result
        still proves the statement parsed and every named column resolved.
        """
        cur.execute(
            SELECT_STAGING_ROWS_UP_TO_PK.format(
                columns=", ".join(config["db_columns"]),
                table=config["table"],
                pk=config["pk"],
            ),
            (-1,),
        )
        returned = {description[0] for description in cur.description}
        assert set(config["db_columns"]) == returned

    @pytest.mark.parametrize("config", _TABLE_CONFIGS, ids=_CONFIG_IDS)
    def test_delete_up_to_pk_plans_for_every_flushed_table(self, cur, config):
        cur.execute(
            DELETE_STAGING_ROWS_UP_TO_PK.format(table=config["table"], pk=config["pk"]),
            (-1,),
        )
        assert cur.rowcount == 0

    def test_the_three_statements_round_trip_one_real_row(self, cur):
        """One table end to end, in the order ``_flush_one`` runs them."""
        config = next(
            c for c in _TABLE_CONFIGS if c["table"] == "staging.artifacts_queue_events"
        )
        event_id = _insert_aq_event(cur)

        cur.execute(
            SELECT_STAGING_MAX_PK.format(pk=config["pk"], table=config["table"])
        )
        max_pk = cur.fetchone()["max"]
        assert max_pk == event_id

        cur.execute(
            SELECT_STAGING_ROWS_UP_TO_PK.format(
                columns=", ".join(config["db_columns"]),
                table=config["table"],
                pk=config["pk"],
            ),
            (max_pk,),
        )
        rows = cur.fetchall()
        assert [r[config["pk"]] for r in rows] == sorted(r[config["pk"]] for r in rows)
        assert event_id in {r[config["pk"]] for r in rows}

        cur.execute(
            DELETE_STAGING_ROWS_UP_TO_PK.format(table=config["table"], pk=config["pk"]),
            (max_pk,),
        )
        assert cur.rowcount == len(rows)
