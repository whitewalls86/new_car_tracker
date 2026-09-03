"""
Layer 2 — SQL smoke tests for statements owned by the shared library.

``shared/`` is used by several services rather than being one, so its SQL has
no service query module to live in and got its own in Plan 162 Stage 7. The
statement here reads ``ops.compression_dictionaries``, which Plan 129 created
and the decompression path depends on: a rename there breaks every service
that reads a dictionary-compressed artifact, and until now nothing executed it.
"""
import json

import pytest

from shared.lake_snapshot_postgres import (
    POSTGRES_SNAPSHOT_TABLES,
    dump_table,
    load_table,
)
from shared.queries import (
    INSERT_COMPRESSION_DICTIONARY,
    REPLACE_POSTGRES_SNAPSHOT_TABLE,
    SELECT_COMPRESSION_DICTIONARY,
    SELECT_COMPRESSION_DICTIONARY_REGISTRATION,
    SELECT_DEPLOY_INTENT_PAUSE,
    SELECT_POSTGRES_SNAPSHOT_TABLE,
)

pytestmark = pytest.mark.integration


class TestCompressionDictionaryQueries:

    def test_select_compression_dictionary(self, cur):
        cur.execute(SELECT_COMPRESSION_DICTIONARY, (0,))
        # dict_id 0 is never issued, so this asserts the shape of the statement
        # against the real table rather than the contents of any one row.
        assert cur.fetchone() is None


class TestDictionaryRegistrationQueries:
    """The write half of ``ops.compression_dictionaries``.

    ``scripts/train_html_dictionary.py`` held both of these at its call sites
    until Plan 162 Stage 7, which meant a second statement against a table
    ``shared/compression.py`` already owned, in a file no rule scanned. They
    are executed here in the order the script runs them: the collision check
    refuses first, the insert registers second.
    """

    def test_registration_collision_check_matches_nothing(self, cur):
        cur.execute(SELECT_COMPRESSION_DICTIONARY_REGISTRATION, (0, 999_999_998))
        assert cur.fetchone() is None

    def test_insert_then_collision_check_finds_it(self, cur):
        # version is integer NOT NULL CHECK (version > 0) -- V041, not text.
        dict_id, version = 999_999_999, 999_999_999
        cur.execute(
            INSERT_COMPRESSION_DICTIONARY,
            (
                dict_id,
                version,
                "s3://bronze/dictionaries/zstd/v999999999.dict",
                b"\x00\x01",
                2,
                19,
                json.dumps({"steps": 4}),
                json.dumps(["sample/key.html.zst"]),
                "0" * 64,
            ),
        )
        assert cur.rowcount == 1

        # The collision check is what makes the dictionary immutable: it has to
        # see the row this insert just wrote, on either half of the identity.
        cur.execute(SELECT_COMPRESSION_DICTIONARY_REGISTRATION, (dict_id, 999_999_997))
        assert cur.fetchone()["dict_id"] == dict_id
        cur.execute(SELECT_COMPRESSION_DICTIONARY_REGISTRATION, (0, version))
        assert cur.fetchone()["version"] == version

        # And the read path resolves the same row.
        cur.execute(SELECT_COMPRESSION_DICTIONARY, (dict_id,))
        assert cur.fetchone() is not None


class TestDeployIntentPause:
    """The cooperative pause every long-running job checks at its boundaries.

    It reads ``deploy_intent`` -- a singleton row in the public schema, not
    under ``ops`` -- and until Plan 162 it lived in a Python literal in
    ``shared/deploy_intent.py`` with nothing executing it. A rename of either
    column would have surfaced as a ten-hour pack that stopped checking, failing
    open and silently.
    """

    def test_returns_one_boolean_for_the_singleton_row(self, cur):
        cur.execute(SELECT_DEPLOY_INTENT_PAUSE)
        row = cur.fetchone()
        assert row is not None, "deploy_intent row 1 is created by migration"
        assert len(row) == 1
        assert isinstance(next(iter(row.values())), bool)

    def test_true_only_when_the_intent_is_pending_and_the_flag_is_set(self, cur):
        """Both halves matter. A `pending` intent that does not ask for the
        pause must not stop a job, and a set flag with no pending deploy must
        not either -- the AND is the statement's whole content."""
        for intent, pause, expected in (
            ("pending", True, True),
            ("pending", False, False),
            ("none", True, False),
        ):
            cur.execute(
                "UPDATE deploy_intent SET intent = %s, pause_long_jobs = %s WHERE id = 1",
                (intent, pause),
            )
            cur.execute(SELECT_DEPLOY_INTENT_PAUSE)
            assert next(iter(cur.fetchone().values())) is expected, (intent, pause)


class TestPostgresSnapshotTableQueries:
    """The two halves of the Plan 162 Stage 10 round trip, executed together.

    Neither statement is interesting alone: the export's value is that Postgres
    produces exactly the JSON its own rowtype reads back, so every cast --
    timestamptz from an ISO string, jsonb from a nested object -- happens in the
    engine rather than in a mapping written in Python. Asserting that means
    running both against the real tables, on every relation the allowlist names.

    A plain cursor, not the ``cur`` fixture: ``dump_table`` reads the single
    column positionally, which is what ``shared.db.get_conn`` hands the exporter
    in production. Binding to a RealDictCursor here would test a shape nothing
    runs.
    """

    def test_the_helpers_execute_the_published_statements_not_a_copy(self, db_conn):
        """``dump_table`` and ``load_table`` are the only callers that format a
        relation into ``SELECT_POSTGRES_SNAPSHOT_TABLE`` and
        ``REPLACE_POSTGRES_SNAPSHOT_TABLE``. Running the constants here directly
        and comparing is what stops the helpers drifting into a paraphrase --
        the failure mode the whole .sql-file convention exists to prevent."""
        with db_conn.cursor() as cursor:
            cursor.execute(
                SELECT_POSTGRES_SNAPSHOT_TABLE.format(
                    schema="public", table="search_configs",
                )
            )
            direct = cursor.fetchone()[0]
            assert dump_table(cursor, "public", "search_configs") == direct

            cursor.execute(
                REPLACE_POSTGRES_SNAPSHOT_TABLE.format(
                    schema="ops", table="tracked_models",
                ),
                ('[{"search_key": "k", "make": "toyota", "model": "camry"}]',),
            )
            assert cursor.rowcount == 1

    @pytest.mark.parametrize(("schema", "table"), POSTGRES_SNAPSHOT_TABLES)
    def test_dump_then_replace_round_trips_the_whole_table(self, db_conn, schema, table):
        with db_conn.cursor() as cursor:
            before = dump_table(cursor, schema, table)
            rows_before = json.loads(before)

            written = load_table(cursor, schema, table, before)
            assert written == len(rows_before)

            after = dump_table(cursor, schema, table)

        # Byte-identical, not merely equivalent: the statement's ORDER BY is a
        # total order over the jsonb text, which is what makes the archive's
        # checksum stable across two exports of the same data.
        assert after == before

    @pytest.mark.parametrize(("schema", "table"), POSTGRES_SNAPSHOT_TABLES)
    def test_an_empty_table_dumps_as_an_array_not_null(self, db_conn, schema, table):
        """jsonb_agg over no rows is NULL, which json.loads cannot read and the
        seeder would carry all the way to a jsonb cast failure. The coalesce in
        the statement is what makes an empty production table a zero-row export."""
        with db_conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {schema}.{table}")
            assert json.loads(dump_table(cursor, schema, table)) == []

    def test_replace_removes_the_rows_that_were_there_before(self, db_conn):
        """V001 seeds public.search_configs, so the DELETE inside the statement
        is what stops a seed from leaving migration rows and snapshot rows in
        the same table."""
        with db_conn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM public.search_configs")
            assert cursor.fetchone()[0] > 0, "V001 should have seeded search_configs"

            load_table(cursor, "public", "search_configs", "[]")

            cursor.execute("SELECT count(*) FROM public.search_configs")
            assert cursor.fetchone()[0] == 0

    def test_a_jsonb_column_survives_the_round_trip_as_json(self, db_conn):
        """``params`` is the one column with structure, and
        ``stg_search_configs`` reads into it with ``->>`` and
        ``json_extract_string``. A round trip that stringified it would compile
        and return nulls."""
        with db_conn.cursor() as cursor:
            load_table(
                cursor, "public", "search_configs",
                json.dumps([{
                    "search_key": "roundtrip-test",
                    "enabled": True,
                    "source": "cars.com",
                    "params": {"zip": "60614", "makes": ["toyota"]},
                    "created_at": "2026-09-03T00:00:00+00:00",
                    "updated_at": "2026-09-03T00:00:00+00:00",
                    "rotation_order": None,
                    "last_queued_at": None,
                    "rotation_slot": 1,
                }]),
            )
            cursor.execute(
                "SELECT params->>'zip', params->'makes'->>0 FROM public.search_configs"
            )
            assert cursor.fetchone() == ("60614", "toyota")

            # And it comes back out as an object rather than a quoted string.
            dumped = json.loads(dump_table(cursor, "public", "search_configs"))
            assert dumped[0]["params"]["makes"] == ["toyota"]
