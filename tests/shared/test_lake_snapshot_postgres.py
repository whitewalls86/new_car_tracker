"""Unit tests for shared/lake_snapshot_postgres.py (Plan 162 Stage P)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from shared.lake_snapshot_postgres import (
    POSTGRES_PREFIX,
    POSTGRES_SNAPSHOT_TABLES,
    UnknownSnapshotTableError,
    dump_table,
    load_table,
    parse_snapshot_object_name,
    row_count,
    snapshot_object_name,
)


class TestObjectNames:
    def test_round_trips_every_allowlisted_table(self):
        for schema, table in POSTGRES_SNAPSHOT_TABLES:
            name = snapshot_object_name(schema, table)
            assert name.startswith(POSTGRES_PREFIX)
            assert parse_snapshot_object_name(name) == (schema, table)

    @pytest.mark.parametrize(
        "name",
        [
            # The shape of a table the allowlist has never heard of.
            "postgres/public.users.json",
            # A relation name that would be formatted straight into a statement.
            'postgres/public.search_configs"; DROP TABLE ops.hot; --.json',
            # Right table, wrong prefix -- the prefix is what routes it to a
            # database rather than a bucket, so it is part of the identity.
            "public.search_configs.json",
            "silver_normalized/public.search_configs.json",
            # Traversal, which safe_extract_tar_zst already refuses; refused
            # here too because this guard must not depend on that one.
            "postgres/../public.search_configs.json",
        ],
    )
    def test_refuses_anything_outside_the_allowlist(self, name):
        with pytest.raises(UnknownSnapshotTableError):
            parse_snapshot_object_name(name)

    def test_the_error_names_what_it_expected(self):
        """A refusal that does not say what was acceptable sends the reader to
        the source; this one is the whole diagnosis of an outdated archive."""
        with pytest.raises(UnknownSnapshotTableError) as excinfo:
            parse_snapshot_object_name("postgres/public.users.json")
        assert "postgres/public.search_configs.json" in str(excinfo.value)


class TestRowCount:
    def test_counts_the_stored_array(self):
        assert row_count(json.dumps([{"a": 1}, {"a": 2}])) == 2

    def test_an_empty_export_is_zero_not_an_error(self):
        """The exporter's SELECT coalesces to '[]', so this is the shape an
        empty production table actually produces."""
        assert row_count("[]") == 0


class TestDumpAndLoad:
    def test_dump_formats_the_relation_into_the_statement_and_returns_the_text(self):
        cur = MagicMock()
        cur.fetchone.return_value = ('[{"search_key": "a"}]',)

        assert dump_table(cur, "public", "search_configs") == '[{"search_key": "a"}]'

        sql = cur.execute.call_args[0][0]
        assert "public.search_configs" in sql
        assert "{schema}" not in sql and "{table}" not in sql

    def test_load_binds_the_json_as_a_parameter_never_as_text(self):
        """The rows are the untrusted half and must never be formatted in.

        The relation is an identifier and has to be interpolated; the payload is
        a value and must not be, which is why these two travel through different
        arguments of the same call.
        """
        cur = MagicMock()
        cur.rowcount = 3
        rows_json = '[{"search_key": "a"}]'

        assert load_table(cur, "ops", "tracked_models", rows_json) == 3

        sql, params = cur.execute.call_args[0]
        assert "ops.tracked_models" in sql
        assert params == (rows_json,)
        assert rows_json not in sql

    def test_load_replaces_rather_than_appends(self):
        """V001 seeds public.search_configs, so an INSERT alone hits its primary
        key -- and a caller that remembered the DELETE separately would be one
        forgotten call away from a table holding migration rows and snapshot
        rows at once."""
        cur = MagicMock()
        cur.rowcount = 0
        load_table(cur, "public", "search_configs", "[]")

        sql = cur.execute.call_args[0][0]
        assert "DELETE FROM public.search_configs" in sql
        assert "INSERT INTO public.search_configs" in sql
