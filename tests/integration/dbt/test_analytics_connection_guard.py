"""The read-only guard on this directory's warehouse connection.

Deliberately **not** marked `integration`, so it runs in the unit job as well:
it needs no MinIO, no Postgres and no dbt, and the property it protects is
worth knowing about before a 118-second job starts. `docs/TESTING.md` settles
that a file's location does not decide where it runs -- the marker does.

This exists because Plan 162 Stage E replaced a mechanism with a different
mechanism. `read_only=True` on the connection used to make it impossible for
an assertion to mutate the warehouse it was inspecting; in-process dbt holds
the file open read-write, so that is no longer available and
`ReadOnlyConnection` refuses non-read statements instead. A repair with no
assertion behind it is the plan's success criterion 2 failing, so here is the
assertion.
"""

import duckdb
import pytest

from tests.sql_loader import queries

from .real_build import ReadOnlyConnection, refuse_writes

SQL = queries(__file__)


@pytest.fixture()
def con():
    """A throwaway in-memory warehouse, wrapped the way the real one is."""
    raw = duckdb.connect(":memory:")
    raw.execute(SQL("duckdb/create_t"))
    return ReadOnlyConnection(raw)


class TestReadsAreAllowed:
    """The guard is worthless if it also blocks the suite's real queries."""

    def test_a_plain_select_runs(self, con):
        assert con.execute(SQL("duckdb/select_a_from_t_2")).fetchall() == [(1,)]

    def test_a_cte_runs(self, con):
        """`WITH ... SELECT` is the shape most of the real queries take.

        DuckDB's parser types it SELECT, which is the reason classification
        is delegated to `extract_statements` rather than matching a leading
        keyword.
        """
        rows = con.execute(
            SQL("duckdb/select_a_from_c")
        ).fetchall()
        assert rows == [(1,)]

    def test_parameters_are_passed_through(self, con):
        assert con.execute(SQL("duckdb/select_a_from_t_3"), ["x"]).fetchall() == [(1,)]

    def test_fetchone_and_description_still_work(self, con):
        result = con.execute(SQL("duckdb/select_a_b_from_t"))
        assert result.fetchone() == (1, "x")
        assert [column[0] for column in con.description] == ["a", "b"]


class TestWritesAreRefused:
    """Each of these would have been impossible on a read-only connection."""

    @pytest.mark.parametrize("name", [
        "insert_t",
        "update_t",
        "delete_t_row",
        "create_u_as_select",
        "drop_t",
        "alter_t_rename",
        # A read-only connection never had an opinion about this one: it reads
        # the warehouse and writes the filesystem.
        "copy_t_to_file",
        "attach_memory",
    ])
    def test_a_mutating_statement_is_refused(self, con, name):
        with pytest.raises(AssertionError, match="for reading"):
            con.execute(SQL(f"duckdb/{name}"))

    def test_a_write_hidden_behind_a_read_is_refused(self, con):
        """Multi-statement input is where a leading-keyword check would fail."""
        with pytest.raises(AssertionError, match="INSERT"):
            con.execute(SQL("duckdb/select_a_from_t"))

    def test_the_refusal_names_the_statement_type(self, con):
        with pytest.raises(AssertionError, match="DELETE"):
            con.execute(SQL("duckdb/delete_t"))

    def test_nothing_was_written(self, con):
        """The refusal has to happen before execution, not after it."""
        for name in ("insert_t", "delete_t_row"):
            with pytest.raises(AssertionError):
                con.execute(SQL(f"duckdb/{name}"))
        assert con.execute(SQL("duckdb/select_count_from_t")).fetchone() == (1,)


def test_the_allowlist_denies_by_default():
    """The direction that matters: unknown statement kinds are refused.

    An allowlist that grew into a denylist would let the next DuckDB release
    add a writing statement type this suite silently permits.
    """
    with pytest.raises(AssertionError):
        refuse_writes("vacuum")
    with pytest.raises(AssertionError):
        refuse_writes("set threads = 1")
