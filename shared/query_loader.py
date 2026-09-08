"""Load a SQL statement from the file that owns it.

The one function every service reaches an ``.sql`` file through. Plan 162
Stage X gave it a second job: what it returns remembers where it came from, so
that the execution recorder can attribute a string that reached an engine back
to the file it was written in.

**Why a ``str`` subclass rather than a lookup table.** A statement stored as a
template is rendered before it executes -- ``SELECT_STAGING_MAX_PK.format(pk=pk,
table=table)`` -- and what the driver sees is not what any file holds. Matching
a recorded string back to a template by turning the template into a pattern was
the alternative, and it is approximate: a placeholder spanning lines, or one
sitting inside a quoted literal, defeats it. Carrying the origin on the value
is exact, and ``.format()`` is the only transformation production applies to a
loaded statement, so it is the only one that has to preserve it.

**Why a set of origins and not one.** A rendered statement does not always come
from a single file. ``archiver/processors/lake_snapshot_cohort.py`` loads
``wrap_candidate_query.sql`` and formats a *selector's own statement* into it,
so what reaches DuckDB is one string owned by two files. With a single origin
the wrapper was credited and all fourteen selectors read as never executed --
true about the wrapper, false about them, and the falsehood is the one that
would have been repaired by writing fourteen tests for statements that already
run. ``.format()`` unions in the origins of every :class:`SqlText` argument, so
nesting is a case the type answers instead of a case it hides.

The cost is a production change made for a test instrument, which is stated
here rather than hidden: :class:`SqlText` is a ``str`` in every respect a
database driver cares about, and a caller that does not ask about ``origins``
cannot tell the difference.
"""
from collections.abc import Iterable
from pathlib import Path


class SqlText(str):
    """A SQL statement that knows which files it was composed from.

    ``origins`` holds the ``.sql`` paths this text came from: one for a
    statement executed as it was written, more when a loaded statement was
    formatted into another. ``.format()`` returns another :class:`SqlText`
    carrying this statement's origins together with those of any
    :class:`SqlText` argument -- a rendered template is still its file's
    statement, and an embedded one is still its own file's.

    Every other ``str`` method returns a plain ``str`` and loses the origins.
    That is deliberate and is not a gap: the recorder reports what it could not
    attribute rather than guessing, so a new transformation applied to a loaded
    statement shows up as an unattributed execution instead of a wrong one.
    """

    # No ``__slots__``: a nonempty one is not permitted on a ``str`` subclass.
    origins: frozenset[Path]

    def __new__(cls, text: str, origins: Iterable[Path]) -> "SqlText":
        instance = super().__new__(cls, text)
        instance.origins = frozenset(origins)
        return instance

    def format(self, *args: object, **kwargs: object) -> "SqlText":
        nested = [
            value.origins
            for value in (*args, *kwargs.values())
            if isinstance(value, SqlText)
        ]
        return SqlText(str.format(self, *args, **kwargs), self.origins.union(*nested))


def load_query(sql_dir: Path, name: str) -> SqlText:
    path = sql_dir / f"{name}.sql"
    return SqlText(path.read_text(encoding="utf-8"), (path,))
