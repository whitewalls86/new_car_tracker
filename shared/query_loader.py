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

The cost is a production change made for a test instrument, which is stated
here rather than hidden: :class:`SqlText` is a ``str`` in every respect a
database driver cares about, and a caller that does not ask about ``origin``
cannot tell the difference.
"""
from pathlib import Path


class SqlText(str):
    """A SQL statement that knows which file it was loaded from.

    ``origin`` is the ``.sql`` file's path. ``.format()`` returns another
    :class:`SqlText` carrying the same origin, because a rendered template is
    still that file's statement -- that is the whole reason this type exists.

    Every other ``str`` method returns a plain ``str`` and loses the origin.
    That is deliberate and is not a gap: the recorder reports what it could not
    attribute rather than guessing, so a new transformation applied to a loaded
    statement shows up as an unattributed execution instead of a wrong one.
    """

    # No ``__slots__``: a nonempty one is not permitted on a ``str`` subclass.
    origin: Path

    def __new__(cls, text: str, origin: Path) -> "SqlText":
        instance = super().__new__(cls, text)
        instance.origin = origin
        return instance

    def format(self, *args: object, **kwargs: object) -> "SqlText":
        return SqlText(str.format(self, *args, **kwargs), self.origin)


def load_query(sql_dir: Path, name: str) -> SqlText:
    path = sql_dir / f"{name}.sql"
    return SqlText(path.read_text(encoding="utf-8"), path)
