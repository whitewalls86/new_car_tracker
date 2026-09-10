"""The version being written must be one the reader accepts.

Plan 162 Stage AA, gap G31.

An earlier draft of this stage restated these integers in ``archiver`` and
``ops`` and asserted the two copies matched. Both copies now live in
``shared/lake_snapshot_schema.py``, so that test would compare a constant to
itself. What survives is the invariant it was really protecting, which is not
equality: ``ops`` refuses a manifest whose version it does not recognise, so a
written version outside the readable set is a total outage of the CI snapshot
download path -- produced by editing one line.
"""
from __future__ import annotations

from shared.lake_snapshot_schema import (
    ARCHIVE_CACHE_SCHEMA_VERSION,
    EXPORT_CACHE_SCHEMA_VERSION,
    READABLE_ARCHIVE_CACHE_SCHEMAS,
    READABLE_EXPORT_CACHE_SCHEMAS,
)


def test_every_written_manifest_version_is_one_the_reader_serves():
    """The bump ordering, enforced rather than documented.

    A format migration goes reader first: add the new version to the readable
    set, deploy ``ops``, then move the written version and deploy ``archiver``.
    Doing it the other way round means every manifest written between the two
    deploys is answered 409 by ``ops``, and this is what stops the second half
    landing without the first.
    """
    assert ARCHIVE_CACHE_SCHEMA_VERSION in READABLE_ARCHIVE_CACHE_SCHEMAS, (
        f"archiver stamps archive_cache_schema_version="
        f"{ARCHIVE_CACHE_SCHEMA_VERSION} and ops serves "
        f"{sorted(READABLE_ARCHIVE_CACHE_SCHEMAS)}. Every manifest written "
        f"under that version would be refused with 409. Add it to the readable "
        f"set and deploy ops before moving the written version."
    )
    assert EXPORT_CACHE_SCHEMA_VERSION in READABLE_EXPORT_CACHE_SCHEMAS, (
        f"archiver stamps export_cache_schema_version="
        f"{EXPORT_CACHE_SCHEMA_VERSION} and ops serves "
        f"{sorted(READABLE_EXPORT_CACHE_SCHEMAS)}. Same failure as above: the "
        f"export half of the manifest moved and the reader did not."
    )


def test_the_readable_sets_are_not_derived_from_the_written_versions():
    """A frozenset built from the constant above could never disagree with it.

    This rule exists because the check that matters is a check between two
    values that *can* differ -- deliberately, during a migration, when the
    readable set holds both the old version and the new one. If someone
    simplifies those literals to ``frozenset({ARCHIVE_CACHE_SCHEMA_VERSION})``
    the tidier code silently deletes the only assertion standing between a
    one-line edit and an outage, and the suite would stay green while it
    happened.
    """
    assert READABLE_ARCHIVE_CACHE_SCHEMAS == frozenset({1})
    assert READABLE_EXPORT_CACHE_SCHEMAS == frozenset({3})
