"""
SQL query constants for the shared library.

Same arrangement as the four services' ``queries.py``: statements live in
``.sql`` files and are loaded at import time, so a test can execute the exact
text production runs rather than a retyped copy of it.

``shared/`` gets its own because ``compression.py`` reads
``ops.compression_dictionaries`` directly -- it is a library used by several
services rather than one of them, so the statement belongs to none of their
query modules.

The same reasoning admits statements *two* services issue against tables they
share. ``ops`` and ``processing`` both mark an artifact's status and both emit
its queue event, and both wrote to ``ops.artifacts_queue`` and
``staging.artifacts_queue_events`` through byte-identical copies of the same
three files until Plan 162 Stage 7. Duplicating the SQL decoupled nothing --
the schema already couples them -- it only made two places to edit, with
nothing to notice when one moved. Each service re-exports these below, so
``ops.queries.MARK_ARTIFACT_STATUS`` still resolves and no call site changed.
"""
from pathlib import Path

from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"


def _q(name: str) -> str:
    return load_query(_SQL_DIR, name)


# Plan 129: trained zstd dictionaries
SELECT_COMPRESSION_DICTIONARY = _q("select_compression_dictionary")
# The registration pair, used by scripts/train_html_dictionary.py. They live
# here rather than in the script because the table is shared/compression.py's:
# a second statement against it, written at a call site, is the drift this
# arrangement exists to prevent.
SELECT_COMPRESSION_DICTIONARY_REGISTRATION = _q(
    "select_compression_dictionary_registration"
)
INSERT_COMPRESSION_DICTIONARY = _q("insert_compression_dictionary")

# Shared by ops and processing, which issue them against the same tables.
# One definition rather than two copies: see the module docstring.
MARK_ARTIFACT_STATUS = _q("mark_artifact_status")
INSERT_ARTIFACT_EVENT = _q("insert_artifact_event")
INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT = _q("insert_blocked_cooldown_cleared_event")

# Plan 162 Stage 10: the two Postgres dimension tables that travel inside a
# Plan 120 lake snapshot. Templates over a schema-qualified relation, filled
# only from shared.lake_snapshot_postgres.POSTGRES_SNAPSHOT_TABLES -- the
# producer (archiver's exporter) and the consumer (scripts/seed_lake_snapshot)
# are two halves of one round trip, so both statements live in one place for
# the same reason the artifact pair above does.
SELECT_POSTGRES_SNAPSHOT_TABLE = _q("select_postgres_snapshot_table")
REPLACE_POSTGRES_SNAPSHOT_TABLE = _q("replace_postgres_snapshot_table")
