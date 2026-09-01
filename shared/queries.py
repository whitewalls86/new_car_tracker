"""
SQL query constants for the shared library.

Same arrangement as the four services' ``queries.py``: statements live in
``.sql`` files and are loaded at import time, so a test can execute the exact
text production runs rather than a retyped copy of it.

``shared/`` gets its own because ``compression.py`` reads
``ops.compression_dictionaries`` directly -- it is a library used by several
services rather than one of them, so the statement belongs to none of their
query modules.
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
