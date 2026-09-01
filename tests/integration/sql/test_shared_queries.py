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

from shared.queries import (
    INSERT_COMPRESSION_DICTIONARY,
    SELECT_COMPRESSION_DICTIONARY,
    SELECT_COMPRESSION_DICTIONARY_REGISTRATION,
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
        cur.execute(SELECT_COMPRESSION_DICTIONARY_REGISTRATION, (0, "v0-absent"))
        assert cur.fetchone() is None

    def test_insert_then_collision_check_finds_it(self, cur):
        dict_id, version = 999_999_999, "v-test-999"
        cur.execute(
            INSERT_COMPRESSION_DICTIONARY,
            (
                dict_id,
                version,
                "s3://bronze/dictionaries/zstd/v-test-999.dict",
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
        cur.execute(SELECT_COMPRESSION_DICTIONARY_REGISTRATION, (dict_id, "v-other"))
        assert cur.fetchone()["dict_id"] == dict_id
        cur.execute(SELECT_COMPRESSION_DICTIONARY_REGISTRATION, (0, version))
        assert cur.fetchone()["version"] == version

        # And the read path resolves the same row.
        cur.execute(SELECT_COMPRESSION_DICTIONARY, (dict_id,))
        assert cur.fetchone() is not None
