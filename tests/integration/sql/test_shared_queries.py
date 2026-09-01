"""
Layer 2 — SQL smoke tests for statements owned by the shared library.

``shared/`` is used by several services rather than being one, so its SQL has
no service query module to live in and got its own in Plan 162 Stage 7. The
statement here reads ``ops.compression_dictionaries``, which Plan 129 created
and the decompression path depends on: a rename there breaks every service
that reads a dictionary-compressed artifact, and until now nothing executed it.
"""
import pytest

from shared.queries import SELECT_COMPRESSION_DICTIONARY

pytestmark = pytest.mark.integration


class TestCompressionDictionaryQueries:

    def test_select_compression_dictionary(self, cur):
        cur.execute(SELECT_COMPRESSION_DICTIONARY, (0,))
        # dict_id 0 is never issued, so this asserts the shape of the statement
        # against the real table rather than the contents of any one row.
        assert cur.fetchone() is None
