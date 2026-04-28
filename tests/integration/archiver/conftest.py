"""
Archiver integration test fixtures.

Sets DATABASE_URL from TEST_DATABASE_URL so shared.db.get_conn() connects
to the CI test database. Must run before shared.db is imported.
"""
import os

# Bridge TEST_DATABASE_URL → DATABASE_URL so shared.db.get_conn() works in CI.
_test_url = os.environ.get("TEST_DATABASE_URL", "")
if _test_url:
    os.environ["DATABASE_URL"] = _test_url
