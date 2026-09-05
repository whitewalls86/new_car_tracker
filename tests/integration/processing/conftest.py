"""
Integration test fixtures for the processing service.

Provides seed helpers for artifacts_queue, price_observations,
vin_to_listing, detail_scrape_claims, and search_configs.

Two fixture families:
  - Rollback-based (cur): for SQL-level tests that use the shared db_conn
  - Committed (vc / pg_conn): for writer-function tests that call real Python
    functions which open their own connections via shared.db.db_cursor
"""
import os
import tempfile
import uuid

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

from tests.sql_loader import queries

SQL = queries(__file__)

# ---------------------------------------------------------------------------
# Ensure shared.db connects to the test database, not the Docker hostname.
# Set env vars before any service imports so DB_KWARGS is built correctly.
# ---------------------------------------------------------------------------
os.environ.setdefault("PGHOST", "localhost")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGDATABASE", "cartracker")
os.environ.setdefault("PGUSER", "cartracker")
os.environ.setdefault("POSTGRES_PASSWORD", "cartracker")
os.environ.setdefault("LOG_PATH", os.path.join(tempfile.gettempdir(), "processing_test.log"))

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)


def _parse_dsn(url: str) -> dict:
    from urllib.parse import urlparse
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 5432,
        "dbname": p.path.lstrip("/") or "cartracker",
        "user": p.username or "cartracker",
        "password": p.password or "cartracker",
    }


@pytest.fixture(scope="session", autouse=True)
def _patch_shared_db_kwargs():
    """
    Ensure shared.db.DB_KWARGS targets localhost, regardless of import order.
    Runs once per session and restores the original values on teardown.
    """
    import shared.db
    original = dict(shared.db.DB_KWARGS)
    shared.db.DB_KWARGS.update(_parse_dsn(_DATABASE_URL))
    yield
    shared.db.DB_KWARGS.update(original)


# ---------------------------------------------------------------------------
# Autocommit connection fixtures (for writer-function integration tests)
# ---------------------------------------------------------------------------

@pytest.fixture()
def pg_conn():
    """
    Autocommit psycopg2 connection.

    Data inserted here is immediately visible to writer functions that open
    their own connections via shared.db.db_cursor.
    """
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture()
def vc(pg_conn):
    """Autocommit RealDictCursor — shorthand for verify cursor."""
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        yield cur


# ---------------------------------------------------------------------------
# Committed seed factories (use vc — autocommit, with explicit cleanup)
# ---------------------------------------------------------------------------

@pytest.fixture()
def seed_artifact_c(vc):
    """
    Factory: insert ops.artifacts_queue row via autocommit; delete on teardown.

    Returns a factory callable. Tracks all inserted artifact_ids for cleanup.
    """
    inserted_ids = []

    def _factory(
        artifact_type="results_page",
        listing_id=None,
        minio_path="s3://bronze/test/artifact.html.zst",
        status="pending",
        run_id=None,
        search_key=None,
    ):
        listing_id = listing_id or str(uuid.uuid4())
        run_id = run_id or str(uuid.uuid4())
        vc.execute(
            SQL("insert_ops_artifacts_queue"),
            (minio_path, artifact_type, listing_id, run_id, status, search_key),
        )
        artifact_id = vc.fetchone()["artifact_id"]
        inserted_ids.append(artifact_id)
        return {
            "artifact_id": artifact_id,
            "artifact_type": artifact_type,
            "listing_id": listing_id,
            "minio_path": minio_path,
            "run_id": run_id,
            "status": status,
        }

    yield _factory

    if inserted_ids:
        vc.execute(
            SQL("delete_ops_artifacts_queue"),
            (inserted_ids,),
        )


@pytest.fixture()
def seed_price_observation_c(vc):
    """Factory: insert ops.price_observations row via autocommit; delete on teardown."""
    inserted_ids = []

    def _factory(
        listing_id=None, vin=None, price=25000,
        make="Honda", model="CR-V", customer_id=None, artifact_id=1,
    ):
        listing_id = listing_id or str(uuid.uuid4())
        vc.execute(
            SQL("insert_ops_price_observations"),
            (listing_id, vin, price, make, model, customer_id, artifact_id),
        )
        inserted_ids.append(listing_id)
        return listing_id

    yield _factory

    if inserted_ids:
        vc.execute(
            SQL("delete_ops_price_observations"),
            (inserted_ids,),
        )


@pytest.fixture()
def seed_vin_to_listing_c(vc):
    """Factory: insert ops.vin_to_listing row via autocommit; delete on teardown."""
    inserted_vins = []

    def _factory(vin, listing_id, artifact_id=1, mapped_at=None):
        vc.execute(
            SQL("insert_ops_vin_to_listing"),
            (vin, listing_id, mapped_at, artifact_id),
        )
        inserted_vins.append(vin)

    yield _factory

    if inserted_vins:
        vc.execute(
            SQL("delete_ops_vin_to_listing"),
            (inserted_vins,),
        )


@pytest.fixture()
def seed_detail_claim_c(vc):
    """Factory: insert ops.detail_scrape_claims row via autocommit; delete on teardown."""
    inserted_ids = []

    def _factory(listing_id, run_id=None):
        run_id = run_id or str(uuid.uuid4())
        vc.execute(
            SQL("insert_ops_detail_scrape_claims"),
            (listing_id, run_id),
        )
        inserted_ids.append(listing_id)
        return run_id

    yield _factory

    if inserted_ids:
        vc.execute(
            SQL("delete_ops_detail_scrape_claims"),
            (inserted_ids,),
        )


@pytest.fixture()
def seed_tracked_model_c(vc):
    """
    Factory: insert search_configs + ops.tracked_models row via autocommit.

    Seeds the minimum data for carousel filtering to match a make/model.
    Returns search_key.
    """
    inserted_keys = []

    def _factory(make: str, model: str, search_key: str | None = None):
        key = search_key or f"test-{make.lower()}-{uuid.uuid4().hex[:8]}"
        vc.execute(
            SQL("insert_search_configs_2"),
            (key,),
        )
        vc.execute(
            SQL("insert_ops_tracked_models"),
            (key, make.lower(), model.lower()),
        )
        inserted_keys.append(key)
        return key

    yield _factory

    if inserted_keys:
        vc.execute(
            SQL("delete_ops_tracked_models"),
            (inserted_keys,),
        )
        vc.execute(
            SQL("delete_search_configs"),
            (inserted_keys,),
        )


@pytest.fixture()
def clear_tracked_models_cache():
    """Reset the in-process carousel filter cache so DB changes are visible immediately."""
    import processing.writers.detail_writer as dw
    dw._TRACKED_MODELS_CACHE = None
    yield
    dw._TRACKED_MODELS_CACHE = None


# ---------------------------------------------------------------------------
# Rollback-based fixtures (unchanged — used by existing SQL-level tests)
# ---------------------------------------------------------------------------

@pytest.fixture()
def seed_artifact(cur):
    """
    Factory fixture: insert an artifacts_queue row.

    Usage:
        artifact = seed_artifact(artifact_type="results_page", listing_id="...")
    """
    def _factory(
        artifact_type="results_page",
        listing_id=None,
        minio_path="s3://bronze/test/artifact.html.zst",
        status="pending",
        run_id=None,
    ):
        listing_id = listing_id or str(uuid.uuid4())
        run_id = run_id or str(uuid.uuid4())
        cur.execute(
            SQL("insert_ops_artifacts_queue_2"),
            (minio_path, artifact_type, listing_id, run_id, status),
        )
        artifact_id_val = cur.fetchone()["artifact_id"]
        return {
            "artifact_id": artifact_id_val,
            "artifact_type": artifact_type,
            "listing_id": listing_id,
            "minio_path": minio_path,
            "run_id": run_id,
            "status": status,
        }

    return _factory


@pytest.fixture()
def seed_price_observation(cur):
    """Factory: insert a price_observations row."""
    def _factory(listing_id=None, vin=None, price=25000, make="Honda", model="CR-V",
                 customer_id=None, artifact_id=1):
        listing_id = listing_id or str(uuid.uuid4())
        cur.execute(
            SQL("insert_ops_price_observations"),
            (listing_id, vin, price, make, model, customer_id, artifact_id),
        )
        return listing_id

    return _factory


@pytest.fixture()
def seed_vin_to_listing(cur):
    """Factory: insert a vin_to_listing row."""
    def _factory(vin, listing_id, artifact_id=1, mapped_at=None):
        cur.execute(
            SQL("insert_ops_vin_to_listing"),
            (vin, listing_id, mapped_at, artifact_id),
        )

    return _factory


@pytest.fixture()
def seed_detail_claim(cur):
    """Factory: insert a detail_scrape_claims row."""
    def _factory(listing_id, run_id=None):
        run_id = run_id or str(uuid.uuid4())
        cur.execute(
            SQL("insert_ops_detail_scrape_claims"),
            (listing_id, run_id),
        )
        return run_id

    return _factory


@pytest.fixture()
def seed_honda_search_config(cur):
    """Insert a search_config for Honda that carousel filtering can match against."""
    key = f"test-honda-{uuid.uuid4().hex[:8]}"
    cur.execute(
        SQL("insert_search_configs"),
        (key,),
    )
    return key
