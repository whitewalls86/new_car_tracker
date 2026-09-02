"""Layer 4 fixtures for `scraper`: a loopback origin, and nothing mocked.

`scraper`'s dependency is an HTTP origin it does not own, so the Layer 4
question — "endpoint behaviour against a real dependency" — is answered the way
`container_health` answers it: a real HTTP server on loopback serving responses
recorded from the real one, with the whole path left intact.

**Nothing is mocked, and that is the entire point of this suite.** Until Plan
162 Stage 8, `POST /scrape_detail` had never run unmocked in any layer:
`tests/scraper/conftest.py` patches `shared.db.get_conn` and
`shared.minio.write_html` for every test in that directory, autouse, so the
half of the request path that writes — MinIO object, `ops.artifacts_queue` row,
`staging.artifacts_queue_events` twin, and the blocked-cooldown pair on a 403 —
was reached by nothing. Here the path runs end to end: `TestClient` → router →
`scrape_detail_fetch` → `_fetch_url` → `curl_cffi` → HTTP → zstd → real MinIO →
real Postgres.

**Why a loopback origin can carry this and a recorded corpus cannot carry
`container_health`'s.** `payload.url` is caller-supplied in production — the
processor falls back to a cars.com URL only when the caller omits one — so
pointing the fetch at `127.0.0.1` exercises the production code path rather
than a test-only branch. The pages served are the real captures already in
`tests/fixtures/html/`, recorded from cars.com.

**What this suite cannot see**, stated so it is a decision: cars.com changing
its markup, or Cloudflare changing what it serves. `container_health` answers
that with a verifier script against the real proxy; there is no equivalent here
and there should not be one — a CI job that fetched cars.com would be scraping
from a datacentre IP, which is the thing the whole cooldown mechanism exists to
survive. The parser suites in `tests/processing/` own markup drift instead,
against the same captures.

`FLARESOLVERR_URL` is read into a module constant at import, and it defaults to
a **non-empty** `http://flaresolverr:8191` — so the solver branch is entered
and only falls through on a connection error, costing a timeout per fetch. The
CI step sets it empty; this conftest sets it before any scraper import so a
local run behaves the same way.

Plan 162 Stage 8.
"""
import gzip
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# ---------------------------------------------------------------------------
# Env, before any scraper import reaches module scope.
#
# ``setdefault`` only, and never ``FLARESOLVERR_URL``. A conftest is imported
# at collection, so an assignment here reaches every test in the session, not
# only this directory's -- and setting ``FLARESOLVERR_URL`` empty at import
# silently turned twelve ``tests/scraper/test_metrics.py`` tests green-to-red
# by short-circuiting the solver branch they exist to count. That is the
# harness deciding another test's outcome, in the suite whose whole subject is
# that nothing is faked. The solver is disabled by the fixture below instead,
# scoped to this session's scraper imports and reversed afterwards.
# ---------------------------------------------------------------------------
os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("MINIO_ROOT_USER", "cartracker")
os.environ.setdefault("MINIO_ROOT_PASSWORD", "cartracker123")
os.environ.setdefault("MINIO_BUCKET", "bronze")
os.environ.setdefault("PGHOST", "localhost")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGDATABASE", "cartracker")
os.environ.setdefault("PGUSER", "cartracker")
os.environ.setdefault("POSTGRES_PASSWORD", "cartracker")

import psycopg2  # noqa: E402
import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "html"

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)


def _capture(name: str) -> bytes:
    """One of the real cars.com captures, decompressed."""
    return gzip.decompress((_FIXTURES / name).read_bytes())


# ---------------------------------------------------------------------------
# The loopback origin.
# ---------------------------------------------------------------------------
# Path -> (status, body). Strict: anything else is a 404, so a request the
# suite did not intend fails loudly rather than being quietly served.
def _routes() -> dict[str, tuple[int, bytes]]:
    return {
        "/detail/ok": (200, _capture("real_detail_crv.html.gz")),
        "/detail/other": (200, _capture("real_detail_2.html.gz")),
        # Cloudflare serves the interstitial *as* the response body on a block,
        # which is why the 403 case carries real challenge HTML rather than an
        # empty body: the artifact written to MinIO is what processing later
        # has to recognise.
        "/detail/blocked": (403, _capture("challenge_just_a_moment.html.gz")),
        # A 200 that is really a challenge -- the shape Plan 128 found being
        # swallowed as a successful scrape.
        "/detail/challenge": (200, _capture("challenge_just_a_moment.html.gz")),
    }


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler's spelling
        status, body = self.server.routes.get(self.path, (404, b"not found"))
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        """Silence per-request logging; pytest output is the signal here."""


@pytest.fixture(scope="session", autouse=True)
def _no_solver(session_mocker):
    """Take the plain curl_cffi path, the way the CI step configures it.

    This is configuration, not a mock: ``FLARESOLVERR_URL`` is a production
    environment variable and empty is a value production itself uses. It is
    applied to the constant rather than to ``os.environ`` because
    ``cf_session`` reads the variable once, at import, into a module global
    that ``scrape_detail`` then imports into its own namespace — so by the time
    any fixture runs, an env change would be too late to matter and too early
    to be safe.

    Without it the fetch still works and still reaches the same code: the
    solver branch is entered, the connection to ``flaresolverr:8191`` fails,
    and the ``except`` falls through to the identical plain path. It would cost
    a connection timeout per fetch and make the suite's behaviour depend on
    whether something happened to be listening on that name.
    """
    session_mocker.patch(
        "scraper.processors.scrape_detail.FLARESOLVERR_URL", ""
    )


@pytest.fixture(scope="session")
def origin():
    """A real HTTP server on loopback, serving the recorded captures.

    Session-scoped: it holds no per-test state, and standing one up per test
    would add a bind/teardown to every case for nothing.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.routes = _routes()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture(scope="session")
def scraper_client():
    """The scraper app, entered as a context so its lifespan really runs.

    ``TestClient`` as a context manager runs the lifespan hook, which calls
    ``get_pool()`` — a real ``asyncpg`` pool against the CI Postgres. That is
    the other half of what this suite adds: the pool has been mocked in every
    scraper test that ever ran, so ``scraper/db.py`` reaching a database was
    asserted by nothing.
    """
    os.environ.setdefault("DATABASE_URL", _DATABASE_URL)
    from scraper.app import app

    with TestClient(app, raise_server_exceptions=True) as client:
        yield client


@pytest.fixture()
def verify_cur():
    """Autocommit cursor for reading what a request committed.

    The request path commits through its own connection, so a transaction this
    fixture held open would not see the row. Each test cleans up the ids it
    created rather than relying on a rollback that cannot reach them.
    """
    from urllib.parse import urlparse

    parsed = urlparse(_DATABASE_URL)
    conn = psycopg2.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        dbname=(parsed.path.lstrip("/") or "cartracker"),
        user=parsed.username or "cartracker",
        password=parsed.password or "cartracker",
        cursor_factory=RealDictCursor,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        conn.close()
