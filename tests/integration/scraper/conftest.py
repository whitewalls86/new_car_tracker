"""Layer 4 fixtures for `scraper`: a loopback origin, and nothing mocked.

`scraper`'s dependency is an HTTP origin it does not own, so the Layer 4
question — "endpoint behaviour against a real dependency" — is answered the way
`container_health` answers it: a real HTTP server on loopback serving responses
recorded from the real one, with the whole path left intact.

**Nothing is mocked, and that is the entire point of this suite.** Until Plan
162 Stage 8, neither fetch path had ever run unmocked in any layer:
`tests/scraper/conftest.py` patches `shared.db.get_conn` and
`shared.minio.write_html` for every test in that directory, autouse, so the
half of the request path that writes — MinIO object, `ops.artifacts_queue` row,
`staging.artifacts_queue_events` twin, and the blocked-cooldown pair on a 403 —
was reached by nothing. Here both paths run end to end: `TestClient` → router →
processor → `curl_cffi` → HTTP → zstd → real MinIO → real Postgres.

**The two paths reach the origin differently, and that asymmetry is why the
SRP half needed a production change.** `scrape_detail` takes `payload.url` from
its caller, so a request can point it anywhere. `scrape_results` *composes* its
URL from `BASE_URL`, so Stage 8 made that origin an environment variable —
`SCRAPER_RESULTS_BASE_URL`, unset in production.

**The pages served are the real captures in `tests/fixtures/html/`**, recorded
from cars.com. `real_results_page.html.gz` was pulled from production MinIO for
this suite: a full 24-listing page, and specifically **page 1 of 19**.

That last detail is load-bearing and cost a diagnosis to find. `_fetch_page`
reads `result_page_number` out of the page's own paging metadata and sets
`_break_no_save` when it disagrees with the page it asked for — cars.com
clamping a request to a different page means duplicate territory, so the
artifact is discarded. The first capture taken for this suite happened to be
page 7, so `scrape_results` fetched it, correctly refused to save it, and
returned zero artifacts. Nothing was wrong with the code. **A replacement
capture has to be page 1**, or every test here fails for a reason that looks
nothing like its cause.

**What this suite cannot see**, stated so it is a decision: cars.com changing
its markup, or Cloudflare changing what it serves. `container_health` answers
that with a verifier script against the real proxy; there is no equivalent here
and there should not be one — a CI job that fetched cars.com would be scraping
from a datacentre IP, which is what the cooldown machinery exists to survive.
The parser suites own markup drift instead, against these same captures.

Plan 162 Stage 8.
"""
import gzip
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

# ---------------------------------------------------------------------------
# Env, before any scraper import reaches module scope.
#
# ``setdefault`` only, and never a bare assignment. A conftest is imported at
# collection, so an assignment here reaches every test in the session, not only
# this directory's -- setting ``FLARESOLVERR_URL`` empty at import turned twelve
# ``tests/scraper/test_metrics.py`` tests red by short-circuiting the solver
# branch they exist to count. That is the harness deciding another test's
# outcome, in the suite whose whole subject is that nothing is faked. The
# solver is disabled by the fixture below instead, scoped and reversed.
#
# The origin's port is fixed rather than ephemeral because
# ``scrape_results.BASE_URL`` is resolved at import: the URL has to be known
# before the first scraper import, which is earlier than any fixture runs.
# ---------------------------------------------------------------------------
ORIGIN_PORT = int(os.environ.get("SCRAPER_TEST_ORIGIN_PORT", "8731"))
ORIGIN = f"http://127.0.0.1:{ORIGIN_PORT}"

# 127.0.0.1 is on the unpaced list in scrape_results, so pointing the SRP
# origin here also switches off the 13-35s human-cadence sleep before page 1.
os.environ.setdefault("SCRAPER_RESULTS_BASE_URL", f"{ORIGIN}/results/")
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
# Keyed by path with the query string discarded: the SRP request carries the
# whole search (makes, models, zip, page) as query parameters, and a random ZIP
# is picked per job, so matching on the full target would match nothing. The
# map is otherwise strict -- an unrecognised path is a 404, so a request this
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
        "/results/": (200, _capture("real_results_page.html.gz")),
    }


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler's spelling
        path = urlsplit(self.path).path
        status, body = self.server.routes.get(path, (404, b"not found"))
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        """Silence per-request logging; pytest output is the signal here."""


class _Origin(ThreadingHTTPServer):
    # The port is fixed, so a run that follows a crashed one would otherwise
    # fail to bind for the length of TIME_WAIT.
    allow_reuse_address = True


@pytest.fixture(scope="session", autouse=True)
def origin():
    """A real HTTP server on loopback, serving the recorded captures.

    Autouse and session-scoped: `scrape_results` reaches it without any test
    naming it, because the URL it fetches is composed from `BASE_URL` rather
    than passed in, so a fixture the SRP tests forgot to request would leave
    them fetching a closed port.
    """
    try:
        server = _Origin(("127.0.0.1", ORIGIN_PORT), _Handler)
    except OSError as exc:  # pragma: no cover - environment, not logic
        pytest.fail(
            f"could not bind the loopback origin on port {ORIGIN_PORT}: {exc}. "
            "Set SCRAPER_TEST_ORIGIN_PORT to a free port; it has to be fixed "
            "because scrape_results resolves BASE_URL at import."
        )
    server.routes = _routes()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield ORIGIN
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture(scope="session", autouse=True)
def _no_solver(session_mocker):
    """Take the plain curl_cffi path, the way the CI step configures it.

    This is configuration, not a mock: ``FLARESOLVERR_URL`` is a production
    environment variable and empty is a value production itself uses. It is
    applied to the constants rather than to ``os.environ`` because
    ``cf_session`` reads the variable once, at import, into a module global —
    so by the time any fixture runs, an env change would be too late to matter
    and too early to be safe.

    **Both names, because the two fetch paths read different copies.**
    ``scrape_detail`` imported the value into its own namespace, so patching
    only ``cf_session`` would leave the detail path entering the solver branch;
    ``scrape_results`` calls ``get_cf_credentials`` directly, so patching only
    ``scrape_detail`` would leave the SRP path raising inside ``_do_fetch``,
    where the handler answers a connection error with ``time.sleep(10)`` and a
    retry before giving up.
    """
    session_mocker.patch("scraper.processors.cf_session.FLARESOLVERR_URL", "")
    session_mocker.patch("scraper.processors.scrape_detail.FLARESOLVERR_URL", "")


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
