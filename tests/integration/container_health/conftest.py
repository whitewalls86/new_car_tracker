"""Layer 4 fixtures for container_health: a strict fake of the socket proxy.

`container_health` has no database. Its dependency is the Docker API, reached
over real HTTP by `DockerApi`, so the Layer 4 question "endpoint behaviour
against a real dependency" has to be answered differently here than it is for
the services that own tables.

**Nothing is mocked.** The fake is a real HTTP server on loopback serving
responses recorded from a real `tecnativa/docker-socket-proxy` in front of a
real daemon, so the whole path runs: TestClient -> router -> handler ->
`DockerApi` -> `urllib` -> HTTP -> JSON parsing. That is what makes this a
Layer 4 suite rather than a unit test with a `TestClient` in it, and it is why
the `v1.44` path segment, the `filters` JSON encoding and the two-step inspect
are exercised rather than assumed.

**Why a fake and not the real proxy.** `collector.health_values` raises
`NoContainersFound` on an empty fleet by design, so a real proxy pointed at a
CI daemon would 500 rather than answer -- the suite would need real containers
carrying `com.docker.compose.project=cartracker` before it could assert a
single status code. The recording costs nothing per run and the real proxy is
still exercised, once, by
`scripts/verify_container_health_docker_contract.py`. One corpus, two
consumers, neither importing the other.

**The fake is strict, and that is the whole design.** It serves exactly the
recorded exchanges and 404s everything else, and `docker_api_contract` asserts
both directions at the end of the session: a request the corpus does not hold
fails, and a recorded exchange nothing asked for fails too. So the day
`DockerApi` changes a URL, this suite fails rather than quietly exercising a
path production no longer takes.

What it cannot see is the corpus drifting from the real API -- no test that
does not talk to a daemon can. That is the verifier script's job, named above,
and the split is deliberate rather than an omission.

Plan 162 Stage H.
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CORPUS_PATH = (
    _REPO_ROOT / "tests" / "fixtures" / "container_health" / "docker_api_contract.json"
)
_CORPUS = json.loads(_CORPUS_PATH.read_text(encoding="utf-8"))


def _key(path: str, params: dict | None) -> tuple:
    """One exchange's identity: the path plus its query, order-independent.

    The query has to be part of the key. Two recorded exchanges are both
    ``/containers/json`` and differ only by the project in their ``filters``
    argument -- keying on path alone would serve the `cartracker` fleet to
    `/project-status/cartracker-lakehouse` and the suite would assert a lie.
    """
    return (path, tuple(sorted((params or {}).items())))


_EXCHANGES = {
    _key(exchange["path"], exchange["params"]): exchange["response"]
    for exchange in _CORPUS["exchanges"]
}

# Populated by the handler, asserted at session teardown.
_SERVED: set = set()
_UNKNOWN: list = []
# Every URL as it arrived on the wire, version prefix intact.
_RAW: list = []


class _ProxyHandler(BaseHTTPRequestHandler):
    """Serve the corpus, and refuse -- loudly -- to invent anything else."""

    def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler's spelling)
        _RAW.append(self.path)
        split = urlsplit(self.path)
        path = split.path
        # `DockerApi` prefixes every request with the pinned API version. It is
        # stripped rather than recorded so the corpus stays keyed on the
        # endpoint; the version itself is asserted in test_routes.py, where a
        # silent change to it is a visible failure rather than a 404 here.
        parts = path.split("/", 2)
        if len(parts) == 3 and parts[1].startswith("v"):
            path = "/" + parts[2]
        params = dict(parse_qsl(split.query)) or None

        key = _key(path, params)
        if key not in _EXCHANGES:
            _UNKNOWN.append(self.path)
            self.send_error(404, "not in the recorded corpus")
            return

        _SERVED.add(key)
        body = json.dumps(_EXCHANGES[key]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        """Silence the default stderr access log; failures speak for themselves."""


# Bind before importing the app. `container_health.app` reads DOCKER_API_URL at
# module scope and builds two `DockerApi` instances from it, so an import that
# happens first would point the suite at `docker-socket-proxy:2375` and fail
# for a reason that has nothing to do with the code under test. This is the
# same ordering `tests/integration/dbt_runner/conftest.py` keeps, for the same
# reason, and it is the harness-decides-the-outcome rule applied to ourselves.
_SERVER = ThreadingHTTPServer(("127.0.0.1", 0), _ProxyHandler)
threading.Thread(target=_SERVER.serve_forever, daemon=True).start()

os.environ["DOCKER_API_URL"] = f"http://127.0.0.1:{_SERVER.server_address[1]}"
os.environ.setdefault("COMPOSE_PROJECT", "cartracker")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from container_health.app import app  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def api_client():
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest.fixture(scope="session")
def corpus():
    """The recording itself, so tests assert against what was captured."""
    return _CORPUS


@pytest.fixture
def wire_paths():
    """Every URL the client put on the wire, version prefix intact."""
    return _RAW


@pytest.fixture(scope="session", autouse=True)
def docker_api_contract():
    """Both directions of the fake's contract, asserted once per session.

    An unrecognised request means `DockerApi` asks for something no daemon was
    ever recorded answering. An unserved exchange means the corpus carries a
    request nothing makes any more. The first is a regression, the second is
    rot, and a fake that checked neither would pass forever while the client
    drifted away from it -- which is the paraphrase failure docs/TESTING.md
    names for SQL, wearing a different hat.
    """
    yield
    assert not _UNKNOWN, (
        "container_health asked the Docker API for requests the corpus does "
        "not hold:\n  " + "\n  ".join(_UNKNOWN) + "\n\nRe-record with "
        "`python scripts/verify_container_health_docker_contract.py --record` "
        "if the client legitimately changed."
    )
    unserved = sorted(f"{path} {dict(params)}" for path, params in _EXCHANGES if
                      (path, params) not in _SERVED)
    assert not unserved, (
        "these recorded exchanges were never requested, so the corpus is "
        "describing a client that no longer exists:\n  " + "\n  ".join(unserved)
    )
