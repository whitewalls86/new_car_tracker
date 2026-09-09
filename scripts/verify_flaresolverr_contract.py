"""Ask the real FlareSolverr whether its solve envelope still has our fields.

Plan 162 Stage AB / CAR-106.

``scraper/processors/cf_session.get_cf_credentials`` reads six things out of a
FlareSolverr ``/v1`` response and every one of them is a key this repository
types out: ``status``, ``solution``, ``solution.userAgent``,
``solution.response``, ``solution.status``, and ``solution.cookies`` as a list
of ``{name, value}``. FlareSolverr owns all six. Two of them are read with
``[...]`` and raise if they move; **the other four are read with ``.get`` and
default**, so a renamed key there is silent: ``html`` becomes empty, the status
falls back to 200, and the cookie jar comes back empty while the credentials
are cached as though the bootstrap worked. That is the 2026-08-14 failure mode
with a different cause.

**Unlike the cars.com corpus beside it, this one replays against the real
thing, in CI, for free** -- and the reason is worth stating because it looked
like it would not. The obvious blocker is that exercising FlareSolverr means
pointing it at cars.com, which CI cannot reach. It does not: FlareSolverr will
solve *any* URL, and a page with no challenge on it produces the same envelope
as one with. So this stands up the pinned image, serves it a four-line page
from a thread in this process, and asserts the response still carries every
field the scraper reads.

    python scripts/verify_flaresolverr_contract.py

**The page sets a cookie on purpose.** A page that sets none comes back with
``solution.cookies == []``, which satisfies "is a list" and proves nothing
about the ``{name, value}`` shape the scraper's dict comprehension depends on.
One ``Set-Cookie`` is the difference between checking the container and
checking what is in it.

**What this deliberately does not assert.** Not that a *challenge* is solved --
there is nothing here to challenge it, and a test that needed one would need
cars.com and would therefore need production. Only that the envelope a
successful solve returns still has the shape ``get_cf_credentials`` reads out
of it. Same limit as ``verify_container_health_docker_contract.py``: shape, not
meaning.
"""
from __future__ import annotations

import http.server
import json
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE = REPO_ROOT / "docker-compose.yml"
CF_SESSION = REPO_ROOT / "scraper" / "processors" / "cf_session.py"

CONTAINER = "cartracker-flaresolverr-contract"
SOLVER_PORT = 18191
PAGE_PORT = 18080

# The page is trivial on purpose -- what is under test is the envelope, not
# the parsing. The cookie is not: see the module docstring.
PAGE = b"<html><head><title>flaresolverr contract</title></head><body>ok</body></html>"
COOKIE = "cartracker_contract=1; Path=/"

STARTUP_TIMEOUT = 180.0
SOLVE_TIMEOUT_MS = 60000


def _image() -> str:
    """The tag production runs, read from Compose rather than pinned again here.

    ``docker-compose.yml`` writes it as ``${FLARESOLVERR_IMAGE:-<default>}``.
    The default is what production runs when the variable is unset, which it
    is, so the default is the answer -- and reading it here rather than
    restating it is the same rule this script exists to enforce, applied to
    itself.
    """
    text = COMPOSE.read_text(encoding="utf-8")
    match = re.search(r"\$\{FLARESOLVERR_IMAGE:-([^}]+)\}", text)
    if not match:
        raise SystemExit(f"no FLARESOLVERR_IMAGE default found in {COMPOSE}")
    return match.group(1)


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Set-Cookie", COOKIE)
        self.send_header("Content-Length", str(len(PAGE)))
        self.end_headers()
        self.wfile.write(PAGE)

    def log_message(self, *args):  # noqa: A003 - silence the default access log
        pass


def _host_ip() -> str:
    """An address the container can reach this process on.

    ``localhost`` inside the container is the container. ``host.docker.internal``
    resolves on Docker Desktop and not on the Linux runner, so neither is
    portable; the address of the interface that would carry outbound traffic is.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    finally:
        sock.close()


def _solve(url: str) -> dict:
    payload = json.dumps(
        {"cmd": "request.get", "url": url, "maxTimeout": SOLVE_TIMEOUT_MS}
    ).encode()
    request = urllib.request.Request(
        f"http://localhost:{SOLVER_PORT}/v1",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=SOLVE_TIMEOUT_MS / 1000 + 30) as response:
        return json.load(response)


def _await_solve(url: str) -> dict:
    """Retry until the solver answers or the deadline passes.

    FlareSolverr starts a headless Chrome before it will serve ``/v1``, so the
    first several attempts are refused connections rather than errors. The
    deadline is generous because it is only ever paid in full when something
    is actually wrong.
    """
    deadline = time.time() + STARTUP_TIMEOUT
    last: Exception | None = None
    while time.time() < deadline:
        try:
            return _solve(url)
        except (urllib.error.URLError, OSError, TimeoutError) as error:
            last = error
            time.sleep(3)
    raise SystemExit(f"FlareSolverr never answered within {STARTUP_TIMEOUT:.0f}s: {last}")


def _fields_the_scraper_reads() -> set[str]:
    """The solution keys ``get_cf_credentials`` actually reads, from its source.

    Derived rather than listed, so a seventh field added to the scraper is
    checked here without anybody remembering to add it. The two access shapes
    are ``solution["x"]`` and ``solution.get("x"...)``.
    """
    source = CF_SESSION.read_text(encoding="utf-8")
    return set(re.findall(r'solution(?:\[|\.get\()"([a-zA-Z]+)"', source))


def _check(envelope: dict) -> list[str]:
    problems = []
    if envelope.get("status") != "ok":
        problems.append(
            f"top-level status is {envelope.get('status')!r}, not 'ok' -- "
            f"message: {envelope.get('message')!r}"
        )
    solution = envelope.get("solution")
    if not isinstance(solution, dict):
        problems.append("no `solution` object in the response")
        return problems

    expected = _fields_the_scraper_reads()
    if not expected:
        problems.append(
            f"read no solution fields out of {CF_SESSION} -- the access shape "
            "changed and this checker is now asserting nothing"
        )
    missing = sorted(expected - set(solution))
    if missing:
        problems.append(
            f"the scraper reads {missing} out of `solution` and FlareSolverr "
            f"no longer sends them. Present: {sorted(solution)}"
        )

    if not isinstance(solution.get("cookies"), list):
        problems.append("`solution.cookies` is not a list")
    else:
        cookies = solution["cookies"]
        if not cookies:
            problems.append(
                "`solution.cookies` is empty, so the {name, value} shape the "
                "scraper's dict comprehension needs was not exercised -- the "
                "page is supposed to set one"
            )
        else:
            for cookie in cookies:
                if not {"name", "value"} <= set(cookie):
                    problems.append(f"a cookie has no name/value pair: {sorted(cookie)}")
                    break
    if not solution.get("userAgent"):
        problems.append(
            "`solution.userAgent` is empty, and the impersonation target "
            "derives from it"
        )
    if not isinstance(solution.get("status"), int):
        problems.append(f"`solution.status` is {solution.get('status')!r}, not an int")
    return problems


def main() -> int:
    image = _image()
    print(f"flaresolverr: {image}")

    server = http.server.ThreadingHTTPServer(("0.0.0.0", PAGE_PORT), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://{_host_ip()}:{PAGE_PORT}/"
    print(f"serving a challenge-free page at {url}")

    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, timeout=60)
    started = subprocess.run(
        ["docker", "run", "-d", "--name", CONTAINER,
         "-p", f"{SOLVER_PORT}:8191", "-e", "LOG_LEVEL=warning", image],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=300,
    )
    if started.returncode != 0:
        server.shutdown()
        raise SystemExit(f"could not start {image}: {started.stderr.strip()}")

    try:
        envelope = _await_solve(url)
        problems = _check(envelope)
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, timeout=60)
        server.shutdown()

    if problems:
        print("\nFlareSolverr's response no longer matches what the scraper reads:")
        for problem in problems:
            print(f"  - {problem}")
        print(
            "\nget_cf_credentials reads four of these with `.get` and a default, "
            "so a rename here caches empty credentials and reports success."
        )
        return 1

    solution = envelope["solution"]
    print(
        "ok: status={status!r}, solution.status={inner!r}, "
        "{cookies} cookie(s), userAgent {ua} chars, response {size} bytes".format(
            status=envelope["status"],
            inner=solution["status"],
            cookies=len(solution["cookies"]),
            ua=len(solution["userAgent"]),
            size=len(solution.get("response") or ""),
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
