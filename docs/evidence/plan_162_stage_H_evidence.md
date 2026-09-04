# Plan 162 Stage H — route coverage and `container_health`'s test home

**Legacy:** Stage 6 · **Issue:** CAR-50 · **Closed:** 2026-09-01

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage H. It carries the summary; the sections below are the detail.

---

#### Five of the twelve were never uncovered

This is the finding, and it is the second instance of one Stage F already
recorded under [the instrument was weaker than its own
docstring](plan_162_stage_F_evidence.md#the-instrument-was-weaker-than-its-own-docstring).

The three `/admin/snapshots/adaptive-refresh/` reads and the two safe-lifecycle
coordination routes had tests going through `TestClient` and asserting status
codes the whole time — 200, 409 and 503 among them, including four exemplary
parametrized cases in `tests/ops/routers/test_coordination.py`. **The rule
could not see them.** `_requested_routes` matched only an `ast.Constant` first
argument, so both of the repository's ordinary ways of writing a request
vanished:

| Written as | Seen before | Where |
|---|---|---|
| `mock_client.get(f"{BASE}/latest")` | nothing | `test_snapshots.py`, 3 routes |
| `mock_client.post(path)` under `parametrize` | nothing | `test_coordination.py`, 2 routes |

So G6's census — "twelve routes reached by no test through any routing table" —
was wrong about five of them, and wrong in the direction that costs work: it
would have had someone rewrite five sound tests to satisfy a reader, leaving
the next f-string just as invisible.

**The repair widened how the argument is read, not what counts as a request.**
`_resolve_path` now resolves a module-level string constant, `+` concatenation,
an f-string whose parts resolve, and a `parametrize`-injected argument. It
still requires an HTTP-verb call, and it still yields nothing for an expression
it cannot resolve rather than guessing — because "named somewhere in `tests/`"
is the weak reading `docs/TESTING.md` rejects by name, and a reader that
degraded to it would pass 83 of 87 routes on the strength of a mention. `ops`
went from 54 to 61 request literals against 54 routes with no test added.

The three that were real gaps stayed failing until they got tests:
`GET /coordination/status` and the two `/maintenance` routes were exercised
only by calling their helpers. `/coordination/status` is the one
`scripts/host_maintenance.py` polls before it will proceed, so a rename would
have stranded the host maintenance workflow while this suite stayed green.

#### `container_health` had nowhere to put a `TestClient`, which is why G6 and G9 were one stage

Four routes were a genuine gap and could not have been closed separately. A
test is attributed to a service by its directory, so
`tests/test_container_health_app.py` could not have counted for
`container_health` even after growing a `TestClient` while it sat at the top
level. Moving it was not filing tidiness; it was the precondition.

Both files moved to `tests/container_health/` and pass unchanged (39 tests).

#### The Layer 4 suite has no database, and the substitute is a recording

`container_health`'s dependency is the Docker API over real HTTP. Standing up
the real `docker-socket-proxy` in CI was considered and rejected on a specific
fact: `collector.health_values` raises `NoContainersFound` on an empty fleet by
design, so a real proxy against a CI daemon returns 500 rather than an answer.
The suite would have needed real containers labelled
`com.docker.compose.project=cartracker` before it could assert one status code.

`tests/integration/container_health/` therefore serves a corpus recorded from a
real proxy through a strict fake on loopback. **Nothing is mocked** — the path
runs `TestClient` → router → handler → `DockerApi` → `urllib` → HTTP → parsing,
so the `v1.44` prefix, the `filters` JSON encoding and the two-step inspect are
exercised rather than assumed. The fake 404s anything not recorded, and the
session asserts both directions: an unrecorded request fails, and a recorded
exchange nothing asked for fails too.

The corpus was recorded against a daemon that also had an unrelated
`de-podcast` project running, which is why the project-label filter has
something real to exclude rather than a fixture built to agree with it.

**The import-time hazard was handled deliberately.** `container_health.app`
reads `DOCKER_API_URL` at module scope and builds two `DockerApi` instances
from it, so an import that happened first would point the suite at
`docker-socket-proxy:2375` and fail for a reason unrelated to the code. The
fake binds its port before the app import, in conftest module scope — the same
ordering `tests/integration/dbt_runner/conftest.py` keeps, and the
harness-decides-the-outcome rule applied to ourselves.

#### What the recording cannot see, and who owns that

A fake is a recording, so nothing in the Layer 4 suite can notice the day
Docker or the proxy changes a response shape. That is stated rather than
implied, and it has an owner:
`scripts/verify_container_health_docker_contract.py` stands up the real proxy
against a throwaway labelled fleet and asserts the live responses still carry
every field `collector.py` reads. It runs in its own
`container_health Docker contract (real proxy)` job.

This is the split Plan 141 already uses for Promtail — one corpus, two
consumers, neither importing the other, so what runs where is a CI-wiring
question rather than a code change. The script's `--record` mode is what
refreshes the corpus, so the fixtures stay re-derivable instead of hand-edited.

Both failure directions were exercised rather than assumed: a bogus required
field makes the shape check fail, and the request-set comparison fails when the
client asks for something the corpus does not hold.

#### What was deliberately not done

- **No Windows runner, and no ruff rule.** Both belong to Stage J, which this
  stage filed rather than absorbed.
- **The `de-podcast` containers on the recording machine were not cleaned up or
  hidden.** They are somebody else's project and their presence is the point.
- **Two plan documents still name `tests/test_container_health_app.py`** at its
  old path — Plan 136 §3a and Plan 161. They are dated records of what was true
  when written, and the gap list's own convention is that history lives in the
  plan documents.
- **The `enough` table's `container_health` row was updated, not its
  neighbours.** The other counts are a dated measurement and re-deriving them
  was not this stage's work.

#### What CI said, and what only CI could have said

Merged from run `33521767976` on `4b88d4b`, all eleven jobs green
(`Documentation tests` skipped by design on a changeset that is not docs-only).

| | |
|---|---|
| Unit suite | 3355 passed, 1 skipped, 479 deselected, 48.7s |
| Coverage | **78%** against a floor of 75 |
| `container_health` Layer 4 | **8 passed in 0.09s** |
| `container_health` Docker contract (real proxy) | **green in 16s**, verify step ~4s |

The Layer 4 suite passed in CI on its first attempt and needed no change. The
loopback fake, the background thread and the conftest import ordering behave the
same on `ubuntu-latest` as on Windows, which was the part with no prior evidence
either way.

**The real-proxy job earned its place on its first run by failing.** It died in
nine seconds on `ModuleNotFoundError: No module named 'prometheus_client'`: the
job ran `setup-python` and installed nothing. The cause is a consequence of a
decision worth keeping — the verifier imports the production label constants
from `container_health.collector` rather than restating them, because a copy of
`com.docker.compose.project` in a checker is the paraphrase failure this contract
names for SQL — and that import chain reaches `prometheus_client`. Repaired by
installing `container_health/requirements.txt`, so the pin has one source.

The repair was verified against a **cold venv**, not the development environment
that already had the package, which is the only reason the fix was known to work
before the second run rather than guessed at.

**The corpus proved portable, which was the open risk.** It was recorded on a
Windows machine against Docker 29.1.3 (api 1.52) and verified against the
runner's own daemon — a different machine, a different daemon, the same seven
exchanges. That is the property the whole two-part design rests on, and until
this run it was an assumption.

#### Three times the same mistake: citing a precedent and copying half of it

Worth recording because the shape repeated inside one stage, and none of the
three was caught by reading:

1. The contract job was modelled on `promtail-config` and copied without its
   `pip install` step. CI caught it.
2. The verifier was modelled on `verify_promtail_contract.py` and shipped
   without the test file that sits beside it. 171 uncovered statements, caught
   by reading the coverage report rather than by any rule.
3. The first pass at those tests stopped at 45% on the reasoning that the rest
   "needs a daemon". Most of it did not: `main`'s exit codes, `_capture`'s
   one-stats-read-per-capped-container rule and `_start_fleet`'s argument
   construction are all decision logic over data, and every one of their failure
   modes is silent. A dropped `--memory` does not fail anything; it removes
   `memory_capped`'s only input and the corpus quietly stops carrying a stats
   exchange for ever.

Coverage after the third correction: the verifier 45% → **99%**, the two
remaining lines being a one-line `subprocess` wrapper and the `__main__` guard.

**The sister script was cleaned up in the same pass**, unscoped and deliberately
so: `verify_promtail_contract.py` sat at 48% two files away, and the argument
that the coverage was cheap applies identically. 48% → **69%**. What it gained is
not more verdict testing but the replay *setup* — the image read from compose
(so the checker cannot agree with a version production stopped running), the
`docker: {}` envelope strip, the `service` label `_parse_entries` filters on,
and `main`'s exit codes. One test written for it asserted the wrong thing and
the code was right: absence on every attempt is a real drop, and inconclusive
means lost and then recovered.

Both scripts stop at the same line. `_run`'s `Popen` and threading needs a
daemon, and faking it would assert the shape of the mocks rather than the
behaviour of Promtail or Docker — rule 3 of what must never be mocked. That half
is CI's in both cases, which is the whole argument of this stage stated twice.
