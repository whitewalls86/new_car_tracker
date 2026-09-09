"""Words this repository types out that belong to systems it does not build.

Plan 162 Stage AB / CAR-106. The census is
``tests/external_vocabulary_census.py``; this module is the half of it that
runs in the main venv.

**What this class of defect is.** Stage W closed it for the database: a
``CHECK`` constraint is the owner, so a rename makes the *query* wrong and
Postgres says so. Stages Z and AA close it for our own services: a generated
``contracts/<pkg>.json`` is the owner, so a caller fabricating a response its
callee cannot produce fails a diff. Neither instrument can reach a system this
repository does not build. There is no ``contracts/cars_com.json`` and there
never will be, so a test asserting cars.com returns 403 is asserting something
nothing in this tree can contradict.

**The remedy is the corpus/replay split already running twice here** --
``scripts/verify_promtail_contract.py`` and
``scripts/verify_container_health_docker_contract.py``, *"one corpus, two
consumers, neither importing the other"*. The fast tests read a recording; a
CI step asks the real thing whether the recording is still true.

**And the limit travels with it.** Replaying a corpus proves the *shape* still
holds. It does not prove the meaning has not changed: cars.com can keep
returning 403 and start meaning something else by it. ``docs/TESTING.md``
already says this about ``container_health`` and it is true of every entry in
the census.
"""
from __future__ import annotations

import ast
import gzip
import json
import re
from pathlib import Path

import pytest

from tests.external_vocabulary_census import CENSUS, OUT_OF_SCOPE, REPLAYED

REPO_ROOT = Path(__file__).resolve().parent.parent
CORPUS = REPO_ROOT / "tests" / "fixtures" / "external" / "cars_com_responses.json"
CHALLENGE_FIXTURE = (
    REPO_ROOT / "tests" / "fixtures" / "html" / "challenge_just_a_moment.html.gz"
)
CF_SESSION = REPO_ROOT / "scraper" / "processors" / "cf_session.py"

# The three modules that fabricate a cars.com response, and the seam they do it
# at. Named rather than discovered: a fourth one is a decision to add a row
# here, and a rule whose scope silently follows the tree is a rule that can be
# narrowed by moving a file.
CARS_COM_TEST_MODULES = (
    "tests/scraper/conftest.py",
    "tests/scraper/processors/test_scrape_detail.py",
    "tests/scraper/processors/test_scrape_results.py",
)


def _declared(what: str) -> tuple:
    rows = [row for row in CENSUS if row["what"] == what]
    assert len(rows) == 1, f"the census declares {len(rows)} rows for {what!r}, expected 1"
    return rows[0]["members"]


def _corpus() -> dict:
    assert CORPUS.exists(), (
        f"{CORPUS} is missing. Record it with "
        "`python scripts/record_cars_com_status_corpus.py --record` from "
        "somewhere that can reach production's Loki and Prometheus. It is not "
        "reachable from CI, which is why the file is committed."
    )
    return json.loads(CORPUS.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# cars.com -- the statuses a test may fabricate
# ---------------------------------------------------------------------------


def _fabricated_cars_com_statuses() -> list[tuple[str, int, int]]:
    """Every ``<mock>.status_code = <int>`` in the cars.com test modules.

    Returns (path, lineno, status). A non-literal assignment -- the
    ``_mock_http(self, mocker, status=200)`` helpers -- is resolved through its
    default and its call sites instead, below.
    """
    found = []
    for rel in CARS_COM_TEST_MODULES:
        path = REPO_ROOT / rel
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if not isinstance(target, ast.Attribute) or target.attr != "status_code":
                    continue
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, int):
                    found.append((rel, node.lineno, node.value.value))
    return found


def _parametrised_cars_com_statuses() -> list[tuple[str, int, int]]:
    """The statuses reaching the ``_mock_http(..., status=...)`` helpers.

    Three assignments in these modules are ``mock_resp.status_code = status``,
    a parameter. Reading only literals would take them out of scope, so the
    helper's default and every keyword argument at its call sites are resolved
    here. This is the shape
    ``test_the_sql_in_python_rule_sees_every_shape_that_can_hold_a_statement``
    exists for one rule over: a reader that sees one syntax and calls the
    absence of the others a clean result.
    """
    found = []
    for rel in CARS_COM_TEST_MODULES:
        path = REPO_ROOT / rel
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_mock_http":
                for arg, default in zip(
                    node.args.args[-len(node.args.defaults):] if node.args.defaults else [],
                    node.args.defaults,
                ):
                    if arg.arg == "status" and isinstance(default, ast.Constant):
                        found.append((rel, node.lineno, default.value))
            if isinstance(node, ast.Call):
                func = node.func
                name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
                if name != "_mock_http":
                    continue
                for keyword in node.keywords:
                    if keyword.arg == "status" and isinstance(keyword.value, ast.Constant):
                        found.append((rel, node.lineno, keyword.value.value))
    return found


def test_no_test_fabricates_a_cars_com_status_production_has_never_seen():
    """A fabricated cars.com status must be one the real site has returned.

    This is Stage AA's rule for a callee with no contract. The corpus stands in
    for the artifact AA generates, and it is recorded from production rather
    than written here, because production is the only consumer of cars.com this
    repository has.
    """
    observed = set(_corpus()["statuses_observed"])
    fabricated = _fabricated_cars_com_statuses() + _parametrised_cars_com_statuses()

    assert fabricated, (
        "no fabricated cars.com status found in "
        f"{CARS_COM_TEST_MODULES}. The seam moved and this reader did not "
        "follow it, which makes this rule vacuous rather than satisfied."
    )

    invented = sorted(
        {(rel, lineno, status) for rel, lineno, status in fabricated if status not in observed}
    )
    assert not invented, (
        "these tests fabricate a cars.com status production has never "
        "observed:\n  "
        + "\n  ".join(f"{rel}:{lineno} -> {status}" for rel, lineno, status in invented)
        + f"\n\nObserved: {sorted(observed)} (see {CORPUS.name}).\n"
        "Either the status is real and the corpus is stale -- re-record it "
        "with scripts/record_cars_com_status_corpus.py --record -- or the "
        "test is asserting against a response cars.com does not send."
    )


def test_every_observed_cars_com_status_is_handled():
    """Every status production has returned classifies to a decided outcome.

    **The rule above is the negative direction and this is the positive one.**
    That one says a test may not invent a status; this says the *code* may not
    ignore one. They are different failures: the suite fabricated 200 and 403
    and nothing else, while cars.com was also returning 302, 500, 502, 503 and
    504 — 3,103 times in 91 days — and every one of them fell through an
    ``else`` into the arm meant for success.

    ``UNKNOWN`` is the catchall and it is deliberately **not** an acceptable
    answer for a status the corpus contains. A status nobody has reasoned
    about should be handled conservatively when it turns up in production;
    once it has turned up often enough to be in a recording, somebody owes it
    a decision. That is what this assertion collects.
    """
    from scraper.fetch_outcomes import FetchOutcome, classify

    observed = _corpus()["statuses_observed"]
    undecided = sorted(
        status for status in observed if classify(status) is FetchOutcome.UNKNOWN
    )
    assert not undecided, (
        f"cars.com has returned {undecided} in production and "
        "scraper/fetch_outcomes.py has no opinion about them, so they take the "
        "UNKNOWN catchall.\n\n"
        "The catchall is for statuses nobody has seen yet. A status in the "
        "recorded corpus has been seen, so decide it: add it to _HANDLING with "
        "the outcome that says what the scraper should do — back off or not, "
        "and enqueue for parsing or not. Getting this wrong is not loud: "
        "processing's listing_state default is 'active'."
    )


def test_the_unknown_catchall_is_reachable():
    """And the catchall still exists, for the statuses that are not in it yet.

    The rule above would also pass if ``classify`` returned ``OK`` for
    everything, which is the cheapest possible way to satisfy it and the worst
    possible behaviour. This is the other half: a status cars.com has never
    sent must still land somewhere conservative.
    """
    from scraper.fetch_outcomes import (
        FetchOutcome,
        classify,
        handled_statuses,
        should_back_off,
        should_enqueue_for_parsing,
    )

    unseen = 418
    assert unseen not in handled_statuses(), (
        "418 was chosen as a status nothing decides about and something now "
        "decides about it. Pick another."
    )
    assert classify(unseen) is FetchOutcome.UNKNOWN
    assert classify(None) is FetchOutcome.UNKNOWN, (
        "a fetch that raised has no status and must classify UNKNOWN, not "
        "fall through to the success arm"
    )
    assert should_back_off(FetchOutcome.UNKNOWN), (
        "an unrecognised status must slow the scraper down, not speed it up"
    )
    assert not should_enqueue_for_parsing(FetchOutcome.UNKNOWN), (
        "an unrecognised body must not reach a parser whose listing_state "
        "default is 'active'"
    )


def test_the_cars_com_corpus_is_not_empty():
    """The corpus cannot pass the rule above by holding nothing.

    ``test_the_check_constraint_corpus_is_not_empty`` and
    ``test_the_route_code_corpus_is_not_empty`` exist for the same reason: a
    reader that silently matched nothing would turn every rule keyed on it
    green. The floor is 2 because 200 and 403 are the two statuses the suite
    actually fabricates, so anything less cannot even cover what is written
    today.
    """
    corpus = _corpus()
    observed = corpus["statuses_observed"]
    assert len(observed) >= 2, (
        f"{CORPUS} claims cars.com has returned only {observed}. That is fewer "
        "than the suite fabricates, so the recording failed rather than the "
        "site changing. Re-record it."
    )
    assert corpus["observations"], f"{CORPUS} records no observations"
    for row in corpus["observations"]:
        assert row["instrument"] in {"loki", "prometheus"}, row
        assert row["count"] > 0, row


def test_the_corpus_records_which_instrument_saw_each_status():
    """A count with no instrument is a number nobody can re-take.

    Loki counts log lines and one fetch logs twice; Prometheus counts fetches.
    They disagree by construction, and the corpus deliberately does not
    reconcile them -- so each observation has to say which one produced it, or
    the file is asserting a total that was never measured.
    """
    for row in _corpus()["observations"]:
        assert row.get("instrument"), f"observation with no instrument: {row}"
        assert row.get("window"), f"observation with no window: {row}"


# ---------------------------------------------------------------------------
# curl_cffi -- the impersonation targets
# ---------------------------------------------------------------------------


def _cf_session_targets() -> list[str]:
    """The targets ``_CHROME_CFFI_TARGETS`` actually holds, read from source."""
    tree = ast.parse(CF_SESSION.read_text(encoding="utf-8"))
    # `_CHROME_CFFI_TARGETS: List[Tuple[int, str]] = sorted([...])` is an
    # AnnAssign wrapping a call wrapping the list, so the list is reached by
    # walking the value rather than indexing into it.
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id != "_CHROME_CFFI_TARGETS":
                continue
            return [
                element.elts[1].value
                for call in ast.walk(node.value)
                if isinstance(call, ast.List)
                for element in call.elts
                if isinstance(element, ast.Tuple) and len(element.elts) == 2
            ]
    raise AssertionError(f"_CHROME_CFFI_TARGETS not found in {CF_SESSION}")


def _real_browser_types() -> set[str]:
    """``BrowserType``'s members, read from the installed package.

    **A plain ``from curl_cffi.requests import BrowserType`` does not get
    them, and the reason is this stage's own subject matter.**
    ``tests/scraper/conftest.py`` puts a ``MagicMock`` into
    ``sys.modules["curl_cffi"]`` at module scope, saying the package is "only
    present inside Docker". That was true once and is not now: ``curl_cffi``
    is line 4 of ``scraper/requirements.txt``, which the ``unit-tests`` job
    installs, and it is installed in every developer venv that follows the
    README. The stub is never torn down, so from the moment pytest collects
    ``tests/scraper/`` every later test in the process sees the mock -- and a
    mock iterates as empty, which would turn every set-difference below green.

    So this reads the real package with the stub lifted, and puts it back. The
    assertion that it is not a mock is the load-bearing line: without it this
    helper fails open in exactly the way the census exists to stop.
    """
    import sys

    saved = {name: mod for name, mod in sys.modules.items() if name.startswith("curl_cffi")}
    for name in saved:
        del sys.modules[name]
    try:
        from curl_cffi.requests import BrowserType

        members = {browser.value for browser in BrowserType}
        assert members, (
            "curl_cffi.requests.BrowserType has no members, which means this "
            "read got a stub rather than the package. Every rule below would "
            "pass vacuously."
        )
        return members
    finally:
        sys.modules.update(saved)


def test_every_restated_curl_cffi_target_is_a_real_browser_type():
    """Every impersonation target we name is one curl_cffi actually has.

    Silent in the direction that matters. ``cffi_target_for_ua`` walks this
    list to match FlareSolverr's reported Chrome major version and falls back
    to the nearest lower entry, so a target curl_cffi has dropped raises at the
    session, but a target curl_cffi has *added* and we have not simply never
    gets picked: the scraper keeps impersonating an older Chrome while
    presenting the newer user-agent. That is a TLS-fingerprint mismatch, which
    is the shape of the 2026-08-14 outage, and nothing raises.
    """
    real = _real_browser_types()
    declared = _declared("curl_cffi.requests.BrowserType impersonation targets")

    unreal = sorted(set(declared) - real)
    assert not unreal, (
        f"the census declares these curl_cffi targets and the installed "
        f"curl_cffi does not have them: {unreal}.\n"
        "scraper/processors/cf_session.py would hand one of these to "
        "`impersonate=`, which raises at the session rather than at import."
    )


def test_the_declared_curl_cffi_targets_are_the_ones_the_scraper_holds():
    """The census and ``cf_session.py`` name the same list.

    The other end of the pin, and neither rule works alone: the test above
    checks the census against curl_cffi, so a census that had drifted away
    from the scraper would pass it while the scraper carried a target nothing
    checked.
    """
    declared = set(_declared("curl_cffi.requests.BrowserType impersonation targets"))
    held = set(_cf_session_targets())
    assert declared == held, (
        "the census and scraper/processors/cf_session.py disagree about the "
        f"impersonation targets.\n  only in the census: {sorted(declared - held)}"
        f"\n  only in cf_session.py: {sorted(held - declared)}"
    )


def test_no_chrome_target_curl_cffi_offers_is_missing_from_the_scraper():
    """And the direction that is actually silent: a target we never added.

    This is the assertion the entry exists for. It fails when curl_cffi ships
    support for a newer Chrome than the list knows about, which is exactly when
    FlareSolverr starts reporting that Chrome and the fingerprint stops
    matching the cookie.
    """
    real = {
        target
        for target in _real_browser_types()
        if re.fullmatch(r"chrome\d+", target)
    }
    held = set(_cf_session_targets())
    missing = sorted(real - held, key=lambda t: int(t.removeprefix("chrome")))
    assert not missing, (
        f"curl_cffi offers desktop Chrome targets the scraper does not know "
        f"about: {missing}.\n"
        "Add them to _CHROME_CFFI_TARGETS in scraper/processors/cf_session.py "
        "and to the census row, or cffi_target_for_ua will keep falling back "
        "to an older fingerprint than the user-agent it is paired with."
    )


# ---------------------------------------------------------------------------
# Cloudflare -- the interstitial markers
# ---------------------------------------------------------------------------


def test_the_challenge_marker_set_still_classifies_the_recorded_interstitial():
    """The captured interstitial is still recognised as one.

    Plan 128's outage was eight hours of interstitials counted as successful
    scrapes. The marker set in ``shared/challenge.py`` is a restatement of what
    Cloudflare puts in a ``<title>``, and the corpus is a real page captured
    from production.
    """
    from shared.challenge import html_title, title_looks_like_challenge

    html = gzip.decompress(CHALLENGE_FIXTURE.read_bytes())
    title = html_title(html)
    assert title, f"no <title> read out of {CHALLENGE_FIXTURE.name}"
    assert title_looks_like_challenge(title), (
        f"{CHALLENGE_FIXTURE.name} has title {title!r} and "
        "shared.challenge no longer classifies it as an interstitial. Either "
        "Cloudflare changed the wording -- in which case capture a current "
        "page and widen CHALLENGE_TITLE_RE -- or the marker set was narrowed "
        "and this is Plan 128's outage returning."
    )


def test_a_real_detail_page_is_not_classified_as_an_interstitial():
    """And the negative case, which is what makes the positive one mean anything.

    A marker present in the challenge page proves nothing until it is absent
    from a good one: the discriminator that keyed on
    ``cdn-cgi/challenge-platform`` looked right and matched every cars.com page
    ever served.
    """
    from shared.challenge import html_title, title_looks_like_challenge

    good = REPO_ROOT / "tests" / "fixtures" / "html" / "real_detail_crv.html.gz"
    html = gzip.decompress(good.read_bytes())
    title = html_title(html)
    assert title, f"no <title> read out of {good.name}"
    assert not title_looks_like_challenge(title), (
        f"{good.name} is a real detail page and its title {title!r} matches "
        "CHALLENGE_TITLE_RE. The marker set has been widened into something "
        "that would drop good pages."
    )


# ---------------------------------------------------------------------------
# The census itself
# ---------------------------------------------------------------------------


def test_every_census_entry_is_replayed_or_carries_a_reason():
    """The stage's exit clause, asserted rather than asserted-to.

    *Either* replayed *or* declared out of scope with the reason. An entry
    that is neither is a vocabulary somebody listed and then did nothing
    about, which is the state this stage exists to leave behind.
    """
    for row in CENSUS:
        assert row["verdict"] in {REPLAYED, OUT_OF_SCOPE}, row
        if row["verdict"] == REPLAYED:
            assert row.get("checked"), (
                f"{row['owner']} / {row['what']} is marked replayed and names "
                "no test or script that does the replaying."
            )
        assert row.get("why"), (
            f"{row['owner']} / {row['what']} carries no reason. A replayed "
            "entry owes one too: the reason is what says whether the replay "
            "reaches the real thing or a recording of it."
        )


def test_every_replayed_census_entry_names_something_that_exists():
    """A named check that does not exist is a claim, not a check.

    The same duty ``test_every_asserted_rule_names_a_real_test`` performs for
    ``docs/TESTING.md``'s ``Asserted by`` column, one table over.

    **This rule checked the file and not the function for one commit, and
    that was long enough to let a false entry through.** The census landed
    claiming FlareSolverr's solve body was replayed by
    ``test_every_flaresolverr_field_the_scraper_reads_is_in_the_recorded_body``
    in this module -- a function nobody had written. The file existed, so the
    rule passed and the register asserted a replay that did not happen. A
    check on the containing file is not a check on the thing named, and the
    gap is exactly the width of the thing being claimed.
    """
    missing = []
    for row in CENSUS:
        if row["verdict"] != REPLAYED:
            continue
        target, _, test_name = row["checked"].partition("::")
        path = REPO_ROOT / target
        if not path.exists():
            missing.append((row["what"], target, "file does not exist"))
            continue
        if not test_name:
            # A script rather than a node -- `verify_promtail_contract.py` and
            # its sibling are named whole, because the replay is the script.
            continue
        source = path.read_text(encoding="utf-8")
        if not re.search(rf"^def {re.escape(test_name)}\(", source, re.M):
            missing.append((row["what"], row["checked"], "no such test in that file"))
    assert not missing, (
        "the census names these as the replay and they are not there:\n  "
        + "\n  ".join(f"{what} -> {target} ({why})" for what, target, why in missing)
    )


def test_every_census_site_still_exists():
    """An entry keyed on a file that has moved is checking nothing.

    Stage Y broke five waivers by adding lines above the anchors they named,
    and they failed loudly because a waiver is asserted. These are the same
    fragility with none of the protection unless this test exists.
    """
    missing = []
    for row in CENSUS:
        for site in row["sites"]:
            if not (REPO_ROOT / site).exists():
                missing.append((row["what"], site))
    assert not missing, (
        "the census names these sites and they do not exist:\n  "
        + "\n  ".join(f"{what} -> {site}" for what, site in missing)
    )


@pytest.mark.parametrize(
    "row",
    [row for row in CENSUS if "members" in row],
    ids=lambda r: r["what"],
)
def test_no_declared_vocabulary_is_empty(row):
    """A members tuple that has been emptied turns its rule green.

    Every rule above is a set-difference against ``members``. An empty tuple
    makes the difference empty and the assertion vacuous, which is the one
    edit that silently disarms this whole module.
    """
    assert row["members"], f"{row['what']} declares no members"
