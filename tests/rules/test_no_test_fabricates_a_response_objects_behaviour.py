"""No test fabricates a response object's behaviour.

Plan 162 Stage AL, gap G33 — statement 8's other half.

**The body half already exists and cannot see this.**
``test_no_mock_invents_a_service_response`` holds a fabricated body against
the callee's contract, and a test can pass it completely — build the body with
``service_response()``, byte-faithful to what the service sends — while lying
about what the service *does*. A bare ``Mock``'s ``raise_for_status()``
returns another mock instead of raising, so a caller that checks the status
before the body (Stage Y's own rule) never takes its refusal branch under
test. Measured on 2026-09-10, recorded in plan_162 §Record: ``_service_jobs``
given a real 503 ``requests.Response`` answers ``unknown``; given a bare
``Mock`` carrying the identical body it answers ``known, count 2`` — which is
why ``test_service_503_body_is_still_known_positive_evidence`` passes while
production returns ``unknown``.

**The object, unlike the body, always has an owner this repository can ask.**
A body's owner may be cars.com, which nothing can interrogate — that is Stage
AB's tiering. The *object* is ``requests.Response``: its behaviour is the
client library's contract, installed in every venv that runs these tests. So
this rule's scope is every seam, owned and third-party alike — the solver
mocks in ``tests/scraper/`` fabricate the same object the coordination mocks
do, and the far side being cars.com excuses the body, not the object.

**Keyed on the shape, not on seam attribution.** Configuring ``.json`` on a
mock is the signature of a hand-built response object — nothing else in this
suite has a ``.json`` whose return value a test assigns. The sibling rule's
seam resolution missed three of these sites (a mock built inside a
``side_effect`` closure, one handed over in a list) precisely because
attribution is escapable; the shape is not.

**An entry is "not yet converted."** The drain is a response object whose
behaviour derives from the client library — a real ``requests.Response``, or
a helper that wires ``raise_for_status`` from the status it carries — and
building that helper is conversion work this stage does not do.
"""
from __future__ import annotations

import ast

from tests.rules.test_testing_contract import REPO_ROOT

TESTS = REPO_ROOT / "tests"

_SHAPES = (".json.return_value", ".json.side_effect")


def _sites_in(tree: ast.AST) -> set[str]:
    """``<enclosing function>::<assignment target>`` for every fabrication.

    Keyed on the function and the expression, never a line number, for
    ``GUESSED_BOUND_WAIVERS``'s stated reason. Two identical assignments in
    one function collapse to one entry, which is the grain at which the
    repair happens anyway.
    """
    found: set[str] = set()
    enclosing: dict[int, str] = {}
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for node in ast.walk(function):
                enclosing.setdefault(id(node), function.name)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            rendered = ast.unparse(target)
            if rendered.endswith(_SHAPES):
                where = enclosing.get(id(node), "<module>")
                found.add(f"{where}::{rendered}")
    return found


def fabricated_response_objects() -> set[str]:
    """Every ``file::function::target`` where a test configures a mock's ``.json``."""
    found: set[str] = set()
    for path in sorted(TESTS.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        relative = path.relative_to(REPO_ROOT).as_posix()
        found |= {f"{relative}::{site}" for site in _sites_in(tree)}
    return found


# Seeded 2026-09-10 from this rule's own first run: 26 assignment sites in 6
# files, 25 keys (two sites in one closure share a function and a target).
# §Stage AL's measurement recorded 24 — its reader resolved seams and missed
# a closure-built mock and one handed over in a `side_effect` list, which is
# why this rule keys on the shape instead. Every entry is a hand-built object
# standing where a `requests.Response` stands in production; each drains by
# the object deriving from the client library rather than by the entry being
# argued with.
FABRICATED_OBJECT_LEDGER: tuple[str, ...] = (
    "tests/airflow/test_scrape_listings.py::test_makes_and_models_reach_scraper_correctly::poll_resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_makes_and_models_reach_scraper_correctly::submit_resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_one_post_per_config_scope::poll_resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_one_post_per_config_scope::resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_post_body_is_wrapped_in_params_key::fetched_resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_post_body_is_wrapped_in_params_key::poll_resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_post_body_is_wrapped_in_params_key::submit_resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_scope_sent_as_query_param::poll_resp.json.return_value",
    "tests/airflow/test_scrape_listings.py::test_scope_sent_as_query_param::submit_resp.json.return_value",
    "tests/ops/routers/test_admin.py::test_dbt_docs_generate_fails::mock_requests['post'].return_value.json.return_value",
    "tests/ops/routers/test_admin.py::test_dbt_docs_generate_ok::mock_requests['post'].return_value.json.return_value",
    "tests/ops/routers/test_admin.py::test_dbt_trigger_sends_a_selector::mock_requests['post'].return_value.json.return_value",
    "tests/ops/routers/test_admin.py::test_dbt_trigger_with_select_override::mock_requests['post'].return_value.json.return_value",
    "tests/ops/routers/test_admin.py::test_fetch_dbt_context_reads_a_503_as_busy_not_broken::busy.json.side_effect",
    "tests/ops/routers/test_admin.py::test_fetch_dbt_context_reads_ready_and_docs::mock_requests['get'].return_value.json.side_effect",
    "tests/ops/test_coordination_drain.py::test_container_evidence_filters_live_oneoffs_by_declared_scope::response.json.return_value",
    "tests/ops/test_coordination_drain.py::test_scraper_evidence_is_partitioned_by_surface::response.json.return_value",
    "tests/ops/test_coordination_drain.py::test_service_503_body_is_still_known_positive_evidence::response.json.return_value",
    "tests/ops/test_coordination_drain.py::test_unknown_oneoff_service_fails_closed::response.json.return_value",
    "tests/ops/test_coordination_release.py::test_auxiliary_gate_fails_closed_when_sibling_evidence_is_unreadable::response.json.side_effect",
    "tests/ops/test_coordination_release.py::test_auxiliary_gate_is_keyed_on_sibling_project::response.json.side_effect",
    "tests/ops/test_coordination_release.py::test_auxiliary_gate_passes_when_all_siblings_remain_stopped::response.json.return_value",
    "tests/scraper/processors/test_cf_session.py::_flaresolverr_response::mock_resp.json.return_value",
    "tests/scraper/processors/test_cf_session.py::test_flaresolverr_error_status_raises::mock_resp.json.return_value",
    "tests/scraper/test_metrics.py::_solver_response::resp.json.return_value",
)


def test_the_object_reader_sees_the_shape_a_bare_mock_takes():
    """The floor: a canary source the reader must find both sites in.

    The same design as the SQL detector's canary — the reader's predicate is
    a string comparison that can be narrowed without any test going red, so a
    specimen of each shape is parsed here and must be seen. The corpus
    non-emptiness rides along: a suite with no fabrication sites at all is
    what draining the ledger looks like, so that direction is the ledger's,
    not this floor's.
    """
    canary = ast.parse(
        "def test_specimen(mocker):\n"
        "    resp = mocker.Mock()\n"
        "    resp.json.return_value = {'ok': True}\n"
        "    resp.json.side_effect = [{'ok': True}]\n"
    )
    seen = _sites_in(canary)
    assert seen == {
        "test_specimen::resp.json.return_value",
        "test_specimen::resp.json.side_effect",
    }, (
        f"the reader found {sorted(seen)} in a specimen holding both shapes; "
        f"a predicate that has narrowed reads fewer fabrications everywhere "
        f"else too, silently."
    )


def test_no_test_fabricates_a_response_objects_behaviour():
    """A mock whose ``.json`` a test assigns is an object the test invented.

    Its body can be contract-faithful while its behaviour — the raise, the
    ``ok``, the ``status_code`` the production code branches on — answers
    whatever a bare ``Mock`` answers, which is how a caller's refusal branch
    passes its test and never runs in production. Both directions: a new
    fabrication fails, and a converted one fails until its entry is deleted.
    """
    found = fabricated_response_objects()
    ledgered = set(FABRICATED_OBJECT_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "these tests configure a mock's .json by hand, fabricating the "
        "response object's behaviour:\n  " + "\n  ".join(unwaived)
        + "\n\nBuild the object from the client library — a real "
        "requests.Response, or a helper that derives raise_for_status from "
        "the status it carries. The ledger records what is not yet "
        "converted; adding to it is a decision, not a convenience."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these ledger entries no longer describe a fabrication and must be "
        "deleted:\n  " + "\n  ".join(stale)
    )
