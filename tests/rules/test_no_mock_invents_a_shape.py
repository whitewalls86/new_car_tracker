"""A mock may not hand a literal to a seam whose shape this repository defines.

Plan 162 Stage AA, gap G32.

**This is the general form of the defect the whole plan is named after**, and
it is the third time this repository has had to state it. ``db/migrations/``
owns relation shapes, so
``test_no_test_invents_the_shape_of_a_relation_production_defines`` refuses a
fixture that declares one -- *"build the fixture by applying the migration
rather than by retyping it"*. ``db/migrations/`` also owns status vocabularies,
so ``test_no_module_retypes_a_database_vocabulary_it_could_import`` refuses a
restatement wherever an import exists. A function that returns a dict owns that
dict's keys, and until this rule nothing said so.

The measurement that opened it: **53 mock sites handed a dict literal to a
function whose returned keys this repository can read**, and at least four were
already short of the producer -- ``_run_cleanup_queue`` missing ``error``,
``_compact_silver`` missing both size fields, ``promote_snapshot_pointers``
missing ``alias_key`` and ``latest_key``. Those are tests asserting bodies
their own production code cannot return, which is exactly what this stage found
31 of at the endpoint layer and repaired by hand before writing this.

**The trigger is exact rather than a threshold.** A site is in scope when the
patched target resolves to a producer whose keys are readable; there is no
"looks like a shape" judgement to argue with. What a site must hand over
instead is a call -- ``fixture_for`` for a producer a model serialises,
``produced_by`` for one with no model -- and both build from the definition.

**Every waiver is a test that can still pass while production disagrees with
it**, so the ledger is not a backlog of tidying. It is a list of assertions
nobody can currently trust.
"""
from __future__ import annotations

import ast
from pathlib import Path

from tests.response_fixtures import producer_shapes, resolve_producer

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS = REPO_ROOT / "tests"

# Calls that build a fake from a definition rather than restating one. A site
# handing over one of these is what draining a waiver looks like.
_DERIVED = frozenset({
    "fixture_for", "produced_by", "manifest_fixture",
    "_pack_result", "_prune_result", "_verify_result",
    "_compact_result", "_disk_result", "_snapshot_result",
})

_MOCK_VALUE_KEYWORDS = frozenset({"return_value", "side_effect"})


def _patched_target(call: ast.Call) -> str | None:
    """The last segment of what this ``patch`` call replaces.

    ``mocker.patch("archiver.app._pack_bronze_html", ...)`` and
    ``mocker.patch.object(archiver_app, "_run_disk_usage", ...)`` both name the
    same thing in different positions, and both are used here.
    """
    name = ast.unparse(call.func) if not isinstance(call.func, ast.Name) else call.func.id
    if name.endswith("patch.object"):
        if len(call.args) >= 2 and isinstance(call.args[1], ast.Constant):
            return str(call.args[1].value)
        return None
    if name.endswith(".patch") or name == "patch":
        if call.args and isinstance(call.args[0], ast.Constant):
            return str(call.args[0].value)
    return None


def _literal_mock_sites() -> list[tuple[str, str, tuple[str, ...]]]:
    """Every mock handing a dict literal to a readable producer."""
    sites: list[tuple[str, str, tuple[str, ...]]] = []
    for path in sorted(TESTS.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            target = _patched_target(node)
            if target is None:
                continue
            producer = resolve_producer(target)
            if producer is None:
                continue
            for keyword in node.keywords:
                if keyword.arg not in _MOCK_VALUE_KEYWORDS:
                    continue
                value = keyword.value
                if isinstance(value, ast.Call):
                    called = getattr(value.func, "id", "") or getattr(value.func, "attr", "")
                    if called in _DERIVED:
                        continue
                if not isinstance(value, ast.Dict):
                    continue
                where = f"{path.relative_to(REPO_ROOT).as_posix()}:{node.lineno}"
                sites.append((where, producer, tuple(sorted(
                    key.value for key in value.keys
                    if isinstance(key, ast.Constant) and isinstance(key.value, str)
                ))))
    return sites


# Seeded 2026-09-09 at the measurement this rule opened on, and drained by
# converting each site to `fixture_for` or `produced_by`. Every entry is a test
# that can pass while the producer it stands in for disagrees with it.
FABRICATED_PRODUCER_WAIVERS: tuple[str, ...] = ()

def test_the_mock_site_corpus_is_not_empty():
    """The floor. A reader that resolves no producers accuses nobody.

    Every shape this walks -- a ``patch`` call, a keyword, a producer name --
    can stop matching without anyone editing this file, and a rule that finds
    no sites is indistinguishable from a repository with no fabrications.
    """
    shapes = producer_shapes()
    assert len(shapes) > 50, (
        f"only {len(shapes)} producers resolved out of this repository. The "
        f"reader in tests/response_fixtures.py is looking for a shape the tree "
        f"no longer has, and with an empty corpus the rule below accuses "
        f"nobody rather than failing."
    )


def test_no_mock_invents_a_shape_production_defines():
    """A fake short of its producer is an assertion about a body nobody returns.

    The repair is never to add the missing keys by hand -- that is what the
    first half of this stage did, and it left 53 transcriptions that were
    correct on the day and free to drift the next. It is to build the fake from
    the definition, so the two cannot disagree.
    """
    offenders = []
    for where, producer, keys in _literal_mock_sites():
        if where in FABRICATED_PRODUCER_WAIVERS:
            continue
        source, real = producer_shapes()[producer]
        missing = sorted(set(real) - set(keys))
        offenders.append(
            f"{where} mocks {producer} with a {len(keys)}-key literal; "
            f"{source} returns {len(real)}"
            + (f", missing {missing}" if missing else "")
        )
    assert not offenders, (
        f"{len(offenders)} mock(s) restate a shape a production function owns, "
        f"instead of building it from that function:\n  "
        + "\n  ".join(offenders)
        + "\n\nHand over `fixture_for(Model, ...)` where a response model "
          "serialises the producer, or `produced_by('name', ...)` where none "
          "does. Both read the definition, so a producer that gains a key "
          "moves every fake with it."
    )


def test_every_waiver_names_a_site_that_still_exists():
    """A waiver for a repaired site grandfathers nothing and hides the next one.

    The same clause `test_no_waiver_outlives_the_plan_that_owns_it` states for
    the ledgers in `test_testing_contract.py`, applied here because this ledger
    is keyed on a file and a line -- which is the most fragile key a waiver can
    have, and the reason it drains rather than lives.
    """
    live = {where for where, _producer, _keys in _literal_mock_sites()}
    stale = sorted(set(FABRICATED_PRODUCER_WAIVERS) - live)
    assert not stale, (
        f"these waivers name mock sites that no longer fabricate: {stale}. "
        f"Either the site was repaired and the waiver should go, or it moved "
        f"and the waiver now grandfathers a line nobody chose."
    )
