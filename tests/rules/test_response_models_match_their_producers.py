"""A response model may not be short of the function whose result it serialises.

Plan 162 Stage AA, gap G32.

**``tests/plugins/response_model_fidelity.py`` closes this at runtime and
cannot reach the case that matters most here.** It compares a model against
what a handler actually returned, so it sees the truth wherever a test drives
the real handler. Every ``archiver`` endpoint mocks its processor -- those
processors need MinIO -- so what the plugin compares there is the *fake*, and a
processor that grows a key is invisible to it. Measured: adding a seventeenth
key to ``pack_bronze_html``'s summary left **592 tests passing** while FastAPI
would have deleted that key from the response in production.

So this rule reads the producer instead of running it. It is the same shape
``test_no_test_invents_the_shape_of_a_relation_production_defines`` already
uses one layer down -- compare what production defines against what is declared
for it -- and it exists for the same reason that rule gives: *"build the fixture
by applying the migration rather than by retyping it"*. A dict literal in a
processor, a model in a service and a fake in a test file were three copies of
one shape with nothing comparing them, which is the defect this whole plan is
named after.

**Only the drop direction fails.** A producer key the model omits is deleted
from the response silently, and nothing else in this repository would notice --
the generated contract is built from the model, so it agrees with the model.
The reverse, a declared field no producer sets, is not judged here: an optional
field belonging to another return path is legitimately absent from the one this
reads, and calling that a defect would be a rule that has to be argued with.

**Unresolvable producers fail rather than being skipped**, which is Stage W's
clause and the reason it exists: "we could not tell what this returns" is the
state the defect lives in. A handler this cannot follow is listed, not passed.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.response_fixtures import (
    _delegating_routes,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

def test_the_producer_corpus_is_not_empty():
    """The floor. A rule that resolves no producers passes over nothing.

    Every shape this reads -- a decorator keyword, an import alias, an
    assign-then-return -- is a pattern that can stop matching without anyone
    editing this file, and a set difference over an empty corpus is empty.
    """
    rows = _delegating_routes()
    assert rows, (
        "no route resolved to a producer function. Either the handlers stopped "
        "delegating, `response_model=` moved, or the reader below is looking "
        "for a shape this repository no longer has -- and an empty corpus makes "
        "the rule beside this one pass unconditionally."
    )


@pytest.mark.parametrize(
    ("route", "model", "producer", "keys", "fields"),
    # The last two fields of a row -- the producer names and the package -- are
    # for `produced_by`, which needs to find the model that will serialise a
    # given producer. This rule needs neither, so it takes the first five.
    [pytest.param(*row[:5], id=row[0]) for row in _delegating_routes()],
)
def test_no_response_model_is_short_of_its_producer(
    route: str, model: str, producer: str, keys: set[str], fields: set[str],
) -> Any:
    """A key the producer emits and the model omits is deleted in production.

    Silently, and with every other instrument agreeing: the tests mock the
    producer, so the runtime plugin compares the model against a fake, and the
    generated contract is built from the model, so it agrees with the model
    too. This is the only reader that sees the producer itself.
    """
    dropped = sorted(keys - fields)
    assert not dropped, (
        f"{producer} returns {dropped}, and {model} -- the response model "
        f"{route} declares -- does not. FastAPI filters the response to the "
        f"model, so those keys never reach a caller, and nothing else here "
        f"would say so: the endpoint's tests mock the producer and the "
        f"generated contract is built from the model. Either add them to the "
        f"model or stop returning them."
    )
