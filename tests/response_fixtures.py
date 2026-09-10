"""Build a test's stand-in response from the model, rather than typing it out.

Plan 162 Stage AA, gap G32.

**Every processor fake in this repository used to be a dict somebody read off
the producer and retyped.** ``archiver``'s endpoint tests held five of them,
one of which was ``{}``, and completing them by hand was the first half of this
stage: 31 tests were asserting bodies their own production code cannot return.
Completing a transcription leaves it a transcription, which is what this plan
is about, so the fakes are built here instead.

The model is the pivot rather than the producer, and both sides are bound to
it: ``test_no_response_model_is_short_of_its_producer`` holds the model against
the function whose dict it serialises, and
``tests/plugins/response_model_fidelity.py`` holds it against the handler
wherever a test drives the real one. A fixture built from the model therefore
inherits both, and a field added to any of the three moves all of them
together.

This is the same clause ``test_no_test_invents_the_shape_of_a_relation_
production_defines`` states for database relations -- *"build the fixture by
applying the migration rather than by retyping it"* -- applied to the other
kind of shape a test is tempted to restate.
"""
from __future__ import annotations

import typing
from typing import Any

from pydantic import BaseModel

_ZEROS: dict[Any, Any] = {bool: False, int: 0, float: 0.0, str: ""}


def _placeholder(annotation: Any) -> Any:
    """A value of *annotation*'s type, or ``None`` where the field allows it.

    Values are deliberately uninteresting. A fixture's job here is to carry
    every key the model declares; the one or two values a test actually asserts
    on arrive as overrides, and everything else being a zero makes it obvious
    which those are when reading the test.
    """
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)

    if origin is typing.Literal:
        return args[0]
    if origin in (typing.Union, getattr(__import__("types"), "UnionType", None)):
        # `X | None` carries the None; a field that admits it is a field whose
        # absence from this branch is legitimate, so that is what it gets.
        if type(None) in args:
            return None
        return _placeholder(args[0])
    if origin in (list, set, tuple, frozenset):
        return []
    if origin is dict:
        return {}
    if isinstance(annotation, type):
        if issubclass(annotation, BaseModel):
            return fixture_for(annotation)
        if annotation in _ZEROS:
            return _ZEROS[annotation]
    return None


def fixture_for(model: type[BaseModel], **overrides: Any) -> dict[str, Any]:
    """A dict carrying every field *model* declares, with *overrides* applied.

    Returns a dict rather than a model instance on purpose: these stand in for
    what a *producer* hands back, and every producer in this repository returns
    a dict. Handing a test a model instance would make the fake a shape the
    thing it replaces cannot produce, which is the defect in miniature.
    """
    built = {
        name: _placeholder(field.annotation)
        for name, field in model.model_fields.items()
    }
    unknown = sorted(set(overrides) - set(built))
    if unknown:
        raise KeyError(
            f"{model.__name__} declares no {unknown}. A test overriding a field "
            f"the model does not have is a test asserting on a key the endpoint "
            f"cannot return -- which is what building these from the model is "
            f"meant to make impossible."
        )
    built.update(overrides)
    return built
