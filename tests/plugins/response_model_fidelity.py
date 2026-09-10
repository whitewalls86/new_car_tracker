"""No response model may silently delete a key its handler produced.

Plan 162 Stage AA, gap G32.

**A ``response_model`` is an assertion about a handler that nothing verifies,
and getting it wrong is worse than not having one.** FastAPI *filters* the
response to the declared fields, so a model short by one key deletes that key
from every response in production. The handler still returns it, every test
still passes, and the generated contract still looks right -- because the
contract is generated from the model, and the model is what is wrong.

That was measured on this branch rather than reasoned about. Adding a
``generated_at`` key to ``dbt_runner``'s ``/dbt/docs/status`` handler left 57
tests passing and ``generate_service_contracts.py --check`` at exit 0, with the
key dropped on the wire. Stage Z retired its "the normalisation names what it
drops" clause because a maintained record of discarded fields is a document
whose drift is invisible by construction; declaring 100 response models by hand
rebuilds exactly that document, one layer down, unless something compares them
to the handlers.

**Wrapped at the library's entry point, so the list of routes stops existing.**
``fastapi.routing.serialize_response`` is the single place every response
passes through with both halves in scope -- the raw object the handler returned
and the field it is about to be filtered against. Wrapping it means every route
any test exercises is checked, and no registry of routes has to be kept
truthful. That is ``sql_execution_recorder``'s reasoning applied to a second
boundary, and for the same reason: a list of what to check is one more thing
that can quietly stop matching.

**The strongest available evidence, and its limit stated rather than implied.**
This checks the routes the suite actually exercises. A route no test reaches is
not checked here -- but that route is a hole the coverage rules already own
(``test_every_status_code_a_route_can_produce_is_asserted``), and it is a
visible one. What this closes is the invisible case: a route that *is* tested,
whose model is wrong, and whose tests pass anyway.

**Only the drop direction fails.** A model declaring a field the handler did
not set is a different defect -- a phantom field, Stage Y's defect at the body
level -- and it cannot be judged from one response, because an optional field
belonging to another branch is legitimately absent from this one. Judging it
needs the whole suite's evidence and it is not judged here.
"""
from __future__ import annotations

from typing import Any

import pytest


def _declared_fields(model: Any) -> frozenset[str] | None:
    """The field names *model* serialises, or ``None`` if it is not a model.

    Both the attribute name and its serialisation alias count as declared: a
    field written ``alias="x"`` is not dropping ``x``, it is renaming it, and
    reporting that as a deletion would be a false positive.
    """
    fields = getattr(model, "model_fields", None)
    if not isinstance(fields, dict):
        return None
    names: set[str] = set()
    for name, info in fields.items():
        names.add(name)
        for attr in ("alias", "serialization_alias", "validation_alias"):
            value = getattr(info, attr, None)
            if isinstance(value, str):
                names.add(value)
    return frozenset(names)


def _dropped(content: Any, model: Any, path: str = "") -> list[str]:
    """Every key *model* would delete from *content*, recursively.

    Recursion is the point rather than a refinement. ``archiver``'s pack
    summary carries a list of buckets carrying a list of packs, and a model
    short by one key three levels down deletes it just as silently as one short
    at the top -- more silently, because nothing at the top looks wrong.
    """
    if isinstance(content, dict):
        declared = _declared_fields(model)
        if declared is None:
            return []
        missing = [f"{path}{key}" for key in sorted(set(content) - declared)]
        for key, value in content.items():
            if key not in declared:
                continue
            info = model.model_fields.get(key)
            if info is None:
                continue
            missing += _dropped(value, _unwrap(info.annotation), f"{path}{key}.")
        return missing
    if isinstance(content, (list, tuple)):
        out: list[str] = []
        for index, item in enumerate(content):
            out += _dropped(item, model, f"{path}[{index}].")
        return out
    return []


def _unwrap(annotation: Any) -> Any:
    """The model inside ``list[X]``, ``X | None``, ``dict[str, X]``.

    Returns the first argument that carries ``model_fields``; a plain
    ``Dict[str, Any]`` yields nothing and its contents go unchecked, which is
    correct -- a free-form object declares no fields, so it deletes none.
    """
    if getattr(annotation, "model_fields", None) is not None:
        return annotation
    for arg in getattr(annotation, "__args__", ()) or ():
        found = _unwrap(arg)
        if getattr(found, "model_fields", None) is not None:
            return found
    return annotation


@pytest.fixture(scope="session", autouse=True)
def _response_model_fidelity() -> Any:
    """Fail any response whose model would delete a key the handler produced.

    **The one session without FastAPI is a real one, and it is not this
    plugin's to fail.** ``docs-tests`` installs ``pytest``, ``pytest-mock`` and
    ``markdown-it-py`` and nothing else, on purpose -- it runs
    ``test_planning_docs.py`` with ``--noconftest`` so a broken service import
    cannot take the planning assertions down. But ``-p`` in ``addopts`` is
    exactly the registration that *survives* ``--noconftest``, which is why
    Stage U used it for the declared-skips gate, so this autouse fixture runs
    there too and raised ``ModuleNotFoundError`` on every test it collected.
    The mechanism that gets this plugin into that job is the same one that
    broke it.

    Yielding unwrapped is the safe direction rather than a hole: a session with
    no ``fastapi`` has no handler to serialise and no response to filter, so
    there is nothing this could have checked. The direction that would be
    unsafe -- a session that *does* exercise handlers running without the wrap
    -- is closed by ``test_the_response_fidelity_plugin_is_registered``, which
    runs in the unit job where ``fastapi`` is installed.
    """
    try:
        import fastapi.routing
    except ModuleNotFoundError:
        yield
        return

    original = fastapi.routing.serialize_response

    async def checked(*, field: Any = None, response_content: Any = None, **kwargs: Any):
        if field is not None:
            model = _unwrap(getattr(getattr(field, "field_info", None), "annotation", None))
            # `HTTPException(detail=...)` arrives already wrapped under
            # `detail`, and the models for those codes declare the envelope, so
            # it needs no special case -- it is compared as it stands.
            dropped = _dropped(response_content, model)
            if dropped:
                ctx = kwargs.get("endpoint_ctx") or {}
                where = ctx.get("path") or ctx.get("function") or "this route"
                raise AssertionError(
                    f"{where} returns {dropped}, and its response model "
                    f"{getattr(model, '__name__', model)} does not declare "
                    f"{'them' if len(dropped) > 1 else 'it'}. FastAPI would "
                    f"delete {'those keys' if len(dropped) > 1 else 'that key'} "
                    f"from the response in production, and nothing else in this "
                    f"repository would notice: the generated contract is built "
                    f"from the model, so it would agree with the model. Either "
                    f"the handler should stop returning "
                    f"{'them' if len(dropped) > 1 else 'it'}, or the model is "
                    f"short and {ctx.get('file', 'the model')} is where it is "
                    f"declared."
                )
        return await original(field=field, response_content=response_content, **kwargs)

    fastapi.routing.serialize_response = checked
    yield
    fastapi.routing.serialize_response = original
