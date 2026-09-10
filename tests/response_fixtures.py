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

import ast
import importlib
import typing
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVICES = ("archiver", "container_health", "dbt_runner", "ops", "processing", "scraper")

# A handler whose body composes the response inline has no separate producer to
# read, and the runtime plugin already covers it: those handlers are exercised
# by tests that do not mock anything below them.
_VERBS = frozenset({"get", "post", "put", "patch", "delete"})

# Marks a producer that is module-level mutable state rather than a
# function, so the two kinds cannot collide in one namespace.
_STATE = "state:"

# The status a `(status, payload)` helper returns beside the payload that
# becomes a 200 body. Every such helper in this repository uses it, and a
# tuple-returning producer without an `_OK` branch is left unresolved
# rather than read as if its refusal payloads were the response.
_OK = "ok"


def _module_paths(package: str) -> list[Path]:
    return [
        path for path in sorted((REPO_ROOT / package).rglob("*.py"))
        if "__pycache__" not in path.parts
    ]


def _dict_keys(node: ast.AST) -> set[str]:
    return {
        key.value for key in node.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }


def _returned_keys(fn: ast.AST) -> tuple[set[str], list[str]]:
    """Top-level keys *fn* can return, and the returns this could not follow.

    Three shapes reach it, and all three are in this repository: a dict
    literal returned directly, a dict literal assigned to a local that is later
    returned, and that same local with keys added by subscript assignment --
    which is how ``delete_packed_source_html`` builds its summary and how
    ``_finalize`` completes it.
    """
    keys: set[str] = set()
    unresolved: list[str] = []
    returned_names: set[str] = set()

    for node in ast.walk(fn):
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        value = node.value
        if isinstance(value, ast.Tuple):
            # `return "ok", {...}` -- a status and a payload together, which
            # the coordination write endpoints all use. Only the success
            # branch's dict is the response body: the others become
            # `HTTPException(detail=...)` on a 4xx, so counting them would
            # report `reason` and `failing_gates` as keys the 200 drops. That
            # was this reader's first draft and the rule said so immediately.
            leading = value.elts[0] if value.elts else None
            if not (isinstance(leading, ast.Constant) and leading.value == _OK):
                continue
            for element in value.elts[1:]:
                if isinstance(element, ast.Dict):
                    keys |= _dict_keys(element)
            continue
        if isinstance(value, ast.Dict):
            keys |= _dict_keys(value)
        elif isinstance(value, ast.Name):
            returned_names.add(value.id)
        elif isinstance(value, ast.Call):
            unresolved.append(f"line {value.lineno}: returns {ast.unparse(value)[:60]}")
        elif isinstance(value, ast.Constant):
            continue
        else:
            unresolved.append(f"line {value.lineno}: returns {ast.unparse(value)[:60]}")

    for name in returned_names:
        for node in ast.walk(fn):
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if (isinstance(target, ast.Name) and target.id == name
                            and isinstance(node.value, ast.Dict)):
                        keys |= _dict_keys(node.value)
                    if (isinstance(target, ast.Subscript)
                            and isinstance(target.value, ast.Name)
                            and target.value.id == name
                            and isinstance(target.slice, ast.Constant)):
                        keys.add(target.slice.value)
    return keys, unresolved


def _producers(package: str) -> dict[str, tuple[str, set[str], list[str]]]:
    """Every producer in *package*: functions that return dicts, and the
    module-level stores that hold them.

    A store counts because a route can hand back what a *different* function
    wrote into it -- ``scraper``'s two job listings serialise dicts built in
    ``run_scrape_results`` and ``scrape_detail_batch_endpoint``, neither of
    which the listing route calls. Reading the assignments into the store is
    how that shape is recovered without following execution.
    """
    found: dict[str, tuple[str, set[str], list[str]]] = {}
    for path in _module_paths(package):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = path.relative_to(REPO_ROOT).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                keys, _ = _returned_keys(node)
                if keys:
                    found[f"{rel}::{node.name}"] = (f"{rel}::{node.name}", keys, [])
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
                for target in node.targets:
                    if (isinstance(target, ast.Subscript)
                            and isinstance(target.value, ast.Name)):
                        store = f"{rel}::{_STATE}{target.value.id}"
                        blank = (f"{rel}::{target.value.id}", set(), [])
                        where, existing, _ = found.get(store, blank)
                        found[store] = (where, existing | _dict_keys(node.value), [])

    # A producer that dispatches to other producers returns their union.
    # `_process_artifact` answers `_process_results_page(...)` or
    # `_process_detail_page(...)` and carries a `skip` literal of its own, so
    # reading only its own returns reports a key set no caller ever sees --
    # which is how a test overriding `error` on it came to be refused for
    # naming a key the producer "does not emit". Two passes rather than a
    # fixpoint loop: this repository nests one level and a cycle would spin.
    delegations: dict[str, set[str]] = {}
    for path in _module_paths(package):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = path.relative_to(REPO_ROOT).as_posix()
        # Where an imported name was defined, so a delegation that crosses a
        # module resolves. `_process_results_page` spreads
        # `write_srp_observations`'s counters, and that writer lives in
        # `processing/writers/`, so qualifying every target with the *calling*
        # module reported a key set short of what the caller returns.
        imported: dict[str, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                origin = node.module.replace(".", "/") + ".py"
                for alias in node.names:
                    imported[alias.asname or alias.name] = f"{origin}::{alias.name}"

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            # Locals filled by a call, so a `**spread` of one can be followed.
            from_call: dict[str, str] = {}
            for inner in ast.walk(node):
                if isinstance(inner, ast.Assign) and isinstance(inner.value, ast.Call):
                    fn = inner.value.func
                    called_name = getattr(fn, "id", None) or getattr(fn, "attr", None)
                    for tgt in inner.targets:
                        if isinstance(tgt, ast.Name) and called_name:
                            from_call[tgt.id] = called_name

            for inner in ast.walk(node):
                if not isinstance(inner, ast.Return) or inner.value is None:
                    continue
                names: list[str] = []
                if isinstance(inner.value, ast.Call):
                    fn = inner.value.func
                    got = getattr(fn, "id", None) or getattr(fn, "attr", None)
                    if got:
                        names.append(got)
                elif isinstance(inner.value, ast.Dict):
                    # `return {"status": ..., **result}` -- the spread carries
                    # whatever the call that filled `result` returns, which is
                    # how `_process_results_page` passes the writer's counters
                    # up. Reading only the literal half reports a key set the
                    # caller never sees.
                    for key, item in zip(inner.value.keys, inner.value.values):
                        if key is None and isinstance(item, ast.Name):
                            spread = from_call.get(item.id)
                            if spread:
                                names.append(spread)
                for name in names:
                    if name == node.name:
                        continue
                    for qual in (imported.get(name), f"{rel}::{name}"):
                        if qual and qual in found:
                            delegations.setdefault(
                                f"{rel}::{node.name}", set()).add(qual)
                            break
    for _pass in range(2):
        for caller, targets in delegations.items():
            gained: set[str] = set()
            for target in targets:
                gained |= found[target][1]
            if caller in found:
                where, keys, notes = found[caller]
                found[caller] = (where, keys | gained, notes)
            elif gained:
                found[caller] = (f"{caller} (via {sorted(targets)})", gained, [])
    return found



def _import_aliases(tree: ast.AST) -> dict[str, str]:
    """``as`` names back to the names they bind, so `_pack_bronze_html` resolves."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                aliases[alias.asname or alias.name] = alias.name
    return aliases


def _handler_producers(fn: ast.AST, aliases: dict[str, str]) -> set[str]:
    """Every repo function whose dict this handler can hand back.

    A set rather than one name, because a handler legitimately has more than
    one producer -- ``scrape_detail`` dispatches on a mode and returns
    ``scrape_detail_fetch`` or ``scrape_detail_dummy``. Taking their union is
    right: the model has to cover whichever ran.

    Four shapes reach this and all four are in this repository:

    * ``return producer(...)``
    * ``result = producer(...)`` then ``return result``
    * ``result, other = producer(...)`` then ``return result`` -- tuple
      unpacking, which the coordination routes use for all three of their
      write endpoints
    * ``return {"artifact_id": x, **result}`` -- a literal that spreads a
      producer's dict into itself, which is how the artifact route answers

    The first version of this rule followed only the first two and bailed
    whenever a handler had more than one candidate. That left nine routes
    resolving to nothing and silently outside the rule, which is the shape of
    hole this plan exists to close -- a check that passes because it declined
    to look.
    """
    found: set[str] = set()
    aliased: set[str] = set()

    def name_of(call: ast.Call) -> str | None:
        called = call.func
        raw = getattr(called, "id", None) or getattr(called, "attr", None)
        return aliases.get(raw, raw) if raw else None

    # Locals that hold a producer's result, and the producer that filled them.
    from_local: dict[str, str] = {}
    for node in ast.walk(fn):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        producer = name_of(node.value)
        if not producer:
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Name):
                from_local[target.id] = producer
            elif isinstance(target, ast.Tuple):
                for element in target.elts:
                    if isinstance(element, ast.Name):
                        from_local[element.id] = producer

    for node in ast.walk(fn):
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        value = node.value
        if isinstance(value, ast.Call):
            producer = name_of(value)
            if producer:
                found.add(producer)
        elif isinstance(value, ast.Name):
            if value.id in from_local:
                found.add(from_local[value.id])
        elif isinstance(value, ast.Dict):
            # `**spread` of a local a producer filled.
            for key, item in zip(value.keys, value.values):
                if key is None and isinstance(item, ast.Name) and item.id in from_local:
                    found.add(from_local[item.id])
        elif isinstance(value, (ast.ListComp, ast.SetComp)):
            # `[job for job in _jobs.values()]` -- the shape is whatever the
            # module writes into `_jobs`, which is a literal in this same file.
            for inner in ast.walk(value):
                if (isinstance(inner, ast.Call)
                        and getattr(inner.func, "attr", "") == "values"
                        and isinstance(inner.func.value, ast.Name)):
                    aliased.add(inner.func.value.id)
    found |= {f"{_STATE}{name}" for name in aliased}
    return found


def _model_fields(dotted: str, package: str) -> set[str] | None:
    """The fields of a model named in a decorator, or ``None`` if not a model."""
    if dotted in {"bool", "int", "str"} or dotted.startswith(("List[", "list[")):
        return None
    module = importlib.import_module(f"{package}.api_models")
    model = getattr(module, dotted, None)
    if model is None:
        shared = importlib.import_module("shared.api_models")
        model = getattr(shared, dotted, None)
    fields = getattr(model, "model_fields", None)
    if not isinstance(fields, dict):
        return None
    names = set(fields)
    for info in fields.values():
        for attr in ("alias", "serialization_alias"):
            value = getattr(info, attr, None)
            if isinstance(value, str):
                names.add(value)
    return names


def _qualify(name: str, caller: Path, producers: dict[str, Any]) -> str | None:
    """A bare producer name, qualified by the module that defines it."""
    rel = caller.relative_to(REPO_ROOT).as_posix()
    if f"{rel}::{name}" in producers:
        return f"{rel}::{name}"
    for candidates in (
        [q for q in producers if q.rsplit("::", 1)[1] == name],
        [q for q in producers if q.rsplit("::", 1)[1].lstrip("_") == name.lstrip("_")],
    ):
        if len(candidates) == 1:
            return candidates[0]
    return None


def _delegating_routes() -> list[tuple[Any, ...]]:
    """Every route whose body hands back another function's dict.

    Each row is (route, model name, producer locations, the keys those
    producers emit, the model's fields, the producer names, the package).
    The last two are for :func:`produced_by`, which needs to find the
    model that will serialise a given producer.
    """
    rows: list[tuple[Any, ...]] = []
    for package in SERVICES:
        producers = _producers(package)
        for path in _module_paths(package):
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            aliases = _import_aliases(tree)
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                declared = None
                for decorator in node.decorator_list:
                    if not isinstance(decorator, ast.Call):
                        continue
                    verb = getattr(decorator.func, "attr", "")
                    if verb not in _VERBS:
                        continue
                    for keyword in decorator.keywords:
                        if keyword.arg == "response_model":
                            declared = ast.unparse(keyword.value)
                if declared is None:
                    continue
                fields = _model_fields(declared, package)
                if fields is None:
                    continue
                producers_named = _handler_producers(node, aliases)
                # `_producers` is keyed by module so two same-named helpers in
                # one package cannot merge; the handler names them bare, so
                # each is resolved against the module that defined it.
                resolved = sorted({
                    qual for qual in (
                        _qualify(name, path, producers) for name in producers_named
                    ) if qual is not None
                })
                if not resolved:
                    continue
                keys: set[str] = set()
                wheres: list[str] = []
                for name in resolved:
                    where, produced, _ = producers[name]
                    keys |= produced
                    wheres.append(where)
                own, _unresolved = _returned_keys(node)
                rows.append((
                    f"{path.relative_to(REPO_ROOT).as_posix()}::{node.name}",
                    declared, " + ".join(wheres), keys | own, fields,
                    tuple(resolved), package,
                ))
    return rows



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


@lru_cache(maxsize=1)
def producer_shapes() -> dict[str, tuple[str, frozenset[str]]]:
    """Every dict-returning function and state store this repository holds.

    Cached because it parses every production module, and both the fixture
    builder below and the rules that read it want the same answer.
    """
    shapes: dict[str, tuple[str, frozenset[str]]] = {}
    for package in SERVICES:
        for name, (where, keys, _) in _producers(package).items():
            shapes[name] = (where, frozenset(keys))
    return shapes


def resolve_producer(target: str) -> str | None:
    """The producer a patch target names, keyed by module and not by name alone.

    ``mocker.patch("ops.coordination_drain._database_count")`` carries the
    module, and using it is not a refinement -- ``_known`` and ``_unknown`` are
    defined in *both* ``ops/coordination_drain.py`` and
    ``ops/coordination_release.py`` with different keys, so a bare-name index
    merged them and reported ``gate`` as a key the drain helper returns. It
    does not.

    A bare name still resolves when exactly one definition answers to it. Two
    definitions and no module is genuinely undetermined, and this returns
    ``None`` rather than choosing: the rule that reads this treats an
    unresolvable seam as out of scope, and the floor above it is what catches
    the reader going blind altogether.
    """
    shapes = producer_shapes()
    if target in shapes:
        return target

    parts = target.split(".")
    name = parts[-1]
    module = "/".join(parts[:-1]) + ".py" if len(parts) > 1 else None

    # Exact before tolerant. `maintenance.py` holds both the route handler
    # `reap_stuck_processing` and the helper `_reap_stuck_processing`, and the
    # underscore-tolerant pass alone treats them as one name and refuses both
    # as ambiguous. The prefix only ever bridges an import alias, so an exact
    # hit is never the wrong answer.
    for candidates in (
        [q for q in shapes if q.rsplit("::", 1)[1] == name],
        [q for q in shapes if q.rsplit("::", 1)[1].lstrip("_") == name.lstrip("_")],
    ):
        if module:
            scoped = [q for q in candidates if q.split("::")[0] == module]
            if len(scoped) == 1:
                return scoped[0]
        if len(candidates) == 1:
            return candidates[0]
    return None


def produced_by(name: str, **overrides: Any) -> dict[str, Any]:
    """A complete stand-in for what *name* returns, read from *name* itself.

    For a producer whose result a route serialises, prefer
    :func:`fixture_for` with that route's model -- the model is the richer
    declaration and carries types. This is for the rest: internal helpers with
    no model, whose shape lives only in their own return statement.

    Values default to ``None`` because this reads keys and not types. That is
    the honest limit of an AST reader, and it is enough for the defect: a fake
    short of the producer is a fake asserting a body the producer cannot
    return, and that is a question about keys.
    """
    shapes = producer_shapes()
    resolved = resolve_producer(name)
    if resolved is None:
        raise KeyError(
            f"nothing in this repository named {name!r} returns a dict literal "
            f"this reader can see. A mock of a seam whose shape cannot be read "
            f"is a mock nothing can check -- give the function a literal return "
            f"or a model, rather than writing the fake by hand."
        )

    _where, keys = shapes[resolved]

    # Where a response model serialises this producer, borrow its *types* --
    # `None` for an `int` field would not survive FastAPI's own validation.
    # Only its types: the field list stays the producer's own, because a model
    # describes the route's response and a route may add keys the producer
    # never returns. `/process/artifact/{id}` answers
    # `{"artifact_id": id, **result}`, so taking the model's field list here
    # would have put `artifact_id` inside `result` and let it overwrite the one
    # the route set. The suite caught that immediately.
    model = _model_by_producer().get(resolved)
    typed = fixture_for(model) if model is not None else {}
    unknown = sorted(set(overrides) - set(keys))
    if unknown:
        raise KeyError(
            f"{name} returns no {unknown}. Overriding a key the producer does "
            f"not emit is asserting on a value the caller cannot receive."
        )
    built: dict[str, Any] = {key: typed.get(key) for key in sorted(keys)}
    built.update(overrides)
    return built


@lru_cache(maxsize=1)
def _model_by_producer() -> dict[str, Any]:
    """Producer name to the response model that serialises its dict, if any."""
    mapping: dict[str, Any] = {}
    for _route, model_name, _where, _keys, _fields, producers, package in _delegating_routes():
        module = importlib.import_module(f"{package}.api_models")
        model = getattr(module, model_name, None)
        if model is None:
            model = getattr(importlib.import_module("shared.api_models"), model_name, None)
        if model is None:
            continue
        for producer in producers:
            mapping.setdefault(producer, model)
    return mapping
