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

import ast
import importlib
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICES = ("archiver", "container_health", "dbt_runner", "ops", "processing", "scraper")

# A handler whose body composes the response inline has no separate producer to
# read, and the runtime plugin already covers it: those handlers are exercised
# by tests that do not mock anything below them.
_VERBS = frozenset({"get", "post", "put", "patch", "delete"})


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
        if isinstance(value, ast.Dict):
            keys |= _dict_keys(value)
            for item in value.values:
                pass
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
    """Every function in *package* that returns a dict, by name."""
    found: dict[str, tuple[str, set[str], list[str]]] = {}
    for path in _module_paths(package):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            keys, _ = _returned_keys(node)
            if keys:
                rel = path.relative_to(REPO_ROOT).as_posix()
                found[node.name] = (f"{rel}::{node.name}", keys, [])
    return found


def _import_aliases(tree: ast.AST) -> dict[str, str]:
    """``as`` names back to the names they bind, so `_pack_bronze_html` resolves."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                aliases[alias.asname or alias.name] = alias.name
    return aliases


def _handler_producer(fn: ast.AST, aliases: dict[str, str]) -> str | None:
    """The single repo function whose dict this handler hands back, if any."""
    candidates: set[str] = set()
    returned_names: set[str] = set()
    for node in ast.walk(fn):
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        if isinstance(node.value, ast.Call):
            called = node.value.func
            name = getattr(called, "id", None) or getattr(called, "attr", None)
            if name:
                candidates.add(aliases.get(name, name))
        elif isinstance(node.value, ast.Name):
            returned_names.add(node.value.id)

    for name in returned_names:
        for node in ast.walk(fn):
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    if (isinstance(target, ast.Name) and target.id == name
                            and isinstance(node.value, ast.Call)):
                        called = node.value.func
                        inner = getattr(called, "id", None) or getattr(called, "attr", None)
                        if inner:
                            candidates.add(aliases.get(inner, inner))
    return sorted(candidates)[0] if len(candidates) == 1 else None


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


def _delegating_routes() -> list[tuple[str, str, str, set[str], set[str]]]:
    """Every route whose body hands back another function's dict."""
    rows: list[tuple[str, str, str, set[str], set[str]]] = []
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
                producer = _handler_producer(node, aliases)
                if producer is None or producer not in producers:
                    continue
                where, keys, _ = producers[producer]
                rows.append((
                    f"{path.relative_to(REPO_ROOT).as_posix()}::{node.name}",
                    declared, where, keys, fields,
                ))
    return rows


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
    [pytest.param(*row, id=row[0]) for row in _delegating_routes()],
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
