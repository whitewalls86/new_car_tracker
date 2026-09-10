"""Which service a caller talks to, and what that service can answer.

Plan 162 Stage AA, gap G32.

**Ownership is derived from ``docker-compose.yml``, not declared.** A service
this repository builds has a ``build.dockerfile: <package>/Dockerfile``, and a
package with a committed contract can be asked what it returns. Everything else
is somebody else's: a compose service with an ``image:`` and no ``build:`` is
third-party infrastructure, and a host that is not a compose service at all is
external. Those two are Stage AB's, and routing them there is what stops this
stage's rule accusing a test of fabricating a `cars.com` response it has no way
to check.

**The mapping is not identity, which is why it has to be read.** ``pack-worker``
and ``snapshot-worker`` both build ``archiver/Dockerfile`` and
``april-processor`` builds ``processing/Dockerfile``, so three of the seventeen
compose services answer for a package under another name. A rule keyed on
``<NAME>_URL`` matching a package would have got all three wrong.

**A caller's candidate endpoints are a union, deliberately.** One
``requests.get`` seam in a module can reach several endpoints --
``ops/coordination_drain.py`` asks five services for ``/ready`` through one
call site, choosing the host from a table at runtime. Rather than guess which,
this reports every host the module names and every path it builds, and the rule
above it asks whether a fabricated body is answerable by *any* of them. That is
the permissive direction on purpose: a fabrication this cannot place is left
alone rather than reported against the wrong endpoint.
"""
from __future__ import annotations

import ast
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = REPO_ROOT / "contracts"

_URL_HOST = re.compile(r"https?://([a-zA-Z0-9_.-]+)")
_VERBS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})


@lru_cache(maxsize=1)
def _compose() -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def owned_hosts() -> dict[str, str]:
    """Compose service name to the package that answers for it, where we build it."""
    owned: dict[str, str] = {}
    for name, service in (_compose().get("services") or {}).items():
        build = service.get("build")
        if not isinstance(build, dict):
            continue
        dockerfile = build.get("dockerfile") or ""
        if "/" not in dockerfile:
            continue
        package = dockerfile.split("/")[0]
        if (CONTRACTS / f"{package}.json").is_file():
            owned[name] = package
    return owned


@lru_cache(maxsize=1)
def third_party_hosts() -> frozenset[str]:
    """Compose services this repository runs but does not build."""
    return frozenset(
        name for name, service in (_compose().get("services") or {}).items()
        if not isinstance(service.get("build"), dict)
    )


@lru_cache(maxsize=None)
def contract(package: str) -> dict[str, Any]:
    return json.loads((CONTRACTS / f"{package}.json").read_text(encoding="utf-8"))


def _resolve(schema: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    ref = schema.get("$ref")
    if not ref:
        return schema
    return spec["components"]["schemas"][ref.rsplit("/", 1)[-1]]


def responses(package: str) -> dict[tuple[str, str], dict[str, dict[str, Any]]]:
    """``(verb, path)`` to ``{status code: resolved JSON schema}`` for *package*."""
    spec = contract(package)
    out: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for path, operations in spec["paths"].items():
        for verb, operation in operations.items():
            if verb not in _VERBS:
                continue
            bodies: dict[str, dict[str, Any]] = {}
            for code, response in (operation.get("responses") or {}).items():
                schema = ((response.get("content") or {})
                          .get("application/json") or {}).get("schema")
                if schema is not None:
                    bodies[code] = _resolve(schema, spec)
            out[(verb.upper(), path)] = bodies
    return out


def _path_of(expression: str) -> str | None:
    """The path an outbound URL expression builds, with parameters normalised."""
    tail = expression.split("}", 1)[-1] if "{" in expression else expression
    match = re.search(r"(/[A-Za-z0-9_\-{}/.]*)", tail)
    if not match:
        return None
    path = match.group(1).split("?")[0].rstrip("'\"")
    return re.sub(r"\{[^}]+\}", "{}", path) or None


@lru_cache(maxsize=None)
def caller_endpoints(module: str) -> tuple[tuple[str, str, str], ...]:
    """Every ``(package, verb, path)`` *module* can call, as a union.

    ``module`` is a repository-relative path. Hosts come from every URL literal
    the module names -- including the ones inside a dispatch table, which is
    how ``coordination_drain`` picks between five services at runtime -- and
    paths from the expressions handed to ``requests``.
    """
    path = REPO_ROOT / module
    if not path.is_file():
        return ()
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))

    hosts = {host for host in _URL_HOST.findall(source) if host in owned_hosts()}
    packages = {owned_hosts()[host] for host in hosts if host in owned_hosts()}
    if not packages:
        return ()

    found: set[tuple[str, str, str]] = set()
    for node in ast.walk(tree):
        # Any call whose first argument builds a URL, not only
        # `requests.post(...)`. The DAGs reach every service through
        # `sensors.post_json`, so a reader keyed on the client's own verb
        # methods resolved eight DAG modules to nothing and reported them as
        # calling no endpoint at all -- found by this rule's own floor, which
        # asks that every module naming an owned host resolve to one.
        if not isinstance(node, ast.Call) or not node.args:
            continue
        # Every positional argument, not the first. `pack_bronze_html` reaches
        # pack-worker through `_post_result(context, url, payload, timeout)`,
        # so a reader keyed on `args[0]` saw a DAG context object and resolved
        # the module to nothing. A path only counts if it equals one a contract
        # declares, so widening the search cannot invent a match.
        routes = {
            candidate for candidate in (
                _path_of(ast.unparse(argument)) for argument in node.args
            ) if candidate is not None
        }
        if not routes:
            continue
        # The verb comes from the callee's name where it says one -- `.post`,
        # `post_json` -- and is otherwise left open, because a helper that does
        # not name its method still names its path, and this set is consumed as
        # a union of candidates rather than as a single answer.
        called = getattr(node.func, "attr", None) or getattr(node.func, "id", "") or ""
        named = {verb for verb in _VERBS if verb in called.lower().split("_")}
        for package in packages:
            for (declared_verb, declared_path) in responses(package):
                if named and declared_verb.lower() not in named:
                    continue
                normalised = re.sub(r"\{[^}]+\}", "{}", declared_path)
                if normalised in routes:
                    found.add((package, declared_verb, declared_path))
    return tuple(sorted(found))


def body_violations(
    body: dict[str, Any], candidates: tuple[tuple[str, str, str], ...],
) -> list[str]:
    """Why *body* is answerable by none of *candidates*, or an empty list.

    Two checks, and both are about what the callee *cannot* say. A key no
    candidate declares is a key the caller will never receive, and a value a
    schema pins with ``const`` is one the callee cannot vary -- which is how
    ``container_health``'s ``known`` reaches here, pinned ``True`` because
    neither of its routes has a code path that emits anything else.
    """
    if not candidates:
        return []
    reasons: list[str] = []
    for package, verb, path in candidates:
        schemas = responses(package).get((verb, path), {})
        for code, schema in sorted(schemas.items()):
            properties = schema.get("properties") or {}
            if not properties:
                continue
            unknown = sorted(set(body) - set(properties))
            pinned = [
                f"{key}={body[key]!r} but the schema pins {properties[key]['const']!r}"
                for key in body
                if key in properties and "const" in properties[key]
                and body[key] != properties[key]["const"]
            ]
            if not unknown and not pinned:
                return []
            reasons.append(
                f"{package} {verb} {path} {code}: "
                + "; ".join(
                    ([f"declares no {unknown}"] if unknown else []) + pinned
                )
            )
    return reasons


_JSON_ZEROS: dict[str, Any] = {
    "string": "", "integer": 0, "number": 0.0, "boolean": False,
    "array": [], "object": {},
}


def _from_schema(schema: dict[str, Any], spec: dict[str, Any]) -> Any:
    """A value satisfying *schema*, with every declared property present."""
    schema = _resolve(schema, spec)
    if "const" in schema:
        return schema["const"]
    if schema.get("enum"):
        return schema["enum"][0]
    for option in schema.get("anyOf") or schema.get("oneOf") or []:
        resolved = _resolve(option, spec)
        if resolved.get("type") != "null":
            return _from_schema(resolved, spec)
        return None
    kind = schema.get("type")
    if kind == "object":
        properties = schema.get("properties") or {}
        return {
            name: _from_schema(sub, spec) for name, sub in properties.items()
        }
    if kind == "array":
        # The zero for an array, the way "" is the zero for a string. A nested
        # array is a value the callee may well send empty, and filling it with
        # a specimen made an empty-collection test read as a populated one.
        # The top-level case is different and `service_response` handles it.
        return []
    return _JSON_ZEROS.get(kind)


def _override(built: dict[str, Any], overrides: dict[str, Any], where: str) -> dict[str, Any]:
    """*built* with *overrides* applied, refusing any key the callee never sends."""
    unknown = sorted(set(overrides) - set(built))
    if unknown:
        raise KeyError(
            f"{where} declares no {unknown}. Overriding a key the callee does "
            f"not send is asserting on a value the caller cannot receive."
        )
    return {**built, **overrides}

def service_response(
    package: str, verb: str, path: str, code: str = "200", **overrides: Any,
) -> dict[str, Any]:
    """The body *package* answers on ``verb path`` with *code*, from its contract.

    The HTTP counterpart of ``tests/response_fixtures.py``'s ``fixture_for``,
    and it exists for the same reason: a test that writes another service's
    response down by hand passes for whatever its author typed, and goes on
    passing when that service changes. ``contracts/`` is generated from the
    running apps, so a body built from it is a body the callee can produce --
    and stops being buildable the moment the callee stops producing it.
    """
    spec = contract(package)
    schemas = responses(package)
    key = (verb.upper(), path)
    if key not in schemas:
        raise KeyError(
            f"{package} serves no {verb.upper()} {path}. A test fabricating a "
            f"response for a route the contract does not carry is a test of a "
            f"call that 404s -- which is the defect, not the fixture."
        )
    if code not in schemas[key]:
        raise KeyError(
            f"{package} {verb.upper()} {path} declares no {code}; it declares "
            f"{sorted(schemas[key])}. A caller cannot receive a code the callee "
            f"does not answer with."
        )
    where = f"{package} {verb.upper()} {path} {code}"
    schema = _resolve(schemas[key][code], spec)
    if schema.get("type") == "array":
        # A body that IS an array carries its shape only in its elements, so an
        # empty list here asserts nothing at all -- which is how four fabricated
        # `Job` bodies in tests/airflow/test_scrape_listings.py sat at a checked
        # seam without the check ever reaching them. One specimen, and the
        # overrides land on it, because a caller asserting on a collection is
        # asserting on what is in it.
        items = schema.get("items")
        element = _from_schema(items, spec) if items else None
        if not isinstance(element, dict):
            return [] if element is None else [element]
        return [_override(element, overrides, where)]
    built = _from_schema(schema, spec)
    if isinstance(built, list):
        # An array body stays an array. Wrapping it in a `detail` key -- which
        # is what this did -- invents FastAPI's error key for a route that has
        # never sent it, so the helper against which fabrications are checked was
        # itself fabricating. Overrides apply to the element, because a caller
        # asserting on a collection is asserting on what is in it.
        if not built:
            return _override({}, overrides, where) if overrides else []
        return [_override(built[0], overrides, where)]
    if not isinstance(built, dict):
        return built
    return _override(built, overrides, where)
