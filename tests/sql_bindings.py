"""What a templated test statement is actually rendered with, read from its call site.

Plan 162 Stage X left 25 statements under ``tests/sql/`` waived against G19:
each holds a ``{placeholder}`` that only its call site can fill, and ``PREPARE``
cannot plan a template. The ledger named the repair rather than doing it --
*the bindings are derivable, since every one of them is a module-level constant
or an element of a literal tuple the module iterates* -- and this module is that
repair.

**Derived, not declared.** A table mapping statement to binding would be one
more inventory to remember, which is the shape this plan keeps deleting. The
bindings are read out of the module that owns the statement: the *shape* of the
call comes from its AST -- which keyword takes which name, and which loop or
``parametrize`` row that name is bound by -- and the *values* come from
importing the module and reading the attribute. Neither half alone is enough.
The AST cannot see ``RECEIPT_TABLE`` because it is imported rather than
assigned, and the import alone cannot say which of a module's constants reaches
which placeholder.

**Unresolvable is a real answer and stays waived.**
``insert_ops_price_observations`` builds ``{columns}`` and ``{values}`` with
``", ".join(...)`` over a list assembled per case; no static reading produces
that, and guessing would put a statement under a check that was not really
planning it. :func:`renderings` returns ``None`` there, the ledger keeps its
one entry, and the entry now describes a genuine limit rather than a backlog.

**Pairing is preserved.** ``@pytest.mark.parametrize(("schema", "table"),
POSTGRES_SNAPSHOT_TABLES)`` binds two names from one row, and expanding them
into independent lists would manufacture combinations the suite never executes
-- a ``schema`` from one row against a ``table`` from another, which would
fail to plan and read as a defect in the statement. Bindings are therefore
carried as whole rows and multiplied, never flattened.
"""
from __future__ import annotations

import ast
import importlib
import re
from functools import lru_cache
from pathlib import Path

from tests.sql_loader import SQL_ROOT

#: The name ``queries(__file__)`` is bound to at the top of a test module. It is
#: read from the assignment rather than assumed, so a module that calls it
#: something else is followed instead of silently producing no renderings.
_QUERIES_FACTORY = "queries"


def owning_module(sql_path: Path) -> str:
    """The dotted module whose statements live in *sql_path*'s directory.

    The inverse of :func:`tests.sql_loader.sql_dir_for`, and derived the same
    way: ``tests/sql/integration/ops/test_scrape/x.sql`` is owned by
    ``tests.integration.ops.test_scrape``, so a statement's path *is* the name
    of the module that renders it.
    """
    relative = sql_path.resolve().parent.relative_to(SQL_ROOT)
    return ".".join(("tests", *relative.parts))


def _literal_strings(node: ast.AST, module) -> list | None:
    """*node* as a list of rows, or ``None`` if it is not statically knowable.

    A row is a string for a single-target loop and a tuple for a
    ``parametrize`` over several names. Both a literal tuple in the source and
    a name resolved against the imported module are accepted -- the second is
    what reaches ``PROTECTED_TABLES`` and ``POSTGRES_SNAPSHOT_TABLES``, neither
    of which is assigned in the module that iterates it.
    """
    if isinstance(node, (ast.Tuple, ast.List)):
        values = [element.value for element in node.elts
                  if isinstance(element, ast.Constant)]
        return values if len(values) == len(node.elts) else None
    if isinstance(node, ast.Name):
        value = getattr(module, node.id, None)
        if isinstance(value, (tuple, list)) and value:
            return list(value)
    return None


def _targets(node: ast.AST) -> list[str] | None:
    """The names a loop target or ``parametrize`` argument spec binds.

    The two spell the same thing differently and both have to be read: a loop
    target is a ``Name`` node, while ``parametrize`` takes its argument names as
    *strings*, so ``("schema", "table")`` is a tuple of constants and not of
    names. Accepting only one shape is what made the parametrized case look
    unresolvable when its values were sitting in a module constant all along.
    """
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.Tuple, ast.List)):
        names = []
        for element in node.elts:
            if isinstance(element, ast.Name):
                names.append(element.id)
            elif isinstance(element, ast.Constant) and isinstance(element.value, str):
                names.append(element.value)
        return names if len(names) == len(node.elts) else None
    return None


def _rows_from(names: list[str], values: list) -> list[dict[str, str]] | None:
    """One binding dict per row, with *names* taken across each row together."""
    rows = []
    for value in values:
        cells = value if isinstance(value, (tuple, list)) else (value,)
        if len(cells) != len(names) or not all(isinstance(c, str) for c in cells):
            return None
        rows.append(dict(zip(names, cells)))
    return rows


def _scope_bindings(function: ast.FunctionDef, module) -> list[dict[str, str]]:
    """Every combination of names *function* binds by loop or ``parametrize``.

    Starts from one empty binding and multiplies, so a function with neither
    yields exactly one -- the statement is rendered once, with whatever its
    keywords resolve to at module level.
    """
    combinations: list[dict[str, str]] = [{}]

    for decorator in function.decorator_list:
        if not isinstance(decorator, ast.Call) or len(decorator.args) != 2:
            continue
        attribute = decorator.func
        if not (isinstance(attribute, ast.Attribute) and attribute.attr == "parametrize"):
            continue
        names = _targets(decorator.args[0])
        values = _literal_strings(decorator.args[1], module)
        rows = _rows_from(names, values) if names and values else None
        if rows:
            combinations = [{**base, **row} for base in combinations for row in rows]

    for node in ast.walk(function):
        if not isinstance(node, ast.For):
            continue
        names = _targets(node.target)
        values = _literal_strings(node.iter, module)
        rows = _rows_from(names, values) if names and values else None
        if rows:
            combinations = [{**base, **row} for base in combinations for row in rows]

    return combinations


def _statement_name(node: ast.AST, alias: str) -> str | None:
    """``SQL("insert_x")`` -> ``insert_x``, for the module's own loader alias."""
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == alias and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)):
        return node.args[0].value
    return None


def _loader_alias(tree: ast.Module) -> str | None:
    """The name this module bound ``queries(__file__)`` to, usually ``SQL``."""
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        function = node.value.func
        if isinstance(function, ast.Name) and function.id == _QUERIES_FACTORY:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                return target.id
    return None


@lru_cache(maxsize=None)
def _module_renderings(dotted: str) -> dict[str, tuple | None]:
    """Every rendering each statement in *dotted* is executed with.

    ``None`` against a name means the module renders it with something this
    cannot read -- the value is computed rather than named -- and the caller
    must treat that statement as unplannable rather than as having no bindings.
    """
    module = importlib.import_module(dotted)
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    alias = _loader_alias(tree)
    if alias is None:
        return {}

    found: dict[str, set | None] = {}
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        scopes = _scope_bindings(function, module)
        for node in ast.walk(function):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "format"):
                continue
            name = _statement_name(node.func.value, alias)
            if name is None:
                continue
            for scope in scopes:
                binding = {}
                for keyword in node.keywords:
                    value = _resolve(keyword.value, module, scope)
                    if value is None:
                        found[name] = None
                        binding = None
                        break
                    binding[keyword.arg] = value
                if binding is None:
                    break
                if found.get(name, set()) is not None:
                    found.setdefault(name, set()).add(tuple(sorted(binding.items())))
    return {name: (None if rows is None else tuple(sorted(rows)))
            for name, rows in found.items()}


def _resolve(node: ast.AST, module, scope: dict[str, str]) -> str | None:
    """One keyword's value: a loop variable, a module constant, or a literal."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in scope:
            return scope[node.id]
        value = getattr(module, node.id, None)
        if isinstance(value, str):
            return value
    return None


def holds_a_placeholder(sql: str) -> bool:
    """Whether *sql* holds a ``str.format`` placeholder, ignoring SQL literals.

    **The naive reading is ``"{" in sql``, and it was wrong seven times.**
    ``'{"makes": ["test"]}'::jsonb`` is a JSONB literal: the braces sit inside a
    quoted SQL string, ``%s`` beside them is a real bind parameter, and the
    statement plans exactly as written. Reading those as templates waived seven
    statements out of the schema check that could have been in it all along --
    a coverage hole dressed as a known limit, which is worse than an open gap
    because the ledger made it look decided.

    So string literals are removed before the search. Doubled ``''`` inside a
    literal is Postgres's own escape and stays part of the literal it sits in.
    """
    without_literals = re.sub(r"'(?:[^']|'')*'", "''", sql)
    return bool(re.search(r"\{\w*\}", without_literals))


def renderings(sql_path: Path) -> list[str] | None:
    """Every text *sql_path* actually reaches an engine as, or ``None``.

    ``None`` means the call site fills the placeholder with something no static
    reading can produce. That is the honest answer and the one case G19 still
    waives; returning an empty list instead would read as "renders nowhere",
    which is a different and much more alarming claim -- so a template nothing
    was found rendering is ``None`` too, rather than an empty list that would
    let the integration check plan nothing and still pass.
    """
    template = sql_path.read_text(encoding="utf-8")
    try:
        found = _module_renderings(owning_module(sql_path))
    except ImportError:
        return None
    bindings = found.get(sql_path.stem, ())
    if bindings is None:
        return None
    rendered = [template.format(**dict(binding)) for binding in bindings]
    if not rendered:
        return [template] if not holds_a_placeholder(template) else None
    return rendered
