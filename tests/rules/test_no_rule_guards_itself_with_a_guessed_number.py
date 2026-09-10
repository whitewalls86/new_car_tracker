"""A rule's floor may assert nothing, or everything -- never a number in between.

Plan 162 Stage AA, gap G32.

**Every rule in this directory is a set difference, and a set difference over
an empty corpus is empty.** So each carries a floor: a second assertion that
the reader still found something. The floor is what makes the rule's failure
mode loud, because the rule itself passes when its reader breaks.

**A floor written as `>= 80` is the same guess the rule exists to forbid.** It
catches the corpus collapsing and misses it eroding, and the boundary between
those moves every time the repository grows. This stage wrote one and then
watched it fail both ways in a single sitting: `>= 80` caught an operation-id
bug at 73 of 93, and the `>= len(rows) - 5` that replaced it would have waved
the same bug through. Loosening the number to fit the tree is how a rule stops
being a rule.

**So a floor may say two things.** That the corpus is *non-empty*, which is a
fact about nothing in particular and needs no number. Or that it equals
something *derived* -- every service with a contract is represented, every
declaration resolves to a handler, every producer the sibling rule found is
present. Anything between those is a number somebody picked, and it will be
wrong later without anyone noticing.

**Making the four floors in this stage exact found four reader bugs**, none of
which a threshold would have surfaced: three `/metrics` routes registered by a
library closure rather than by this repository, eight DAG modules reached
through `post_json` rather than `requests.post`, a URL passed as the second
argument rather than the first, and a docstring filter comparing
`ast.get_docstring`'s cleaned text against `Constant.value`'s raw text so that
every docstring read as code. Exactness is not tidiness; it is the only setting
at which a floor reports anything.

**The ledger is the work.** Twenty-odd floors predate this rule, and each is
either an exact invariant nobody has written down yet or a number that should
never have been one. They are listed rather than exempted, and an entry that
stops describing a guess fails until it is deleted.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RULES = REPO_ROOT / "tests" / "rules"

# `> 0` and `>= 1` say "the corpus is not empty", which is the one bound that
# is not a guess: it names no size, so no growth can make it stale. `!= 0` is
# the same statement spelled differently.
_NON_EMPTY = {(ast.Gt, 0), (ast.GtE, 1), (ast.NotEq, 0)}

_INEQUALITIES = (ast.Gt, ast.GtE, ast.Lt, ast.LtE)

#: Parsed once per module path -- `test_testing_contract.py` alone imports from
#: several, and this reader runs at collection time.
_IMPORTED_CONSTANTS: dict[str, dict[str, int]] = {}


def _is_int(node: ast.AST) -> bool:
    """An int literal, and not ``True``/``False``, which `isinstance` calls int."""
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, int)
        and not isinstance(node.value, bool)
    )


def _module_constants(tree: ast.Module) -> dict[str, int]:
    """Module-level names bound to an int literal, in one file."""
    found: dict[str, int] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        else:
            continue
        if not _is_int(value):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                found[target.id] = value.value
    return found


def _constants_in_scope(tree: ast.Module) -> dict[str, int]:
    """Every int constant a module can name -- its own, and the ones it imports.

    ``_SQL_CORPUS_FLOOR = 100`` above ``assert len(corpus) > _SQL_CORPUS_FLOOR``
    says exactly what ``> 100`` says. Giving a guess a name does not stop it
    being one, and the name is the more dangerous form, because the number is
    no longer on the line the reader is reading.

    Imports are followed one level and repo-local: ``DECLARED_SKIP_CEILING``
    crosses a module boundary to reach its assertion. Package ``__init__``
    re-exports and aliased module paths are not resolved, because no bound in
    this directory arrives that way -- and a resolver that guessed at the rest
    would be the thing this rule forbids.
    """
    found = dict(_module_constants(tree))
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module is None or node.level:
            continue
        if node.module not in _IMPORTED_CONSTANTS:
            source = REPO_ROOT.joinpath(*node.module.split(".")).with_suffix(".py")
            try:
                imported = _module_constants(
                    ast.parse(source.read_text(encoding="utf-8"))
                )
            except (OSError, SyntaxError):
                imported = {}
            _IMPORTED_CONSTANTS[node.module] = imported
        for alias in node.names:
            if alias.name in _IMPORTED_CONSTANTS[node.module]:
                found[alias.asname or alias.name] = _IMPORTED_CONSTANTS[node.module][
                    alias.name
                ]
    return found


def _table_bound_names(tree: ast.Module, constants: dict[str, int]) -> set[str]:
    """Loop variables that a literal table unpacks a chosen number into.

    ``for waivers, ceiling, ... in ((WAIVERS, MAX_WAIVERS, ...), ...)`` moves
    the number one hop further from its assertion without changing what it is,
    and the assertion then reads ``<= ceiling``, which looks derived.

    Only literal tables of literal rows are followed. A loop over anything
    computed is iterating over a value that came from somewhere, which is the
    shape a floor is allowed to have.
    """
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.For) or not isinstance(node.target, ast.Tuple):
            continue
        if not isinstance(node.iter, (ast.Tuple, ast.List)):
            continue
        for row in node.iter.elts:
            if not isinstance(row, (ast.Tuple, ast.List)):
                continue
            for target, element in zip(node.target.elts, row.elts):
                if not isinstance(target, ast.Name):
                    continue
                if _is_int(element) or (
                    isinstance(element, ast.Name) and element.id in constants
                ):
                    names.add(target.id)
    return names


def _guessed_bounds() -> list[str]:
    """Every assertion in this directory bounded by a number somebody chose."""
    found: list[str] = []
    for path in sorted(RULES.glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError:
            # Reported by `test_every_rule_file_is_read`, not raised here. This
            # runs inside a `parametrize`, so raising makes an unparseable
            # sibling a *collection* error for this whole module -- pytest
            # exits 4, no assertion runs, and the harness records the mutation
            # for the floor as unnoticed rather than caught. Which is exactly
            # what it did.
            continue
        relative = path.relative_to(REPO_ROOT).as_posix()
        constants = _constants_in_scope(tree)
        table_bound = _table_bound_names(tree, constants)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assert):
                continue
            for comparison in ast.walk(node.test):
                if not isinstance(comparison, ast.Compare):
                    continue
                for operator, right in zip(comparison.ops, comparison.comparators):
                    if not isinstance(operator, _INEQUALITIES + (ast.NotEq,)):
                        continue
                    # `x >= len(rows) - 5`: derived, then slackened. The slack
                    # is the guess, and it is the shape that replaced a working
                    # floor in this very stage.
                    if (
                        isinstance(right, ast.BinOp)
                        and isinstance(right.op, (ast.Sub, ast.Add))
                        and _is_int(right.right)
                    ):
                        found.append(f"{relative}: {ast.unparse(comparison)}")
                        continue
                    # A name that resolves to an int literal is the same guess
                    # spelled at a distance. The non-empty exemption still
                    # applies to it: a constant that *is* 0 asserts nothing
                    # about size, whatever it is called.
                    if isinstance(right, ast.Name):
                        if right.id in table_bound or (
                            right.id in constants
                            and (type(operator), constants[right.id]) not in _NON_EMPTY
                        ):
                            found.append(f"{relative}: {ast.unparse(comparison)}")
                        continue
                    if not _is_int(right):
                        continue
                    if (type(operator), right.value) in _NON_EMPTY:
                        continue
                    found.append(f"{relative}: {ast.unparse(comparison)}")
    return sorted(set(found))


# Keyed on the file and the expression, never the line. A line number is the
# most fragile key a ledger can carry -- every edit above one of these rots it,
# and a rotted waiver either grandfathers a bound nobody chose or silently
# stops covering the one it named.
#
# Seeded 2026-09-09 from this rule's own first run. **The list is the work**:
# nothing else records which floors have been made exact, and an entry that
# stops describing a guessed bound fails until it is deleted, so the ledger
# cannot lag the repair.
#
# Each is one of two things. Some are exact invariants nobody has written down
# -- `len(_compose_files()) >= 2` means "both compose files are present", which
# is a set the repository can name. The rest are numbers chosen to sit below
# whatever the count was that day, and those stop meaning anything the moment
# the tree grows past them.
GUESSED_BOUND_WAIVERS: tuple[str, ...] = (
    "tests/rules/test_env_example_wiring.py: len(_compose_files()) >= 2",
    "tests/rules/test_env_example_wiring.py: len(documented_keys()) >= 20",
    "tests/rules/test_env_example_wiring.py: len(interpolated_variables()) >= 20",
    "tests/rules/test_external_vocabularies.py: len(observed) >= 2",
    "tests/rules/test_image_keep_set.py: len(_compose_files()) >= 2",
    "tests/rules/test_maintenance_running_set.py: len(reason) > 40",
    "tests/rules/test_planning_docs.py: len(found) > 50",
    "tests/rules/test_planning_docs.py: len(gap_claims()) >= 10",
    "tests/rules/test_planning_docs.py: len(gap_entries()) >= 10",
    "tests/rules/test_planning_docs.py: found > 200",
    "tests/rules/test_testing_contract.py: len(found) > 250",
    "tests/rules/test_testing_contract.py: len(matched) > 2",
    "tests/rules/test_testing_contract.py: len(entries) >= 50",
    "tests/rules/test_testing_contract.py: len(anchored) >= 40",
    "tests/rules/test_testing_contract.py: len(found) >= 100",
    "tests/rules/test_testing_contract.py: len(statements) >= 30",
    "tests/rules/test_testing_contract.py: len(resolved) >= 20",
    "tests/rules/test_testing_contract.py: len(types_found) >= 4",
    "tests/rules/test_testing_contract.py: len(handlers) >= 80",
    "tests/rules/test_testing_contract.py: pairs >= 150",
    "tests/rules/test_testing_contract.py: len(covered) >= 75",
    "tests/rules/test_testing_contract.py: len(ambiguous) <= 8",
    "tests/rules/test_testing_contract.py: len(requests) >= 100",
    "tests/rules/test_testing_contract.py: len(found) >= 4",
    # Added 2026-09-10, Stage AK, when this reader learned to resolve a name.
    # The seed above was 24 because the reader only saw int *literals*, so a
    # guess survived it by being given a name -- which is the more dangerous
    # form, since the number is then not on the line anyone is reading. Six
    # more, one from each depth the resolution now reaches: a constant in the
    # same file (`_SQL_CORPUS_FLOOR = 100`, `_DB_VOCABULARY_FLOOR = 10`,
    # `UNDELIVERED_CEILING = 1`, `UNDOCUMENTED_CEILING = 4`), a constant
    # imported from another module (`DECLARED_SKIP_CEILING = 2`), and one a
    # literal loop table unpacks into a variable that then reads as derived
    # (`ceiling`, from `MAX_WHAT_THIS_PLAN_IS_FOR_WAIVERS` and
    # `MAX_THE_CHECKS_WAIVERS`).
    "tests/rules/test_env_example_wiring.py: len(UNDELIVERED) <= UNDELIVERED_CEILING",
    "tests/rules/test_env_example_wiring.py: len(UNDOCUMENTED) <= UNDOCUMENTED_CEILING",
    "tests/rules/test_planning_docs.py: len(waivers) <= ceiling",
    "tests/rules/test_testing_contract.py: len(DECLARED_SKIPS) <= DECLARED_SKIP_CEILING",
    "tests/rules/test_testing_contract.py: len(corpus) > _SQL_CORPUS_FLOOR",
    "tests/rules/test_testing_contract.py: len(owners) >= _DB_VOCABULARY_FLOOR",
)


def test_every_rule_file_is_read():
    """The floor under the rule below, held to the rule below's own standard.

    Not "at least twenty files" -- every `.py` in this directory parses and is
    walked. A file this cannot parse is a file whose floors go unexamined, and
    the number of rule modules is not something to guess about when the
    directory itself says.
    """
    files = sorted(RULES.glob("*.py"))
    assert files, f"{RULES} holds no rule modules; this rule reads nothing."

    unreadable = []
    for path in files:
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            unreadable.append(f"{path.name}: {exc}")
    assert not unreadable, (
        f"these rule modules could not be parsed, so their floors were not "
        f"examined: {unreadable}"
    )


def test_every_waiver_names_a_bound_that_still_exists():
    """A waiver for a repaired floor grandfathers nothing and hides the next.

    The same clause the other ledgers in this directory carry, and the reason
    this one is keyed on an expression rather than a line: an entry that stops
    describing a guess has to fail, and it cannot do that if the key moved for
    a reason unrelated to the bound.
    """
    stale = sorted(set(GUESSED_BOUND_WAIVERS) - set(_guessed_bounds()))
    assert not stale, (
        f"these waivers name bounds that are no longer guessed: {stale}. "
        f"Either the floor was made exact and the entry should go, or the "
        f"assertion moved and the entry now covers nothing."
    )


@pytest.mark.parametrize("bound", _guessed_bounds() or [None])
def test_no_rule_guards_itself_with_a_guessed_number(bound):
    """A floor is a promise that the rule above it measured something.

    Kept exactly, or not kept. The repair is to ask what the count *should* be
    and assert that -- and where the code makes an exact answer impossible, to
    change the code, which is how the three `/metrics` routes came to be
    declared in their own services rather than by a library closure.
    """
    if bound is None:
        return
    assert bound in GUESSED_BOUND_WAIVERS, (
        f"{bound}\n\nA floor may assert the corpus is non-empty, or that it "
        f"equals something derived. A bound in between is a number that was "
        f"true the day it was written."
    )
