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
                        and isinstance(right.right, ast.Constant)
                        and isinstance(right.right.value, int)
                    ):
                        found.append(f"{relative}: {ast.unparse(comparison)}")
                        continue
                    if not (
                        isinstance(right, ast.Constant)
                        and isinstance(right.value, int)
                        and not isinstance(right.value, bool)
                    ):
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
