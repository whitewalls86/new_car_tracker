"""Every status code a route declares is one the standard names.

Plan 162 Stage AL, gap G33 — the vocabulary's outer boundary, and the
stage exit's first demonstration: a route declaring a code the standard
does not name must fail.

**The meaning rule beside this one governs the codes the envelope splits;
this rule governs which codes exist at all.** Without it the envelope
polices the meanings of the codes it knows while a new code walks in
ungoverned — a route declaring 418 or 429 today passes every seam rule,
because the retyping rule reads call-site literals, the meaning rule reads
ambiguous codes, and nothing reads the declaration against the table that
is supposed to own it.

**The table is the owner and is parsed, not restated.** `docs/TESTING.md`
§*What a status code means* fixes the vocabulary — eleven codes, with
`202`, `429` and `Retry-After` deliberately absent and recorded as
considered — and this rule reads those rows the way the contract-structure
tests read every other table there: the parser asserts what it expected to
find and fails by name when the section moves, so this file cannot edit
its own input.

**Found a live defect on its first run, seeded rather than waived
quietly:** `ops GET /admin` declares `307`, the code the table names only
to refuse — *"`308` … is not `307`, which promises the opposite."*
`/info` got exactly this repair in Stage AA; `/admin` never did. Changing
the answered code is wire behaviour, so the entry names its repair and
waits for the deploy that makes it.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache

from tests.rules.test_testing_contract import REPO_ROOT
from tests.rules.test_the_refusal_envelope_is_one_declaration import (
    SHARED,
    declared_refusals,
)

CONTRACTS = REPO_ROOT / "contracts"
CONTRACT_DOC = REPO_ROOT / "docs" / "TESTING.md"

_VERBS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})


@lru_cache(maxsize=1)
def standard_codes() -> frozenset[int]:
    """The eleven codes §*What a status code means* fixes, parsed from it."""
    text = CONTRACT_DOC.read_text(encoding="utf-8")
    sections = re.split(r"^#### ", text, flags=re.M)
    owning = [s for s in sections if s.startswith("What a status code means")]
    assert len(owning) == 1, (
        f"{CONTRACT_DOC} no longer has exactly one '#### What a status code "
        f"means' section; this rule reads the standard from it and cannot."
    )
    codes = frozenset(
        int(match) for match in re.findall(r"^\| `(\d{3})` \|", owning[0], re.M)
    )
    assert codes, (
        "the code table parsed to nothing; the standard moved under this "
        "reader and every declaration below is being judged against an "
        "empty vocabulary."
    )
    return codes


def declared_codes() -> dict[int, set[str]]:
    """Every code any contract declares, to the routes declaring it."""
    found: dict[int, set[str]] = {}
    for file in sorted(CONTRACTS.glob("*.json")):
        spec = json.loads(file.read_text(encoding="utf-8"))
        for route, operations in sorted(spec["paths"].items()):
            for verb, operation in operations.items():
                if verb not in _VERBS or not isinstance(operation, dict):
                    continue
                for code in (operation.get("responses") or {}):
                    if code.isdigit():
                        found.setdefault(int(code), set()).add(
                            f"{file.stem} {verb.upper()} {route} {code}"
                        )
    return found


# Keyed on service-verb-route-code. One entry, a live defect with its
# repair named: `/admin` answers its success as a temporary redirect where
# the standard commits to 308 — the same repair `/info` received in Stage
# AA (`status_code=308` on the decorator). Changing the answered code is
# wire behaviour, so it is recorded here and drained by that deploy, not
# by this session.
UNNAMED_CODE_LEDGER: tuple[str, ...] = (
    "ops GET /admin 307",
)


def test_every_declared_code_is_one_the_standard_names():
    """A code outside the table is a word outside the vocabulary.

    Both directions against the ledger: a new out-of-standard declaration
    fails on arrival, and a repaired one fails until its entry is deleted.
    """
    outside = {
        entry
        for code, entries in declared_codes().items()
        if code not in standard_codes()
        for entry in entries
    }
    ledgered = set(UNNAMED_CODE_LEDGER)

    unwaived = sorted(outside - ledgered)
    assert not unwaived, (
        "these routes declare a status code the standard does not name:\n  "
        + "\n  ".join(unwaived)
        + "\n\nUse a code from docs/TESTING.md's table, or argue the table "
        "into naming the new one — in that document, with the meaning a "
        "caller can rely on, never here."
    )

    stale = sorted(ledgered - outside)
    assert not stale, (
        "these ledger entries name declarations that are now inside the "
        "standard and must be deleted:\n  " + "\n  ".join(stale)
    )


def test_every_standard_code_is_declared_somewhere():
    """The reverse direction: a code nothing declares is dead vocabulary.

    The same clause `db_vocabularies`' pair carries — a vocabulary naming
    no `CHECK` fails — because a table row no route uses is a meaning
    nobody can rely on anybody honouring, and it goes stale invisibly.
    Clean today: all eleven are declared.
    """
    undeclared = sorted(standard_codes() - set(declared_codes()))
    assert not undeclared, (
        f"the standard names {undeclared} and no route declares them. "
        f"Either a route should, or the table row should leave with its "
        f"reasoning recorded — in docs/TESTING.md, not by this list "
        f"quietly disagreeing with it."
    )


def test_the_envelope_stays_inside_the_standard():
    """The envelope's literals are members of the table, every one.

    The envelope is the standard's importable copy, and a copy may not
    grow a word its owner does not have — the same direction the
    `container_health` equality rule holds one file over.
    """
    outside = sorted(
        f"{name} = {code}"
        for name, (code, _meaning) in declared_refusals(SHARED).items()
        if code not in standard_codes()
    )
    assert not outside, (
        f"shared/api_envelope.py declares {outside} and the standard's "
        f"table does not name those codes; the copy has outgrown its owner."
    )
