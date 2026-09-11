# Plan 181: the rule readers are code

## What this plan is for

Moves eleven thousand lines of non-test code out of this repository's test
tree and into a package coverage can measure, so the helpers, readers and
analysers every test depends on are themselves unit tested.

## The case

**43% of `tests/rules/` is not tests.** Measured 2026-09-10 by walking the AST
of every module in the directory and splitting functions on whether their name
begins with `test_`:

| | count | lines |
|---|---|---|
| test functions | 225 | 5,393 |
| helper functions — the readers | **229** | **4,138** |

`test_testing_contract.py` alone carries 105 helper functions across 2,128
lines. They are AST walkers, regex classifiers, path resolvers and dataflow
analyses: `route_handlers()` resolves every FastAPI route in six services,
`_database_triggered_codes()` decides whether a handler branches to a status
code on a rowcount, `mutating_statements()` classifies 169 `.sql` files by
whether they write. Another **3,627 lines** of the same species live in sixteen
helper modules elsewhere under `tests/` — `branch_probe.py` at 510,
`response_fixtures.py` at 505, `branch_list.py` at 392.

**None of it can be measured, and the exclusion looks correct.**
`pyproject.toml` sets the coverage source to the ten production directories,
and CI runs `--cov-fail-under=75` against that list. `tests/` is not in it —
by universal convention, because you do not measure coverage of test code. So
a reader with a dead branch, an unreachable condition, or a regex arm that
never fires is invisible to every instrument this repository owns, and the
reason it is invisible is a line of configuration nobody would question.

**The floor pattern is the symptom.** Every rule in the directory carries a
"floor" — a second assertion that the reader found anything at all — because
a rule is a set difference and a set difference over an empty corpus is empty.
Plan 162 Stage AA measured that twenty-four of those floors asserted a count
against a number somebody picked, and Stage AK set out to drain them. The
draining is what exposed the cause: a floor is not a weak test chosen over a
better one, it is **the only instrument available to code that lives where
instruments do not go**. Asking "what should this reader have found" is
unit-testing it by hand, once, in somebody's head.

**Which is why converting them keeps finding defects rather than tidying
prose.** Stage AA's four conversions found four reader bugs — three `/metrics`
routes registered by a library closure and reachable by no rule, eight DAG
modules reached through `sensors.post_json` rather than `requests.post`, a URL
read from the wrong positional argument, and a docstring filter comparing
cleaned text against raw so that every docstring read as code. Stage AK found
more: `_MUTATING_VERB` matches the `FOR UPDATE` locking clause, so
`ops/sql/select_legacy_search_config.sql` — a `SELECT` that writes nothing —
carries the obligation meant for statements that mutate rows. A `>= 2` floor
over nine Compose files tolerated losing seven. `MAX_WHAT_THIS_PLAN_IS_FOR_
WAIVERS` sat at 35 against a ledger of 34, one silent append of headroom in the
ratchet whose comment says never to raise it.

**Somebody already hit this wall and improvised.**
`test_the_sql_in_python_rule_sees_every_shape_that_can_hold_a_statement` feeds
`_sql_statements_in_python` the shapes it must catch, with the note that they
are there "so that a future narrowing of this rule fails loudly rather than
quietly restoring the blind spot". That is a unit test, written inside a test
module because there was nowhere else to put one, for one reader out of 229.

**What moving them buys, and it is three things at once.** Readers in an
importable package enter the coverage source list, so dead branches become
visible and the existing ratchet applies to them. They become unit-testable
without ceremony — `assert is_mutating(text) is True` needs no fixture file, no
`SQL()` loader and no canary idiom, because the module under test is not a test
module. And the rules left in `tests/rules/` shrink toward thin assertions over
the package's output, which is the shape they should have had: most floors stop
being necessary rather than becoming better.

**Origin, recorded:** raised 2026-09-10 from Plan 162 Stage AK, which had
drained thirteen of its thirty guessed bounds when the pattern behind them
became clear. Nine of those thirteen turned out to be redundant — something
else already asserted what the number approximated — which is itself evidence
that the floors were reached for as a reflex rather than chosen. Stage AK's
remaining eleven are held pending the decision this plan asks for, because
converting a floor that this plan would delete is work done twice.

## Design

**A test module defines tests and fixtures. Everything else in it is code.**
That is the whole boundary, and it is mechanical rather than a judgement: a
module-level function whose name does not begin with `test_` and which is not
a pytest fixture belongs in an importable package, where the coverage source
list reaches it and where a unit test needs no ceremony to exist.

**Measured 2026-09-10** by walking every module under `tests/`:

| | functions | lines |
|---|---|---|
| movable — module-level, not `test_`, not a fixture | **754** | **11,616** |
| pinned — pytest fixtures | 164 | 2,054 |
| pinned — `conftest.py` | — | 72 |
| pinned — `tests/plugins/` | — | 339 |

`tests/rules/` alone holds 218 of the movable functions across 4,075 lines —
43% of that directory by line — and `tests/scripts/`, `tests/integration/` and
`tests/dbt/` hold most of the rest.

**Earlier drafts of this boundary reached for a predicate and were wrong to.**
"A function that reads a file or parses source" classifies 147 of `tests/rules`'
229 helpers, and both its error classes are instructive: the two-line
`_compose_files()` is caught, correctly, because it is the very reader whose
erosion Plan 162 Stage AK spent a day on; while `_statement_text` and
`_declared_columns` are missed, because they transform input another function
already parsed. Repairing that needs transitive closure over the call graph, or
a size threshold — and a size threshold is a chosen number in the one rule
whose purpose is to forbid chosen numbers. The mechanical boundary has neither
problem, and its exception set in `tests/rules/` is one fixture and eight
methods of two to nine lines each.

**Rejected on the way:**

* **Add `tests/` to the coverage source instead of moving anything.** Coverage
  of test code is meaningless in aggregate — test bodies are covered by being
  run — and it would corrupt the `--cov-fail-under=75` ratchet's denominator.
* **Canary tests written inside the test modules.** This is the improvisation
  already in the tree at
  `test_the_sql_in_python_rule_sees_every_shape_that_can_hold_a_statement`,
  and it is a unit test with none of the tooling: no coverage, and fixtures
  contorted through `.sql` files because the no-inline-SQL rule reaches test
  modules.
* **Move everything in one stage.** Plan 180's retirement maps delete a
  material fraction of `tests/rules/`'s analysis — the census says the HTTP
  seam alone carries "hundreds of lines of AST analysis" that its mechanisms
  would remove. Moving code scheduled for deletion is work done twice.
* **Do nothing and keep the floors.** Plan 162 Stage AK measured the cost:
  of thirteen guessed bounds drained, nine were redundant with a check that
  already existed, and the conversions that were not redundant each surfaced a
  reader defect.

**The sequencing decision, and it is the non-obvious part.** New code lands in
the package from the day the boundary rule exists; legacy code moves in
stages. This matters against [Plan 180](plan_180_seam_program.md) specifically:
its Stage B writes a border guard per seam, each one a new reader with a new
floor, so the directory those are written into is decided by whether this
plan's Stage A has landed. Plan 180 already puts its *mechanisms* in `shared/`,
inside the coverage source — this plan is the same instinct applied to the
checking half rather than the derivation half.

**One risk to settle in Stage A rather than discover later.** A new top-level
package interacts with `service_packages()`, `production_python_files()` and
the "enough" table in `docs/TESTING.md`. If it reads as a service package it
inherits obligations — a coverage row, the no-inline-SQL rule — that may or may
not be wanted. Its classification is Stage A's first work.

## Stages

### Stage A: a test module defines tests, and the rest is a package

**Issue:** CAR-127 · **State:** `next`

Creates the package, settles its classification against `service_packages()`
and the "enough" table, moves a first group, and lands the rule that fills it.

**Exit:** the package is importable and named in `[tool.coverage.run] source`,
with coverage reporting a figure for it; a module-level non-test function
defined under `tests/` fails, with pytest fixtures, `conftest.py` and
`tests/plugins/` the named exceptions; the ledger seeded at the 754 functions
still outside. Demonstrated by a new helper added to a test module failing,
and by a dead branch in moved code reported uncovered.

### Stage B: a rule and its reader retire together

**Issue:** CAR-128 · **State:** `—`

Plan 180's expiry meta-rule — *a scaffolding rule whose named ledger has
emptied fails until it is deleted* — knows only about assertions. Once a rule's
reader lives in the package, retirement has two halves and deleting one leaves
the other as dead code no ledger describes.

**Exit:** the expiry rule fails a rule whose assertion is deleted while its
reader remains in the package. Demonstrated by deleting one assertion and
leaving its reader.

### Stage C: the repository-wide analysers move

**Issue:** CAR-129 · **State:** `—`

`tests/rules/`, `tests/scripts/`, `tests/dbt/` and the top level — 494
functions across 8,533 lines, the AST walkers, classifiers and resolvers that
read the whole repository. Each arrives with unit tests.

Ordering note: where Plan 180's retirement maps schedule a reader for deletion,
it is left in place rather than moved and unit tested, and leaves with its rule.

**Exit:** every repository-wide analyser outside Plan 180's retirement map
lives in the package with unit tests; Stage A's ledger is down to the
per-service helpers and the 180-scheduled readers only.

### Stage D: the per-service test helpers move

**Issue:** CAR-130 · **State:** `—`

`tests/integration/`, `tests/archiver/`, `tests/ops/` and the rest — 260
functions across 3,083 lines of builders, resolvers and assertions shared
between a service's test modules.

**Exit:** Stage A's ledger reaches 0 and the boundary rule stops being
scaffolding — it has no ledger left to drain and becomes a permanent guard on
where code is written.

## The order

| # | Stage | Est. | Production-gated exit |
|---|---|---|---|
| 1 | A | 2 | no |
| 2 | B | 2 | no |
| 3 | C | 3 | no |
| 4 | D | 3 | no |
