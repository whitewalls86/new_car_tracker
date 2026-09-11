# Plan 182: what a unit owes itself

## What this plan is for

Gives every unit of code a stated obligation to itself: its branches are
exercised, the outcomes it declares are the ones it produces, and the
analysers that check everything else are measured code. The second frame
beside Plan 180's seams, for what no boundary can see.

## The case

**The seam frame is two parties by definition, and this repository has no
frame for one.** [Plan 180](plan_180_seam_program.md) gives every boundary in
the system one declared way across it, and
[the seam census](../planning/seam_rule_census.md) says of its own scope that
the meta-class is "deliberately not forced into the seam template" because it
does not fit. What a service owes another service, what code owes the
database, what CI owes production: each of those is a seam and each now has a
grade. What a module owes its own caller, whether the code inside one party
is right, has no section, no grade, no statement of what evidence discharges
it, and no plan.

**The obligations exist. They are scattered across other kinds because no
kind holds them.** [The artifact obligation index](../planning/artifact_obligation_index.md),
built 2026-09-11 by joining every rule in `tests/rules/` to the corpus it
reads, found that every obligation under *a service* crosses a boundary, and
that the within-party ones are filed elsewhere:

- The "enough" floor's third clause, *every failure branch another service's
  behaviour depends on*, is judgement and has been since Plan 161 wrote it.
- The coverage ratchet is one repository-wide number. Plan 162 Stage AJ
  measured `ops/coordination_release.py` at 72% with its three response
  parsers at 1 of 12, 1 of 16 and 1 of 10 statements executed, riding inside
  a repository at 78% against a floor of 75. The only within-party instrument
  the contract asserts is the one that cannot see a file-shaped hole.
- Stage Y's rules, that a handler observes its effects, does not swallow a
  write, and does not discard an outcome it asked for, are filed under seams
  by the census. They are really *a function owes its caller an honest
  answer*, which is within-party.
- Fourteen of 23 production functions that parse a response from a system
  this repository does not own are executed by no test, per Stage AJ's
  measurement. That stage is now Plan 180 Stage L.
- 298 unit-test fixtures build a row in memory that the database would
  reject, per Stage AD's measurement. That stage is now Plan 180 Stage J.
- [Plan 181](plan_181_rule_readers_are_code.md) found 754 module-level
  functions across 11,616 lines under `tests/` that are code rather than
  tests, living where the coverage source list does not reach, so a reader
  with a dead branch is invisible to every instrument the repository owns.
- All three remaining judgement rules in [`docs/TESTING.md`](../TESTING.md)
  are within-party: the thing under test is not the thing mocked, the
  assertion is meaningful, the failure branch matters. They are one statement
  seen from three sides.

Stages L and J sit in Plan 180 because they fell out of Plan 162's tail while
that plan was being split, not because either is a seam. Plan 181 says of
itself that it is "the same instinct applied to the checking half rather than
the derivation half". All three are instances of a frame nobody has named.

**The frame already exists once, finished, for a different language.**
Plan 162 Stage S is to a dbt model exactly what this plan is to a Python
module. The branch list is derived from the compiled SQL rather than
maintained, every branch is exercised in both arms by a unit test, every model
materialises at least one row with no waiver list, and every declared
constraint is shown load-bearing by mutating the guard that produces it or is
recorded as decorative. It measured 295 of 295 branches covered and 15 of 161
constraints load-bearing, and on the way it found a `listing_state` bug that
had been wrong for as long as the model existed while `dbt build` stayed
green. Nothing in that design is dbt-specific.

**The instruments for Python are the same three, and each already has a
precedent here.** Branch coverage per module rather than a global average,
which is what Stage AJ's measurement was taken with. Outcome types derived
from shape, which Stage Y already does for the outcome-discarded rule, so that
"every outcome a function declares is one some test produces" is a derived
set difference rather than a list. And mutation testing as the answer to
whether an assertion is load-bearing, which is the decorative-versus-load-
bearing split Stage S made for constraints. Plan 162 Stage AF argued that a
mutation generator cannot say what *a rule* proves, and that objection was
right for rules and does not apply here: for service code the question is only
whether the suite is sensitive to the module, which is precisely what a
generator answers.

**The method is Stage X's, and it is what makes the judgement rules
strikeable.** That stage removed a judgement rule by moving 505 SQL literals
out of test files rather than by teaching the checker to tell a seed from a
paraphrase. "The thing under test is not the thing mocked" stops being
judgement once every double comes from a builder and every fixture row from a
factory, which is what Plan 180's `service_double()` and Stage J's factory
already build. This plan is where that retirement is stated and collected,
not where the mechanisms are built twice.

**Why now, and why one plan rather than several.** The conversation that
produced this, on 2026-09-11, set out to decide whether the testing contract's
shape needed to shift and concluded that the universe has exactly two kinds of
obligation: what a thing owes across a boundary, and what a thing owes inside
itself. Plan 180 is the frame for the first. This is the frame for the second.
The artifact index sits on top of both, because any kind's full obligation is
the union of its between-party statements and its within-party ones, and
today it can only state half. Whether Plan 181, 180 Stage L and 180 Stage J
become this plan's instances or stay where they are is a `plan-start`
question, recorded here as the origin's proposal rather than decided.

**One limit stated now rather than discovered.** *Is this assertion
meaningful* does not fully mechanise. Mutation is the proxy, and it is a good
one for service code, but a suite can be sensitive to a module for the wrong
reason. This plan should record that residue as a named number rather than
promise ironclad, which is the same honesty Stage J owed G13 and the same
shape the contract's *Specified here, not yet asserted* section already
carries.

## Design

**The frame.** A unit of production Python owes three things to itself, and
they are Plan 162 Stage S's three obligations for a dbt model carried across a
language boundary: its branches are exercised, the outcomes it declares are
the ones it produces, and its tests are load-bearing. The reader half, that
the analysers checking all of this are measured code, is
[Plan 181](plan_181_rule_readers_are_code.md)'s and stays there; this plan
depends on its package for Stage B's reader and re-homes nothing from it.

**Module grain, not line grain, and no percentage anywhere.** A production
module is either held to every branch or is "not yet converted", in a
shrink-only ledger, which is Stage AL's shape for the HTTP seam. A global
threshold cannot see a file-shaped hole, which Stage AJ measured, and a
per-file threshold is a guessed bound multiplied by the module count, which
Stage AK forbids. Both are rejected. The existing `--cov-fail-under` stays as
the coarse regression guard until the ledger empties and then retires under
Plan 180's expiry meta-rule. Coverage is unioned across every job that runs
pytest, the correction Stage AJ said its own measurement needed and the shape
`scripts/check_sql_execution_coverage.py` already has for SQL.

**Mutation is the conversion check, not a CI step.** A module leaves the
ledger only when its surviving mutants are each recorded as equivalent,
deleted as dead code, or answered by a test. That is Stage S's split between
decorative and load-bearing constraints, where every entry in the
"unreachable" ledger turned out to be removable, and Stage AF's cost decision:
anchors in CI, mutations by command. The generator is chosen by measurement
between the two Stage AF named rather than by taste.

**Judgement rules are struck by moving code, not by sharpening checkers.**
Once the double builder from Plan 180 Stage A and the fixture factory from
Stage G exist, "the thing under test is not the thing mocked" and "the
assertion is meaningful" leave the contract's judgement section the way
Stage X struck the paraphrase rule, and the residue is recorded as a named
number rather than a promise.

**Two stages arrive from Plan 180, on 2026-09-11.** Its Stage J, a fixture
cannot fabricate a row the database would reject, and Stage L, a parser of a
response we do not own that no test executes, are within-party obligations
that sat in the seam program only because they fell out of Plan 162's tail
while it was being split. They are Stages G and H here; their arguments stay
in Plan 162's own sections, as they did under 180, and only the exits travel.
Plan 180 Stage K, configuration is what Compose delivers, is a seam-7
obligation and stays where it is.

**Rejected on the way:** raising the global threshold; per-file thresholds;
running a mutation generator in CI on every push; keeping three judgement
rules once their premise is gone; folding Plan 181 in wholesale, which would
have restarted a plan already sequenced with its issue set filed.

## Stages

**`Order` is numbered and may be renumbered; `Stage` is lettered in discovery
order and never changes.** G and H carry later letters than E and F because
they were discovered later, at the `plan-start` interview, and sit earlier in
the order because H is the first drain target and G is what F consumes.

| Order | Stage | What it delivers | State | Issue |
|---:|:---:|---|---|---|
| 1 | [**A**](#stage-a-the-frame-in-the-contract) | The frame in the contract | `next` | CAR-131 |
| 2 | [**B**](#stage-b-per-module-branch-coverage-derived-and-ledgered) | Per-module branch coverage, derived and ledgered | `—` | CAR-131 |
| 3 | [**C**](#stage-c-declared-outcomes-are-produced) | Declared outcomes are produced | `—` | CAR-131 |
| 4 | [**D**](#stage-d-the-mutation-harness-and-the-first-module) | The mutation harness and the first module | `—` | CAR-131 |
| 5 | [**H**](#stage-h-the-parsers-nobody-ran-ex-180-stage-l) | The parsers nobody ran *(ex-180 Stage L)* | `—` | CAR-131 |
| 6 | [**G**](#stage-g-a-fixture-cannot-fabricate-a-forbidden-row-ex-180-stage-j) | A fixture cannot fabricate a forbidden row *(ex-180 Stage J)* | `—` | CAR-131 |
| 7 | [**E**](#stage-e-the-drain) | The drain | `—` | CAR-132 |
| 8 | [**F**](#stage-f-the-judgement-rules-struck) | The judgement rules struck | `—` | CAR-133 |

### Stage A: the frame in the contract

**State:** `next` · **Production-gated exit:** no

**Exit:** `docs/TESTING.md` carries a within-party section stating the three
obligations and the evidence that discharges each; the "enough" floor's third
clause, *every failure branch another service's behaviour depends on*, is
restated as a statement with a rule named or struck; the structure parsers
and membership rules are green against the changed document.

### Stage B: per-module branch coverage, derived and ledgered

**State:** `—` · **Production-gated exit:** no

**Exit:** a rule unions branch coverage across every job that runs pytest and
holds each production module to every branch or to its "not yet converted"
ledger entry; the ledger is seeded at the measured count; a converted module
regressing fails; demonstrated by mutation, not asserted.

### Stage C: declared outcomes are produced

**State:** `—` · **Production-gated exit:** no

**Exit:** for every function returning an outcome type by Stage Y's shape
derivation, every declared member is produced by some test; the ledger is
seeded at the measured count and shrinks only; demonstrated by a declared
outcome losing its last producing test failing.

### Stage D: the mutation harness and the first module

**State:** `—` · **Production-gated exit:** no

**Exit:** a per-module mutation command exists, its generator chosen by a
recorded measurement between `mutmut` and `cosmic-ray`;
`ops/coordination_release.py` is converted end to end, with its surviving
mutants recorded as an exact set, each equivalent, deleted or answered, and
the recipe in the Record; demonstrated by a test deleted from that module
leaving a surviving mutant the set does not name.

### Stage H: the parsers nobody ran *(ex-180 Stage L)*

**State:** `—` · **Production-gated exit:** no

**Exit** (Plan 180 Stage L's, carried): every production function that
parses a response from a system this repository does not own is exercised by
a test or waived with its reason; the ledger seeded from a re-measurement at
this stage's start and drained to 0; demonstrated by a parser whose only
exercising test is removed failing.

### Stage G: a fixture cannot fabricate a forbidden row *(ex-180 Stage J)*

**State:** `—` · **Production-gated exit:** no

**Exit** (Plan 180 Stage J's, carried): a unit fixture cannot carry a value
the owning column forbids; `FABRICATED_ROW_WAIVERS` seeded at its measured
count and drained to 0; demonstrated by a fabricated row failing at
construction.

### Stage E: the drain

**State:** `—` · **Production-gated exit:** yes

**Exit:** every production module is converted, service by service; the
branch ledger and the outcome ledger are both empty; the global
`--cov-fail-under` is retired by the expiry meta-rule; and every repair the
drain makes to production code is verified in production, by asking the
deployed container rather than the checkout.

### Stage F: the judgement rules struck

**State:** `—` · **Production-gated exit:** no

**Exit:** the contract's judgement rules go from three to at most one, the
struck rules replaced by the double-builder and fixture-factory border guards
plus the mutation check; the `testing-contract` skill loses the same rules in
the same change; the residue is recorded as a named number.
