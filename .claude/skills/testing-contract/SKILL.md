---
name: testing-contract
description: "Review a change against docs/TESTING.md before it is committed — run the mechanical assertions, then judge the four rules no test can check, saying plainly which half is which. Use when the user asks whether a change meets the testing contract, asks for a review of new or edited tests, or is about to commit a change that touches tests, a route, a .sql file, a service directory or a CI step. This skill reads and reports: it edits no file, adds no waiver, and refuses to certify the rules it cannot check rather than implying coverage it does not have."
---

# Reviewing a change against the testing contract

[`docs/TESTING.md`](../../../docs/TESTING.md) is the standard. It exists in
three forms and they are the same contract:

| Form | Where | What it can do |
|---|---|---|
| For a person | `docs/TESTING.md` | say what is right |
| For CI | `tests/test_testing_contract.py` | fail on the mechanical rules |
| For a coding agent | this skill | the seven, plus the four nobody can mechanise |

The reason there are three is written into the plan that produced them:
`ARCHITECTURE.md:179` described the suite accurately in April 2026 and was
quietly false by August, because nothing could tell the difference. A document
alone drifts. A test alone checks seven things and blesses everything else by
silence. **This skill exists for the gap between them, and its whole value is
in being honest about where that gap is.**

## Why this skill runs the test instead of re-checking it

The seven mechanical rules are already implemented, once, in
`tests/test_testing_contract.py`. Re-deriving any of them here would produce a
second implementation that drifts from the first, and the first is the one CI
runs — so a disagreement between them would be resolved in favour of the copy
nobody reads.

So: **run the test. Report what it says. Do not reason about its subject.**

Where a rule seems wrong, that is a finding about the rule, and it belongs in
the report as one — not in a workaround.

## Phase 1 — Establish what actually changed

Read, and write nothing. `git diff` against the merge base, not the working
tree alone.

Then classify. Each of these has a contract consequence, and the classification
is what tells you which parts of phase 3 apply:

| The change adds or edits | Then |
|---|---|
| a test file | its directory decides its layer, and its layer decides what it may depend on |
| a route | it must be reached through `app.routes` by a test in that service's own test directory |
| a `.sql` file, or a statement at an `.execute()` call site | Layer 2 must execute it, and it must be a file unless it is structurally generated |
| a service directory | it needs a row in the "enough" table, and it starts below the floor |
| a `tests/integration/<dir>` | a CI step must invoke it, or a waiver must say why not |
| a CI step running pytest | it sets `PYTHONPATH` |
| a mock, patch, or fixture | phase 3, all of it |

A change that touches none of these still gets phase 2, because a change can
break a rule it does not mention — deleting the last test that requested a
route is the obvious one, and it looks like a pure deletion in the diff.

## Phase 2 — Run the mechanical half

```
pytest tests/test_testing_contract.py -q
```

Report its output as-is. Three outcomes and they mean different things:

- **Passes.** The mechanical rules hold. That is all it means, and saying more
  than that is the failure this skill is written to avoid. Since Plan 162
  Stage X that set includes *no SQL literal appears under `tests/`* — which was
  judgement rule 4 here until the repository changed underneath it — so a
  passing run now settles a question this skill used to have to weigh.
- **Fails on an unwaived violation.** The change introduced it, or uncovered
  it. Say which — `git stash` and re-run settles it in one step.
- **Fails on a stale waiver.** Something was *repaired*. The waiver must be
  deleted in the same change. This is the mechanism working; report it as
  progress with an action, not as a problem.

If the test cannot run, that is a finding, not a reason to continue on
inspection. A skill that reports "looks fine" because it could not execute the
check has produced exactly the advisory report the contract refuses.

## Phase 3 — The three rules no test can check

This is the work. Each of these looks mechanical from a distance and is not,
and the reason it is not is worth carrying — it is what keeps the next person
from "fixing" it with a checker that fails on correct code.

### 1. The thing under test is not the thing you mock

**Why judgement:** no rule can say which function in a module is the subject.

The `search_path` incident is the shape to look for. `ops/coordination_drain.py`
queried `task_instance` unqualified and `public.detail_scrape_claims` instead
of `ops.`; the drain hung on its first production deploy. It escaped because
`tests/ops/test_coordination_drain.py` patches `_database_count` **itself** —
the function whose job is to build and run the query. The only string that ever
reached a cursor was the literal `"SELECT evidence"`.

Ask, of every patch in the diff: *if the patched thing were wrong, would this
test still pass?* If yes, the test is asserting the mock.

Patching the thing that *runs* a query stays legitimate. What is forbidden is
that being the only thing that ever happens to the statement. The other half of
that used to be rule 4 here; it is mechanical now (see below), so what is left
for judgement is the patch itself.

### 2. Whether a failure branch matters to another service

**Why judgement:** it needs the call graph across service boundaries, and the
consequence of the branch, neither of which is in the file.

`_database_count` returning `unknown` was not a bug. `unknown` failing closed
was not a bug. The two together drained forever. The question is never "is this
branch covered" but *"who reads this, and what do they do when it says that?"*

Health and readiness endpoints are the standing example: they look like
boilerplate and they are what another service's drain logic reads.

### 3. Whether an assertion is meaningful

**Why judgement:** `assert response.status_code == 200` is either the point or
theatre, depending on what else the request was supposed to do.

The specific failure to look for is a test that would pass against a stub of
the code under test. Also: an assertion on a mock's call arguments where the
production caller's arguments were never checked against the real signature.

### Struck: whether a `SELECT` in a test file paraphrases or seeds

**This was rule 4 and it is mechanical now.** Plan 162 Stage X moved all 505
SQL literals out of `tests/` into `tests/sql/`, and
`test_no_test_module_holds_a_sql_statement` fails on a new one. The rule was
judgement for an exact reason — fixture seeds are SQL in test files too, and a
checker that cannot tell a seed from a paraphrase fails on correct code — and
that reason stopped applying when there was no literal left for the ambiguity
to live in. **Do not weigh this by hand.** Report what the test says.

What is still judgement, and belongs under rule 1 rather than here: *how* a
test reaches a production statement. The order of preference is unchanged —
load the `.sql` file; or import the module-level constant or `(sql, params)`
builder; or, last, read it out of the source with `ast`. A test that asserts a
*substring* of production's statement is the shape to flag: `assert "DELETE
FROM authorized_users" in sql` passes just as well after the `WHERE` clause is
dropped.

**`ast` is a last resort and each use marks a defect elsewhere.** There is one
in the tree, `_sensor_constant()`, and it is correct — `airflow/dags/sensors.py`
imports Airflow at module scope and Layer 2 runs in the main venv, so the
constant cannot be imported. A rising count of `ast` readers is a signal to act
on, not a pattern to spread. Where one is genuinely necessary, it carries a
comment naming the import that forced it, so the next reader can tell a
constraint from an accident.

### And a fifth, half-mechanical: the harness must not decide the outcome

`tests/test_testing_contract.py` checks the one part of this that is
mechanical — every pytest step in CI sets `PYTHONPATH`. The rest is yours.

**An unexplained mock of a filesystem, clock, platform or path primitive is a
finding.** Not a violation: several are correct, and the correct ones say why.
`21333ab` is the pattern — `Path.is_symlink` is mocked, with a comment reading
*"the behavior owned here is our refusal to traverse a run directory that the
filesystem classifies as a symlink, not pathlib's OS integration."* That
sentence draws the line between the capability the test depends on and the
behaviour the test owns. A mock of the same primitive with no such sentence is
a test whose result the environment might be deciding.

Both directions have bitten this repository:

- **Fails where it should pass** — a real symlink needs elevated privileges on
  Windows, so the test failed on a developer machine for a reason unrelated to
  pruning.
- **Passes where it should fail** — `tests/test_planning_docs.py` gave 35
  passed in a checkout named `cartracker-scraper` and 2 failed in one named
  `new_car_tracker`, same commit, same machine. Nothing about the code
  differed.

The second is worse, and the practical rule that follows is: **a green run on a
developer machine is not evidence the test passes in CI.** Check the job's
`conclusion`, never just the run's colour — a job that is usually *skipped* is
not evidence of anything at all.

## Phase 4 — Report

In this order:

1. **The mechanical result**, verbatim, and whether the change caused any
   failure or merely inherited it.
2. **Findings**, each naming the rule and the specific line. A finding is a
   claim about this change, not a general observation about the file.
3. **What was not checked.** Name the four rules you could not reach, and why —
   a diff that only touched CI does not put you in a position to judge whether
   an assertion is meaningful, and saying so is more useful than silence.
4. **Anything the contract does not cover** that the change raises. The
   contract is young; a rule it is missing is worth a sentence and, if it
   matters, a ticket.

Never end with a bare approval. The honest closing is which rules were checked
and which were not.

## Waivers are not this skill's to add

A waiver grandfathers a violation with an owner plan and a date, and the
contract says adding one is a decision, not a convenience. **Propose one if the
change genuinely cannot be made to comply; never write one.** Say which gap
entry it would attach to and which plan would own the repair, and let the user
decide.

The one waiver movement this skill should push for is **deletion**. A stale
waiver — one the test now reports as describing no violation — means a repair
happened. Deleting it belongs in the same change.

## What this skill must never do

- **Edit any file.** It reads, runs one test, and reports.
- **Add a waiver**, or suggest one as a way past a failing check rather than as
  a decision the user makes.
- **Certify a judgement rule.** "No patch of the subject under test found" is
  a claim this skill can make; "the mocks are honest" is not.
- **Re-implement a mechanical check** in prose because the test was
  inconvenient to run, or report "looks fine" from inspection when the test did
  not execute.
- **Treat a passing test as a passing review.** The mechanical rules are what
  they are, and three more are this skill's; reporting the first as coverage of
  both is the failure the contract's question 7 was written about. Do not
  restate the split as a count — `docs/TESTING.md` carries it, and the count
  in this file was wrong for weeks before Stage X noticed.
- **Extend the contract by inference.** Where `docs/TESTING.md` is silent, say
  it is silent. A deliberate absence reads differently from an oversight, and
  guessing which one this is destroys the distinction.
