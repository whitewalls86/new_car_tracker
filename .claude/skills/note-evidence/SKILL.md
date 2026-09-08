---
name: note-evidence
description: "Record one measurement into a plan document's ## Record while the work is still happening — a soak reading, a production number, a baseline, a mutation that failed the way it should — with the recipe that would reproduce it. Use when a number has just been taken and the stage it belongs to is not finished, or when a check a plan is waiting on has come back. This skill operates at the grain of a single measurement: it appends to ## Record and touches nothing else, writes no Linear, no docs/PLANS.md row and no order-table cell, and never closes a stage or a plan — those are stage-close and close-out, named here and never run from here."
---

# Noting a measurement while the work is happening

A number is cheapest to record the moment it is taken and most expensive to
recover afterwards. This skill exists for the window in between: the stage is
still open, `stage-close` has nothing to close yet, and the reading would
otherwise live in a scrollback buffer until someone re-runs it from a recipe
nobody wrote down.

It is the smallest of the plan skills and deliberately so. One measurement, one
entry, one approval, nothing else touched.

| Grain | Skill |
|---|---|
| one **measurement**, mid-work | **this skill** |
| one **stage** finished | `stage-close` |
| the **plan** changes state | `close-out` |

## Two moments this is for

1. **A number taken during a stage.** A baseline before a change, a size after
   it, a mutation that failed the way it should, a dry run in a disposable clone,
   a timing. The stage is not finished; the number is finished.
2. **A check coming back.** `## The checks` names, per check, which `## Record`
   entry will receive the result. This skill is what writes it there. Recording
   the reading is **not** the same as the gate closing — that reading may be the
   evidence a gate needs, and deciding it satisfies the gate is `close-out`'s
   proposal and the user's decision, never this skill's.

## The rules this skill encodes, and where they are stated

`docs/PLAN_DOCUMENT.md` is the contract; this skill does not restate it. Read it
for the record's shape — one `## Record` section, last in the document, one
`###` entry per stage, chronological with the oldest first — and for
`docs/evidence/plan_NNN_stage_X_evidence.md` as the home for evidence too large
to inline.

If this skill and that document disagree, that document is right and this file
is the bug.

## Phase 1 — Gather

Read, write nothing:

1. **The measurement itself.** The number or result, and **how it was
   produced** — the exact command, query, dashboard panel, or test invocation,
   with its window or arguments. See below: this is the part that gets dropped
   and the part that makes the entry worth having.
2. **Which plan and which stage it belongs to.** One stage, by its letter. A
   measurement that belongs to no stage of any plan does not belong in a
   `## Record`; say so rather than filing it somewhere plausible.
3. **The plan document's `## Record`** — whether that stage already has a `###`
   entry, and what the last entry is, so the append lands in chronological
   order.
4. **The stage's exit condition**, so the entry can say what the number means
   for it — met, not met, or one clause of several.

## Phase 2 — Propose the entry, then stop

Show the **exact text** you propose to append and exactly where it goes. That is
the whole stop, and it is one thing to look at on purpose: a skill meant to be
run mid-task cannot afford a ritual, and the thing worth reading before it is
written is the wording of a number that will be cited later.

Then **stop**. Do not write until it is approved.

### Where the text lands

- **The stage has no `###` entry yet** → create one, headed for that stage,
  after the last existing entry. The record reads forward, oldest first.
- **The stage already has one** — because an earlier measurement or an earlier
  `note-evidence` run wrote it → append this measurement **inside** that entry,
  as its own paragraph. One `###` entry per stage: do not add a second heading
  for the same stage, and do not open a deeper heading level inside it.
- **The plan has no `## Record` section at all** → create it, last in the
  document. If the plan has a `## Public summary`, `## Record` goes after it.

Nothing else in the document changes. Not the stage section, not the order
table, not `## What this plan is for` — the contract freezes that against work
progressing, and a measurement is work progressing.

### What an entry has to carry

Three things, and the second is the one that gets dropped:

- **The number, and what it is a number of.** Units, and the thing measured.
  `73%` is not a measurement; `73% logical reduction across 1,200 sampled
  objects` is.
- **The recipe.** The command or query, its window, and where it was run — this
  machine, the VM, a disposable clone, CI. A number whose method is not written
  down cannot be re-measured, and a plan that re-measures against a different
  method compares nothing. This plan has that failure in its own history: Stage
  G's before-number was recorded without the rule that produced it, and the
  recorded recipe now returns 96 commits against the 79 it claims, which is why
  recovering the definition is Stage G's first work rather than its last.
- **What it means for the exit**, in a sentence. A number with no bearing stated
  is a number the next reader has to re-interpret.

Two more rules:

- **Record a number that did not move**, and a measurement that failed or came
  back inconclusive. Those are results. Recording only the numbers that helped
  is how a record becomes an argument.
- **Never invent, round toward a conclusion, or fill a gap with an estimate.**
  If part of the measurement was not taken, say that it was not taken. An
  invented number in a record is worse than an absent one, because the next
  reader cannot tell.

### Bulky evidence

If the artifact is large — a full test log, a query result set, a table of
per-object sizes, a profile — write it to
`docs/evidence/plan_NNN_stage_X_evidence.md` and link it from the entry.

**The entry still says in prose what the artifact shows.** An entry that is only
a link records nothing: the reader has to open a second file to find out whether
it is worth opening.

## Phase 3 — Write

Only after approval, and only what was approved. There is one step and one file,
plus the `docs/evidence/` file if phase 2 proposed one:

1. Append the entry, or the paragraph inside the existing entry, to
   `## Record`.
2. Write the `docs/evidence/plan_NNN_stage_X_evidence.md` file, if any.

That is the end of this skill. Do not go on to set an order-table cell, touch
`docs/PLANS.md`, or update Linear, however obviously the measurement completes
the stage. If it does complete it, say so and name `stage-close`.

## After writing

Report:

- the plan, the stage, and the entry as written;
- whether a new `###` entry was created or an existing one appended to;
- the `docs/evidence/` file, if one was written;
- what the number means for the stage's exit — and, if it completes it, that
  `stage-close` is the next, separate invocation and this skill did not run it;
- that nothing else was touched: no order-table cell, no `docs/PLANS.md`, no
  Linear.

## What this skill must never do

- **write before the approval stop.**
- **record a number without the recipe that produced it.** That is the failure
  this skill exists against, and it is the one that is invisible until someone
  tries to re-measure.
- **invent, estimate, or round a number toward the conclusion the stage wants.**
  A measurement not taken is reported as not taken.
- **suppress a result that did not help** — a flat number, a failed run, an
  inconclusive reading.
- **touch `docs/PLANS.md`.** Not a row, not a slice cell, not a gate, not
  through `plans` and not directly.
- **set an order-table `State` cell**, or mark a stage `done`. A measurement is
  not a stage closing; `stage-close` is.
- **write to Linear.** No status, no comment, no estimate.
- **write a status marker.** The document carries none; the index owns state.
- **write `## The checks`, `## Public summary`, or `## Superseded`.** Those are
  `close-out`'s, and each implies a state change a measurement does not make.
- **edit `## What this plan is for`**, a stage section, `## Design`, or
  `## The case`. This skill appends to `## Record` and nothing else.
- **decide that a gate has closed** because the reading it was waiting on came
  back. Record the reading; `close-out` proposes and the user decides.
- **put evidence anywhere but `## Record`** and the `docs/evidence/` file that
  entry links.
- **record more than one measurement per invocation.** Each has its own wording
  and its own stop.
