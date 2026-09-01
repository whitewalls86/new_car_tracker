---
name: plans
description: "Perform a state transition in this repo's planning index — move a plan between the backlog, build order, closeout and superseded tables in docs/PLANS.md, add a closeout row, archive a completed plan into docs/planning/completed_plans.md, transcribe a soak result the user supplies, or update a plan's next-executable-slice pointer without moving its row. Use when the user asks to move, promote, start, close out, archive, supersede, or otherwise re-file a plan's state. This skill writes state and never prose: every title, gate, trigger, date and description arrives from outside it — supplied by the user, or reasoned out and approved before this skill runs — and it does not author one mid-transition, choose a priority or a build-order position, or decide that a gate has closed."
---

# Moving a plan between states

Plan 146 rebuilt `docs/` around one rule: **every plan sits in exactly one
table, and every row carries the condition that removes it.** These operations
are what move a row from one to the next. They are small, repetitive, and
touch two or three files in a fixed pattern — which is why they are worth
automating, and why the automation must be narrow.

## The boundary, before anything else

You place values. You do not compose them.

| You do | The user does |
|---|---|
| find the row, remove it, write it into the destination's columns | supply — or approve, before this skill runs — the title, gate, trigger, `Lands` date, description, superseding plan |
| renumber `Order` after an insert at a named position | name the position |
| update the archive's row count in the index | decide the gate has closed |
| record where each authored value came from | approve every sentence that ends up in a file |
| report what has no status marker, or what does not add up | reject a proposal on its evidence |

If an operation needs a sentence you do not have, **stop and ask for it.** You
never derive one mid-transition — not from the plan document, not from the
commits, not from what the last row said.

**A value may reach you two ways, and you record which.** Either the user
supplied it directly, or it was reasoned out and proposed *in the open session
before this skill was invoked* and the user approved it. Both are explicit
values arriving from outside; neither is you writing a sentence while moving a
row.

That distinction is the whole of it, and it is narrower than it looks. What
Plan 146 Stage 6 separates is **one tool doing both** — a thing that summarises
work and moves rows can move a row because its own summary said so, and the
record then confirms itself. Reasoning that happens in front of the user, gets
argued with, and is approved before any file is touched is not that. The
approval is a real decision point with the evidence on screen.

So it stays out of this skill. **Do not propose a gate, a trigger or a
description from inside an operation** — that is the shape the rule forbids,
because a proposal made mid-write is one the user reads as a formality. If you
find yourself needing a value you were not given, stop, leave the files alone,
and do the reasoning outside.

A proposal that arrives here should carry **where each value came from** — the
plan document section, the commit, the measurement. A gate you can trace is one
the user can reject on the evidence; a plausible sentence is one they can only
rubber-stamp. In your report, say of every authored value whether it was
supplied verbatim or approved from a proposal, and name the source.

Never emit a provenance label — `*(observed)*`, `*(corroborated)*`,
`*(inferred)*`. Those mark dates Stage 1 reconstructed from git history. A date
you write today was recorded at the time, which is the unmarked default, and
applying a provenance label to it devalues every genuine one.

## The files

| File | Holds |
|---|---|
| `docs/PLANS.md` | closeout, build order, backlog, superseded — four tables, one index. **Public: its top four build-order rows are published** |
| `docs/planning/completed_plans.md` | the archive. Newest first, **prepend-only**, one row per plan. **Public: its top four rows are published** |
| `docs/plans/plan_NNN_*.md` | the plan's own document, and the authority when it and the index disagree |
| `tests/test_planning_docs.py` | what "correct" means. 33 assertions, ~0.2s |
| `docs/planning/plans_decision_log.md` | narrative. You do not write here; the user may |

## Splice, never reflow

**This is the rule the whole skill stands on.** Every edit is anchored on the
exact text of one line. You never hold a whole table, and you never rewrite
one.

Do it with `Edit`, whose `old_string` must match exactly and uniquely or the
call fails. That failure is the point: a row you were not told about is a row
that never passes through your output, so it cannot be dropped by omission —
dropping it would take a bug, not a lapse of attention.

Do **not** read a table, mutate a list of rows, and re-render it. That is the
tempting shape and it is wrong here for two reasons. Every untouched row goes
through the writer, so any one of them can come out subtly changed or not at
all. And the diff stops being reviewable: reflowing a table you were not asked
to touch buries the next real change in noise.

Stage 1's sweep measured what the alternative costs. Of 122 reconstructed state
transitions, **92 are a row disappearing rather than moving.** Strip the two
bulk events and **33 rows still vanished, one and two at a time, across 16
separate days.** Reproducing that with a tool automates the leak.

After every operation the diff should be the row that moved and nothing else —
plus, for a build-order insert, exactly one changed `Order` cell per row below
it. Read it before you report.

## Cell formats you must match

Stage 2 froze the column headers and the test reads them. The em dash in
`Gate — what removes this row` and the question mark in `Workable?` are
load-bearing.

The index's `Plan` cell takes exactly two forms, optionally followed by a bold
stage marker:

```
| [135](plans/plan_135_storage_observability.md) |
| [140](plans/plan_140_service_health_contract.md) **Stage 4** |
| **88** |
```

Links are relative to `docs/`, so `plans/plan_NNN_*.md` from `PLANS.md` and
`../plans/plan_NNN_*.md` from the archive. **The archive's `Plan` column is
different**: a bare number, `| 135 |`, never a link and never bold.

Three things that will bite:

- **The link text and the target must name the same plan.**
  `[112](plans/plan_113_...)` is well-formed, resolves, and sends the reader to
  the wrong plan. Copy the path from the row you are moving; do not retype it.
- **Escape pipes inside a cell** as `\|`. Build-order row 4 quotes the LogQL
  fragment `\|= "403"`; an unescaped one makes the row read a column short.
- **One plan may hold several rows in one table.** Plan 139 holds build-order
  rows for Stage C and Stage D — different slices, different blockers. That is
  the design. Never deduplicate a table.
- **A separator line is not a unique anchor.** Closeout and superseded both
  have three columns, so `|---|---|---|` appears twice in `PLANS.md`. To insert
  at the top of a table, anchor on its **header line and separator together**;
  to insert anywhere else, anchor on the row that will sit above.

## The operations

Every one of them: **run the test first**, so a failure afterwards is yours.

```bash
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py -q
```

### 1. Move a plan between states

Backlog → build order, build order → closeout, anything → superseded.

1. Read the source row. Keep its `Plan` cell verbatim — link text, path and any
   `**Stage N**` marker.
2. `Edit` the source table: `old_string` is the whole row line plus its
   trailing newline, `new_string` is empty. One row leaves.
3. `Edit` the destination: anchor on the row that will sit above the new one
   (or on the header separator to go first), and append your row after it.
4. Fill the destination's columns. Every value is carried from the source row
   or supplied by the user. The destination's **exit condition is mandatory**
   and the user supplies it:

   | Destination | The user must supply |
   |---|---|
   | closeout | a `Lands` date (`YYYY-MM-DD`) and a gate |
   | build order | the position, and `Workable?` / `Blocked by` |
   | backlog | a trigger |
   | superseded | what superseded it |

   Nothing here has a sensible default. A closeout row with no gate is a row
   whose date arrives and nothing happens — Plan 123 sat that way from
   2026-07-10 for six weeks, which is why the column exists.

5. **Build-order inserts renumber.** The user names the position; you do not
   choose it. Insert there, then `Edit` each row below it — one call per row,
   changing only the `Order` cell, `old_string` running through the `Plan` cell
   so it is unique. `Order` must read `1..N` with no gaps and no duplicates,
   and the test checks it.

### 2. Add a closeout row

A plan is deployed and its evidence is pending. **No code is owed on a closeout
row** — a plan that still owes code belongs in the build order with the wait
recorded as its blocker. If the user asks to close out a plan that owes code,
say so and ask.

Same mechanics as above. `Lands` is the date somebody looks, not the date the
code shipped.

### 3. Archive a completed plan

Two files, and the second is the one that gets forgotten.

1. Remove the row from its table in `docs/PLANS.md`.
2. **Prepend** to `docs/planning/completed_plans.md`. Anchor the `Edit` on the
   header separator:

   ```
   |------|-------------|------|
   ```

   and put the new row immediately after it. The archive is newest-first, so a
   new row goes at the **top**. Appending is what a writer does by default and
   it is wrong here.

   Columns are `| <bare number> | <the user's description> | YYYY-MM-DD |`. No
   provenance label.

3. **Update the count in the index.** `docs/PLANS.md` says
   `— NNN rows, newest first` and that number is maintained by hand. Increment
   it. `tests/test_planning_docs.py` now checks it, so forgetting fails
   loudly rather than leaving the index quietly disagreeing with the record it
   points at.

Superseded plans do **not** go here. They were replaced, not delivered, and
merging them in would make the archive claim work that never happened.

### 4. Record a soak result

The plan lists this as an operation and forbids what it requires: a soak result
is a summary, and this skill does not summarise.

**What it does instead: the sentence arrives, you move the row.** Transcribe
it exactly and perform the state change that follows — usually closeout →
archive. If you have not been given the text, stop and ask.

A soak result is the case where the two-ways rule is most easily abused, so it
is narrowest here. **Never read logs, dashboards or metrics and write up what
you found** — that is Stage 6's `plan-week` skill and it is separate on
purpose. A soak result approved from a proposal must have been reasoned from
evidence the user could see, and the row records which run it came from. "The
soak passed" with no run behind it is the sentence this operation exists to
refuse, whoever typed it.

You also do not decide the gate has closed. The user says it has.

### 5. Update a plan's next executable slice

**A pointer move is not a state change, and it still belongs here.** A slice
finishes, the plan stays exactly where it is, and the build order's **Next
executable slice** cell has to name what comes next. No row moves. Every other
column, and every other row, is untouched.

It is an operation of this skill for one reason: **that cell is published
copy**, and the "After every operation" section below is where this repository
keeps that fact. A pointer update made anywhere else is an edit to the landing
page by someone who does not know they are making one — which is how it went
wrong on 2026-09-01, when Plan 138's own closeout wrote the cell from the
`close-out` skill and reached none of this.

1. Read the row. Confirm the plan number and that the row is where the user
   thinks it is.
2. `Edit` the **slice cell only** — `old_string` running from the cell's first
   words through enough of it to be unique. The `Order`, `Plan`, `Title`,
   `Workable?`, `Blocked by`, `Priority`, `Effort` and `Depends on` cells all
   stay byte-identical. Changing any of them is a different operation.
3. Run the checks below. **The `--check` is not optional here** — a pointer
   update inside the top four rows always moves the artifact.

**The text arrives from outside, like everything else.** The user supplies the
new cell, or approves it in the open session before this skill runs. You do not
read the plan document and decide what the next slice is: that is composing, it
is the boundary this skill exists to hold, and a pointer that names the wrong
next step is worse than a stale one because it reads as deliberate.

If the same request also moves the row, that is operation 1 **and** this one.
Do them as two edits and say so, rather than rewriting a row and a cell in one
motion.

## Plan documents and their status

**Yes, this skill touches a plan document's status marker — and nothing else in
the document.**

It has to. `PLANS.md` states that when the index and a plan document disagree,
the plan document wins. Move a row and leave the document asserting the old
state and you have not just created a contradiction, you have made the
*authority* the wrong one. That is worse than the defect Plan 146 was written
to fix.

Status is written three ways across 79 documents, and often not at all:

| Form | Count | What you do |
|---|---|---|
| a `## Status` section | 25 | replace the state phrase in its first line |
| a `**Status:**` line near the top | 37 | replace the state phrase on that line |
| neither | 17 | **nothing.** Report it |

Three hard limits:

- **Never create a status marker in a document that has none.** Seventeen
  documents have never had one; inventing structure for them is authoring, and
  it would make this skill grow a shape the documents did not ask for.
- **Replace the state phrase only** — the leading `**Draft — not started.**`,
  `**COMPLETE (2026-04-29)**`, `PLANNED`. Everything after it on the line, and
  every other line in the document, stays byte-identical. The replacement is
  either a state word the index already uses (backlog, build order, closeout,
  complete, superseded) or text the user supplies verbatim.
- **When there is no marker, say so and move on.** Report it in your summary as
  a fact the user may want to act on. Do not treat it as a failure and do not
  fix it.

## After every operation

```bash
LOG_PATH=/tmp/ct.log .venv/bin/python -m pytest tests/test_planning_docs.py -q
python scripts/build_public_roadmap.py --check
git diff
```

**Both of this skill's files are generator input, and the test above cannot
see it.** Plan 138 Stage 1d publishes `docs/PLANS.md`'s build order and
`docs/planning/completed_plans.md` to the landing page through
`ops/static_ops/generated/project-updates.json`. The build order's **Next executable
slice** cell *is* the published `summary` for a planned plan, so a one-cell
edit changes public copy — and since Plan 138 Stage 7 mounts that directory into
`ops` from the checkout, the copy goes live on the next `git pull`, with no
deploy standing between the edit and the public page.

If `--check` reports the artifact stale, run
`python scripts/build_public_roadmap.py` and leave its output in the diff.
Regenerating is not authoring: the generator is deterministic and reads only
values already placed and approved. It is the same kind of derived-state
upkeep as the archive row count above.

Only the first four rows of each side are published (`MAX_ITEMS = 4`), so not
every edit moves the artifact — but do not try to reason about which do.
**Archiving always does**, because the archive is prepended and a new row is
therefore always in the top four; a build-order insert renumbers and can carry
a plan across the boundary in either direction. Run the check and believe it.

This is how it goes wrong: `tests/test_planning_docs.py` passes on a stale
artifact, so a green run there is not evidence. The assertion that catches it
lives in `tests/scripts/test_build_public_roadmap.py`, which the unit job runs
and this operation does not.

Read every changed line. Then report, briefly:

- which row moved, from where to where
- which files changed, and the diff's line count
- anything you did not do: a missing status marker, a gate the user has not
  confirmed, a sentence you needed and asked for

If the test fails, **fix the document, not the test.** These assertions were
each watched failing against a deliberate mutation before they were trusted;
one that starts failing is reporting something real.

`docs/PLANS.md` also has a 250-line budget and sits near 169. Rows are single
lines, so the budget binds on row *count* — but if you are close to it, the
cause is almost certainly that a row is carrying context it should not. Context
is prose. Do not add it.

## What this skill must never do

- **decide an order.** Insert where told; never choose a priority, an effort,
  or a build-order position.
- **author or summarise mid-transition.** Titles, gates, triggers,
  descriptions, soak results and slice pointers arrive from outside —
  supplied by the user, or approved by them before this skill runs. Needing one
  you do not have is a stop, never a draft.
- **edit plan document content** beyond the status marker above.
- **grow a list of special cases.** If a plan seems to need an exception, the
  structure is wrong. That is the argument `tests/test_planning_docs.py` is
  built on and it applies here.
- **run a transition and a summary in one operation.** Stage 6 is separate on
  purpose.
