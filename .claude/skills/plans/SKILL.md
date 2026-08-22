---
name: plans
description: Perform a state transition in this repo's planning index — move a plan between the backlog, build order, closeout and superseded tables in docs/PLANS.md, add a closeout row, archive a completed plan into docs/planning/completed_plans.md, or transcribe a soak result the user supplies. Use when the user asks to move, promote, start, close out, archive, supersede, or otherwise re-file a plan's state. This skill writes state and never prose: it does not author a title, gate, trigger or summary, does not choose a priority or a build-order position, and does not decide that a gate has closed.
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
| find the row, remove it, write it into the destination's columns | supply the title, gate, trigger, `Lands` date, description, superseding plan |
| renumber `Order` after an insert at a named position | name the position |
| update the archive's row count in the index | decide the gate has closed |
| report what has no status marker, or what does not add up | write every sentence that ends up in a file |

If an operation needs a sentence you would have to write, **stop and ask for
it.** Do not draft one for approval either — an approved draft is still your
sentence, and Plan 146 Stage 6 exists precisely so that summarising work and
moving rows stay in different hands. A tool that does both can move a row
because its own summary said so.

Never emit a provenance label — `*(observed)*`, `*(corroborated)*`,
`*(inferred)*`. Those mark dates Stage 1 reconstructed from git history. A date
you write today was recorded at the time, which is the unmarked default, and
applying a provenance label to it devalues every genuine one.

## The files

| File | Holds |
|---|---|
| `docs/PLANS.md` | closeout, build order, backlog, superseded — four tables, one index |
| `docs/planning/completed_plans.md` | the archive. Newest first, **prepend-only**, one row per plan |
| `docs/plans/plan_NNN_*.md` | the plan's own document, and the authority when it and the index disagree |
| `tests/test_planning_docs.py` | what "correct" means. 27 assertions, ~0.1s |
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

**What it does instead: the user writes the sentence, you move the row.** Ask
for the result verbatim, transcribe it exactly, and perform the state change
that follows — usually closeout → archive. If the user has not given you the
text, stop and ask. Do not read logs, dashboards or metrics and write up what
you found; that is Stage 6's `plan-week` skill and it is separate on purpose.

You also do not decide the gate has closed. The user says it has.

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
git diff
```

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
- **author or summarise.** Titles, gates, triggers, descriptions and soak
  results come from the user.
- **edit plan document content** beyond the status marker above.
- **grow a list of special cases.** If a plan seems to need an exception, the
  structure is wrong. That is the argument `tests/test_planning_docs.py` is
  built on and it applies here.
- **run a transition and a summary in one operation.** Stage 6 is separate on
  purpose.
