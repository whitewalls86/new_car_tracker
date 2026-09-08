---
name: stage-close
description: "Close one finished stage of a Cartracker plan — gather its evidence, record what it actually cost against its estimate, append the stage's entry to the plan document's ## Record, set its cell in the order table, and set the issue to Done only once every stage that issue carries is done. Use when the user says a stage is finished, landed, or ready to close. This skill operates at the grain of a stage, not a plan: it gathers and proposes first and writes nothing until the user approves, and it never moves a row in docs/PLANS.md, never writes ## Public summary, and never decides a plan has changed state — that is close-out's, named here and never run from here."
---

# Closing a finished stage

This is the per-stage half of what `close-out` used to do at both grains at
once. The two are split along the line `docs/PLAN_DOCUMENT.md` already draws:
**the index owns a plan's state; the document owns its content.**

| Grain | Skill | Writes |
|---|---|---|
| one **stage** finished | **this skill** | its `## Record` entry, the order table's `State` cell, the cost comment, `Done` when the issue's last stage closes |
| the **plan** stops being work | `close-out` | `## The checks`, the gate, the row move via `plans`, the archive, `## Public summary` |

A stage landing is not a plan transition. Plan 149 is explicit that a finished
slice reaching `Done` *"does not by itself close, archive or move a plan in
`PLANS.md`."* That is why this skill exists and why it **cannot touch
`docs/PLANS.md` at all** — not a row, not a slice cell, not a gate. If the work
you are closing has also changed the plan's state, that is a second, separate
invocation of `close-out`, named at the end and never run from here.

## The rules this skill encodes, and where they are stated

`docs/PLAN_DOCUMENT.md` is the contract; this skill does not restate it.
Read it for:

- **the record's shape** — one `## Record` section, last in the document, one
  `###` entry per closed stage, chronological with the oldest first;
- **where bulky evidence goes** — `docs/evidence/plan_NNN_stage_X_evidence.md`,
  linked from the entry, which is also why Linear's `Evidence destination`
  always reads `plan_NNN §Record`;
- **the order table's `State` vocabulary** — `—`, `next`, `blocked`, `done`,
  `canceled`, and that in-progress is not a document state.

If this skill and that document disagree, that document is right and this file
is the bug.

## Why this skill stops in the middle

Plan 146 Stage 6 separates summarising from transitioning, and the `plans`
skill states the reason:

> a thing that summarises work and moves rows can move a row because its own
> summary said so, and the record then confirms itself.

This skill summarises. It gathers and proposes with **every file untouched**,
stops, and writes only what the user approved. The stop is not a formality and
must not be run past: if you find yourself writing before an approval, the
operation is wrong regardless of how obvious the conclusion looked.

## Phase 1 — Gather

Read, write nothing:

1. **Which stage is closing.** One stage, by its letter. Not "the issue" — an
   issue can carry several stages, and which of them is finished is the whole
   input to this skill.
2. **The plan document's order table** — every stage that plan has, each one's
   current `State`, and the `Issue` cell for the closing stage.
3. **The set of stages that issue carries.** From the issue's own `## Exit`
   sections and the order table's `Issue` column together, since either alone
   can be stale. This set decides whether phase 3 sets `Done` or leaves the
   issue open, so establish it before proposing anything.
4. **The stage section itself** — the exit condition it states, verbatim, and
   what the stage said it owes.
5. **The Linear issue** — the `Exit` checklist for the closing stage,
   `estimate`, `startedAt`, `completedAt`, current status.
6. **The commits** — plan-attributed via the `commit-plan-attribution`
   convention, so the plan number resolves them from the message alone.
7. **The evidence** — the test run, the measurement, the mutation, the dry run
   the exit names. Read it if it is readable from here; say plainly if it is
   not.

**Do not read or touch `docs/PLANS.md`.** Nothing this skill writes depends on
it. Reading it invites proposing an edit to it, which is the one thing this
grain must never do.

## Phase 2 — Propose, then stop

Present, in this order:

1. **What the stage claimed to do**, and whether its exit is met, unmet, or
   unverifiable from here — against the exit's own words, not against whether
   the surrounding work looks finished. An exit with three clauses is met when
   all three are.
2. **Cost.** The estimate, the actual, and the delta — see below.
3. **Whether the work moved a public surface** — one question, answered from
   what you have just read. See below. Its answer becomes a line in the record
   entry, so ask it before you write that text.
4. **The `## Record` entry**, as the exact text you propose to append and where
   — after the last existing entry, since the record reads forward.
5. **The order table's `State` cell** — the closing stage to `done`, and, if
   the plan's `next` moved as a result, which stage now carries `next`. Exactly
   one stage per plan carries it.
6. **Whether the issue closes**, from the covered set phase 1 established:
   - **every covered stage is now `done`** → the issue goes to `Done` and the
     cost comment is posted.
   - **at least one covered stage is still `next`, `blocked` or `—`** → the
     issue **stays open**. Name which stages are outstanding. The record entry
     and the state cell are still written; nothing else about this case is
     different.
7. **What you could not verify**, named explicitly.

Then **stop**. Do not write. Do not call another skill.

### One issue can carry several stages

`plan-start` groups a plan's stages into issues by one fact: whether proving a
stage's exit needs its code running in production. A stage whose exit needs
production gets its own issue; a run of consecutive locally-verified stages
bundles into one issue carrying each as its own `## Stage X` section. So the
common shape is an issue with more than one stage inside it, and the rule is:

> Closing a stage **always** writes its `## Record` entry and its order-table
> `State` cell. The **issue** moves to `Done` only when every stage it carries
> is `done` in the order table.

**A one-stage issue is this same rule with a covered set of one.** It is not a
separate branch and must not be coded as one — the moment the two cases get
separate handling, the multi-stage case becomes the one nobody exercises.

Two failure modes this ordering exists to prevent:

- **Closing the issue on the first stage's evidence.** The remaining stages
  would then read as delivered on a neighbour's proof, which is exactly what
  the grouping rule was designed to make impossible.
- **Holding the record entry back until the issue closes.** Evidence
  written days later is evidence re-derived from memory. The entry is written
  when the stage lands, which is what `note-evidence` exists for mid-stage and
  what this skill does at the end of one.

### The record entry

Append one `###` entry, named for the stage, to `## Record`. It records what
landed, how it was verified, and what the work took — the three things the
contract asks of an entry.

Two rules about what an entry is not:

- **It is additive, and it never revises what the plan intended.** Do not
  rewrite the plan's design, its case, or its stage sections because the work
  turned out differently. A plan that needs redesigning is a separate
  conversation, and the record's value is that it says what actually happened
  next to what was actually planned.
- **`## What this plan is for` is not touched.** `docs/PLAN_DOCUMENT.md` freezes
  it against exactly this event: a stage landing does not change what a plan is
  for. It is edited only when the plan's purpose changes, which is not what
  closing a stage means.

**Bulky evidence goes to `docs/evidence/plan_NNN_stage_X_evidence.md`**, with
the entry linking it and still saying in prose what the artifact shows. An
entry that is only a link records nothing.

### Did this work move a public surface?

Ask it as a step, every time:

> Did this work change a **mechanism**, a **name**, or a **quantity** that
> `README.md` or `ops/templates/info.html` states?

That is the `public-surface-check` taxonomy read from the other end. That skill
checks a surface someone edited against the repository; this asks whether the
repository has just moved out from under a surface nobody edited. **The commit
gate cannot see that case at all** — `public_surface_gate.py` fires on
`README.md` or `ops/templates/info.html` appearing in `git diff --cached
--name-only`, so a stage that adds migrations, adds containers or replaces a
solver and edits no prose never reaches it. Every defect Plan 138's Gate 0
found was that case: a surface standing still while the tree moved underneath
it.

**It must be cheap, and it will usually end in "no".** Answer it from the
commits and evidence phase 1 has already gathered. Do not open either surface
unless the answer is yes — a step that stops every stage close to deliberate
will be skipped within a month, and then it holds nothing.

Three outcomes, and **all three are recorded**:

- **No** — one line in the record entry, in the shape `Public surfaces: no
  mechanism, name or quantity either surface states was changed by this work.`
  Write it even though it is uneventful. That line is the only thing that
  distinguishes a stage close where the question was asked and answered from
  one where it was skipped.
- **Yes, and small** — propose the correction as exact replacement text in the
  approval stop this phase already makes, and land it with the stage. It stages
  a surface, so `public_surface_gate.py` fires and `public-surface-check` reads
  the diff in the normal way. The two compose; neither replaces the other.
- **Yes, and larger than this stage** — a ticket, via `ticket-now`, naming the
  surface and the claim that is now wrong. Closing a stage is a bad place to
  rewrite a section of the front door.

**Propose; never write.** The approval stop covers this exactly as it covers
everything else in phase 2.

This is a step, not a hook, and saying so is part of using it honestly. Nothing
forces a stage to be closed through this skill, so unlike the commit gate it is
a check you can still miss by never closing out. It is worth doing anyway
because it rides a ritual that already exists, and because the alternative is
auditing accumulated drift after it has been published.

## Cost, and why elapsed time is not it

Record the **original estimate untouched** and the actual alongside it. Never
overwrite `estimate` with the actual: the delta between them is the only thing
that ever improves sizing, and overwriting destroys it.

**Actual cannot be derived, and you must not try.** Linear's `startedAt` and
`completedAt` give elapsed calendar time, which is not effort — a 1-point issue
that sits in `In Review` over a weekend shows three days and cost an hour.
Feeding elapsed time into a calibration table produces numbers that look
rigorous and mean nothing.

So: **ask.** Offer elapsed time and the commit span as context, on the same
Fibonacci scale the estimate used (`1` under half a day, `2` about a day, `3`
two to three days, `5` most of a week). If the user does not give a number,
record that it was not measured rather than guessing one.

**An issue's estimate covers every stage it carries**, so a bundled issue's
cost is asked and recorded once, when its last stage closes and the comment is
posted. Closing an earlier stage of that issue records what that stage took in
its `## Record` entry — prose, not a point score against an estimate the stage
does not have on its own.

Write the cost as a comment on the issue, never into the description body the
`ticket-now`, `fill-cycle` and `plan-start` skills own:

```markdown
**Closeout** YYYY-MM-DD
estimate N → actual M (±D)
cause: one line, only when the delta is worth a reason
stages: the letters this issue carried
evidence: plan_NNN §Record
```

A delta of zero is still worth writing. A run of accurate estimates is the
evidence that the scale works.

## Phase 3 — Write, in this order

Only after approval, and only what was approved.

1. **The `## Record` entry**, appended to the plan document's `## Record`
   section — after the last existing entry. If the plan has no `## Record` yet,
   this stage is its first to close and the section is created here, last in
   the document.
2. **Bulky evidence**, if any, to `docs/evidence/plan_NNN_stage_X_evidence.md`,
   with the entry linking it.
3. **The order table's `State` cell** — the closing stage to `done`, and the
   `next` marker moved if phase 2 proposed it. Edit the cells, nothing else in
   the table; `Order` is not renumbered because a stage finished, and `Stage`
   letters are never touched.
4. **The public surface correction**, if the surface question ended in "yes,
   and small" and the user approved the exact text. Write that text and nothing
   else — closing a stage is not the moment to tidy the rest of the file. The
   commit then stages a surface, so `public_surface_gate.py` blocks it until
   `public-surface-check` has read the diff and stamped it. Run that skill; do
   not bypass the hook.
5. **Linear.**
   - **Every covered stage now `done`** → set the issue to `Done` and post the
     cost comment. Leave the estimate alone.
   - **Otherwise** → leave the issue's status exactly as it is and post
     nothing. Say in the report which stages it is still waiting on.

If any step fails, stop and report. Do not carry on to the next: a plan
document recording a stage as `done` whose issue was never updated is a
contradiction the next reader has to untangle.

## Which status, and when not to use Done

- **`Done`** — every stage the issue carries is `done` and each one's exit is
  met.
- **left as it is** — the issue's remaining stages are still open. This is not a
  status decision at all; it is the absence of one, and it is the common case
  for a bundled issue.
- **`Soaking`** — implementation is complete but a time or evidence gate is
  running. The issue names the gate and its end time. Write the record entry,
  set the state cell, and stop; a stage waiting on a soak is `blocked` in the
  order table, not `done`.
- **`Canceled`** — the stage was rejected, superseded, or made unnecessary. Its
  order-table state is `canceled` and **its letter stays spent** — the contract
  is explicit that a canceled stage's letter is never reused. Cost is still
  worth recording if work was done.

A stage whose exit is not met is not `done` because the user is finished with
it. Say which clauses are outstanding and let them decide.

## The workspace

- Team **Cartracker**, `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
- Estimation is **Fibonacci**, enabled 2026-08-25. Points are live, so an
  unestimated closed issue silently undercounts its cycle.
- Statuses: `Backlog`, `Ready`, `In Progress`, `In Review`, `Soaking`, `Done`,
  `Canceled`. `Duplicate` is reserved — never set it.

## After writing

Report:

- the stage closed, and its order-table state before and after;
- the `## Record` entry, and any `docs/evidence/` file it links;
- every file changed, and the diff's line count;
- **the issue's status, and if it stayed open, which stages it is waiting on**;
- the cost line recorded, or that no cost comment was posted because the issue
  stayed open;
- that `docs/PLANS.md` was not touched — and, if the plan's own state has now
  changed, that `close-out` is the separate invocation for it and this skill did
  not run it;
- which values were supplied by the user and which were approved from your
  proposal, naming the source of each;
- anything left outstanding — an unverified exit clause, an unmeasured actual,
  evidence that was not readable from here.

## What this skill must never do

- **write anything before the approval stop.**
- **touch `docs/PLANS.md`.** Not a row, not a slice cell, not a gate, not
  through `plans` and not directly. A stage landing is not a plan transition,
  and a plan whose state has actually changed is `close-out`'s separate
  invocation.
- **write `## Public summary`, `## The checks`, or `## Superseded`.** Those are
  `close-out`'s sections, at the plan's grain, and each one implies a state
  change this skill has no standing to assert.
- **set an issue to `Done` while any stage it carries is not `done`.** Check
  the covered set every time, including when the set has one member.
- **mark an exit met because the work looks finished**, or close a stage with
  outstanding exit clauses without naming them.
- **overwrite `estimate` with the actual**, or derive an actual from elapsed
  calendar time.
- **rewrite a plan's design, case, or stage sections** because the work diverged
  from them. The record is additive.
- **edit `## What this plan is for`.** A stage landing is the exact event
  `docs/PLAN_DOCUMENT.md` freezes it against.
- **renumber the order table, or rename or reuse a stage letter** — including a
  canceled stage's letter.
- **write evidence anywhere but `## Record`** and the `docs/evidence/` file that
  section links.
- **silently edit a public surface.** A correction to `README.md` or
  `ops/templates/info.html` is proposed as exact text and approved like
  everything else here. Skipping the question, or answering it and recording
  nothing, is the same failure one step earlier.
- **close more than one stage per invocation.** Each has its own evidence and
  its own stop.
