---
name: close-out
description: "Close out a Cartracker plan — the plan-level transition, not a stage. Read the gate, propose the row move through the plans skill, and write the sections that state change owes: ## The checks when a plan enters closeout, the archive description and ## Public summary when it archives, ## Superseded when it is replaced. Use when the user says a plan is deployed and waiting on evidence, that its gate has closed, that it should be archived, or that something has superseded it. This skill gathers and proposes first and writes nothing until the user approves: it never decides that a gate has closed, never moves a row on the strength of a summary it wrote itself, and never closes a stage — that is stage-close, named here and never run from here."
---

# Closing out a plan

This skill operates at the grain of a **plan**. A stage finishing is a different
event with a different skill, and conflating them is the main way work here gets
recorded wrong.

| Grain | Skill | Writes |
|---|---|---|
| one **stage** finished | `stage-close` | its `## Record` entry, the order table's `State` cell, the cost comment, `Done` when the issue's last stage closes |
| the **plan** stops being work, or changes state | **this skill** | `## The checks`, the archive description, `## Public summary`, `## Superseded`, and the row move via `plans` |

**If nothing about the plan's state has changed, this is not the right skill.**
A finished stage on a plan that continues is `stage-close` — it writes the
evidence and the order-table cell and stops, and no row moves. Plan 149 is
explicit that a finished slice reaching `Done` *"does not by itself close,
archive or move a plan in `PLANS.md`."* This skill used to carry that case as
its own most common outcome, proposing "nothing" for `PLANS.md`; that case is no
longer its job. Say so and name `stage-close`.

The one exception is a plan that continues but whose published **slice pointer**
is now wrong. That is a `PLANS.md` edit and therefore this skill's, even though
the plan has not moved table — see phase 2.

## The rules this skill encodes, and where they are stated

`docs/PLAN_DOCUMENT.md` is the contract; this skill does not restate it. Read it
for the section ratchet and its fixed order, for the fact that
`## Public summary` is inserted immediately before `## Record` so the record
stays last, for the 320-character cap on both public sections, and for the
superseded exit — a superseded plan gets `## Superseded` and never `## Record` or
`## Public summary`, because it was replaced rather than delivered.

If this skill and that document disagree, that document is right and this file
is the bug.

## Why this skill stops in the middle

Plan 146 Stage 6 separates summarising from transitioning, and the `plans` skill
states the reason:

> a thing that summarises work and moves rows can move a row because its own
> summary said so, and the record then confirms itself.

This skill does both jobs, so it must not do them in one motion. It gathers and
proposes with **every file untouched**, stops, and writes only what the user
approved. The same document sanctions exactly that:

> Reasoning that happens in front of the user, gets argued with, and is
> approved before any file is touched is not that. The approval is a real
> decision point with the evidence on screen.

The stop is not a formality and must not be run past. If you find yourself
writing before an approval, the operation is wrong regardless of how obvious
the conclusion looked.

## Phase 1 — Gather

Read, write nothing:

1. **The plan's row in `docs/PLANS.md`** — which table it is in, and the exact
   words of its `Gate`, `Blocked by` or `Trigger` cell, and of its
   `Next executable slice` cell.
2. **The plan document's order table** — every stage and its `State`. This is
   the preflight for the whole skill: see below.
3. **The evidence the gate names** — a soak result, a metric, a migration, a
   deploy, a test run. Read it if it is readable from here; say plainly if it is
   not.
4. **`## Record`** — the entries `stage-close` and `note-evidence` already
   wrote. That is the material a `## Public summary` is drawn from, and this
   skill reads it rather than re-deriving it from commits.
5. **The plan's Linear issues** — their statuses only. A plan cannot archive
   with work still open; this skill does not change any of them.

### The order-table preflight

A plan does not archive while a stage is unfinished, and the order table is
where that is visible:

- **Every stage `done` or `canceled`** → an archive transition is coherent.
- **Any stage still `—`, `next` or `blocked`** → stop and say which. A plan with
  live stages is not finished, and a `## Public summary` written over open
  stages publishes a claim the plan has not met.
- **A stage that is `done` but whose issue is still open** → the issue carries
  other stages that have not closed. That is `stage-close`'s business, not a
  reason for this skill to reach into Linear.

A **build order → closeout** transition is the one case where open stages are
expected: the code has landed and something is being watched. Say which stages
the checks belong to.

## Phase 2 — Propose, then stop

Present, in this order:

1. **The gate, in its own words**, and your reading of whether the evidence
   satisfies it — as a reading, never as a conclusion. See below.
2. **The `PLANS.md` consequence**, chosen from:
   - **the slice pointer moves, the plan does not** — the row stays where it is
     and its slice cell now names the wrong next step. Propose the exact
     replacement text. It is `plans` operation 5 and it goes through that skill
     like any other `PLANS.md` edit — because the table is that skill's, not
     because the cell is public. Since Plan 138 Stage 9 it is not: the landing
     page publishes the plan document's `## What this plan is for` instead.
   - **build order → closeout** — deployed, evidence pending. Needs a `Lands`
     date and a gate, and neither is yours to invent. Propose `## The checks`
     alongside it — see below.
   - **closeout → archive** — the gate closed. Needs the archive description
     **and the plan's public summary** — see below. Propose both together.
   - **anything → superseded** — needs what superseded it. Propose
     `## Superseded`: the date, a link to the plan or plans that replaced it,
     and why.

   **"Nothing" is not on this list.** If that is the answer, the event was a
   stage closing and `stage-close` is the skill; say so and stop.
3. **The plan document edit**, as the exact text you propose to add and where —
   `## The checks`, `## Public summary`, or `## Superseded`, at the position the
   contract's ratchet gives it.
4. **What you could not verify**, named explicitly.

Then **stop**. Do not write. Do not call another skill.

### Deciding a gate has closed is not yours

The `plans` skill is unambiguous: *"You also do not decide the gate has closed.
The user says it has."*

Phase 2 may say "the gate reads X, and commit `abc123` appears to satisfy it
because Y." It may not say "the gate is met, moving the row." The difference is
whether the user can reject your reading on its evidence — which requires the
evidence on screen and the row still unmoved.

### `## The checks`, written when the plan enters closeout

A plan in closeout has deployed and is waiting on evidence. The row carries a
`Lands` date and a one-sentence gate; the document carries the rest, because a
gate sentence in an index holding thirty other rows cannot say what was deployed
or where the reading goes.

Per check, name four things:

- **what was deployed, and when** — the concrete change, with its date;
- **what is being watched** — the metric, dashboard, log, table or query, by
  name, so the next reader takes the same reading rather than a similar one;
- **when the answer is due** — the same date the row's `Lands` cell holds;
- **which `## Record` entry receives the result** — so the reading has a
  destination before it is taken. `note-evidence` is what writes it there when
  the answer lands.

The row's `Gate` cell is the one-line summary of this section, and the index
stays where the *date* lives so nothing has to scan documents to find what is
due.

### The public summary, written once, when the plan archives

A plan that reaches the archive can appear on the public landing page. Stage 1d
of Plan 138 generates that feed from the archive table, and its generator
prefers a `## Public summary` section in the plan's own document over cutting a
sentence out of the archive row.

The two are written **at the same moment and for different readers**, which is
the whole point of writing them together:

| | Archive Description | `## Public summary` |
|---|---|---|
| Reader | someone who knows this system | someone who has never seen it |
| Length | as long as the evidence needs | one or two sentences |
| Names | migrations, services, columns, object paths | none of those |

So when phase 2 proposes **closeout → archive**, propose both texts, one after
the other, and let the user argue with them side by side. The archive
description is the record; the public summary is what a stranger reads on
`cartracker.info`.

The section goes in the plan document, in the same shape as an archive cell so
the same parser reads it, and **immediately before `## Record`** so the record
remains the last section:

```markdown
## Public summary

**Scrape state ownership** — Separated the timestamp that says a listing was
fetched from the one that says it was enriched, so a stalled processor no
longer causes the same listings to be re-fetched every fifteen minutes.
```

Three rules, and the first is the one that gets broken:

- **Write it for someone who has never read this repository.** No column
  names, no service names, no migration versions, no object prefixes, no PR
  numbers. If a sentence needs one of those to make sense, it is the archive
  description and not this.
- **Say what changed for the system, not what the work touched.** "Deleted
  1,172 legacy objects across six relations" is the archive's sentence;
  "reclaimed 20 GB of duplicated storage" is this one.
- **Within the cap `docs/PLAN_DOCUMENT.md` sets**, which
  `build_public_roadmap.MAX_SUMMARY_CHARS` holds as a number. The generator
  fails the build over it rather than publishing a paragraph, and the fix is
  shorter copy, not a wider cap.

A plan whose work has no public meaning — an internal register, a documentation
reshuffle — should say so and get no section. The generator falls back to
extraction and names the plan in its output, which is Gate 1d's worklist rather
than an error.

**`## What this plan is for` is not rewritten here.** It is present tense and
describes planned work; a plan reaching the archive leaves the planned window
rather than needing its earlier claim edited. The contract freezes it against
work progressing, and archiving is the last thing that could be mistaken for a
reason to touch it.

Nothing here changes `completed_plans.md`. That file is Plan 146's and gains no
column; a plan's own document is the plan's to write in.

**`## Public summary` is a public surface, and this skill is its only author.**
The archive's top rows are published, so the sentence written here reaches
`https://cartracker.info/` on the next `git pull` — Stage 7 left no deploy
between the file and a stranger. **After writing it, run the
`public-surface-check` skill against it**, before the row moves. The order
matters: a claim caught here is a sentence rewritten, and the same claim caught
after `plans` has archived the row is a correction to something already
published.

Two things make this worth a step of its own rather than trust in careful
writing. A public summary is written at the moment the author is most invested
in the work having gone well, which is when an overstated claim is easiest to
write and hardest to see. And it is the **last** sentence a plan ever
publishes — nothing downstream re-reads it, so an error here is permanent in a
way an error in a stage description is not.

**This is a remembered check, not an enforced one.**
`scripts/public_surface_gate.py` holds `README.md` and
`ops/templates/info.html` so they cannot be committed unread; it does not hold
this section. Plan 138 Stage 1c's own argument is that "a check you must
remember is weaker than one you cannot forget", and the trade is affordable
only because this skill is the section's sole sanctioned author. Skipping the
step is not a shortcut — it is the whole of the coverage.

## Phase 3 — Write, in this order

Only after approval, and only what was approved.

1. **The plan document section** — `## The checks`, `## Public summary`, or
   `## Superseded`, at the ratchet's position for it. Additive: do not rewrite
   the plan's problem statement, design, stages or record because the work
   turned out differently. A plan that needs redesigning is a separate
   conversation. **For `## Public summary`, run `public-surface-check` against
   the written section before step 2 moves the row.**
2. **`docs/PLANS.md`, via the `plans` skill** — for a transition *or* a
   slice-pointer update, whichever phase 2 proposed and the user approved. Pass
   the approved values through and tell that skill they were approved in the
   open session, with their source. **Never edit `PLANS.md` directly from
   here**, and that includes a one-cell pointer edit: `plans` is where this
   repository keeps the fact that the build order's top four rows are published
   through `ops/static_ops/generated/project-updates.json`, along with the
   regeneration step and the check that proves it. An edit made from here
   reaches none of that.
3. **Regenerate the public projection** with
   `python scripts/build_public_roadmap.py` and commit its output, if the edit
   touched a published window — CI's `--check` fails on a stale artifact. Let
   `plans` run its own after-every-operation checks; do not duplicate them
   ahead of it.

If any step fails, stop and report. Do not carry on to the next: a plan document
carrying a public summary for a row that never moved is a contradiction the next
reader has to untangle.

## The workspace

This skill **writes nothing to Linear.** It reads issue statuses as part of the
archive preflight and stops there. Setting an issue to `Done` and recording its
cost belong to `stage-close`, at the grain where the evidence for them exists.

- Team **Cartracker**, `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
- Statuses: `Backlog`, `Ready`, `In Progress`, `In Review`, `Soaking`, `Done`,
  `Canceled`. `Duplicate` is reserved — never set it.

## After writing

Report:

- the plan, the transition, and the row before and after;
- which section was added to the plan document, and where in the ratchet;
- every file changed, and the diff's line count;
- whether the public projection was regenerated, and whether `--check` is clean;
- which values were supplied by the user and which were approved from your
  proposal, naming the source of each;
- that no Linear issue was changed, and that no stage was closed from here;
- anything left outstanding — a gate still open, evidence that was not readable
  from here, a stage whose state contradicts the transition.

## What this skill must never do

- **write anything before the approval stop.**
- **decide that a gate has closed.** Propose a reading; the user decides.
- **close a stage.** Not its `## Record` entry, not its order-table `State`
  cell, not its cost comment, not its issue's status. That is `stage-close`,
  which this skill names and never runs.
- **write to Linear at all.**
- **move a `PLANS.md` row directly.** That is the `plans` skill's job, with
  values the user approved.
- **propose "nothing" as a `PLANS.md` consequence.** If nothing moves, this is
  the wrong skill for the event.
- **archive a plan with a stage still `—`, `next` or `blocked`** without naming
  it and stopping.
- **rewrite a plan's design or its `## Record`** because the work diverged from
  the plan.
- **edit `## What this plan is for`.** The contract freezes it against work
  progressing, and a state change is work progressing.
- **write a `## Public summary` the user has not approved.** It is public copy on
  a public page, and it is the one thing here a reader outside this project will
  ever see.
- **give a superseded plan a `## Record` or a `## Public summary`.** It was
  replaced, not delivered; it gets `## Superseded` and the ratchet stops there.
- **close out more than one plan per invocation.** Each has its own gate and its
  own stop.
