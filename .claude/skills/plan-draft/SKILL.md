---
name: plan-draft
description: "Turn an idea into a plan document — allocate its number and permanent filename, write its first two sections (what it is, and the case for building it), then ask where it lands: the build order, the backlog behind a named trigger, or the backlog with the trigger still unknown. Use when the user has an idea, a defect, or a measurement that is not yet a plan and says draft it, write it up, or make this a plan. This skill owns prose and never state: it writes no design, no stages, no status marker, and no Linear issue, and it hands a backlog row to the plans skill only after the user approves every value in it. Starting the plan — design, stages, the build-order row, Linear — is always a separate invocation, plan-start, which this skill names but never runs."
---

# Drafting a plan

This is the first of the two writing passes `docs/PLAN_DOCUMENT.md` splits
apart: **draft**, which is generated and skimmed, and **start**, which is
argued line by line. This skill is the first half only. It never runs the
second half itself, even when drafting and starting happen back to back in one
sitting — the split is between two pieces of work, not two sittings, and what
must never happen is skipping the review pass that a separate invocation
forces into view.

## What this skill produces, and what it deliberately does not

| Writes | Never |
|---|---|
| the plan number and its permanent filename | `## Design`, `## Stages`, or any stage |
| `## What this plan is for` | a status marker |
| `## The case` | a `docs/PLANS.md` row it composed itself |
| — for a backlog landing, the row via `plans` operation 6 | a Linear issue or project |
| the destination question, asked once | `plan-start`, or anything `plan-start` owns |

A plan going to the build order gets **no backlog row on the way** — writing
one and removing it minutes later would put a transition in the record that
never happened, which is exactly what `plan_state_reconciliation.md` exists to
keep honest.

## Phase 1 — Gather

Read, write nothing:

1. **The idea itself** — what prompted it. A measurement, a defect found
   mid-task, a rejected alternative from another plan, a standing frustration.
   Whatever it is, it is the seed of `## The case`.
2. **The next plan number.** List `docs/plans/plan_*.md`, take the highest
   `NNN`, and use the next integer. Confirm it is not already claimed by
   scanning the `Plan` columns of `docs/PLANS.md`'s four tables and
   `docs/planning/completed_plans.md` — the same check operation 6 makes
   before it will insert a row.
3. **Whether this is genuinely new**, or a stage of a plan that already has a
   document. A slice of existing work gets a stage in that document, not a new
   plan number — check before allocating one.

## Phase 2 — Allocate the number and filename

`docs/plans/plan_NNN_<slug>.md`, where `<slug>` is a short, readable stand-in
for the title. This filename is permanent: inbound links — Linear issues, other
plan documents, commit messages — outlive later changes to the plan's wording,
so it is fixed the moment the file is created and never renamed for a better
slug later.

## Phase 3 — Write the first two sections, and nothing else

### `## What this plan is for`

Present tense, for a reader who has never seen this repository. One or two
sentences, under **320 characters** — it is one of the two sections
`docs/PLAN_DOCUMENT.md` caps and publishes, and the cap is enforced at build
time, not proposed as a guideline.

**Refuse to write a placeholder.** If the idea cannot yet be stated in one
sentence a stranger could read, that is a sign the idea itself is not ready to
be a plan — say so and stop, rather than inventing prose to fill the section.

This section is also **frozen going forward**: `docs/PLAN_DOCUMENT.md` states
that it is not edited because work progressed — not a stage landing, not a
slice being re-pointed, not a ticket closing. Only a change in the plan's
actual purpose reopens it. Worth saying once at draft time, since this skill is
the only one that ever writes it fresh.

### `## The case`

Makes the case for building it. Required to exist; **free in form**. Carry over
the prose that produced the idea if it already argues the point — a rejected
alternative from another plan, the paragraph around a measurement, a defect
report. Do not manufacture a measurement to justify a plan that does not have
one yet; an invented number here is worse than an argument with none.

If the plan has an origin worth recording — raised from a measurement, split
out of another plan, rescoped after a review — that belongs here too. It is the
one place a plan's own history lives, since the document carries no status
marker to hold it instead.

## Phase 4 — Ask where it lands

This is the question `docs/PLAN_DOCUMENT.md` treats as **the** decision, not a
default: *should I build this?* Ask it once, after the first two sections are
written, and offer exactly three answers:

| Answer | Lands in | What you need from the user |
|---|---|---|
| Yes, sequence it | build order | nothing more from this skill — name `plan-start` as the next, separate invocation |
| Yes, but not yet | backlog | one sentence: the trigger that would move it |
| Don't know yet | backlog | one sentence: what would tell you |

**The build-order path stops here.** This skill writes no position, priority,
effort, or stage — those are `plan-start`'s interview, and it is always its own
invocation, offered but never run automatically. Say plainly that `plan-start`
is next and stop.

**The backlog path continues to the approval stop below.** The trigger sentence
doubles as the reason to come back and run `plan-start`: a backlog row without
one is a wish, not a plan, and `docs/PLANS.md` says exactly that.

## Phase 5 — Backlog only: propose the row, then stop

Gather every value `plans` operation 6 requires for a backlog insert, and do
not derive any of them from the document:

- **placement** — `first`, or `after Plan N`;
- **title** — as it should read in the `Title` column;
- **priority** — the index's numeric scale;
- **effort** — `XS`/`S`/`M`/`L`/`XL`;
- **trigger** — the sentence from Phase 4, verbatim.

Present all five together with the exact row text, and **stop**. Do not call
`plans` yet. This is the one approval stop this skill has, and it is the
moment `docs/PLAN_DOCUMENT.md` means by "its one approval stop is the
trigger" — reasoning the user can still argue with, before any file changes.

Once approved, hand the five values to the `plans` skill's operation 6 exactly
as approved. Do not run `tests/test_planning_docs.py` or
`build_public_roadmap.py` yourself first — operation 6 owns its own preflight
and after-every-operation checks; running a parallel copy here only invites the
two to disagree.

## After writing

Report:

- the plan number and filename;
- the text of both sections, as written;
- which of the three answers Phase 4 got, and the trigger sentence if backlog;
- for a backlog landing, whether `plans` operation 6 completed and what row
  resulted — or that it is still waiting on approval;
- for a build-order landing, that `plan-start` is the next step and that this
  skill did not run it;
- anything Phase 1 could not settle — a number collision, an idea that turned
  out to be a stage of an existing plan, a case with no argument yet.

## What this skill must never do

- **write `## Design` or `## Stages`**, or any stage content. That crosses the
  start boundary this split exists to protect.
- **write a status marker.** The document carries none; the index owns state.
- **write or edit a `docs/PLANS.md` row directly.** Every row goes through
  `plans`, with values the user approved.
- **write before the backlog approval stop**, or derive a placement, title,
  priority, effort, or trigger from the document instead of asking.
- **create a Linear issue or project.** `plan-start` owns the issue set; this
  skill produces nothing Linear reads.
- **run `plan-start`.** Name it and stop, even when drafting and starting are
  clearly going to happen in the same sitting.
- **emit a placeholder `## What this plan is for`** when the idea cannot yet
  support one sentence a stranger could read.
- **give a plan going to the build order a backlog row on the way.**
