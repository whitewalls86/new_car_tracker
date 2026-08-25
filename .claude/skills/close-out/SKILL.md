---
name: close-out
description: "Close a finished Cartracker Linear issue — gather its evidence, record what it actually cost against its estimate, write the plan document's evidence section, and move the plan's row in docs/PLANS.md if and only if the plan itself changed state. Use when the user says an issue or slice is done, finished, shipped, or ready to close. This skill gathers and proposes first and writes nothing until the user approves: it never decides that a gate has closed, and it never moves a row on the strength of a summary it wrote itself."
---

# Closing out a finished slice

Three records describe one finished piece of work, and they are finished at
different moments:

| Record | Says | Finished when |
|---|---|---|
| the Linear issue | this slice is done | its `Exit` checks pass |
| the plan document | what the work proved | its evidence is written |
| `docs/PLANS.md` | where the plan now sits | a **gate** closes — often much later |

Most closeouts touch the first two and leave the third alone. Plan 149 is
explicit: a finished slice reaching `Done` *"does not by itself close, archive
or move a plan in `PLANS.md`."* Treating every closed ticket as a plan
transition is the main way this skill could do damage.

## Why this skill stops in the middle

Plan 146 Stage 6 separates summarising from transitioning, and the `plans`
skill states the reason:

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

1. **The Linear issue** — its `Exit` checklist, `Blocked by`, `Evidence
   destination`, `estimate`, `startedAt`, `completedAt`, current status.
2. **Its canonical plan document** — the stage the issue names, and what that
   stage says it owes.
3. **The commits** — plan-attributed via the `commit-plan-attribution`
   convention, so the plan number resolves them from the message alone.
4. **The gate**, if the plan has a row in `PLANS.md` — the exact words of the
   `Gate`, `Blocked by` or `Trigger` cell.
5. **The evidence the gate names** — a soak result, a metric, a migration, a
   test run. Read it if it is readable from here; say plainly if it is not.

## Phase 2 — Propose, then stop

Present, in this order:

1. **What the issue claimed to do**, and whether each `Exit` check is met,
   unmet, or unverifiable from here. Never mark a check met because the
   surrounding work looks finished.
2. **Cost.** The estimate, the actual, and the delta — see below.
3. **The plan document edit**, as the exact text you propose to add and where.
4. **The `PLANS.md` consequence**, chosen from:
   - **nothing** — the slice is done, the plan continues. *This is the common
     case and should be proposed without apology.*
   - **build order → closeout** — deployed, evidence pending. Needs a `Lands`
     date and a gate, and neither is yours to invent.
   - **closeout → archive** — the gate closed. Needs the archive description.
   - **anything → superseded** — needs what superseded it.
5. **What you could not verify**, named explicitly.

Then **stop**. Do not write. Do not call another skill.

### Deciding a gate has closed is not yours

The `plans` skill is unambiguous: *"You also do not decide the gate has closed.
The user says it has."*

Phase 2 may say "the gate reads X, and commit `abc123` appears to satisfy it
because Y." It may not say "the gate is met, moving the row." The difference is
whether the user can reject your reading on its evidence — which requires the
evidence on screen and the row still unmoved.

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

Write it as a comment on the issue, never into the description body the
`ticket-now` and `fill-cycle` skills own:

```markdown
**Closeout** YYYY-MM-DD
estimate N → actual M (±D)
cause: one line, only when the delta is worth a reason
evidence: plan_NNN_*.md §Stage N
```

A delta of zero is still worth writing. A run of accurate estimates is the
evidence that the scale works.

## Phase 3 — Write, in this order

Only after approval, and only what was approved.

1. **The plan document's evidence section.** Add to the section the issue's
   `Evidence destination` names. Additive — you are recording what happened,
   not revising what the plan intended. Do not rewrite the plan's problem
   statement, stages, or design because the work turned out differently; a plan
   that needs redesigning is a separate conversation.
2. **`docs/PLANS.md`, via the `plans` skill** — only if phase 2 proposed a
   transition and the user approved it. Pass the approved values through and
   tell that skill they were approved in the open session, with their source.
   Never edit `PLANS.md` directly from here.
3. **Linear** — set the issue to `Done`, post the closeout comment. Leave the
   estimate alone.

If any step fails, stop and report. Do not carry on to the next: a plan
document recording evidence for a row that never moved is a contradiction the
next reader has to untangle.

## Which status, and when not to use Done

- **`Done`** — the `Exit` checks are met.
- **`Soaking`** — implementation is complete but a time or evidence gate is
  running. The issue names the gate and its end time. This is not a closeout;
  do the cost comment and stop.
- **`Canceled`** — the slice was rejected, superseded, or made unnecessary.
  Plan 149: the reason belongs in the canonical plan when it changes that
  plan's design. Cost is still worth recording if work was done.

An issue whose `Exit` checks are not all met is not `Done` because the user is
finished with it. Say which checks are outstanding and let them decide.

## The workspace

- Team **Cartracker**, `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
- Estimation is **Fibonacci**, enabled 2026-08-25. Points are live, so an
  unestimated closed issue silently undercounts its cycle.
- Statuses: `Backlog`, `Ready`, `In Progress`, `In Review`, `Soaking`, `Done`,
  `Canceled`. `Duplicate` is reserved — never set it.
- Cycle 1 (2026-08-25 → 08-31) is six days and contains work completed before
  it opened. Its totals are not a clean baseline; say so if a closeout report
  compares against it.

## After writing

Report:

- the issue, its final status, and the cost line recorded;
- every file changed, and the diff's line count;
- whether `PLANS.md` moved, and if not, why not;
- which values were supplied by the user and which were approved from your
  proposal, naming the source of each;
- anything left outstanding — an unverified `Exit` check, an unmeasured actual,
  a gate still open.

## What this skill must never do

- **write anything before the approval stop.**
- **decide that a gate has closed.** Propose a reading; the user decides.
- **move a `PLANS.md` row directly.** That is the `plans` skill's job, with
  values the user approved.
- **mark an `Exit` check met because the work looks finished**, or close an
  issue with outstanding checks without naming them.
- **overwrite `estimate` with the actual**, or derive an actual from elapsed
  calendar time.
- **rewrite a plan's design** because the work diverged from it.
- **close more than one issue per invocation.** Each has its own evidence and
  its own stop.
