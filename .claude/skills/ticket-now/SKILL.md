---
name: ticket-now
description: "Add one issue to the Cartracker Linear cycle right now, for work just discovered that needs doing — a defect found mid-task, a gate that turns out to be unowned, a follow-up that cannot wait for the next seeding. Use when the user says to make a ticket, add this to the cycle, file it now, or track this. This skill writes Linear and never repository state: it does not edit docs/PLANS.md, a plan document, a status marker or the archive, and it refuses work that has no canonical plan rather than inventing one."
---

# Adding one issue mid-flight

Plan 149 makes Linear a **projection** of repository planning, never a second
owner of it. This skill exists for the one case that projection cannot absorb
on a weekly rhythm: you found something during the work, it needs doing, and
waiting until the next seeding would lose it.

That is a real need. It is also the exact behaviour Plan 149 is measuring.

## The measure this skill must not corrupt

Plan 149 Stage 2 tracks six measures. One of them is:

> **Issues added after cycle start** — is unplanned work visible, or merely
> normalized?

A skill that makes mid-cycle insertion frictionless answers that question by
destroying it. So this skill's job is not to make the interruption cheap. It is
to make the interruption **legible**: every issue it creates carries when it was
added, against which cycle, and why it could not wait.

Frictionless capture with an honest record is the goal. Silent capture is the
failure.

## The boundary

| You do | The user does |
|---|---|
| create one Linear issue, filled to the six-field contract | supply the **Why now** sentence |
| derive Outcome, Canonical plan, Exit and Evidence destination from the plan document | decide the work is worth interrupting for |
| resolve the target cycle and say which one it is | approve a new plan, if the work has none |
| stamp the insertion date and cycle state | choose priority and estimate, or decline to |
| report what you could not fill | |

**Why now is never derived.** Every other field has a source in the plan
document; this one has a source only in the user's head. It is the field that
separates genuine urgency from convenience, and a plausible sentence written
here would make the Stage 2 measure meaningless. If you were not given one, ask
for it and wait.

## Repository state is not yours

Plan 149 lists out of scope: *"allowing Linear status changes to edit repository
planning state"* and *"automations that create or reorder plan rows."*

This skill therefore never touches `docs/PLANS.md`, a plan document, a status
marker, `completed_plans.md`, or `docs/recaps/`. The mirror of the `plans`
skill, which writes repository state and never Linear.

If creating the issue reveals that the build order is wrong, a plan is
mis-stated, or a row's trigger has silently been met — **report it, do not fix
it.** That is a decision the user makes in the repository, with the `plans`
skill if it moves a row.

## Refusing work with no canonical plan

Field 2 of the issue contract is a link to `docs/plans/plan_NNN_*.md` and its
stage. If the work you are asked to ticket has no such home, **stop and say
so.** Do not link the nearest plausible plan and do not write "n/a".

Execution work with no plan is one of three things, and all three are
repository decisions the user makes outside this skill:

- a slice of an existing plan whose document does not yet describe it — the plan
  document gets the stage, then the issue links it;
- a defect in shipped work — it belongs to the plan that shipped it;
- genuinely new work — it needs a plan document before it needs a ticket.

The exception is Plan 149's own measurement work, which is its own project.

Naming the missing plan is useful output. Inventing a link is not.

## The workspace, as it actually is

Verified 2026-08-25. Re-read rather than trusting these values if anything
disagrees.

- One team: **Cartracker**, `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
- Statuses: `Backlog`, `Ready`, `In Progress`, `In Review`, `Soaking`, `Done`,
  `Canceled`. `Duplicate` is reserved by Linear — never set it.
- Projects exist per plan, named `Plan NNN — Title`. Create none here; a plan
  outside the current horizon should not gain a project as a side effect of one
  ticket. If the plan has no project, report that and put the issue in the cycle
  without one.
- **Estimates are required.** `1` under half a day, `2` about a day, `3` two to
  three days, `5` most of a week — the Fibonacci scale, enabled team-wide on
  2026-08-25.

  An issue with no estimate is **invisible to throughput**: it consumes a cycle
  slot and contributes zero points, so the cycle reads as less work than it
  held. CAR-6 is the existing example — Cycle 1 shows 12 points across six
  issues, and one of those six is uncounted.

  Never derive the estimate from a plan's effort label. `PLANS.md` effort sizes
  a whole plan; a point estimate sizes one issue, and one plan produces several
  independently sized issues. Plan 141 is effort `S` while its first slice
  CAR-10 is 3 points. If you cannot size the work, ask — do not guess and do not
  leave it blank.

## Resolving the target cycle

**Do not assume a cycle is current.** On 2026-08-25 none was: Cycle 1 opened
2026-08-31, and all three cycles returned `isCurrent: false`.

```
list_cycles(teamId, type: "current")  -> is there an active cycle?
```

Three cases, and they are not the same:

| State | Target | Stamp |
|---|---|---|
| A cycle is current | that cycle | **mid-cycle insertion**, with the date |
| No cycle is current, one starts later | the next cycle | **pre-cycle seeding** — *not* an insertion |
| No cycle exists at all | no cycle; `Backlog` status | report it |

The middle row is the one that matters. An issue added on 2026-08-27 to a cycle
that opens 2026-08-31 is not "added after cycle start" — it is ordinary
seeding, and counting it as an interruption would inflate the measure with work
that interrupted nothing.

Say which case you hit, in your report.

## The issue

Title: `Plan NNN [Stage N]: <the outcome, imperative>`. Match the existing
corpus — `Plan 142 Stage 1: scoped operational coordination + drain contract`.

Description is exactly the six fields, in this order, and nothing else:

```markdown
## Outcome

One sentence describing the finished state.

## Canonical plan

Plan NNN: `docs/plans/plan_NNN_*.md`, Stage N.

## Why now

The user's sentence, verbatim.

## Exit

- [ ] Observable check.
- [ ] Observable check.

## Blocked by

The issue relation plus the plan's actual gate in words. "Nothing" if nothing.

## Evidence destination

The plan section that receives deploy and soak facts.

---
Added mid-cycle YYYY-MM-DD to Cycle N.
```

The trailing line is the stamp. Use it **only** for a genuine mid-cycle
insertion; for pre-cycle seeding write `Seeded YYYY-MM-DD before Cycle N
opened.` and for no cycle omit it entirely.

Do not paste the plan's problem statement, architecture, rejected alternatives,
runbook or evidence into the issue. Plan 149 is explicit: if the issue needs
that context, it **links** to it.

## Status on creation

`Ready` if it is genuinely pullable now. `Backlog` if it is not, in which case
say why it was added to the cycle at all — an unpullable mid-cycle addition is
usually a sign the work belongs in the next seeding instead.

Never create an issue directly in `In Progress`. If the user is already doing
the work, create it `Ready` and say that it needs moving — the transition is
theirs, and a ticket that is born mid-flight has no start time.

## After creating

Report:

- the issue identifier and URL;
- which cycle it landed in and which of the three cycle cases applied;
- the cycle's **point total** against Plan 149 Stage 1's budget, and whether
  this addition crossed it. Sum estimates, do not count issues: Stage 1's cap
  became a points budget on 2026-08-29, because Cycle 1 seeded exactly eight
  issues and still ran to 21. Read the current number from the plan;
- anything you did not fill — a missing project, an absent estimate, a `Blocked
  by` you could not state;
- anything you noticed about repository state and deliberately did not change.

If the addition takes the cycle past its budget, say so plainly. The budget is
Plan 149's, it is about whether a cycle is an honest commitment, and quietly
exceeding it is how a board stops meaning anything. You do not refuse the
addition — the user decided — but the total goes in the report.

This is the measure `ticket-now` is most able to distort. "Issues added after
cycle start" ran at **62% in Cycle 1**, and every one of them came through this
skill. That is not an argument for adding less; it is why the number has to be
reported accurately every time, so the next seeding is calibrated against what
actually happened rather than against what was planned.

## What this skill must never do

- **write repository state.** No `PLANS.md`, no plan document, no status marker,
  no archive, no recap.
- **invent a canonical plan link.** Missing plan is a stop.
- **write the Why now sentence.** Missing sentence is a stop.
- **create a project**, reorder a cycle, or move another issue's status.
- **stamp pre-cycle seeding as a mid-cycle insertion**, which would corrupt the
  measure this skill is built around.
- **create more than one issue.** Several issues at once is seeding, and
  seeding is `fill-cycle`.
