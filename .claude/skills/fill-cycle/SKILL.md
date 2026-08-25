---
name: fill-cycle
description: "Seed or top up a Cartracker Linear cycle from the authoritative build order in docs/PLANS.md — compose a full cycle's worth of work, or add just enough to fill the remaining headroom. Use when the user asks to fill the cycle, seed the next cycle, plan the week, or add more work to the board. This skill proposes the selection and writes nothing until the user approves it, and it writes Linear only: it never edits docs/PLANS.md, a plan document, a status marker or the archive."
---

# Filling a cycle from the build order

Plan 149 Stage 1 is this operation, written out: seed **no more than eight
issues**, composed from the top of `docs/PLANS.md` **as it stands that
morning**. Not from a frozen list, not from the whole roadmap.

> A board that begins by mirroring the whole roadmap has already failed the
> experiment.

That sentence is the design constraint. This skill selects a horizon, not a
backlog.

## Two modes, one selection rule

| Mode | When | Target |
|---|---|---|
| **seed** | the cycle is empty or nearly so | compose up to the cap of eight |
| **top-up** | the cycle is running and has room | fill only the measured headroom |

Both read the build order fresh. Both propose before writing. The only
difference is how many slots you are filling.

If the user does not say which, count the target cycle's issues and pick: an
empty cycle is a seed, a partly-full one is a top-up. Say which you chose.

## The composition

Stage 1 defines what a cycle is made of. It is five categories, not "the top
eight rows":

1. the currently active top slice;
2. the next two unblocked slices;
3. any higher-priority slice whose **timed gate ends inside the cycle**;
4. at most **two** safe fillers that become pullable only while a higher item
   soaks;
5. Plan 149's own bootstrap and measurement work.

Category 3 is the one that gets missed. A plan blocked today whose soak or
observation window closes on Wednesday is *in scope for this cycle* — the board
should hold it, because the person will be able to pull it. Read every `Blocked
by` cell for a date, not just for a plan number.

Category 4 has a hard limit of two, and each filler must name the item it soaks
behind. A filler with no soak to hide under is just extra work, and it is how a
cycle quietly becomes a wish list.

## What is not eligible

- **Backlog rows whose trigger has not been met.** The backlog table's rule is
  that every row names the trigger that would move it; an unmet trigger means
  the work is not startable, and putting it in a cycle asserts otherwise. If you
  believe a trigger *has* been met, that is a finding to report — the row moves
  via the `plans` skill, with the user's decision, before it can be seeded.
- **Superseded and closeout rows.** Closeout rows owe no code by definition.
- **Blocked rows whose gate does not close inside the cycle.**
- **Plans with no document.**

## Repository state is not yours

Plan 149 lists out of scope: *"automations that create or reorder plan rows"*
and *"allowing Linear status changes to edit repository planning state."*

So this skill reads `docs/PLANS.md` and plan documents, and writes neither. It
does not move a backlog row whose trigger has fired, does not renumber the build
order, does not touch a status marker. The mirror of the `plans` skill, which
writes repository state and never Linear.

Findings go in the report. Edits do not happen.

## The workspace, as it actually is

Verified 2026-08-25. Re-read rather than trusting these values.

- One team: **Cartracker**, `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
- Cycles are Monday-to-Sunday. On 2026-08-25 **none was current** — Cycle 1
  opened 2026-08-31 and all three returned `isCurrent: false`. Resolve the
  target explicitly and name it in the proposal; never assume "the cycle" is
  unambiguous.
- Statuses: `Backlog`, `Ready`, `In Progress`, `In Review`, `Soaking`, `Done`,
  `Canceled`. `Duplicate` is reserved — never set it.
- **An issue in a plan's project is not necessarily in the cycle.** On
  2026-08-25 CAR-8 and CAR-9 sat in their projects with no `cycleId`, held out
  of Cycle 1 on purpose. Count `cycleId`, never project membership, and never
  sweep an uncycled issue into a cycle because it looked orphaned — ask.
- **Estimates are required on every seeded issue.** `1` under half a day, `2`
  about a day, `3` two to three days, `5` most of a week — the Fibonacci scale,
  enabled team-wide on 2026-08-25. An unestimated issue consumes a cycle slot
  and contributes zero points, so the cycle under-reports its own load.
  Never mechanically convert a plan's effort label into points: `PLANS.md`
  effort sizes a whole plan, a point estimate sizes one issue.
- The proposal's point total is part of what the user is approving. Say it, and
  say it against the previous cycle's total once one exists.

## Counting headroom

```
list_issues(team: "Cartracker", cycle: <target>)  -> current count
```

Headroom is **eight minus the issues already carrying that `cycleId`**,
including ones already `Done` — a finished issue still consumed a slot, and
pretending otherwise inflates the next cycle.

If the cycle is already at or over eight, do not propose additions. Report the
count and stop. The user may still decide to add, but that is `ticket-now` with
its cap warning, not a bulk fill.

## Propose, then write

**Nothing is created until the user approves the list.** Unlike `ticket-now`,
which trades a confirmation step for speed on a single issue, this operation
creates several at once and a wrong selection is tedious to unwind.

The proposal is a table:

| Plan | Slice | Category | Est | Why it is pullable |
|---|---|---|---:|---|

One row per proposed issue. `Category` is the Stage 1 number, so the shape of
the cycle is visible — five rows all reading "next unblocked slice" is a cycle
with no soak-aware sequencing, and the user should be able to see that before
agreeing.

State the target cycle, the current count, the headroom, and what you are
leaving out and why. Then wait.

## Writing the issues

On approval, create each to Plan 149's six-field contract — the same body
`ticket-now` writes:

```markdown
## Outcome

## Canonical plan

## Why now

## Exit

## Blocked by

## Evidence destination
```

For seeded issues, **Why now is derivable** and you write it: it is the
build-order position or the higher-row gate that makes this safe filler. That is
the opposite of `ticket-now`, where Why now is the user's sentence about an
interruption — here the answer genuinely is "because it is next," and the build
order is the evidence.

Every other field comes from the plan document. Do not paste the plan's problem
statement, architecture, rejected alternatives or evidence; link to them.

Status on creation:

- `Ready` for genuinely pullable slices;
- `Backlog` for category-3 items whose gate has not yet closed, with the gate
  written in `Blocked by`.

Never create in `In Progress`, `In Review`, `Soaking` or `Done`.

Set `project` to the plan's existing project. If a plan in the horizon has no
project, say so and create the issue without one — a project is a Plan 149
horizon decision and creating one silently expands the board's scope.

## After writing

Report:

- each issue created, with identifier and URL;
- the cycle's count before and after, against the cap of eight;
- the category mix, so an all-category-2 cycle is visible;
- every eligible row you left out and why;
- every finding you did not act on — a backlog trigger that looks met, a stale
  issue status, a build-order row contradicting its plan document.

That last item matters more than it looks. On 2026-08-25 CAR-7 sat `In Review`
after its PR had merged that morning, and `PLANS.md` still described that PR as
a draft. Seeding is when such drift is most visible, and reporting it is the
whole of your part in fixing it.

## What this skill must never do

- **write repository state.** No `PLANS.md`, no plan document, no status marker,
  no archive, no recap. A backlog trigger you believe has fired is a report.
- **create issues before the user approves the list.**
- **exceed eight** without saying so, or silently count `Done` issues out of the
  cycle to make room.
- **seed a blocked row** whose gate does not close inside the cycle.
- **create a project** for a plan outside the horizon.
- **move an existing issue between cycles** or change another issue's status.
  Rollover is the user's decision and one of Plan 149's six measures.
- **choose a build-order position or a priority.** Those live in `PLANS.md` and
  arrive from the user.
