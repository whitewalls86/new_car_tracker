---
name: fill-cycle
description: "Seed or top up a Cartracker Linear cycle from the authoritative build order in docs/PLANS.md — compose a full cycle's worth of work, or add just enough to fill the remaining headroom. Use when the user asks to fill the cycle, seed the next cycle, plan the week, or add more work to the board. This skill proposes the selection and writes nothing until the user approves it, and it writes Linear only: it never edits docs/PLANS.md, a plan document, a status marker or the archive."
---

# Filling a cycle from the build order

Plan 149 Stage 1 is this operation, written out: seed to a **points budget**,
composed from the top of `docs/PLANS.md` **as it stands that morning**. Not from
a frozen list, not from the whole roadmap.

> A board that begins by mirroring the whole roadmap has already failed the
> experiment.

That sentence is the design constraint. This skill selects a horizon, not a
backlog.

## The budget is a number this skill reads, never one it holds

**Stage 1 of [Plan 149](../../../docs/plans/plan_149_linear_execution_layer.md)
owns the budget. Read it there, every time.** It is calibrated on measured
completion and moves as cycles close; a copy here would be a second owner of a
number whose whole purpose is to change.

**Read the number, and check its date against the last closed cycle.** Stage 1
names **~30 points**, derived from a *mid-cycle* read of Cycle 1 on 2026-08-29.
Cycle 1 then closed at **44 points completed across 23 issues** — the mid-cycle
read undercounted by about 47%, because the last two days of the cycle landed
four plans. Plan 149's *Cycle measures* section records the close and says the
budget should be re-derived from it; until the maintainer sets the new figure,
say which number you are proposing against and why.

That is the general rule, not a one-off: a budget derived from an open cycle is
a floor, never the value.

This replaced a cap of *eight issues*, and the reason it was replaced is worth
carrying. Cycle 1 seeded 8 issues / **17 points** and closed at **25 issues / 48
points** — **68% of the issues and 65% of the points arrived after the cycle
started**. The cap held perfectly and measured the wrong thing: it counts issues
while the work is done in points, so seeding 17 points into a cycle that absorbs
48 guarantees the remainder shows up as unplanned work, which is what made
"issues added after cycle start" unreadable.

The issue count now falls out of the budget rather than driving it.

## Two modes, one selection rule

| Mode | When | Target |
|---|---|---|
| **seed** | the cycle is empty or nearly so | compose up to the budget |
| **top-up** | the cycle is running and has room | fill only the measured headroom |

Both read the build order fresh. Both propose before writing. The only
difference is how many points you are filling.

If the user does not say which, sum the target cycle's estimates and pick: an
empty cycle is a seed, a partly-full one is a top-up. Say which you chose.

## The budget is not a velocity promise

This is the caveat that travels with the number, and it is the one most likely
to produce a cycle that cannot be delivered.

Cycle 1's ~30 points were **Plan 145** — compute-bound, code-bound recovery
work, which is the fastest-moving kind here. Much of the build order is not that
shape:

- a **deploy-gated** slice waits on production between steps;
- an **observation window** — Plan 134's seven days, a soak, a re-measure gate —
  consumes calendar and cannot be compressed by working harder;
- a **wall-clock maintenance window** takes the time it takes.

**Velocity does not compress a seven-day window.** So the budget sizes a cycle
made of pullable work; a cycle whose top rows are gates and windows should be
seeded *below* it, and the proposal must say so rather than filling to the
number for its own sake.

State in the proposal how much of the budget is work that can actually be pulled
this week, and how much is waiting on a clock. If most of it is the latter, say
that the budget does not apply cleanly and propose the smaller figure.

## The composition

Stage 1 defines what a cycle is made of. It is five categories, not "whatever
fills the budget from the top":

1. the currently active top slice;
2. the next two unblocked slices;
3. any higher-priority slice whose **timed gate ends early enough inside the
   cycle to start the slice**;
4. at most **two** safe fillers that become pullable only while a higher item
   soaks;
5. Plan 149's own bootstrap and measurement work.

Category 3 is the one that gets missed, and it is also the one most easily read
too literally. Both halves matter:

**Read every `Blocked by` cell for a date, not just for a plan number.** A plan
blocked today whose soak or observation window closes on Wednesday is in scope —
the board should hold it, because the person will be able to pull it.

**Then check that the slice actually fits in what the gate leaves behind.** A
gate closing inside the cycle is necessary and not sufficient. Ask how long the
*next slice* needs and compare it against the time between the gate closing and
the cycle ending; if the answer is that the work starts next cycle, the row
belongs to next cycle.

The worked case, from Cycle 2's seeding on 2026-08-31. Plan 134 is priority 88 —
the highest on the board — and its blocker reads `Stage 1 observation window to
2026-09-06`, which closes inside a cycle ending 2026-09-07 05:00Z. By the date
test alone it is a category-3 hold. It is not one: the window closes with about
five hours of the cycle left, and Stage 2 is *three deploys at 48-hour intervals
in ascending blast radius*. Five hours buys none of it. Seeding it would have put
a highest-priority row on the board that nobody could start, which is the same
failure as seeding a blocked row — arrived at through the rule rather than
around it.

Where the two halves disagree, say so in the proposal and leave the row out.
That is a finding worth reporting, not a silent omission: it usually means the
`Blocked by` cell states the gate but not the lead time the slice needs after
it.

Category 4 has a hard limit of two, and each filler must name the item it soaks
behind. A filler with no soak to hide under is just extra work, and it is how a
cycle quietly becomes a wish list.

**The composition is now the primary guard, not the count.** With the budget in
points, nothing stops fourteen small issues arriving from one plan; what stops a
board mirroring the roadmap is that every issue traces to the top of the build
order through one of these five categories. Cycle 1's fourteen issues came from
six build-order rows against a 19-row build order and a 16-row backlog — the
selection rule was holding even while the count ran to 21. Report the category
mix and let the user see it.

## What is not eligible

- **Backlog rows whose trigger has not been met.** The backlog table's rule is
  that every row names the trigger that would move it; an unmet trigger means
  the work is not startable, and putting it in a cycle asserts otherwise. If you
  believe a trigger *has* been met, that is a finding to report — the row moves
  via the `plans` skill, with the user's decision, before it can be seeded.
- **Superseded and closeout rows.** Closeout rows owe no code by definition.
- **Blocked rows whose gate does not close inside the cycle**, or whose gate
  closes too late in it to start the slice.
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

Verified 2026-08-25, corrected 2026-08-29. Re-read rather than trusting these
values.

- One team: **Cartracker**, `ee63b26b-de49-4fa5-8617-bbaed7c1227d`.
- Cycles are Monday-to-Sunday. Cycle 1 ran **2026-08-25 → 08-31**, six days:
  Linear generated its first cycle from the Monday *after* the workspace was
  bootstrapped, and the cycles were shifted onto the Monday grid to correct it.
  Resolve the target explicitly and name it in the proposal; never assume "the
  cycle" is unambiguous, and never assume one is current.
- Statuses: `Backlog`, `Ready`, `In Progress`, `In Review`, `Soaking`, `Done`,
  `Canceled`. `Duplicate` is reserved — never set it.
- **An issue in a plan's project is not necessarily in the cycle.** On
  2026-08-25 CAR-8 and CAR-9 sat in their projects with no `cycleId`, held out
  of Cycle 1 on purpose. Count `cycleId`, never project membership, and never
  sweep an uncycled issue into a cycle because it looked orphaned — ask.
- **Estimates are required on every seeded issue**, and they are now what the
  budget is spent in. An unestimated issue contributes zero points, so it makes
  the cycle under-report its own load *and* silently widens the budget.
  Never mechanically convert a plan's effort label into points: `PLANS.md`
  effort sizes a whole plan, a point estimate sizes one issue.
- **Treat the scale as relative size, not as days.** Stage 1's table reads `1`
  under half a day, `2` about a day, `3` two to three days, `5` most of a week —
  and measured against Cycle 1 that mapping is **wrong by roughly 3x**: fourteen
  issues totalling 30 points is about 20 estimated working days delivered in 6
  calendar days. Plan 149 leaves the table standing deliberately rather than
  re-anchoring it on one short cycle, so estimate by comparison with issues
  already sized, and never defend a proposal with the day column.
- **Prefer several small issues to one large one**, and not only for estimation.
  Linear's merge automation flips an issue to `Soaking` once per merged PR, so a
  multi-PR issue gets pushed to `Soaking` mid-build and hand-corrected back —
  CAR-21 needed six PRs and read `In Progress → Soaking → In Progress → Soaking
  → In Progress → Done`. That is the dominant source of state corrections here,
  and it argues for slicing, not for disabling the automation.
- **Every PR title must carry its issue identifier**, `(CAR-NN)`. This is the
  whole difference between an issue whose automation fires and one that sits in
  `Ready` through its own build: attachment and status automation are separate
  mechanisms, and the branch-name convention establishes only the first. Say so
  in the report when you seed, since the branches start from these issues.
- The proposal's point total is part of what the user is approving. Say it,
  against the budget and against the previous cycle's completed total.

## Counting headroom

```
list_issues(team: "Cartracker", cycle: <target>)  -> issues and their estimates
```

Headroom is **the budget minus the points already carrying that `cycleId`**,
including issues already `Done` — finished work still spent the budget, and
pretending otherwise inflates the next cycle.

Two ways to get this wrong, both of which silently widen the budget:

- **counting issues instead of summing estimates.** That is the unit error the
  budget exists to fix.
- **treating an unestimated issue as zero.** It is not zero, it is unmeasured.
  Report it as a finding and ask, rather than seeding on top of it.

If the cycle is already at or over budget, do not propose additions. Report the
total and stop. The user may still decide to add, but that is `ticket-now` with
its warning, not a bulk fill.

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
- the cycle's point total before and after, against the budget, and the issue
  count as a consequence of it rather than as the target;
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
- **exceed the budget** without saying so; silently count `Done` issues out of
  the cycle to make room; or treat an unestimated issue as zero points, which
  widens the budget without anyone deciding to.
- **fill to the budget when the horizon is gates and windows.** The number sizes
  pullable work, and no amount of velocity shortens an observation window.
- **seed a blocked row** whose gate does not close inside the cycle, or closes
  so late in it that the slice cannot begin.
- **create a project** for a plan outside the horizon.
- **move an existing issue between cycles** or change another issue's status.
  Rollover is the user's decision and one of Plan 149's six measures.
- **choose a build-order position or a priority.** Those live in `PLANS.md` and
  arrive from the user.
