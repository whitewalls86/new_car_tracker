# The cycle-close order

**Owner:** [Plan 164](../plans/plan_164_cycle_close_ritual.md) Stage 0.

Closing a cycle is eight steps owned by six skills and two gaps, and the order is
not a preference. Two of the steps are only ever *correct* at the close, one of
them destroys the evidence another one reads, and one of them has to happen
before a deadline that passes whether anyone is looking or not.

This file is the sequence. Nothing here decides that a gate closed, that a plan
is finished or that a measure is good — every step names its owner, and the
owner does the judging.

## The two rules the order falls out of

Everything below is a consequence of these, and a step whose position looks
arbitrary is a step one of them explains.

1. **A read comes before any write that would perturb it.** The cycle measures
   are read before anything touches a `cycleId`; the recap is written before any
   ref is deleted.
2. **A step that destroys evidence runs after every step that reads it.** There
   is exactly one such step — ref hygiene — and it is last for that reason
   alone.

A third rule is not about ordering but about the boundary, and it decides which
half of this file a step lives in: **nothing touches a `cycleId` until the
cycle has actually ended.** Moving work forward early falsifies Plan 149's own
rollover measure, which is a measure *of* how much moved.

## Before the boundary

The cycle's `endsAt` is a deadline, not a checkpoint. Linear moves every
incomplete issue to the next cycle when it passes — Cycle 1's two rollovers
happened that way, with nobody doing anything — so an issue still open at the
boundary has left the cycle, and closing it afterwards credits the wrong one.

| # | Step | Owner | Must be preceded by | Why here |
|---|---|---|---|---|
| 1 | Close every stage that is actually finished, with its evidence | [`stage-close`](../../.claude/skills/stage-close/SKILL.md) | the stage being finished | Completion is counted at the boundary. A stage closed on Monday is Monday's cycle's |

That is the only step with a deadline, and it is the one most easily left until
"the close" — at which point it is too late to count.

## The boundary

`endsAt` passes. **Confirm it before running anything below**: read the cycle's
state, and read which issues Linear already moved on its own. Cycle 1's rollover
needed no manual action at all, so the first job of the rollover step is to find
out what is actually left to do rather than to move things.

Reading the measures early is the specific failure this boundary exists to
prevent. The mid-cycle read of 2026-08-29 recorded ~30 points completed against
the closed cycle's 44 — **a 47% undercount**, which was written into a plan's
budget and into two skills before anyone caught it.

## After the boundary

| # | Step | Owner | Must be preceded by | Must not run before |
|---|---|---|---|---|
| 2 | Record Plan 149's cycle measures | *unbuilt — Plan 164 Stage 1* | the boundary | anything that changes a `cycleId`, which is step 3 |
| 3 | Roll the unfinished issues that Linear did not move | *unbuilt — Plan 164 Stage 2* | step 2 | the boundary |
| 4 | Close out plans whose gates closed | [`close-out`](../../.claude/skills/close-out/SKILL.md) | step 1, for the stages those plans carry | — |
| 5 | Move the rows step 4 proposed | [`plans`](../../.claude/skills/plans/SKILL.md) | step 4 | — |
| 6 | Write the week's recap | [`plan-week`](../../.claude/skills/plan-week/SKILL.md) | steps 4 and 5 | — |
| 7 | Seed the next cycle | [`fill-cycle`](../../.claude/skills/fill-cycle/SKILL.md) | steps 3 and 5 | step 3 |
| 8 | Git ref and worktree hygiene | [`ref-hygiene`](../../.claude/skills/ref-hygiene/SKILL.md) | step 6 | step 6 |

### What each ordering constraint is for

**2 before 3** is rule 1. The measures are a reading of a closed cycle, and step
3 rewrites the field they are read from. Take the number first.

**3 before 7.** `fill-cycle` composes against the cycle's remaining headroom,
and rolled-in work consumes headroom. Seeding first produces an over-full cycle
whose overflow rolls again next week.

**4 and 5 before 6.** The recap reads plan documents for *why* a commit
happened. A plan that closed out during this cycle and has not been written up
yet reads to the recap as still in progress, and the recap is the durable
record — the wrong version of it is the one that survives.

**5 before 7.** `fill-cycle`'s authoritative build order is
[`PLANS.md`](../PLANS.md). Seeding from an index whose rows have not moved yet
selects work that is already finished.

**6 before 8, and 8 last of everything.** This is rule 2. Step 8 destroys
evidence by design, so everything that reads git history goes ahead of it.

**How much this ordering is worth was measured on 2026-09-04, and it is less
than it first appeared.** Over 2026-08-24..30, 200 of 209 non-merge commits
attributed from their own subject or body. For the 9 that did not,
`git branch --contains` returned **75 refs each** and discriminated nothing;
what answered all 9 was the enclosing merge commit, whose subject carries the
branch name in `master`'s history permanently and survives any branch deletion.
`--contains` is only informative for commits *not* on `origin/master` — 24 of
them, 1-2 refs each — and those sit on branches `ref-hygiene` refuses to delete,
because they carry unlanded commits.

So the ordering is **defence in depth rather than the load-bearing constraint**.
It is kept because it costs nothing and the risk is one-sided, and it is
recorded honestly because a rationale that does not survive being checked is
worse than no rationale.

The thing that *would* cost the recap its fallback is enabling **squash
merging**, which leaves no merge commit and therefore no permanent record of the
branch name. That is a repository setting, not an ordering.

## Two steps are unbuilt, and this file says so on purpose

Steps 2 and 3 have no owner today. Plan 164 Stages 1 and 2 are where they get
one, and both need a *closed* cycle to develop against, so they could not be
built at the same time as this file.

Until they exist, those steps are done by hand, in that position, with the same
constraints: read the measures before touching any `cycleId`, record a partial
measure as partial, and confirm what Linear already moved before moving
anything.

**A partial measure is recorded as partial.** Cycle 1's "state corrections" row
is three known occurrences with no exhaustive count, and says so. A total that
is not one is worse than an admitted gap.

## Prevention sits outside this order

Step 8 exists because refs accrete. Three settings decide whether they accrete
at all, and none of them is a step in this sequence — they are configured once
and then hold:

| Setting | Where it lives |
|---|---|
| `delete_branch_on_merge` | the remote, so it is shared by every machine |
| `fetch.prune = true` | each machine's `~/.gitconfig` |
| `push.autoSetupRemote = true` | each machine's `~/.gitconfig` |

With all three on, a branch merged and deleted from one checkout reads `gone` on
every other checkout at its next fetch. That is git's own propagation between
machines; it needs no hook and no coordination.

`scripts/audit_git_refs.py` reports on all three from wherever it runs, which is
how a two-machine setup notices its own drift without a registry to maintain.
Measured 2026-09-04 with all three off: 69 remote branches, 67 already merged,
and no local branch anywhere that could read `gone`.

**The remote half is enforced in CI.**
`scripts/verify_git_ref_hygiene_contract.py` fails the build if
`delete_branch_on_merge` is turned off, if squash or rebase merging is enabled,
or if merged-but-undeleted branches climb past a dated ratchet. Its scope is the
remote on purpose: that is the one piece of configuration both machines share,
and local branch state cannot be checked from a fresh clone without producing a
green that means nothing.

## What enforces this order, and what cannot

Steps here are performed by a person invoking a skill, so most of the sequence
is a discipline rather than a mechanism. Two parts of it are held mechanically,
and it is worth being exact about which:

| Held by | What it catches |
|---|---|
| `verify_git_ref_hygiene_contract.py`, in CI | the remote being reconfigured to accrete refs, from any machine |
| `test_the_recap_series_has_no_interior_gap` | a week that was skipped and never noticed |
| `test_the_recap_series_is_not_more_than_a_week_behind` | the weekly habit stopping, with one week of grace before it goes red |

Nothing mechanises steps 1 through 5 or step 7. They write Linear and `docs/`
state from judgements no test can make, and a check that asserted they had "run"
would be asserting that a file changed, which is not the same claim.

## What this file is not

It is not a status surface. Which plans are live, which have closed out and what
each cycle measured live in [`PLANS.md`](../PLANS.md),
[`completed_plans.md`](completed_plans.md) and Plan 149's own document. This is
the order of operations and nothing else, and it changes only when a step's
owner or a step's constraint changes.
